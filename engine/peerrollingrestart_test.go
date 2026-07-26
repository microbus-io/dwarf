/*
Copyright (c) 2026 Microbus LLC and various contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// restartFleet is the set of replicas a sampler should currently be pricing. A replacement is added
// BEFORE its Startup, so the sampler prices its bootstrap pool too - which is the only thing standing
// between the joining replica and the budget its peers have not yet given back.
type restartFleet struct {
	mu   sync.Mutex
	live []*Engine
}

func (f *restartFleet) add(e *Engine) {
	f.mu.Lock()
	f.live = append(f.live, e)
	f.mu.Unlock()
}

func (f *restartFleet) remove(e *Engine) {
	f.mu.Lock()
	for i, live := range f.live {
		if live == e {
			f.live = append(f.live[:i], f.live[i+1:]...)
			break
		}
	}
	f.mu.Unlock()
}

func (f *restartFleet) snapshot() []*Engine {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*Engine(nil), f.live...)
}

// ceiling is what the fleet may currently acquire from one shard's server: the sum of every live
// replica's applied pool ceiling. A replica mid-Startup or mid-Shutdown holds no shard and prices zero,
// which is the truth - it is holding no connections there.
func (f *restartFleet) ceiling(shard int) int {
	total := 0
	for _, e := range f.snapshot() {
		db, err := e.db.Shard(shard)
		if err != nil {
			continue
		}
		total += db.DB.Stats().MaxOpenConnections
	}
	return total
}

// awaitFleetSettled waits for every replica to agree on the fleet size and to hold the pool that size
// implies. Agreement is what makes the assertions either side of it meaningful: a per-replica reading
// that has not converged yet would let a stale pool pass for a settled one.
func awaitFleetSettled(t *testing.T, fleet *restartFleet, shard, wantR, wantOpen int) {
	t.Helper()
	assert := testarossa.For(t)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		settled := true
		for _, e := range fleet.snapshot() {
			db, err := e.db.Shard(shard)
			if err != nil || e.replicasOn(shard) != wantR || db.DB.Stats().MaxOpenConnections != wantOpen {
				settled = false
				break
			}
		}
		if settled {
			return
		}
		time.Sleep(time.Millisecond)
	}
	for i, e := range fleet.snapshot() {
		open := -1
		if db, err := e.db.Shard(shard); err == nil {
			open = db.DB.Stats().MaxOpenConnections
		}
		assert.Equal(wantR, e.replicasOn(shard), "replica %d never settled on a fleet of %d", i, wantR)
		assert.Equal(wantOpen, open, "replica %d never settled on a pool of %d", i, wantOpen)
	}
}

// TestPeerRollingRestart_FleetNeverExceedsTheShardBudget rolls every replica of a four-replica fleet, one
// at a time, and pins the property the announce-before-consume ordering exists for: the fleet's aggregate
// claim on one shard's server never exceeds that server's budget, at any instant of the rollout.
//
// The rollout is the case that can break it, because it moves the count in BOTH directions within a second
// or two. A replica leaving RAISES every survivor's share (three replicas of a 48-connection budget take 16
// each, not 12), and the replacement then has to fit inside a budget its peers have not given back yet. If
// it sized its pool from the fleet it is joining before its peers had read its row, the shard's server
// would see four replicas' worth of a three-replica split - over budget by a full share, on every step of
// every deploy. `Join` closes that by announcing, waiting two read cadences, and only then sizing, so a
// joining replica's whole claim during the window is its tiny bootstrap pool.
//
// The bound asserted is therefore the budget PLUS one bootstrap pool, and the margin is zero by design:
// three survivors at 16 plus a joiner at 4 is exactly 52. A regression in the ordering does not shave this
// bound, it blows past it by 12.
//
// What is priced is the CEILING each pool may acquire, not the connections open at that moment - lowering a
// limit closes nothing, so the surplus of a shrinking pool drains as connections are returned. The ceiling
// is the number the server's own limit has to accommodate, and it is the one a deploy can get wrong.
func TestPeerRollingRestart_FleetNeverExceedsTheShardBudget(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	const (
		shard    = 1
		vCPUs    = 8
		budget   = connsPerVCPU * vCPUs // 48: the whole per-database budget, whatever the fleet size
		replicas = 4
	)

	// Every replica of the fleet shares one registry, which is all a fleet is. SetWorkers(1) keeps the
	// resident crews small - this test asserts on pool arithmetic, and a derived crew per replica would
	// spawn hundreds of goroutines that none of it depends on.
	build := func() *Engine {
		e := NewEngineUnderTest(t)
		e.testConnCap = 0 // assert the real derived sizes, not the test-mode cap
		assert.NoError(e.SetHost(noopHost{}))
		assert.NoError(e.SetWorkers(1))
		assert.NoError(e.SetShard(ShardSpec{Index: shard, VirtualCPUs: vCPUs}))
		return e
	}

	fleet := &restartFleet{}
	originals := make([]*Engine, 0, replicas)
	for range replicas {
		e := build()
		fleet.add(e)
		originals = append(originals, e)
		assert.NoError(e.Startup(ctx))
	}
	awaitFleetSettled(t, fleet, shard, replicas, budget/replicas)

	// Price the fleet continuously from here, so the assertion covers the transitions rather than the
	// settled points either side of them - the settled points are exactly where nothing is at risk.
	// peak is written only by the sampler and read only after its Wait, so it needs no lock of its own.
	peak := 0
	samplerStop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			select {
			case <-samplerStop:
				return
			default:
			}
			peak = max(peak, fleet.ceiling(shard))
			time.Sleep(time.Millisecond)
		}
	})

	// The rollout: one replica at a time leaves, the fleet settles on the smaller size, and a replacement
	// joins. Waiting for the survivors to actually TAKE their larger share is what makes each step the hard
	// case rather than the easy one - a replacement that joins before its peers have grown into the gap
	// never has to fit inside a budget somebody else is holding.
	for i, old := range originals {
		assert.NoError(old.Shutdown(ctx))
		fleet.remove(old)
		awaitFleetSettled(t, fleet, shard, replicas-1, budget/(replicas-1))

		fresh := build()
		fleet.add(fresh)
		assert.NoError(fresh.Startup(ctx), "replacement %d failed to start", i)
		awaitFleetSettled(t, fleet, shard, replicas, budget/replicas)
	}

	close(samplerStop)
	sampler.Wait()
	assert.True(peak <= budget+startupBootstrapConns,
		"the fleet claimed %d connections against a %d budget (+%d bootstrap) at some point in the rollout",
		peak, budget, startupBootstrapConns)
}
