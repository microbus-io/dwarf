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
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestPoolSizing_ShardPool pins the per-shard pool derivation: the explicit override pins the pool
// exactly; VirtualCPUs derives the open ceiling at the measured knee (~6x CPUs) with a warm idle core;
// unknown CPUs fall back to the measured-safe default (8) rather than a guessed CPU count - the
// over-connection collapse measured on small tiers is why honest ignorance beats a wrong guess. The
// derived budgets are per database, so the observed replica count splits them; the override is a
// per-replica exact number and is never divided.
func TestPoolSizing_ShardPool(t *testing.T) {
	assert := testarossa.For(t)

	cases := []struct {
		vcpus    int
		override int
		replicas int
		idle     int
		open     int
	}{
		{0, 0, 1, 8, 8},    // unknown CPUs, no override: safe default
		{1, 0, 1, 3, 6},    // 1 vCPU: knee at 6
		{2, 0, 1, 6, 12},   // 2 vCPU: knee at 12
		{8, 0, 1, 24, 48},  // 8 vCPU: knee at 48
		{8, 30, 1, 30, 30}, // override wins over derived, pinned exactly
		{0, 5, 1, 5, 5},    // override with unknown CPUs
		{8, 0, 2, 12, 24},  // replicas split the derived budget: each takes its 1/R share of the knee
		{8, 0, 3, 8, 16},
		{1, 0, 4, 2, 2},    // floor: even many replicas keep a usable minimum pool
		{0, 0, 2, 4, 4},    // the unknown-CPU default splits too
		{8, 30, 4, 30, 30}, // the override is per replica and is never divided
	}
	for _, c := range cases {
		idle, open := shardPool(ShardSpec{VirtualCPUs: c.vcpus}, c.override, c.replicas)
		assert.Equal(c.idle, idle, "idle for %+v", c)
		assert.Equal(c.open, open, "open for %+v", c)
	}
}

// TestPoolSizing_ObservedReplicasLive pins the observed-R path end to end through the real signal
// seam: a peer's hello shrinks the derived pool to the 1/R share immediately (and is answered with a
// ping); a second peer shrinks it further; a goodbye regrows it; and the SetMaxOpenConns override,
// once set, is never divided by fleet changes.
func TestPoolSizing_ObservedReplicasLive(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngine()
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	e.RunInTest(t)

	db, err := e.db.Shard(1)
	assert.NoError(err)
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "full budget while alone (R=1)")
	assert.Equal(1, e.observedReplicas())

	hello := func(id string) {
		b, _ := json.Marshal(peerPayload{Origin: id})
		assert.NoError(e.DeliverSignal(ctx, string(signalOpHello), b))
	}
	goodbye := func(id string) {
		b, _ := json.Marshal(peerPayload{Origin: id})
		assert.NoError(e.DeliverSignal(ctx, string(signalOpGoodbye), b))
	}

	hello("peer-a")
	assert.Equal(2, e.observedReplicas())
	assert.Equal(24, db.DB.Stats().MaxOpenConnections, "1/2 share on the peer's hello")

	hello("peer-b")
	assert.Equal(3, e.observedReplicas())
	assert.Equal(16, db.DB.Stats().MaxOpenConnections, "1/3 share at three replicas")

	hello("peer-b") // duplicate hello: no fleet change, no recompute
	assert.Equal(16, db.DB.Stats().MaxOpenConnections)

	goodbye("peer-a")
	goodbye("peer-b")
	assert.Equal(1, e.observedReplicas())
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "graceful goodbyes restore the full budget")

	// The pinned override wins over any fleet change.
	assert.NoError(e.SetMaxOpenConns(11))
	hello("peer-c")
	assert.Equal(11, db.DB.Stats().MaxOpenConnections, "the pinned override is never divided")

	// The engine's own echoed hello must not create a phantom peer.
	before := e.observedReplicas()
	b, _ := json.Marshal(peerPayload{Origin: e.instanceID})
	assert.NoError(e.DeliverSignal(ctx, string(signalOpHello), b))
	assert.Equal(before, e.observedReplicas())
}

// TestPoolSizing_PeerExpiry pins the crashed-peer path: a peer that stops pinging (its goodbye never
// comes) is pruned by the heartbeat loop after ~3 ping intervals, and the derived pool regrows.
func TestPoolSizing_PeerExpiry(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngine()
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	e.pingInterval = 50 * time.Millisecond // 3x expiry = 150ms
	e.RunInTest(t)

	db, err := e.db.Shard(1)
	assert.NoError(err)

	b, _ := json.Marshal(peerPayload{Origin: "peer-crash"})
	assert.NoError(e.DeliverSignal(ctx, string(signalOpHello), b))
	assert.Equal(24, db.DB.Stats().MaxOpenConnections, "1/2 share while the peer lives")

	// The peer never pings again (crashed). The loop must prune it and regrow the pool.
	deadline := time.Now().Add(5 * time.Second)
	for e.observedReplicas() != 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(1, e.observedReplicas(), "crashed peer pruned after expiry")
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "full budget restored")
}

// TestPoolSizing_PoolGrowsForLongTasks pins the grow-on-demand pool: with every worker parked in a
// slow ExecuteTask and more work waiting, the pool spawns beyond its resident set (up to the ceiling),
// so long tasks are not capped by the connection-derived dispatch count. This is the whole point of
// deriving the worker maximum from the lease margin rather than from the connection budget.
func TestPoolSizing_PoolGrowsForLongTasks(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	release := make(chan struct{})
	var running atomic.Int32
	g := workflow.NewGraph("Grow")
	g.SetEndpoint("Slow", "grow/slow")
	g.AddTransition("Slow", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("grow/g", g)
	proxy.HandleTask("grow/slow", func(ctx context.Context, f *workflow.Flow) error {
		running.Add(1)
		<-release // park: hold the worker exactly as a minutes-long LLM call would
		return nil
	})

	e := NewEngine()
	assert.NoError(e.SetHost(proxy))
	e.RunInTest(t)
	resident := e.workersResident.Load()
	assert.Equal(int32(64), resident, "resident set is the connection-derived dispatch count")

	// Start more flows than the resident set can hold concurrently.
	const flows = 80
	for range flows {
		_, err := e.Create(ctx, "grow/g", nil, nil)
		assert.NoError(err)
	}
	// The pool must grow past its resident set to run them all at once.
	deadline := time.Now().Add(10 * time.Second)
	for running.Load() < flows && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(int32(flows), running.Load(), "every parked task got a worker")
	assert.True(e.workersResident.Load() > resident, "the pool grew beyond its resident set (got %d, was %d)",
		e.workersResident.Load(), resident)
	assert.True(int(e.workersResident.Load()) <= int(e.workers.Load()), "growth stays under the ceiling")
	close(release)
}

// TestPoolSizing_CapacityWeight pins the placement-weight curve: flat up to 2 vCPUs (the measured
// 1- and 2-vCPU tiers ceiling at the same ~745 steps/s), then ~450 steps/s per vCPU.
func TestPoolSizing_CapacityWeight(t *testing.T) {
	assert := testarossa.For(t)

	assert.Equal(0, capacityWeight(0)) // unknown: resolved by pickShard
	assert.Equal(745, capacityWeight(1))
	assert.Equal(745, capacityWeight(2))
	assert.Equal(1800, capacityWeight(4))
	assert.Equal(3600, capacityWeight(8))
}

// TestPoolSizing_PickShard pins the weighted placement: cordoned shards are never picked, weights
// follow the capacity curve (an 8-vCPU shard drawing ~4.8x a 1-vCPU shard's flows), and a shard with
// unknown CPUs gets the smallest known weight.
func TestPoolSizing_PickShard(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, VirtualCPUs: 8}))
	assert.NoError(e.SetShard(ShardSpec{Index: 3, VirtualCPUs: 4, Cordoned: true}))
	assert.NoError(e.SetShard(ShardSpec{Index: 4})) // unknown CPUs -> smallest known weight (745)

	counts := map[int]int{}
	for range 10000 {
		idx, err := e.pickShard()
		assert.NoError(err)
		counts[idx]++
	}
	assert.Equal(0, counts[3], "cordoned shard must never be picked")
	// Expected proportions: 745 : 3600 : 745 (total 5090). Allow generous slack for randomness.
	assert.True(counts[2] > counts[1]*3, "8-vCPU shard should draw ~4.8x the 1-vCPU shard (got %d vs %d)", counts[2], counts[1])
	assert.True(counts[1] > 800 && counts[4] > 800, "low-weight shards still receive flows (got %d, %d)", counts[1], counts[4])
}

// TestPoolSizing_AllCordoned pins the loud failure: when every shard is cordoned there is nowhere to
// place a new flow, and pickShard errors rather than silently violating the cordon.
func TestPoolSizing_AllCordoned(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetShard(ShardSpec{Index: 1, Cordoned: true}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, Cordoned: true}))
	_, err := e.pickShard()
	assert.Error(err)
}

// TestPoolSizing_LiveOverride pins that SetMaxOpenConns pushes the pinned pool to every live shard
// immediately (the expert/benchmark path).
func TestPoolSizing_LiveOverride(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetHost(noopHost{}))
	for i := 1; i <= 2; i++ {
		assert.NoError(e.SetShard(ShardSpec{Index: i, VirtualCPUs: 1}))
	}
	e.RunInTest(t)

	// Derived from VirtualCPUs=1: open = 6.
	for i := 1; i <= 2; i++ {
		db, err := e.db.Shard(i)
		assert.NoError(err)
		assert.Equal(6, db.DB.Stats().MaxOpenConnections, "shard %d derived pool", i)
	}

	// Live override pins exactly.
	assert.NoError(e.SetMaxOpenConns(11))
	for i := 1; i <= 2; i++ {
		db, _ := e.db.Shard(i)
		assert.Equal(11, db.DB.Stats().MaxOpenConnections, "shard %d after override", i)
	}

	assert.Error(e.SetMaxOpenConns(0)) // must be >= 1
}

// TestPoolSizing_WorkerCeiling pins the lease-margin ceiling: N_max = M x margin / txTime x safety,
// with txTime = 7 x RTT + 3ms. Every input is engine-visible (the pool, the engine's own 30s margin,
// and the RTT probed at Startup) - no task duration appears, which is the property that makes it a
// derivable default rather than a guess.
func TestPoolSizing_WorkerCeiling(t *testing.T) {
	assert := testarossa.For(t)

	// Same-zone (RTT 0.3ms): txTime = 7*0.3 + 3 = 5.1ms. M=48 -> 48 * 30000/5.1 * 0.25 = 70,588.
	assert.Equal(70588, workerCeiling(48, 0.3))
	// The small-tier pool scales the ceiling down proportionally: M=6 -> 8,823.
	assert.Equal(8823, workerCeiling(6, 0.3))
	// Cross-zone (RTT 1.1ms): txTime = 10.7ms - roughly halves the ceiling at the same pool.
	assert.Equal(33644, workerCeiling(48, 1.1))
	// A high-latency link is dominated by the round trips: RTT 5ms -> txTime 38ms.
	assert.Equal(9473, workerCeiling(48, 5))
	// Degenerate inputs never yield a zero-worker pool.
	assert.True(workerCeiling(1, 0) >= 1)
}

// TestPoolSizing_DerivedWorkers pins the derived worker default (the lease-margin ceiling, so it holds
// for any task duration) and the resident/dispatch split: the eagerly-spawned set and the candidate
// cache stay sized by the connection budget (8x conns, floor 64), because a worker parked in a long
// ExecuteTask holds no connection and must not inflate the refill scan.
func TestPoolSizing_DerivedWorkers(t *testing.T) {
	assert := testarossa.For(t)

	// Zero-config: one default shard (pool 8). The max is the ceiling (large - it is set by the 30s
	// margin, not by the task); the resident set and cache stay at the historical 64.
	e := NewEngine()
	assert.NoError(e.SetHost(noopHost{}))
	e.RunInTest(t)
	assert.Equal(64, e.workersDispatch, "resident/dispatch set stays connection-derived")
	assert.Equal(128, e.cache.Capacity(), "cache is 2x the resident set, never 2x the ceiling")
	assert.True(int(e.workers.Load()) > 1000, "the worker max is the lease-margin ceiling (got %d)", e.workers.Load())
	assert.Equal(int32(64), e.workersResident.Load(), "only the resident set is spawned eagerly")

	// An 8-vCPU shard (pool 48) + a 2-vCPU shard (pool 12): dispatch = max(64, 8*60) = 480, and the
	// ceiling is keyed on the WORST shard (the 2-vCPU pool of 12), never the aggregate.
	e2 := NewEngine()
	assert.NoError(e2.SetHost(noopHost{}))
	assert.NoError(e2.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e2.SetShard(ShardSpec{Index: 2, VirtualCPUs: 2}))
	e2.RunInTest(t)
	assert.Equal(480, e2.workersDispatch)
	assert.True(int(e2.workers.Load()) < workerCeiling(48, 0.3), "the smallest pool sets the ceiling")

	// Explicit SetWorkers pins, regardless of shards.
	e3 := NewEngine()
	assert.NoError(e3.SetHost(noopHost{}))
	assert.NoError(e3.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e3.SetWorkers(2))
	e3.RunInTest(t)
	assert.Equal(2, int(e3.workers.Load()))
}
