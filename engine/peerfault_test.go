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
	"testing"
	"time"

	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// These drive the peer registry's failure modes against a REAL assembled engine. internal/peers pins the
// same policy against its own clock, which is where the arithmetic belongs; what these add is that the
// engine wired to it derives the right things - pool sizes, the partition, and the fact that work keeps
// moving - when the registry stops answering.

// awaitPoolSize waits for one shard's applied pool ceiling, reporting what it settled at.
func awaitPoolSize(t *testing.T, db *sequel.DB, want int) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := db.DB.Stats().MaxOpenConnections; got == want {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	return db.DB.Stats().MaxOpenConnections
}

// TestPeerFault_BlindHoldsThePoolsAndFailsOpen pins the two directions a blind replica must move in, which
// are opposite and both deliberate.
//
// The POOL divisor is held: a reading that did not happen is not an observation that anybody left, and
// collapsing it would GROW every pool derived from it - against a database that is evidently already
// unwell, since the reading is what just failed. The PARTITION switches off: it can no longer be justified,
// and a wrong residue class strands work while declining to partition only overlaps, which the claim CAS
// settles at the cost of a lost round trip.
//
// Both are unreachable without a failing read, which is what the seam exists for.
func TestPeerFault_BlindHoldsThePoolsAndFailsOpen(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	e.testConnCap = 0 // assert the real derived sizes, not the test-mode cap
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8})) // budget 48
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	addPeerRow(t, e, 5001)
	addPeerRow(t, e, 5002)
	assert.Equal(3, e.replicasOn(1))
	assert.Equal(16, awaitPoolSize(t, db, 16), "1/3 share at three replicas")
	awaitPartition(t, e, 1, 3, -1)

	// The registry stops answering on this shard.
	e.seams.InjectN(1<<20, FaultPeerReadErr)

	// The fleet is HELD. Even after the peers' rows are deleted outright, this replica cannot see it, so it
	// must go on sizing for three - the safe direction, and the one a blip must not undo.
	assert.NoError(e.db.OnEach(context.Background(), func(ctx context.Context, sdb *sequel.DB, shard int) error {
		_, derr := sdb.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id IN (5001, 5002)")
		return derr
	}))
	time.Sleep(10 * testPeerCadence)
	assert.Equal(3, e.replicasOn(1), "a failed reading publishes nothing")
	assert.Equal(16, db.DB.Stats().MaxOpenConnections, "so the pools stay where the last good reading put them")

	// The partition, by contrast, is off: the pair is stale and nothing justifies excluding rows on it.
	_, _, ok := e.partitionOn(1)
	assert.False(ok, "a blind replica selects everything rather than trusting a stale residue class")

	// Reading resumes. The first reading back must not shrink the fleet - this is the correlated stall,
	// where a database hiccup makes every row look stale at once and every replica would otherwise size for
	// a fleet of one simultaneously. It settles on the reading after that.
	e.seams.Withdraw(FaultPeerReadErr)
	assert.Equal(48, awaitPoolSize(t, db, 48), "the fleet really did shrink, so the budget comes back")
	assert.Equal(1, e.replicasOn(1))
}

// TestPeerFault_BlindnessIsPerShard pins the property the whole per-shard design exists to provide: one
// shard's registry going dark must not disturb any other shard's accounting. A fleet-global count could not
// express it, and without a shard-scoped seam it could not be tested.
func TestPeerFault_BlindnessIsPerShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	e.testConnCap = 0
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))

	db1, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	db2, err := e.db.Shard(2)
	if !assert.NoError(err) {
		return
	}
	addPeerRow(t, e, 6001) // lands on every shard
	assert.Equal(24, awaitPoolSize(t, db1, 24))
	assert.Equal(24, awaitPoolSize(t, db2, 24))

	// Blind shard 1 only, then take the peer away everywhere.
	e.seams.InjectN(1<<20, FaultPeerReadErr, "1")
	assert.NoError(e.db.OnEach(context.Background(), func(ctx context.Context, sdb *sequel.DB, shard int) error {
		_, derr := sdb.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=6001")
		return derr
	}))

	// Shard 2 sees it and regrows; shard 1 cannot, and holds. The two shards' pools are now legitimately
	// different, derived from different readings of different tables.
	assert.Equal(48, awaitPoolSize(t, db2, 48), "the shard that can read follows the fleet")
	assert.Equal(24, db1.DB.Stats().MaxOpenConnections, "the blind shard holds its last good reading")
	assert.Equal(2, e.replicasOn(1))
	assert.Equal(1, e.replicasOn(2))
}

// TestPeerFault_BeatFailureEvictsFromTheDispatchers pins the view from the OTHER side: a replica that is
// running and reading perfectly well, but can no longer refresh its own row, must drop out of its peers'
// divisors. That is the crashed-replica case as a peer experiences it - nobody sends a goodbye - and the
// beat seam is the only way to occupy that view without killing a process.
//
// It drops out of the WORK divisor first (five beats) and out of the POOL divisor much later (forty), and
// the ordering is the whole design: a residue class owned by a replica that stopped serving is work nobody
// runs, while a pool divisor that drops a live replica over-sizes every pool.
func TestPeerFault_BeatFailureEvictsFromTheDispatchers(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	// Two engines sharing one test database - which is all a fleet is.
	worker := NewEngineUnderTest(t)
	assert.NoError(worker.SetHost(noopHost{}))
	assert.NoError(worker.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(worker.Startup(t.Context()))

	peer := NewEngineUnderTest(t)
	assert.NoError(peer.SetHost(noopHost{}))
	assert.NoError(peer.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(peer.Startup(ctx))
	t.Cleanup(func() { peer.Shutdown(ctx) })

	// Both dispatching: two divisors, and each holds an ordinal in the other's view.
	awaitPartition(t, worker, 1, 2, -1)
	assert.Equal(2, worker.replicasOn(1))

	// The peer loses the ability to prove it is alive. It keeps running - it just goes silent in the
	// registry - so nothing about it changes except what its own row says.
	peer.seams.InjectN(1<<20, FaultPeerBeatErr)

	// The worker drops it from the WORK divisor once its dispatch evidence ages out, and stops partitioning:
	// with one dispatcher left there is nothing to divide, so the survivor selects everything. That is the
	// takeover - no rebalancing, just a divisor that shrank.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, ok := worker.partitionOn(1); !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, _, ok := worker.partitionOn(1)
	assert.False(ok, "a peer that stopped proving it serves must not keep a residue class")

	// And it is STILL counted for the connections it holds, because that window is far more generous - the
	// two errors point in opposite directions, so the two windows must not converge.
	assert.Equal(2, worker.replicasOn(1),
		"dropping it from the pool divisor this early would over-size every pool in the fleet")
}
