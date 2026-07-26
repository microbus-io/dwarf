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
	"math"
	"testing"
	"time"

	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestPartition_DisabledBeforeTheFleetIsKnown pins the engine's own fail-open case, the one that is its
// rather than the Sonar's: a shard with no Sonar at all - one that failed to build, or any lookup before
// Startup - must not partition. Partitioning EXCLUDES rows, so a wrong pair strands a residue class, while
// declining only restores overlapping selection, which the claim CAS arbitrates.
//
// The pair's other fail-open cases (solo dispatcher, unknown ordinal, out-of-range ordinal, a blind Sonar)
// belong to internal/peers and are pinned there, and internal/piston validates the pair a third time as its
// own advertised posture. Three guards, deliberately: each package answers for what it knows.
func TestPartition_DisabledBeforeTheFleetIsKnown(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngine()

	_, _, ok := e.partitionOn(1)
	assert.False(ok, "no Sonar for the shard: select everything")
	assert.Equal(1, e.replicasOn(1), "and size for a solo replica, which is the pool-safe direction")
}

// The predicate this pair drives now lives in internal/piston (partitionPredicate), which validates it a
// second time - see TestPiston_PartitionPairIsValidated. Both guards are deliberate: this one is the
// engine's knowledge of its own fleet, that one is the package's advertised fail-open posture.

// TestPartition_ResidueClassesCoverEveryStepExactlyOnce is the completeness proof: across a fleet of R,
// every step id is selected by exactly ONE replica. Under-coverage strands work; over-coverage is the
// collision this exists to remove.
func TestPartition_ResidueClassesCoverEveryStepExactlyOnce(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, r := range []int{2, 3, 4, 8} {
		owners := make([]int, 64)
		for ordinal := range r {
			for stepID := range 64 {
				if stepID%r == ordinal {
					owners[stepID]++
				}
			}
		}
		for stepID, n := range owners {
			assert.Equal(1, n, "R=%d step %d owned by %d replicas, want exactly 1", r, stepID, n)
		}
	}
}

// TestPartition_AppliedFromRegistry drives the real path end to end: peer rows in the shared registry ->
// heartbeat read -> (R, ordinal) -> predicate. It is the one test that would catch the halves being read
// from different places.
func TestPartition_AppliedFromRegistry(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))

	// Solo: registered, but nothing to divide.
	_, _, ok := e.partitionOn(1)
	assert.False(ok, "a solo replica must not partition")

	// Two peers join with ids straddling this engine's own, so the ordinal is a real position rather
	// than an artifact of always sorting first or last.
	addPeerRow(t, e, 1)
	addPeerRow(t, e, math.MaxInt64)
	awaitPartition(t, e, 1, 3, 1) // own id sorts between them, so ordinal 1

	// A peer leaves: the divisor drops and the ordinal re-seats, both off the same reading.
	delPeerRow(t, e, 1)
	awaitPartition(t, e, 1, 2, 0) // with the lower id gone, this engine is now first

	// Back to solo - partitioning switches off rather than leaving a stale residue class.
	delPeerRow(t, e, math.MaxInt64)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, ok := e.partitionOn(1); !ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("returning to solo must disable partitioning")
}

// TestPartition_AwaitOnlyPeerOwnsNoSlice is the regression test for the flaw that shipped in the first
// cut of this feature: an await-only replica (SetWorkers(0)) registers in dwarf_peers - it holds
// connections, so it must count toward R - but claims nothing. Partitioning on R therefore handed it a
// residue class of step_id that NO replica would ever select, stranding those steps until an operator
// noticed. Caught by fixtures/crossreplicaawait_test.go hanging; pinned here at the unit level.
func TestPartition_AwaitOnlyPeerOwnsNoSlice(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))

	// A peer that holds connections but dispatches nothing.
	addPeerRowWithDispatch(t, e, 4242, false)
	assert.Equal(2, e.replicasOn(1), "an await-only peer still divides the connection pools")
	_, _, ok := e.partitionOn(1)
	assert.False(ok, "one dispatcher means nothing to partition, whatever the pool divisor says")

	// A second DISPATCHING peer does open partitioning - proving the exclusion is about dispatch, not
	// about peer count.
	addPeerRowWithDispatch(t, e, 4243, true)
	assert.Equal(3, e.replicasOn(1), "the pool divisor counts all three")
	awaitPartition(t, e, 1, 2, -1) // only the two dispatchers divide the candidates
}

// addPeerRowWithDispatch inserts a fake peer that either has or has not demonstrably dispatched, then
// forces a recount. The dispatching variant is what addPeerRow already does; this spells it out so the
// await-only case is expressible.
func addPeerRowWithDispatch(t *testing.T, e *Engine, id int64, dispatches bool) {
	t.Helper()
	before := peerCounts(e)
	err := e.db.OnEach(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		// A non-dispatching peer leaves dispatched_at at its far-past default, which is exactly how an
		// await-only replica presents itself: alive in seen_at, never advancing the evidence column. There
		// is no flag to set - the timestamp IS the signal.
		cols, vals := "engine_id, seen_at", "?, NOW_UTC()"
		if dispatches {
			cols, vals = cols+", dispatched_at", vals+", NOW_UTC()"
		}
		_, err := db.ExecContext(ctx, "INSERT INTO dwarf_peers ("+cols+") VALUES ("+vals+")", id)
		return err
	})
	if err != nil {
		t.Fatalf("insert peer %d: %v", id, err)
	}
	awaitPeerChange(t, e, before)
}

// The intra-peer claim reservation lives in internal/claimstracker with its own tests (the roll window,
// relinquish, shard-keying, and concurrency under -race). The engine-level guard that its window can never
// break single-replica lease recovery - a worker whose step's lease expires mid-execution must still be
// re-claimable by a sibling - is the integration test TestLeaseFence_CompletionNoDuplicateSuccessor.

// awaitPartition waits until one shard reports the expected (dispatchers, ordinal) pair.
//
// A wait rather than a read, because this replica's own place among the dispatchers is EARNED: the
// registry's dispatch column is stamped only once a cycle has actually turned - evidence, not intent - so
// asserting straight after a registry write would race that cycle. The wait is milliseconds.
// A negative wantOrdinal means "any": this engine's id is random, so where it sorts among the dispatchers
// is only fixed when the test plants peer ids that straddle it.
func awaitPartition(t *testing.T, e *Engine, shard, wantDispatchers, wantOrdinal int) {
	t.Helper()
	assert := testarossa.For(t)
	deadline := time.Now().Add(10 * time.Second)
	var d, o int
	var ok bool
	for time.Now().Before(deadline) {
		if d, o, ok = e.partitionOn(shard); ok && d == wantDispatchers && (wantOrdinal < 0 || o == wantOrdinal) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	assert.True(false, "shard %d settled at (%d,%d,%v), want (%d,%d,true)",
		shard, d, o, ok, wantDispatchers, wantOrdinal)
}
