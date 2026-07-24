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

	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestPartition_OrdinalFromSortedRoster pins the assignment rule the whole scheme rests on: every
// replica sorts the SAME registry rows the same way, so each derives a DISTINCT ordinal with no
// coordination. A rule that did not agree across replicas would seat two on one residue class (back to
// colliding) or leave one owned by nobody (stranded work).
func TestPartition_OrdinalFromSortedRoster(t *testing.T) {
	t.Parallel()
	roster := []int64{11, 22, 33, 44}
	for want, id := range roster {
		testarossa.Equal(t, want, rosterOrdinal(roster, id), "engine %d should be ordinal %d", id, want)
	}
	// Absent from the roster is -1, NOT 0: 0 is a legitimate ordinal, so collapsing the two would make an
	// unregistered replica silently claim the first residue class.
	testarossa.Equal(t, -1, rosterOrdinal(roster, 99))
	testarossa.Equal(t, -1, rosterOrdinal(nil, 11))
}

// TestPartition_DisabledUnlessSafe pins the fail-open direction. Partitioning EXCLUDES rows, so a wrong
// (R, ordinal) strands a residue class; declining to partition only restores the pre-partition
// overlapping selection, which is slower but complete. Every uncertain case must therefore fail open.
func TestPartition_DisabledUnlessSafe(t *testing.T) {
	t.Parallel()
	e := NewEngine()

	// Solo replica: nothing to divide.
	e.observedDispatchers.Store(1)
	e.observedOrdinal.Store(0)
	_, _, ok := e.observedPartition()
	testarossa.False(t, ok, "R=1 must not partition")
	sql, args := e.partitionPredicate()
	testarossa.Equal(t, "", sql, "R=1 must add no predicate")
	testarossa.Len(t, args, 0)

	// Unknown ordinal (self absent from the roster) - the case that would otherwise guess.
	e.observedDispatchers.Store(4)
	e.observedOrdinal.Store(-1)
	_, _, ok = e.observedPartition()
	testarossa.False(t, ok, "unknown ordinal must not partition")

	// Ordinal out of range for R - a stale-high ordinal against a freshly-lowered R.
	e.observedDispatchers.Store(2)
	e.observedOrdinal.Store(3)
	_, _, ok = e.observedPartition()
	testarossa.False(t, ok, "out-of-range ordinal must not partition")

	// The one case that DOES partition.
	e.observedDispatchers.Store(4)
	e.observedOrdinal.Store(2)
	replicas, ordinal, ok := e.observedPartition()
	testarossa.True(t, ok)
	testarossa.Equal(t, 4, replicas)
	testarossa.Equal(t, 2, ordinal)
	sql, args = e.partitionPredicate()
	testarossa.Equal(t, " AND step_id % ? = ?", sql)
	testarossa.Equal(t, []any{4, 2}, args)
}

// TestPartition_ResidueClassesCoverEveryStepExactlyOnce is the completeness proof: across a fleet of R,
// every step id is selected by exactly ONE replica. Under-coverage strands work; over-coverage is the
// collision this exists to remove.
func TestPartition_ResidueClassesCoverEveryStepExactlyOnce(t *testing.T) {
	t.Parallel()
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
			testarossa.Equal(t, 1, n, "R=%d step %d owned by %d replicas, want exactly 1", r, stepID, n)
		}
	}
}

// TestPartition_FreshestRosterWins pins the shard pick. R is a roster's LENGTH and the ordinal a
// position within it, so both must come from ONE shard's view - assembling them from two could seat two
// replicas on one ordinal.
func TestPartition_FreshestRosterWins(t *testing.T) {
	t.Parallel()
	// The freshest shard wins even though it is not the longest: a longer-but-staler roster is a view of
	// the fleet as it was, and its extra entry is exactly the dead peer that has not aged out yet.
	got := freshestRoster([]shardRoster{
		{ids: []int64{1, 2, 3}, dispatchers: []int64{1, 2, 3}, freshest: 9000},
		{ids: []int64{1, 2}, dispatchers: []int64{1, 2}, freshest: 100},
	})
	testarossa.Equal(t, []int64{1, 2}, got.ids)

	// A shard that reported no peers is skipped rather than winning with a MaxInt64 age.
	got = freshestRoster([]shardRoster{
		{freshest: math.MaxFloat64},
		{ids: []int64{7}, dispatchers: []int64{7}, freshest: 5000},
	})
	testarossa.Equal(t, []int64{7}, got.ids)

	// Every shard silent - "unknown", which applyReplicaCount treats as "keep the last good pair" rather
	// than collapsing R to 1 and re-expanding the pools on a blip.
	testarossa.Len(t, freshestRoster([]shardRoster{{freshest: math.MaxFloat64}}).ids, 0)
	testarossa.Len(t, freshestRoster(nil).ids, 0)
}

// TestPartition_AppliedFromRegistry drives the real path end to end: peer rows in the shared registry ->
// heartbeat read -> (R, ordinal) -> predicate. It is the one test that would catch the halves being read
// from different places.
func TestPartition_AppliedFromRegistry(t *testing.T) {
	t.Parallel()
	e := NewEngineUnderTest(t)
	if err := e.SetHost(noopHost{}); err != nil {
		t.Fatal(err)
	}
	if err := e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}); err != nil {
		t.Fatal(err)
	}
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Solo: registered, but nothing to divide.
	_, _, ok := e.observedPartition()
	testarossa.False(t, ok, "a solo replica must not partition")

	// Two peers join with ids straddling this engine's own, so the ordinal is a real position rather
	// than an artifact of always sorting first or last.
	addPeerRow(t, e, 1)
	addPeerRow(t, e, math.MaxInt64)
	replicas, ordinal, ok := e.observedPartition()
	testarossa.True(t, ok, "R=3 must partition")
	testarossa.Equal(t, 3, replicas)
	testarossa.Equal(t, 1, ordinal, "own id sorts between 1 and MaxInt64, so ordinal 1")

	// A peer leaves: R drops and the ordinal re-seats, both off the same re-read roster.
	delPeerRow(t, e, 1)
	replicas, ordinal, ok = e.observedPartition()
	testarossa.True(t, ok)
	testarossa.Equal(t, 2, replicas)
	testarossa.Equal(t, 0, ordinal, "with the lower id gone, this engine is now first")

	// Back to solo - partitioning switches off rather than leaving a stale residue class.
	delPeerRow(t, e, math.MaxInt64)
	_, _, ok = e.observedPartition()
	testarossa.False(t, ok, "returning to solo must disable partitioning")
}

// TestPartition_AwaitOnlyPeerOwnsNoSlice is the regression test for the flaw that shipped in the first
// cut of this feature: an await-only replica (SetWorkers(0)) registers in dwarf_peers - it holds
// connections, so it must count toward R - but claims nothing. Partitioning on R therefore handed it a
// residue class of step_id that NO replica would ever select, stranding those steps until an operator
// noticed. Caught by fixtures/crossreplicaawait_test.go hanging; pinned here at the unit level.
func TestPartition_AwaitOnlyPeerOwnsNoSlice(t *testing.T) {
	t.Parallel()
	e := NewEngineUnderTest(t)
	if err := e.SetHost(noopHost{}); err != nil {
		t.Fatal(err)
	}
	if err := e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}); err != nil {
		t.Fatal(err)
	}
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// A peer that holds connections but dispatches nothing.
	addPeerRowWithDispatch(t, e, 4242, false)
	testarossa.Equal(t, 2, e.observedReplicas(), "an await-only peer still divides the connection pools")
	_, _, ok := e.observedPartition()
	testarossa.False(t, ok, "one dispatcher means nothing to partition, whatever R says")

	// A second DISPATCHING peer does open partitioning - proving the exclusion is about dispatch, not
	// about peer count.
	addPeerRowWithDispatch(t, e, 4243, true)
	testarossa.Equal(t, 3, e.observedReplicas(), "R counts all three")
	dispatchers, _, ok := e.observedPartition()
	testarossa.True(t, ok)
	testarossa.Equal(t, 2, dispatchers, "only the two dispatchers divide the candidates")
}

// addPeerRowWithDispatch inserts a fake peer that either does or does not claim work, then forces a
// recount. The dispatching variant is what addPeerRow already does; this spells the flag out so the
// await-only case is expressible.
func addPeerRowWithDispatch(t *testing.T, e *Engine, id int64, dispatches bool) {
	t.Helper()
	ctx := context.Background()
	flag := 0
	if dispatches {
		flag = 1
	}
	err := e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		_, err := db.ExecContext(ctx,
			"INSERT INTO dwarf_peers (engine_id, seen_at, dispatches) VALUES (?, NOW_UTC(), ?)", id, flag)
		return err
	})
	if err != nil {
		t.Fatalf("insert peer %d: %v", id, err)
	}
	e.refreshReplicaCount(ctx)
}

// The intra-peer claim reservation lives in internal/claimstracker with its own tests (the roll window,
// relinquish, shard-keying, and concurrency under -race). The engine-level guard that its window can never
// break single-replica lease recovery - a worker whose step's lease expires mid-execution must still be
// re-claimable by a sibling - is the integration test TestLeaseFence_CompletionNoDuplicateSuccessor.
