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
	e := NewEngine()
	if err := e.SetHost(noopHost{}); err != nil {
		t.Fatal(err)
	}
	if err := e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}); err != nil {
		t.Fatal(err)
	}
	e.RunInTest(t)

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
	e := NewEngine()
	if err := e.SetHost(noopHost{}); err != nil {
		t.Fatal(err)
	}
	if err := e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}); err != nil {
		t.Fatal(err)
	}
	e.RunInTest(t)

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

// TestClaimReservation_ExpiresSoAnEntryCannotStrandAStep pins the safety valve on the intra-peer
// reservation. The reservation suppresses a duplicate claim while a worker runs the step; if an entry
// ever outlived its worker WITHOUT an expiry, this replica would refuse to claim that step for the rest
// of the process's life - converting an optimization into a permanent loss of one step. The expiry makes
// the worst case a delay instead.
func TestClaimReservation_ExpiresSoAnEntryCannotStrandAStep(t *testing.T) {
	t.Parallel()
	e := NewEngine()

	testarossa.True(t, e.beginClaim(1, 42), "first reservation must be granted")
	testarossa.False(t, e.beginClaim(1, 42), "a sibling worker must be turned away while it is held")

	// The shard is part of the key: step 42 exists on every shard, and a step-id-only key would turn
	// away live candidates on every shard but the one holding the reservation.
	testarossa.True(t, e.beginClaim(2, 42), "same step id on another shard is a different step")

	// Simulate an entry that outlived its worker by backdating it past the horizon.
	e.claimsLock.Lock()
	e.claimsInFlight[stepRef{shard: 1, stepID: 42}] = time.Now().Add(-time.Second)
	e.claimsLock.Unlock()
	testarossa.True(t, e.beginClaim(1, 42), "an expired reservation must not keep the step unclaimable")

	// Explicit release is by key, and leaves the other shard's reservation intact.
	e.endClaim(1, 42)
	testarossa.True(t, e.beginClaim(1, 42))
	testarossa.False(t, e.beginClaim(2, 42), "releasing shard 1 must not release shard 2")

	// Idempotent, so a caller need not track whether it already released.
	e.endClaim(1, 42)
	e.endClaim(1, 42)
	testarossa.True(t, e.beginClaim(1, 42))
}

// TestClaimReservation_DoesNotOutlastALease is the regression test for the shape that broke
// TestLeaseFence_CompletionNoDuplicateSuccessor: a worker parked in a long ExecuteTask must not keep its
// reservation for the whole task, or a step whose lease expired mid-execution (an overrun, a DB clock
// step) can never be re-claimed by a sibling worker - single-replica lease recovery stops working.
// The reservation is a TIMEOUT, and it must sit far below leaseMargin.
func TestClaimReservation_DoesNotOutlastALease(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	testarossa.True(t, claimReservationTTL < e.leaseMargin,
		"a reservation outliving the lease margin would block the very re-claim lease recovery exists to perform")

	prev := claimReservationTTL
	claimReservationTTL = 20 * time.Millisecond
	defer func() { claimReservationTTL = prev }()

	testarossa.True(t, e.beginClaim(1, 7))
	testarossa.False(t, e.beginClaim(1, 7), "held while the window stands")
	time.Sleep(40 * time.Millisecond)
	testarossa.True(t, e.beginClaim(1, 7), "a sibling must be able to re-claim once the window lapses")
}

// TestClaimReservation_SweepBoundsLeakedEntries pins that the map cannot grow without bound. Entries are
// released by a defer, so a leak needs a fatal throw - but "cannot happen" is not a memory bound.
func TestClaimReservation_SweepBoundsLeakedEntries(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	e.workers.Store(4)

	// Far more expired entries than live dispatch could account for, as a leak would eventually produce.
	e.claimsLock.Lock()
	for i := range 200 {
		e.claimsInFlight[stepRef{shard: 1, stepID: i}] = time.Now().Add(-time.Minute)
	}
	e.claimsLock.Unlock()

	testarossa.True(t, e.beginClaim(1, 9999))
	e.claimsLock.Lock()
	remaining := len(e.claimsInFlight)
	e.claimsLock.Unlock()
	testarossa.Equal(t, 1, remaining, "the sweep should leave only the live reservation")
}
