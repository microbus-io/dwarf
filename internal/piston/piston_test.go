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

package piston

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/pipeline"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// rig is one piston over its own isolated, migrated database.
type rig struct {
	p       *Piston
	sh      *database.Shard
	db      *sequel.DB
	planner *planner.Planner
	cache   *candidates.Cache
}

// newRig stands up an isolated database for the test, migrates it, and wires a piston over it with
// pacing off so a cycle can be driven synchronously. internal/database is a test-only dependency here -
// the piston itself never opens or closes a handle.
func newRig(t *testing.T) *rig {
	t.Helper()
	var set database.ShardSet
	err := set.Open(context.Background(), database.Config{
		Shards:      map[int]database.ShardConfig{1: {MaxIdleConns: 2, MaxOpenConns: 4}},
		TestID:      database.TestID(t.Name()),
		TestConnCap: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(set.Close)
	sh, err := set.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	cache := &candidates.Cache{}
	cache.Init(4) // capacity 8
	t.Cleanup(cache.Close)
	pl := planner.New()
	p, err := New(sh, pl, cache)
	if err != nil {
		t.Fatal(err)
	}
	p.SetTallyCadence(0, 0)
	p.SetSupplyCadence(0, 0)
	p.SetMaxPriority(40) // the highest band the tests use; every band costs the counts statement a branch
	return &rig{p: p, sh: sh, db: db, planner: pl, cache: cache}
}

// insertStep adds one due pending step and returns its id. Steps are inserted oldest-first by call
// order, which is what the fetch's created_at ordering keys on.
func (r *rig) insertStep(t *testing.T, flowID, priority int, key string, weight float64) int {
	t.Helper()
	_, err := r.db.ExecContext(context.Background(),
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status,"+
			" time_budget_ms, priority, fairness_key, fairness_weight, not_before, lease_expires, created_at, flow_created_hour)"+
			" VALUES (?, 0, 'tok0123456789ab', 'T', 'u', '"+workflow.StatusPending+"', 60000, ?, ?, ?,"+
			" NOW_UTC(), NOW_UTC(), NOW_UTC(), 0)",
		flowID, priority, key, weight)
	if err != nil {
		t.Fatal(err)
	}
	var id int
	err = r.db.QueryRowContext(context.Background(),
		"SELECT MAX(step_id) FROM dwarf_steps").Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// fetch runs FetchSteps for keys at one band and returns the result keyed by key, the view most tests assert on.
func (r *rig) fetch(ctx context.Context, band int, keys []string, perKey int) (map[string][]int, error) {
	got, err := r.p.FetchSteps(ctx, 1, []planner.Fetch{{Band: band, Keys: keys, PerKeyCap: perKey}})
	if got == nil {
		return nil, err
	}
	byKey := map[string][]int{}
	for slot, ids := range got {
		byKey[slot.Key] = ids
	}
	return byKey, err
}

// park makes a step invisible to selection, the way the engine does for a step awaiting a subgraph.
func (r *rig) park(t *testing.T, stepID int) {
	t.Helper()
	_, err := r.db.ExecContext(context.Background(),
		"UPDATE dwarf_steps SET parked=1 WHERE step_id=?", stepID)
	if err != nil {
		t.Fatal(err)
	}
}

// idsByResidue splits every pending step id into the ones this ordinal owns and the ones it does not,
// oldest-first within each - the order FetchSteps returns them in.
func (r *rig) idsByResidue(t *testing.T, replicas, ordinal int) (own, foreign []int) {
	t.Helper()
	rows, err := r.db.QueryContext(context.Background(),
		"SELECT step_id FROM dwarf_steps ORDER BY created_at, step_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		if id%replicas == ordinal {
			own = append(own, id)
		} else {
			foreign = append(foreign, id)
		}
	}
	return own, foreign
}

func drain(c *candidates.Cache) []int {
	out := make([]int, 0, c.Len())
	for c.Len() > 0 {
		j, ok := c.Pop()
		if !ok {
			break
		}
		out = append(out, j.StepID)
	}
	return out
}

// TestPiston_ScanBandTalliesBandsUntilCapacity pins phase one against real SQL: one aggregate row per fairness
// key per band, carrying the count, the OLDEST step's age and the LOWEST weight, best band first - and the tally stops
// at the band where the counts reach the cache capacity, since no fill reaches past it.
func TestPiston_ScanBandTalliesBandsUntilCapacity(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	r.insertStep(t, 1, 5, "alpha", 3)
	r.insertStep(t, 2, 5, "alpha", 2)
	r.insertStep(t, 3, 5, "beta", 1)
	r.insertStep(t, 4, 9, "gamma", 1)
	r.insertStep(t, 5, 12, "alpha", 1) // the same key at a third band

	band, tallies, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	assert.Equal(5, band, "the best due band")
	assert.Equal(4, len(tallies), "one row per key per band, never one per step")

	bySlot := map[planner.Slot]planner.Tally{}
	for _, tl := range tallies {
		bySlot[planner.Slot{Band: tl.Band, Key: tl.Key}] = tl
	}
	assert.Equal(2, bySlot[planner.Slot{Band: 5, Key: "alpha"}].Count)
	assert.Equal(2.0, bySlot[planner.Slot{Band: 5, Key: "alpha"}].Weight, "the lowest weight among the key's due steps")
	assert.Equal(1, bySlot[planner.Slot{Band: 5, Key: "beta"}].Count)
	assert.Equal(1, bySlot[planner.Slot{Band: 9, Key: "gamma"}].Count, "a worse band is tallied while capacity remains")
	assert.Equal(1, bySlot[planner.Slot{Band: 12, Key: "alpha"}].Count)
	assert.Equal(5, tallies[0].Band, "best band first")

	// Five more at band 5 make eight - the capacity - so the tally stops there.
	for i := range 5 {
		r.insertStep(t, 10+i, 5, "beta", 1)
	}
	_, tallies, err = r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	for _, tl := range tallies {
		assert.Equal(5, tl.Band, "a band past the one that fills the cache is never scanned")
	}
}

// TestPiston_BetterBandsThatFillTheCacheHoldTheRestBack pins strict priority over capacity end to end against
// real SQL: four bands due, the best two deep enough to fill the cache. One tally and one push must put only
// those two in the cache, best first, never scan the other two at all, and leave the doorbell declining them.
func TestPiston_BetterBandsThatFillTheCacheHoldTheRestBack(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t) // capacity 8

	flow := 0
	insert := func(band int, key string, n int) {
		for range n {
			flow++
			r.insertStep(t, flow, band, key, 1)
		}
	}
	insert(10, "a", 2)
	insert(10, "b", 1)
	insert(20, "c", 3)
	insert(20, "d", 3) // bands 10 and 20 hold 9 due steps against a capacity of 8
	insert(30, "e", 4)
	insert(40, "f", 4)

	_, tallies, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	for _, tl := range tallies {
		assert.True(tl.Band <= 20, "the tally stops at the band that fills the cache, but tallied band %d", tl.Band)
	}

	tr, sr := r.p.Cycle(ctx)
	assert.NoError(tr.Err)
	assert.NoError(sr.Err)
	assert.Equal(8, sr.Selected, "the cache is filled")

	var bands []int
	for r.cache.Len() > 0 {
		j, ok := r.cache.Pop()
		if !ok {
			break
		}
		bands = append(bands, j.Priority)
		if len(bands) == 1 {
			// A slot is free now, so only the limit decides: the reach admits, a held-back band does not.
			assert.False(r.cache.Offer(candidates.Job{StepID: 9001, Shard: 1}, 30), "band 30 is past the reach")
			assert.False(r.cache.Offer(candidates.Job{StepID: 9002, Shard: 1}, 40), "band 40 is past the reach")
			assert.True(r.cache.Offer(candidates.Job{StepID: 9003, Shard: 1}, 20), "band 20 is the reach")
		}
	}
	assert.Equal([]int{10, 10, 10, 20, 20, 20, 20, 20, 20}, bands,
		"all of band 10, the rest of the cache from band 20 (plus the admitted offer), nothing worse")
}

// TestPiston_ScanBandSkipsASleepingBand pins that a band whose pending steps are all asleep neither stops the
// scan nor is tallied: under the counts' equality seek its band is an empty range, so the due band behind it is
// still reached.
func TestPiston_ScanBandSkipsASleepingBand(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	r.insertStep(t, 1, 10, "a", 1)
	asleep := r.insertStep(t, 2, 20, "b", 1)
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), 3600000) WHERE step_id=?", asleep)
	assert.NoError(err)
	r.insertStep(t, 3, 30, "c", 1)

	band, tallies, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	assert.Equal(10, band)
	var bands []int
	for _, tl := range tallies {
		bands = append(bands, tl.Band)
	}
	assert.Equal([]int{10, 30}, bands, "the sleeping band is stepped over, not tallied and not the end of the scan")
}

// TestPiston_ScanBandServesTheOverflowLast pins what happens to work at a priority beyond the range - a flow from
// before the maximum existed, or from a replica configured with a larger one: it is counted as one overflow band,
// served after every band in the range, each priority in its own order, and held back like any worse band when
// the range fills the cache.
func TestPiston_ScanBandServesTheOverflowLast(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t) // capacity 8
	r.p.SetMaxPriority(10)

	r.insertStep(t, 1, 10, "edge", 1)
	r.insertStep(t, 2, 100, "legacy", 1)
	r.insertStep(t, 3, 50, "wider", 1)

	_, tallies, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	got := map[string]int{}
	for _, tl := range tallies {
		got[tl.Key] = tl.Band
	}
	assert.Equal(map[string]int{"edge": 10, "legacy": 100, "wider": 50}, got,
		"the overflow is tallied at its actual priorities, so the planner orders it after band 10 and among itself")

	// Band 10 deep enough to fill the cache holds the overflow back like any worse band.
	for i := range 8 {
		r.insertStep(t, 10+i, 10, "edge", 1)
	}
	_, tallies, err = r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	for _, tl := range tallies {
		assert.Equal(10, tl.Band, "past the reach, the overflow is not tallied")
	}
}

// TestPiston_FetchTrimsEachBandToItsOwnCap pins the per-band trim: one statement over-fetches every pair at the
// largest per-key cap, and each pair is cut back to its own band's cap. Cutting to the largest instead would hand
// the lighter band more steps than the plan gave it, and count stolen steps the batch never keeps.
func TestPiston_FetchTrimsEachBandToItsOwnCap(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	for i := range 4 {
		r.insertStep(t, 1+i, 2, "a", 1)
		r.insertStep(t, 10+i, 9, "b", 1)
	}
	got, err := r.p.FetchSteps(ctx, 1, []planner.Fetch{
		{Band: 2, Keys: []string{"a"}, PerKeyCap: 1},
		{Band: 9, Keys: []string{"b"}, PerKeyCap: 3},
	})
	assert.NoError(err)
	assert.Len(got[planner.Slot{Band: 2, Key: "a"}], 1)
	assert.Len(got[planner.Slot{Band: 9, Key: "b"}], 3)
}

// TestPiston_CountBandsReachesContiguously pins that the bands handed to the tally run from 1 to the reach with
// none skipped, empty ones included, so the tally's statement text is a function of the reach alone - not of
// which sparse bands happen to hold work, which would prepare (and on SQL Server compile) a new statement each
// time one came or went.
func TestPiston_CountBandsReachesContiguously(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.p.SetMaxPriority(10)

	r.insertStep(t, 1, 1, "a", 1)
	r.insertStep(t, 2, 4, "b", 1)

	reached, err := r.p.countBands(ctx, "", nil, r.cache.Capacity())
	assert.NoError(err)
	assert.Equal([]int{1, 2, 3, 4}, reached, "bands 2 and 3 are empty and still tallied; nothing past band 4 is")
}

// TestPiston_ScanBandExcludesUnselectableSteps pins the due-ness predicates: a parked step is invisible
// to selection by construction, so tallying one would advertise a backlog no worker can ever pick up.
func TestPiston_ScanBandExcludesUnselectableSteps(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	id := r.insertStep(t, 1, 5, "parked", 1)
	r.park(t, id)
	r.insertStep(t, 2, 5, "live", 1)

	_, tallies, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	assert.Equal(1, len(tallies))
	assert.Equal("live", tallies[0].Key)
}

// TestPiston_ScanBandOnEmptyShard pins that a shard with nothing due reports NoBand and no tallies -
// which the planner needs, since "nothing due here" may free capacity for a peer to spill into.
func TestPiston_ScanBandOnEmptyShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	band, tallies, err := r.p.ScanBand(context.Background(), 1)
	assert.NoError(err)
	assert.Equal(pipeline.NoBand, band)
	assert.Equal(0, len(tallies))
}

// TestPiston_ScanBandCapsCountAtCapacity pins the lossless cap: a key's count is capped at the cache capacity,
// since no key can be assigned more than the whole batch. The cap bounds the reported demand, not the scan.
func TestPiston_ScanBandCapsCountAtCapacity(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	for i := range 20 { // capacity is 8
		r.insertStep(t, i+1, 5, "flood", 1)
	}
	_, tallies, err := r.p.ScanBand(context.Background(), 1)
	assert.NoError(err)
	assert.Equal(1, len(tallies))
	assert.Equal(8, tallies[0].Count, "capped at capacity, not the 20 actually present")
}

// TestPiston_FetchStepsReturnsOldestFirst pins phase three: only the chosen keys, at most perKey each,
// ordered oldest-first within each key - the order the plan replay consumes.
func TestPiston_FetchStepsReturnsOldestFirst(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	a1 := r.insertStep(t, 1, 5, "alpha", 1)
	a2 := r.insertStep(t, 2, 5, "alpha", 1)
	a3 := r.insertStep(t, 3, 5, "alpha", 1)
	b1 := r.insertStep(t, 4, 5, "beta", 1)
	r.insertStep(t, 5, 5, "unchosen", 1)

	got, err := r.fetch(ctx, 5, []string{"alpha", "beta"}, 2)
	assert.NoError(err)
	assert.Equal(2, len(got), "only the chosen keys come back")
	assert.Equal([]int{a1, a2}, got["alpha"], "the two OLDEST of alpha's three, in order")
	assert.Equal([]int{b1}, got["beta"])
	_, hasUnchosen := got["unchosen"]
	assert.False(hasUnchosen)
	assert.NotEqual(a3, 0)
}

// TestPiston_FetchStepsDegenerateArgs pins the two no-op guards - no keys, or no demand - which must not
// build an empty IN-list and hand the database a syntax error.
func TestPiston_FetchStepsDegenerateArgs(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.insertStep(t, 1, 5, "alpha", 1)

	got, err := r.fetch(ctx, 5, nil, 4)
	assert.NoError(err)
	assert.Equal(0, len(got))

	got, err = r.fetch(ctx, 5, []string{"alpha"}, 0)
	assert.NoError(err)
	assert.Equal(0, len(got))
}

// TestPiston_PartitionSplitsSelectionAcrossReplicas pins that the residue class restricts which rows this
// replica tallies and fetches, so replicas sharing a database select disjoint candidates instead of
// racing. Together the two ordinals must see everything exactly once.
func TestPiston_PartitionSplitsSelectionAcrossReplicas(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	// The class is strict only while nothing has aged past the STEAL GRACE, which is a multiple of the cycle
	// period. The fetches are driven directly, so the cadence feeds nothing but the grace: an hour keeps the
	// production predicate with no wall-clock stake. A 2s grace lost to six inserts and the first fetch's
	// compile on a loaded SQL Server.
	r.p.SetTallyCadence(time.Hour, 0)
	r.p.SetSupplyCadence(time.Hour, 0)

	var all []int
	for i := range 6 {
		all = append(all, r.insertStep(t, i+1, 5, "k", 1))
	}

	seen := map[int]int{}
	for ordinal := range 2 {
		r.setResidency(2, ordinal, true)
		got, err := r.fetch(ctx, 5, []string{"k"}, 8)
		assert.NoError(err)
		for _, id := range got["k"] {
			seen[id]++
		}
	}
	for _, id := range all {
		assert.Equal(1, seen[id], "step %d must be selected by exactly one ordinal", id)
	}

	// ok=false disables partitioning: one replica sees everything, which is right for a solo engine.
	r.setResidency(2, 0, false)
	got, err := r.fetch(ctx, 5, []string{"k"}, 8)
	assert.NoError(err)
	assert.Equal(6, len(got["k"]))

	// A class past its expiry behaves the same as ok=false: the shard reports none.
	r.sh.SetResidency(2, 0, true, time.Now().Add(-time.Second))
	got, err = r.fetch(ctx, 5, []string{"k"}, 8)
	assert.NoError(err)
	assert.Equal(6, len(got["k"]))
}

// TestPiston_PartitionNarrowsTheBands pins that the residue class filters the band counts as well as the
// rows: a replica whose own class holds only worse-band work tallies that work, rather than stopping at a
// better band only a peer's class holds - which would idle this replica while the peer serves its own.
func TestPiston_PartitionNarrowsTheBands(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	// Stealing off, so the residue class is strict: what is pinned here is the partition, and a peer's step
	// that aged past a grace - a slow first compile on SQL Server is enough - would be admitted by design.
	r.p.SetStealAfter(0)

	best := r.insertStep(t, 1, 1, "urgent", 1) // band 1, will belong to one ordinal only
	r.insertStep(t, 2, 7, "bulk", 1)
	r.insertStep(t, 3, 7, "bulk", 1)

	for ordinal := range 2 {
		r.setResidency(2, ordinal, true)
		band, tallies, err := r.p.ScanBand(ctx, 1)
		if err != nil {
			t.Fatalf("ordinal %d: %+v", ordinal, err)
		}
		if ordinal == best%2 {
			assert.Equal(1, band, "the ordinal owning the band-1 step tallies it")
			assert.Equal("urgent", tallies[0].Key, "first")
		} else {
			// This replica owns nothing at band 1; its own class holds band-7 work, and that is what it tallies.
			assert.Equal(7, band, "a replica holding nothing at band 1 tallies its own best band")
			assert.Equal(1, len(tallies))
		}
	}
}

// TestPiston_RunDispatchesAndReportsItsTurns drives the whole loop end to end: real steps in, candidates
// in the cache, and a turn count an owner can see moving.
func TestPiston_RunDispatchesAndReportsItsTurns(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetTallyCadence(5*time.Millisecond, 0)
	r.p.SetSupplyCadence(5*time.Millisecond, 0)

	turns, busy, idle := r.p.Liveness()
	assert.Equal(uint64(0), turns, "nothing has turned yet")
	assert.False(busy)
	assert.False(idle)

	sm := seamster.New(true)
	r.p.SetSeams(sm)

	want := []int{
		r.insertStep(t, 1, 5, "k", 1),
		r.insertStep(t, 2, 5, "k", 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	stop := sync.OnceFunc(func() { cancel(); <-done })
	defer stop()
	tallied := sm.Waiter(CheckpointTallyDone)
	go func() { defer close(done); r.p.Run(ctx) }()

	// A scan, then a push planned from it, rendezvoused rather than polled for. The first cycle of each loop
	// runs its statements for the first time against a fresh database, and on a loaded SQL Server that alone
	// takes seconds (measured to 6s alongside other suites, under 1s alone), so the ceiling is a hang guard
	// and nothing else.
	hang := time.After(60 * time.Second)
	select {
	case <-tallied:
	case <-hang:
		t.Fatal("the tally loop never completed a scan")
	}
	pushed := sm.Waiter(CheckpointCycleDone)
	select {
	case <-pushed:
	case <-hang:
		t.Fatal("the supply loop never pushed")
	}
	stop()

	assert.Equal(want, drain(r.cache), "the loop pushed both due steps, oldest first")
	turns, _, _ = r.p.Liveness()
	assert.True(turns > 0, "and a turning loop says so")

	// Reading it again reports the same count: the evidence is a counter, not a flag the reader clears. A
	// consuming getter would let any second caller - a metric, a test - silently swallow it and leave a
	// healthy piston looking stalled.
	again, _, _ := r.p.Liveness()
	assert.Equal(turns, again, "looking twice reports the same turns twice")
}

// TestPiston_RunSuppliesWhileTheScanIsStalled pins what the two loops are FOR, over the real loops and a
// real database.
//
// The band scan costs O(due rows at the band) on every dialect, so a deep backlog can stretch it far past
// the cycle period. A single loop would let that scan set the rate candidates reach the workers - supplying
// least exactly when the backlog is deepest. Here the tally loop is pinned to an interval it will not come
// round on again within the test, standing in for a scan that is still running, and the supply loop must go
// on filling the partition from the tally already in the planner regardless.
//
// A fused build fails this outright: with one loop, no push can happen until the scan does.
func TestPiston_RunSuppliesWhileTheScanIsStalled(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	want := []int{
		r.insertStep(t, 1, 5, "k", 1),
		r.insertStep(t, 2, 5, "k", 1),
	}

	// One tally by hand, so the planner holds this shard's band - the scan's whole contribution.
	assert.NoError(r.p.TallyCycle(context.Background()).Err)

	// Now stall the tally loop and let Run drive both. Only the supply loop can make progress.
	r.p.SetTallyCadence(time.Hour, 0)
	r.p.SetSupplyCadence(time.Millisecond, time.Millisecond)
	sm := seamster.New(true)
	r.p.SetSeams(sm)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	pushed := sm.Waiter(CheckpointCycleDone)
	go func() { defer close(done); r.p.Run(ctx) }()

	// The first push is planned from the hand tally, so it carries both steps. The bound is a hang guard only:
	// a first fetch against a fresh database on a loaded server takes seconds.
	select {
	case <-pushed:
	case <-time.After(60 * time.Second):
		t.Error("the supply loop never pushed")
	}
	cancel()
	<-done

	assert.Equal(want, drain(r.cache), "the supply loop filled the partition while the scan was stalled")
}

// TestPiston_IdleWithdrawsAgainstARunningLoop pins going idle against a cycle ALREADY IN FLIGHT, which is
// the only interleaving that matters and the one SetIdle alone cannot cover.
//
// SetIdle clears what is there when it runs. A loop that entered its cycle before that - past its own idle
// check - finishes afterwards and re-enters the shard it just withdrew, or re-pushes the partition it just
// emptied. Both loops then park, and the stale state stands FOREVER: every live piston on the replica finds
// none of its own keys at that band and dispatches nothing, which is the exact wedge SetIdle exists to
// prevent. The band scan runs for seconds on a deep backlog, so the window is not a sliver.
//
// Both halves are staged rather than raced, in the one order that leaves each loop something to undo. Each loop's
// cycle-begin checkpoint is past its idle check, so breaking it holds the loop inside a cycle, and the two are
// separate checkpoints, released one at a time. The test starts the two loops itself, exactly as Run does. The tally loop goes first and is frozen at CheckpointTallyDone, after its scan
// re-entered the shard and before its own idle check; only then does the supply loop run, so it plans from
// that stale tally and re-pushes. Released together, the supply loop usually plans from the tally SetIdle had
// already cleared, pushes nothing, and its withdrawal goes untested.
func TestPiston_IdleWithdrawsAgainstARunningLoop(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetTallyCadence(time.Millisecond, 0)
	r.p.SetSupplyCadence(time.Millisecond, 0)
	sm := seamster.New(true)
	r.p.SetSeams(sm)
	r.insertStep(t, 1, 5, "alpha", 1)

	// Each loop is held at its cycle's begin checkpoint, past its idle check.
	sm.Break(CheckpointTallyBegin)
	sm.Break(CheckpointSupplyBegin)
	defer sm.Resume(CheckpointTallyBegin)
	defer sm.Resume(CheckpointSupplyBegin)
	tallyIn := sm.Waiter(CheckpointTallyBegin)
	supplyIn := sm.Waiter(CheckpointSupplyBegin)

	ctx, cancel := context.WithCancel(context.Background())
	var loops sync.WaitGroup
	loops.Add(2)
	go func() { defer loops.Done(); r.p.runTallier(ctx) }()
	go func() { defer loops.Done(); r.p.runSupplier(ctx) }()
	defer func() { cancel(); loops.Wait() }()

	hang := time.After(60 * time.Second)
	await := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-hang:
			t.Fatal(what)
		}
	}
	await(tallyIn, "the tally loop never entered a cycle")
	await(supplyIn, "the supply loop never entered a cycle")
	r.p.SetIdle(true) // both loops are inside a cycle, past their idle checks

	sm.Break(CheckpointTallyDone)
	defer sm.Resume(CheckpointTallyDone)
	frozen := sm.Waiter(CheckpointTallyDone)
	sm.Resume(CheckpointTallyBegin)
	await(frozen, "the held tally cycle never completed")
	assert.True(r.planner.Plan(1, 8).Tallied, "the held scan re-entered the shard after SetIdle")

	pushed := sm.Waiter(CheckpointCycleDone)
	sm.Resume(CheckpointSupplyBegin)
	await(pushed, "the held supply cycle never pushed from the stale tally")

	// Each withdrawal must take, and must STAY taken: an idle loop starts no further cycle, so once it has
	// withdrawn nothing puts the state back.
	deadline := time.Now().Add(60 * time.Second)
	for r.cache.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	assert.Equal(0, r.cache.Len(), "the supply loop empties the partition it re-pushed")
	sm.Resume(CheckpointTallyDone)
	for r.planner.Plan(1, 8).Tallied && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	assert.False(r.planner.Plan(1, 8).Tallied, "an idle shard must not be left in the planner")

	// Asserted from a PEER's point of view too, which is where the wedge is felt: with shard 1 gone, a peer
	// holding only band 9 is the best band in the fleet and is released to dispatch. A stale band-5 claim
	// from the idle shard would outrank it forever.
	r.planner.Tally(2, []planner.Tally{{Band: 9, Key: "beta", Weight: 1, Count: 1}})
	plan := r.planner.Plan(2, 8)
	assert.Equal(9, plan.GlobalBand, "the idle shard's band claim must not come back")
	assert.True(len(plan.Slots) > 0, "so the live shard is released to dispatch")
	assert.Equal(0, r.cache.Len(), "and the partition stays empty")
}

// TestPiston_RunIdleTurnsNothing pins the idle mode over the real loop: it selects nothing at all and
// reports itself idle, so an owner can keep the replica counted for the connections it holds while
// excluding it from anything that divides work.
func TestPiston_RunIdleTurnsNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetIdle(true)
	r.insertStep(t, 1, 5, "k", 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.p.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done

	turns, busy, idle := r.p.Liveness()
	assert.Equal(uint64(0), turns, "an idle piston never turns, however much is due")
	assert.False(busy)
	assert.True(idle, "and says so, so nothing has to infer it from the silence")
	assert.Equal(0, r.cache.Len())
	band, _ := r.planner.LastBand()
	assert.Equal(-1, band, "it never touches the planner, so nothing was ever planned")
}

// TestPiston_RunStopsOnCancel pins prompt shutdown: both queries are read-only, so there is nothing to
// commit and the loop can be abandoned mid-flight.
func TestPiston_RunStopsOnCancel(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetTallyCadence(30*time.Second, 0)
	r.p.SetSupplyCadence(30*time.Second, 0) // would hang if cancellation were ignored

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.p.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	stopped := false
	select {
	case <-done:
		stopped = true
	case <-time.After(5 * time.Second):
	}
	assert.True(stopped, "Run must return promptly on cancellation")
}

// TestPiston_ScanErrSeamDrivesThePipelineErrorPolicy pins the one seam this package consults, and the
// reason it earns its place: the scan-error policy - clear this shard from planning, leave its cache
// partition ALONE - is otherwise reachable only by breaking a real database mid-run. The two halves are
// asymmetric on purpose (an error means "unknown", not "nothing is due") and now sit in DIFFERENT loops -
// the Tallier clears, the Supplier spares - so both are asserted, and the supply loop is turned SEVERAL
// times to prove it keeps sparing rather than merely happening to run once.
func TestPiston_ScanErrSeamDrivesThePipelineErrorPolicy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.insertStep(t, 1, 5, "alpha", 1)

	// A healthy round trip first, so there is a tally to clear and a partition to preserve.
	_, sup := r.p.Cycle(ctx)
	assert.NoError(sup.Err)
	band, keys := r.planner.LastBand()
	assert.Equal(5, band)
	assert.Equal(1, keys)
	assert.True(r.cache.Len() > 0, "the healthy cycle populated the partition")
	held := r.cache.Len()

	seams := seamster.New(true)
	r.p.SetSeams(seams)
	seams.Inject(FaultScanErr)

	// The scan fails without reaching the database at all.
	_, _, err := r.p.ScanBand(ctx, 1)
	assert.Error(err)

	// And through the loops: the Tallier clears itself from planning, and every Supplier cycle after it
	// leaves the candidates alone, because a cleared shard means UNKNOWN rather than "nothing is due".
	seams.InjectN(FaultScanErr, 4)
	tal := r.p.TallyCycle(ctx)
	assert.Error(tal.Err, "the cycle reports the failure rather than returning it")
	for range 3 {
		sup = r.p.SupplyCycle(ctx)
		assert.NoError(sup.Err, "an untallied shard is not the Supplier's error to report")
		assert.False(sup.Reconciled, "and nothing was reconciled")
		assert.Equal(held, r.cache.Len(), "a FAILED scan must not wholesale-replace a healthy partition")
		assert.Equal(pipeline.NoBand, sup.GlobalBand, "the cleared shard no longer claims a band")
	}
}

// TestPiston_SeamsDefaultInert pins that a piston nobody handed seams to consults nothing - the
// production shape, and the reason SetSeams can be left unwired without a nil check at the consult site.
func TestPiston_SeamsDefaultInert(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.insertStep(t, 1, 5, "alpha", 1)

	_, _, err := r.p.ScanBand(context.Background(), 1)
	assert.NoError(err, "the default seams are inert")

	// Nil restores that default after a live one was set.
	live := seamster.New(true)
	r.p.SetSeams(live)
	r.p.SetSeams(nil)
	live.Inject(FaultScanErr)
	_, _, err = r.p.ScanBand(context.Background(), 1)
	assert.NoError(err, "nil restores the inert default")
}

// TestPiston_IdleWithdrawsTheShard pins that going idle makes the same positive statement an empty plan
// does. The planner's contract is that every shard tallies or clears each cycle, and an idle piston runs no
// cycle - so without this its last tally stands forever. The planner is SHARED across a replica's pistons,
// so a stale claim on a better band that fills the batch leaves every live piston no capacity to spill into,
// and they dispatch nothing, indefinitely. The cache partition goes for the reason an empty plan clears it: dead hints cost a
// worker a claim round-trip each.
func TestPiston_IdleWithdrawsTheShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.insertStep(t, 1, 5, "alpha", 1)

	_, sup := r.p.Cycle(ctx)
	assert.NoError(sup.Err)
	assert.True(r.cache.Len() > 0, "the cycle populated the partition")
	band, _ := r.planner.LastBand()
	assert.Equal(5, band, "and claimed its band in the shared planner")

	r.p.SetIdle(true)
	assert.Equal(0, r.cache.Len(), "idling empties the partition rather than leaving dead hints")

	// The withdrawal is visible to a PEER planning off the same shared planner: with shard 1 cleared, a
	// peer holding only band 9 is now the best band in the fleet and dispatches, instead of deferring
	// forever to a band nobody is serving.
	r.planner.Tally(2, []planner.Tally{{Band: 9, Key: "beta", Weight: 1, Count: 1}})
	plan := r.planner.Plan(2, 8)
	assert.Equal(9, plan.GlobalBand, "the idle shard's stale band claim is gone")
	assert.True(len(plan.Slots) > 0, "so the live shard is released to dispatch")
}

// TestPiston_PartitionPairIsValidated pins the fail-open posture against a bad (replicas, ordinal) pair.
// replicas=0 would emit `step_id % 0` and error every query; an ordinal at or past replicas matches nothing
// and reports NoBand while genuinely holding work - silent, and the worse of the two.
func TestPiston_PartitionPairIsValidated(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.p.SetSeams(seamster.New(true)) // so CheckpointStole visits are countable
	for i := 1; i <= 6; i++ {
		r.insertStep(t, i, 5, "k", 1)
	}

	for _, tc := range []struct {
		name              string
		replicas, ordinal int
		ok                bool
	}{
		{"zero replicas", 0, 0, true},
		{"ordinal past replicas", 2, 2, true},
		{"negative ordinal", 2, -1, true},
		{"solo replica", 1, 0, true},
		{"not ok", 4, 1, false},
	} {
		r.setResidency(tc.replicas, tc.ordinal, tc.ok)
		band, tallies, err := r.p.ScanBand(ctx, 1)
		assert.NoError(err, "%s must not error the scan", tc.name)
		if assert.Equal(1, len(tallies), "%s must select everything, not nothing", tc.name) {
			assert.Equal(6, tallies[0].Count, "%s: no rows excluded", tc.name)
		}
		assert.Equal(5, band, "%s reports the real band", tc.name)

		// The FETCH must fail open on the same pairs, and - the part a weaker second read gets wrong - it
		// must also RANK as if unpartitioned. An out-of-range ordinal disables the SQL predicate correctly
		// while leaving a ranking built from that bad pair with nothing in tier 0, so every kept step counts
		// as taken from a peer. That shows up as stolen candidates the fetch never stole.
		before := r.p.seams.Load().Visits(CheckpointStole)
		got, err := r.fetch(ctx, 5, []string{"k"}, 6)
		assert.NoError(err, "%s must not error the fetch", tc.name)
		assert.Equal(6, len(got["k"]), "%s: the fetch must select everything too", tc.name)
		assert.Equal(before, r.p.seams.Load().Visits(CheckpointStole),
			"%s: a fail-open pair must rank as unpartitioned, so nothing is reported stolen", tc.name)
	}
}

// TestPiston_NewValidates pins that a wiring mistake is caught at construction.
func TestPiston_NewValidates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	assert.Equal(1, r.p.Shard())
	assert.Equal(pipeline.DefaultInterval, func() time.Duration {
		p, _ := New(r.sh, r.planner, r.cache)
		i, _ := p.TallyCadence()
		return i
	}(), "a fresh piston inherits the pipeline's paced default")

	_, err := New(nil, r.planner, r.cache)
	assert.Error(err, "shard is required")
	_, err = New(database.NewShard(0, r.db), r.planner, r.cache)
	assert.Error(err, "shard must be positive")
	_, err = New(database.NewShard(1, nil), r.planner, r.cache)
	assert.Error(err, "db is required")
	_, err = New(r.sh, nil, r.cache)
	assert.Error(err, "planner is required")
	_, err = New(r.sh, r.planner, nil)
	assert.Error(err, "cache is required")
}

// TestPiston_RecordsItsInstruments pins the instrument names the two loops emit, BY NAME. The names are a public
// surface that dashboards bind to, so this test failing on a rename is the point of it.
func TestPiston_RecordsItsInstruments(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	reader := sdkmetric.NewManualReader()
	assert.NoError(r.p.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")))

	// Seed the cache so the cycle's push has something to discard, exercising that counter too.
	r.cache.Refill(1, []candidates.Job{{StepID: 9999, Shard: 1}}, 100)
	r.insertStep(t, 1, 5, "k", 1)

	tal, sup := r.p.Cycle(ctx)
	assert.NoError(tal.Err)
	assert.NoError(sup.Err)
	r.p.recordTally(ctx, tal)
	r.p.recordSupply(ctx, sup)

	var rm metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &rm))
	got := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = true
		}
	}
	for _, name := range []string{
		"dwarf_refill_query_duration_seconds",
		"dwarf_refill_candidates_selected",
		"dwarf_refill_candidates_discarded",
	} {
		assert.True(got[name], "instrument %q must be emitted under its published name", name)
	}
}

// TestPiston_SettersAreLive pins that the knobs apply without a restart, including the nil-logger reset.
func TestPiston_SettersAreLive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	// Set the two loops APART, which also pins that neither setter reaches the other's pacer.
	r.p.SetTallyCadence(77*time.Millisecond, 11*time.Millisecond)
	r.p.SetSupplyCadence(31*time.Millisecond, 7*time.Millisecond)
	ti, tg := r.p.TallyCadence()
	assert.Equal(77*time.Millisecond, ti)
	assert.Equal(11*time.Millisecond, tg)
	si, sg := r.p.SupplyCadence()
	assert.Equal(31*time.Millisecond, si, "the supply loop keeps its own interval")
	assert.Equal(7*time.Millisecond, sg, "and its own gap")
	r.p.SetLogger(nil)
	assert.NotNil(r.p.logger.Load(), "a nil logger restores the discarding default, never nil")
	assert.NoError(r.p.SetMeter(nil), "a nil meter restores no-op instruments")
}

// TestPiston_CadenceIsPerLoop pins that nothing derived from "the cycle period" keys off ONE loop while
// covering both. The two loops are paced independently by design, so a shared threshold is right for at
// most one of them - and while an owner happens to set both to the same value, keying off the wrong one is
// invisible. This asserts the property directly, so it stays true when the cadences diverge.
func TestPiston_CadenceIsPerLoop(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetStealAfter(4)

	// The SUPPLY loop is the slow one here, which is the case a tallier-only derivation gets wrong. Both
	// values sit above DefaultMinGap so the floor is not what is being measured.
	r.p.SetTallyCadence(50*time.Millisecond, 0)
	r.p.SetSupplyCadence(time.Second, 0)

	assert.Equal(50*time.Millisecond, r.p.tallier.Period(), "the tally loop keeps its own short period")
	assert.Equal(time.Second, r.p.supplier.Period(), "the supply loop keeps its own long one")

	// The grace measures how long a HEALTHY owner takes to reach its own work, which is a full round trip -
	// so it follows the LONGER loop, not whichever one the facade happens to report.
	assert.Equal(4*time.Second, r.p.stealGrace(),
		"the steal grace must follow the slower loop; keying off the tallier would give %v", 4*50*time.Millisecond)

	// And the floor still applies per loop when a caller pins the cadence to zero.
	r.p.SetTallyCadence(0, 0)
	r.p.SetSupplyCadence(0, 0)
	assert.Equal(pipeline.DefaultMinGap, r.p.tallier.Period(), "a zeroed loop floors at the constant")
	assert.Equal(4*pipeline.DefaultMinGap, r.p.stealGrace(), "so the grace can never reach zero")
}

// TestPiston_FailingCyclesReportNoLiveness pins the distinction the whole dispatch-evidence design rests
// on: a piston whose every SCAN fails must report itself as not serving, so its owner can stop handing it
// work that nobody would then select.
//
// The supply loop goes on turning perfectly happily here - it plans from an empty planner, pushes nothing,
// and reports no error at all - so a turn count taken from IT would read a piston that cannot see its own
// shard as fully alive. That is why the count is the MINIMUM of the two loops; the mirror case, where every
// fetch fails and the scan is healthy, is TestPiston_FailingFetchesReportNoLiveness.
//
// The busy half is easy to get wrong in a way that looks right. A failing cycle is still briefly inside its
// queries - building the error, recording the phase, logging it - so a busy flag meaning "a cycle is in
// flight" reads true a small but nonzero fraction of the time (measured ~1.2% with a scan that fails
// instantly), and a reader sampling on its own clock catches that within seconds. It then keeps a broken
// piston looking alive for good, which is exactly the stranding the evidence exists to prevent. Busy
// therefore means a loop has been working LONGER THAN ONE PERIOD, which neither loop is here.
func TestPiston_FailingCyclesReportNoLiveness(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	sm := seamster.New(true)
	r.p.SetSeams(sm)
	r.insertStep(t, 1, 5, "k", 1) // rig default interval is 0 - the degenerate case the floor covers

	// A healthy ROUND TRIP first - both loops, since the turn count is their minimum and a tally alone would
	// leave it pinned at whatever the supply loop has managed.
	before, _, _ := r.p.Liveness()
	r.p.Cycle(ctx)
	after, busy, _ := r.p.Liveness()
	assert.Equal(before+1, after, "a completed round trip is a turn")
	assert.False(busy, "and it is not still working")

	// From here every scan fails. Sample hard while the loop runs: the turn count must not move and busy
	// must never once read true.
	sm.InjectN(FaultScanErr, 1<<20)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); r.p.Run(runCtx) }()

	// EPISODES, not samples, and the distinction is what makes this measurable on a shared machine. The two
	// things a busy reading can mean have opposite shapes:
	//
	//   - A busy flag meaning merely "a cycle is in flight" reads true for the sliver each failing cycle
	//     spends in its queries, over and over. Against a 1ms sampler that is a scatter of MANY short
	//     episodes: measured 61-103 of them per window under -race, by running this against a build whose
	//     predicate was `WorkingFor() > 0`.
	//   - A machine that starves this piston's goroutine inside one cycle produces ONE episode, however long
	//     the stall: every sample for its duration reads busy. Counting samples cannot tell a scatter of
	//     short episodes from one long stall - measured 8 busy samples in a starved window, against 175-271
	//     under the broken predicate - while counting rising edges tells them apart by an order of magnitude.
	//
	// So a handful of episodes is tolerated and a scatter is not, which is the honest reading of the
	// evidence: under the duration-qualified predicate a busy sample is only ever produced by a cycle that
	// genuinely outran its period, and on this workload nothing but the scheduler can do that. The bound sits
	// ~6x under the broken build's floor and ~5x over the worst starvation seen (2 episodes, in a window
	// whose sample count had itself collapsed from ~450 to 175 - the same contention, visible twice).
	// A SAMPLE BUDGET, NOT A WALL-CLOCK ONE, and it fixes the bound above as much as the flakiness. Both
	// counts scale with how many samples a window fits: the broken predicate's 61-103 episodes and the
	// starved window's 2 were both measured per ~450 samples, so "fewer than 10" only means what it was
	// calibrated to mean at that density. Budgeting wall clock makes the density an OUTCOME - a loaded
	// machine fits fewer samples, the bound silently loosens, and the test keeps reporting pass while
	// testing less (measured: ~450 samples collapsing to 175 under contention). Budgeting samples makes it
	// an INPUT: load can only make this take longer, never make it weaker.
	//
	// The safety deadline is 30x the healthy duration, so it bounds a pathologically starved machine
	// without ever binding on a merely busy one; the density assertion below is what refuses to conclude
	// anything if it ever does fire.
	const wantSamples = 450
	stalled, _, _ := r.p.Liveness()
	var busyEpisodes, busySamples, samples int
	wasBusy := false
	safety := time.Now().Add(15 * time.Second)
	for samples < wantSamples && time.Now().Before(safety) {
		n, b, _ := r.p.Liveness()
		samples++
		if b {
			busySamples++
			if !wasBusy {
				busyEpisodes++
			}
		}
		wasBusy = b
		assert.Equal(stalled, n, "a failing cycle must never count as a turn")
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	assert.True(busyEpisodes < 10,
		"a piston that only fails must not read as busy across separate cycles (%d episodes, %d of %d samples)",
		busyEpisodes, busySamples, samples)
	assert.True(samples > 100,
		"the sampling has to be dense enough to catch a brief window - only reachable if the safety deadline "+
			"fired, i.e. this machine could not take %d samples in 15s (took %d)", wantSamples, samples)
}

// TestPiston_FailingFetchesReportNoLiveness pins the OTHER half of the dispatch evidence, and it is the half
// a per-loop turn count gets wrong.
//
// A piston whose every FETCH fails is invisible from outside: it scans honestly, publishes a real tally,
// claims its band, and logs nothing a reader can see - while its partition takes zero candidates. So it
// looks fully alive, keeps its residue class, and the work in that class is selected by nobody. That is the
// same stranding a failing SCAN causes, reached from the opposite direction, which is why the turn count is
// the MINIMUM of the two loops rather than either one of them.
//
// It fails against a build that counts tallies alone: the tally loop turns freely here.
func TestPiston_FailingFetchesReportNoLiveness(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	sm := seamster.New(true)
	r.p.SetSeams(sm)
	r.insertStep(t, 1, 5, "k", 1)

	// A healthy round trip first: both loops turn, so the minimum moves.
	before, _, _ := r.p.Liveness()
	r.p.Cycle(ctx)
	after, _, _ := r.p.Liveness()
	assert.Equal(before+1, after, "a completed round trip is a turn")

	// From here every fetch fails while every scan keeps succeeding.
	sm.InjectN(FaultFetchErr, 1<<20)
	for range 20 {
		tal, sup := r.p.Cycle(ctx)
		assert.NoError(tal.Err, "the scan is healthy throughout - that is the point")
		assert.Error(sup.Err, "and every fetch fails")
	}

	stalled, busy, idle := r.p.Liveness()
	assert.Equal(after, stalled,
		"a piston that cannot fetch must not report turns, however well it scans (was %d, now %d)", after, stalled)
	assert.False(busy, "and it is not merely slow")
	assert.False(idle)

	// The tally loop really was turning throughout, so the frozen count is the MINIMUM doing its job rather
	// than both loops having stopped.
	assert.True(r.p.tallyTurns.Load() > r.p.supplyTurns.Load(),
		"the tally loop kept turning (%d) while the supply loop did not (%d)",
		r.p.tallyTurns.Load(), r.p.supplyTurns.Load())

	// And it recovers on the first cycle that can fetch again, with no cooldown.
	sm.Withdraw(FaultFetchErr)
	r.p.Cycle(ctx)
	recovered, _, _ := r.p.Liveness()
	assert.True(recovered > stalled, "recovery costs exactly one round trip")
}

// TestPiston_BankedTurnsDoNotCarryAFailingLoop pins that a turn needs BOTH loops to have turned since the last
// one, so a surplus one loop banked while healthy cannot keep a piston looking alive once the other stops. The two
// loops are paced independently, and a minimum over their lifetime counts moved again for every turn of the
// slower loop while the faster one failed, until its historical lead ran out: 1 -> 11 over ten cycles with every
// fetch failing, against at most one turn now. Both directions: a supply surplus over failing fetches, a tally
// surplus over failing scans.
func TestPiston_BankedTurnsDoNotCarryAFailingLoop(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name  string
		bank  func(p *Piston, ctx context.Context)
		fault string
	}{
		{"supply_surplus_then_failing_fetches", func(p *Piston, ctx context.Context) { p.SupplyCycle(ctx) }, FaultFetchErr},
		{"tally_surplus_then_failing_scans", func(p *Piston, ctx context.Context) { p.TallyCycle(ctx) }, FaultScanErr},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := context.Background()
			r := newRig(t)
			sm := seamster.New(true)
			r.p.SetSeams(sm)
			r.insertStep(t, 1, 5, "k", 1)

			r.p.Cycle(ctx)
			for range 20 {
				c.bank(r.p, ctx) // one loop runs ahead while everything is healthy
			}
			healthy, _, _ := r.p.Liveness()

			// The banked loop's last healthy cycle and the other's next one are one genuine pair, so at most one more
			// turn is reported; past it nothing banked may count.
			sm.InjectN(c.fault, 1<<20)
			r.p.Cycle(ctx)
			first, _, _ := r.p.Liveness()
			assert.True(first <= healthy+1, "at most the one pair already under way (%d -> %d)", healthy, first)
			for range 10 {
				r.p.Cycle(ctx)
			}
			stalled, _, _ := r.p.Liveness()
			assert.Equal(first, stalled, "a loop that fails every cycle must stop the turns, whatever the other banked")

			sm.Withdraw(c.fault)
			r.p.Cycle(ctx)
			recovered, _, _ := r.p.Liveness()
			assert.True(recovered > stalled, "and one healthy round trip resumes them")
		})
	}
}

// TestPiston_StealPredicateIsAlwaysRelaxed pins that there is no gate: the residue class is relaxed on
// every query, and only the AGE terms decide whether a foreign step is admitted at all.
//
// Nothing arms this. Do not gate the relaxation on a flag armed when the previous scan's own class came up
// short: the flag's meaning depends on which clause that scan ran, so it means one thing after a strict scan
// and another after a relaxed one. The fill order in rankByResidue asks the same question exactly and
// currently instead.
func TestPiston_StealPredicateIsAlwaysRelaxed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.setResidency(2, 0, true)

	sql, args := func() (string, []any) {
		rep, ord := r.p.resolvePartition()
		return r.p.partitionPredicate(rep, ord, r.p.stealGrace())
	}()
	assert.Contains(sql, "OR not_before <=", "the class is relaxed with nothing having to arm it")
	// Two tiers plus the strict term: own class, then the neighbour's after one grace, then anyone's after
	// two. Six binds - (replicas, ordinal), (replicas, neighbour, -grace), (-2*grace).
	assert.Equal(6, len(args))
	assert.Equal(2, args[0], "the strict term keeps the real divisor")
	assert.Equal(0, args[1], "and this replica's own ordinal")
	assert.Equal(1, args[3], "the second tier names the NEIGHBOUR's ordinal, (0+1) mod 2")
	assert.Equal(2*args[4].(int64), args[5], "and anyone's class only after TWICE the grace")
	// The relaxation only ever ADMITS rows - the strict term survives intact, so no class is stranded.
	assert.Contains(sql, "step_id % ? = ?")

	// Zero periods disables it outright, which is the strict-partitioning escape hatch.
	r.p.SetStealAfter(0)
	sql, _ = func() (string, []any) {
		rep, ord := r.p.resolvePartition()
		return r.p.partitionPredicate(rep, ord, r.p.stealGrace())
	}()
	assert.Equal(" AND step_id % ? = ?", sql, "stealAfter=0 restores strict partitioning")
	r.p.SetStealAfter(defaultStealAfter)

	// Nothing to relax when there is no partition: a solo replica already selects everything.
	r.setResidency(1, 0, true)
	sql, _ = func() (string, []any) {
		rep, ord := r.p.resolvePartition()
		return r.p.partitionPredicate(rep, ord, r.p.stealGrace())
	}()
	assert.Equal("", sql, "stealing must not resurrect a partition the pair disabled")
}

// TestPiston_FetchPrefersItsOwnClass pins the fill order: among steps the grace ADMITTED, this replica's
// own class is ranked first, its designated neighbour's second, everyone else's last - and the fetch keeps
// only the per-key cap off the top.
//
// The over-fetch is what makes this possible and is not an optimisation. The query returns rows oldest
// first, and the oldest admitted rows are precisely the stalled peer's, so fetching only perKey would come
// back ENTIRELY foreign and leave the ranking nothing of this replica's own to prefer - failing in exactly
// the case the mechanism exists for.
func TestPiston_FetchPrefersItsOwnClass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	// Ordinal 0 of 2: this replica owns EVEN step ids, so odd ones are foreign.
	r.setResidency(2, 0, true)
	r.p.SetTallyCadence(500*time.Millisecond, 0)
	r.p.SetSupplyCadence(500*time.Millisecond, 0)
	r.p.SetStealAfter(4) // grace = 2s

	for range 8 {
		r.insertStep(t, 1, 5, "k", 1)
	}
	own, foreign := r.idsByResidue(t, 2, 0)
	assert.True(len(own) > 0 && len(foreign) > 0, "the fixture needs both classes populated")

	// Age EVERYTHING past the grace, so admission cannot be what orders the result - only the ranking can.
	// The oldest rows are therefore a mix of both classes, and the SQL would hand back the foreign ones
	// first if nothing re-ranked them. Backdated with the DATABASE clock, never a bound Go time.
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -5000)")
	assert.NoError(err)

	// Asking for exactly this replica's own count must yield its own class and nothing else.
	got, err := r.fetch(ctx, 5, []string{"k"}, len(own))
	assert.NoError(err)
	assert.Equal(own, got["k"], "own-class steps rank ahead of admitted foreign ones")

	// Asking for more than it owns fills the remainder from the foreign classes, own first, and each tier
	// stays oldest-first inside itself.
	got, err = r.fetch(ctx, 5, []string{"k"}, 8)
	assert.NoError(err)
	assert.Equal(append(append([]int{}, own...), foreign...), got["k"],
		"the shortfall is filled from foreign classes, behind every own-class step")

	// And the per-key cap is honoured despite the over-fetch, so the plan gets what it asked for.
	got, err = r.fetch(ctx, 5, []string{"k"}, 2)
	assert.NoError(err)
	assert.Equal(2, len(got["k"]), "the over-fetch is trimmed back to the plan's per-key cap")
}

// TestPiston_FetchSurfacesItsShareFromAForeignHead pins the over-fetch's factor of 2R. Under saturation every
// class is admitted and the oldest rows are the classes interleaved by chance, so a window of perKey*R holds
// this replica's share only on average. The head here is a run of foreign rows longer than a perKey*R window
// can absorb (6 of 8 at perKey=4) and shorter than a 2R one (6 of 16), with this replica's own class still
// deeper than the cap behind it - so the fetch must come back own-class only.
func TestPiston_FetchSurfacesItsShareFromAForeignHead(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	// Ordinal 0 of 2: this replica owns EVEN step ids, so odd ones are foreign.
	r.setResidency(2, 0, true)
	r.p.SetTallyCadence(500*time.Millisecond, 0)
	r.p.SetSupplyCadence(500*time.Millisecond, 0)
	r.p.SetStealAfter(4) // grace = 2s

	for range 24 {
		r.insertStep(t, 1, 5, "k", 1)
	}
	_, foreign := r.idsByResidue(t, 2, 0)
	// Six foreign rows become the oldest, and everything is past the grace, so admission orders nothing.
	// Both backdated with the DATABASE clock, never a bound Go time.
	for _, id := range foreign[:6] {
		_, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET created_at=DATE_ADD_MILLIS(NOW_UTC(), -10000) WHERE step_id=?", id)
		assert.NoError(err)
	}
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -5000)")
	assert.NoError(err)
	own, _ := r.idsByResidue(t, 2, 0)

	got, err := r.fetch(ctx, 5, []string{"k"}, 4)
	assert.NoError(err)
	assert.Equal(own[:4], got["k"], "a foreign head the window can see past must not displace this replica's own class")
}

// TestPiston_StealTakesOnlyLongDueForeignSteps drives the relaxed predicate against real SQL, which is the
// only way to know the clause means what it reads as. The age is measured from not_before, NOT created_at:
// not_before is stamped at creation and pushed forward by flow.Sleep and every retry backoff, so this reads
// as "has been DUE for at least the grace". Against created_at, an hour-long sleep would be stolen the
// instant it came due, on a fleet with nothing wrong with it.
func TestPiston_StealTakesOnlyLongDueForeignSteps(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	// Ordinal 0 of 2: this replica owns EVEN step ids, so odd ones are foreign.
	r.setResidency(2, 0, true)
	// The grace has to dominate any stall of the database, because "young" is measured against the DATABASE
	// clock from the moment a step became due - so the round trips between the re-stamp and the fetch count
	// against it. A 200ms grace was outrun by inserts under -race on a busy box, and a 2s grace by a loaded
	// SQL Server. FetchSteps is driven directly here, so the interval paces nothing and only feeds
	// stealGrace; 10 minutes is past any stall, and the last phase backdates by an hour to age a step PAST it.
	r.p.SetTallyCadence(time.Minute, 0)
	r.p.SetSupplyCadence(time.Minute, 0)
	r.p.SetStealAfter(10) // grace = 10m

	for range 8 {
		r.insertStep(t, 1, 5, "k", 1)
	}
	own, foreign := r.idsByResidue(t, 2, 0)
	assert.True(len(own) > 0 && len(foreign) > 0, "the fixture needs both classes populated")

	// Every step is freshly due - younger than the grace - so a healthy owner's work is left alone even
	// though this replica is asking for far more than its own class holds. This is the moderate-load case
	// the grace exists for, and it is the ONLY thing covering it: the fill order cannot, because a batch is
	// sized to cache capacity rather than to what is due, so every replica in an under-saturated fleet has
	// spare slots it would otherwise fill from healthy peers.
	//
	// Re-stamped rather than trusted to still be young: this makes "freshly due" true at the instant of the
	// fetch rather than a bet on how long the setup above took, so only the fetch's own round trip is
	// charged against the grace. Same database clock as the ageing UPDATE below, never a bound Go time.
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=NOW_UTC()")
	assert.NoError(err)
	got, err := r.fetch(ctx, 5, []string{"k"}, 100)
	assert.NoError(err)
	assert.Equal(own, got["k"], "a young foreign step belongs to its owner")

	// Age the foreign steps past the grace: a stalled owner's class ages without bound, and only then is it
	// admitted. Backdated with the DATABASE clock, never a bound Go time.
	_, err = r.db.ExecContext(ctx,
		"UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -3600000) WHERE step_id % 2 = 1")
	assert.NoError(err)
	got, err = r.fetch(ctx, 5, []string{"k"}, 100)
	assert.NoError(err)
	assert.Equal(8, len(got["k"]), "a long-due foreign step is taken by an idle peer")
}

// TestPiston_StealReachesPastTheNeighbourAfterTwoGraces pins the second tier against real SQL, which takes
// three classes: with two, the neighbour IS everyone else, and the tiers coincide. One grace admits the
// designated neighbour's class only; a class further away waits for two, so a far class whose own stealer is
// itself degraded is still reached rather than stranded.
func TestPiston_StealReachesPastTheNeighbourAfterTwoGraces(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	// Ordinal 0 of 3: this replica owns step_id%3==0, its neighbour's class is 1, and class 2 is the far one.
	r.setResidency(3, 0, true)
	// FetchSteps is driven directly, so the cadence only feeds stealGrace: 10 minutes. Age only grows, so the
	// neighbour's admission cannot be lost to delay; what the margin protects is the far class staying out,
	// which holds while less than 5 minutes pass between the first backdate and its fetch - past any stall of
	// the database.
	r.p.SetTallyCadence(time.Minute, 0)
	r.p.SetSupplyCadence(time.Minute, 0)
	r.p.SetStealAfter(10) // grace = 10m

	for range 9 {
		r.insertStep(t, 1, 5, "k", 1)
	}
	own, _ := r.idsByResidue(t, 3, 0)
	neighbour, _ := r.idsByResidue(t, 3, 1)
	far, _ := r.idsByResidue(t, 3, 2)
	assert.True(len(own) > 0 && len(neighbour) > 0 && len(far) > 0, "the fixture needs all three classes populated")

	// Due for half past one grace: past the first tier, well short of the second. Backdated with the
	// DATABASE clock, never a bound Go time.
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -900000)")
	assert.NoError(err)
	got, err := r.fetch(ctx, 5, []string{"k"}, 100)
	assert.NoError(err)
	assert.Equal(append(append([]int{}, own...), neighbour...), got["k"],
		"one grace admits the neighbour's class and not the far one")

	// Past two graces the far class opens too, ranked behind the nearer tiers.
	_, err = r.db.ExecContext(ctx,
		"UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -1800000) WHERE step_id % 3 = 2")
	assert.NoError(err)
	got, err = r.fetch(ctx, 5, []string{"k"}, 100)
	assert.NoError(err)
	assert.Equal(append(append(append([]int{}, own...), neighbour...), far...), got["k"],
		"two graces admit anyone's class")
}

// TestPiston_DispatchLag pins the lag reading the band scan publishes: absent until a scan completes and while
// idle, zero when nothing is due, measured from not_before rather than created_at, and zero for a shard whose
// bands are all past the reach of another shard's better one.
func TestPiston_DispatchLag(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	_, ok := r.p.DispatchLag()
	assert.False(ok, "no reading before a scan")
	_, _, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	lag, ok := r.p.DispatchLag()
	assert.True(ok)
	assert.Equal(time.Duration(0), lag, "nothing due reads zero")

	// Created now, due for 30s: a created_at-based reading would be ~0.
	id := r.insertStep(t, 1, 5, "alpha", 1)
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -30000) WHERE step_id=?", id)
	assert.NoError(err)
	_, _, err = r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	lag, ok = r.p.DispatchLag()
	assert.True(ok)
	assert.True(lag >= 29*time.Second, "measured from not_before, got %v", lag)

	// Another shard holds a better band that fills the cache: this shard's work is held by priority, not lagging.
	r.planner.Tally(2, []planner.Tally{{Band: 1, Key: "urgent", Weight: 1, Count: 8}})
	lag, ok = r.p.DispatchLag()
	assert.True(ok)
	assert.Equal(time.Duration(0), lag, "a shard held back by strict priority reads zero")
	r.planner.Tally(2, []planner.Tally{{Band: 1, Key: "urgent", Weight: 1, Count: 1}})
	lag, _ = r.p.DispatchLag()
	assert.True(lag >= 29*time.Second, "a better band that leaves capacity holds nothing back")
	r.planner.Clear(2)
	lag, _ = r.p.DispatchLag()
	assert.True(lag >= 29*time.Second, "back at the served band, the lag reads again")

	r.p.SetIdle(true)
	_, ok = r.p.DispatchLag()
	assert.False(ok, "an idle piston reports nothing")
}

// setResidency sets the shard's residue class for the rest of the test.
func (r *rig) setResidency(replicas, ordinal int, ok bool) {
	r.sh.SetResidency(replicas, ordinal, ok, time.Now().Add(time.Hour))
}
