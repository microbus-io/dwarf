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
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
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
	db      *sequel.DB
	planner *planner.Planner
	cache   *candidatecache.Cache
}

// newRig stands up an isolated database for the test, migrates it, and wires a piston over it with
// pacing off so a cycle can be driven synchronously. internal/database is a test-only dependency here -
// the piston itself never opens or closes a handle.
func newRig(t *testing.T) *rig {
	t.Helper()
	sum := sha256.Sum256([]byte(t.Name()))
	var set database.ShardSet
	err := set.Open(context.Background(), database.Config{
		Shards:      map[int]database.ShardConfig{1: {MaxIdleConns: 2, MaxOpenConns: 4}},
		TestID:      hex.EncodeToString(sum[:8]),
		TestConnCap: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(set.Close)
	db, err := set.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	cache := &candidatecache.Cache{}
	cache.Init(4) // capacity 8
	t.Cleanup(cache.Close)
	pl := planner.New()
	p, err := New(1, db, pl, cache)
	if err != nil {
		t.Fatal(err)
	}
	p.SetInterval(0)
	p.SetMinGap(0)
	return &rig{p: p, db: db, planner: pl, cache: cache}
}

// insertStep adds one due pending step and returns its id. Steps are inserted oldest-first by call
// order, which is what the fetch's created_at ordering keys on.
func (r *rig) insertStep(t *testing.T, flowID, priority int, key string, weight float64) int {
	t.Helper()
	_, err := r.db.ExecContext(context.Background(),
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status,"+
			" time_budget_ms, priority, fairness_key, fairness_weight, not_before, lease_expires, created_at)"+
			" VALUES (?, 0, 'tok0123456789ab', 'T', 'u', '"+workflow.StatusPending+"', 60000, ?, ?, ?,"+
			" NOW_UTC(), NOW_UTC(), NOW_UTC())",
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

func drain(c *candidatecache.Cache) []int {
	out := make([]int, 0, c.Len())
	for c.Len() > 0 {
		j, ok, _ := c.Pop()
		if !ok {
			break
		}
		out = append(out, j.StepID)
	}
	return out
}

// TestPiston_ScanBandTalliesPerKeyAtTheMinimumBand pins phase one against real SQL: one aggregate row per
// fairness key, only at this shard's minimum due band, carrying the count and the OLDEST step's age and
// weight. A worse band contributes nothing until the better one drains.
func TestPiston_ScanBandTalliesPerKeyAtTheMinimumBand(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	r.insertStep(t, 1, 5, "alpha", 2)
	r.insertStep(t, 2, 5, "alpha", 2)
	r.insertStep(t, 3, 5, "beta", 1)
	r.insertStep(t, 4, 9, "gamma", 1) // worse band: invisible while band 5 has work

	band, tallies, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	assert.Equal(5, band, "the minimum due band")
	assert.Equal(2, len(tallies), "one row per key at that band, never one per step")

	byKey := map[string]planner.Tally{}
	for _, tl := range tallies {
		byKey[tl.Key] = tl
	}
	assert.Equal(2, byKey["alpha"].Count)
	assert.Equal(2.0, byKey["alpha"].Weight, "the oldest step's weight")
	assert.Equal(1, byKey["beta"].Count)
	_, hasGamma := byKey["gamma"]
	assert.False(hasGamma, "a worse band must not be tallied at all")
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
// which the planner needs, since "nothing due here" may raise the global band and release a peer.
func TestPiston_ScanBandOnEmptyShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	band, tallies, err := r.p.ScanBand(context.Background(), 1)
	assert.NoError(err)
	assert.Equal(pipeline.NoBand, band)
	assert.Equal(0, len(tallies))
}

// TestPiston_ScanBandCapsCountAtCapacity pins the lossless cap: the count is MAX(rn) under an
// rn<=capacity cut, not an exact COUNT, so the scan stops touching a key's rows past capacity instead of
// counting the whole partition - the O(backlog) cost this shape exists to avoid.
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

	got, err := r.p.FetchSteps(ctx, 1, 5, []string{"alpha", "beta"}, 2)
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

	got, err := r.p.FetchSteps(ctx, 1, 5, nil, 4)
	assert.NoError(err)
	assert.Equal(0, len(got))

	got, err = r.p.FetchSteps(ctx, 1, 5, []string{"alpha"}, 0)
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

	var all []int
	for i := range 6 {
		all = append(all, r.insertStep(t, i+1, 5, "k", 1))
	}

	seen := map[int]int{}
	for ordinal := range 2 {
		r.p.SetPartitionFunc(func() (int, int, bool) { return 2, ordinal, true })
		got, err := r.p.FetchSteps(ctx, 1, 5, []string{"k"}, 8)
		assert.NoError(err)
		for _, id := range got["k"] {
			seen[id]++
		}
	}
	for _, id := range all {
		assert.Equal(1, seen[id], "step %d must be selected by exactly one ordinal", id)
	}

	// ok=false disables partitioning: one replica sees everything, which is right for a solo engine.
	r.p.SetPartitionFunc(func() (int, int, bool) { return 2, 0, false })
	got, err := r.p.FetchSteps(ctx, 1, 5, []string{"k"}, 8)
	assert.NoError(err)
	assert.Equal(6, len(got["k"]))

	// A nil func behaves the same as ok=false rather than panicking.
	r.p.SetPartitionFunc(nil)
	got, err = r.p.FetchSteps(ctx, 1, 5, []string{"k"}, 8)
	assert.NoError(err)
	assert.Equal(6, len(got["k"]))
}

// TestPiston_PartitionDoesNotNarrowTheBand pins the deliberate asymmetry in the scan: the residue class
// filters the rows this replica tallies but NOT the MIN(priority) subquery. The band is a cluster-wide
// fact, so a replica whose own slice holds only worse-band work must still report the better band it can
// see - otherwise replicas disagree about which band is open.
func TestPiston_PartitionDoesNotNarrowTheBand(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	best := r.insertStep(t, 1, 1, "urgent", 1) // band 1, will belong to one ordinal only
	r.insertStep(t, 2, 7, "bulk", 1)
	r.insertStep(t, 3, 7, "bulk", 1)

	for ordinal := range 2 {
		r.p.SetPartitionFunc(func() (int, int, bool) { return 2, ordinal, true })
		band, tallies, err := r.p.ScanBand(ctx, 1)
		assert.NoError(err)
		if ordinal == best%2 {
			assert.Equal(1, band, "the ordinal owning the band-1 step tallies it")
			assert.Equal(1, len(tallies))
		} else {
			// This replica owns nothing at band 1, so it tallies zero rows and reports NoBand - which is
			// correct: its own band-7 work must not be served while band 1 is open.
			assert.Equal(pipeline.NoBand, band, "a replica holding nothing at the open band tallies nothing")
			assert.Equal(0, len(tallies))
		}
	}
}

// TestPiston_RunDispatchesAndReportsItsTurns drives the whole loop end to end: real steps in, candidates
// in the cache, and a turn count an owner can see moving.
func TestPiston_RunDispatchesAndReportsItsTurns(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetInterval(5 * time.Millisecond)

	turns, busy, idle := r.p.Liveness()
	assert.Equal(uint64(0), turns, "nothing has turned yet")
	assert.False(busy)
	assert.False(idle)

	want := []int{
		r.insertStep(t, 1, 5, "k", 1),
		r.insertStep(t, 2, 5, "k", 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.p.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n, _, _ := r.p.Liveness(); n > 0 && r.cache.Len() >= len(want) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	assert.Equal(want, drain(r.cache), "the loop pushed both due steps, oldest first")
	turns, _, _ = r.p.Liveness()
	assert.True(turns > 0, "and a turning loop says so")

	// Reading it again reports the same count: the evidence is a counter, not a flag the reader clears. A
	// consuming getter would let any second caller - a metric, a test - silently swallow it and leave a
	// healthy piston looking stalled.
	again, _, _ := r.p.Liveness()
	assert.Equal(turns, again, "looking twice reports the same turns twice")
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
	r.p.SetInterval(30 * time.Second) // would hang if cancellation were ignored

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

// TestPiston_NewValidates pins that a wiring mistake is caught at construction.
// TestPiston_ScanErrSeamDrivesThePipelineErrorPolicy pins the one seam this package consults, and the
// reason it earns its place: the pipeline's scan-error policy - clear this shard from planning, leave its
// cache partition ALONE - is otherwise reachable only by breaking a real database mid-run. The two halves
// are asymmetric on purpose (an error means "unknown", not "nothing is due"), so both are asserted.
func TestPiston_ScanErrSeamDrivesThePipelineErrorPolicy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.insertStep(t, 1, 5, "alpha", 1)

	// A healthy cycle first, so there is a tally to clear and a partition to preserve.
	assert.NoError(r.p.Cycle(ctx).Err)
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

	// And through a whole cycle: the shard clears itself from planning, but its candidates survive.
	seams.Inject(FaultScanErr)
	res := r.p.Cycle(ctx)
	assert.Error(res.Err, "the cycle reports the failure rather than returning it")
	assert.Equal(held, r.cache.Len(), "a FAILED scan must not wholesale-replace a healthy partition")
	assert.Equal(pipeline.NoBand, res.GlobalBand, "the cleared shard no longer claims a band")
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
// so a stale claim on the best band makes every live piston find none of its own keys there and dispatch
// nothing, indefinitely. The cache partition goes for the reason an empty plan clears it: dead hints cost a
// worker a claim round-trip each.
func TestPiston_IdleWithdrawsTheShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.insertStep(t, 1, 5, "alpha", 1)

	assert.NoError(r.p.Cycle(ctx).Err)
	assert.True(r.cache.Len() > 0, "the cycle populated the partition")
	band, _ := r.planner.LastBand()
	assert.Equal(5, band, "and claimed its band in the shared planner")

	r.p.SetIdle(true)
	assert.Equal(0, r.cache.Len(), "idling empties the partition rather than leaving dead hints")

	// The withdrawal is visible to a PEER planning off the same shared planner: with shard 1 cleared, a
	// peer holding only band 9 is now the best band in the fleet and dispatches, instead of deferring
	// forever to a band nobody is serving.
	r.planner.Tally(2, 9, []planner.Tally{{Key: "beta", Weight: 1, Count: 1}})
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
		r.p.SetPartitionFunc(func() (int, int, bool) { return tc.replicas, tc.ordinal, tc.ok })
		band, tallies, err := r.p.ScanBand(ctx, 1)
		assert.NoError(err, "%s must not error the scan", tc.name)
		if assert.Equal(1, len(tallies), "%s must select everything, not nothing", tc.name) {
			assert.Equal(6, tallies[0].Count, "%s: no rows excluded", tc.name)
		}
		assert.Equal(5, band, "%s reports the real band", tc.name)
	}
}

func TestPiston_NewValidates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	assert.Equal(1, r.p.Shard())
	assert.Equal(pipeline.DefaultInterval, func() time.Duration {
		p, _ := New(1, r.db, r.planner, r.cache)
		return p.Interval()
	}(), "a fresh piston inherits the pipeline's paced default")

	_, err := New(0, r.db, r.planner, r.cache)
	assert.Error(err, "shard must be positive")
	_, err = New(1, nil, r.planner, r.cache)
	assert.Error(err, "db is required")
	_, err = New(1, r.db, nil, r.cache)
	assert.Error(err, "planner is required")
	_, err = New(1, r.db, r.planner, nil)
	assert.Error(err, "cache is required")
}

// TestPiston_RecordsItsInstruments pins the four metrics a cycle emits, BY NAME. The names are a public
// surface that dashboards bind to, so this test failing on a rename is the point of it.
func TestPiston_RecordsItsInstruments(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	reader := sdkmetric.NewManualReader()
	assert.NoError(r.p.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")))

	// Seed the cache so the cycle's push has something to discard, exercising that counter too.
	r.cache.Refill(1, []candidatecache.Job{{StepID: 9999, Shard: 1}}, 100)
	r.insertStep(t, 1, 5, "k", 1)

	res := r.p.Cycle(ctx)
	assert.NoError(res.Err)
	r.p.record(ctx, res)

	var rm metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &rm))
	got := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = true
		}
	}
	for _, name := range []string{
		"dwarf_refill_duration_seconds",
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

	r.p.SetInterval(77 * time.Millisecond)
	assert.Equal(77*time.Millisecond, r.p.Interval())
	r.p.SetMinGap(11 * time.Millisecond)
	assert.Equal(11*time.Millisecond, r.p.MinGap())
	r.p.SetLogger(nil)
	assert.NotNil(r.p.logger.Load(), "a nil logger restores the discarding default, never nil")
	assert.NoError(r.p.SetMeter(nil), "a nil meter restores no-op instruments")
}

// TestPiston_FailingCyclesReportNoLiveness pins the distinction the whole dispatch-evidence design rests
// on: a piston whose every cycle FAILS must report itself as not serving, so its owner can stop handing it
// work that nobody would then select.
//
// It is easy to get wrong in a way that looks right. A failing cycle is still briefly inside its queries -
// building the error, recording the phase, logging it - so a busy flag meaning "a cycle is in flight" reads
// true a small but nonzero fraction of the time (measured ~1.2% with a scan that fails instantly), and a
// reader sampling on its own clock catches that within seconds. It then keeps a broken piston looking alive
// for good, which is exactly the stranding the evidence exists to prevent. Busy therefore means a cycle has
// been working LONGER THAN ONE PERIOD, which a failing cycle never is.
func TestPiston_FailingCyclesReportNoLiveness(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	sm := seamster.New(true)
	r.p.SetSeams(sm)
	r.insertStep(t, 1, 5, "k", 1) // rig default interval is 0 - the degenerate case the floor covers

	// A healthy cycle first: it completes, so the turn count is the evidence.
	before, _, _ := r.p.Liveness()
	r.p.Cycle(ctx)
	after, busy, _ := r.p.Liveness()
	assert.Equal(before+1, after, "a completed cycle is a turn")
	assert.False(busy, "and it is not still working")

	// From here every scan fails. Sample hard while the loop runs: the turn count must not move and busy
	// must never once read true.
	sm.InjectN(1<<20, FaultScanErr)
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
	stalled, _, _ := r.p.Liveness()
	var busyEpisodes, busySamples, samples int
	wasBusy := false
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
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
	assert.True(samples > 100, "the sampling has to be dense enough to catch a brief window")
}

// TestPiston_StealIsGatedOnAnEmptyOwnClass pins the fuse. Partitioning hands each replica a disjoint class
// of step ids, which is what stops peers racing for the same rows - but a replica that is SLOW rather than
// dead keeps its class while being unable to serve it, and nobody else will look at those steps. Stealing
// closes that, and the gate is what keeps it from costing anything the rest of the time: a replica holding
// work of its OWN never steals, so a uniformly loaded fleet adds no overlapping selection at all.
func TestPiston_StealIsGatedOnAnEmptyOwnClass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetPartitionFunc(func() (int, int, bool) { return 2, 0, true })

	// Not armed: the strict residue class, whatever the grace is set to.
	sql, args := r.p.partitionPredicate()
	assert.Equal(" AND step_id % ? = ?", sql, "an unarmed piston partitions strictly")
	assert.Equal(2, len(args))

	// Armed by a cycle that found nothing due in this replica's own class.
	r.p.stealing.Store(true)
	sql, args = r.p.partitionPredicate()
	assert.Contains(sql, "OR not_before <=", "an idle replica relaxes the class")
	// Two tiers plus the strict term: own class, then the neighbour's after one grace, then anyone's after
	// two. Six binds - (replicas, ordinal), (replicas, neighbour, -grace), (-2*grace).
	assert.Equal(6, len(args))
	assert.Equal(2, args[0], "the strict term keeps the real divisor")
	assert.Equal(0, args[1], "and this replica's own ordinal")
	assert.Equal(1, args[3], "the second tier names the NEIGHBOUR's ordinal, (0+1) mod 2")
	assert.Equal(2*args[4].(int64), args[5], "and anyone's class only after TWICE the grace")
	// The relaxation only ever ADMITS rows - the strict term survives intact, so no class is stranded.
	assert.Contains(sql, "step_id % ? = ?")

	// Zero periods disables it outright, even while armed.
	r.p.SetStealAfter(0)
	sql, _ = r.p.partitionPredicate()
	assert.Equal(" AND step_id % ? = ?", sql, "stealAfter=0 restores strict partitioning")
	r.p.SetStealAfter(defaultStealAfter)

	// Nothing to steal when there is no partition to relax: a solo replica already selects everything.
	r.p.SetPartitionFunc(func() (int, int, bool) { return 1, 0, true })
	sql, _ = r.p.partitionPredicate()
	assert.Equal("", sql, "stealing must not resurrect a partition the pair disabled")
}

// TestPiston_StealArmsFromTheCycleItSaw pins where the gate comes from: NoBand means nothing was due in
// this replica's own class, which is a property of the DATABASE backlog rather than of anything the piston
// did - so it stays true while a peer is stalled and clears itself the moment this class refills.
func TestPiston_StealArmsFromTheCycleItSaw(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.p.SetPartitionFunc(func() (int, int, bool) { return 2, 0, true })

	// An empty shard: nothing due anywhere, so certainly nothing in our class.
	res := r.p.Cycle(ctx)
	assert.Equal(pipeline.NoBand, res.Band)
	assert.True(r.p.stealing.Load(), "a cycle that found nothing due arms the steal")

	// Enough work arrives in THIS replica's class to FILL its batch, and the next cycle disarms. The bar is
	// the batch, not mere presence: with continuous arrivals a replica keeping up still finds a step or two
	// due on every scan, so "any work at all" would leave the gate shut while a peer's class backed up
	// unboundedly beside it - measured as zero steals against an open-loop bench with a crippled peer.
	// Ordinal 0 of 2 owns the even ids, so 20 inserts yield ~10 own steps against a rig capacity of 8.
	for range 20 {
		r.insertStep(t, 1, 5, "k", 1)
	}
	r.p.Cycle(ctx)
	assert.False(r.p.stealing.Load(), "a replica that can fill its own batch must not steal")

	// A FAILED cycle leaves the flag alone: an error means "unknown", not "nothing is due" - the same
	// distinction the pipeline draws when it clears the shard from planning but spares its cache partition.
	sm := seamster.New(true)
	r.p.SetSeams(sm)
	sm.InjectN(1, FaultScanErr)
	res = r.p.Cycle(ctx)
	assert.Error(res.Err)
	assert.False(r.p.stealing.Load(), "a failed cycle must not arm the steal by accident")
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
	r.p.SetPartitionFunc(func() (int, int, bool) { return 2, 0, true })
	r.p.SetInterval(50 * time.Millisecond)
	r.p.SetMinGap(0)
	r.p.SetStealAfter(4) // grace = 200ms

	for range 8 {
		r.insertStep(t, 1, 5, "k", 1)
	}
	own, foreign := r.idsByResidue(t, 2, 0)
	assert.True(len(own) > 0 && len(foreign) > 0, "the fixture needs both classes populated")

	// Strict: only our own class is visible.
	got, err := r.p.FetchSteps(ctx, 1, 5, []string{"k"}, 100)
	assert.NoError(err)
	assert.Equal(own, got["k"], "strict partitioning sees only this replica's class")

	// Armed, but every step is freshly due - younger than the grace - so a healthy owner's work is left
	// alone. This is the moderate-load case the grace exists for: spare capacity is common in a healthy
	// fleet, and the gate alone would re-enable overlapping selection fleet-wide.
	r.p.stealing.Store(true)
	got, err = r.p.FetchSteps(ctx, 1, 5, []string{"k"}, 100)
	assert.NoError(err)
	assert.Equal(own, got["k"], "a young foreign step belongs to its owner")

	// Age the foreign steps past the grace: a stalled owner's class ages without bound, and only then is it
	// taken. Backdated with the DATABASE clock, never a bound Go time.
	_, err = r.db.ExecContext(ctx,
		"UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -5000) WHERE step_id % 2 = 1")
	assert.NoError(err)
	got, err = r.p.FetchSteps(ctx, 1, 5, []string{"k"}, 100)
	assert.NoError(err)
	assert.Equal(8, len(got["k"]), "a long-due foreign step is taken by an idle peer")
}
