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

// defaultTestEngineID is the replica identity every rig is built with; the registry is keyed on it.
const defaultTestEngineID = 4242

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
	p, err := New(defaultTestEngineID, 1, db, pl, cache)
	if err != nil {
		t.Fatal(err)
	}
	p.SetInterval(0)
	p.SetMinGap(0)
	r := &rig{p: p, db: db, planner: pl, cache: cache}
	r.register(t)
	return r
}

// register creates this replica's registry row, which in production the OWNER does once at startup
// (before any piston runs). The beat only ever refreshes it, so without this every beat is a no-op.
func (r *rig) register(t *testing.T) {
	t.Helper()
	_, err := r.db.ExecContext(context.Background(),
		"INSERT INTO dwarf_peers (engine_id, seen_at, dispatches) VALUES (?, NOW_UTC(), 1)",
		defaultTestEngineID)
	if err != nil {
		t.Fatal(err)
	}
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

// peerRow is one registry row as a test sees it. DispatchedAgeMs is computed in SQL against the database
// clock - never round-tripped through Go - so a "fresh" check means the same thing here as in the engine.
type peerRow struct {
	EngineID        int64
	Dispatches      int
	SeenAgeMs       float64
	DispatchedAgeMs float64
}

func (r *rig) peerRows(t *testing.T) []peerRow {
	t.Helper()
	rows, err := r.db.QueryContext(context.Background(),
		"SELECT engine_id, dispatches, DATE_DIFF_MILLIS(NOW_UTC(), seen_at),"+
			" DATE_DIFF_MILLIS(NOW_UTC(), dispatched_at) FROM dwarf_peers ORDER BY engine_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []peerRow
	for rows.Next() {
		var e peerRow
		if err := rows.Scan(&e.EngineID, &e.Dispatches, &e.SeenAgeMs, &e.DispatchedAgeMs); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

// staleDispatch is any age old enough that no live piston could have produced it - the default sits
// decades back, so a generous threshold still separates "never" from "just now" unambiguously.
const staleDispatch = 24 * 60 * 60 * 1000.0

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

// TestPiston_HeartbeatInsertsThenUpdates pins the dialect-agnostic upsert: the first beat INSERTs, later
// beats UPDATE the same row rather than accumulating duplicates.
func TestPiston_HeartbeatUpdatesInPlace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	r.p.beat(ctx)
	rows := r.peerRows(t)
	assert.Equal(1, len(rows))
	assert.Equal(int64(defaultTestEngineID), rows[0].EngineID)
	assert.Equal(1, rows[0].Dispatches, "a dispatching piston publishes dispatches=1")

	time.Sleep(2 * time.Millisecond) // NOW_UTC() is millisecond-precision; ensure seen_at changes
	r.p.beat(ctx)
	assert.Equal(1, len(r.peerRows(t)), "the second beat updates in place")

	// A beat with no row to update is a NO-OP, never an insert. That is what lets the owner delete the row
	// at shutdown without having to prove every piston stopped beating first - a straggler beat simply
	// matches nothing instead of resurrecting the replica in a registry it has just left.
	_, err := r.db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=?", defaultTestEngineID)
	if err != nil {
		t.Fatal(err)
	}
	r.p.beat(ctx)
	assert.Equal(0, len(r.peerRows(t)), "a beat must never re-create a deleted row")
}

// TestPiston_DispatchedAtNeedsEvidence pins the distinction between the two timestamps. seen_at says the
// replica is alive and holding connections - what R counts. dispatched_at says it is genuinely serving -
// what the candidate partition divides across - and it moves ONLY after a cycle actually completed. A
// piston that beats without ever cycling must not look like a dispatcher, because a replica handed a
// residue class it never selects strands those steps.
func TestPiston_DispatchedAtNeedsEvidence(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	// Beating with no cycle behind it: alive, but no evidence of dispatch.
	r.p.beat(ctx)
	row := r.peerRows(t)[0]
	assert.True(row.SeenAgeMs < staleDispatch, "seen_at is fresh from the first beat")
	assert.True(row.DispatchedAgeMs > staleDispatch,
		"dispatched_at keeps its never-dispatched default (age %.0fms)", row.DispatchedAgeMs)

	// A completed cycle is the evidence; the next beat publishes it.
	res := r.p.Cycle(ctx)
	assert.NoError(res.Err)

	time.Sleep(2 * time.Millisecond)
	r.p.beat(ctx)
	row = r.peerRows(t)[0]
	assert.True(row.DispatchedAgeMs < staleDispatch, "a completed cycle advances dispatched_at")

	// And it is consumed, not sticky forever: a beat with no cycle since leaves it where it was.
	before := row.DispatchedAgeMs
	time.Sleep(5 * time.Millisecond)
	r.p.beat(ctx)
	assert.True(r.peerRows(t)[0].DispatchedAgeMs > before,
		"with no cycle since the last beat, dispatched_at stops advancing and ages")
}

// TestPiston_IdleNeverAdvancesDispatchedAt pins how the two populations separate with nothing trusted:
// an idle piston runs no cycle, so its dispatched_at simply never moves. No flag has to be believed.
// NOT parallel: it shortens the package-level heartbeatInterval, which every other test reads.
func TestPiston_IdleNeverAdvancesDispatchedAt(t *testing.T) {
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetIdle(true)
	r.insertStep(t, 1, 5, "k", 1)

	restore := heartbeatInterval
	heartbeatInterval = 5 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = restore })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.p.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(r.peerRows(t)) == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond) // several more beats
	cancel()
	<-done

	row := r.peerRows(t)[0]
	assert.True(row.SeenAgeMs < staleDispatch, "idling keeps the replica alive in R")
	assert.True(row.DispatchedAgeMs > staleDispatch,
		"but never claims to dispatch, however many times it beats (age %.0fms)", row.DispatchedAgeMs)
}

// TestPiston_IdlePublishesDispatchesZero pins that idling rides every beat rather than only the insert, so
// a piston that starts or stops idling republishes it immediately instead of waiting to be pruned. The
// column and the behaviour come from one setter, so they cannot disagree - and a replica counted as a
// dispatcher while claiming nothing would be handed a residue class it strands.
func TestPiston_IdlePublishesDispatchesZero(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	r.p.beat(ctx)
	assert.Equal(1, r.peerRows(t)[0].Dispatches)

	r.p.SetIdle(true)
	assert.True(r.p.Idle())
	time.Sleep(2 * time.Millisecond)
	r.p.beat(ctx)
	assert.Equal(0, r.peerRows(t)[0].Dispatches, "idling republishes on the next beat")

	r.p.SetIdle(false)
	time.Sleep(2 * time.Millisecond)
	r.p.beat(ctx)
	assert.Equal(1, r.peerRows(t)[0].Dispatches, "and so does leaving idle")
}

// TestPiston_RunDispatchesAndBeats drives the whole loop end to end: real steps in, candidates in the
// cache, and a registry row written.
// NOT parallel: it shortens the package-level heartbeatInterval, which every other test reads.
func TestPiston_RunDispatchesAndBeats(t *testing.T) {
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetInterval(5 * time.Millisecond)

	// The beat runs on its OWN loop now, so the evidence of dispatch is published by whichever beat follows
	// the first successful cycle - not by the cycle itself. The very first beat fires before any cycle has
	// run and correctly reports no dispatch, so the interval has to be short enough for a second one to land
	// inside the test.
	restore := heartbeatInterval
	heartbeatInterval = 5 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = restore })

	want := []int{
		r.insertStep(t, 1, 5, "k", 1),
		r.insertStep(t, 2, 5, "k", 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.p.Run(ctx) }()

	// Wait for the real end state - candidates pushed AND the dispatch evidence published - rather than for
	// a peer row merely existing, which the pre-cycle beat satisfies on its own.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows := r.peerRows(t)
		if r.cache.Len() >= len(want) && len(rows) > 0 && rows[0].DispatchedAgeMs < staleDispatch {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	assert.Equal(want, drain(r.cache), "the loop pushed both due steps, oldest first")
	rows := r.peerRows(t)
	assert.Equal(1, len(rows))
	assert.Equal(int64(defaultTestEngineID), rows[0].EngineID)
	assert.Equal(1, rows[0].Dispatches)
	assert.True(rows[0].SeenAgeMs < staleDispatch)
	assert.True(rows[0].DispatchedAgeMs < staleDispatch,
		"a loop that really dispatched publishes the evidence for it (age %.0fms)", rows[0].DispatchedAgeMs)
}

// TestPiston_RunIdleBeatsWithoutDispatching pins the idle mode over the real loop: it keeps the registry
// row fresh - so the replica goes on dividing the pools - while selecting nothing at all.
// NOT parallel: it shortens the package-level heartbeatInterval, which every other test reads.
func TestPiston_RunIdleBeatsWithoutDispatching(t *testing.T) {
	assert := testarossa.For(t)
	r := newRig(t)
	r.p.SetIdle(true)
	r.insertStep(t, 1, 5, "k", 1)

	// Shortened so the test observes several beats without waiting out real seconds.
	restore := heartbeatInterval
	heartbeatInterval = 5 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = restore })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.p.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(r.peerRows(t)) == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond) // several more beats
	cancel()
	<-done

	rows := r.peerRows(t)
	assert.Equal(1, len(rows), "beats update one row, they do not accumulate")
	assert.Equal(int64(defaultTestEngineID), rows[0].EngineID)
	assert.Equal(0, rows[0].Dispatches)
	assert.Equal(0, r.cache.Len(), "an idle piston selects nothing, however much is due")
	band, _ := r.planner.LastBand()
	assert.Equal(-1, band, "and never touches the planner, so nothing was ever planned")
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
	assert.Equal(int64(defaultTestEngineID), r.p.EngineID())
	assert.Equal(pipeline.DefaultInterval, func() time.Duration {
		p, _ := New(7, 1, r.db, r.planner, r.cache)
		return p.Interval()
	}(), "a fresh piston inherits the pipeline's paced default")

	_, err := New(0, 1, r.db, r.planner, r.cache)
	assert.Error(err, "engine id 0 is the registry's colliding primary key, not a default")
	_, err = New(-1, 1, r.db, r.planner, r.cache)
	assert.Error(err, "engine id must be positive")
	_, err = New(7, 0, r.db, r.planner, r.cache)
	assert.Error(err, "shard must be positive")
	_, err = New(7, 1, nil, r.planner, r.cache)
	assert.Error(err, "db is required")
	_, err = New(7, 1, r.db, nil, r.cache)
	assert.Error(err, "planner is required")
	_, err = New(7, 1, r.db, r.planner, nil)
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
