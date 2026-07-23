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

package fixtures

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// These fixtures CHARACTERIZE how the engine treats multiple transitions that point at the same target node -
// in particular several `when` edges to one node, and edges into a fan-in. They are not regression pins for a
// fix; they document that these shapes behave exactly as the `when` fan-out contract specifies, and that the
// mis-authored variants are rejected by Validate rather than silently mis-executed.
//
// Background: a `when` transition is an INDEPENDENT parallel branch (see AddTransitionWhen's godoc), so two
// `when` edges from one node are a conditional fan-out - if both conditions hold at runtime, BOTH branches run.
// Pointing them at the same target therefore runs that target more than once, on purpose. This looks alarming
// ("the reducer double-counts!") but is the contract doing what it was asked; whether two clauses co-match is a
// property of the runtime state, not knowable at authoring time. The tests below sweep that co-matching to show
// the branch width tracks exactly how many clauses match, with no hidden miscount of the cohort.

// TestDuplicateEdge_WhenFanOutWidthTracksMatchingClauses runs a fan-out source whose TWO `when` edges point at
// the same non-fan-in target under overlapping conditions:
//
//	Split -Work when gate==1-> Work -> Join
//	Split -Work when gate>0 -> Work -> Join   (Join is the fan-in; "total" folds with `add`)
//
// The number of Work branches equals the number of clauses that match the input `gate`, and the `add` reducer
// sums exactly that many deltas. Nothing overshoots cohort_size: a cohort of width W has W arrivals. This is
// the shape once suspected of "double-counting cohort_arrivals" (since retired) - it does not; it
// is an ordinary conditional fan-out whose width happens to be author-surprising.
func TestDuplicateEdge_WhenFanOutWidthTracksMatchingClauses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	var workRuns atomic.Int32

	graph := workflow.NewGraph("WhenFanOutWidth")
	graph.SetEndpoint("Split", "duplicateedgefaninflow.verify:701/split")
	graph.SetEndpoint("Work", "duplicateedgefaninflow.verify:701/work")
	graph.SetEndpoint("Join", "duplicateedgefaninflow.verify:701/join")
	graph.SetFanIn("Join")
	graph.SetReducer("total", workflow.ReducerAdd)
	graph.AddTransitionWhen("Split", "Work", "gate == 1")
	graph.AddTransitionWhen("Split", "Work", "gate > 0")
	graph.AddTransitionChain("Work", "Join", workflow.END)

	// Validate has no duplicate-transition check, and it should not: two `when` edges to one target under
	// DIFFERENT guards is a legitimate conditional fan-out, not an error.
	testarossa.For(t).NoError(graph.Validate())
	proxy.HandleGraph("duplicateedgefaninflow.verify:701/when-fan-out", graph)

	proxy.HandleTask("duplicateedgefaninflow.verify:701/split", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("duplicateedgefaninflow.verify:701/work", func(ctx context.Context, f *workflow.Flow) error {
		workRuns.Add(1)
		f.SetInt("total", 1) // reducer delta: one per branch
		return nil
	})
	proxy.HandleTask("duplicateedgefaninflow.verify:701/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	cases := []struct {
		name         string
		gate         int
		wantWorkRuns int32
		wantTotal    any // nil == field absent (empty cohort routed straight to the fan-in)
	}{
		// Both clauses true -> width 2. Two Work branches, `add` sums two deltas.
		{"both_clauses_match", 1, 2, 2.0},
		// Only `gate > 0` is true (gate != 1) -> width 1. One branch, one delta.
		{"one_clause_matches", 5, 1, 1.0},
		// Neither clause true -> width 0. The fan-out source has an empty cohort, so it routes straight to the
		// fan-in on its own state (no branch ran, so "total" was never written).
		{"no_clause_matches", 0, 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)
			workRuns.Store(0)

			_, outcome, err := eng.Run(ctx, "duplicateedgefaninflow.verify:701/when-fan-out",
				map[string]any{"gate": tc.gate}, nil)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)

			t.Logf("gate=%d workRuns=%d total=%v", tc.gate, workRuns.Load(), outcome.State["total"])
			assert.Equal(tc.wantWorkRuns, workRuns.Load(), "Work runs once per matching `when` clause")
			if tc.wantTotal == nil {
				assert.Nil(outcome.State["total"], "no branch ran, so the reducer produced nothing")
			} else {
				assert.Equal(tc.wantTotal, outcome.State["total"], "`add` sums exactly one delta per branch")
			}
		})
	}
}

// TestDuplicateEdge_CohortMemberOneRejectedByValidate characterizes the shape once thought to double-count a
// cohort's arrivals: a fan-out cohort (Split -> {WorkA, WorkB} -> Join) where ONE member (WorkA) has two edges
// into the shared fan-in. It is UNREACHABLE - Validate rejects it. WorkA's two edges make it a fan-out source,
// and a fan-out source reaching a fan-in keeps its incoming lineage frame (it maps its own fan-out onto Join),
// so Join is reached carrying stack [Split] via WorkA. WorkB is a plain member, so it pops the frame and
// reaches Join with []. One node, two lineage stacks -> rejected. There is no runtime miscount because the
// graph never runs.
func TestDuplicateEdge_CohortMemberOneRejectedByValidate(t *testing.T) {
	t.Parallel()
	graph := workflow.NewGraph("DupMemberOne")
	graph.SetEndpoint("Split", "duplicateedgefaninflow.verify:701/m-split")
	graph.SetEndpoint("WorkA", "duplicateedgefaninflow.verify:701/m-worka")
	graph.SetEndpoint("WorkB", "duplicateedgefaninflow.verify:701/m-workb")
	graph.SetEndpoint("Join", "duplicateedgefaninflow.verify:701/m-join")
	graph.SetFanIn("Join")
	graph.AddTransitionFanOut("Split", "WorkA", "WorkB")
	graph.AddTransitionWhen("WorkA", "Join", "gate == 1")
	graph.AddTransitionWhen("WorkA", "Join", "gate > 0")
	graph.AddTransition("WorkB", "Join")
	graph.AddTransitionChain("Join", workflow.END)

	err := graph.Validate()
	t.Logf("Validate() = %v", err)
	testarossa.For(t).Error(err, "a cohort mixing a plain member with a fan-out-source member into one fan-in must not validate")
}

// TestDuplicateEdge_CohortMemberAllRejectedByValidate is the sibling shape where EVERY member has two edges into
// the fan-in. Now the lineage stacks agree (both members keep [Split]), but Split's own fan-out frame is then
// never popped, so Validate rejects it the OTHER way: a branch reaches END with an unpopped [Split] frame. So
// the member-double-count shape is unreachable regardless of whether one or all members carry the extra edge.
func TestDuplicateEdge_CohortMemberAllRejectedByValidate(t *testing.T) {
	t.Parallel()
	graph := workflow.NewGraph("DupMemberAll")
	graph.SetEndpoint("Split", "duplicateedgefaninflow.verify:701/a-split")
	graph.SetEndpoint("WorkA", "duplicateedgefaninflow.verify:701/a-worka")
	graph.SetEndpoint("WorkB", "duplicateedgefaninflow.verify:701/a-workb")
	graph.SetEndpoint("Join", "duplicateedgefaninflow.verify:701/a-join")
	graph.SetFanIn("Join")
	graph.AddTransitionFanOut("Split", "WorkA", "WorkB")
	graph.AddTransitionWhen("WorkA", "Join", "gate == 1")
	graph.AddTransitionWhen("WorkA", "Join", "gate > 0")
	graph.AddTransitionWhen("WorkB", "Join", "gate == 1")
	graph.AddTransitionWhen("WorkB", "Join", "gate > 0")
	graph.AddTransitionChain("Join", workflow.END)

	err := graph.Validate()
	t.Logf("Validate() = %v", err)
	testarossa.For(t).Error(err, "a cohort whose every member is itself a fan-out source into the shared fan-in must not validate")
}

// TestDuplicateEdge_TrunkSourceIntoFanInIsDegenerate is the one member-shaped graph that DOES validate: a trunk
// fan-out source whose two overlapping `when` edges go straight into the fan-in (no outer cohort - the source
// is the entry, lineage 0). Because both edges target the fan-in, the source opens its OWN cohort whose
// cohort_size and arrival count are BOTH derived from the same matched-edge slice, so they agree by
// construction and the pinned cohort_arrivals <= cohort_size invariant is never breached. The cohort has zero
// member steps (both "children" are the fan-in itself), so the fan-in fires once on the source's own state.
// Self-consistent - this is what a "member with two edges to its fan-in" collapses to once it validates.
func TestDuplicateEdge_TrunkSourceIntoFanInIsDegenerate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	var joinRuns atomic.Int32

	graph := workflow.NewGraph("DupTrunkSource")
	graph.SetEndpoint("Work", "duplicateedgefaninflow.verify:701/t-work")
	graph.SetEndpoint("Join", "duplicateedgefaninflow.verify:701/t-join")
	graph.SetEntryPoint("Work")
	graph.SetFanIn("Join")
	graph.SetReducer("total", workflow.ReducerAdd)
	graph.AddTransitionWhen("Work", "Join", "gate == 1")
	graph.AddTransitionWhen("Work", "Join", "gate > 0")
	graph.AddTransitionChain("Join", workflow.END)

	assert := testarossa.For(t)
	assert.NoError(graph.Validate(), "a trunk fan-out source converging directly on its fan-in validates")
	proxy.HandleGraph("duplicateedgefaninflow.verify:701/dup-trunk", graph)

	proxy.HandleTask("duplicateedgefaninflow.verify:701/t-work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("total", 5) // a would-be reducer delta from the source itself
		return nil
	})
	proxy.HandleTask("duplicateedgefaninflow.verify:701/t-join", func(ctx context.Context, f *workflow.Flow) error {
		joinRuns.Add(1)
		return nil
	})

	_, outcome, err := eng.Run(ctx, "duplicateedgefaninflow.verify:701/dup-trunk",
		map[string]any{"gate": 1}, nil)
	assert.NoError(err)
	t.Logf("status=%v joinRuns=%d total=%v", outcome.Status, joinRuns.Load(), outcome.State["total"])
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal(int32(1), joinRuns.Load(), "the fan-in fires exactly once despite the duplicate edge")
}
