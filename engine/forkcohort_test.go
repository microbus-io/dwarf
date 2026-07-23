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

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestForkCohort_MultiStepBranchCountsBranchesNotMembers pins Fork's cohort recompute against the
// lineage-is-not-a-DAG trap: every step of a per-element sub-pipeline inherits the spawn's lineage_id, so a
// cohort of 3 branches x 2 steps has SIX lineage members. Counting members (which the recompute once did)
// writes cohort_arrivals=5 against cohort_size=3 on the clone - an overshoot the pinned invariant forbids,
// and a fan-in that is already "fully arrived" before the rewound branch has re-run.
//
// The graph is Seed -forEach(cells)-> Cell -> Enrich -> J, one branch failing; the fork rewinds that branch's
// Cell with an override that makes it pass. The single-step branch of the existing fork fixtures is exactly
// the degenerate case (members == branches) where the bug is invisible.
func TestForkCohort_MultiStepBranchCountsBranchesNotMembers(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("ForkCohort")
	g.SetEndpoint("Seed", "fkc/seed")
	g.SetEndpoint("Cell", "fkc/cell")
	g.SetEndpoint("Enrich", "fkc/enrich")
	g.SetEndpoint("J", "fkc/j")
	g.SetFanIn("J")
	g.SetReducer("enriched", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Cell", "cells", "cell")
	g.AddTransitionChain("Cell", "Enrich", "J", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("fkc/g", g)

	proxy.HandleTask("fkc/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("fkc/cell", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("cell") == "b" && !f.GetBool("fixed") {
			return errors.New("cell b is broken")
		}
		return nil
	})
	proxy.HandleTask("fkc/enrich", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("enriched", []any{f.GetString("cell") + "!"})
		return nil
	})
	proxy.HandleTask("fkc/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	originKey, outcome, err := eng.Run(ctx, "fkc/g", map[string]any{"cells": []any{"a", "b", "c"}}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, outcome.Status) // branch "b" failed with no onError

	// Fork at the failed Cell of branch "b", overriding it into success. Its branch (Cell+Enrich) re-runs;
	// the other two branches are cloned already-arrived.
	var failedCell string
	hist, err := eng.History(ctx, originKey)
	if !assert.NoError(err) {
		return
	}
	for _, s := range hist {
		if s.TaskName == "Cell" && s.Status == workflow.StatusFailed {
			failedCell = s.StepKey
		}
	}
	if !assert.NotEqual("", failedCell) {
		return
	}
	forkKey, err := eng.Fork(ctx, failedCell, map[string]any{"fixed": true})
	if !assert.NoError(err) {
		return
	}
	forkOutcome, err := eng.Await(ctx, forkKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)
	// All three branches contributed to the fan-in merge, so the fork recovered the whole cohort. The order
	// is not asserted: fan_out_ordinal is stamped on a branch's FIRST step, so the exit steps of a multi-step
	// branch fold in step_id order, which the clone renumbers.
	enriched, _ := forkOutcome.State["enriched"].([]any)
	got := map[any]bool{}
	for _, v := range enriched {
		got[v] = true
	}
	assert.Equal(map[any]bool{"a!": true, "b!": true, "c!": true}, got)

	_, forkID, _, err := keys.ParseFlowKey(forkKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	var arrivals, size, failures int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT cohort_arrivals, cohort_size, cohort_failures FROM dwarf_steps WHERE flow_id=? AND task_name='Seed'",
		forkID).Scan(&arrivals, &size, &failures))
	assert.Equal(3, size)     // three branches
	assert.Equal(3, arrivals) // 2 cloned-as-arrived + the 1 that re-ran; NOT the 6 lineage members
	assert.Equal(0, failures) // the clone dropped the origin's failure with the branch that re-runs

	assertInvariants(t, eng)
}

// TestForkCohort_RewindMidBranchExcludesWholeBranch is the other half of the branch-vs-member fix: the
// recompute must exclude the whole BRANCH being rewound, not merely the rewind STEP. Forking at Enrich
// rewinds that branch from its second step, so its already-completed Cell is still a kept lineage member -
// and counting members would score that Cell as an arrival of a branch that has not arrived.
func TestForkCohort_RewindMidBranchExcludesWholeBranch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("ForkCohortMid")
	g.SetEndpoint("Seed", "fkm/seed")
	g.SetEndpoint("Cell", "fkm/cell")
	g.SetEndpoint("Enrich", "fkm/enrich")
	g.SetEndpoint("J", "fkm/j")
	g.SetFanIn("J")
	g.SetReducer("enriched", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Cell", "cells", "cell")
	g.AddTransitionChain("Cell", "Enrich", "J", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("fkm/g", g)

	proxy.HandleTask("fkm/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("fkm/cell", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("fkm/enrich", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("enriched", []any{f.GetString("cell") + f.GetString("suffix")})
		return nil
	})
	proxy.HandleTask("fkm/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	originKey, outcome, err := eng.Run(ctx, "fkm/g",
		map[string]any{"cells": []any{"a", "b"}, "suffix": "!"}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Fork at one branch's Enrich - a mid-branch rewind whose own Cell stays cloned as a kept member.
	var enrichKey string
	hist, err := eng.History(ctx, originKey)
	if !assert.NoError(err) {
		return
	}
	for _, s := range hist {
		if s.TaskName == "Enrich" {
			enrichKey = s.StepKey
			break
		}
	}
	if !assert.NotEqual("", enrichKey) {
		return
	}
	forkKey, err := eng.Fork(ctx, enrichKey, map[string]any{"suffix": "?"})
	if !assert.NoError(err) {
		return
	}
	forkOutcome, err := eng.Await(ctx, forkKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)

	_, forkID, _, err := keys.ParseFlowKey(forkKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	var arrivals, size int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT cohort_arrivals, cohort_size FROM dwarf_steps WHERE flow_id=? AND task_name='Seed'",
		forkID).Scan(&arrivals, &size))
	assert.Equal(2, size)
	assert.Equal(2, arrivals) // 1 cloned-as-arrived + the rewound branch re-arriving; its kept Cell is not an arrival

	assertInvariants(t, eng)
}

// nestedForkGraph builds Seed -forEach(cells)-> Cell -forEach(chunks)-> Chunk -> JoinChunk -> JoinCell, the
// shape where a fan-out branch contains a fan-out of its own. It is the shape the cohort recompute was blind
// to: a NESTED fan-out re-lineages its children (childLineageID = the inner spawn), so the Chunk steps carry
// lineage_id = Cell, not Seed - while JoinChunk, inserted with the inner spawn's OWN lineage, carries Seed and
// is reachable only THROUGH those Chunks (its predecessor is the inner cohort's last completer). A recompute
// that filtered children on `lineage_id == spawn` therefore dead-ended the outer walk at the inner spawn and
// never saw the rest of the branch.
func nestedForkGraph(t *testing.T, proxy *TestProxy, brokenChunk string) *workflow.Graph {
	t.Helper()
	g := workflow.NewGraph("NestedFork")
	for _, n := range []string{"Seed", "Cell", "Chunk", "JoinChunk", "JoinCell"} {
		g.SetEndpoint(n, "nfk/"+n)
	}
	g.SetFanIn("JoinChunk")
	g.SetFanIn("JoinCell")
	g.SetReducer("done", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Cell", "cells", "cell")
	g.AddTransitionForEach("Cell", "Chunk", "chunks", "chunk")
	g.AddTransitionChain("Chunk", "JoinChunk")
	g.AddTransitionChain("JoinChunk", "JoinCell", workflow.END)
	testarossa.For(t).NoError(g.Validate())
	proxy.HandleGraph("nfk/g", g)

	proxy.HandleTask("nfk/Seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("nfk/Cell", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("chunks", []any{f.GetString("cell") + "1", f.GetString("cell") + "2"})
		return nil
	})
	proxy.HandleTask("nfk/Chunk", func(ctx context.Context, f *workflow.Flow) error {
		if brokenChunk != "" && f.GetString("chunk") == brokenChunk && !f.GetBool("fixed") {
			return errors.New("chunk %s is broken", brokenChunk)
		}
		f.Set("done", []any{f.GetString("chunk")})
		return nil
	})
	proxy.HandleTask("nfk/JoinChunk", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("nfk/JoinCell", func(ctx context.Context, f *workflow.Flow) error { return nil })
	return g
}

// TestForkCohort_NestedRewindExcludesTheWholeOuterBranch pins the first consequence of the dead-end: a rewind at
// or past the INNER frame was invisible to the outer walk, so the outer branch was counted as ARRIVED. The clone
// was written cohort_arrivals == cohort_size, and when the re-run branch actually re-arrived the bump pushed
// arrivals PAST size - the very `cohort_arrivals <= cohort_size` invariant the branch-counting fix exists to
// hold, breaking one nesting level deeper.
func TestForkCohort_NestedRewindExcludesTheWholeOuterBranch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	nestedForkGraph(t, proxy, "")

	eng := NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	originKey, outcome, err := eng.Run(ctx, "nfk/g", map[string]any{"cells": []any{"a", "b", "c"}}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Fork at a Chunk - a step INSIDE the inner cohort, i.e. past the inner frame from the outer cohort's
	// point of view. The outer walk must still recognize that its whole Cell branch is being rewound.
	var chunkKey string
	hist, err := eng.History(ctx, originKey)
	if !assert.NoError(err) {
		return
	}
	for _, s := range hist {
		if s.TaskName == "Chunk" && chunkKey == "" {
			chunkKey = s.StepKey
		}
	}
	if !assert.NotEqual("", chunkKey) {
		return
	}
	forkKey, err := eng.Fork(ctx, chunkKey, nil)
	if !assert.NoError(err) {
		return
	}
	forkOutcome, err := eng.Await(ctx, forkKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)
	// The counter must not overshoot. Counting the rewound outer branch as already-arrived wrote
	// arrivals == size on the clone, and the re-run branch's real arrival then pushed it to size+1.
	assertInvariants(t, eng)
}

// TestForkCohort_NestedFailureInAKeptBranchStillFailsTheFork pins the worse consequence, and the one that
// inverts Fork's contract. A branch failure lives INSIDE a kept branch's inner cohort, so the dead-ended outer
// walk never saw it: the clone was written cohort_failures = 0 where the origin had 1, the outer cohort then
// resolved with zero failures, and the fork COMPLETED - silently reporting success for a flow whose failed
// branch was never recovered, with that branch's output missing from the merge.
func TestForkCohort_NestedFailureInAKeptBranchStillFailsTheFork(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	nestedForkGraph(t, proxy, "b1") // a chunk of the "b" cell fails, with no onError

	eng := NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	originKey, outcome, err := eng.Run(ctx, "nfk/g", map[string]any{"cells": []any{"a", "b", "c"}}, nil)
	if !assert.NoError(err) {
		return
	}
	// The inner cohort of cell "b" resolves with a failure, which propagates up to the outer cohort
	// (propagateCohortFailure walks the lineage chain), failing the flow.
	assert.Equal(workflow.StatusFailed, outcome.Status)

	// Fork at a chunk of a DIFFERENT cell ("a"), so cell "b" - the one carrying the failure, deep inside its
	// own inner cohort - is CLONED AS-IS. Its failure must survive the recompute.
	var otherChunk string
	hist, err := eng.History(ctx, originKey)
	if !assert.NoError(err) {
		return
	}
	for _, s := range hist {
		if s.TaskName == "Chunk" && s.Status == workflow.StatusCompleted && otherChunk == "" {
			step, serr := eng.Step(ctx, s.StepKey)
			if serr == nil {
				if c, _ := step.State["chunk"].(string); c == "a1" {
					otherChunk = s.StepKey
				}
			}
		}
	}
	if !assert.NotEqual("", otherChunk) {
		return
	}
	forkKey, err := eng.Fork(ctx, otherChunk, nil)
	if !assert.NoError(err) {
		return
	}
	forkOutcome, err := eng.Await(ctx, forkKey)
	if !assert.NoError(err) {
		return
	}
	// Cell "b" is still broken in the clone, so the fork must RE-FAIL. Completing here would mean the fork
	// silently absorbed an unrecovered failure - which is exactly what the partial-recovery fork exists to
	// make impossible (fix one failed branch at a time; the fork re-fails cleanly until every one is fixed).
	assert.Equal(workflow.StatusFailed, forkOutcome.Status,
		"a failure inside a KEPT branch's nested cohort must survive the clone and re-fail the fork")
	assertInvariants(t, eng)
}
