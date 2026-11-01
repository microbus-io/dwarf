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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestWideflow drives the operations whose statements list a cohort's steps or a subgraph tree's flows past
// SQL Server's 2,100-parameter ceiling, the tightest of the four dialects. Each list goes out in chunks; the
// widths here exceed what one statement could bind there, so this fixture is the regression test on the SQL
// Server leg of the dialect matrix, and on every dialect it spans several chunks.
func TestWideflow(t *testing.T) {
	t.Parallel()

	t.Run("cohort_past_the_ceiling", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		ctx := context.Background()
		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		startWideflow(t, eng)

		// The resolving arrival links every exit step to the fan-in by id: 2,200 ids.
		const width = 2200
		flowKey, out, err := eng.Run(ctx, "wideflow.verify:428/cohort", wideflowItems(width), nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
			return
		}
		assert.Equal(float64(width), stateVal(out.State, "done"))

		// Forking at the spawn walks its DAG subtree, whose first frontier is the whole cohort.
		history, err := eng.History(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		var seedKey string
		for _, step := range history {
			if step.TaskName == "Seed" {
				seedKey = step.StepKey
			}
		}
		forkKey, err := eng.Fork(ctx, seedKey, nil)
		if !assert.NoError(err) {
			return
		}
		out, err = eng.Await(ctx, forkKey)
		if assert.NoError(err) && assert.Equal(workflow.StatusCompleted, out.Status) {
			assert.Equal(float64(width), stateVal(out.State, "done"))
		}
	})

	t.Run("subgraph_tree_past_the_ceiling", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		ctx := context.Background()
		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		startWideflow(t, eng)

		// Root -> Top's child (the tree) -> one Pause flow per item. The tree's flow has 2,200 descendants, so
		// its fingerprint lists them; Cancel and Terminate list every flow of the root's tree.
		const width = 2200
		flowKey, err := eng.Create(ctx, "wideflow.verify:428/outer", wideflowItems(width), nil)
		if !assert.NoError(err) {
			return
		}
		// The root rests interrupted at the first leaf's interrupt, so wait for every one of them.
		awaitPropagatedInterrupts(t, eng, flowKey, width)
		trees, err := eng.List(ctx, workflow.Query{WorkflowURL: "wideflow.verify:428/tree", IncludeSubgraphs: true})
		if !assert.NoError(err) || !assert.Len(trees, 1) {
			return
		}

		for _, key := range []string{flowKey, trees[0].FlowKey} {
			fp1, _, err := eng.Fingerprint(ctx, key)
			assert.NoError(err)
			fp2, _, err := eng.Fingerprint(ctx, key)
			assert.NoError(err)
			assert.Equal(fp1, fp2)
		}

		// Cancel marks every live flow of the tree. Its interrupted steps consume the mark only on resume,
		// so the tree stays interrupted here.
		assert.NoError(eng.Cancel(ctx, flowKey, "too wide"))

		assert.NoError(eng.Terminate(ctx, flowKey, "too wide"))
		out, err := eng.Await(ctx, flowKey)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusTerminated, out.Status)
		}
		terminated, err := eng.List(ctx, workflow.Query{WorkflowURL: "wideflow.verify:428/pause", IncludeSubgraphs: true, Status: workflow.StatusTerminated, Limit: 2 * width})
		if assert.NoError(err) {
			assert.Len(terminated, width)
		}
	})

	t.Run("retry_reaps_a_tree_past_the_ceiling", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		ctx := context.Background()
		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		startWideflow(t, eng)

		// Top's retry reaps its child and the 2,200 leaves under it, listing them level by level.
		const width = 2200
		_, out, err := eng.Run(ctx, "wideflow.verify:428/retry", wideflowItems(width), nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
			return
		}
		for _, url := range []string{"wideflow.verify:428/donetree", "wideflow.verify:428/leaf"} {
			left, err := eng.List(ctx, workflow.Query{WorkflowURL: url, IncludeSubgraphs: true, Limit: 2 * width})
			if assert.NoError(err) {
				assert.Len(left, 0, "%s flows left after the retry", url)
			}
		}
	})
}

// startWideflow registers the fixture's graphs and tasks on a fresh mux and starts eng on it. Each subtest owns
// its own engine, so the three widths run in parallel rather than one after another on a shared engine.
func startWideflow(t *testing.T, eng *engine.Engine) {
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	eng.SetHost(mux)

	// Seed fans out over items to Work, converging at Join.
	cohort := workflow.NewGraph("WideCohort")
	cohort.SetURL("Seed", "wideflow.verify:428/seed")
	cohort.SetURL("Work", "wideflow.verify:428/work")
	cohort.SetURL("Join", "wideflow.verify:428/join")
	cohort.SetFanIn("Join")
	cohort.SetReducer("done", workflow.ReducerAdd)
	cohort.AddTransitionForEach("Seed", "Work", "items", "item")
	cohort.AddTransitionChain("Work", "Join", workflow.End)
	mux.HandleGraph("wideflow.verify:428/cohort", cohort)

	// Seed fans out over items to Call, each calling a subgraph whose only task interrupts.
	tree := workflow.NewGraph("WideTree")
	tree.SetURL("Seed", "wideflow.verify:428/seed")
	tree.SetURL("Call", "wideflow.verify:428/call")
	tree.SetURL("Join", "wideflow.verify:428/join")
	tree.SetFanIn("Join")
	tree.AddTransitionForEach("Seed", "Call", "items", "item")
	tree.AddTransitionChain("Call", "Join", workflow.End)
	mux.HandleGraph("wideflow.verify:428/tree", tree)

	pause := workflow.NewGraph("Pause")
	pause.SetURL("Wait", "wideflow.verify:428/wait")
	pause.AddTransition("Wait", workflow.End)
	mux.HandleGraph("wideflow.verify:428/pause", pause)

	// The same tree with a leaf that completes.
	doneTree := workflow.NewGraph("WideDoneTree")
	doneTree.SetURL("Seed", "wideflow.verify:428/seed")
	doneTree.SetURL("Call", "wideflow.verify:428/calldone")
	doneTree.SetURL("Join", "wideflow.verify:428/join")
	doneTree.SetFanIn("Join")
	doneTree.AddTransitionForEach("Seed", "Call", "items", "item")
	doneTree.AddTransitionChain("Call", "Join", workflow.End)
	mux.HandleGraph("wideflow.verify:428/donetree", doneTree)

	leaf := workflow.NewGraph("Leaf")
	leaf.SetURL("Leaf", "wideflow.verify:428/seed")
	leaf.AddTransition("Leaf", workflow.End)
	mux.HandleGraph("wideflow.verify:428/leaf", leaf)

	// Top calls a tree as a subgraph, so the tree's flow is a mid-tree flow with every Call's child beneath it.
	outer := workflow.NewGraph("WideOuter")
	outer.SetURL("Top", "wideflow.verify:428/top")
	outer.AddTransition("Top", workflow.End)
	mux.HandleGraph("wideflow.verify:428/outer", outer)

	// Top calls the completing tree, then retries once, which reaps that child and the whole tree under it.
	retry := workflow.NewGraph("WideRetry")
	retry.SetURL("Top", "wideflow.verify:428/topretry")
	retry.AddTransition("Top", workflow.End)
	mux.HandleGraph("wideflow.verify:428/retry", retry)

	noop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	mux.HandleTask("wideflow.verify:428/seed", noop)
	mux.HandleTask("wideflow.verify:428/join", noop)
	mux.HandleTask("wideflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("done", 1)
		return nil
	})
	mux.HandleTask("wideflow.verify:428/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("wideflow.verify:428/pause", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("wideflow.verify:428/calldone", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("wideflow.verify:428/leaf", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("wideflow.verify:428/top", func(ctx context.Context, f *workflow.Flow) error {
		var ids []int
		if _, err := f.Get("items", &ids); err != nil {
			return err
		}
		yield, err := f.Subgraph("wideflow.verify:428/tree", map[string]any{"items": ids}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("wideflow.verify:428/topretry", func(ctx context.Context, f *workflow.Flow) error {
		if f.Attempt() > 0 {
			return nil
		}
		var ids []int
		if _, err := f.Get("items", &ids); err != nil {
			return err
		}
		yield, err := f.Subgraph("wideflow.verify:428/donetree", map[string]any{"items": ids}, nil)
		if yield || err != nil {
			return err
		}
		f.Retry(time.Millisecond, 1, time.Millisecond, 0)
		return nil
	})
	mux.HandleTask("wideflow.verify:428/wait", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// Under -race a refill scan plus fetch over ~2,200 due rows takes ~350-400ms on the pure-Go SQLite, so at the
	// derived ~67ms period both loops run back to back on the lock and core the workers need: a subtest spent 57%
	// of its CPU there against 5.5% executing steps. A 1s period leaves the workers the database.
	if enginetest.Race() {
		assert.NoError(engineseams.SetRefillInterval(eng, time.Second))
	}
	if !assert.NoError(eng.Startup(t.Context())) {
		t.FailNow()
	}
}

func wideflowItems(n int) map[string]any {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return map[string]any{"items": s}
}
