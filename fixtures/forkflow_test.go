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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// stepKeyByTask returns the key of the first step whose task matches taskName.
func stepKeyByTask(t *testing.T, eng *engine.Engine, flowKey, taskName string) string {
	t.Helper()
	hist, err := eng.History(context.Background(), flowKey)
	testarossa.For(t).NoError(err)
	for _, s := range hist {
		if s.TaskName == taskName {
			return s.StepKey
		}
	}
	return ""
}

// failedCellKeyByIndex returns the key of a failed step whose forEach index (itemIndex, injected into
// state by the fan-out) equals idx. History omits per-step state, so each candidate is fetched via Step.
func failedCellKeyByIndex(t *testing.T, eng *engine.Engine, flowKey string, idx int) string {
	t.Helper()
	ctx := context.Background()
	hist, err := eng.History(ctx, flowKey)
	testarossa.For(t).NoError(err)
	for _, s := range hist {
		if s.Status != workflow.StatusFailed {
			continue
		}
		full, err := eng.Step(ctx, s.StepKey)
		if err != nil {
			continue
		}
		switch v := full.State["itemIndex"].(type) {
		case float64:
			if int(v) == idx {
				return s.StepKey
			}
		case int:
			if v == idx {
				return s.StepKey
			}
		}
	}
	return ""
}

// TestForkflow_LinearWithOverride forks a completed linear flow at a middle step with a state override
// and confirms the fork re-runs from there with the override while the original stays untouched.
func TestForkflow_LinearWithOverride(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	var bRuns int32

	g := workflow.NewGraph("Linear")
	g.SetEndpoint("A", "forkflow.verify:428/a")
	g.SetEndpoint("B", "forkflow.verify:428/b")
	g.SetEndpoint("C", "forkflow.verify:428/c")
	g.AddTransitionChain("A", "B", "C", workflow.END)
	proxy.HandleGraph("forkflow.verify:428/linear", g)

	proxy.HandleTask("forkflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("forkflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		atomic.AddInt32(&bRuns, 1)
		f.SetString("bSaw", f.GetString("seed"))
		return nil
	})
	proxy.HandleTask("forkflow.verify:428/c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	originKey, outcome, err := eng.Run(ctx, "forkflow.verify:428/linear", map[string]any{"seed": "orig"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("orig", outcome.State["bSaw"])
	assert.Equal(int32(1), atomic.LoadInt32(&bRuns))

	bKey := stepKeyByTask(t, eng, originKey, "B")
	assert.NotEqual("", bKey)

	forkKey, err := eng.Fork(ctx, bKey, map[string]any{"seed": "forked"})
	assert.NoError(err)
	assert.NotEqual(originKey, forkKey)

	forkOutcome, err := eng.Await(ctx, forkKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)
	// The fork re-ran B (and C) with the override.
	assert.Equal("forked", forkOutcome.State["bSaw"])
	assert.Equal(true, forkOutcome.State["done"])
	assert.Equal(int32(2), atomic.LoadInt32(&bRuns))

	// The original is unchanged and still its own completed flow.
	origAfter, err := eng.Snapshot(ctx, originKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, origAfter.Status)
	assert.Equal("orig", origAfter.State["bSaw"])
}

// TestForkflow_RecoverFailedStep forks a failed flow at the failing step with an override that makes it
// succeed, confirming the fork completes while the original stays failed (non-destructive recovery).
func TestForkflow_RecoverFailedStep(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	g := workflow.NewGraph("Recoverable")
	g.SetEndpoint("A", "forkflow2.verify:428/a")
	g.SetEndpoint("B", "forkflow2.verify:428/b")
	g.SetEndpoint("C", "forkflow2.verify:428/c")
	g.AddTransitionChain("A", "B", "C", workflow.END)
	proxy.HandleGraph("forkflow2.verify:428/g", g)

	proxy.HandleTask("forkflow2.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("forkflow2.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		if !f.GetBool("ok") {
			return errors.New("B needs ok=true")
		}
		f.SetString("b", "ran")
		return nil
	})
	proxy.HandleTask("forkflow2.verify:428/c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	originKey, outcome, err := eng.Run(ctx, "forkflow2.verify:428/g", map[string]any{}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)

	bKey := stepKeyByTask(t, eng, originKey, "B")
	assert.NotEqual("", bKey)

	forkKey, err := eng.Fork(ctx, bKey, map[string]any{"ok": true})
	assert.NoError(err)

	forkOutcome, err := eng.Await(ctx, forkKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)
	assert.Equal(true, forkOutcome.State["done"])

	// Original is still failed - forking never mutates it.
	origAfter, err := eng.Snapshot(ctx, originKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, origAfter.Status)
}

// TestForkflow_ContinueExcludesFork confirms a fork shares the thread for List grouping but is invisible
// to Continue, which bases the next turn on the original completed flow, not the fork.
func TestForkflow_ContinueExcludesFork(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	g := workflow.NewGraph("Threaded")
	g.SetEndpoint("A", "forkflow3.verify:428/a")
	g.SetEndpoint("B", "forkflow3.verify:428/b")
	g.AddTransitionChain("A", "B", workflow.END)
	proxy.HandleGraph("forkflow3.verify:428/g", g)

	proxy.HandleTask("forkflow3.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("forkflow3.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("tag", f.GetString("seed"))
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	originKey, outcome, err := eng.Run(ctx, "forkflow3.verify:428/g", map[string]any{"seed": "origin"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("origin", outcome.State["tag"])

	// Fork the origin at B with a distinct override; it completes in the same thread.
	bKey := stepKeyByTask(t, eng, originKey, "B")
	forkKey, err := eng.Fork(ctx, bKey, map[string]any{"seed": "forked"})
	assert.NoError(err)
	forkOutcome, err := eng.Await(ctx, forkKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)
	assert.Equal("forked", forkOutcome.State["tag"])

	// Continue on the thread must base on the ORIGIN (tag=origin), not the later fork (tag=forked).
	contKey, err := eng.Continue(ctx, originKey, map[string]any{})
	assert.NoError(err)
	contOutcome, err := eng.Await(ctx, contKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, contOutcome.Status)
	// B re-ran in the continuation reading the carried-over seed from the origin's final state.
	assert.Equal("origin", contOutcome.State["tag"])
}

// TestForkflow_FanoutForkOfFork forks a partially-failed fan-out one failed branch at a time. The first
// fork still fails (the other branch is still broken) - converging cleanly via cohort accounting, no
// limbo - and the second fork (of the first) completes once both branches are fixed.
func TestForkflow_FanoutForkOfFork(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	var mu sync.Mutex
	overrides := map[int]bool{} // indices forced to succeed via override carry "fix<idx>"=true

	g := workflow.NewGraph("Fan")
	g.SetEndpoint("Seed", "forkflow4.verify:428/seed")
	g.SetEndpoint("Cell", "forkflow4.verify:428/cell")
	g.SetEndpoint("Join", "forkflow4.verify:428/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Seed", "Cell", "items", "item")
	g.AddTransitionChain("Cell", "Join", workflow.END)
	proxy.HandleGraph("forkflow4.verify:428/g", g)

	proxy.HandleTask("forkflow4.verify:428/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("items", []int{0, 1, 2})
		return nil
	})
	proxy.HandleTask("forkflow4.verify:428/cell", func(ctx context.Context, f *workflow.Flow) error {
		idx := f.GetInt("itemIndex")
		f.SetInt("cellIndex", idx)
		// Branches 0 and 2 fail unless this branch was given a fix override.
		if (idx == 0 || idx == 2) && !f.GetBool("fix") {
			mu.Lock()
			_ = overrides
			mu.Unlock()
			return errors.New("cell %d broken", idx)
		}
		return nil
	})
	proxy.HandleTask("forkflow4.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("joined", true)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	originKey, outcome, err := eng.Run(ctx, "forkflow4.verify:428/g", map[string]any{}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)

	// Fork branch 0 with a fix. Branch 2 is still broken, so the fork must also fail (clean convergence,
	// no orphan/limbo): when the rewound branch re-arrives, arrivals==size && failures>0 -> failed.
	cell0 := failedCellKeyByIndex(t, eng, originKey, 0)
	assert.NotEqual("", cell0)
	fork1, err := eng.Fork(ctx, cell0, map[string]any{"fix": true})
	assert.NoError(err)
	fork1Outcome, err := eng.Await(ctx, fork1)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, fork1Outcome.Status)

	// Fork the FIRST fork at its still-failed branch 2 with a fix -> both branches good -> Join -> completed.
	cell2 := failedCellKeyByIndex(t, eng, fork1, 2)
	assert.NotEqual("", cell2)
	fork2, err := eng.Fork(ctx, cell2, map[string]any{"fix": true})
	assert.NoError(err)
	fork2Outcome, err := eng.Await(ctx, fork2)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, fork2Outcome.Status)
	assert.Equal(true, fork2Outcome.State["joined"])
}

// subgraphForkProxy builds a parent (A -> RunSub -> C) that calls a child subgraph (X -> Y), counting
// child executions so a re-run is observable.
func subgraphForkProxy(childRuns *int32) *engine.TestProxy {
	proxy := engine.NewTestProxy()

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("A", "forksub.verify:428/a")
	parent.SetEndpoint("RunSub", "forksub.verify:428/run-sub")
	parent.SetEndpoint("C", "forksub.verify:428/c")
	parent.AddTransitionChain("A", "RunSub", "C", workflow.END)
	proxy.HandleGraph("forksub.verify:428/parent", parent)

	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "forksub.verify:428/x")
	child.SetEndpoint("Y", "forksub.verify:428/y")
	child.AddTransitionChain("X", "Y", workflow.END)
	proxy.HandleGraph("forksub.verify:428/child", child)

	proxy.HandleTask("forksub.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("forksub.verify:428/x", func(ctx context.Context, f *workflow.Flow) error {
		atomic.AddInt32(childRuns, 1)
		f.SetString("xr", f.GetString("seed"))
		return nil
	})
	proxy.HandleTask("forksub.verify:428/y", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("childOut", "Y("+f.GetString("xr")+")")
		return nil
	})
	proxy.HandleTask("forksub.verify:428/run-sub", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("forksub.verify:428/child", map[string]any{"seed": f.GetString("seed")}, &out)
		if yield || err != nil {
			return err
		}
		if v, ok := out["childOut"]; ok {
			f.Set("subResult", v)
		}
		return nil
	})
	proxy.HandleTask("forksub.verify:428/c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})
	return proxy
}

// TestForkflow_AtSubgraphCaller forks a completed flow at the step that launched a subgraph. The reset
// clears the subgraph park, so the fork re-spawns a *fresh* child subgraph (re-running it end to end) and
// the override flows into the child. History of the fork re-shows the subgraph (its new child).
func TestForkflow_AtSubgraphCaller(t *testing.T) {
	ctx := context.Background()

	var childRuns int32
	proxy := subgraphForkProxy(&childRuns)
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	originKey, outcome, err := eng.Run(ctx, "forksub.verify:428/parent", map[string]any{"seed": "orig"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("Y(orig)", outcome.State["subResult"])
	assert.Equal(int32(1), atomic.LoadInt32(&childRuns))

	runSubKey := stepKeyByTask(t, eng, originKey, "RunSub")
	assert.NotEqual("", runSubKey)

	forkKey, err := eng.Fork(ctx, runSubKey, map[string]any{"seed": "forked"})
	assert.NoError(err)
	forkOutcome, err := eng.Await(ctx, forkKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)
	// The subgraph re-ran from scratch with the override.
	assert.Equal("Y(forked)", forkOutcome.State["subResult"])
	assert.Equal(int32(2), atomic.LoadInt32(&childRuns))

	// The fork's history re-shows the subgraph (its fresh child).
	hist, err := eng.History(ctx, forkKey)
	assert.NoError(err)
	var sawSubgraph bool
	for _, s := range hist {
		if s.TaskName == "RunSub" && s.Subgraph && len(s.SubHistory) > 0 {
			sawSubgraph = true
		}
	}
	assert.True(sawSubgraph)

	// Original is untouched.
	origAfter, err := eng.Snapshot(ctx, originKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, origAfter.Status)
	assert.Equal("Y(orig)", origAfter.State["subResult"])
}

// TestForkflow_InsideSubgraph forks at a step INSIDE a subgraph child. The clone re-roots at the parent,
// re-parks the subgraph caller up the chain, and re-runs from the inner step; when the child re-completes
// the caller revives and execution bubbles back to the root. The override reaches the inner step.
func TestForkflow_InsideSubgraph(t *testing.T) {
	ctx := context.Background()

	var childRuns int32
	proxy := subgraphForkProxy(&childRuns)
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	originKey, outcome, err := eng.Run(ctx, "forksub.verify:428/parent", map[string]any{"seed": "orig"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("Y(orig)", outcome.State["subResult"])
	assert.Equal(int32(1), atomic.LoadInt32(&childRuns))

	// Find inner step X inside the child subgraph via SubHistory.
	hist, err := eng.History(ctx, originKey)
	assert.NoError(err)
	var innerXKey string
	for _, s := range hist {
		if s.Subgraph {
			for _, sub := range s.SubHistory {
				if sub.TaskName == "X" {
					innerXKey = sub.StepKey
				}
			}
		}
	}
	assert.NotEqual("", innerXKey)

	forkKey, err := eng.Fork(ctx, innerXKey, map[string]any{"seed": "forked"})
	assert.NoError(err)
	forkOutcome, err := eng.Await(ctx, forkKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)
	// The inner step re-ran with the override and bubbled the new result up to the root.
	assert.Equal("Y(forked)", forkOutcome.State["subResult"])
	assert.Equal(int32(2), atomic.LoadInt32(&childRuns))

	// The fork's history re-shows the subgraph (its re-run child).
	fhist, err := eng.History(ctx, forkKey)
	assert.NoError(err)
	var sawChild bool
	for _, s := range fhist {
		if s.TaskName == "RunSub" && s.Subgraph && len(s.SubHistory) > 0 {
			sawChild = true
		}
	}
	assert.True(sawChild)

	// Original untouched.
	origAfter, err := eng.Snapshot(ctx, originKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, origAfter.Status)
	assert.Equal("Y(orig)", origAfter.State["subResult"])
}

// budgetRec records the most recent observed remaining time budget (ctx.Deadline) per task name.
type budgetRec struct {
	mu sync.Mutex
	m  map[string]time.Duration
}

func newBudgetRec() *budgetRec { return &budgetRec{m: map[string]time.Duration{}} }

func (b *budgetRec) record(task string, ctx context.Context) {
	if dl, ok := ctx.Deadline(); ok {
		b.mu.Lock()
		b.m[task] = time.Until(dl)
		b.mu.Unlock()
	}
}

func (b *budgetRec) get(task string) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.m[task]
}

// findStepKeyDeep searches a History (and nested SubHistories) for the first step with the given task.
func findStepKeyDeep(steps []workflow.FlowStep, task string) string {
	for _, s := range steps {
		if s.TaskName == task {
			return s.StepKey
		}
		if s.Subgraph {
			if k := findStepKeyDeep(s.SubHistory, task); k != "" {
				return k
			}
		}
	}
	return ""
}

// nestedSubgraphProxy builds a 3-level nested tree: Outer(A -> CallL1 -> Z), CallL1 -> Middle(B -> CallL2
// -> Y), CallL2 -> Inner(C). Every task records its observed remaining time budget so a Fork's resolved
// FlowOptions can be checked at every nesting level.
func nestedSubgraphProxy(rec *budgetRec) *engine.TestProxy {
	proxy := engine.NewTestProxy()

	l0 := workflow.NewGraph("Outer")
	l0.SetEndpoint("A", "forknest.verify:428/a")
	l0.SetEndpoint("CallL1", "forknest.verify:428/call-l1")
	l0.SetEndpoint("Z", "forknest.verify:428/z")
	l0.AddTransitionChain("A", "CallL1", "Z", workflow.END)
	proxy.HandleGraph("forknest.verify:428/outer", l0)

	l1 := workflow.NewGraph("Middle")
	l1.SetEndpoint("B", "forknest.verify:428/b")
	l1.SetEndpoint("CallL2", "forknest.verify:428/call-l2")
	l1.SetEndpoint("Y", "forknest.verify:428/y")
	l1.AddTransitionChain("B", "CallL2", "Y", workflow.END)
	proxy.HandleGraph("forknest.verify:428/middle", l1)

	l2 := workflow.NewGraph("Inner")
	l2.SetEndpoint("C", "forknest.verify:428/c")
	l2.AddTransitionChain("C", workflow.END)
	proxy.HandleGraph("forknest.verify:428/inner", l2)

	proxy.HandleTask("forknest.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		rec.record("A", ctx)
		return nil
	})
	proxy.HandleTask("forknest.verify:428/call-l1", func(ctx context.Context, f *workflow.Flow) error {
		rec.record("CallL1", ctx)
		var out map[string]any
		yield, err := f.Subgraph("forknest.verify:428/middle", map[string]any{"seed": f.GetString("seed")}, &out)
		if yield || err != nil {
			return err
		}
		if v, ok := out["deep"]; ok {
			f.Set("deep", v)
		}
		return nil
	})
	proxy.HandleTask("forknest.verify:428/z", func(ctx context.Context, f *workflow.Flow) error {
		rec.record("Z", ctx)
		f.SetBool("done", true)
		return nil
	})
	proxy.HandleTask("forknest.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		rec.record("B", ctx)
		return nil
	})
	proxy.HandleTask("forknest.verify:428/call-l2", func(ctx context.Context, f *workflow.Flow) error {
		rec.record("CallL2", ctx)
		var out map[string]any
		yield, err := f.Subgraph("forknest.verify:428/inner", map[string]any{"seed": f.GetString("seed")}, &out)
		if yield || err != nil {
			return err
		}
		if v, ok := out["deep"]; ok {
			f.Set("deep", v)
		}
		return nil
	})
	proxy.HandleTask("forknest.verify:428/y", func(ctx context.Context, f *workflow.Flow) error {
		rec.record("Y", ctx)
		return nil
	})
	proxy.HandleTask("forknest.verify:428/c", func(ctx context.Context, f *workflow.Flow) error {
		rec.record("C", ctx)
		f.SetString("deep", f.GetString("seed"))
		return nil
	})
	return proxy
}

// TestForkflow_NestedSubgraphBudgetInherited forks at the deepest step (C, inside Inner inside Middle).
// All three flow levels are cloned, and the inherited budget (Fork takes no FlowOptions) must reach the
// re-running step at every nesting level - including the deepest, which lives in a descendant flow.
func TestForkflow_NestedSubgraphBudgetInherited(t *testing.T) {
	ctx := context.Background()

	// The tasks that re-run when forking at C, across all three nesting levels.
	reRun := []string{"C", "CallL2", "Y", "CallL1", "Z"}

	rec := newBudgetRec()
	proxy := nestedSubgraphProxy(rec)
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	// Original runs with a distinctive 75s budget (inherited by every nested subgraph).
	originKey, outcome, err := eng.Run(ctx, "forknest.verify:428/outer",
		map[string]any{"seed": "orig"}, &workflow.FlowOptions{TimeBudget: 75 * time.Second})
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	hist, err := eng.History(ctx, originKey)
	assert.NoError(err)
	cKey := findStepKeyDeep(hist, "C")
	assert.NotEqual("", cKey)

	// Fork (no opts) inherits the origin's 75s budget; it must reach every re-run step at every level.
	forkKey, err := eng.Fork(ctx, cKey, nil)
	assert.NoError(err)
	forkOutcome, err := eng.Await(ctx, forkKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status)

	for _, task := range reRun {
		b := rec.get(task)
		assert.True(b > 55*time.Second && b < 90*time.Second, "task %s inherited budget %s not ~75s", task, b)
	}
}
