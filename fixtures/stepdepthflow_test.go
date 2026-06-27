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
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// findStep returns the first step matching taskName (searching nested SubHistory too).
func findStep(steps []workflow.FlowStep, taskName string) (workflow.FlowStep, bool) {
	for _, s := range steps {
		if s.TaskName == taskName {
			return s, true
		}
		if s.Subgraph {
			if found, ok := findStep(s.SubHistory, taskName); ok {
				return found, ok
			}
		}
	}
	return workflow.FlowStep{}, false
}

// depthByTask returns the StepDepth of the first step matching taskName (searching nested SubHistory too).
func depthByTask(steps []workflow.FlowStep, taskName string) (int, bool) {
	s, ok := findStep(steps, taskName)
	return s.StepDepth, ok
}

// TestStepDepth_SubgraphContinuesFromCaller verifies a subgraph's steps are numbered as a continuation of
// the caller: if the caller step is at depth X, the subgraph's entry step is at X+1.
func TestStepDepth_SubgraphContinuesFromCaller(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	parent := workflow.NewGraph("DepthParent")
	parent.SetEndpoint("A", "depthsub.verify:428/a")
	parent.SetEndpoint("Call", "depthsub.verify:428/call")
	parent.AddTransitionChain("A", "Call", workflow.END)
	proxy.HandleGraph("depthsub.verify:428/parent", parent)

	child := workflow.NewGraph("DepthChild")
	child.SetEndpoint("Inner", "depthsub.verify:428/inner")
	child.SetEndpoint("Inner2", "depthsub.verify:428/inner2")
	child.AddTransitionChain("Inner", "Inner2", workflow.END)
	proxy.HandleGraph("depthsub.verify:428/child", child)

	proxy.HandleTask("depthsub.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("depthsub.verify:428/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("depthsub.verify:428/child", map[string]any{}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("depthsub.verify:428/inner", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("depthsub.verify:428/inner2", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	flowKey, _, err := eng.Run(ctx, "depthsub.verify:428/parent", map[string]any{}, nil)
	assert.NoError(err)

	hist, err := eng.History(ctx, flowKey)
	assert.NoError(err)

	aDepth, ok := depthByTask(hist, "A")
	assert.True(ok)
	assert.Equal(1, aDepth) // entry
	callDepth, ok := depthByTask(hist, "Call")
	assert.True(ok)
	assert.Equal(2, callDepth) // A -> Call
	innerDepth, ok := depthByTask(hist, "Inner")
	assert.True(ok)
	assert.Equal(3, innerDepth) // subgraph entry = caller (2) + 1
	inner2Depth, ok := depthByTask(hist, "Inner2")
	assert.True(ok)
	assert.Equal(4, inner2Depth) // continues inside the child
}

// TestStepDepth_FanInIsMaxCohortDepthPlus1 verifies the fan-in step sits one below the DEEPEST cohort
// branch, not merely below the last sibling to complete. A forEach fans out two branches; one is extended
// by a goto (deeper) while the other goes straight to the fan-in. The fan-in depth must reflect the deeper
// branch regardless of completion order.
func TestStepDepth_FanInIsMaxCohortDepthPlus1(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	g := workflow.NewGraph("DepthFanIn")
	g.SetEndpoint("Seed", "depthfanin.verify:428/seed")
	g.SetEndpoint("Work", "depthfanin.verify:428/work")
	g.SetEndpoint("Deep", "depthfanin.verify:428/deep")
	g.SetEndpoint("J", "depthfanin.verify:428/j")
	g.SetFanIn("J")
	g.AddTransitionForEach("Seed", "Work", "items", "n")
	g.AddTransition("Work", "J")        // normal: straight to fan-in
	g.AddTransitionGoto("Work", "Deep") // goto: extend this branch one level deeper
	g.AddTransition("Deep", "J")
	proxy.HandleGraph("depthfanin.verify:428/g", g)

	// gate1 holds element 1's shallow branch (Work -> J, depth 2) until the deep branch (Work -> Deep -> J,
	// depth 3) has already arrived at the fan-in. That makes the SHALLOW step the last arrival, so a
	// last-completer+1 fan-in depth would compute 3 while the correct max-cohort+1 is 4.
	gate1 := make(chan struct{})
	proxy.HandleTask("depthfanin.verify:428/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("items", []int{1, 2})
		return nil
	})
	proxy.HandleTask("depthfanin.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("n") == 2 {
			f.Goto("Deep") // deep branch runs immediately
			return nil
		}
		select { // shallow branch waits until the deep branch has reached the fan-in
		case <-gate1:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	proxy.HandleTask("depthfanin.verify:428/deep", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("depthfanin.verify:428/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngine()
	testarossa.For(t).NoError(eng.SetWorkers(4)) // so the gated shallow branch doesn't starve the deep one
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	flowKey, err := eng.Create(ctx, "depthfanin.verify:428/g", map[string]any{}, nil)
	assert.NoError(err)

	// Wait until the deep branch's Deep step has completed (its fan-in arrival committed); only then release
	// the shallow branch, guaranteeing it is the last arrival that triggers the fan-in.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s, ok := findStep(mustHistory(t, eng, flowKey), "Deep")
		if ok && s.Status == workflow.StatusCompleted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(gate1)

	final, err := eng.Await(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, final.Status)

	hist := mustHistory(t, eng, flowKey)
	// Seed=1, Work cohort=2, Deep=3 (deep branch), J = max(2,2,3)+1 = 4 - even though the depth-2 shallow
	// branch was the last to arrive.
	workDepth, ok := depthByTask(hist, "Work")
	assert.True(ok)
	assert.Equal(2, workDepth)
	deepDepth, ok := depthByTask(hist, "Deep")
	assert.True(ok)
	assert.Equal(3, deepDepth)
	jDepth, ok := depthByTask(hist, "J")
	assert.True(ok)
	assert.Equal(4, jDepth) // max cohort depth (Deep=3) + 1, NOT last-completer (shallow=2) + 1
}

// mustHistory fetches History or fails the test.
func mustHistory(t *testing.T, eng *engine.Engine, flowKey string) []workflow.FlowStep {
	t.Helper()
	hist, err := eng.History(context.Background(), flowKey)
	testarossa.For(t).NoError(err)
	return hist
}
