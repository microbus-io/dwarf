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
	"slices"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// childCapture records the step key of each subgraph child as its entry task runs, so a test can
// later assert which children survived and which were reaped. Tasks read their own identity off the
// Flow carrier (f.StepKey()), which is why no interrupt staging is needed to observe a prior attempt.
type childCapture struct {
	mu       sync.Mutex
	stepKeys []string
}

func (c *childCapture) record(stepKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stepKeys = append(c.stepKeys, stepKey)
}

func (c *childCapture) keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.stepKeys...)
}

// TestRetrySubgraphReapflow canonizes the decision that when a step that launched a subgraph is
// retried in place (flow.Retry), the prior attempt's child flow is reaped, recursively. The retry
// re-spawns a *fresh* child, so leaving the old one would make the execution DAG claim two paths
// (X -> iter1 -> iter2 -> Y) when the model is single-path. We capture iteration 1's child step key
// before the retry and assert it can no longer be loaded afterward, while iteration 2's child can.
func TestRetrySubgraphReapflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	captured := &childCapture{}

	// Parent graph: TaskA -> RunInner -> TaskZ
	parent := workflow.NewGraph("Parent", "retrysubgraphreapflow.verify:428/parent")
	parent.AddTask("TaskA", "retrysubgraphreapflow.verify:428/task-a")
	parent.AddTask("RunInner", "retrysubgraphreapflow.verify:428/run-inner")
	parent.AddTask("TaskZ", "retrysubgraphreapflow.verify:428/task-z")
	parent.AddTransition("TaskA", "RunInner")
	parent.AddTransition("RunInner", "TaskZ")
	parent.AddTransition("TaskZ", workflow.END)
	proxy.HandleGraph("retrysubgraphreapflow.verify:428/parent", parent)

	// Inner graph: TaskX -> TaskY
	inner := workflow.NewGraph("Inner", "retrysubgraphreapflow.verify:428/inner")
	inner.AddTask("TaskX", "retrysubgraphreapflow.verify:428/task-x")
	inner.AddTask("TaskY", "retrysubgraphreapflow.verify:428/task-y")
	inner.AddTransition("TaskX", "TaskY")
	inner.AddTransition("TaskY", workflow.END)
	proxy.HandleGraph("retrysubgraphreapflow.verify:428/inner", inner)

	proxy.HandleTask("retrysubgraphreapflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// The inner entry task records the child's own step key off the carrier each time a child spawns.
	proxy.HandleTask("retrysubgraphreapflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		captured.record(f.StepKey())
		f.SetString("innerResult", "X")
		return nil
	})
	proxy.HandleTask("retrysubgraphreapflow.verify:428/task-y", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("innerResult", "Y("+f.GetString("innerResult")+")")
		return nil
	})
	// RunInner launches the subgraph, then retries exactly once (maxAttempts=1: true at attempt 0,
	// false at attempt 1), re-spawning a fresh child on the retry.
	proxy.HandleTask("retrysubgraphreapflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("retrysubgraphreapflow.verify:428/inner", map[string]any{"seed": f.GetString("seed")}, &out)
		if yield || err != nil {
			return err
		}
		if r, ok := out["innerResult"]; ok {
			f.Set("innerResult", r)
		}
		f.Retry(1, 0, 0, 0)
		return nil
	})
	proxy.HandleTask("retrysubgraphreapflow.verify:428/task-z", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", "Z("+f.GetString("innerResult")+")")
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	assert := testarossa.For(t)

	outcome, err := eng.Run(ctx, "retrysubgraphreapflow.verify:428/parent", map[string]any{"seed": "s"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("Z(Y(X))", outcome.State["result"])

	// Two children were spawned: iteration 1 (reaped on retry) and iteration 2 (the survivor).
	keys := captured.keys()
	assert.Equal(2, len(keys))
	assert.NotEqual(keys[0], keys[1])

	// Iteration 1's child was reaped - its step can no longer be loaded.
	_, err = eng.Step(ctx, keys[0])
	assert.Error(err)

	// Iteration 2's child survives and is loadable.
	survivor, err := eng.Step(ctx, keys[1])
	assert.NoError(err)
	assert.NotNil(survivor)

	// And the parent's history renders the survivor's subtree, not the reaped iteration-1 subtree, so
	// the DAG is single-path. The survivor's step key appears in SubHistory; the reaped one does not.
	hist, err := eng.History(ctx, outcome.FlowKey)
	assert.NoError(err)
	var subKeys []string
	for _, step := range hist {
		if step.Subgraph {
			for _, sub := range step.SubHistory {
				subKeys = append(subKeys, sub.StepKey)
			}
		}
	}
	assert.True(slices.Contains(subKeys, keys[1]))
	assert.False(slices.Contains(subKeys, keys[0]))
}

// TestRestartFromSubgraphReapflow canonizes the same reap for the operator-facing in-place rewind:
// RestartFrom on a subgraph caller re-spawns a fresh child, and the prior child must be reaped so no
// dangling iteration-1 subtree survives.
func TestRestartFromSubgraphReapflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	captured := &childCapture{}

	parent := workflow.NewGraph("Parent", "restartfromsubgraphreapflow.verify:428/parent")
	parent.AddTask("TaskA", "restartfromsubgraphreapflow.verify:428/task-a")
	parent.AddTask("RunInner", "restartfromsubgraphreapflow.verify:428/run-inner")
	parent.AddTask("TaskZ", "restartfromsubgraphreapflow.verify:428/task-z")
	parent.AddTransition("TaskA", "RunInner")
	parent.AddTransition("RunInner", "TaskZ")
	parent.AddTransition("TaskZ", workflow.END)
	proxy.HandleGraph("restartfromsubgraphreapflow.verify:428/parent", parent)

	inner := workflow.NewGraph("Inner", "restartfromsubgraphreapflow.verify:428/inner")
	inner.AddTask("TaskX", "restartfromsubgraphreapflow.verify:428/task-x")
	inner.AddTask("TaskY", "restartfromsubgraphreapflow.verify:428/task-y")
	inner.AddTransition("TaskX", "TaskY")
	inner.AddTransition("TaskY", workflow.END)
	proxy.HandleGraph("restartfromsubgraphreapflow.verify:428/inner", inner)

	proxy.HandleTask("restartfromsubgraphreapflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("restartfromsubgraphreapflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		captured.record(f.StepKey())
		f.SetString("innerResult", "X")
		return nil
	})
	proxy.HandleTask("restartfromsubgraphreapflow.verify:428/task-y", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("innerResult", "Y("+f.GetString("innerResult")+")")
		return nil
	})
	proxy.HandleTask("restartfromsubgraphreapflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("restartfromsubgraphreapflow.verify:428/inner", map[string]any{"seed": f.GetString("seed")}, &out)
		if yield || err != nil {
			return err
		}
		if r, ok := out["innerResult"]; ok {
			f.Set("innerResult", r)
		}
		return nil
	})
	proxy.HandleTask("restartfromsubgraphreapflow.verify:428/task-z", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", "Z("+f.GetString("innerResult")+")")
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	assert := testarossa.For(t)

	// First run to completion - one subgraph child created.
	outcome, err := eng.Run(ctx, "restartfromsubgraphreapflow.verify:428/parent", map[string]any{"seed": "s"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	keys := captured.keys()
	assert.Equal(1, len(keys))
	child1Key := keys[0]

	// Find the RunInner caller step key from the parent's history.
	hist, err := eng.History(ctx, outcome.FlowKey)
	assert.NoError(err)
	var runInnerKey string
	for _, step := range hist {
		if step.TaskName == "RunInner" {
			runInnerKey = step.StepKey
		}
	}
	assert.NotEqual("", runInnerKey)

	// Rewind the caller in place. This re-spawns a fresh child and must reap the prior one.
	err = eng.RestartFrom(ctx, runInnerKey, nil)
	assert.NoError(err)

	final, err := eng.Await(ctx, outcome.FlowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, final.Status)

	// A second child was spawned, distinct from the first.
	keys = captured.keys()
	assert.Equal(2, len(keys))
	child2Key := keys[1]
	assert.NotEqual(child1Key, child2Key)

	// Iteration 1's child was reaped; iteration 2's survives.
	_, err = eng.Step(ctx, child1Key)
	assert.Error(err)
	survivor, err := eng.Step(ctx, child2Key)
	assert.NoError(err)
	assert.NotNil(survivor)
}
