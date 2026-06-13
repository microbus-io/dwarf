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

/*
An interrupt raised by a task *inside a subgraph* propagates up the surgraph chain
so the caller awaiting the root flow observes status=interrupted with the inner
payload. Resume on the root flow propagates back down to the interrupted leaf,
delivers the resume data to the inner flow.Interrupt call, and the whole chain
runs to completion. Covers "Interrupt/Resume Propagation Across Subgraphs".
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestSubgraphinterruptflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Parent: A -> runInner -> Z
	parent := workflow.NewGraph("subgraphinterruptflow.verify:428/parent")
	parent.AddTask("taskA", "subgraphinterruptflow.verify:428/task-a")
	parent.AddTask("runInner", "subgraphinterruptflow.verify:428/run-inner")
	parent.AddTask("taskZ", "subgraphinterruptflow.verify:428/task-z")
	parent.AddTransition("taskA", "runInner")
	parent.AddTransition("runInner", "taskZ")
	parent.AddTransition("taskZ", workflow.END)
	proxy.HandleGraph("subgraphinterruptflow.verify:428/parent", parent)

	// Inner: X -> pause (interrupts) -> Y
	inner := workflow.NewGraph("subgraphinterruptflow.verify:428/inner")
	inner.AddTask("taskX", "subgraphinterruptflow.verify:428/task-x")
	inner.AddTask("pause", "subgraphinterruptflow.verify:428/pause")
	inner.AddTask("taskY", "subgraphinterruptflow.verify:428/task-y")
	inner.AddTransition("taskX", "pause")
	inner.AddTransition("pause", "taskY")
	inner.AddTransition("taskY", workflow.END)
	proxy.HandleGraph("subgraphinterruptflow.verify:428/inner", inner)

	proxy.HandleTask("subgraphinterruptflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		return nil
	})
	proxy.HandleTask("subgraphinterruptflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		f.SetString("inner", "X")
		return nil
	})
	proxy.HandleTask("subgraphinterruptflow.verify:428/pause", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		resumeData, yield, err := f.Interrupt(map[string]any{"need": "input"})
		if yield || err != nil {
			return err
		}
		if a, ok := resumeData["answer"]; ok {
			f.Set("answer", a)
		}
		return nil
	})
	proxy.HandleTask("subgraphinterruptflow.verify:428/task-y", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		f.SetString("innerResult", f.GetString("inner")+"->Y("+f.GetString("answer")+")")
		return nil
	})
	proxy.HandleTask("subgraphinterruptflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		out, yield, err := f.Subgraph("subgraphinterruptflow.verify:428/inner", nil)
		if yield || err != nil {
			return err
		}
		if r, ok := out["innerResult"]; ok {
			f.Set("innerResult", r)
		}
		return nil
	})
	proxy.HandleTask("subgraphinterruptflow.verify:428/task-z", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		f.SetString("result", "Z("+f.GetString("innerResult")+")")
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("inner_interrupt_surfaces_and_resumes_at_root", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "subgraphinterruptflow.verify:428/parent", nil, nil, nil)
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(eng.Start(ctx, flowKey)) {
			return
		}

		// The root flow surfaces the interrupt raised deep inside the subgraph.
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)
		assert.Equal("input", outcome.InterruptPayload["need"])

		// Resuming the root propagates the answer down to the inner leaf.
		if !assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"answer": "42"})) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("Z(X->Y(42))", outcome.State["result"])
	})
}
