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

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestBreakpointflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("breakpointflow.verify:428/breakpoint")
	graph.AddTask("taskA", "breakpointflow.verify:428/task-a")
	graph.AddTask("taskB", "breakpointflow.verify:428/task-b")
	graph.AddTask("taskC", "breakpointflow.verify:428/task-c")
	graph.AddTransition("taskA", "taskB")
	graph.AddTransition("taskB", "taskC")
	graph.AddTransition("taskC", workflow.END)
	proxy.HandleGraph("breakpointflow.verify:428/breakpoint", graph)

	proxy.HandleTask("breakpointflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("stepA", true)
		return nil
	})
	proxy.HandleTask("breakpointflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("stepB", f.GetBool("stepA"))
		return nil
	})
	proxy.HandleTask("breakpointflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("stepC", f.GetBool("stepB"))
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("breakpoint_pauses_before_TaskB_then_resume_completes_flow", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "breakpointflow.verify:428/breakpoint", nil, nil)
		if !assert.NoError(err) {
			return
		}
		err = eng.BreakBefore(ctx, flowKey, "breakpointflow.verify:428/task-b", true)
		if !assert.NoError(err) {
			return
		}
		err = eng.Start(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)
		assert.Equal(true, outcome.State["stepA"])

		// Resume (for flow.Interrupt) should reject with 409 on a breakpoint pause.
		err = eng.Resume(ctx, flowKey, nil)
		assert.Error(err)

		// ResumeBreak with state override: stepA=false propagates through B and C.
		err = eng.ResumeBreak(ctx, flowKey, map[string]any{"stepA": false})
		if !assert.NoError(err) {
			return
		}

		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(false, outcome.State["stepC"])
	})
}
