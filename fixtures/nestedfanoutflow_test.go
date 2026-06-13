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
	"fmt"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestNestedfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Outer graph: A -> {NormalB, RunInner} -> J
	outer := workflow.NewGraph("nestedfanoutflow.verify:428/nested")
	outer.AddTask("taskA", "nestedfanoutflow.verify:428/task-a")
	outer.AddTask("normalB", "nestedfanoutflow.verify:428/normal-b")
	outer.AddTask("runInner", "nestedfanoutflow.verify:428/run-inner")
	outer.AddTask("taskJ", "nestedfanoutflow.verify:428/task-j")
	outer.SetFanIn("taskJ")
	outer.AddTransition("taskA", "normalB")
	outer.AddTransition("taskA", "runInner")
	outer.AddTransition("normalB", "taskJ")
	outer.AddTransition("runInner", "taskJ")
	outer.AddTransition("taskJ", workflow.END)
	proxy.HandleGraph("nestedfanoutflow.verify:428/nested", outer)

	// Inner subgraph: X -> {Y, Z} -> W with ReducerAdd on "inner"
	inner := workflow.NewGraph("nestedfanoutflow.verify:428/inner")
	inner.AddTask("taskX", "nestedfanoutflow.verify:428/task-x")
	inner.AddTask("taskY", "nestedfanoutflow.verify:428/task-y")
	inner.AddTask("taskZ", "nestedfanoutflow.verify:428/task-z")
	inner.AddTask("taskW", "nestedfanoutflow.verify:428/task-w")
	inner.SetFanIn("taskW")
	inner.SetReducer("inner", workflow.ReducerAdd)
	inner.AddTransition("taskX", "taskY")
	inner.AddTransition("taskX", "taskZ")
	inner.AddTransition("taskY", "taskW")
	inner.AddTransition("taskZ", "taskW")
	inner.AddTransition("taskW", workflow.END)
	proxy.HandleGraph("nestedfanoutflow.verify:428/inner", inner)

	proxy.HandleTask("nestedfanoutflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		return nil
	})
	proxy.HandleTask("nestedfanoutflow.verify:428/normal-b", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetString("normalResult", "normal")
		return nil
	})
	proxy.HandleTask("nestedfanoutflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		return nil
	})
	proxy.HandleTask("nestedfanoutflow.verify:428/task-y", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetInt("inner", 10)
		return nil
	})
	proxy.HandleTask("nestedfanoutflow.verify:428/task-z", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetInt("inner", 20)
		return nil
	})
	proxy.HandleTask("nestedfanoutflow.verify:428/task-w", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetInt("innerResult", f.GetInt("inner"))
		return nil
	})
	proxy.HandleTask("nestedfanoutflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		out, yield, err := f.Subgraph("nestedfanoutflow.verify:428/inner", nil)
		if yield || err != nil {
			return err
		}
		if r, ok := out["innerResult"]; ok {
			f.Set("innerResult", r)
		}
		return nil
	})
	proxy.HandleTask("nestedfanoutflow.verify:428/task-j", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetString("result", fmt.Sprintf("%s/%d", f.GetString("normalResult"), f.GetInt("innerResult")))
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("nested_fan_out_via_subgraph", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "nestedfanoutflow.verify:428/nested", nil, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("normal/30", outcome.State["result"])
	})
}
