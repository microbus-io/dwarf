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
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := engine.NewHostMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	// Outer graph: A -> {NormalB, RunInner} -> J
	outer := workflow.NewGraph("Nested")
	outer.SetURL("TaskA", "nestedfanoutflow.verify:428/task-a")
	outer.SetURL("NormalB", "nestedfanoutflow.verify:428/normal-b")
	outer.SetURL("RunInner", "nestedfanoutflow.verify:428/run-inner")
	outer.SetURL("TaskJ", "nestedfanoutflow.verify:428/task-j")
	outer.SetFanIn("TaskJ")
	outer.AddTransition("TaskA", "NormalB")
	outer.AddTransition("TaskA", "RunInner")
	outer.AddTransition("NormalB", "TaskJ")
	outer.AddTransitionChain("RunInner", "TaskJ", workflow.End)
	mux.HandleGraph("nestedfanoutflow.verify:428/nested", outer)

	// Inner subgraph: X -> {Y, Z} -> W with ReducerAdd on "inner"
	inner := workflow.NewGraph("Inner")
	inner.SetURL("TaskX", "nestedfanoutflow.verify:428/task-x")
	inner.SetURL("TaskY", "nestedfanoutflow.verify:428/task-y")
	inner.SetURL("TaskZ", "nestedfanoutflow.verify:428/task-z")
	inner.SetURL("TaskW", "nestedfanoutflow.verify:428/task-w")
	inner.SetFanIn("TaskW")
	inner.SetReducer("inner", workflow.ReducerAdd)
	inner.AddTransition("TaskX", "TaskY")
	inner.AddTransition("TaskX", "TaskZ")
	inner.AddTransition("TaskY", "TaskW")
	inner.AddTransitionChain("TaskZ", "TaskW", workflow.End)
	mux.HandleGraph("nestedfanoutflow.verify:428/inner", inner)

	mux.HandleTask("nestedfanoutflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("nestedfanoutflow.verify:428/normal-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("normalResult", "normal")
		return nil
	})
	mux.HandleTask("nestedfanoutflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("nestedfanoutflow.verify:428/task-y", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("inner", 10)
		return nil
	})
	mux.HandleTask("nestedfanoutflow.verify:428/task-z", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("inner", 20)
		return nil
	})
	mux.HandleTask("nestedfanoutflow.verify:428/task-w", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("innerResult", f.GetInt("inner"))
		return nil
	})
	mux.HandleTask("nestedfanoutflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("nestedfanoutflow.verify:428/inner", nil, &out)
		if yield || err != nil {
			return err
		}
		if r, ok := out["innerResult"]; ok {
			f.Set("innerResult", r)
		}
		return nil
	})
	mux.HandleTask("nestedfanoutflow.verify:428/task-j", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", fmt.Sprintf("%s/%d", f.GetString("normalResult"), f.GetInt("innerResult")))
		return nil
	})

	t.Run("nested_fan_out_via_subgraph", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "nestedfanoutflow.verify:428/nested", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("normal/30", stateVal(outcome.State, "result"))
	})
}
