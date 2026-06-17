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

func TestSubgraphflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Parent graph: A -> RunInner -> Z
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("TaskA", "subgraphflow.verify:428/task-a")
	parent.SetEndpoint("RunInner", "subgraphflow.verify:428/run-inner")
	parent.SetEndpoint("TaskZ", "subgraphflow.verify:428/task-z")
	parent.AddTransitionChain("TaskA", "RunInner", "TaskZ", workflow.END)
	proxy.HandleGraph("subgraphflow.verify:428/parent", parent)

	// Inner graph: X -> Y
	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("TaskX", "subgraphflow.verify:428/task-x")
	inner.SetEndpoint("TaskY", "subgraphflow.verify:428/task-y")
	inner.AddTransitionChain("TaskX", "TaskY", workflow.END)
	proxy.HandleGraph("subgraphflow.verify:428/inner", inner)

	proxy.HandleTask("subgraphflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("subgraphflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("innerResult", fmt.Sprintf("X(%s)", f.GetString("seed")))
		return nil
	})
	proxy.HandleTask("subgraphflow.verify:428/task-y", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("innerResult", fmt.Sprintf("Y(%s)", f.GetString("innerResult")))
		return nil
	})
	proxy.HandleTask("subgraphflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("subgraphflow.verify:428/inner", map[string]any{"seed": f.GetString("seed")}, &out)
		if yield || err != nil {
			return err
		}
		if r, ok := out["innerResult"]; ok {
			f.Set("innerResult", r)
		}
		return nil
	})
	proxy.HandleTask("subgraphflow.verify:428/task-z", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", fmt.Sprintf("Z(%s)", f.GetString("innerResult")))
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("subgraph_output_merges_into_parent_state", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"seed": "seed1"}
		_, outcome, err := eng.Run(ctx, "subgraphflow.verify:428/parent", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("Z(Y(X(seed1)))", outcome.State["result"])
	})
}
