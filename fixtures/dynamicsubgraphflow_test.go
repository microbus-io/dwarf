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

func TestDynamicsubgraphflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Parent graph: single task
	parent := workflow.NewGraph("dynamicsubgraphflow.verify:428/dynamic-subgraph")
	parent.AddTask("parent", "dynamicsubgraphflow.verify:428/parent")
	parent.AddTransition("parent", workflow.END)
	proxy.HandleGraph("dynamicsubgraphflow.verify:428/dynamic-subgraph", parent)

	// Inner graph: InnerA -> InnerB
	inner := workflow.NewGraph("dynamicsubgraphflow.verify:428/inner")
	inner.AddTask("innerA", "dynamicsubgraphflow.verify:428/inner-a")
	inner.AddTask("innerB", "dynamicsubgraphflow.verify:428/inner-b")
	inner.AddTransition("innerA", "innerB")
	inner.AddTransition("innerB", workflow.END)
	proxy.HandleGraph("dynamicsubgraphflow.verify:428/inner", inner)

	proxy.HandleTask("dynamicsubgraphflow.verify:428/parent", func(ctx context.Context, f *workflow.Flow) error {
		out, yield, err := f.Subgraph("dynamicsubgraphflow.verify:428/inner", map[string]any{"value": f.GetInt("value")})
		if yield || err != nil {
			return err
		}
		f.SetString("result", fmt.Sprintf("parent:%v", out["innerResult"]))
		return nil
	})
	proxy.HandleTask("dynamicsubgraphflow.verify:428/inner-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("innerStage", f.GetInt("value")*2)
		return nil
	})
	proxy.HandleTask("dynamicsubgraphflow.verify:428/inner-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("innerResult", f.GetInt("innerStage")+3)
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("parent_re_runs_after_dynamic_subgraph_completes", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"value": 5}
		outcome, err := eng.Run(ctx, "dynamicsubgraphflow.verify:428/dynamic-subgraph", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		// InnerA: 5*2=10, InnerB: 10+3=13, Parent: "parent:13"
		assert.Equal("parent:13", outcome.State["result"])
	})
}
