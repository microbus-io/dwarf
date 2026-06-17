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

func TestSubgraphentryflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Outer graph: RunInner -> RunTail -> END
	outer := workflow.NewGraph("Outer")
	outer.SetEndpoint("RunInner", "subgraphentryflow.verify:428/run-inner")
	outer.SetEndpoint("RunTail", "subgraphentryflow.verify:428/run-tail")
	outer.AddTransition("RunInner", "RunTail")
	outer.AddTransition("RunTail", workflow.END)
	proxy.HandleGraph("subgraphentryflow.verify:428/outer", outer)

	// Inner subgraph: TaskInner -> END
	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("TaskInner", "subgraphentryflow.verify:428/task-inner")
	inner.AddTransition("TaskInner", workflow.END)
	proxy.HandleGraph("subgraphentryflow.verify:428/inner", inner)

	// Tail subgraph: TaskTail -> END
	tail := workflow.NewGraph("Tail")
	tail.SetEndpoint("TaskTail", "subgraphentryflow.verify:428/task-tail")
	tail.AddTransition("TaskTail", workflow.END)
	proxy.HandleGraph("subgraphentryflow.verify:428/tail", tail)

	proxy.HandleTask("subgraphentryflow.verify:428/task-inner", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("innerResult", "inner")
		return nil
	})
	proxy.HandleTask("subgraphentryflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("subgraphentryflow.verify:428/inner", nil, &out)
		if yield || err != nil {
			return err
		}
		if r, ok := out["innerResult"]; ok {
			f.Set("innerResult", r)
		}
		return nil
	})
	proxy.HandleTask("subgraphentryflow.verify:428/task-tail", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("finalResult", f.GetString("innerResult")+"/tail")
		return nil
	})
	proxy.HandleTask("subgraphentryflow.verify:428/run-tail", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("subgraphentryflow.verify:428/tail", map[string]any{"innerResult": f.GetString("innerResult")}, &out)
		if yield || err != nil {
			return err
		}
		if r, ok := out["finalResult"]; ok {
			f.Set("finalResult", r)
		}
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("subgraph_as_first_and_last_node", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "subgraphentryflow.verify:428/outer", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("inner/tail", outcome.State["finalResult"])
	})
}
