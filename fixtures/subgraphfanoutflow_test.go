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

func TestSubgraphfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Outer graph: A -> {NormalB, RunSub, NormalD} -> E
	outer := workflow.NewGraph("subgraphfanoutflow.verify:428/sub-fan-out")
	outer.AddTask("taskA", "subgraphfanoutflow.verify:428/task-a")
	outer.AddTask("normalB", "subgraphfanoutflow.verify:428/normal-b")
	outer.AddTask("runSub", "subgraphfanoutflow.verify:428/run-sub")
	outer.AddTask("normalD", "subgraphfanoutflow.verify:428/normal-d")
	outer.AddTask("taskE", "subgraphfanoutflow.verify:428/task-e")
	outer.SetFanIn("taskE")
	outer.AddTransition("taskA", "normalB")
	outer.AddTransition("taskA", "runSub")
	outer.AddTransition("taskA", "normalD")
	outer.AddTransition("normalB", "taskE")
	outer.AddTransition("runSub", "taskE")
	outer.AddTransition("normalD", "taskE")
	outer.AddTransition("taskE", workflow.END)
	proxy.HandleGraph("subgraphfanoutflow.verify:428/sub-fan-out", outer)

	// Sub graph: X -> Y
	sub := workflow.NewGraph("subgraphfanoutflow.verify:428/sub")
	sub.AddTask("taskX", "subgraphfanoutflow.verify:428/task-x")
	sub.AddTask("taskY", "subgraphfanoutflow.verify:428/task-y")
	sub.AddTransition("taskX", "taskY")
	sub.AddTransition("taskY", workflow.END)
	proxy.HandleGraph("subgraphfanoutflow.verify:428/sub", sub)

	proxy.HandleTask("subgraphfanoutflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("subgraphfanoutflow.verify:428/normal-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("bResult", "b")
		return nil
	})
	proxy.HandleTask("subgraphfanoutflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("xPassed", true)
		return nil
	})
	proxy.HandleTask("subgraphfanoutflow.verify:428/task-y", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetBool("xPassed") {
			f.SetString("subResult", "sub")
		} else {
			f.SetString("subResult", "sub-no-x")
		}
		return nil
	})
	proxy.HandleTask("subgraphfanoutflow.verify:428/run-sub", func(ctx context.Context, f *workflow.Flow) error {
		out, yield, err := f.Subgraph("subgraphfanoutflow.verify:428/sub", nil)
		if yield || err != nil {
			return err
		}
		if r, ok := out["subResult"]; ok {
			f.Set("subResult", r)
		}
		return nil
	})
	proxy.HandleTask("subgraphfanoutflow.verify:428/normal-d", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("dResult", "d")
		return nil
	})
	proxy.HandleTask("subgraphfanoutflow.verify:428/task-e", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", fmt.Sprintf("%s/%s/%s", f.GetString("bResult"), f.GetString("subResult"), f.GetString("dResult")))
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("subgraph_as_sibling_in_fan_out", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "subgraphfanoutflow.verify:428/sub-fan-out", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("b/sub/d", outcome.State["result"])
	})
}
