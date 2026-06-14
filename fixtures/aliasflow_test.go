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

func TestAliasflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Alias", "aliasflow.verify:428/alias")
	graph.AddTask("S", "aliasflow.verify:428/task-s")
	graph.AddTask("A", "aliasflow.verify:428/task-a")
	graph.AddTask("B", "aliasflow.verify:428/task-b")
	graph.AddTask("C", "aliasflow.verify:428/task-c")
	graph.AddTask("BPrime", "aliasflow.verify:428/task-b") // same URL as "B"
	graph.AddTask("D", "aliasflow.verify:428/task-d")
	graph.AddTransition("S", "A")
	graph.AddTransitionGoto("S", "BPrime")
	graph.AddTransition("A", "B")
	graph.AddTransition("B", "C")
	graph.AddTransition("C", workflow.END)
	graph.AddTransition("BPrime", "D")
	graph.AddTransition("D", workflow.END)
	proxy.HandleGraph("aliasflow.verify:428/alias", graph)

	proxy.HandleTask("aliasflow.verify:428/task-s", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("branch") == "alt" {
			f.Goto("BPrime")
		}
		return nil
	})
	proxy.HandleTask("aliasflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("path", f.GetString("path")+"A")
		return nil
	})
	proxy.HandleTask("aliasflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("path", f.GetString("path")+"B")
		return nil
	})
	proxy.HandleTask("aliasflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("path", f.GetString("path")+"C")
		return nil
	})
	proxy.HandleTask("aliasflow.verify:428/task-d", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("path", f.GetString("path")+"D")
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("default_path_runs_s_a_b_c", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"branch": ""}
		outcome, err := eng.Run(ctx, "aliasflow.verify:428/alias", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("ABC", outcome.State["path"])
	})

	t.Run("alt_path_runs_s_bPrime_d", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"branch": "alt"}
		outcome, err := eng.Run(ctx, "aliasflow.verify:428/alias", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("BD", outcome.State["path"])
	})

	t.Run("history_distinguishes_b_and_bPrime_by_node_name", func(t *testing.T) {
		assert := testarossa.For(t)

		// Default path: history should include "B" but not "BPrime".
		flowKey, err := eng.Create(ctx, "aliasflow.verify:428/alias", map[string]any{"branch": ""}, nil)
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
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		history, err := eng.History(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		nodeNames := map[string]int{}
		for _, s := range history {
			nodeNames[s.TaskName]++
		}
		assert.Equal(1, nodeNames["S"])
		assert.Equal(1, nodeNames["A"])
		assert.Equal(1, nodeNames["B"])
		assert.Equal(1, nodeNames["C"])
		assert.Equal(0, nodeNames["BPrime"])
		assert.Equal(0, nodeNames["D"])

		// Alt path: history should include "BPrime" but not "B".
		flowKey, err = eng.Create(ctx, "aliasflow.verify:428/alias", map[string]any{"branch": "alt"}, nil)
		if !assert.NoError(err) {
			return
		}
		err = eng.Start(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		history, err = eng.History(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		nodeNames = map[string]int{}
		for _, s := range history {
			nodeNames[s.TaskName]++
		}
		assert.Equal(1, nodeNames["S"])
		assert.Equal(1, nodeNames["BPrime"])
		assert.Equal(1, nodeNames["D"])
		assert.Equal(0, nodeNames["A"])
		assert.Equal(0, nodeNames["B"])
		assert.Equal(0, nodeNames["C"])
	})
}
