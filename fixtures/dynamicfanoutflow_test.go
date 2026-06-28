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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestDynamicfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	graph := workflow.NewGraph("DynamicFanOut")
	graph.SetEndpoint("TaskA", "dynamicfanoutflow.verify:428/task-a")
	graph.SetEndpoint("TaskB", "dynamicfanoutflow.verify:428/task-b")
	graph.SetEndpoint("TaskC", "dynamicfanoutflow.verify:428/task-c")
	graph.SetFanIn("TaskC")
	graph.SetReducer("processed", workflow.ReducerAdd)
	graph.SetReducer("seenIndices", workflow.ReducerAppend)
	graph.SetReducer("seenCounts", workflow.ReducerUnion)
	graph.AddTransitionForEach("TaskA", "TaskB", "items", "item")
	graph.AddTransitionChain("TaskB", "TaskC", workflow.END)
	commonProxy.HandleGraph("dynamicfanoutflow.verify:428/dynamic-fan-out", graph)

	commonProxy.HandleTask("dynamicfanoutflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	commonProxy.HandleTask("dynamicfanoutflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("item") == "" {
			return nil
		}
		f.SetInt("processed", 1)
		f.Set("seenIndices", []int{f.GetInt("itemIndex")})
		f.Set("seenCounts", []int{f.GetInt("itemCount")})
		return nil
	})
	commonProxy.HandleTask("dynamicfanoutflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("processedCount", f.GetInt("processed"))
		return nil
	})

	t.Run("three_elements", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"items": []string{"x", "y", "z"}}
		_, outcome, err := commonEngine.Run(ctx, "dynamicfanoutflow.verify:428/dynamic-fan-out", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3.0, outcome.State["processedCount"])
	})

	t.Run("single_element", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"items": []string{"only"}}
		_, outcome, err := commonEngine.Run(ctx, "dynamicfanoutflow.verify:428/dynamic-fan-out", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(1.0, outcome.State["processedCount"])
	})

	t.Run("empty_list_completes_at_for_each_source", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"items": []string{}}
		_, outcome, err := commonEngine.Run(ctx, "dynamicfanoutflow.verify:428/dynamic-fan-out", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	})
}
