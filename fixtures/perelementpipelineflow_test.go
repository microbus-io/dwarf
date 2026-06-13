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
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestPerelementpipelineflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// S -> forEach(items) -> H -> {A, B} -> M -> L
	graph := workflow.NewGraph("perelementpipelineflow.verify:428/per-element-pipeline")
	graph.AddTask("taskS", "perelementpipelineflow.verify:428/task-s")
	graph.AddTask("taskH", "perelementpipelineflow.verify:428/task-h")
	graph.AddTask("taskA", "perelementpipelineflow.verify:428/task-a")
	graph.AddTask("taskB", "perelementpipelineflow.verify:428/task-b")
	graph.AddTask("taskM", "perelementpipelineflow.verify:428/task-m")
	graph.AddTask("taskL", "perelementpipelineflow.verify:428/task-l")
	graph.SetFanIn("taskM")
	graph.SetFanIn("taskL")
	graph.SetReducer("mergedItems", workflow.ReducerUnion)
	graph.AddTransitionForEach("taskS", "taskH", "items", "item")
	graph.AddTransition("taskH", "taskA")
	graph.AddTransition("taskH", "taskB")
	graph.AddTransition("taskA", "taskM")
	graph.AddTransition("taskB", "taskM")
	graph.AddTransition("taskM", "taskL")
	graph.AddTransition("taskL", workflow.END)
	proxy.HandleGraph("perelementpipelineflow.verify:428/per-element-pipeline", graph)

	proxy.HandleTask("perelementpipelineflow.verify:428/task-s", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		return nil
	})
	proxy.HandleTask("perelementpipelineflow.verify:428/task-h", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		f.SetString("item", strings.ToUpper(f.GetString("item")))
		return nil
	})
	proxy.HandleTask("perelementpipelineflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		f.SetString("aResult", "a:"+f.GetString("item"))
		return nil
	})
	proxy.HandleTask("perelementpipelineflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		f.SetString("bResult", "b:"+f.GetString("item"))
		return nil
	})
	proxy.HandleTask("perelementpipelineflow.verify:428/task-m", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		f.Set("mergedItems", []string{f.GetString("aResult") + "+" + f.GetString("bResult")})
		return nil
	})
	proxy.HandleTask("perelementpipelineflow.verify:428/task-l", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		var items []string
		f.Get("mergedItems", &items)
		f.SetInt("finalCount", len(items))
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("three_elements_produce_three_pipelines", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"items": []string{"x", "y", "z"}}
		outcome, err := eng.Run(ctx, "perelementpipelineflow.verify:428/per-element-pipeline", initialState, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3.0, outcome.State["finalCount"])
	})
}
