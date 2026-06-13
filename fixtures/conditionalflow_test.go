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

func TestConditionalflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("conditionalflow.verify:428/conditional")
	graph.AddTask("taskA", "conditionalflow.verify:428/task-a")
	graph.AddTask("taskHigh", "conditionalflow.verify:428/task-high")
	graph.AddTask("taskLow", "conditionalflow.verify:428/task-low")
	graph.AddTask("taskC", "conditionalflow.verify:428/task-c")
	graph.SetFanIn("taskC")
	graph.AddTransitionWhen("taskA", "taskHigh", "score >= 50")
	graph.AddTransitionWhen("taskA", "taskLow", "score < 50")
	graph.AddTransition("taskHigh", "taskC")
	graph.AddTransition("taskLow", "taskC")
	graph.AddTransition("taskC", workflow.END)
	proxy.HandleGraph("conditionalflow.verify:428/conditional", graph)

	proxy.HandleTask("conditionalflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/task-high", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		f.SetString("branch", "high")
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/task-low", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		f.SetString("branch", "low")
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("score_high_takes_high_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 80}
		outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("high", outcome.State["branch"])
	})

	t.Run("score_low_takes_low_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 20}
		outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("low", outcome.State["branch"])
	})

	t.Run("boundary_50_takes_high_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 50}
		outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("high", outcome.State["branch"])
	})
}
