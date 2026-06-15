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

	graph := workflow.NewGraph("Conditional", "conditionalflow.verify:428/conditional")
	graph.AddTask("TaskA", "conditionalflow.verify:428/task-a")
	graph.AddTask("TaskHigh", "conditionalflow.verify:428/task-high")
	graph.AddTask("TaskLow", "conditionalflow.verify:428/task-low")
	graph.AddTask("TaskC", "conditionalflow.verify:428/task-c")
	graph.SetFanIn("TaskC")
	graph.AddTransitionWhen("TaskA", "TaskHigh", "score >= 50")
	graph.AddTransitionWhen("TaskA", "TaskLow", "score < 50")
	graph.AddTransition("TaskHigh", "TaskC")
	graph.AddTransition("TaskLow", "TaskC")
	graph.AddTransition("TaskC", workflow.END)
	proxy.HandleGraph("conditionalflow.verify:428/conditional", graph)

	proxy.HandleTask("conditionalflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/task-high", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "high")
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/task-low", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "low")
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("score_high_takes_high_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 80}
		outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("high", outcome.State["branch"])
	})

	t.Run("score_low_takes_low_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 20}
		outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("low", outcome.State["branch"])
	})

	t.Run("boundary_50_takes_high_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 50}
		outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("high", outcome.State["branch"])
	})
}
