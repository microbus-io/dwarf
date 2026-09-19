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
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("Conditional")
	graph.SetEndpoint("TaskA", "conditionalflow.verify:428/task-a")
	graph.SetEndpoint("TaskHigh", "conditionalflow.verify:428/task-high")
	graph.SetEndpoint("TaskLow", "conditionalflow.verify:428/task-low")
	graph.SetEndpoint("TaskC", "conditionalflow.verify:428/task-c")
	graph.SetFanIn("TaskC")
	graph.AddTransitionWhen("TaskA", "TaskHigh", "score >= 50")
	graph.AddTransitionWhen("TaskA", "TaskLow", "score < 50")
	graph.AddTransition("TaskHigh", "TaskC")
	graph.AddTransitionChain("TaskLow", "TaskC", workflow.END)
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

	t.Run("score_high_takes_high_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 80}
		_, outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("high", stateVal(outcome.State, "branch"))
	})

	t.Run("score_low_takes_low_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 20}
		_, outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("low", stateVal(outcome.State, "branch"))
	})

	t.Run("boundary_50_takes_high_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"score": 50}
		_, outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("high", stateVal(outcome.State, "branch"))
	})
}

// A when expression now compares integers exactly rather than through a float64, which held integers
// only up to 2^53 and would have collapsed these two neighboring ids onto the same value.
func TestConditionalflow_ExactIntegerComparison(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	const id = 1234567890123456789
	const neighbor = 1234567890123456788

	graph := workflow.NewGraph("ConditionalExactID")
	graph.SetEndpoint("TaskA", "conditionalflow.verify:428/exact-task-a")
	graph.SetEndpoint("TaskMatch", "conditionalflow.verify:428/exact-task-match")
	graph.SetEndpoint("TaskNoMatch", "conditionalflow.verify:428/exact-task-nomatch")
	graph.SetEndpoint("TaskDone", "conditionalflow.verify:428/exact-task-done")
	graph.SetFanIn("TaskDone")
	graph.AddTransitionWhen("TaskA", "TaskMatch", "orderID == 1234567890123456789")
	graph.AddTransitionWhen("TaskA", "TaskNoMatch", "orderID != 1234567890123456789")
	graph.AddTransition("TaskMatch", "TaskDone")
	graph.AddTransition("TaskNoMatch", "TaskDone")
	graph.AddTransition("TaskDone", workflow.END)
	proxy.HandleGraph("conditionalflow.verify:428/conditional-exact-id", graph)

	proxy.HandleTask("conditionalflow.verify:428/exact-task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/exact-task-match", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "match")
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/exact-task-nomatch", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "no-match")
		return nil
	})
	proxy.HandleTask("conditionalflow.verify:428/exact-task-done", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	t.Run("exact_id_matches", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"orderID": int64(id)}
		_, outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional-exact-id", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("match", stateVal(outcome.State, "branch"))
	})

	t.Run("neighboring_id_does_not_match", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"orderID": int64(neighbor)}
		_, outcome, err := eng.Run(ctx, "conditionalflow.verify:428/conditional-exact-id", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("no-match", stateVal(outcome.State, "branch"))
	})
}
