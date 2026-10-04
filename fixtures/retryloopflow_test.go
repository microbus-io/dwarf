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
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestRetryloopflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := engine.NewHostMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("RetryLoop")
	graph.SetURL("TaskA", "retryloopflow.verify:428/task-a")
	graph.SetURL("TaskB", "retryloopflow.verify:428/task-b")
	graph.SetURL("Handler", "retryloopflow.verify:428/handler")
	graph.SetURL("TaskC", "retryloopflow.verify:428/task-c")
	graph.AddTransition("TaskA", "TaskB")
	graph.AddTransitionOnError("TaskB", "Handler")
	graph.AddTransition("TaskB", "TaskC")
	graph.AddTransition("Handler", "TaskB")
	graph.AddTransition("TaskC", workflow.End)
	mux.HandleGraph("retryloopflow.verify:428/retry-loop", graph)

	mux.HandleTask("retryloopflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("retryloopflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("attempts") >= f.GetInt("target") {
			return nil
		}
		return errors.New("not yet")
	})
	mux.HandleTask("retryloopflow.verify:428/handler", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("attempts", f.GetInt("attempts")+1)
		return nil
	})
	mux.HandleTask("retryloopflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("finalAttempts", f.GetInt("attempts"))
		return nil
	})

	t.Run("loops_until_target_then_succeeds", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"target": 3}
		_, outcome, err := eng.Run(ctx, "retryloopflow.verify:428/retry-loop", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3.0, stateVal(outcome.State, "finalAttempts"))
	})
}
