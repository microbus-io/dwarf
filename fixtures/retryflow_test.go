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

func TestRetryflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("retryflow.verify:428/retry")
	graph.AddTask("taskA", "retryflow.verify:428/task-a")
	graph.AddTask("flaky", "retryflow.verify:428/flaky")
	graph.AddTask("taskB", "retryflow.verify:428/task-b")
	graph.AddTransition("taskA", "flaky")
	graph.AddTransition("flaky", "taskB")
	graph.AddTransition("taskB", workflow.END)
	proxy.HandleGraph("retryflow.verify:428/retry", graph)

	proxy.HandleTask("retryflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("retryflow.verify:428/flaky", func(ctx context.Context, f *workflow.Flow) error {
		attempts := f.GetInt("attempts") + 1
		f.SetInt("attempts", attempts)
		if attempts >= f.GetInt("target") {
			return nil
		}
		if !f.Retry(5, 0, 0, 0) {
			return errors.New("flaky exhausted retries at attempt %d", attempts)
		}
		return nil
	})
	proxy.HandleTask("retryflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("finalAttempts", f.GetInt("attempts"))
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("succeeds_on_target_attempt", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"target": 3}
		outcome, err := eng.Run(ctx, "retryflow.verify:428/retry", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3.0, outcome.State["finalAttempts"])
	})

	t.Run("exhausts_retries_and_fails", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"target": 10}
		outcome, err := eng.Run(ctx, "retryflow.verify:428/retry", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, outcome.Status)
	})
}
