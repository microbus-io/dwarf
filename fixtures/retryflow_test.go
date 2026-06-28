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
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	graph := workflow.NewGraph("Retry")
	graph.SetEndpoint("TaskA", "retryflow.verify:428/task-a")
	graph.SetEndpoint("Flaky", "retryflow.verify:428/flaky")
	graph.SetEndpoint("TaskB", "retryflow.verify:428/task-b")
	graph.AddTransitionChain("TaskA", "Flaky", "TaskB", workflow.END)
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
		// Bound by count via Attempt(): up to 5 retries (attempts 0..4), then give up.
		if f.Attempt() >= 5 {
			return errors.New("flaky exhausted retries at attempt %d", attempts)
		}
		f.Retry(0, 0, 0, 0)
		return nil
	})
	proxy.HandleTask("retryflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("finalAttempts", f.GetInt("attempts"))
		return nil
	})

	t.Run("succeeds_on_target_attempt", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"target": 3}
		_, outcome, err := eng.Run(ctx, "retryflow.verify:428/retry", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3.0, outcome.State["finalAttempts"])
	})

	t.Run("exhausts_retries_and_fails", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"target": 10}
		_, outcome, err := eng.Run(ctx, "retryflow.verify:428/retry", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, outcome.Status)
	})
}
