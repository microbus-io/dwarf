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
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestRetryHorizonflow pins flow.Retry's wall-clock giveUpAfter horizon (distinct from count-based give-up
// via Attempt(), which retryflow_test covers). The horizon is measured from the step's first creation:
// Retry returns true while the next attempt would still land within giveUpAfter, else false - so an
// always-failing task with a bounded horizon retries a few times and then fails, rather than looping
// forever or giving up on the first try.
func TestRetryHorizonflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	graph := workflow.NewGraph("RetryHorizon")
	graph.SetEndpoint("Flaky", "retryhorizonflow.verify:428/flaky")
	graph.SetEndpoint("Done", "retryhorizonflow.verify:428/done")
	graph.AddTransitionChain("Flaky", "Done", workflow.END)
	proxy.HandleGraph("retryhorizonflow.verify:428/retry-horizon", graph)

	// Flaky reads its own policy from state: succeed once Attempt() reaches successAt (0 = never); otherwise
	// retry against a giveUpAfter horizon read from giveUpMs. Backoff is short so the horizon is reached fast.
	proxy.HandleTask("retryhorizonflow.verify:428/flaky", func(ctx context.Context, f *workflow.Flow) error {
		successAt := f.GetInt("successAt")
		if successAt > 0 && f.Attempt() >= successAt {
			return nil
		}
		horizon := time.Duration(f.GetInt("giveUpMs")) * time.Millisecond
		if f.Retry(20*time.Millisecond, 2.0, 60*time.Millisecond, horizon) {
			return nil // task returns nil while a further attempt is still within the horizon
		}
		return errors.New("flaky exhausted its retry horizon")
	})
	proxy.HandleTask("retryhorizonflow.verify:428/done", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("reachedDone", true)
		return nil
	})

	// flakyAttempts returns the attempt counter recorded on the Flaky step (0-indexed: 0 = ran once).
	flakyAttempts := func(t *testing.T, flowKey string) int {
		steps, err := eng.History(ctx, flowKey)
		testarossa.NoError(t, err)
		for _, s := range steps {
			if s.TaskName == "Flaky" {
				return s.Attempt
			}
		}
		t.Fatalf("no Flaky step in history of %s", flowKey)
		return 0
	}

	t.Run("gives_up_after_horizon", func(t *testing.T) {
		assert := testarossa.For(t)

		// Never succeeds; a ~250ms horizon must stop the retries and fail the flow.
		flowKey, err := eng.Create(ctx, "retryhorizonflow.verify:428/retry-horizon",
			map[string]any{"successAt": 0, "giveUpMs": 250}, nil)
		assert.NoError(err)

		outcome, err := eng.Await(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, outcome.Status)

		attempts := flakyAttempts(t, flowKey)
		// It must have retried at least once (horizon is not first-try) but still given up (not unbounded).
		assert.True(attempts >= 1, "expected the horizon to allow retries, got attempt=%d", attempts)
		assert.True(attempts <= 12, "expected the horizon to bound retries, got attempt=%d", attempts)
		assert.True(outcome.State["reachedDone"] == nil, "Done must not run when Flaky exhausts its horizon")
	})

	t.Run("succeeds_within_horizon", func(t *testing.T) {
		assert := testarossa.For(t)

		// Succeeds on the 3rd dispatch (Attempt()==2), well within a generous 10s horizon.
		flowKey, err := eng.Create(ctx, "retryhorizonflow.verify:428/retry-horizon",
			map[string]any{"successAt": 2, "giveUpMs": 10000}, nil)
		assert.NoError(err)

		outcome, err := eng.Await(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, outcome.State["reachedDone"])
		assert.Equal(2, flakyAttempts(t, flowKey), "should have taken exactly 2 retries before success")
	})
}
