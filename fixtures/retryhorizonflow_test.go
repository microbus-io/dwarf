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
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestRetryHorizonflow pins flow.Retry's giveUpAfter bound end to end (distinct from count-based give-up via
// Attempt(), which retryflow_test covers). The bound is the total backoff: Retry returns true while the delays of
// every attempt so far, the next included, sum to at most giveUpAfter - so an always-failing task retries a fixed
// number of times and then fails, however slowly the engine dispatches it.
func TestRetryHorizonflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	graph := workflow.NewGraph("RetryHorizon")
	graph.SetURL("Flaky", "retryhorizonflow.verify:428/flaky")
	graph.SetURL("Done", "retryhorizonflow.verify:428/done")
	graph.AddTransitionChain("Flaky", "Done", workflow.End)
	mux.HandleGraph("retryhorizonflow.verify:428/retry-horizon", graph)

	// Flaky reads its own policy from state: succeed once Attempt() reaches successAt (0 = never); otherwise
	// retry against a giveUpAfter bound read from giveUpMs.
	mux.HandleTask("retryhorizonflow.verify:428/flaky", func(ctx context.Context, f *workflow.Flow) error {
		successAt := f.GetInt("successAt")
		if successAt > 0 && f.Attempt() >= successAt {
			return nil
		}
		horizon := time.Duration(f.GetInt("giveUpMs")) * time.Millisecond
		if yield, err := f.Retry(100*time.Millisecond, 2.0, 400*time.Millisecond, horizon); yield || err != nil {
			return err // task returns nil while a further attempt is still within the horizon
		}
		return errors.New("flaky exhausted its retry horizon")
	})
	mux.HandleTask("retryhorizonflow.verify:428/done", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("reachedDone", true)
		return nil
	})

	// flakyAttempts returns the attempt counter recorded on the Flaky step (0-indexed: 0 = ran once).
	flakyAttempts := func(t *testing.T, flowKey string) int {
		assert := testarossa.For(t)
		steps, err := eng.History(ctx, flowKey)
		assert.NoError(err)
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

		// Never succeeds; a 2s bound stops the retries and fails the flow. The delays 100/200/400/400/400/400ms sum
		// to 1.9s through attempt 5, and attempt 6's would take the total to 2.3s, so attempt 6 gives up - whatever
		// the dispatch latency, which a clock-based bound would have spent.
		flowKey, err := eng.Create(ctx, "retryhorizonflow.verify:428/retry-horizon",
			map[string]any{"successAt": 0, "giveUpMs": 2000}, nil)
		assert.NoError(err)

		outcome, err := eng.Await(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, outcome.Status)

		attempts := flakyAttempts(t, flowKey)
		assert.Equal(6, attempts, "the bound must give up exactly when the total backoff would pass it")
		assert.True(stateVal(outcome.State, "reachedDone") == nil, "Done must not run when Flaky exhausts its horizon")
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
		assert.Equal(true, stateVal(outcome.State, "reachedDone"))
		assert.Equal(2, flakyAttempts(t, flowKey), "should have taken exactly 2 retries before success")
	})
}
