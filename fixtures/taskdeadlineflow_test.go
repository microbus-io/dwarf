/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestTaskdeadlineflow verifies that the engine applies the step's time_budget_ms (set via
// WithTimeBudget) as the deadline on the context handed to the TaskExecutor. Unlike timebudgetflow,
// the task here installs NO timeout of its own — it blocks purely on the engine-provided ctx.Done(),
// mimicking how the Microbus connector enforces sub.TimeBudget and returns a 408 when the unicast
// deadline lapses. If the engine fails to deadline the call (the migration regression), ctx.Done()
// never fires, the task only returns when its safety net trips, and the flow completes instead of
// failing — which this fixture catches.
func TestTaskdeadlineflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("taskdeadlineflow.verify:428/flow")
	graph.AddTask("work", "taskdeadlineflow.verify:428/work")
	graph.AddTransition("work", workflow.END)
	proxy.HandleGraph("taskdeadlineflow.verify:428/flow", graph)

	const budget = 300 * time.Millisecond
	const safety = 3 * time.Second

	var deadlineObserved atomic.Bool
	proxy.HandleTask("taskdeadlineflow.verify:428/work", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		// No self-installed timeout: rely solely on the engine-provided budget deadline.
		select {
		case <-ctx.Done():
			// The connector surfaces a lapsed unicast deadline as a 408 RequestTimeout.
			deadlineObserved.Store(true)
			return errors.New("task deadline exceeded", http.StatusRequestTimeout)
		case <-time.After(safety):
			// Safety net so a missing deadline can't hang the worker pool on shutdown.
			return nil
		}
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithTimeBudget(budget)
	eng.RunInTest(t)

	start := time.Now()
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	outcome, err := eng.Run(timeoutCtx, "taskdeadlineflow.verify:428/flow", nil, nil, nil)
	elapsed := time.Since(start)
	if !assert.NoError(err) {
		return
	}

	// The engine-applied deadline must have fired the task's ctx (not the safety net).
	assert.True(deadlineObserved.Load())
	// The timed-out task error fails the flow.
	assert.Equal(workflow.StatusFailed, outcome.Status)
	// And it must happen at ~budget, well before the safety net would have returned nil.
	assert.True(elapsed < safety)
}
