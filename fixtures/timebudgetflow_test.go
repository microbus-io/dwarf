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
	"github.com/microbus-io/testarossa"
)

func TestTimebudgetflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("TimeBudget")
	graph.SetEndpoint("TaskA", "timebudgetflow.verify:428/task-a")
	graph.SetEndpoint("Slow", "timebudgetflow.verify:428/slow")
	graph.AddTransition("TaskA", "Slow")
	graph.AddTransition("Slow", workflow.END)
	proxy.HandleGraph("timebudgetflow.verify:428/time-budget", graph)

	proxy.HandleTask("timebudgetflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// In Microbus, the connector enforced a per-subscription time budget (sub.TimeBudget).
	// In dwarf, the task handler enforces its own timeout.
	proxy.HandleTask("timebudgetflow.verify:428/slow", func(ctx context.Context, f *workflow.Flow) error {
		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		select {
		case <-time.After(500 * time.Millisecond):
			f.SetBool("done", true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("slow_task_exceeds_budget_and_fails_flow", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "timebudgetflow.verify:428/time-budget", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, outcome.Status)
	})
}
