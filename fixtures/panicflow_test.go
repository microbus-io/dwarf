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

/*
The host runs in-process, so a panic in a task handler is caught at the host-call
boundary and treated as any other task error: it flows through the graph's onError
transition if one exists, else it fails the step. The step never wedges `running`
until lease expiry, and the process survives. Covers "Host-call panic isolation".
*/
package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestPanicflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Graph 1: a bare panicking task with no onError -> the flow fails.
	bare := workflow.NewGraph("Bare")
	bare.SetEndpoint("Boom", "panicflow.verify:0/boom")
	bare.AddTransition("Boom", workflow.END)
	commonProxy.HandleGraph("panicflow.verify:0/bare", bare)

	// Graph 2: a panicking task with an onError handler -> the flow recovers via the handler.
	handled := workflow.NewGraph("Handled")
	handled.SetEndpoint("Boom", "panicflow.verify:0/boom")
	handled.SetEndpoint("Rescue", "panicflow.verify:0/rescue")
	handled.AddTransition("Boom", workflow.END)
	handled.AddTransitionOnError("Boom", "Rescue")
	handled.AddTransition("Rescue", workflow.END)
	commonProxy.HandleGraph("panicflow.verify:0/handled", handled)

	commonProxy.HandleTask("panicflow.verify:0/boom", func(ctx context.Context, f *workflow.Flow) error {
		var nilMap map[string]int
		nilMap["x"] = 1 // assignment to entry in nil map -> panic
		return nil
	})
	commonProxy.HandleTask("panicflow.verify:0/rescue", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("recovered", "yes")
		return nil
	})

	t.Run("panic_without_onError_fails_the_step", func(t *testing.T) {
		assert := testarossa.For(t)

		// A bounded Await: if the panic wedged the step, this would block until the deadline rather than
		// returning a prompt terminal outcome.
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		_, outcome, err := commonEngine.Run(awaitCtx, "panicflow.verify:0/bare", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.True(outcome.Error != "")
	})

	t.Run("panic_with_onError_routes_to_handler", func(t *testing.T) {
		assert := testarossa.For(t)

		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		_, outcome, err := commonEngine.Run(awaitCtx, "panicflow.verify:0/handled", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("yes", outcome.State["recovered"])
	})
}
