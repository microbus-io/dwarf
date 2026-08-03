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

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestPanicflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// Graph 1: a bare panicking task with no onError -> the flow fails.
	bare := workflow.NewGraph("Bare")
	bare.SetEndpoint("Boom", "panicflow.verify:0/boom")
	bare.AddTransition("Boom", workflow.END)
	proxy.HandleGraph("panicflow.verify:0/bare", bare)

	// Graph 2: a panicking task with an onError handler -> the flow recovers via the handler.
	handled := workflow.NewGraph("Handled")
	handled.SetEndpoint("Boom", "panicflow.verify:0/boom")
	handled.SetEndpoint("Rescue", "panicflow.verify:0/rescue")
	handled.AddTransition("Boom", workflow.END)
	handled.AddTransitionOnError("Boom", "Rescue")
	handled.AddTransition("Rescue", workflow.END)
	proxy.HandleGraph("panicflow.verify:0/handled", handled)

	// Graph 3: a task reading a mistyped state field with a typed getter. The getters panic rather than
	// hand back a zero value the task never wrote, and this is what makes that safe: the panic is caught at
	// the task-call boundary, so the mistyped read is an ordinary step failure the graph's onError handles.
	mistyped := workflow.NewGraph("Mistyped")
	mistyped.SetEndpoint("Read", "panicflow.verify:0/read")
	mistyped.SetEndpoint("Rescue", "panicflow.verify:0/rescue")
	mistyped.AddTransition("Read", workflow.END)
	mistyped.AddTransitionOnError("Read", "Rescue")
	mistyped.AddTransition("Rescue", workflow.END)
	proxy.HandleGraph("panicflow.verify:0/mistyped", mistyped)

	proxy.HandleTask("panicflow.verify:0/boom", func(ctx context.Context, f *workflow.Flow) error {
		panic("boom")
	})
	proxy.HandleTask("panicflow.verify:0/read", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("delay", f.GetInt("retryAfter")) // retryAfter is 1.5 - a mistyped read, never a silent 0
		return nil
	})
	proxy.HandleTask("panicflow.verify:0/rescue", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("recovered", "yes")
		return nil
	})

	t.Run("panic_without_onError_fails_the_step", func(t *testing.T) {
		assert := testarossa.For(t)

		// A bounded Await: if the panic wedged the step, this would block until the deadline rather than
		// returning a prompt terminal outcome.
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		_, outcome, err := eng.Run(awaitCtx, "panicflow.verify:0/bare", nil, nil)
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

		_, outcome, err := eng.Run(awaitCtx, "panicflow.verify:0/handled", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("yes", stateVal(outcome.State, "recovered"))
	})

	t.Run("mistyped_getter_fails_the_step_and_routes_to_onError", func(t *testing.T) {
		assert := testarossa.For(t)

		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		_, outcome, err := eng.Run(awaitCtx, "panicflow.verify:0/mistyped",
			map[string]any{"retryAfter": 1.5}, nil)
		if !assert.NoError(err) {
			return
		}
		// Not a wedged step, and not a task that silently proceeded on a 0 it never wrote.
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("yes", stateVal(outcome.State, "recovered"))
		_, wroteDelay := stateVal(outcome.State, "delay"), outcome.State.Has("delay")
		assert.False(wroteDelay, "the task must not have proceeded past the mistyped read")
	})
}
