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

// TestErrorchangesflow pins that AN ERROR VOIDS THE TASK'S CHANGES - the same contract a Go function has when
// it returns an error: the other results are not to be trusted. A task that writes state and then returns an
// error has those writes DISCARDED, on both error paths (routed to an onError handler, or failing the flow).
//
// This is deliberate and not merely convenient. Execution is at-least-once: if a worker loses its lease
// mid-task a peer re-runs the task from the same input snapshot and RECOMPUTES its changes, so "what the
// failing attempt wrote before it died" is not a stable fact about the flow - it depends on which attempt you
// observed. Preserving it would be a record that looks dependable and is not, and compensation logic built on
// it would pass its tests and be wrong under lease recovery.
//
// A task that wants to tell its handler something puts it in the ERROR (onErr carries the message, status
// code, trace id and properties), or - for an external side effect that must be compensated - lives in its own
// task, so its success is durably recorded before anything downstream can fail.
func TestErrorchangesflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// Handled: Work errors after writing, and an onError handler runs next.
	handled := workflow.NewGraph("HandledErrorChanges")
	handled.SetEndpoint("Work", "errorchangesflow.verify:428/work")
	handled.SetEndpoint("Rescue", "errorchangesflow.verify:428/rescue")
	handled.AddTransition("Work", workflow.END)
	handled.AddTransitionOnError("Work", "Rescue")
	handled.AddTransition("Rescue", workflow.END)
	proxy.HandleGraph("errorchangesflow.verify:428/handled", handled)

	// Unhandled: the same task with no onError, so the flow fails.
	unhandled := workflow.NewGraph("UnhandledErrorChanges")
	unhandled.SetEndpoint("Work", "errorchangesflow.verify:428/work")
	unhandled.AddTransition("Work", workflow.END)
	proxy.HandleGraph("errorchangesflow.verify:428/unhandled", unhandled)

	var handlerSawScratch bool
	var handlerSawErr string

	proxy.HandleTask("errorchangesflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("scratch", "half-written") // a write the task makes before it fails
		if f.GetBool("ok") {
			return nil
		}
		return errors.New("Work failed after writing scratch")
	})
	proxy.HandleTask("errorchangesflow.verify:428/rescue", func(ctx context.Context, f *workflow.Flow) error {
		handlerSawScratch = f.Has("scratch")
		var onErr map[string]any
		_ = f.Get("onErr", &onErr)
		if msg, ok := onErr["error"].(string); ok {
			handlerSawErr = msg
		}
		f.SetString("rescued", "yes")
		return nil
	})

	t.Run("onError_handler_does_not_see_the_failed_task_s_changes", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "errorchangesflow.verify:428/handled", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status) // the handler recovered the flow
		assert.Equal("yes", stateVal(outcome.State, "rescued"))

		// The write the failing task made is gone - from the handler's state...
		assert.False(handlerSawScratch, "the onError handler must not see the failed task's changes")
		// ... and from the flow's final state.
		_, inFinal := stateVal(outcome.State, "scratch"), outcome.State.Has("scratch")
		assert.False(inFinal, "a failed task's changes must not reach final_state")

		// The error IS the channel to the handler: onErr carried the message through.
		assert.Equal("Work failed after writing scratch", handlerSawErr)
	})

	t.Run("unhandled_failure_also_drops_the_changes", func(t *testing.T) {
		assert := testarossa.For(t)

		// The other error path (failStep, no onError) must agree: it writes status/error only, never the
		// task's changes. If these two paths ever disagree, the contract is decided by whether the author
		// happened to declare a handler.
		_, outcome, err := eng.Run(ctx, "errorchangesflow.verify:428/unhandled", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusFailed, outcome.Status)
		_, inFinal := stateVal(outcome.State, "scratch"), outcome.State.Has("scratch")
		assert.False(inFinal, "a failed task's changes must not reach final_state on the failStep path either")
	})

	t.Run("a_successful_task_s_changes_are_kept", func(t *testing.T) {
		assert := testarossa.For(t)

		// The control: the same write survives when the task returns nil, so the drop above is the error
		// voiding the changes - not the write being lost some other way.
		_, outcome, err := eng.Run(ctx, "errorchangesflow.verify:428/unhandled",
			map[string]any{"ok": true}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("half-written", stateVal(outcome.State, "scratch"))
	})
}
