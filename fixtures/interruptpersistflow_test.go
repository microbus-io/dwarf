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
	"github.com/microbus-io/testarossa"
)

// TestInterruptpersistflow verifies that a state mutation made BEFORE flow.Interrupt, in the same
// dispatch that yields, is persisted with the parked step and is visible when the task re-runs on
// Resume. This is the invariant that makes a side-effecting park idempotent: a task that creates an
// external resource, records its identifier in state, and then parks must NOT re-create the resource
// when its body replays from the top on re-entry, because the recorded identifier is already in state.
//
// The crux assertion is the creation counter: if the pre-park write were dropped at the park, the
// guard would see an empty identifier on re-entry and "create" the resource a second time.
func TestInterruptpersistflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("InterruptPersist")
	graph.SetEndpoint("ParkAfterWrite", "interruptpersistflow.verify:428/park-after-write")
	graph.SetEndpoint("Verify", "interruptpersistflow.verify:428/verify")
	graph.AddTransitionChain("ParkAfterWrite", "Verify", workflow.END)
	proxy.HandleGraph("interruptpersistflow.verify:428/interrupt-persist", graph)

	// creations counts how many times the task "created" the external resource. It must stay 1
	// across the park-and-resume cycle: the body replays on re-entry, but the guard reads the
	// ticketID written before the park and skips creation.
	creations := 0
	proxy.HandleTask("interruptpersistflow.verify:428/park-after-write", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("ticketID") == "" {
			// Simulate creating an external resource and recording its id, then parking - all in the
			// dispatch that yields. The write must survive the park.
			creations++
			f.SetString("ticketID", "TICKET-1")
		}
		yield, err := f.Interrupt(map[string]any{"awaiting": "resolution"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("interruptpersistflow.verify:428/verify", func(ctx context.Context, f *workflow.Flow) error {
		// Prove the pre-park write also flows downstream past the parked step.
		f.SetString("seenTicketID", f.GetString("ticketID"))
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("write_before_park_survives_resume", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "interruptpersistflow.verify:428/interrupt-persist", nil, nil)
		if !assert.NoError(err) {
			return
		}

		// The task records the ticket id, then parks.
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)
		// The pre-park write is already persisted on the interrupted flow's state.
		assert.Equal("TICKET-1", outcome.State["ticketID"])
		assert.Equal(1, creations)

		// Resume re-dispatches the same step; the body replays from the top.
		err = eng.Resume(ctx, flowKey, map[string]any{})
		if !assert.NoError(err) {
			return
		}

		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		// The id written before the park survived and reached the downstream task...
		assert.Equal("TICKET-1", outcome.State["ticketID"])
		assert.Equal("TICKET-1", outcome.State["seenTicketID"])
		// ...and the guard held on re-entry, so the resource was created exactly once.
		assert.Equal(1, creations)
	})
}
