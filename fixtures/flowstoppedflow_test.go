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
A flow created with FlowOptions.NotifyOnStop fires the host's FlowStopped callback with a FlowOutcome when
it stops, and the flow's baggage rides on the callback's context so the host can resolve where to deliver
the notification. This covers the three terminal stops — completed (State populated), failed (Error
populated), and cancelled (CancelReason populated) — and asserts the notify target carried in baggage is
delivered on the ctx.
*/
package fixtures

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

type stopEvent struct {
	hostname string
	flowKey  string
	outcome  *workflow.FlowOutcome
}

func TestFlowstoppedflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Flow")
	graph.SetEndpoint("Gate", "flowstoppedflow.verify:428/gate")
	graph.AddTransition("Gate", workflow.END)
	proxy.HandleGraph("flowstoppedflow.verify:428/flow", graph)

	proxy.HandleTask("flowstoppedflow.verify:428/gate", func(ctx context.Context, f *workflow.Flow) error {
		switch f.GetString("mode") {
		case "fail":
			return errors.New("gate refused", http.StatusInternalServerError)
		case "interrupt":
			yield, err := f.Interrupt(map[string]any{"need": "input"}, nil)
			if yield || err != nil {
				return err
			}
			return nil
		default:
			f.SetString("result", "ok")
			return nil
		}
	})

	// The callback is fire-and-forget; capture events on a buffered channel. The notify target rides in
	// baggage on the ctx (the engine carries no delivery address), so the host reads it from there.
	events := make(chan stopEvent, 16)
	cb := func(ctx context.Context, flowKey string, outcome *workflow.FlowOutcome) {
		host, _ := workflow.BaggageFrom(ctx).(map[string]any)["host"].(string)
		events <- stopEvent{hostname: host, flowKey: flowKey, outcome: outcome}
	}
	proxy.OnFlowStopped(cb)

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	// waitStop drains the channel for the stop event of a specific flow.
	waitStop := func(t *testing.T, flowKey string) stopEvent {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case ev := <-events:
				if ev.flowKey == flowKey {
					return ev
				}
			case <-deadline:
				t.Fatalf("no stop callback fired for %s", flowKey)
				return stopEvent{}
			}
		}
	}

	t.Run("completed_fires_callback_with_state", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "flowstoppedflow.verify:428/flow", map[string]any{"mode": "complete"},
			&workflow.FlowOptions{NotifyOnStop: true, Baggage: map[string]any{"host": "host-complete"}})
		if !assert.NoError(err) {
			return
		}
		ev := waitStop(t, flowKey)
		assert.Equal("host-complete", ev.hostname)
		assert.Equal(workflow.StatusCompleted, ev.outcome.Status)
		assert.Equal("ok", ev.outcome.State["result"])
	})

	t.Run("failed_fires_callback_with_error", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "flowstoppedflow.verify:428/flow", map[string]any{"mode": "fail"},
			&workflow.FlowOptions{NotifyOnStop: true, Baggage: map[string]any{"host": "host-fail"}})
		if !assert.NoError(err) {
			return
		}
		ev := waitStop(t, flowKey)
		assert.Equal("host-fail", ev.hostname)
		assert.Equal(workflow.StatusFailed, ev.outcome.Status)
		assert.Contains(ev.outcome.Error, "gate refused")
	})

	t.Run("cancelled_fires_callback_with_reason", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "flowstoppedflow.verify:428/flow", map[string]any{"mode": "interrupt"},
			&workflow.FlowOptions{NotifyOnStop: true, Baggage: map[string]any{"host": "host-cancel"}})
		if !assert.NoError(err) {
			return
		}
		// First stop is the interrupt; drain it before cancelling.
		ev := waitStop(t, flowKey)
		assert.Equal(workflow.StatusInterrupted, ev.outcome.Status)

		if !assert.NoError(eng.Cancel(ctx, flowKey, "operator abort")) {
			return
		}
		ev = waitStop(t, flowKey)
		assert.Equal("host-cancel", ev.hostname)
		assert.Equal(workflow.StatusCancelled, ev.outcome.Status)
		assert.Equal("operator abort", ev.outcome.CancelReason)
	})
}
