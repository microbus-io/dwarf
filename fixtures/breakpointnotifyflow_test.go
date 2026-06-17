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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestBreakpointnotifyflow verifies that a flow created with FlowOptions.NotifyOnStop fires the host's
// FlowStopped callback when it pauses at a breakpoint, exactly as it does for a flow.Interrupt pause. A
// breakpoint produces the interrupted status, which is a stop status NotifyOnStop subscribers must be told
// about. The notify target rides in baggage on the callback ctx.
func TestBreakpointnotifyflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Flow")
	graph.SetEndpoint("TaskA", "breakpointnotifyflow.verify:428/task-a")
	graph.SetEndpoint("TaskB", "breakpointnotifyflow.verify:428/task-b")
	graph.AddTransitionChain("TaskA", "TaskB", workflow.END)
	proxy.HandleGraph("breakpointnotifyflow.verify:428/flow", graph)
	proxy.HandleTask("breakpointnotifyflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("breakpointnotifyflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	var notifiedHost atomic.Value
	var notifiedStatus atomic.Value
	notified := make(chan struct{}, 1)

	proxy.OnFlowStopped(func(ctx context.Context, flowKey string, outcome *workflow.FlowOutcome) {
		host, _ := workflow.BaggageFrom(ctx).(map[string]any)["host"].(string)
		notifiedHost.Store(host)
		notifiedStatus.Store(outcome.Status)
		select {
		case notified <- struct{}{}:
		default:
		}
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	flowKey, err := eng.Create(ctx, "breakpointnotifyflow.verify:428/flow", nil,
		&workflow.FlowOptions{NotifyOnStop: true, Baggage: map[string]any{"host": "host-breakpoint"}})
	if !assert.NoError(err) {
		return
	}
	// Breakpoint before taskB so the flow pauses (interrupted) after taskA.
	if !assert.NoError(eng.BreakBefore(ctx, flowKey, "TaskB", true)) {
		return
	}
	if !assert.NoError(eng.Start(ctx, flowKey)) {
		return
	}

	// The FlowStoppedCallback must fire for the breakpoint pause.
	select {
	case <-notified:
	case <-time.After(10 * time.Second):
		t.Fatal("FlowStoppedCallback never fired for the breakpoint pause")
	}
	assert.Equal("host-breakpoint", notifiedHost.Load())
	assert.Equal(workflow.StatusInterrupted, notifiedStatus.Load())
}
