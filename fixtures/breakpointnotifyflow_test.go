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

// TestBreakpointnotifyflow verifies that a flow started with StartNotify fires the FlowStoppedCallback
// when it pauses at a breakpoint, exactly as it does for a flow.Interrupt pause. A breakpoint produces
// the interrupted status, which is a stop status StartNotify subscribers must be told about.
func TestBreakpointnotifyflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("breakpointnotifyflow.verify:428/flow")
	graph.AddTask("taskA", "breakpointnotifyflow.verify:428/task-a")
	graph.AddTask("taskB", "breakpointnotifyflow.verify:428/task-b")
	graph.AddTransition("taskA", "taskB")
	graph.AddTransition("taskB", workflow.END)
	proxy.HandleGraph("breakpointnotifyflow.verify:428/flow", graph)
	proxy.HandleTask("breakpointnotifyflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		return nil
	})
	proxy.HandleTask("breakpointnotifyflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		return nil
	})

	var notifiedHost atomic.Value
	var notifiedStatus atomic.Value
	notified := make(chan struct{}, 1)

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithFlowStoppedCallback(func(ctx context.Context, hostname string, outcome *workflow.FlowOutcome) {
			notifiedHost.Store(hostname)
			notifiedStatus.Store(outcome.Status)
			select {
			case notified <- struct{}{}:
			default:
			}
		})
	eng.RunInTest(t)

	flowKey, err := eng.Create(ctx, "breakpointnotifyflow.verify:428/flow", nil, nil, nil)
	if !assert.NoError(err) {
		return
	}
	// Breakpoint before taskB so the flow pauses (interrupted) after taskA.
	if !assert.NoError(eng.BreakBefore(ctx, flowKey, "breakpointnotifyflow.verify:428/task-b", true)) {
		return
	}
	if !assert.NoError(eng.StartNotify(ctx, flowKey, "host-breakpoint")) {
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
