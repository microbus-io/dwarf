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
Cancel works on a created, running, OR interrupted flow. Existing fixtures cancel
running/created flows; this covers cancelling a flow parked at an interrupt, and
asserts the cancel reason surfaces on the outcome and a subsequent Resume is
rejected (the flow is terminal).
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestCancelinterruptedflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Flow")
	graph.SetEndpoint("TaskA", "cancelinterruptedflow.verify:428/task-a")
	graph.SetEndpoint("Pause", "cancelinterruptedflow.verify:428/pause")
	graph.SetEndpoint("TaskB", "cancelinterruptedflow.verify:428/task-b")
	graph.AddTransitionChain("TaskA", "Pause", "TaskB", workflow.END)
	proxy.HandleGraph("cancelinterruptedflow.verify:428/flow", graph)

	proxy.HandleTask("cancelinterruptedflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("cancelinterruptedflow.verify:428/pause", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"need": "input"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("cancelinterruptedflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", "reached B")
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("cancel_an_interrupted_flow", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "cancelinterruptedflow.verify:428/flow", nil, nil)
		if !assert.NoError(err) {
			return
		}

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)

		// Cancel the parked flow with a reason.
		if !assert.NoError(eng.Cancel(ctx, flowKey, "no longer needed")) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal("no longer needed", outcome.CancelReason)
		// The downstream task never ran.
		_, reachedB := outcome.State["result"]
		assert.False(reachedB)

		// Resuming a cancelled flow is rejected — it is terminal.
		err = eng.Resume(ctx, flowKey, map[string]any{"answer": "x"})
		assert.Error(err)
	})
}
