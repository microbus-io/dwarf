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
For an interrupted flow, the engine deliberately keeps the raw flow.Interrupt
payload OUT of State: Snapshot/Await return State as the merged step snapshot at
the interrupt point and InterruptPayload as the separate, raw payload. A caller
wanting the combined view merges them itself with workflow.MergeState. This
asserts the split (the earlier folding-into-State behavior was lossy).
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestInterruptpayloadflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Interrupt")
	graph.SetEndpoint("Setup", "interruptpayloadflow.verify:428/setup")
	graph.SetEndpoint("Ask", "interruptpayloadflow.verify:428/ask")
	graph.AddTransitionChain("Setup", "Ask", workflow.END)
	proxy.HandleGraph("interruptpayloadflow.verify:428/interrupt", graph)

	proxy.HandleTask("interruptpayloadflow.verify:428/setup", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("prompt", "choose")
		return nil
	})
	proxy.HandleTask("interruptpayloadflow.verify:428/ask", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{
			"question": "pick one",
			"options":  []string{"a", "b"},
		}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("payload_is_separate_from_state", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "interruptpayloadflow.verify:428/interrupt", nil, nil)
		if !assert.NoError(err) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)

		// The raw payload is exposed via InterruptPayload.
		assert.Equal("pick one", outcome.InterruptPayload["question"])

		// State is the workflow's own state at the interrupt point: it carries the
		// task's prior output ("prompt") but NOT the interrupt payload fields.
		assert.Equal("choose", outcome.State["prompt"])
		_, hasQuestion := outcome.State["question"]
		assert.False(hasQuestion, "payload field leaked into State")

		// A caller wanting the combined view merges them explicitly.
		graph, err := proxy.LoadGraph(ctx, "interruptpayloadflow.verify:428/interrupt")
		if !assert.NoError(err) {
			return
		}
		merged, err := workflow.MergeState(outcome.State, outcome.InterruptPayload, graph.Reducers())
		if !assert.NoError(err) {
			return
		}
		assert.Equal("choose", merged["prompt"])
		assert.Equal("pick one", merged["question"])
	})
}
