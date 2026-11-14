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
the interrupt point and InterruptPayload as the separate, raw payload. This
asserts the split (the earlier folding-into-State behavior was lossy). A caller
that wants the two combined applies the payload onto the state itself, field by
field - a resume request is not fan-in data, so it does not go through the graph's
reducers.
*/
package fixtures

import (
	"context"
	"fmt"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestInterruptpayloadflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	graph := workflow.NewGraph("Interrupt")
	graph.SetURL("Setup", "interruptpayloadflow.verify:428/setup")
	graph.SetURL("Ask", "interruptpayloadflow.verify:428/ask")
	graph.AddTransitionChain("Setup", "Ask", workflow.End)
	mux.HandleGraph("interruptpayloadflow.verify:428/interrupt", graph)

	mux.HandleTask("interruptpayloadflow.verify:428/setup", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("prompt", "choose")
		return nil
	})
	mux.HandleTask("interruptpayloadflow.verify:428/ask", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{
			"question": "pick one",
			"options":  []string{"a", "b"},
		}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

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
		assert.Equal("pick one", stateVal(outcome.InterruptPayload, "question"))

		// State is the workflow's own state at the interrupt point: it carries the
		// task's prior output ("prompt") but NOT the interrupt payload fields.
		assert.Equal("choose", stateVal(outcome.State, "prompt"))
		_, hasQuestion := stateVal(outcome.State, "question"), outcome.State.Has("question")
		assert.False(hasQuestion, "payload field leaked into State")
	})
}

// TestInterruptpayloadflow_AskAgain asks twice through the same row: a step that interrupts, is resumed, retries and
// interrupts again, and a caller whose child interrupts twice. Resume empties the payload of every step on the
// chain it resumes, and the next interrupt writes its payload only where the column is empty; each later prompt
// must still reach Snapshot.
func TestInterruptpayloadflow_AskAgain(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	again := workflow.NewGraph("AskAgain")
	again.SetURL("Ask", "interruptpayloadflow.verify:428/askagain")
	again.AddTransition("Ask", workflow.End)
	mux.HandleGraph("interruptpayloadflow.verify:428/again", again)
	mux.HandleTask("interruptpayloadflow.verify:428/askagain", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"question": fmt.Sprintf("round %d", f.Attempt())}, nil)
		if yield || err != nil {
			return err
		}
		if f.Attempt() == 0 {
			f.Retry(0, 1, 0, 0) // ask again, on the same step row
		}
		return nil
	})

	caller := workflow.NewGraph("Caller")
	caller.SetURL("Call", "interruptpayloadflow.verify:428/call")
	caller.AddTransition("Call", workflow.End)
	mux.HandleGraph("interruptpayloadflow.verify:428/caller", caller)
	child := workflow.NewGraph("Child")
	child.SetURL("First", "interruptpayloadflow.verify:428/first")
	child.SetURL("Second", "interruptpayloadflow.verify:428/second")
	child.AddTransitionChain("First", "Second", workflow.End)
	mux.HandleGraph("interruptpayloadflow.verify:428/child", child)
	mux.HandleTask("interruptpayloadflow.verify:428/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("interruptpayloadflow.verify:428/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	for _, name := range []string{"first", "second"} {
		mux.HandleTask("interruptpayloadflow.verify:428/"+name, func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Interrupt(map[string]any{"question": name}, nil)
			if yield || err != nil {
				return err
			}
			return nil
		})
	}

	// asks resumes the flow until it completes, and returns the questions its interrupts asked.
	asks := func(t *testing.T, workflowURL string) []string {
		assert := testarossa.For(t)
		flowKey, err := eng.Create(ctx, workflowURL, nil, nil)
		assert.Must(assert.NoError(err))
		var questions []string
		for range 3 {
			outcome, err := eng.Await(ctx, flowKey)
			assert.Must(assert.NoError(err))
			if outcome.Status != workflow.StatusInterrupted {
				assert.Equal(workflow.StatusCompleted, outcome.Status)
				return questions
			}
			questions = append(questions, outcome.InterruptPayload.GetString("question"))
			assert.Must(assert.NoError(eng.Resume(ctx, flowKey, nil)))
		}
		t.Fatal("the flow is still interrupted after three resumes")
		return nil
	}

	t.Run("step_retries_and_asks_again", func(t *testing.T) {
		assert := testarossa.For(t)
		assert.Equal([]string{"round 0", "round 1"}, asks(t, "interruptpayloadflow.verify:428/again"))
	})
	t.Run("child_asks_twice", func(t *testing.T) {
		assert := testarossa.For(t)
		assert.Equal([]string{"first", "second"}, asks(t, "interruptpayloadflow.verify:428/caller"))
	})
}
