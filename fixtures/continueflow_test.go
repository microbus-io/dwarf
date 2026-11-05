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
	"net/http"
	"testing"

	"github.com/microbus-io/errors"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestContinueflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()

	graph := workflow.NewGraph("Counting")
	graph.SetURL("Increment", "continueflow.verify:428/increment")
	graph.AddTransition("Increment", workflow.End)
	mux.HandleGraph("continueflow.verify:428/counting", graph)

	mux.HandleTask("continueflow.verify:428/increment", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("counter", f.GetInt("counter")+1)
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	t.Run("counter_persists_across_continue_turns", func(t *testing.T) {
		assert := testarossa.For(t)

		// Turn 1: create (auto-starts) a flow starting from counter=0.
		flowKey, err := eng.Create(ctx, "continueflow.verify:428/counting", map[string]any{"counter": 0}, nil)
		if !assert.NoError(err) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(1.0, stateVal(outcome.State, "counter")) // JSON round-trip: int -> float64

		// Turn 2: continue turn 1, no additional state.
		flowKey2, err := eng.Continue(ctx, flowKey, map[string]any{})
		if !assert.NoError(err) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey2)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(2.0, stateVal(outcome.State, "counter"))
	})
}

// TestContinueInheritsPolicy verifies a flow made by Continue (which takes no FlowOptions) inherits the policy of
// the flow it continues: scheduling priority (not reset to the engine default).
func TestContinueInheritsPolicy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Policy")
	g.SetURL("T", "continuepolicy.verify:428/t")
	g.AddTransition("T", workflow.End)
	mux.HandleGraph("continuepolicy.verify:428/g", g)
	mux.HandleTask("continuepolicy.verify:428/t", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	// Turn 1 with a distinctive priority (4, not the engine default of 3).
	turn1, err := eng.Create(ctx, "continuepolicy.verify:428/g", map[string]any{},
		&workflow.FlowOptions{Priority: 4})
	assert.NoError(err)
	_, err = eng.Await(ctx, turn1)
	assert.NoError(err)

	// Turn 2 via Continue (no opts) inherits turn 1's policy.
	turn2, err := eng.Continue(ctx, turn1, map[string]any{})
	assert.NoError(err)
	_, err = eng.Await(ctx, turn2)
	assert.NoError(err)

	// Priority inherited (4), not reset to the engine default.
	summaries, err := enginetest.ListAll(ctx, eng, workflow.Query{Search: turn2})
	assert.NoError(err)
	if assert.Len(summaries, 1) {
		assert.Equal(4, summaries[0].Priority)
	}
}

// TestContinue_BuildsOnTheNamedFlow pins that Continue builds on exactly the flow its key names, whatever else
// was made from it: continuing one flow twice gives two successors, each built on it, and continuing a successor
// builds on that successor. Only a completed flow can be continued.
func TestContinue_BuildsOnTheNamedFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Named")
	g.SetURL("Step", "continuenamed.verify:428/step")
	g.AddTransition("Step", workflow.End)
	mux.HandleGraph("continuenamed.verify:428/g", g)
	release := make(chan struct{})
	mux.HandleTask("continuenamed.verify:428/step", func(ctx context.Context, f *workflow.Flow) error {
		switch f.GetString("mode") {
		case "fail":
			return errors.New("failed on purpose")
		case "park":
			if yield, err := f.Interrupt(nil, nil); yield || err != nil {
				return err
			}
		case "block":
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		// builtOn records which flow this one was continued from; label names this one.
		f.SetString("builtOn", f.GetString("label"))
		f.SetString("label", f.GetString("next"))
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	run := func(key string, err error) *workflow.FlowOutcome {
		if !assert.NoError(err) {
			return &workflow.FlowOutcome{}
		}
		outcome, err := eng.Await(ctx, key)
		assert.NoError(err)
		return outcome
	}

	turn1, outcome, err := eng.Run(ctx, "continuenamed.verify:428/g", map[string]any{"next": "one"}, nil)
	assert.NoError(err)
	assert.Equal("one", stateVal(outcome.State, "label"))

	// Two successors of turn 1, each built on it: Continue does not look for a later flow.
	turn2a, err := eng.Continue(ctx, turn1, map[string]any{"next": "two-a"})
	outcome = run(turn2a, err)
	assert.Equal("one", stateVal(outcome.State, "builtOn"))
	turn2b, err := eng.Continue(ctx, turn1, map[string]any{"next": "two-b"})
	outcome = run(turn2b, err)
	assert.Equal("one", stateVal(outcome.State, "builtOn"))
	assert.NotEqual(turn2a, turn2b)

	// Continuing a successor builds on that successor.
	turn3, err := eng.Continue(ctx, turn2a, map[string]any{"next": "three"})
	outcome = run(turn3, err)
	assert.Equal("two-a", stateVal(outcome.State, "builtOn"))

	conflict := func(name, key string) {
		_, err := eng.Continue(ctx, key, nil)
		if assert.Error(err, name) {
			assert.Equal(http.StatusConflict, errors.StatusCode(err), "%s: %v", name, err)
		}
	}
	failed, outcome, err := eng.Run(ctx, "continuenamed.verify:428/g", map[string]any{"mode": "fail"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)
	conflict("failed", failed)
	parked, outcome, err := eng.Run(ctx, "continuenamed.verify:428/g", map[string]any{"mode": "park"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusInterrupted, outcome.Status)
	conflict("interrupted", parked)
	running, err := eng.Create(ctx, "continuenamed.verify:428/g", map[string]any{"mode": "block"}, nil)
	assert.NoError(err)
	conflict("running", running)
	close(release)
	_, err = eng.Await(ctx, running)
	assert.NoError(err)

	_, err = eng.Continue(ctx, "1-0-99999-0000000000000000", nil)
	if assert.Error(err) {
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	}
}
