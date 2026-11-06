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
	"errors"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// A forEach branch may reach its fan-in by a Goto or onError edge instead of an ordinary one. Validate accepts
// the shape, and the engine must treat such an arrival like any other: the branch's step is linked to the fan-in
// step, and the flow's final state is the fan-in's onward output - not a merge of the cohort that loses it. An
// empty array must likewise route to the fan-in rather than complete the flow at the source.
func TestOverrideFanInFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	// Goto is the branch's ONLY route to the fan-in.
	gotoGraph := workflow.NewGraph("GotoIntoFanIn")
	gotoGraph.SetURL("Split", "overridefaninflow.verify:428/split")
	gotoGraph.SetURL("Work", "overridefaninflow.verify:428/goto-work")
	gotoGraph.SetURL("Join", "overridefaninflow.verify:428/join")
	gotoGraph.SetURL("After", "overridefaninflow.verify:428/after")
	gotoGraph.SetFanIn("Join")
	gotoGraph.AddTransitionForEach("Split", "Work", "items", "item")
	gotoGraph.AddTransitionGoto("Work", "Join")
	gotoGraph.AddTransitionChain("Join", "After", workflow.End)
	assert.NoError(gotoGraph.Validate())
	mux.HandleGraph("overridefaninflow.verify:428/goto", gotoGraph)

	// One branch errors and reaches the fan-in by onError; the other takes the ordinary path through Step2.
	errorGraph := workflow.NewGraph("OnErrorIntoFanIn")
	errorGraph.SetURL("Split", "overridefaninflow.verify:428/split")
	errorGraph.SetURL("Work", "overridefaninflow.verify:428/error-work")
	errorGraph.SetURL("Step2", "overridefaninflow.verify:428/step2")
	errorGraph.SetURL("Join", "overridefaninflow.verify:428/join")
	errorGraph.SetURL("After", "overridefaninflow.verify:428/after")
	errorGraph.SetFanIn("Join")
	errorGraph.AddTransitionForEach("Split", "Work", "items", "item")
	errorGraph.AddTransition("Work", "Step2")
	errorGraph.AddTransitionOnError("Work", "Join")
	errorGraph.AddTransitionChain("Step2", "Join", "After", workflow.End)
	assert.NoError(errorGraph.Validate())
	mux.HandleGraph("overridefaninflow.verify:428/error", errorGraph)

	mux.HandleTask("overridefaninflow.verify:428/split", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("overridefaninflow.verify:428/goto-work", func(ctx context.Context, f *workflow.Flow) error {
		f.Goto("Join")
		return nil
	})
	mux.HandleTask("overridefaninflow.verify:428/error-work", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("item") == "bad" {
			return errors.New("bad item")
		}
		return nil
	})
	mux.HandleTask("overridefaninflow.verify:428/step2", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("overridefaninflow.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("joined", true)
		return nil
	})
	mux.HandleTask("overridefaninflow.verify:428/after", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("after", true)
		return nil
	})

	run := func(t *testing.T, workflowURL string, items []string) []workflow.FlowStep {
		assert := testarossa.For(t)
		flowKey, outcome, err := eng.Run(ctx, workflowURL, map[string]any{"items": items}, nil)
		if !assert.NoError(err) || !assert.NotNil(outcome) {
			return nil
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, stateVal(outcome.State, "joined"), "final state lost the fan-in's output")
		assert.Equal(true, stateVal(outcome.State, "after"), "final state lost the output past the fan-in")
		steps, err := eng.History(ctx, flowKey)
		assert.NoError(err)
		return steps
	}
	stepKeyOf := func(steps []workflow.FlowStep, taskName string) string {
		for _, s := range steps {
			if s.TaskName == taskName {
				return s.StepKey
			}
		}
		return ""
	}

	t.Run("goto_only_route", func(t *testing.T) {
		assert := testarossa.For(t)
		steps := run(t, "overridefaninflow.verify:428/goto", []string{"a", "b"})
		joinKey := stepKeyOf(steps, "Join")
		assert.NotEqual("", joinKey)
		works := 0
		for _, s := range steps {
			if s.TaskName == "Work" {
				works++
				assert.Equal(joinKey, s.SuccessorKey, "a branch that arrived by Goto is not linked to the fan-in")
			}
		}
		assert.Equal(2, works)
	})

	t.Run("goto_only_route_empty_array", func(t *testing.T) {
		assert := testarossa.For(t)
		steps := run(t, "overridefaninflow.verify:428/goto", []string{})
		assert.NotEqual("", stepKeyOf(steps, "Join"), "an empty array must converge on the fan-in")
	})

	t.Run("on_error_route", func(t *testing.T) {
		assert := testarossa.For(t)
		steps := run(t, "overridefaninflow.verify:428/error", []string{"bad", "good"})
		joinKey := stepKeyOf(steps, "Join")
		assert.NotEqual("", joinKey)
		// An error-routed step completes with the error in its changes under onErr, not in its error column.
		errored := 0
		for _, s := range steps {
			if s.TaskName != "Work" {
				continue
			}
			step, err := eng.Step(ctx, s.StepKey)
			if !assert.NoError(err) || !step.Changes.Has("onErr") {
				continue
			}
			errored++
			assert.Equal(joinKey, s.SuccessorKey, "a branch that arrived by onError is not linked to the fan-in")
		}
		assert.Equal(1, errored, "exactly one branch should have errored into the fan-in")
	})
}
