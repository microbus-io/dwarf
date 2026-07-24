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

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestDeleteguardsflow_ForkRejectsADoomedFlow pins that Fork 409s a flow already scheduled for deletion.
//
// Deletion is deferred (mark, then reap), so a doomed flow is still readable during its grace window - it
// looks perfectly forkable. Fork names a SPECIFIC step, unlike Continue, which searches for a base and can
// simply skip a doomed turn; so naming a doomed flow is an error, not a fallback. Without the guard, Fork
// clones the origin's rows while the reaper concurrently deletes that same root_flow_id tree, and the result
// is a partial or empty clone: a silently corrupt fork instead of a clean 409.
//
// Both roads to a doomed flow are covered: an author's DeleteOnCompletion (stamped on success, grace window),
// and an operator's Delete (stamped due immediately).
func TestDeleteguardsflow_ForkRejectsADoomedFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("ForkGuard")
	graph.SetEndpoint("A", "deleteguardsflow.verify:428/a")
	graph.SetEndpoint("B", "deleteguardsflow.verify:428/b")
	graph.AddTransitionChain("A", "B", workflow.END)
	proxy.HandleGraph("deleteguardsflow.verify:428/g", graph)
	// B reports its own step key, which is the only way to obtain one for a DeleteOnCompletion flow: such a
	// flow is logically gone the moment it completes, so History 404s and cannot hand one out.
	var bStepKey string
	proxy.HandleTask("deleteguardsflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("deleteguardsflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		bStepKey = f.StepKey()
		f.SetString("done", "yes")
		return nil
	})

	t.Run("delete_on_completion_flow_cannot_be_forked", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, outcome, err := eng.Run(ctx, "deleteguardsflow.verify:428/g", nil,
			&workflow.FlowOptions{DeleteOnCompletion: true})
		if !assert.NoError(err) {
			return
		}
		// Inside the grace window the flow is still observable - which is exactly why the guard is needed.
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		snap, err := eng.Snapshot(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, snap.Status)

		// It is logically gone: History 404s even though the outcome is still served.
		_, err = eng.History(ctx, flowKey)
		assert.Error(err, "History of a delete-marked flow 404s - it is logically gone")

		// And Fork of one of its steps is refused. The step is real and the rows are all still present -
		// only the guard stands between this call and a clone racing the reaper.
		if !assert.NotEqual("", bStepKey) {
			return
		}
		_, err = eng.Fork(ctx, bStepKey, nil)
		if assert.Error(err, "Fork must refuse a flow marked DeleteOnCompletion") {
			assert.Equal(http.StatusConflict, errors.StatusCode(err))
		}
	})

	t.Run("operator_deleted_flow_cannot_be_forked", func(t *testing.T) {
		assert := testarossa.For(t)

		// A normal flow: forkable, and we capture a real step key while it is healthy.
		flowKey, outcome, err := eng.Run(ctx, "deleteguardsflow.verify:428/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		stepKey := stepKeyByTask(t, eng, flowKey, "B")
		if !assert.NotEqual("", stepKey) {
			return
		}

		// The control: while healthy, this exact step forks cleanly.
		forkKey, err := eng.Fork(ctx, stepKey, nil)
		if !assert.NoError(err, "the step must be forkable before the flow is doomed") {
			return
		}
		_, err = eng.Await(ctx, forkKey)
		assert.NoError(err)

		// Now mark the origin for deletion. The rows are still there (the reaper is a background sweep), so
		// nothing but the guard stands between Fork and a clone racing the reaper.
		if !assert.NoError(eng.Delete(ctx, flowKey)) {
			return
		}

		_, err = eng.Fork(ctx, stepKey, nil)
		if assert.Error(err, "Fork must refuse a flow scheduled for deletion") {
			assert.Equal(http.StatusConflict, errors.StatusCode(err))
		}
	})
}

// TestDeleteguardsflow_ContinueSkipsADeletedTurn pins that Continue builds on the latest UNDELETED turn.
//
// Continue searches for a base rather than naming one, so a doomed turn is skipped, not an error (the
// opposite disposition from Fork above, and deliberately so). Without the delete_after_ms=0 clause, Continue
// would seed the next turn from a flow that the reaper removes moments later - the thread silently continues
// from a doomed base, and its carried state is whatever that vanishing turn happened to hold.
func TestDeleteguardsflow_ContinueSkipsADeletedTurn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("ContinueGuard")
	graph.SetEndpoint("Turn", "deleteguardsflow2.verify:428/turn")
	graph.AddTransitionChain("Turn", workflow.END)
	proxy.HandleGraph("deleteguardsflow2.verify:428/g", graph)
	// Each turn stamps which base it was built from, so the surviving base is identifiable in the outcome.
	proxy.HandleTask("deleteguardsflow2.verify:428/turn", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("builtOn", f.GetString("marker"))
		f.SetString("marker", f.GetString("next"))
		return nil
	})

	// Turn 1 settles marker="one".
	threadKey, outcome, err := eng.Run(ctx, "deleteguardsflow2.verify:428/g",
		map[string]any{"marker": "seed", "next": "one"}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("one", outcome.State["marker"])

	// Turn 2 settles marker="two".
	turn2Key, err := eng.Continue(ctx, threadKey, map[string]any{"next": "two"})
	if !assert.NoError(err) {
		return
	}
	out2, err := eng.Await(ctx, turn2Key)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out2.Status)
	assert.Equal("one", out2.State["builtOn"]) // it built on turn 1, as expected
	assert.Equal("two", out2.State["marker"])

	// Delete turn 2 - the LATEST turn. Its row survives until the reaper sweeps, so an unguarded Continue
	// would happily pick it as the base.
	if !assert.NoError(eng.Delete(ctx, turn2Key)) {
		return
	}

	// Turn 3 must build on turn 1 (marker "one"), NOT the doomed turn 2 (marker "two").
	turn3Key, err := eng.Continue(ctx, threadKey, map[string]any{"next": "three"})
	if !assert.NoError(err) {
		return
	}
	out3, err := eng.Await(ctx, turn3Key)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out3.Status)
	assert.Equal("one", out3.State["builtOn"],
		"Continue must skip the delete-marked turn and build on the latest UNDELETED one")
}
