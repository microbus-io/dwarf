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
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestDeleteguardsflow_ForkRejectsADoomedFlow pins that Fork 409s a flow already scheduled for deletion.
//
// Deletion is deferred (mark, then reap), so a doomed flow is still readable during its grace window - it
// looks perfectly forkable. Fork names a SPECIFIC step, unlike Continue, which searches for a base and can
// simply skip a doomed turn; so naming a doomed flow is an error, not a fallback. Without the guard, Fork
// clones the origin's rows while the janitor concurrently deletes that same root_flow_id tree, and the result
// is a partial or empty clone: a silently corrupt fork instead of a clean 409.
//
// Both roads to a doomed flow are covered: an author's DeleteOnCompletion (stamped on success, grace window),
// and an operator's Delete (stamped due immediately).
func TestDeleteguardsflow_ForkRejectsADoomedFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	graph := workflow.NewGraph("ForkGuard")
	graph.SetURL("A", "deleteguardsflow.verify:428/a")
	graph.SetURL("B", "deleteguardsflow.verify:428/b")
	graph.AddTransitionChain("A", "B", workflow.End)
	mux.HandleGraph("deleteguardsflow.verify:428/g", graph)
	// B reports its own step key, which is the only way to obtain one for a DeleteOnCompletion flow: such a
	// flow is logically gone the moment it completes, so History 404s and cannot hand one out.
	var bStepKey string
	mux.HandleTask("deleteguardsflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("deleteguardsflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
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
		// only the guard stands between this call and a clone racing the janitor.
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

		// Now mark the origin for deletion. The rows are still there (the janitor is a background sweep), so
		// nothing but the guard stands between Fork and a clone racing the janitor.
		if !assert.NoError(eng.Delete(ctx, flowKey)) {
			return
		}

		_, err = eng.Fork(ctx, stepKey, nil)
		if assert.Error(err, "Fork must refuse a flow scheduled for deletion") {
			assert.Equal(http.StatusConflict, errors.StatusCode(err))
		}
	})
}

// TestDeleteguardsflow_ContinueRejectsADoomedFlow pins that Continue of a flow scheduled for deletion is a 409, as
// Fork's is. The flow's row survives until the janitor sweeps it, so an unguarded Continue would build a new flow
// on one that is logically gone.
func TestDeleteguardsflow_ContinueRejectsADoomedFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	graph := workflow.NewGraph("ContinueGuard")
	graph.SetURL("Turn", "deleteguardsflow2.verify:428/turn")
	graph.AddTransitionChain("Turn", workflow.End)
	mux.HandleGraph("deleteguardsflow2.verify:428/g", graph)
	mux.HandleTask("deleteguardsflow2.verify:428/turn", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	flowKey, outcome, err := eng.Run(ctx, "deleteguardsflow2.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	if !assert.NoError(eng.Delete(ctx, flowKey)) {
		return
	}

	_, err = eng.Continue(ctx, flowKey, nil)
	if assert.Error(err) {
		assert.Equal(http.StatusConflict, errors.StatusCode(err), "%v", err)
	}
}
