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
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestForkStraggler_NormalizedToCancelled pins that a kept non-terminal (running/pending) step
// off the fork path - a straggler sibling that had not settled when the origin terminalized - is normalized
// to cancelled in the fork, not copied verbatim. Copied verbatim it would (a) be re-dispatched by lease
// recovery in the running fork and (b) as a cohort member wedge the fork's fan-in. The state is a race the
// cohort accounting normally prevents (a flow does not terminalize with a live sibling), so it is injected
// directly: run a single-node flow to completion, insert a running straggler off the fork path, then fork.
func TestForkStraggler_NormalizedToCancelled(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	g := workflow.NewGraph("SL")
	g.SetEndpoint("A", "sl/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("sl/g", g)
	proxy.HandleTask("sl/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	// B is only ever the injected straggler; it must never be dispatched (the fix cancels its clone). Register
	// a no-op so an accidental dispatch would not fail the fork step instead of surfacing the real defect.
	proxy.HandleTask("sl/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := engine.NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	fk, _, err := e.Run(ctx, "sl/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.DB().Shard(shardNum)
	assert.NoError(err)

	// The completed entry step A is the fork point.
	var aStepID int
	var aStepToken string
	err = db.QueryRowContext(ctx, "SELECT step_id, step_token FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStepID, &aStepToken)
	if !assert.NoError(err) {
		return
	}

	// Inject a running straggler off A's path (predecessor_id defaults to 0, so it is not a descendant of A
	// and is kept by the fork). Its lease is already expired, so copied verbatim into the running fork lease
	// recovery would reset it to pending and re-dispatch task B.
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, lease_expires) VALUES (?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), -60000))",
		flowID, 1, "btok", "B", "sl/b", workflow.StatusRunning, 1000,
	)
	if !assert.NoError(err) {
		return
	}

	// Fork at A. The fork re-runs A -> END and completes; the straggler must not be resurrected.
	forkKey, err := e.Fork(ctx, keys.New(shardNum, aStepID, aStepToken), nil)
	if !assert.NoError(err) {
		return
	}
	out, err := e.Await(ctx, forkKey)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}

	// The cloned straggler is cancelled, not copied verbatim as running.
	_, forkFlowID, _, err := keys.ParseFlowKey(forkKey)
	assert.NoError(err)
	var bStatus string
	err = db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='B'", forkFlowID).Scan(&bStatus)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCancelled, bStatus)
	}
}
