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

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestWedgeSweep_SubgraphCallerRevived manufactures a wedged parkedSubgraph caller (child reached terminal
// but the revive was lost) and asserts the sweep re-drives completeSurgraphFlow so the caller resumes and
// the flow completes, adopting the child's output. The wedge can't arise naturally (the park-before-spawn
// fix prevents it), so the test forges the DB state the bug would leave and backdates the caller past
// parkWedgeThreshold, then calls the recovery directly (bypassing the time gate).
func TestWedgeSweep_SubgraphCallerRevived(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})

	parent := workflow.NewGraph("Parent", "wedgesub.verify:0/parent")
	parent.AddTask("P", "wedgesub.verify:0/p")
	parent.AddTransition("P", workflow.END)
	proxy.HandleGraph("wedgesub.verify:0/parent", parent)
	child := workflow.NewGraph("Child", "wedgesub.verify:0/child")
	child.AddTask("K", "wedgesub.verify:0/k")
	child.AddTransition("K", workflow.END)
	proxy.HandleGraph("wedgesub.verify:0/child", child)

	proxy.HandleTask("wedgesub.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("wedgesub.verify:0/child", nil, &out)
		if yield || err != nil {
			return err
		}
		v, _ := out["v"].(float64)
		f.SetInt("got", int(v))
		return nil
	})
	// The child blocks so it stays running and the caller stays parked while we forge the wedge.
	proxy.HandleTask("wedgesub.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		<-release
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)
	defer close(release)

	flowKey, err := e.Create(ctx, "wedgesub.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	if !assert.NoError(e.Start(ctx, flowKey)) {
		return
	}
	shard, parentFlowID, _, err := parseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.shard(shard)
	if !assert.NoError(err) {
		return
	}

	// Wait until the subgraph child is fully started (status=running), not merely until the caller parks.
	// The launch is three sequential steps - park the caller (parkedSubgraph), create the child (created),
	// then start the child (running) - and the park commits and is visible before the child is started. If
	// the test forged the child to completed in that window, the engine's start(child) would find it
	// non-created and fail the caller with "flow is already started". A running child implies start(child)
	// has run and the caller is parked. (SQLite serializes these writes so the window never opens; MySQL and
	// Postgres expose it.)
	var parentStepID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db.QueryRowContext(ctx,
			"SELECT surgraph_step_id FROM dwarf_flows WHERE surgraph_flow_id=? AND status=?",
			parentFlowID, workflow.StatusRunning,
		).Scan(&parentStepID) == nil && parentStepID != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !assert.NotEqual(0, parentStepID, "subgraph child never started") {
		return
	}

	// Forge the wedge: the child reached terminal (completed, final_state {"v":7}) but the caller was never
	// revived.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status=?, final_state=? WHERE surgraph_step_id=?",
		workflow.StatusCompleted, `{"v":7}`, parentStepID)
	assert.NoError(err)

	// Recover (minAge=0 bypasses the age guard for the test) and confirm the flow resumes and adopts the
	// child's output.
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0)

	out, err := e.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	got, _ := out.State["got"].(float64)
	assert.Equal(7, int(got))
}

// TestWedgeSweep_BreakerProbeReElected manufactures a breaker backlog that has lost its probe (all steps
// parked=2, none dispatchable) and asserts the sweep re-elects a probe so the backlog recovers. The wedge is
// built without ever running a live breaker cycle: the breaker is tripped in-memory first, so Start parks
// every entry step to parked=2 and elects no probe (parkTrippedSteps). That is deterministic - there is no
// concurrent breaker churn to race a forge against, hence no need to quiesce the engine. The task always
// succeeds, so once the sweep elevates a probe it closes the breaker, breakerClose unparks the rest, and
// every flow completes.
func TestWedgeSweep_BreakerProbeReElected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	const taskURL = "wedgebrk.verify:0/task"

	proxy := NewTestProxy()
	g := workflow.NewGraph("Down", "wedgebrk.verify:0/down")
	g.AddTask("Down", taskURL)
	g.AddTransition("Down", workflow.END)
	proxy.HandleGraph("wedgebrk.verify:0/down", g)
	// The task itself always succeeds; the breaker is tripped directly below, not by a failure, so no probe
	// dispatch ever fails and the only way out of the backlog is the sweep re-electing a probe.
	proxy.HandleTask(taskURL, func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// Trip the breaker in-memory before starting any flow. With the breaker tripped, Start's parkTrippedSteps
	// parks each entry step straight to parked=2 (and elects no probe), so the backlog is born wedged without
	// a single dispatch.
	e.breakerTrip(taskURL, breakerCauseAckTimeout)

	const n = 4
	keys := make([]string, 0, n)
	for range n {
		k, err := e.Create(ctx, "wedgebrk.verify:0/down", nil, nil)
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(e.Start(ctx, k)) {
			return
		}
		keys = append(keys, k)
	}

	db, err := e.shard(1) // RunInTest defaults to a single shard
	if !assert.NoError(err) {
		return
	}

	// Normalize the steps' timing columns using the DB clock (NOW_UTC()) only - never a bound Go time, which
	// the engine itself avoids (clock skew) and which SQLite would store as RFC3339 that its date functions
	// misparse. All on the quiescent (tripped-but-idle) backlog, so no concurrent writer races this UPDATE:
	//   - updated_at backdated an hour so the sweep's age guard (minAge=0 here) matches regardless of how
	//     recently the steps parked (a freshly-parked step can be sub-millisecond young at recovery's age check).
	//   - lease_expires pulled to NOW so the re-elected probe is immediately leasable: a step parked at Start
	//     was never dispatched, so its lease still sits at insert-time + leaseMargin (future).
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?), not_before=NOW_UTC(), lease_expires=NOW_UTC() WHERE task_url=?",
		-int64(time.Hour/time.Millisecond), taskURL,
	)
	assert.NoError(err)

	// Confirm the wedge: n steps parked=2, no dispatchable probe.
	var parked, probes int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE task_url=? AND parked=?", taskURL, parkedBreaker).Scan(&parked)
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE task_url=? AND parked=? AND status IN (?,?)",
		taskURL, parkedNone, workflow.StatusPending, workflow.StatusRunning).Scan(&probes)
	if !assert.Equal(n, parked, "all steps should be breaker-parked") || !assert.Equal(0, probes, "wedge should leave no probe") {
		return
	}

	// Run the sweep: it re-elects a probe, the probe succeeds, breakerClose unparks the rest, all complete.
	e.recoverWedgedBreakerParks(ctx, db, 1, 0)

	for _, k := range keys {
		out, err := e.Await(ctx, k)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}
