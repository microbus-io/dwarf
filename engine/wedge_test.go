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
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// orphanLogCapture records the `flow` attribute of every "Orphaned flow" error log detectOrphanedFlows emits.
type orphanLogCapture struct {
	mu    sync.Mutex
	flows []string
}

func (c *orphanLogCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *orphanLogCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *orphanLogCapture) WithGroup(string) slog.Handler            { return c }
func (c *orphanLogCapture) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "Orphaned flow: running with all steps terminal and no successor" {
		return nil
	}
	var flow string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "flow" {
			flow = a.Value.String()
		}
		return true
	})
	c.mu.Lock()
	c.flows = append(c.flows, flow)
	c.mu.Unlock()
	return nil
}

func (c *orphanLogCapture) seen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.flows...)
}

// TestWedgeSweep_SubgraphCallerRevived manufactures a wedged parkedSubgraph caller (child reached terminal
// but the revive was lost) and asserts the sweep re-drives completeSurgraphFlow so the caller resumes and
// the flow completes, adopting the child's output. The wedge can't arise naturally (the park-before-spawn
// fix prevents it), so the test forges the DB state the bug would leave and backdates the caller past
// parkWedgeThreshold, then calls the recovery directly (bypassing the time gate).
func TestWedgeSweep_SubgraphCallerRevived(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("P", "wedgesub.verify:0/p")
	parent.AddTransition("P", workflow.END)
	proxy.HandleGraph("wedgesub.verify:0/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("K", "wedgesub.verify:0/k")
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
	shard, parentFlowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
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

// TestWedgeSweep_SubgraphCallerWithNoChildFails forges the wedge the sweep's designated last-resort branch
// exists for and could never actually recover: a caller step parked on a subgraph whose child flow does not
// exist (a worker committed the park and died before inserting the child; or the child was deleted). Such a
// step is invisible to lease recovery BY DESIGN - parkedSubgraph carries no lease - so the sweep is the only
// way out. It must fail the caller: the parent re-arm writes the error onto the parked step, the
// re-dispatched flow.Subgraph returns it, and the task fails through its normal disposition.
//
// Before the fix the sweep detected this correctly and then rolled back every time (computeFinalState
// SELECTed `WHERE flow_id=0` -> sql.ErrNoRows), so dwarf_steps_unwedged never moved and the flow hung
// forever - only Delete got it out.
func TestWedgeSweep_SubgraphCallerWithNoChildFails(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("P", "wedgenochild.verify:0/p")
	parent.AddTransition("P", workflow.END)
	proxy.HandleGraph("wedgenochild.verify:0/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("K", "wedgenochild.verify:0/k")
	child.AddTransition("K", workflow.END)
	proxy.HandleGraph("wedgenochild.verify:0/child", child)

	proxy.HandleTask("wedgenochild.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("wedgenochild.verify:0/child", nil, &out)
		if yield || err != nil {
			return err // the wedge recovery delivers its error here, failing this step
		}
		return nil
	})
	proxy.HandleTask("wedgenochild.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		<-release // hold the child running so the caller stays parked while we forge the wedge
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)
	defer close(release)

	flowKey, err := e.Create(ctx, "wedgenochild.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shard, parentFlowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}

	// Wait for the caller to be parked on a running child (see the sibling test for why `running`, not
	// merely parked, is the right signal).
	var parentStepID, childFlowID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db.QueryRowContext(ctx,
			"SELECT surgraph_step_id, flow_id FROM dwarf_flows WHERE surgraph_flow_id=? AND status=?",
			parentFlowID, workflow.StatusRunning,
		).Scan(&parentStepID, &childFlowID) == nil && parentStepID != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !assert.NotEqual(0, parentStepID, "subgraph child never started") {
		return
	}

	// Forge the wedge: the child flow (and its steps) vanish, leaving the caller parked on nothing.
	_, err = db.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id=?", childFlowID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id=?", childFlowID)
	assert.NoError(err)

	// The sweep must recover it (minAge=0 bypasses the age guard) by failing the caller.
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0)

	out, err := e.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, out.Status, "the caller step fails when its subgraph is gone")
	assert.True(strings.Contains(out.Error, "subgraph flow not found"),
		"the failure names the missing subgraph (got %q)", out.Error)
}

// TestWedgeSweep_OrphanedSubgraphChildCancelled forges the zombie a Cancel racing a subgraph spawn leaves - a
// non-terminal subgraph child whose parent tree was cancelled in the window after the caller step parked but
// before the child was inserted, so the teardown missed it. The child rests interrupted with no path out
// (root-key ops 409 on the terminal root, the child's own key is read-only, and recoverWedgedSubgraphParks is
// blind because the caller step is cancelled, not running+parked). recoverOrphanedSubgraphChildren must cancel
// it, while leaving a healthy subgraph child (running under a still-running parent) untouched.
func TestWedgeSweep_OrphanedSubgraphChildCancelled(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})

	// Orphan scenario: parent spawns a child whose entry task interrupts, so the child rests interrupted.
	parent := workflow.NewGraph("OrphanParent")
	parent.SetEndpoint("P", "orphanchild.verify:0/p")
	parent.AddTransition("P", workflow.END)
	proxy.HandleGraph("orphanchild.verify:0/parent", parent)
	ichild := workflow.NewGraph("OrphanChild")
	ichild.SetEndpoint("K", "orphanchild.verify:0/k")
	ichild.AddTransition("K", workflow.END)
	proxy.HandleGraph("orphanchild.verify:0/child", ichild)
	proxy.HandleTask("orphanchild.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		_, err := f.Subgraph("orphanchild.verify:0/child", nil, &out)
		return err
	})
	proxy.HandleTask("orphanchild.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"awaiting": true}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// Healthy control: parent whose child blocks (stays running), so the parent stays running+parked.
	hparent := workflow.NewGraph("HealthyParent")
	hparent.SetEndpoint("P", "orphanchild.verify:0/hp")
	hparent.AddTransition("P", workflow.END)
	proxy.HandleGraph("orphanchild.verify:0/healthyparent", hparent)
	hchild := workflow.NewGraph("HealthyChild")
	hchild.SetEndpoint("K", "orphanchild.verify:0/hk")
	hchild.AddTransition("K", workflow.END)
	proxy.HandleGraph("orphanchild.verify:0/healthychild", hchild)
	proxy.HandleTask("orphanchild.verify:0/hp", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		_, err := f.Subgraph("orphanchild.verify:0/healthychild", nil, &out)
		return err
	})
	proxy.HandleTask("orphanchild.verify:0/hk", func(ctx context.Context, f *workflow.Flow) error {
		<-release
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)
	defer close(release)

	// Orphan: create, await the interrupt, then forge a Cancel that raced the spawn - the parent tree is
	// cancelled but the child (which the racing scan missed) is left interrupted.
	parentKey, err := e.Create(ctx, "orphanchild.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	out, err := e.Await(ctx, parentKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, out.Status) {
		return
	}
	shard, parentFlowID, _, err := keys.ParseFlowKey(parentKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	var childFlowID int
	var childToken string
	if !assert.NoError(db.QueryRowContext(ctx,
		"SELECT flow_id, flow_token FROM dwarf_flows WHERE surgraph_flow_id=?", parentFlowID,
	).Scan(&childFlowID, &childToken)) {
		return
	}
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status=?, cancel_reason=? WHERE flow_id=?",
		workflow.StatusCancelled, "forged", parentFlowID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, parked=? WHERE flow_id=? AND status IN (?, ?)",
		workflow.StatusCancelled, parkedNone, parentFlowID, workflow.StatusInterrupted, workflow.StatusRunning)
	assert.NoError(err)

	// Healthy control: a subgraph child running under a still-running parent.
	healthyKey, err := e.Create(ctx, "orphanchild.verify:0/healthyparent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	_, healthyParentID, _, err := keys.ParseFlowKey(healthyKey)
	if !assert.NoError(err) {
		return
	}
	var healthyChildID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db.QueryRowContext(ctx,
			"SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id=? AND status=?",
			healthyParentID, workflow.StatusRunning,
		).Scan(&healthyChildID) == nil && healthyChildID != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !assert.NotEqual(0, healthyChildID, "healthy subgraph child never started") {
		return
	}

	// Recover (minAge=0 bypasses the age gate).
	e.recoverOrphanedSubgraphChildren(ctx, db, shard, 0)

	// The orphaned child is cancelled with the recovery reason, and its interrupted step is cancelled too.
	var childStatus, childReason string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, cancel_reason FROM dwarf_flows WHERE flow_id=?", childFlowID).
		Scan(&childStatus, &childReason))
	assert.Equal(workflow.StatusCancelled, strings.TrimSpace(childStatus))
	assert.Equal("parent flow terminated (orphan recovery)", strings.TrimSpace(childReason))
	var liveChildSteps int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status IN (?, ?, ?, ?)",
		childFlowID, workflow.StatusCreated, workflow.StatusPending, workflow.StatusInterrupted, workflow.StatusRunning,
	).Scan(&liveChildSteps))
	assert.Equal(0, liveChildSteps, "orphaned child's steps should all be cancelled")

	// The healthy child (parent still running) is untouched.
	var healthyChildStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", healthyChildID).Scan(&healthyChildStatus))
	assert.Equal(workflow.StatusRunning, strings.TrimSpace(healthyChildStatus))
}

// TestOrphanDetection_FlagsWedgedFlow forges the exact state the post-completion transition wedge leaves - a
// `running` flow whose only step is `completed` (no successor) - and asserts detectOrphanedFlows logs it, while
// a genuinely-running flow (with a live non-terminal step) is left alone even when equally old. Log-only by
// design: the detector is the last-resort alarm, not a recovery, so the assertion is on the emitted error log.
func TestOrphanDetection_FlagsWedgedFlow(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	capture := &orphanLogCapture{}
	proxy := NewTestProxy()
	release := make(chan struct{})

	solo := workflow.NewGraph("Solo")
	solo.SetEndpoint("A", "orphan.verify:0/a")
	solo.AddTransition("A", workflow.END)
	proxy.HandleGraph("orphan.verify:0/solo", solo)
	proxy.HandleTask("orphan.verify:0/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	// A separate graph whose task blocks, so its flow stays running with a live step - the healthy control.
	blocked := workflow.NewGraph("Blocked")
	blocked.SetEndpoint("B", "orphan.verify:0/b")
	blocked.AddTransition("B", workflow.END)
	proxy.HandleGraph("orphan.verify:0/blocked", blocked)
	proxy.HandleTask("orphan.verify:0/b", func(ctx context.Context, f *workflow.Flow) error {
		<-release
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	assert.NoError(e.SetLogger(slog.New(capture)))
	e.RunInTest(t)
	defer close(release)

	// A flow run to completion, then forged back to `running`: its step stays `completed`, so the flow is now
	// running with no non-terminal step - the orphan shape a failed post-completion transition would strand.
	orphanKey, err := e.Create(ctx, "orphan.verify:0/solo", nil, nil)
	if !assert.NoError(err) {
		return
	}
	orphanOut, err := e.Await(ctx, orphanKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, orphanOut.Status) {
		return
	}
	shard, orphanFlowID, _, err := keys.ParseFlowKey(orphanKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}

	// The healthy control: a flow blocked in its task, so it holds a `running` step.
	healthyKey, err := e.Create(ctx, "orphan.verify:0/blocked", nil, nil)
	if !assert.NoError(err) {
		return
	}
	_, healthyFlowID, _, err := keys.ParseFlowKey(healthyKey)
	if !assert.NoError(err) {
		return
	}

	// Backdate both flows past orphanFlowThreshold (DB clock, native format), and flip the orphan to running.
	pastMs := -(e.orphanFlowThreshold + time.Minute).Milliseconds()
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET status=?, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		workflow.StatusRunning, pastMs, orphanFlowID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		pastMs, healthyFlowID)
	assert.NoError(err)

	e.detectOrphanedFlows(ctx, db, shard)

	seen := capture.seen()
	assert.Len(seen, 1, "exactly one flow should be flagged as orphaned")
	if len(seen) == 1 {
		// The alarm logs the token-free correlation id (SEC2), never the capability-bearing flowKey.
		assert.Equal(keys.CorrelationID(shard, orphanFlowID), seen[0], "the flagged flow is the forged orphan, not the healthy blocked flow")
		assert.NotEqual(orphanKey, seen[0], "the logged id carries no token")
	}
}
