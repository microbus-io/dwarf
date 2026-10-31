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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// orphanLogCapture records the `flow` attribute of every "Orphaned flow" error log recoverOrphanedFlows emits.
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
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	release := make(chan struct{})

	parent := workflow.NewGraph("Parent")
	parent.SetURL("P", "wedgesub.verify:0/p")
	parent.AddTransition("P", workflow.End)
	mux.HandleGraph("wedgesub.verify:0/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("K", "wedgesub.verify:0/k")
	child.AddTransition("K", workflow.End)
	mux.HandleGraph("wedgesub.verify:0/child", child)

	mux.HandleTask("wedgesub.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
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
	mux.HandleTask("wedgesub.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))
	defer close(release)

	flowKey, err := e.Create(ctx, "wedgesub.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shard, _, parentFlowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}

	// Wait until the subgraph child exists and is running. The caller's park and the child's insert commit
	// together, so a running child implies the caller is parked.
	var parentStepID int
	deadline := time.Now().Add(5 * time.Second * enginetest.TimeoutScale())
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
		workflow.StatusCompleted, []byte(`{"v":7}`), parentStepID)
	assert.NoError(err)

	// Recover (minAge=0 bypasses the age guard for the test) and confirm the flow resumes and adopts the
	// child's output.
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0)

	out, err := e.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	got, _ := stateVal(out.State, "got").(float64)
	assert.Equal(7, int(got))
}

// TestWedgeSweep_SubgraphCallerWithNoChildFails forges the wedge the sweep's designated last-resort branch
// exists for and could never actually recover: a caller step parked on a subgraph whose child flow does not
// exist. The park and the child's insert commit together, so only a bug or a deleted row produces it. Such a
// step is invisible to lease recovery BY DESIGN - parkedSubgraph carries no lease - so the sweep is the only
// way out. It must fail the caller: the parent re-arm writes the error onto the parked step, the
// re-dispatched flow.Subgraph returns it, and the task fails through its normal disposition.
//
// The failure mode pinned: a sweep that detects this correctly and then rolls back every time
// (computeFinalState SELECTing `WHERE flow_id=0` -> sql.ErrNoRows) never moves dwarf_steps_unwedged, and the
// flow hangs forever - only Delete gets it out.
func TestWedgeSweep_SubgraphCallerWithNoChildFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	release := make(chan struct{})

	parent := workflow.NewGraph("Parent")
	parent.SetURL("P", "wedgenochild.verify:0/p")
	parent.AddTransition("P", workflow.End)
	mux.HandleGraph("wedgenochild.verify:0/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("K", "wedgenochild.verify:0/k")
	child.AddTransition("K", workflow.End)
	mux.HandleGraph("wedgenochild.verify:0/child", child)

	mux.HandleTask("wedgenochild.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("wedgenochild.verify:0/child", nil, &out)
		if yield || err != nil {
			return err // the wedge recovery delivers its error here, failing this step
		}
		return nil
	})
	mux.HandleTask("wedgenochild.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		<-release // hold the child running so the caller stays parked while we forge the wedge
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))
	defer close(release)

	flowKey, err := e.Create(ctx, "wedgenochild.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shard, _, parentFlowID, _, err := keys.ParseFlowKey(flowKey)
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
	deadline := time.Now().Add(5 * time.Second * enginetest.TimeoutScale())
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

// TestWedgeSweep_ChildLiveAgainIsLeftAlone pins the sweep's skip of a caller whose latest child is live again by
// the time it is read: a spawn that landed between the sweep's scan and its per-caller read. The caller is not
// wedged, so the sweep must leave it parked. Handed to the terminal-child branch instead, it delivered
// "subgraph running" as an error and failed the caller under its live child.
func TestWedgeSweep_ChildLiveAgainIsLeftAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	started := make(chan struct{})
	release := make(chan struct{})

	parent := workflow.NewGraph("Parent")
	parent.SetURL("P", "wedgelive.verify:0/p")
	parent.AddTransition("P", workflow.End)
	mux.HandleGraph("wedgelive.verify:0/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("K", "wedgelive.verify:0/k")
	child.AddTransition("K", workflow.End)
	mux.HandleGraph("wedgelive.verify:0/child", child)

	mux.HandleTask("wedgelive.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		_, err := f.Subgraph("wedgelive.verify:0/child", nil, &out)
		return err
	})
	// The child holds, so the caller stays parked on it while the test stages the window.
	mux.HandleTask("wedgelive.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		close(started)
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))
	var releaseOnce sync.Once
	releaseChild := func() { releaseOnce.Do(func() { close(release) }) }
	// Below the Shutdown defer, so it unwinds first; the test also releases the child itself, so it runs once.
	defer releaseChild()
	awaitStartupRecoverySweep(t, e) // the Break below must catch only the sweep this test drives

	flowKey, err := e.Create(ctx, "wedgelive.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	select {
	case <-started: // the caller's park and the child's insert commit together, so the caller is parked
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("subgraph child never started")
	}
	shard, _, parentFlowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	var callerStepID, childFlowID int
	err = db.QueryRowContext(ctx, "SELECT surgraph_step_id, flow_id FROM dwarf_flows WHERE surgraph_flow_id=?", parentFlowID).
		Scan(&callerStepID, &childFlowID)
	if !assert.NoError(err) {
		return
	}

	// The scan must see no live child, so the child reads terminal while the sweep selects.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status=? WHERE flow_id=?", workflow.StatusCompleted, childFlowID)
	assert.NoError(err)
	e.seams.Break(engineseams.CheckpointWedgeSweepSelected)
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		e.recoverWedgedSubgraphParks(ctx, db, shard, 0)
	}()
	if !assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointWedgeSweepSelected, 30*time.Second*enginetest.TimeoutScale()),
		"the sweep never selected the caller") {
		e.seams.Resume(engineseams.CheckpointWedgeSweepSelected)
		return
	}
	// Between the scan and the read, the latest child is live again, as a spawn landing in the window leaves it.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status=? WHERE flow_id=?", workflow.StatusRunning, childFlowID)
	assert.NoError(err)
	e.seams.Resume(engineseams.CheckpointWedgeSweepSelected)
	select {
	case <-swept:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the sweep never finished")
	}

	// The caller is untouched: still parked, nothing delivered to it.
	var status string
	var subgraphErr []byte
	var parked, subgraphDone int
	err = db.QueryRowContext(ctx, "SELECT status, parked, subgraph_done, subgraph_error FROM dwarf_steps WHERE step_id=?", callerStepID).
		Scan(&status, &parked, &subgraphDone, &subgraphErr)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusRunning, status)
		assert.Equal(parkedSubgraph, parked)
		assert.Equal(0, subgraphDone)
		assert.Equal("", subgraphErrorText(subgraphErr))
	}

	// And the flow finishes normally once its child does.
	releaseChild()
	out, err := e.Await(ctx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

// TestWedgeSweep_OrphanedSubgraphChildTerminated forges the zombie a Terminate racing a subgraph spawn leaves -
// a non-terminal subgraph child whose parent tree was terminated in the window after the caller step parked but
// before the child was inserted, so the teardown missed it. The child rests interrupted with no path out
// (root-key ops 409 on the terminal root, the child's own key is read-only, and recoverWedgedSubgraphParks is
// blind because the caller step is terminated, not running+parked). recoverOrphanedSubgraphChildren must
// terminate it, while leaving a healthy subgraph child (running under a still-running parent) untouched. Its age
// guard is anchored on the PARENT's terminal write: a child whose own row has been still for longer than the guard
// is spared while its parent has only just gone terminal, and swept once the parent has been terminal that long.
func TestWedgeSweep_OrphanedSubgraphChildTerminated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	release := make(chan struct{})

	// Orphan scenario: parent spawns a child whose entry task interrupts, so the child rests interrupted.
	parent := workflow.NewGraph("OrphanParent")
	parent.SetURL("P", "orphanchild.verify:0/p")
	parent.AddTransition("P", workflow.End)
	mux.HandleGraph("orphanchild.verify:0/parent", parent)
	ichild := workflow.NewGraph("OrphanChild")
	ichild.SetURL("K", "orphanchild.verify:0/k")
	ichild.AddTransition("K", workflow.End)
	mux.HandleGraph("orphanchild.verify:0/child", ichild)
	mux.HandleTask("orphanchild.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		_, err := f.Subgraph("orphanchild.verify:0/child", nil, &out)
		return err
	})
	mux.HandleTask("orphanchild.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"awaiting": true}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// Healthy control: parent whose child blocks (stays running), so the parent stays running+parked.
	hparent := workflow.NewGraph("HealthyParent")
	hparent.SetURL("P", "orphanchild.verify:0/hp")
	hparent.AddTransition("P", workflow.End)
	mux.HandleGraph("orphanchild.verify:0/healthyparent", hparent)
	hchild := workflow.NewGraph("HealthyChild")
	hchild.SetURL("K", "orphanchild.verify:0/hk")
	hchild.AddTransition("K", workflow.End)
	mux.HandleGraph("orphanchild.verify:0/healthychild", hchild)
	mux.HandleTask("orphanchild.verify:0/hp", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		_, err := f.Subgraph("orphanchild.verify:0/healthychild", nil, &out)
		return err
	})
	mux.HandleTask("orphanchild.verify:0/hk", func(ctx context.Context, f *workflow.Flow) error {
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))
	defer close(release)

	// Orphan: create, await the interrupt, then forge a Terminate that raced the spawn - the parent tree is
	// terminated but the child (which the racing scan missed) is left interrupted.
	parentKey, err := e.Create(ctx, "orphanchild.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	out, err := e.Await(ctx, parentKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, out.Status) {
		return
	}
	shard, _, parentFlowID, _, err := keys.ParseFlowKey(parentKey)
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
		workflow.StatusTerminated, "forged", parentFlowID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, parked=? WHERE flow_id=? AND status IN (?, ?)",
		workflow.StatusTerminated, parkedNone, parentFlowID, workflow.StatusInterrupted, workflow.StatusRunning)
	assert.NoError(err)

	// Healthy control: a subgraph child running under a still-running parent.
	healthyKey, err := e.Create(ctx, "orphanchild.verify:0/healthyparent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	_, _, healthyParentID, _, err := keys.ParseFlowKey(healthyKey)
	if !assert.NoError(err) {
		return
	}
	var healthyChildID int
	deadline := time.Now().Add(5 * time.Second * enginetest.TimeoutScale())
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

	// Every child has been still for ten minutes, far past the guard; the orphan's parent went terminal just now.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), -600000) WHERE flow_id IN (?, ?, ?)",
		childFlowID, healthyChildID, healthyParentID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=NOW_UTC() WHERE flow_id=?", parentFlowID)
	assert.NoError(err)
	e.recoverOrphanedSubgraphChildren(ctx, db, shard, time.Minute)
	var sparedStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", childFlowID).Scan(&sparedStatus))
	assert.Equal(workflow.StatusInterrupted, strings.TrimSpace(sparedStatus), "a parent terminal for less than the guard spares the child")

	// Once the parent has been terminal for longer than the guard, the child is swept.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), -600000) WHERE flow_id=?", parentFlowID)
	assert.NoError(err)
	e.recoverOrphanedSubgraphChildren(ctx, db, shard, time.Minute)

	// The orphaned child is terminated with the recovery reason, and its interrupted step is terminated too.
	var childStatus, childReason string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, cancel_reason FROM dwarf_flows WHERE flow_id=?", childFlowID).
		Scan(&childStatus, &childReason))
	assert.Equal(workflow.StatusTerminated, childStatus)
	assert.Equal("parent flow terminated (orphan recovery)", strings.TrimSpace(childReason))
	var liveChildSteps int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status IN (?, ?, ?, ?)",
		childFlowID, workflow.StatusCreated, workflow.StatusPending, workflow.StatusInterrupted, workflow.StatusRunning,
	).Scan(&liveChildSteps))
	assert.Equal(0, liveChildSteps, "orphaned child's steps should all be terminated")

	// The healthy child (parent still running) is untouched.
	var healthyChildStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", healthyChildID).Scan(&healthyChildStatus))
	assert.Equal(workflow.StatusRunning, strings.TrimSpace(healthyChildStatus))
}

// TestOrphanDetection_FlagsWedgedFlow forges the exact state the post-completion transition wedge leaves - a
// `running` flow whose only step is `completed` (no successor) - and asserts recoverOrphanedFlows logs it, while
// a genuinely-running flow (with a live non-terminal step) is left alone even when equally old. The forged step
// carries no advancing flag, so the sweep has nothing to re-run and raises the alarm instead; the assertion is on
// the emitted error log.
func TestOrphanDetection_FlagsWedgedFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	capture := &orphanLogCapture{}
	mux := hosts.NewMux()
	release := make(chan struct{})

	solo := workflow.NewGraph("Solo")
	solo.SetURL("A", "orphan.verify:0/a")
	solo.AddTransition("A", workflow.End)
	mux.HandleGraph("orphan.verify:0/solo", solo)
	mux.HandleTask("orphan.verify:0/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	// A separate graph whose task blocks, so its flow stays running with a live step - the healthy control.
	blocked := workflow.NewGraph("Blocked")
	blocked.SetURL("B", "orphan.verify:0/b")
	blocked.AddTransition("B", workflow.End)
	mux.HandleGraph("orphan.verify:0/blocked", blocked)
	mux.HandleTask("orphan.verify:0/b", func(ctx context.Context, f *workflow.Flow) error {
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.SetLogger(slog.New(capture)))
	assert.NoError(e.Startup(t.Context()))
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
	shard, _, orphanFlowID, _, err := keys.ParseFlowKey(orphanKey)
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
	_, _, healthyFlowID, _, err := keys.ParseFlowKey(healthyKey)
	if !assert.NoError(err) {
		return
	}

	// The Startup sweep runs recoverOrphanedFlows too; let it finish before forging, or it logs the orphan a
	// second time and the "exactly one flagged" assertion below reads two.
	awaitStartupRecoverySweep(t, e)

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
	// The detector's staleness guard is on the STEP row, so the orphan's completed step must ALSO be
	// backdated - a genuine orphan is one whose last step completed longer ago than the threshold. Without
	// this the flow reads as a fresh completed->successor window, not a strand (see TestOrphanDetection_
	// IgnoresCompletedSuccessorWindow). Clearing advancing makes it the strand the sweep cannot repair - one
	// with no lost transition to re-run - which is the case that still raises the alarm.
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET advancing=0, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		pastMs, orphanFlowID)
	assert.NoError(err)

	// A light pass repairs but never raises the alarm; the heavy pass does.
	e.recoverOrphanedFlows(ctx, db, shard, false)
	assert.Len(capture.seen(), 0, "a light pass raised the orphan alarm")
	e.recoverOrphanedFlows(ctx, db, shard, true)

	seen := capture.seen()
	assert.Len(seen, 1, "exactly one flow should be flagged as orphaned")
	if len(seen) == 1 {
		// The alarm logs the token-free correlation id (SEC2), never the capability-bearing flowKey.
		assert.Equal(keys.CorrelationID(shard, orphanFlowID), seen[0], "the flagged flow is the forged orphan, not the healthy blocked flow")
		assert.NotEqual(orphanKey, seen[0], "the logged id carries no token")
	}
}

// TestOrphanDetection_ChecksEveryChunk pins that the scan, which checks the running flows inListChunk at a time,
// finds a stranded flow in every chunk - at both edges of a chunk boundary and in the short last chunk, whose
// last id the IN list's padding repeats - and reports each once and none of the healthy flows around them. The flows are forged as rows: a stranded one is running with no
// step at all, a healthy one holds an interrupted step, which nothing dispatches.
func TestOrphanDetection_ChecksEveryChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	capture := &orphanLogCapture{}
	mux := hosts.NewMux()
	solo := workflow.NewGraph("Solo")
	solo.SetURL("A", "orphanchunk.verify:0/a")
	solo.AddTransition("A", workflow.End)
	mux.HandleGraph("orphanchunk.verify:0/solo", solo)
	mux.HandleTask("orphanchunk.verify:0/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.SetLogger(slog.New(capture)))
	assert.NoError(e.Startup(t.Context()))

	// A completed flow is the template every forged row copies its required columns from.
	key, err := e.Create(ctx, "orphanchunk.verify:0/solo", nil, nil)
	if !assert.NoError(err) {
		return
	}
	out, err := e.Await(ctx, key)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
		return
	}
	shard, _, templateID, _, err := keys.ParseFlowKey(key)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	awaitStartupRecoverySweep(t, e)

	// Two full chunks and a short third of three ids, padded to four by repeating the last.
	const n = 2*inListChunk + 3
	stranded := map[int]bool{0: true, inListChunk - 1: true, inListChunk: true, 2*inListChunk - 1: true, n - 1: true, 300: true}
	var ids []int
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		for range n {
			_, err := tx.ExecContext(ctx,
				"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status)"+
					" SELECT flow_token, workflow_url, workflow_name, graph, graph_hash, '"+workflow.StatusRunning+"' FROM dwarf_flows WHERE flow_id=?",
				templateID)
			if err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE flow_id>? ORDER BY flow_id", templateID)
		if err != nil {
			return err
		}
		ids = ids[:0]
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for i, id := range ids {
			if stranded[i] {
				continue
			}
			_, err := tx.ExecContext(ctx,
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, flow_created_hour) VALUES (?, 1, 'forged', 'A', 'orphanchunk.verify:0/a', '"+workflow.StatusInterrupted+"', 1000, (SELECT created_hour FROM dwarf_flows WHERE flow_id=?))",
				id, id)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if !assert.NoError(err) || !assert.Len(ids, n) {
		return
	}

	e.recoverOrphanedFlows(ctx, db, shard, true)
	var want []string
	for i := range stranded {
		want = append(want, keys.CorrelationID(shard, ids[i]))
	}
	got := capture.seen()
	slices.Sort(want)
	slices.Sort(got)
	assert.Equal(want, got, "the stranded flows of every chunk, and only those, are flagged")
}

// TestOrphanDetection_IgnoresCompletedSuccessorWindow pins that a healthy flow mid-transition, which
// momentarily has all-terminal steps (processStep commits step->completed in a standalone UPDATE, then a
// separate tx inserts the successor), is neither repaired nor flagged. dwarf_flows.updated_at is frozen at
// go-running time, so it cannot tell this window from a strand; what does is the step. This forges the window
// - a `running` flow with a backdated flow row and a RECENTLY completed step - twice: first as it is, the step
// flagged advancing and still under its lease, which the scan skips; then with the flag cleared, the shape no
// known path leaves, which the alarm's age guard must still hold off because the step is fresh. (It is the
// forged orphan of the test above minus the step backdate.)
func TestOrphanDetection_IgnoresCompletedSuccessorWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	capture := &orphanLogCapture{}
	mux := hosts.NewMux()

	solo := workflow.NewGraph("Solo")
	solo.SetURL("A", "orphanwin.verify:0/a")
	solo.AddTransition("A", workflow.End)
	mux.HandleGraph("orphanwin.verify:0/solo", solo)
	mux.HandleTask("orphanwin.verify:0/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.SetLogger(slog.New(capture)))
	assert.NoError(e.Startup(t.Context()))

	key, err := e.Create(ctx, "orphanwin.verify:0/solo", nil, nil)
	if !assert.NoError(err) {
		return
	}
	out, err := e.Await(ctx, key)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
		return
	}
	shard, _, flowID, _, err := keys.ParseFlowKey(key)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}

	// Backdate ONLY the flow row and flip it to running - the step stays freshly completed, which is exactly
	// the completed->successor window a healthy long-running flow passes through on every transition.
	pastMs := -(e.orphanFlowThreshold + time.Minute).Milliseconds()
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET status=?, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		workflow.StatusRunning, pastMs, flowID)
	assert.NoError(err)

	var seqBefore int
	assert.NoError(db.QueryRowContext(ctx, "SELECT lease_seq FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&seqBefore))
	e.recoverOrphanedFlows(ctx, db, shard, true)
	assert.Len(capture.seen(), 0, "a flow whose step was just completed is mid-transition, not orphaned - it must not be flagged")
	var unmoved int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+
		workflow.StatusCompleted+"' AND lease_seq=?", flowID, seqBefore).Scan(&unmoved))
	assert.Equal(1, unmoved, "a flow mid-transition was rewound")

	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET advancing=0 WHERE flow_id=?", flowID)
	assert.NoError(err)
	e.recoverOrphanedFlows(ctx, db, shard, true)
	assert.Len(capture.seen(), 0, "a stranded flow whose step was touched inside orphanFlowThreshold must not be flagged yet")
}
