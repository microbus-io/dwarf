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

// Deterministic race pins for two deferred-deletion behaviors, driven by the fault seam instead of a timing
// hammer:
//   - Resume-loses-to-Delete: a pause fault holds resume() before its transaction so a Delete can win the
//     race every run, proving resume 409s and rolls its step writes back (vs. the stochastic
//     TestDeleteResumeRace, which only hits this branch probabilistically).
//   - Reaper-deletes-a-running-descendant: the reaper removes the whole root_flow_id tree regardless of
//     descendant status, so a live orphan under a terminal root is reaped (deleteFlow guards only the root).
package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestResumeLosesToDelete_Deterministic pins the Resume-vs-Delete race: when a Delete terminalizes an interrupted flow in the
// window after Resume's status check but before its transaction, Resume's in-transaction gate write matches
// zero rows and the whole transaction rolls back - so Resume returns 409 (never a false success) and leaves
// no trace of its step writes (the leaf stays `interrupted`, not flipped to `pending`). The pause fault makes
// the Delete-wins ordering deterministic instead of relying on goroutine timing.
func TestResumeLosesToDelete_Deterministic(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Gate")
	g.SetEndpoint("Gate", "rld/gate")
	g.AddTransition("Gate", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("rld/g", g)
	// Gate interrupts on its first dispatch (yield=true) and rests the flow interrupted.
	proxy.HandleTask("rld/gate", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// Create and wait for it to rest interrupted.
	fk, err := e.Create(ctx, "rld/g", nil, nil)
	assert.NoError(err)
	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusInterrupted, out.Status)
	}

	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)

	// Freeze resume at the checkpoint before its transaction, then launch it. waitFor returns once resume is
	// frozen there (whether it arrives before or after this waitFor).
	e.setBreakpoint(checkpointResumeBeforeFlowWrite)
	resumeDone := make(chan error, 1)
	go func() { resumeDone <- e.Resume(ctx, fk, nil) }()

	waited := make(chan struct{})
	go func() { e.waitFor(checkpointResumeBeforeFlowWrite); close(waited) }()
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
		t.Fatal("Resume never reached the checkpoint")
	}

	// Resume is frozen before its flow-status gate write. Drive a Delete to completion: it flips the flow
	// interrupted->cancelled and stamps delete_after_ms under the flow-row lock.
	assert.NoError(e.Delete(ctx, fk))

	// Release Resume: its transaction now runs, the gate write finds the flow no longer interrupted, and the
	// whole transaction rolls back.
	e.clearBreakpoint(checkpointResumeBeforeFlowWrite)
	select {
	case resumeErr := <-resumeDone:
		assert.Error(resumeErr)
		assert.Equal(409, errors.StatusCode(resumeErr)) // honest 409, not a silent success
	case <-time.After(10 * time.Second):
		t.Fatal("Resume did not return after release")
	}

	// The flow is cancelled (Delete won) with a live deletion stamp.
	var status string
	var deleteAfterMs int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, delete_after_ms FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status, &deleteAfterMs))
	assert.Equal(workflow.StatusCancelled, strings.TrimSpace(status))
	assert.True(deleteAfterMs > 0)

	// Resume's step writes rolled back: no step was flipped to `pending`, and the leaf is still `interrupted`
	// (the transient cancelled-flow-with-non-terminal-steps state the reaper mops up - Resume added nothing to
	// it). A pre-fix Resume would have left the leaf `pending` and returned nil.
	var pending, interrupted int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusPending+"'", flowID).Scan(&pending))
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'", flowID).Scan(&interrupted))
	assert.Equal(0, pending)
	assert.Equal(1, interrupted)

	// The reaper then removes the whole tree, resolving the transient state cleanly. The reaper's due-check is
	// `DATE_ADD_MILLIS(updated_at, delete_after_ms) <= NOW_UTC()`; Delete stamped delete_after_ms=1 but left
	// updated_at at the (just-now) interrupt time, so whether the row is due hinges on sub-ms wall-clock timing
	// - flaky. Backdate updated_at 60s so the due moment is unconditionally in the past and the reap fires every run.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?", -60000, flowID)
	assert.NoError(err)
	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shardNum))
}

// TestReaperDeletesRunningDescendant pins the reaper's tree-delete: it removes the whole root_flow_id tree regardless of
// descendant status. deleteFlow 409s only on a running *root*; a running subgraph *descendant* (the
// Cancel-vs-spawn orphan residue) is deleted anyway. Here the descendant is forged into `running` under a
// terminal root, then the reaper removes it along with the root.
func TestReaperDeletesRunningDescendant(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "rdd/call")
	parent.AddTransition("Call", workflow.END)
	proxy.HandleGraph("rdd/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "rdd/x")
	child.AddTransition("X", workflow.END)
	proxy.HandleGraph("rdd/child", child)
	proxy.HandleTask("rdd/x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("rdd/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("rdd/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// Run parent to completion; the subgraph child completes too, giving a two-flow tree (root + child).
	fk, out, err := e.Run(ctx, "rdd/parent", nil, nil)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	shardNum, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	assert.Equal(2, shardFlowCount(t, e, shardNum)) // root + child

	// Forge the orphan residue: flip the completed child to `running` (a live descendant under a terminal
	// root, as the Cancel-vs-spawn race leaves it - here it inherits root_flow_id = rootID).
	res, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET status='"+workflow.StatusRunning+"' WHERE root_flow_id=? AND surgraph_flow_id<>0", rootID)
	assert.NoError(err)
	if n, _ := res.RowsAffected(); assert.Equal(int64(1), n) {
		var running int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id=? AND surgraph_flow_id<>0 AND status='"+workflow.StatusRunning+"'", rootID).Scan(&running))
		assert.Equal(1, running) // a running descendant now exists
	}

	// Stamp the terminal root due (as Delete/DeleteOnCompletion would) and reap. The reaper only selects roots
	// where `DATE_ADD_MILLIS(updated_at, delete_after_ms) <= NOW_UTC()`, so backdate updated_at 60s to put the
	// due moment unconditionally in the past (delete_after_ms=1 alone hinges on sub-ms timing - flaky). The
	// reaper then deletes by root_flow_id, so the running descendant goes too - no reguard on descendant status.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?", -60000, rootID)
	assert.NoError(err)
	e.reapDueFlows(ctx)

	assert.Equal(0, shardFlowCount(t, e, shardNum)) // whole tree gone, running descendant included
	var steps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps").Scan(&steps))
	assert.Equal(0, steps)
}
