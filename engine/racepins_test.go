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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/enginetest"
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
	t.Parallel()
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

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

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

	// Freeze resume at the checkpoint before its transaction, then launch it. The wait returns once resume is
	// frozen there, whether it arrives before or after the wait is armed.
	e.seams.Break(CheckpointResumeBeforeFlowWrite)
	resumeDone := make(chan error, 1)
	go func() { resumeDone <- e.Resume(ctx, fk, nil) }()

	assert.True(e.seams.WaitTimeout(ctx, CheckpointResumeBeforeFlowWrite, 10*time.Second), "Resume never reached the checkpoint")

	// Resume is frozen before its flow-status gate write. Drive a Delete to completion: it flips the flow
	// interrupted->cancelled and stamps delete_after_ms under the flow-row lock.
	assert.NoError(e.Delete(ctx, fk))

	// Release Resume: its transaction now runs, the gate write finds the flow no longer interrupted, and the
	// whole transaction rolls back.
	e.seams.Resume(CheckpointResumeBeforeFlowWrite)
	select {
	case resumeErr := <-resumeDone:
		assert.Error(resumeErr)
		assert.Equal(409, errors.StatusCode(resumeErr)) // honest 409, not a silent success
	case <-time.After(10 * time.Second):
		assert.True(false, "Resume did not return after release")
		return
	}

	// The flow is cancelled (Delete won) with a live deletion stamp.
	var status string
	var deleteAfterMs int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, delete_after_ms FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status, &deleteAfterMs))
	assert.Equal(workflow.StatusCancelled, status)
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
	t.Parallel()
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

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

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

// TestCancelVsTransition_Deterministic pins the transition tx's write-first terminal-status guard: a Cancel
// that terminalizes a flow after its step was marked completed but before the transition transaction runs
// must make the transition a clean no-op - no successor step is inserted into the cancelled flow, and no
// orphan results. The checkpoint makes the Cancel-wins ordering deterministic.
func TestCancelVsTransition_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var bRan int
	proxy := NewTestProxy()
	g := workflow.NewGraph("Lin")
	g.SetEndpoint("A", "cvt/a")
	g.SetEndpoint("B", "cvt/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("cvt/g", g)
	proxy.HandleTask("cvt/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("cvt/b", func(ctx context.Context, f *workflow.Flow) error { bRan++; return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// Freeze A after it is marked completed, before it inserts B.
	e.seams.Break(CheckpointBeforeTransitionTx)
	fk, err := e.Create(ctx, "cvt/g", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, CheckpointBeforeTransitionTx, 10*time.Second), "engine never reached checkpoint CheckpointBeforeTransitionTx")

	// Cancel wins while A's transition is held: the flow goes cancelled under the flow-row lock.
	assert.NoError(e.Cancel(ctx, fk, "test"))

	// Release A: its transition tx's guard (status NOT IN terminal) matches zero rows and inserts nothing.
	e.seams.Resume(CheckpointBeforeTransitionTx)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCancelled, 10*time.Second)

	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	var bSteps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='B'", flowID).Scan(&bSteps))
	assert.Equal(0, bSteps)           // no successor inserted into the cancelled flow
	assert.Equal(0, bRan)             // B never dispatched
	enginetest.AssertInvariants(t, e) // no orphan
}

// TestCancelVsSubgraphSpawn_Deterministic pins the orphaned-child recovery: a Cancel that terminalizes the
// tree in the window after the caller step parked but before the child flow was inserted leaves a live child
// under a terminal parent - an orphan no lifecycle op reaches. The wedge sweep's recoverOrphanedSubgraphChildren
// must cancel it. The checkpoint manufactures this residue deterministically (vs. the Cancel-vs-spawn timing race).
func TestCancelVsSubgraphSpawn_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	xRelease := make(chan struct{})
	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "cvs/call")
	parent.AddTransition("Call", workflow.END)
	assert.NoError(parent.Validate())
	proxy.HandleGraph("cvs/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "cvs/x")
	child.AddTransition("X", workflow.END)
	assert.NoError(child.Validate())
	proxy.HandleGraph("cvs/child", child)
	proxy.HandleTask("cvs/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("cvs/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	// X blocks so the orphaned child stays non-terminal (running) when the sweep runs; released at cleanup.
	proxy.HandleTask("cvs/x", func(ctx context.Context, f *workflow.Flow) error {
		<-xRelease
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))
	// Release X here, AFTER the Shutdown defer, so it unwinds FIRST (defers are LIFO): Shutdown drains
	// workers, so a worker still blocked in X must be unblocked first or the drain deadlocks.
	defer close(xRelease)

	// Freeze the caller after it parked, before createSubgraphFlow inserts the child.
	e.seams.Break(CheckpointAfterCallerPark)
	fk, err := e.Create(ctx, "cvs/parent", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, CheckpointAfterCallerPark, 10*time.Second), "engine never reached checkpoint CheckpointAfterCallerPark")

	// Cancel the tree while the child does not yet exist: teardown works from a scan taken before the child.
	assert.NoError(e.Cancel(ctx, fk, "test"))

	// Release the caller: createSubgraphFlow now inserts the child under the already-cancelled parent - orphan.
	e.seams.Resume(CheckpointAfterCallerPark)

	shardNum, parentFlowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)

	// Wait for the orphaned child flow to be inserted (surgraph_flow_id -> the cancelled parent).
	var childFlowID int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		db.QueryRowContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id=?", parentFlowID).Scan(&childFlowID)
		if childFlowID != 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !assert.True(childFlowID != 0, "orphaned child flow was never inserted") {
		return
	}

	// The sweep (minAge 0, bypassing the steady-state age guard) must cancel the orphaned child's subtree.
	e.recoverOrphanedSubgraphChildren(ctx, db, shardNum, 0)

	var childStatus string
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", childFlowID).Scan(&childStatus)
		if childStatus == workflow.StatusCancelled {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.Equal(workflow.StatusCancelled, childStatus) // orphan shares the parent's terminal fate
	enginetest.AssertInvariants(t, e)
}

// TestRetryRewindVsCancel_Deterministic pins the flow.Retry rewind's status='running' guard: a Cancel that
// terminalizes the running step before the rewind must stop the rewind from reviving the cancelled (immutable)
// step to pending and from reaping the cancelled tree's children. The checkpoint makes the Cancel-wins ordering
// deterministic.
func TestRetryRewindVsCancel_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aRuns int
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "rrc/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("rrc/g", g)
	proxy.HandleTask("rrc/a", func(ctx context.Context, f *workflow.Flow) error {
		aRuns++
		f.Retry(0, 1.0, 0, time.Hour) // arm a retry so processStep reaches the rewind
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// Freeze A before its retry rewind.
	e.seams.Break(CheckpointBeforeRetryRewind)
	fk, err := e.Create(ctx, "rrc/g", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, CheckpointBeforeRetryRewind, 10*time.Second), "engine never reached checkpoint CheckpointBeforeRetryRewind")

	// Cancel wins: A's running step is flipped cancelled under the cancel transaction.
	assert.NoError(e.Cancel(ctx, fk, "test"))

	// Release A: the rewind's status='running' guard matches zero rows, so the cancelled step is not revived.
	e.seams.Resume(CheckpointBeforeRetryRewind)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCancelled, 10*time.Second)

	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	var stepStatus string
	var attempt int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, attempt FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&stepStatus, &attempt))
	assert.Equal(workflow.StatusCancelled, stepStatus) // not revived to pending
	assert.Equal(0, attempt)                           // rewind did not bump the attempt
	assert.Equal(1, aRuns)                             // no re-dispatch
	enginetest.AssertInvariants(t, e)
}
