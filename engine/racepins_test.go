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

	"github.com/microbus-io/dwarf/internal/engineseams"
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

	mux := NewHostMux()
	g := workflow.NewGraph("Gate")
	g.SetURL("Gate", "rld/gate")
	g.AddTransition("Gate", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("rld/g", g)
	// Gate interrupts on its first dispatch (yield=true) and rests the flow interrupted.
	mux.HandleTask("rld/gate", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
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
	e.seams.Break(engineseams.CheckpointResumeBeforeFlowWrite)
	resumeDone := make(chan error, 1)
	go func() { resumeDone <- e.Resume(ctx, fk, nil) }()

	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointResumeBeforeFlowWrite, 10*time.Second), "Resume never reached the checkpoint")

	// Resume is frozen before its flow-status gate write. Drive a Delete to completion: it flips the flow
	// interrupted->terminated and stamps delete_after_ms under the flow-row lock.
	assert.NoError(e.Delete(ctx, fk))

	// Release Resume: its transaction now runs, the gate write finds the flow no longer interrupted, and the
	// whole transaction rolls back.
	e.seams.Resume(engineseams.CheckpointResumeBeforeFlowWrite)
	select {
	case resumeErr := <-resumeDone:
		assert.Error(resumeErr)
		assert.Equal(409, errors.StatusCode(resumeErr)) // honest 409, not a silent success
	case <-time.After(10 * time.Second):
		assert.True(false, "Resume did not return after release")
		return
	}

	// The flow is terminated (Delete won) with a live deletion stamp.
	var status string
	var deleteAfterMs int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, delete_after_ms FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status, &deleteAfterMs))
	assert.Equal(workflow.StatusTerminated, status)
	assert.True(deleteAfterMs > 0)

	// Resume's step writes rolled back: no step was flipped to `pending`, and the leaf is still `interrupted`
	// (the transient terminated-flow-with-non-terminal-steps state the reaper mops up - Resume added nothing to
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
// Terminate-vs-spawn orphan residue) is deleted anyway. Here the descendant is forged into `running` under a
// terminal root, then the reaper removes it along with the root.
func TestReaperDeletesRunningDescendant(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := NewHostMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "rdd/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("rdd/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("X", "rdd/x")
	child.AddTransition("X", workflow.End)
	mux.HandleGraph("rdd/child", child)
	mux.HandleTask("rdd/x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("rdd/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("rdd/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
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
	// root, as the Terminate-vs-spawn race leaves it - here it inherits root_flow_id = rootID).
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

// TestTerminateVsTransition_Deterministic pins the transition tx's write-first terminal-status guard: a
// Terminate that terminalizes a flow after its step was marked completed but before the transition
// transaction runs must make the transition a clean no-op - no successor step is inserted into the
// terminated flow, and no orphan results. The checkpoint makes the Terminate-wins ordering deterministic.
func TestTerminateVsTransition_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var bRan int
	mux := NewHostMux()
	g := workflow.NewGraph("Lin")
	g.SetURL("A", "cvt/a")
	g.SetURL("B", "cvt/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("cvt/g", g)
	mux.HandleTask("cvt/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("cvt/b", func(ctx context.Context, f *workflow.Flow) error { bRan++; return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	// Freeze A after it is marked completed, before it inserts B.
	e.seams.Break(engineseams.CheckpointBeforeTransitionTx)
	fk, err := e.Create(ctx, "cvt/g", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeTransitionTx, 10*time.Second), "engine never reached checkpoint engineseams.CheckpointBeforeTransitionTx")

	// Terminate wins while A's transition is held: the flow goes terminated under the flow-row lock.
	assert.NoError(e.Terminate(ctx, fk, "test"))

	// Release A: its transition tx's guard (status NOT IN terminal) matches zero rows and inserts nothing.
	e.seams.Resume(engineseams.CheckpointBeforeTransitionTx)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusTerminated, 10*time.Second)

	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	var bSteps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='B'", flowID).Scan(&bSteps))
	assert.Equal(0, bSteps)           // no successor inserted into the terminated flow
	assert.Equal(0, bRan)             // B never dispatched
	enginetest.AssertInvariants(t, e) // no orphan
}

// TestTerminateVsSubgraphSpawn_Deterministic pins that a Terminate racing a subgraph spawn never leaves a live
// flow, or a live step, under an ended parent. A spawn parks its caller step in the transaction that inserts the
// child; Terminate terminates the tree's steps, locks its flow rows, terminates the steps again, scans for children
// once, and re-runs over the larger set if the scan found any. Each subtest drives one ordering:
//
//   - terminate_first: the spawn finds its caller terminated and rolls back, creating no child.
//   - spawn_after_terminate_scan: the child commits after Terminate's pre-transaction scan; Terminate's
//     in-transaction scan finds and terminates it.
//   - grandchild_after_rescan: a child found by Terminate's scan spawns a grandchild before Terminate locks the
//     child's steps. Terminate must re-run rather than scan again in the same transaction - on MySQL's
//     REPEATABLE READ a second scan would reuse the first one's snapshot and miss the grandchild.
//   - step_created_during_terminate: a transition creates the caller step after Terminate's first step UPDATE,
//     so that UPDATE never touched it; the second one, after the flow rows are locked, does - so the step is
//     terminated and its spawn rolls back. (On SQLite, Terminate's write lock
//     holds the transition off entirely, so the subtest passes without reaching the race there.)
//
// None of them runs the wedge sweep's orphaned-child repair.
func TestTerminateVsSubgraphSpawn_Deterministic(t *testing.T) {
	t.Parallel()

	type rig struct {
		e        *Engine
		mux      *HostMux
		release  chan struct{}
		xEntered chan struct{}
	}
	newRig := func(t *testing.T) (*rig, func()) {
		r := &rig{mux: NewHostMux(), release: make(chan struct{}), xEntered: make(chan struct{}, 8)}
		// Leaf tasks block so a flow that escaped termination would stay running; released at cleanup.
		r.mux.HandleTask("cvs/x", func(ctx context.Context, f *workflow.Flow) error {
			r.xEntered <- struct{}{}
			<-r.release
			return nil
		})
		child := workflow.NewGraph("Child")
		child.SetURL("X", "cvs/x")
		child.AddTransition("X", workflow.End)
		testarossa.For(t).NoError(child.Validate())
		r.mux.HandleGraph("cvs/child", child)
		r.e = NewEngineUnderTest(t.Name())
		r.e.SetHost(r.mux)
		return r, func() { close(r.release); r.e.Shutdown(context.Background()) }
	}
	callSubgraph := func(url string, before func()) func(ctx context.Context, f *workflow.Flow) error {
		return func(ctx context.Context, f *workflow.Flow) error {
			if before != nil {
				before()
			}
			yield, err := f.Subgraph(url, nil, nil)
			if yield || err != nil {
				return err
			}
			return nil
		}
	}
	// assertAllTerminated checks every flow on the shard ended terminated - none left running under a terminated
	// parent - and that the tree's invariants hold.
	assertAllTerminated := func(t *testing.T, e *Engine, fk string) {
		assert := testarossa.For(t)
		shardNum, _, _, _ := keys.ParseFlowKey(fk)
		db, _ := e.db.Shard(shardNum)
		rows, err := db.QueryContext(t.Context(), "SELECT flow_id, workflow_url, status FROM dwarf_flows ORDER BY flow_id")
		if !assert.NoError(err) {
			return
		}
		defer rows.Close()
		for rows.Next() {
			var id int
			var url, status string
			assert.NoError(rows.Scan(&id, &url, &status))
			assert.Equal(workflow.StatusTerminated, status, "flow %d (%s)", id, url)
		}
		enginetest.AssertInvariants(t, e)
	}
	countFlows := func(t *testing.T, e *Engine, fk, workflowURL string) int {
		shardNum, _, _, _ := keys.ParseFlowKey(fk)
		db, _ := e.db.Shard(shardNum)
		var n int
		db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dwarf_flows WHERE workflow_url=?", workflowURL).Scan(&n)
		return n
	}
	awaitFlow := func(t *testing.T, e *Engine, fk, workflowURL string) bool {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if countFlows(t, e, fk, workflowURL) > 0 {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return false
	}
	parentGraph := func(t *testing.T, r *rig) {
		parent := workflow.NewGraph("Parent")
		parent.SetURL("Call", "cvs/call")
		parent.AddTransition("Call", workflow.End)
		testarossa.For(t).NoError(parent.Validate())
		r.mux.HandleGraph("cvs/parent", parent)
		r.mux.HandleTask("cvs/call", callSubgraph("cvs/child", nil))
	}

	t.Run("terminate_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := t.Context()
		r, done := newRig(t)
		defer done()
		parentGraph(t, r)
		assert.NoError(r.e.Startup(ctx))

		r.e.seams.Break(engineseams.CheckpointBeforeSubgraphSpawn)
		fk, err := r.e.Create(ctx, "cvs/parent", nil, nil)
		assert.NoError(err)
		assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeSubgraphSpawn, 10*time.Second), "engine never reached the spawn")
		assert.NoError(r.e.Terminate(ctx, fk, "test"))
		abandoned := r.e.seams.Waiter(engineseams.CheckpointSubgraphSpawnAbandoned)
		r.e.seams.Resume(engineseams.CheckpointBeforeSubgraphSpawn)
		select {
		case <-abandoned:
		case <-time.After(10 * time.Second):
			assert.True(false, "the spawn was never abandoned")
		}
		assert.Zero(countFlows(t, r.e, fk, "cvs/child"), "a spawn after Terminate must create no child")
		assertAllTerminated(t, r.e, fk)
	})

	t.Run("spawn_after_terminate_scan", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := t.Context()
		r, done := newRig(t)
		defer done()
		parentGraph(t, r)
		assert.NoError(r.e.Startup(ctx))

		r.e.seams.Break(engineseams.CheckpointBeforeSubgraphSpawn)
		fk, err := r.e.Create(ctx, "cvs/parent", nil, nil)
		assert.NoError(err)
		assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeSubgraphSpawn, 10*time.Second), "engine never reached the spawn")

		// Terminate scans the tree (no child yet) and freezes before its transaction.
		r.e.seams.Break(engineseams.CheckpointTerminateAfterScan)
		terminated := make(chan error, 1)
		go func() { terminated <- r.e.Terminate(ctx, fk, "test") }()
		assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointTerminateAfterScan, 10*time.Second), "Terminate never finished its scan")

		// The spawn commits its child under a tree Terminate scanned without it.
		r.e.seams.Resume(engineseams.CheckpointBeforeSubgraphSpawn)
		ok := awaitFlow(t, r.e, fk, "cvs/child")
		r.e.seams.Resume(engineseams.CheckpointTerminateAfterScan)
		assert.NoError(<-terminated)
		if assert.True(ok, "the spawn never committed its child") {
			assertAllTerminated(t, r.e, fk)
		}
	})

	t.Run("grandchild_after_rescan", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := t.Context()
		r, done := newRig(t)
		defer done()
		parentGraph(t, r)
		// The child calls a grandchild, gated so it spawns only when the test says.
		middle := workflow.NewGraph("Middle")
		middle.SetURL("CallGrand", "cvs/callgrand")
		middle.AddTransition("CallGrand", workflow.End)
		assert.NoError(middle.Validate())
		r.mux.HandleGraph("cvs/middle", middle)
		r.mux.HandleTask("cvs/call", callSubgraph("cvs/middle", nil))
		gate, entered := make(chan struct{}), make(chan struct{}, 1)
		r.mux.HandleTask("cvs/callgrand", callSubgraph("cvs/child", func() {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-gate
		}))
		assert.NoError(r.e.Startup(ctx))

		r.e.seams.Break(engineseams.CheckpointBeforeSubgraphSpawn)
		fk, err := r.e.Create(ctx, "cvs/parent", nil, nil)
		assert.NoError(err)
		assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeSubgraphSpawn, 10*time.Second), "engine never reached the spawn")

		// Terminate scans before the middle flow exists, then the middle flow commits and its task starts.
		r.e.seams.Break(engineseams.CheckpointTerminateAfterScan)
		r.e.seams.Break(engineseams.CheckpointTerminateAfterRescan)
		terminated := make(chan error, 1)
		go func() { terminated <- r.e.Terminate(ctx, fk, "test") }()
		assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointTerminateAfterScan, 10*time.Second), "Terminate never finished its scan")
		r.e.seams.Resume(engineseams.CheckpointBeforeSubgraphSpawn)
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			assert.True(false, "the middle flow's task never ran")
		}

		// Terminate's transaction locks the root, scans, finds the middle flow, and freezes. Now the middle flow's
		// task spawns the grandchild, committing after that scan's snapshot. The grandchild is observed through its
		// task starting, not by reading the database: Terminate holds its transaction open here, and on SQLite
		// that write lock blocks a read - and the spawn - until it is released.
		r.e.seams.Resume(engineseams.CheckpointTerminateAfterScan)
		assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointTerminateAfterRescan, 10*time.Second), "Terminate never scanned inside its transaction")
		close(gate)
		select {
		case <-r.xEntered:
		case <-time.After(3 * time.Second): // SQLite: the spawn waits behind Terminate's write lock
		}
		r.e.seams.Resume(engineseams.CheckpointTerminateAfterRescan)
		assert.NoError(<-terminated)
		assertAllTerminated(t, r.e, fk)
	})

	t.Run("step_created_during_terminate", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := t.Context()
		r, done := newRig(t)
		defer done()
		parent := workflow.NewGraph("Parent")
		parent.SetURL("A", "cvs/a")
		parent.SetURL("Call", "cvs/call")
		parent.AddTransition("A", "Call")
		parent.AddTransition("Call", workflow.End)
		assert.NoError(parent.Validate())
		r.mux.HandleGraph("cvs/parent", parent)
		r.mux.HandleTask("cvs/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
		gate, entered := make(chan struct{}), make(chan struct{}, 1)
		r.mux.HandleTask("cvs/call", callSubgraph("cvs/child", func() {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-gate
		}))
		assert.NoError(r.e.Startup(ctx))

		// A completes and freezes before the transition that creates Call.
		r.e.seams.Break(engineseams.CheckpointBeforeTransitionTx)
		fk, err := r.e.Create(ctx, "cvs/parent", nil, nil)
		assert.NoError(err)
		assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeTransitionTx, 10*time.Second), "A never reached its transition")

		// Terminate terminates the steps that exist, and freezes before it locks the flow rows.
		r.e.seams.Break(engineseams.CheckpointTerminateAfterStepsLocked)
		terminated := make(chan error, 1)
		go func() { terminated <- r.e.Terminate(ctx, fk, "test") }()
		assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointTerminateAfterStepsLocked, 10*time.Second), "Terminate never locked the steps")

		// The transition creates Call, which Terminate's step UPDATE never saw, and Call is claimed. (Where
		// Terminate's write lock holds the transition off - SQLite - Call is never created at all.)
		r.e.seams.Resume(engineseams.CheckpointBeforeTransitionTx)
		claimed := false
		select {
		case <-entered:
			claimed = true
		case <-time.After(3 * time.Second):
		}
		r.e.seams.Resume(engineseams.CheckpointTerminateAfterStepsLocked)
		assert.NoError(<-terminated)
		// Call was terminated by Terminate's second step UPDATE, so its spawn must roll back.
		abandoned := r.e.seams.Waiter(engineseams.CheckpointSubgraphSpawnAbandoned)
		close(gate)
		if claimed {
			select {
			case <-abandoned:
			case <-time.After(10 * time.Second):
				assert.True(false, "the spawn under a terminated flow was never abandoned")
			}
		}
		assert.Zero(countFlows(t, r.e, fk, "cvs/child"), "a spawn under a terminated flow must create no child")
		assertAllTerminated(t, r.e, fk)
	})
}

// TestRetryRewindVsTerminate_Deterministic pins the flow.Retry rewind's status='running' guard: a Terminate
// that terminalizes the running step before the rewind must stop the rewind from reviving the terminated
// (immutable) step to pending and from reaping the terminated tree's children. The checkpoint makes the
// Terminate-wins ordering deterministic.
func TestRetryRewindVsTerminate_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aRuns int
	mux := NewHostMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "rrc/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("rrc/g", g)
	mux.HandleTask("rrc/a", func(ctx context.Context, f *workflow.Flow) error {
		aRuns++
		f.Retry(0, 1.0, 0, time.Hour) // arm a retry so processStep reaches the rewind
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	// Freeze A before its retry rewind.
	e.seams.Break(engineseams.CheckpointBeforeRetryRewind)
	fk, err := e.Create(ctx, "rrc/g", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeRetryRewind, 10*time.Second), "engine never reached checkpoint engineseams.CheckpointBeforeRetryRewind")

	// Terminate wins: A's running step is flipped terminated under the terminate transaction.
	assert.NoError(e.Terminate(ctx, fk, "test"))

	// Release A: the rewind's status='running' guard matches zero rows, so the terminated step is not revived.
	e.seams.Resume(engineseams.CheckpointBeforeRetryRewind)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusTerminated, 10*time.Second)

	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	var stepStatus string
	var attempt int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, attempt FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&stepStatus, &attempt))
	assert.Equal(workflow.StatusTerminated, stepStatus) // not revived to pending
	assert.Equal(0, attempt)                            // rewind did not bump the attempt
	assert.Equal(1, aRuns)                              // no re-dispatch
	enginetest.AssertInvariants(t, e)
}
