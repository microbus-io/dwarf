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

// Ownership pins: a write that settles something - a flow's completion, a parked caller's park, an interrupted
// leaf - must prove it still owns what it settles, not merely that the thing is in the state it expects.
//   - A stale completing worker does not complete a flow its replay is still recovering.
//   - A stale child's outcome does not settle the park a Retry made for a newer child.
//   - Two Resumes of one leaf: one wins, the other 409s rather than discarding its payload.
//   - A Resume of an interrupt another Resume already answered does not answer the step's next interrupt.
package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestCompleteFlow_StaleWorkerIsFenced pins completeFlow's lease fence. A terminal task A records its completion
// and its worker stalls before the flow-completion transaction; the orphan sweep rewinds A under a new
// generation, a Cancel marks it, and its replay routes the cancellation to the onError handler H. While H runs,
// the stalled worker resumes: its completion must abandon, leaving the flow running for H to finish. Unfenced,
// it completed the flow under the running handler. Dispatch is driven by hand (no workers) so each step runs
// exactly where the race needs it.
func TestCompleteFlow_StaleWorkerIsFenced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("StaleComplete")
	g.SetURL("A", "stalecomplete/a")
	g.SetURL("H", "stalecomplete/h")
	g.AddTransition("A", workflow.End)
	g.AddTransitionOnError("A", "H")
	g.AddTransition("H", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("stalecomplete/g", g)
	mux.HandleTask("stalecomplete/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	hStarted := make(chan struct{})
	releaseH := make(chan struct{})
	mux.HandleTask("stalecomplete/h", func(ctx context.Context, f *workflow.Flow) error {
		close(hStarted)
		<-releaseH
		return f.Set("handled", true)
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.SetWorkers(0))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitStartupRecoverySweep(t, e)

	fk, err := e.Create(ctx, "stalecomplete/g", nil, nil)
	assert.Must(assert.NoError(err))
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.Must(assert.NoError(err))
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	stepOf := func(task string) int {
		var id int
		assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name=?", flowID, task).Scan(&id))
		return id
	}
	noTurn := func() {}
	aID := stepOf("A")

	// The original dispatch of A records its completion and freezes before completing the flow.
	e.seams.Break(engineseams.CheckpointBeforeCompleteFlowWrite)
	original := make(chan error, 1)
	go func() { original <- e.processStep(ctx, shard, aID, noTurn) }()
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeCompleteFlowWrite, 10*time.Second), "A never reached flow completion")

	// Its lease lapses and the orphan sweep rewinds it under a new generation.
	var seqBefore int
	assert.NoError(db.QueryRowContext(ctx, "SELECT lease_seq FROM dwarf_steps WHERE step_id=?", aID).Scan(&seqBefore))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE step_id=?", aID)
	assert.NoError(err)
	e.recoverOrphanedFlows(ctx, db, shard, false)
	var status string
	var seqAfter int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, lease_seq FROM dwarf_steps WHERE step_id=?", aID).Scan(&status, &seqAfter))
	assert.Equal(workflow.StatusPending, status)
	assert.True(seqAfter > seqBefore, "the sweep did not rewind A under a new generation")

	// Cancel marks the rewound step, so its replay routes the cancellation to H, which runs and holds.
	assert.NoError(e.Cancel(ctx, fk, "stop"))
	assert.NoError(e.processStep(ctx, shard, aID, noTurn))
	hID := stepOf("H")
	handler := make(chan error, 1)
	go func() { handler <- e.processStep(ctx, shard, hID, noTurn) }()
	select {
	case <-hStarted:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the handler never started")
	}

	// The stalled worker resumes. Its completion is fenced, and the flow stays open under the running handler.
	e.seams.Resume(engineseams.CheckpointBeforeCompleteFlowWrite)
	select {
	case err := <-original:
		assert.NoError(err)
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the stalled worker never returned")
	}
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status))
	assert.Equal(workflow.StatusRunning, status, "a stale worker completed the flow while its handler was still running")

	// The handler finishes the flow.
	close(releaseH)
	select {
	case err := <-handler:
		assert.NoError(err)
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the handler never returned")
	}
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.True(out.State.GetBool("handled"))
	}
}

// TestSubgraphRevive_StaleChildDoesNotSettleNewPark pins the latest-child guard on a parked caller's deliveries. The
// first child completes but its revive is lost; the wedge sweep delivers it, the caller Retries, and a second child
// parks the same step again - the same step_id, running and parkedSubgraph, exactly what the first child's
// delivery looked for. A late delivery of the first child (the stalled original worker's revive, or a sweep acting
// on what it read before the Retry) must leave the new park alone, successful or failed alike. Unguarded, the
// first child's result settled the second park and the root completed with it while the second child still ran.
func TestSubgraphRevive_StaleChildDoesNotSettleNewPark(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	root := workflow.NewGraph("StaleChildRoot")
	root.SetURL("Caller", "stalechild/caller")
	root.AddTransition("Caller", workflow.End)
	mux.HandleGraph("stalechild/root", root)
	child := workflow.NewGraph("StaleChild")
	child.SetURL("Work", "stalechild/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("stalechild/child", child)

	mux.HandleTask("stalechild/caller", func(ctx context.Context, f *workflow.Flow) error {
		var out struct {
			Answer int `json:"answer"`
		}
		yield, err := f.Subgraph("stalechild/child", nil, &out)
		if yield || err != nil {
			return err
		}
		if f.Attempt() == 0 {
			f.Retry(0, 1, 0, 0)
			return nil
		}
		return f.Set("answer", out.Answer)
	})
	var childRuns atomic.Int32
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	mux.HandleTask("stalechild/work", func(ctx context.Context, f *workflow.Flow) error {
		if childRuns.Add(1) == 1 {
			return f.Set("answer", 1)
		}
		close(secondStarted)
		<-releaseSecond
		return f.Set("answer", 2)
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitStartupRecoverySweep(t, e)

	// The first child's revive is lost, wedging the caller on a completed child.
	e.seams.Inject(engineseams.FaultSubgraphReviveLost)
	fk, err := e.Create(ctx, "stalechild/root", nil, nil)
	assert.Must(assert.NoError(err))
	shard, _, rootFlowID, _, err := keys.ParseFlowKey(fk)
	assert.Must(assert.NoError(err))
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var callerID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='Caller'", rootFlowID).Scan(&callerID))
	completedChild := "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_step_id=? AND status='" + workflow.StatusCompleted + "'"
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return enginetest.CountRows(t, e, shard, completedChild, callerID) == 1 }),
		"the first child never completed")
	var firstChildID int
	var firstResult []byte
	assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id, final_state FROM dwarf_flows WHERE surgraph_step_id=?", callerID).Scan(&firstChildID, &firstResult))

	// The sweep delivers the first child; the caller Retries and parks again on a second child, which holds.
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0)
	select {
	case <-secondStarted:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the second child never started")
	}
	parked := func() bool {
		var status string
		var park, done int
		assert.NoError(db.QueryRowContext(ctx, "SELECT status, parked, subgraph_done FROM dwarf_steps WHERE step_id=?", callerID).Scan(&status, &park, &done))
		return status == workflow.StatusRunning && park == parkedSubgraph && done == 0
	}
	assert.True(enginetest.WaitUntil(t, 10*time.Second, parked), "the caller never parked on the second child")

	// Late deliveries for the first child, and the sweep's gone-child form, all leave the second park alone.
	assert.NoError(e.completeSurgraphFlow(ctx, shard, rootFlowID, callerID, firstChildID, firstResult))
	assert.True(parked(), "the first child's result settled the second child's park")
	assert.NoError(e.deliverSubgraphError(ctx, shard, callerID, firstChildID, errors.New("stale failure")))
	assert.True(parked(), "the first child's failure settled the second child's park")
	assert.NoError(e.deliverSubgraphError(ctx, shard, callerID, 0, errors.New("stale gone child")))
	assert.True(parked(), "a gone-child delivery settled a park whose child exists")

	// The second child completes, and its own result reaches the root.
	close(releaseSecond)
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(2, out.State.GetInt("answer"))
	}
	enginetest.AssertInvariants(t, e)
}

// TestResume_ConcurrentResumesOfOneLeaf pins Resume's claim on the leaf it selected. Two branches rest
// interrupted; two Resumes are frozen before their transactions, both having selected the same leaf. One must
// succeed and the other 409, its payload refused rather than silently discarded. The root gate cannot catch it:
// the other branch keeps the root interrupted, so unguarded both returned nil.
func TestResume_ConcurrentResumesOfOneLeaf(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("TwoGates")
	g.SetURL("S", "twogates/s")
	g.SetURL("G1", "twogates/g1")
	g.SetURL("G2", "twogates/g2")
	g.SetURL("J", "twogates/j")
	g.AddTransitionFanOut("S", "G1", "G2")
	g.AddTransition("G1", "J")
	g.AddTransition("G2", "J")
	g.SetFanIn("J")
	g.AddTransition("J", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("twogates/g", g)
	noop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	gate := func(field string) func(ctx context.Context, f *workflow.Flow) error {
		return func(ctx context.Context, f *workflow.Flow) error {
			var in struct {
				V int `json:"v"`
			}
			yield, err := f.Interrupt(nil, &in)
			if yield || err != nil {
				return err
			}
			return f.Set(field, in.V)
		}
	}
	mux.HandleTask("twogates/s", noop)
	mux.HandleTask("twogates/g1", gate("g1"))
	mux.HandleTask("twogates/g2", gate("g2"))
	mux.HandleTask("twogates/j", noop)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk, err := e.Create(ctx, "twogates/g", nil, nil)
	assert.Must(assert.NoError(err))
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.Must(assert.NoError(err))
	interrupted := "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='" + workflow.StatusInterrupted + "'"
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return enginetest.CountRows(t, e, shard, interrupted, flowID) == 2 }),
		"both branches never interrupted")

	// Both Resumes select the same leaf, then freeze before their transactions.
	base := e.seams.Visits(engineseams.CheckpointResumeBeforeFlowWrite)
	e.seams.Break(engineseams.CheckpointResumeBeforeFlowWrite)
	results := make(chan error, 2)
	go func() { results <- e.Resume(ctx, fk, map[string]any{"v": 1}) }()
	go func() { results <- e.Resume(ctx, fk, map[string]any{"v": 2}) }()
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
		return e.seams.Visits(engineseams.CheckpointResumeBeforeFlowWrite) >= base+2
	}), "both Resumes never reached the checkpoint")
	e.seams.Resume(engineseams.CheckpointResumeBeforeFlowWrite)

	var succeeded, conflicted int
	for range 2 {
		select {
		case err := <-results:
			switch {
			case err == nil:
				succeeded++
			case errors.StatusCode(err) == 409:
				conflicted++
			default:
				assert.NoError(err)
			}
		case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
			t.Fatal("a Resume never returned")
		}
	}
	assert.Equal(1, succeeded)
	assert.Equal(1, conflicted, "both Resumes succeeded, but one payload was discarded")
	assert.Equal(1, enginetest.CountRows(t, e, shard, interrupted, flowID))

	// The remaining leaf resumes on its own, and the flow completes with both answers.
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
		var status string
		assert.NoError(func() error {
			sh, err := e.db.Shard(shard)
			if err != nil {
				return err
			}
			db := sh.DB()
			return db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status)
		}())
		return status == workflow.StatusInterrupted
	}), "the flow did not rest interrupted on the remaining leaf")
	assert.NoError(e.Resume(ctx, fk, map[string]any{"v": 3}))
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.True(out.State.GetInt("g1") != 0 && out.State.GetInt("g2") != 0, "a branch's resume data is missing")
	}
	enginetest.AssertInvariants(t, e)
}

// TestSubgraphRevive_StaleDeliveryWaitingOnTheRowLock pins why the park's owner is a column of the caller row
// rather than read from the child flows. On PostgreSQL a delivery's UPDATE takes its snapshot before it waits on the
// caller row's lock; when the lock frees it rechecks the row it then finds, but anything else it reads comes from the
// older snapshot. So the stale delivery here starts while child A owns the park and blocks on a transaction holding
// the caller row; that transaction re-parks the caller on another child - A detached, as a Retry's reap would leave
// it, and a new owner written, as the respawn would - and commits. A guard reading the child flows still sees A and
// lets the stale result settle the new park; the owner column, rechecked on the row, refuses it. The re-park is
// written by hand because no engine operation can run while the test holds the row. PostgreSQL only: the other
// dialects take their snapshot or their lock in the other order.
func TestSubgraphRevive_StaleDeliveryWaitingOnTheRowLock(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	root := workflow.NewGraph("RowLockRoot")
	root.SetURL("Caller", "rowlock/caller")
	root.AddTransition("Caller", workflow.End)
	mux.HandleGraph("rowlock/root", root)
	child := workflow.NewGraph("RowLockChild")
	child.SetURL("Work", "rowlock/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("rowlock/child", child)
	mux.HandleTask("rowlock/caller", func(ctx context.Context, f *workflow.Flow) error {
		var out struct {
			Answer int `json:"answer"`
		}
		yield, err := f.Subgraph("rowlock/child", nil, &out)
		if yield || err != nil {
			return err
		}
		return f.Set("answer", out.Answer)
	})
	started := make(chan struct{})
	release := make(chan struct{})
	mux.HandleTask("rowlock/work", func(ctx context.Context, f *workflow.Flow) error {
		close(started)
		<-release
		return f.Set("answer", 2)
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	if db.DriverName() != "pgx" {
		t.Skip("the snapshot-before-lock interleaving is PostgreSQL's")
	}

	fk, err := e.Create(ctx, "rowlock/root", nil, nil)
	assert.Must(assert.NoError(err))
	shard, _, rootFlowID, _, err := keys.ParseFlowKey(fk)
	assert.Must(assert.NoError(err))
	select {
	case <-started:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the child never started")
	}
	var callerID, owner, childA int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id, parked_child_flow_id FROM dwarf_steps WHERE flow_id=? AND task_name='Caller'", rootFlowID).Scan(&callerID, &owner))
	assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_step_id=?", callerID).Scan(&childA))
	assert.Equal(childA, owner, "the spawn did not record its child as the park's owner")

	// Hold the caller row, and start a stale delivery for A: it takes its snapshot and waits on the row.
	holder, err := db.BeginTx(ctx, nil)
	assert.Must(assert.NoError(err))
	defer holder.Rollback()
	_, err = holder.ExecContext(ctx, "SELECT step_id FROM dwarf_steps WHERE step_id=? FOR UPDATE", callerID)
	assert.Must(assert.NoError(err))
	delivered := make(chan error, 1)
	go func() {
		delivered <- e.completeSurgraphFlow(ctx, shard, rootFlowID, callerID, childA, []byte(`{"answer":1}`))
	}()
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
		var waiting int
		assert.NoError(db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%parked_child_flow_id=0%'",
		).Scan(&waiting))
		return waiting == 1
	}), "the stale delivery never waited on the caller row")

	// Re-park the caller on another child and commit, releasing the stale delivery.
	_, err = holder.ExecContext(ctx, "UPDATE dwarf_flows SET surgraph_step_id=0 WHERE flow_id=?", childA)
	assert.NoError(err)
	_, err = holder.ExecContext(ctx, "UPDATE dwarf_steps SET parked_child_flow_id=? WHERE step_id=?", childA+1_000_000, callerID)
	assert.NoError(err)
	assert.Must(assert.NoError(holder.Commit()))
	select {
	case err := <-delivered:
		assert.NoError(err)
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the stale delivery never returned")
	}
	var status string
	var park, done int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, parked, subgraph_done FROM dwarf_steps WHERE step_id=?", callerID).Scan(&status, &park, &done))
	assert.Equal(workflow.StatusRunning, status, "the stale delivery settled the new park")
	assert.Equal(parkedSubgraph, park)
	assert.Equal(0, done)

	// Put A back as the owner; its own completion then finishes the root.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET surgraph_step_id=? WHERE flow_id=?", callerID, childA)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET parked_child_flow_id=? WHERE step_id=?", childA, callerID)
	assert.NoError(err)
	close(release)
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(2, out.State.GetInt("answer"))
	}
}

// TestResume_StaleResumeOfAReinterruptedLeaf pins Resume's fence on the interrupt it SELECTED, not merely on the
// leaf being interrupted. Resume A selects Gate's first interrupt and freezes; Resume B, on a peer, answers it; Gate
// retries and interrupts again on the same step row. A must 409 and every prompt must get its own answer. A freezes
// either right after its selection or just before its transaction, since the generation must be the one the
// selection saw: read again later, it is the re-interrupt's. With an interrupted sibling, B's Resume records the
// sibling as the next prompt, so a stale A answering Gate's second interrupt also skips the recorded one.
func TestResume_StaleResumeOfAReinterruptedLeaf(t *testing.T) {
	t.Parallel()
	for _, freeze := range []string{engineseams.CheckpointResumeLeafSelected, engineseams.CheckpointResumeBeforeFlowWrite} {
		for _, sibling := range []bool{false, true} {
			name := freeze + "/lone_gate"
			if sibling {
				name = freeze + "/with_sibling"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				staleResumeOfAReinterruptedLeaf(t, freeze, sibling)
			})
		}
	}
}

func staleResumeOfAReinterruptedLeaf(t *testing.T, freeze string, sibling bool) {
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Gate")
	if sibling {
		g.SetURL("S", "reinterrupt/s") // the entry point: the first task given a URL
		g.SetURL("Gate", "reinterrupt/gate")
		g.SetURL("Sibling", "reinterrupt/sibling")
		g.SetURL("J", "reinterrupt/j")
		g.AddTransitionFanOut("S", "Gate", "Sibling")
		g.AddTransition("Gate", "J")
		g.AddTransition("Sibling", "J")
		g.SetFanIn("J")
		g.AddTransition("J", workflow.End)
	} else {
		g.SetURL("Gate", "reinterrupt/gate")
		g.AddTransition("Gate", workflow.End)
	}
	mux.HandleGraph("reinterrupt/g", g)
	type reply struct {
		Answer string `json:"answer"`
	}
	noop := func(context.Context, *workflow.Flow) error { return nil }
	mux.HandleTask("reinterrupt/s", noop)
	mux.HandleTask("reinterrupt/j", noop)
	mux.HandleTask("reinterrupt/gate", func(ctx context.Context, f *workflow.Flow) error {
		var r reply
		yield, err := f.Interrupt(map[string]any{"round": f.Attempt()}, &r)
		if yield || err != nil {
			return err
		}
		if f.Attempt() == 0 {
			f.Retry(0, 1, 0, 0) // ask again, on the same step row
			return nil
		}
		f.SetString("accepted", r.Answer)
		return nil
	})
	releaseSibling := make(chan struct{})
	var siblingRuns atomic.Int32
	mux.HandleTask("reinterrupt/sibling", func(ctx context.Context, f *workflow.Flow) error {
		var r reply
		if siblingRuns.Add(1) == 1 {
			<-releaseSibling // interrupt after Gate, so Gate's interrupt is the recorded one
		}
		yield, err := f.Interrupt(nil, &r)
		if yield || err != nil {
			return err
		}
		f.SetString("sibling", r.Answer)
		return nil
	})

	// The frozen Resume runs on e; the one that answers runs on a peer, whose checkpoints are not armed.
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	p := NewEngineUnderTest(t.Name())
	defer p.Shutdown(ctx)
	defer close(releaseSibling) // ahead of the Shutdowns, so a held Sibling never blocks a drain
	for _, eng := range []*Engine{e, p} {
		eng.SetHost(mux)
		assert.Must(assert.NoError(eng.Startup(t.Context())))
	}

	fk, err := e.Create(ctx, "reinterrupt/g", nil, nil)
	assert.Must(assert.NoError(err))
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.Must(assert.NoError(err))
	interrupted := func(task string, attempt int) bool {
		q := "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name=? AND attempt=? AND status='" + workflow.StatusInterrupted + "'"
		return enginetest.CountRows(t, e, shard, q, flowID, task, attempt) == 1
	}
	assert.Must(assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return interrupted("Gate", 0) }), "Gate never interrupted"))
	if sibling {
		releaseSibling <- struct{}{}
		assert.Must(assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return interrupted("Sibling", 0) }), "Sibling never interrupted"))
	}

	e.seams.Break(freeze)
	defer e.seams.Resume(freeze)
	reached := e.seams.Waiter(freeze)
	stale := make(chan error, 1)
	go func() { stale <- e.Resume(ctx, fk, map[string]any{"answer": "stale"}) }()
	select {
	case <-reached:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the stale Resume never reached its checkpoint")
	}
	assert.Must(assert.NoError(p.Resume(ctx, fk, map[string]any{"answer": "first"}))) // answers Gate's first interrupt
	assert.Must(assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return interrupted("Gate", 1) }), "Gate never interrupted again"))

	e.seams.Resume(freeze)
	select {
	case err := <-stale:
		assert.Equal(409, errors.StatusCode(err), "the stale Resume answered a later prompt: %v", err)
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the stale Resume never returned")
	}
	snap, err := e.Snapshot(ctx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusInterrupted, snap.Status, "a prompt was answered without a Resume of its own")
	}

	if sibling {
		assert.NoError(p.Resume(ctx, fk, map[string]any{"answer": "sibling"})) // the recorded next prompt
	}
	assert.NoError(p.Resume(ctx, fk, map[string]any{"answer": "second"}))
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal("second", out.State.GetString("accepted"))
		if sibling {
			assert.Equal("sibling", out.State.GetString("sibling"))
		}
	}
}
