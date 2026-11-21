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

// New fault sites, each ending a "documented but untested" recovery gap. Every test arms one of the
// new faults (debug.go) and drives the exact recovery path it exists to exercise - the residual orphan hole,
// a subgraph-spawn failure, an atomic lifecycle-transaction rollback, host-panic isolation, a lost
// failure-delivery backstopped by the wedge sweep, and the janitor's SELECT-error resilience.
package engine

import (
	"context"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// orphanShapeCount counts flows in the "orphan" shape on a shard: running with zero non-terminal steps (all
// steps terminal, no successor) - exactly what recoverOrphanedFlows alarms on and assertInvariants forbids.
func orphanShapeCount(t *testing.T, e *Engine, shard int) int {
	return enginetest.CountRows(t, e, shard,
		"SELECT COUNT(*) FROM dwarf_flows f WHERE f.status='"+workflow.StatusRunning+"'"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status IN ('"+
			workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"'))")
}

// TestFaultSite_RecoveryResetErr pins the one fault outcome the recovery defer cannot heal, and its repair. The
// transition transaction exhausts its lock-contention retries - contention is the one failure persist hands to the
// defer rather than retrying in place - and the defer's own completed->pending reset then fails too
// (diagnostics.FaultRecoveryResetErr). A is left `completed` with advancing=1 on a `running` flow and no worker holds it: the
// shape a worker that died in that window leaves. Once A's lease lapses, the orphan sweep rewinds
// it, dispatch replays it, and the flow completes - with A's task having run exactly once.
func TestFaultSite_RecoveryResetErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	g := workflow.NewGraph("Pair")
	g.SetURL("A", "ftbreset/a")
	g.SetURL("B", "ftbreset/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph("ftbreset/g", g)
	var runsA, runsB atomic.Int32
	mux.HandleTask("ftbreset/a", func(ctx context.Context, f *workflow.Flow) error { runsA.Add(1); return nil })
	mux.HandleTask("ftbreset/b", func(ctx context.Context, f *workflow.Flow) error { runsB.Add(1); return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// InjectN(8) is one full round of Transact's retries (transactMaxAttempts), so the transition gives up with a
	// contention error; the reset fault fires once on the defer that follows.
	e.diags.Seams().InjectN(diagnostics.Join(diagnostics.FaultContention, "A"), 8)
	e.diags.Seams().Inject(diagnostics.FaultRecoveryResetErr)
	fk, err := e.Create(ctx, "ftbreset/g", nil, nil)
	assert.NoError(err)
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	stranded := func() bool {
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND status='"+
			workflow.StatusCompleted+"' AND advancing=1", flowID) == 1 &&
			enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", flowID) == 1 &&
			orphanShapeCount(t, e, shard) == 1
	}
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	// The failed reset is the last thing A's dispatch does. Once it has returned, let every shard's piston scan
	// and push twice: nothing else may move the step, since lease recovery never touches a completed one.
	var aStepID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStepID))
	enginetest.AwaitVisitCount(t, e.diags.Seams(), 1, 15*time.Second,
		diagnostics.Join(diagnostics.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(aStepID)))
	enginetest.AwaitShardCycles(t, e, 2)
	assert.True(stranded(), "expected A completed and flagged, alone on a running flow, until the sweep runs")
	assert.Equal(int32(1), runsA.Load())

	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", flowID)
	assert.NoError(err)
	e.recoverOrphanedFlows(ctx, db, shard)

	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int32(1), runsA.Load(), "A ran again instead of being replayed")
	assert.Equal(int32(1), runsB.Load())
	enginetest.AssertInvariants(t, e)
}

// TestFaultSite_SubgraphSpawnErr pins the subgraph spawn's failure outcomes. A spawn that fails once is a
// write persist retries in place: the child is created and the flow completes, with the caller's task run once.
// A spawn that keeps failing fails the caller cleanly rather than leaving it parked, and leaves no child flow -
// the park and the child's insert share a transaction, so a failed spawn never leaves a parked caller behind.
func TestFaultSite_SubgraphSpawnErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "ftbspawn/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("ftbspawn/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("X", "ftbspawn/x")
	child.AddTransition("X", workflow.End)
	mux.HandleGraph("ftbspawn/child", child)
	var calls atomic.Int32
	callChild := subgraphTask("ftbspawn/child")
	mux.HandleTask("ftbspawn/call", func(ctx context.Context, f *workflow.Flow) error {
		calls.Add(1)
		return callChild(ctx, f)
	})
	// childGate, when set, holds the child's task until it is closed.
	var childGate atomic.Pointer[chan struct{}]
	mux.HandleTask("ftbspawn/x", func(ctx context.Context, f *workflow.Flow) error {
		if gate := childGate.Load(); gate != nil {
			select {
			case <-*gate:
			case <-time.After(10 * time.Second):
			}
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(mux)
	e.persistBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	reader := withManualReader(e)
	if !assert.NoError(e.Startup(t.Context())) {
		return
	}
	spawnFault := diagnostics.Join(diagnostics.FaultSubgraphSpawnErr, "ftbspawn/child")

	t.Run("transient", func(t *testing.T) {
		assert := testarossa.For(t)
		calls.Store(0)
		e.diags.Seams().Inject(spawnFault)
		_, out := batteryRun(t, e, "ftbspawn/parent")
		assert.Equal(workflow.StatusCompleted, out.Status)
		// Once to spawn, once on the child's return. A third run is the task re-executed for a failed spawn.
		assert.Equal(int32(2), calls.Load(), "the caller's task ran again instead of its spawn being retried")
		assertFaultRecoveryClean(t, e, reader)
	})

	// The spawn's transaction commits but its acknowledgement is lost. The retry must find the caller already
	// parked and stop there, rather than spawn a second child flow.
	t.Run("commit_lost", func(t *testing.T) {
		assert := testarossa.For(t)
		calls.Store(0)
		// The child is held until the caller's dispatch - the spawn and its retry - has returned, or a quick child
		// would revive the caller first and the retry would find it gone, passing for the wrong reason.
		gate := make(chan struct{})
		childGate.Store(&gate)
		defer childGate.Store(nil)
		base := e.diags.Seams().Visits(diagnostics.CheckpointClaimedDispatchDone)
		go func() {
			for {
				reached := e.diags.Seams().Waiter(diagnostics.CheckpointClaimedDispatchDone)
				if e.diags.Seams().Visits(diagnostics.CheckpointClaimedDispatchDone) > base {
					close(gate)
					return
				}
				<-reached
			}
		}()
		e.diags.Seams().Inject(diagnostics.Join(diagnostics.FaultSubgraphSpawnCommitLost, "ftbspawn/child"))
		fk, out := batteryRun(t, e, "ftbspawn/parent")
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(int32(2), calls.Load(), "the caller's task ran again instead of its spawn being recognized as landed")
		shard, _, flowID, _, err := keys.ParseFlowKey(fk)
		assert.NoError(err)
		assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id=?", flowID),
			"a retried spawn created a second child")
		assertFaultRecoveryClean(t, e, reader)
	})

	t.Run("persistent", func(t *testing.T) {
		assert := testarossa.For(t)
		e.diags.Seams().InjectN(spawnFault, math.MaxInt)
		defer e.diags.Seams().Withdraw(spawnFault)
		fk, out := batteryRun(t, e, "ftbspawn/parent")
		assert.Equal(workflow.StatusFailed, out.Status)
		shard, _, flowID, _, err := keys.ParseFlowKey(fk)
		assert.NoError(err)
		// No child flow was created (the spawn failed before insert), so no orphan lingers.
		assert.Equal(0, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id=?", flowID))
		assertFaultRecoveryClean(t, e, reader) // caller failed + un-parked; invariants clean
	})
}

// createInterruptedFlow creates a flow whose entry task interrupts, and blocks until it rests `interrupted`.
func createInterruptedFlow(t *testing.T, e *Engine, url string) string {
	t.Helper()
	assert := testarossa.For(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second*enginetest.TimeoutScale())
	defer cancel()
	fk, err := e.Create(ctx, url, nil, nil)
	assert.NoError(err)
	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusInterrupted, out.Status)
	}
	return fk
}

// registerGate registers a one-task graph whose entry task interrupts on first dispatch and completes on
// resume, so a test has a stable `interrupted` flow to Cancel/Resume.
func registerGate(mux *hosts.Mux, prefix string) {
	g := workflow.NewGraph("Gate")
	g.SetURL("Gate", prefix+"/gate")
	g.AddTransition("Gate", workflow.End)
	mux.HandleGraph(prefix+"/g", g)
	mux.HandleTask(prefix+"/gate", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
}

// TestFaultSite_TerminateCommit pins that a Terminate whose transaction fails once rolls back atomically (the
// tree is untouched, flow still interrupted) and a retry then terminates cleanly.
func TestFaultSite_TerminateCommit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	registerGate(mux, "ftbcancel")

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	reader := withManualReader(e)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk := createInterruptedFlow(t, e, "ftbcancel/g")
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	// The Terminate transaction fails once: Terminate errors and nothing changed (still interrupted).
	e.diags.Seams().Inject(diagnostics.FaultTerminateCommit)
	err = e.Terminate(ctx, fk, "boom")
	assert.Error(err)
	assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'", flowID))

	// Retry (fault consumed): Terminate succeeds.
	assert.NoError(e.Terminate(ctx, fk, "boom"))
	assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+workflow.StatusTerminated+"'", flowID))
	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultSite_ResumeCommit pins that a Resume whose transaction fails once rolls back atomically (the flow
// stays interrupted, its steps untouched) and a retry then resumes to completion.
func TestFaultSite_ResumeCommit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	registerGate(mux, "ftbresume")

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	reader := withManualReader(e)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk := createInterruptedFlow(t, e, "ftbresume/g")
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	// The Resume transaction fails once: Resume errors and the flow is still interrupted (leaf still parked).
	e.diags.Seams().Inject(diagnostics.FaultResumeCommit)
	err = e.Resume(ctx, fk, nil)
	assert.Error(err)
	assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'", flowID))
	assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'", flowID))

	// Retry (fault consumed): Resume succeeds and the flow completes.
	assert.NoError(e.Resume(ctx, fk, nil))
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)
	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultSite_ForkCommit pins Fork's "crash mid-clone rolls back, origin never mutated" claim: a Fork whose
// clone transaction fails once mutates neither the origin (byte-identical final_state) nor the flow table
// (zero partial clone rows), and a retry then forks cleanly.
func TestFaultSite_ForkCommit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	e, reader, _ := newFaultBatteryEngine(t, "ftbfork", nil)
	defer e.Shutdown(ctx)

	// A completed origin to fork from.
	originKey, out := batteryRun(t, e, "ftbfork/g")
	assert.Equal(workflow.StatusCompleted, out.Status)
	shard, _, flowID, _, err := keys.ParseFlowKey(originKey)
	assert.NoError(err)
	originFS := readFinalState(t, e, originKey)
	flowsBefore := shardFlowCount(t, e, shard)

	// The fork step: the completed entry step A.
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var aStepID, aStepHour int
	var aStepToken string
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id, flow_created_hour, step_token FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStepID, &aStepHour, &aStepToken))
	forkStepKey := keys.NewStepKey(shard, aStepHour, flowID, aStepID, aStepToken)

	// The clone transaction fails once: Fork errors, the origin is byte-identical, and no clone rows exist.
	e.diags.Seams().Inject(diagnostics.FaultForkCommit)
	_, err = e.Fork(ctx, forkStepKey, nil)
	assert.Error(err)
	assert.Equal(originFS, readFinalState(t, e, originKey), "origin final_state mutated by a rolled-back Fork")
	assert.Equal(flowsBefore, shardFlowCount(t, e, shard), "a rolled-back Fork left partial clone rows")

	// Retry (fault consumed): Fork succeeds and the new flow runs to completion.
	forkKey, err := e.Fork(ctx, forkStepKey, nil)
	assert.NoError(err)
	assert.Equal(flowsBefore+1, shardFlowCount(t, e, shard))
	forkOut, err := e.Await(ctx, forkKey)
	assert.NoError(err)
	if assert.NotNil(forkOut) {
		assert.Equal(workflow.StatusCompleted, forkOut.Status)
	}
	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultSite_DeliverFailureErr pins that when a subgraph child's failure-delivery to its parked caller is
// lost (the child terminalizes failed, but the caller is never re-armed), the parked-step wedge sweep
// backstops it: recoverWedgedSubgraphParks re-drives the delivery and the parent still terminalizes failed.
func TestFaultSite_DeliverFailureErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "ftbdeliver/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("ftbdeliver/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("X", "ftbdeliver/x")
	child.AddTransition("X", workflow.End)
	mux.HandleGraph("ftbdeliver/child", child)
	mux.HandleTask("ftbdeliver/call", subgraphTask("ftbdeliver/child"))
	// The child task fails with no onError, so the child flow fails and delivers to the parent.
	mux.HandleTask("ftbdeliver/x", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("child boom")
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	reader := withManualReader(e)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// The child fails, but its re-dispatch of the parked caller is lost: the caller wedges running+parkedSubgraph
	// with a terminal (failed) child.
	e.diags.Seams().Inject(diagnostics.FaultDeliverFailureErr)
	fk, err := e.Create(ctx, "ftbdeliver/parent", nil, nil)
	assert.NoError(err)
	shard, _, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	// Wait for the wedge to form: the child flow gone terminal (failed), the parent still running.
	got := enginetest.WaitUntil(t, 5*time.Second, func() bool {
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id<>0 AND status='"+workflow.StatusFailed+"'") == 1
	})
	assert.True(got, "expected the child to fail and the caller to wedge")

	// The wedge sweep re-drives the lost delivery (minAge 0 bypasses the steady-state age guard); the parent
	// then terminalizes failed.
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusFailed, 10*time.Second)

	// Unlike the other faults in this file, this one drives a step into a genuinely-wedged state on purpose, so the
	// always-on alarm SHOULD fire exactly once - proving the backstop engaged. The structural invariants must
	// still be clean afterward.
	enginetest.AssertInvariants(t, e)
	var rm metricdata.ResourceMetrics
	if assert.NoError(reader.Collect(ctx, &rm)) {
		unwedged, ok := sumCounter(rm, "dwarf_steps_unwedged", "", "")
		assert.True(ok, "expected dwarf_steps_unwedged to have fired")
		assert.Equal(int64(1), unwedged, "the wedge sweep should have unwedged exactly one caller")
	}
}

// TestFaultSite_DeliverFailureLost_DeepSubgraph extends the single-level TestFaultSite_DeliverFailureErr
// to a depth-2 subgraph: Root(Call1) -> Mid(Call2) -> Leaf(X, fails). It proves the wedge sweep composes across
// nesting - a lost failure-delivery at each level wedges that level's caller, and recoverWedgedSubgraphParks
// re-drives deliverSubgraphError UP two levels to the root:
//
//	Leaf X fails -> deliver to Call2 LOST      => Call2 wedges (running+parkedSubgraph, Leaf terminal)
//	sweep recovers Call2 -> Mid re-fails
//	Mid fails    -> deliver to Call1 LOST      => Call1 wedges
//	sweep recovers Call1 -> Root fails         => tree terminalizes
//
// The fault is scoped by the parked caller's task name, so each level's FIRST delivery (the natural one) is
// dropped while its second (the sweep-driven re-delivery) goes through - letting BOTH levels genuinely wedge,
// not just the deepest. Asserts the root terminalizes, dwarf_steps_unwedged{park_type=subgraph} fired once per
// level (==2), and the tree ends structurally clean.
func TestFaultSite_DeliverFailureLost_DeepSubgraph(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	root := workflow.NewGraph("Root")
	root.SetURL("Call1", "ftbdeep/call1")
	root.AddTransition("Call1", workflow.End)
	mux.HandleGraph("ftbdeep/root", root)
	mid := workflow.NewGraph("Mid")
	mid.SetURL("Call2", "ftbdeep/call2")
	mid.AddTransition("Call2", workflow.End)
	mux.HandleGraph("ftbdeep/mid", mid)
	leaf := workflow.NewGraph("Leaf")
	leaf.SetURL("X", "ftbdeep/x")
	leaf.AddTransition("X", workflow.End)
	mux.HandleGraph("ftbdeep/leaf", leaf)
	mux.HandleTask("ftbdeep/call1", subgraphTask("ftbdeep/mid"))
	mux.HandleTask("ftbdeep/call2", subgraphTask("ftbdeep/leaf"))
	// The leaf task fails with no onError, so the leaf child flow fails and delivers up the chain.
	mux.HandleTask("ftbdeep/x", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("leaf boom")
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetWorkers(4))
	e.SetHost(mux)
	reader := withManualReader(e)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// Drop each level's FIRST failure-delivery independently (scoped by the parked caller's task name). Each
	// caller's sweep-driven re-delivery (the second consult of its scope) succeeds, so the sweep can recover it.
	e.diags.Seams().Inject(diagnostics.Join(diagnostics.FaultDeliverFailureErr, "Call2"))
	e.diags.Seams().Inject(diagnostics.Join(diagnostics.FaultDeliverFailureErr, "Call1"))
	fk, err := e.Create(ctx, "ftbdeep/root", nil, nil)
	assert.NoError(err)
	shard, _, rootFlowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	// First wedge forms during normal execution: the leaf child fails, but its delivery to Call2 is lost, so
	// Call2 (in the mid child) is stuck running+parkedSubgraph with a terminal child.
	got := enginetest.WaitUntil(t, 5*time.Second, func() bool {
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE workflow_url='ftbdeep/leaf' AND status='"+workflow.StatusFailed+"'") == 1
	})
	assert.True(got, "expected the leaf child to fail and Call2 to wedge")

	// Drive the sweep (minAge 0 bypasses the steady-state age guard). Pass 1 recovers Call2, which re-fails the
	// mid child, whose delivery to Call1 is then lost (Call1 wedges); a later pass recovers Call1, which re-fails
	// the root. Each pass re-queries the wedged set, so the newly-formed Call1 wedge needs its own pass - hence
	// the driven loop rather than a single sweep call.
	got = enginetest.WaitUntil(t, 15*time.Second, func() bool {
		e.recoverWedgedSubgraphParks(ctx, db, shard, 0)
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+workflow.StatusFailed+"'", rootFlowID) == 1
	})
	assert.True(got, "expected the sweep to propagate the failure up both levels to the root")
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusFailed, 10*time.Second)

	// The two wedges (Call2, Call1) were each unwedged exactly once, and the tree ends structurally clean.
	enginetest.AssertInvariants(t, e)
	var rm metricdata.ResourceMetrics
	if assert.NoError(reader.Collect(ctx, &rm)) {
		unwedged, ok := sumCounter(rm, "dwarf_steps_unwedged", "park_type", "subgraph")
		assert.True(ok, "expected dwarf_steps_unwedged{park_type=subgraph} to have fired")
		assert.Equal(int64(2), unwedged, "the sweep should have unwedged a caller at each of the two levels")
	}
}

// TestFaultSite_ReapSelectErr pins the janitor's resilience to a transient due-root SELECT failure (sibling to
// diagnostics.FaultReapMidTree, which covers the delete half): the pass logs and bails without deleting, and the NEXT
// pass reaps cleanly.
func TestFaultSite_ReapSelectErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "ftbreapsel/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("ftbreapsel/g", g)
	mux.HandleTask("ftbreapsel/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, time.Hour) // due immediately; the test drives reaps
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk, err := e.Create(ctx, "ftbreapsel/g", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, 5*time.Second)
	shard, _, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	time.Sleep(5 * time.Millisecond) // let the 1ms window elapse

	// The due-root SELECT errors: the pass bails without deleting, so the flow is still present.
	e.diags.Seams().InjectN(diagnostics.FaultReapSelectErr, 1000)
	e.reapDueFlows(ctx)
	assert.Equal(1, shardFlowCount(t, e, shard)) // not deleted - the scan bailed

	// The next pass after the fault clears reaps cleanly.
	e.diags.Seams().Withdraw(diagnostics.FaultReapSelectErr)
	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shard))
}

// TestFaultSite_RetryRewindErr pins that a flow.Retry rewind that fails once is retried in place by persist,
// like any other write of a step's outcome, rather than failing the flow - and the task is not run again for it.
func TestFaultSite_RetryRewindErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	g := workflow.NewGraph("RetryRewind")
	g.SetURL("Work", "ftbretry/work")
	g.AddTransition("Work", workflow.End)
	mux.HandleGraph("ftbretry/g", g)
	var calls atomic.Int32
	mux.HandleTask("ftbretry/work", func(ctx context.Context, f *workflow.Flow) error {
		if calls.Add(1) == 1 {
			if yield, err := f.Retry(0, 1, 0, 0); yield || err != nil {
				return err
			}
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(mux)
	e.persistBackoff = []time.Duration{time.Millisecond}
	reader := withManualReader(e)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	e.diags.Seams().Inject(diagnostics.Join(diagnostics.FaultRetryRewindErr, "Work"))
	_, out := batteryRun(t, e, "ftbretry/g")
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(int32(2), calls.Load(), "one attempt that retried, one that completed")
	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultSite_InterruptWriteRetried pins that an interrupt whose write fails once is retried in place by
// persist: the flow rests interrupted promptly, with the task run once, rather than the step sitting `running`
// until its lease lapses and the task runs again.
func TestFaultSite_InterruptWriteRetried(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	g := workflow.NewGraph("Gate")
	g.SetURL("Gate", "ftbintr/gate")
	g.AddTransition("Gate", workflow.End)
	mux.HandleGraph("ftbintr/g", g)
	var calls atomic.Int32
	mux.HandleTask("ftbintr/gate", func(ctx context.Context, f *workflow.Flow) error {
		calls.Add(1)
		_, err := f.Interrupt(nil, nil)
		return err
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(mux)
	e.persistBackoff = []time.Duration{time.Millisecond}
	assert.Must(assert.NoError(e.Startup(t.Context())))

	e.diags.Seams().Inject(diagnostics.FaultInterruptChainWrite)
	defer e.diags.Seams().Withdraw(diagnostics.FaultInterruptChainWrite)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	fk, err := e.Create(ctx, "ftbintr/g", nil, nil)
	assert.NoError(err)
	out, err := e.Await(ctx, fk)
	if assert.NoError(err) && assert.NotNil(out) {
		assert.Equal(workflow.StatusInterrupted, out.Status)
	}
	assert.Equal(int32(1), calls.Load())
	enginetest.AssertInvariants(t, e)
}

// TestFaultSite_InitRuntimeFailureLeavesTheFleet pins what a Startup that fails after joining the fleet undoes:
// the replica's registry row, which peers would otherwise count for the fresh window, and the open shards. The
// engine reports itself stopped, and a later Startup succeeds.
func TestFaultSite_InitRuntimeFailureLeavesTheFleet(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(noopHost{}))
	e.diags.Seams().Inject(diagnostics.FaultInitRuntime)
	assert.Error(e.Startup(ctx))
	assert.False(e.started.Load())
	assert.Equal(0, e.db.NumShards(), "the shards were left open")

	// A peer sharing the test database sees the registry as the failed replica left it.
	peer := NewEngineUnderTest(t.Name())
	defer peer.Shutdown(ctx)
	assert.NoError(peer.SetHost(noopHost{}))
	assert.Must(assert.NoError(peer.Startup(ctx)))
	assert.Equal(0, enginetest.CountRows(t, peer, 1, "SELECT COUNT(*) FROM dwarf_peers WHERE engine_id=?", e.engineID),
		"the failed replica is still registered")

	assert.Must(assert.NoError(e.Startup(ctx)))
	assert.True(e.started.Load())
}

// TestFaultSite_FailFlowFinalStateErr pins the fuse in failing a flow: when its final state cannot be computed,
// the flow still terminalizes as failed - with an empty final state - on both paths that fail a flow, a failing
// trunk step and a cohort that resolves with a failed branch.
func TestFaultSite_FailFlowFinalStateErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	trunk := workflow.NewGraph("Trunk")
	trunk.SetURL("Boom", "ftbfinal/boom")
	trunk.AddTransition("Boom", workflow.End)
	mux.HandleGraph("ftbfinal/trunk", trunk)
	fan := workflow.NewGraph("Fan")
	fan.SetURL("Split", "ftbfinal/split")
	fan.SetURL("Work", "ftbfinal/work")
	fan.SetURL("Join", "ftbfinal/join")
	fan.SetFanIn("Join")
	fan.AddTransitionForEach("Split", "Work", "items", "item")
	fan.AddTransitionChain("Work", "Join", workflow.End)
	mux.HandleGraph("ftbfinal/fan", fan)
	mux.HandleTask("ftbfinal/boom", func(ctx context.Context, f *workflow.Flow) error { return errors.New("boom") })
	mux.HandleTask("ftbfinal/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	// goodGate, when set, holds the "good" branch until it is closed, so the "bad" branch fails first and the
	// good one's arrival is what resolves the cohort - the cohort-fail branch of processStep, not failStep.
	var goodGate atomic.Pointer[chan struct{}]
	mux.HandleTask("ftbfinal/work", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("item") == "bad" {
			return errors.New("bad item")
		}
		if gate := goodGate.Load(); gate != nil {
			select {
			case <-*gate:
			case <-time.After(10 * time.Second):
			}
		}
		return nil
	})
	mux.HandleTask("ftbfinal/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(mux)
	reader := withManualReader(e)
	if !assert.NoError(e.Startup(t.Context())) {
		return
	}

	for _, tc := range []struct{ name, url string }{{"trunk", "ftbfinal/trunk"}, {"cohort", "ftbfinal/fan"}} {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)
			e.diags.Seams().Inject(diagnostics.FaultFailFlowFinalState)
			defer e.diags.Seams().Withdraw(diagnostics.FaultFailFlowFinalState)
			if tc.name == "cohort" {
				// Release "good" once two claimed dispatches have returned: Split's, then the failed "bad" branch's.
				gate := make(chan struct{})
				goodGate.Store(&gate)
				defer goodGate.Store(nil)
				base := e.diags.Seams().Visits(diagnostics.CheckpointClaimedDispatchDone)
				go func() {
					for {
						reached := e.diags.Seams().Waiter(diagnostics.CheckpointClaimedDispatchDone)
						if e.diags.Seams().Visits(diagnostics.CheckpointClaimedDispatchDone) >= base+2 {
							close(gate)
							return
						}
						<-reached
					}
				}()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second*enginetest.TimeoutScale())
			defer cancel()
			_, out, err := e.Run(ctx, tc.url, map[string]any{"carried": 1, "items": []string{"good", "bad"}}, nil)
			if !assert.NoError(err) || !assert.NotNil(out) {
				return
			}
			assert.Equal(workflow.StatusFailed, out.Status)
			assert.False(out.State.Has("carried"), "the final state was computed despite the fault")
			assertFaultRecoveryClean(t, e, reader)
		})
	}
}
