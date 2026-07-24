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
// failure-delivery backstopped by the wedge sweep, and the reaper's SELECT-error resilience.
package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// orphanShapeCount counts flows in the "orphan" shape on a shard: running with zero non-terminal steps (all
// steps terminal, no successor) - exactly what detectOrphanedFlows alarms on and assertInvariants forbids.
func orphanShapeCount(t *testing.T, e *Engine, shard int) int {
	return enginetest.CountRows(t, e, shard,
		"SELECT COUNT(*) FROM dwarf_flows f WHERE f.status='"+workflow.StatusRunning+"'"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status IN ('"+
			workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"'))")
}

// TestFaultSite_RecoveryResetErr pins the residual orphan hole the design explicitly calls out: when the
// post-completion transaction fails AND the recovery defer's own reset UPDATE also fails, the step is stuck
// `completed` and the flow strands `running` with every step terminal - the shape only detectOrphanedFlows
// surfaces (log-only). This is the one fault outcome that is NOT self-healing, so it asserts the orphan
// exists (by SQL) rather than a clean world.
func TestFaultSite_RecoveryResetErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "ftbreset/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("ftbreset/g", g)
	var runs int
	proxy.HandleTask("ftbreset/a", func(ctx context.Context, f *workflow.Flow) error { runs++; return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// A completes (marked `completed`), the flow-completion tx fails (FaultCompleteFlowCommit), then the
	// recovery defer's reset also fails (FaultRecoveryResetErr) - so A never returns to `pending` and the flow
	// never completes. Both fire once.
	e.seams.Inject(FaultCompleteFlowCommit)
	e.seams.Inject(FaultRecoveryResetErr)
	fk, err := e.Create(ctx, "ftbreset/g", nil, nil)
	assert.NoError(err)
	shard, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	// Wait for the strand to settle: A `completed`, the flow still `running`.
	got := enginetest.WaitUntil(t, 5*time.Second, func() bool {
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusCompleted+"'", flowID) == 1 &&
			enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+workflow.StatusRunning+"'", flowID) == 1
	})
	assert.True(got, "expected the flow to strand running with A completed")
	assert.Equal(1, runs) // the reset failed, so no re-dispatch

	// The residual orphan exists - this is the last-resort alarm's input. detectOrphanedFlows is log-only with
	// a 5m age guard, so it is asserted by shape, not by log.
	assert.Equal(1, orphanShapeCount(t, e, shard))
	if db, derr := e.db.Shard(shard); derr == nil {
		e.detectOrphanedFlows(ctx, db, shard) // exercises the detector query; silent under the age guard
	}
}

// TestFaultSite_SubgraphSpawnErr pins that a subgraph spawn failing AFTER the caller step parked
// (execution.go's park-then-create ordering) fails the caller cleanly rather than leaving it parked forever,
// and leaves no orphan child flow.
func TestFaultSite_SubgraphSpawnErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "ftbspawn/call")
	parent.AddTransition("Call", workflow.END)
	proxy.HandleGraph("ftbspawn/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "ftbspawn/x")
	child.AddTransition("X", workflow.END)
	proxy.HandleGraph("ftbspawn/child", child)
	proxy.HandleTask("ftbspawn/call", subgraphTask("ftbspawn/child"))
	proxy.HandleTask("ftbspawn/x", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	reader := withManualReader(e)
	assert.NoError(e.Startup(t.Context()))

	// The caller parks, then createSubgraphFlow errors (no child inserted): failAndReturn must fail the caller
	// step (un-parked) and the flow, not strand it parked.
	e.seams.Inject(FaultSubgraphSpawnErr, "ftbspawn/child")
	fk, out := batteryRun(t, e, "ftbspawn/parent")
	assert.Equal(workflow.StatusFailed, out.Status)

	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	// No child flow was created (the spawn failed before insert), so no orphan lingers.
	assert.Equal(0, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id<>0"))
	assertFaultRecoveryClean(t, e, reader) // caller failed + un-parked; invariants clean
}

// createInterruptedFlow creates a flow whose entry task interrupts, and blocks until it rests `interrupted`.
func createInterruptedFlow(t *testing.T, e *Engine, url string) string {
	t.Helper()
	assert := testarossa.For(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

// registerGate registers a one-node graph whose entry task interrupts on first dispatch and completes on
// resume, so a test has a stable `interrupted` flow to Cancel/Resume.
func registerGate(proxy *TestProxy, prefix string) {
	g := workflow.NewGraph("Gate")
	g.SetEndpoint("Gate", prefix+"/gate")
	g.AddTransition("Gate", workflow.END)
	proxy.HandleGraph(prefix+"/g", g)
	proxy.HandleTask(prefix+"/gate", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
}

// TestFaultSite_CancelCommit pins that a Cancel whose transaction fails once rolls back atomically (the tree
// is untouched, flow still interrupted) and a retry then cancels cleanly.
func TestFaultSite_CancelCommit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	registerGate(proxy, "ftbcancel")

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	reader := withManualReader(e)
	assert.NoError(e.Startup(t.Context()))

	fk := createInterruptedFlow(t, e, "ftbcancel/g")
	shard, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	// The Cancel transaction fails once: Cancel errors and nothing changed (still interrupted).
	e.seams.Inject(FaultCancelCommit)
	err = e.Cancel(ctx, fk, "boom")
	assert.Error(err)
	assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'", flowID))

	// Retry (fault consumed): Cancel succeeds.
	assert.NoError(e.Cancel(ctx, fk, "boom"))
	assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+workflow.StatusCancelled+"'", flowID))
	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultSite_ResumeCommit pins that a Resume whose transaction fails once rolls back atomically (the flow
// stays interrupted, its steps untouched) and a retry then resumes to completion.
func TestFaultSite_ResumeCommit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	registerGate(proxy, "ftbresume")

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	reader := withManualReader(e)
	assert.NoError(e.Startup(t.Context()))

	fk := createInterruptedFlow(t, e, "ftbresume/g")
	shard, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	// The Resume transaction fails once: Resume errors and the flow is still interrupted (leaf still parked).
	e.seams.Inject(FaultResumeCommit)
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

	// A completed origin to fork from.
	originKey, out := batteryRun(t, e, "ftbfork/g")
	assert.Equal(workflow.StatusCompleted, out.Status)
	shard, flowID, _, err := keys.ParseFlowKey(originKey)
	assert.NoError(err)
	originFS := readFinalState(t, e, originKey)
	flowsBefore := shardFlowCount(t, e, shard)

	// The fork step: the completed entry step A.
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	var aStepID int
	var aStepToken string
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id, step_token FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStepID, &aStepToken))
	forkStepKey := fmt.Sprintf("%d-%d-%s", shard, aStepID, aStepToken)

	// The clone transaction fails once: Fork errors, the origin is byte-identical, and no clone rows exist.
	e.seams.Inject(FaultForkCommit)
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

// TestFaultSite_SignalPeersPanic pins host-call panic isolation for SignalPeers: when the host's SignalPeers
// panics on the terminal statusChange broadcast, the boundary CatchPanic swallows it and flow completion /
// the local Await are unaffected (distinct from dropSignalStop, which returns cleanly).
func TestFaultSite_SignalPeersPanic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "ftbpanic/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("ftbpanic/g", g)
	proxy.HandleTask("ftbpanic/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	reader := withManualReader(e)
	assert.NoError(e.Startup(t.Context()))

	// Every statusChange broadcast panics inside the CatchPanic boundary; the local waiter wake (separate from
	// the peer broadcast) still delivers, so Await returns the completed outcome.
	e.seams.InjectN(1000, FaultSignalPeersPanic, string(signalOpStatusChange))
	_, out := batteryRun(t, e, "ftbpanic/g")
	assert.Equal(workflow.StatusCompleted, out.Status)
	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultSite_DeliverFailureErr pins that when a subgraph child's failure-delivery to its parked caller is
// lost (the child terminalizes failed, but the caller is never re-armed), the parked-step wedge sweep
// backstops it: recoverWedgedSubgraphParks re-drives the delivery and the parent still terminalizes failed.
func TestFaultSite_DeliverFailureErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "ftbdeliver/call")
	parent.AddTransition("Call", workflow.END)
	proxy.HandleGraph("ftbdeliver/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "ftbdeliver/x")
	child.AddTransition("X", workflow.END)
	proxy.HandleGraph("ftbdeliver/child", child)
	proxy.HandleTask("ftbdeliver/call", subgraphTask("ftbdeliver/child"))
	// The child task fails with no onError, so the child flow fails and delivers to the parent.
	proxy.HandleTask("ftbdeliver/x", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("child boom")
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	reader := withManualReader(e)
	assert.NoError(e.Startup(t.Context()))

	// The child fails, but its re-dispatch of the parked caller is lost: the caller wedges running+parkedSubgraph
	// with a terminal (failed) child.
	e.seams.Inject(FaultDeliverFailureErr)
	fk, err := e.Create(ctx, "ftbdeliver/parent", nil, nil)
	assert.NoError(err)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)

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
	proxy := NewTestProxy()
	root := workflow.NewGraph("Root")
	root.SetEndpoint("Call1", "ftbdeep/call1")
	root.AddTransition("Call1", workflow.END)
	proxy.HandleGraph("ftbdeep/root", root)
	mid := workflow.NewGraph("Mid")
	mid.SetEndpoint("Call2", "ftbdeep/call2")
	mid.AddTransition("Call2", workflow.END)
	proxy.HandleGraph("ftbdeep/mid", mid)
	leaf := workflow.NewGraph("Leaf")
	leaf.SetEndpoint("X", "ftbdeep/x")
	leaf.AddTransition("X", workflow.END)
	proxy.HandleGraph("ftbdeep/leaf", leaf)
	proxy.HandleTask("ftbdeep/call1", subgraphTask("ftbdeep/mid"))
	proxy.HandleTask("ftbdeep/call2", subgraphTask("ftbdeep/leaf"))
	// The leaf task fails with no onError, so the leaf child flow fails and delivers up the chain.
	proxy.HandleTask("ftbdeep/x", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("leaf boom")
	})

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetWorkers(4))
	e.SetHost(proxy)
	reader := withManualReader(e)
	assert.NoError(e.Startup(t.Context()))

	// Drop each level's FIRST failure-delivery independently (scoped by the parked caller's task name). Each
	// caller's sweep-driven re-delivery (the second consult of its scope) succeeds, so the sweep can recover it.
	e.seams.Inject(FaultDeliverFailureErr, "Call2")
	e.seams.Inject(FaultDeliverFailureErr, "Call1")
	fk, err := e.Create(ctx, "ftbdeep/root", nil, nil)
	assert.NoError(err)
	shard, rootFlowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)

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

// TestFaultSite_ReapSelectErr pins the reaper's resilience to a transient due-root SELECT failure (sibling to
// FaultReapMidTree, which covers the delete half): the pass logs and bails without deleting, and the NEXT
// pass reaps cleanly.
func TestFaultSite_ReapSelectErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "ftbreapsel/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("ftbreapsel/g", g)
	proxy.HandleTask("ftbreapsel/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	shortenDeletion(e, time.Millisecond, time.Hour) // due immediately; the test drives reaps
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "ftbreapsel/g", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, 5*time.Second)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	time.Sleep(5 * time.Millisecond) // let the 1ms window elapse

	// The due-root SELECT errors: the pass bails without deleting, so the flow is still present.
	e.seams.Inject(FaultReapSelectErr)
	e.reapDueFlows(ctx)
	assert.Equal(1, shardFlowCount(t, e, shard)) // not deleted - the scan bailed

	// The next pass (fault consumed) reaps cleanly.
	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shard))
}
