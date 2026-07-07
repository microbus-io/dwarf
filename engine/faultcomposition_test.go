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

// The per-fault tests in faults_test.go use a linear one- or two-node graph, but the interesting
// recovery bugs live where recovery meets fan-out, subgraphs, and repetition. These tests drive the *same*
// faults through those compositions and assert recovery is not just eventually-terminal but leaves a clean
// world (assertFaultRecoveryClean: invariants + the wedge alarm silent), with cohort/subgraph/repetition
// correctness pinned exactly.
package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// withManualReader wires a ManualReader meter provider onto e (so assertFaultRecoveryClean can read
// dwarf_steps_unwedged) and returns it. Must be called after SetHost and before RunInTest.
func withManualReader(e *Engine) sdkmetric.Reader {
	reader := sdkmetric.NewManualReader()
	e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	return reader
}

// subgraphTask returns a task body that calls Subgraph(childURL) and returns once the child resolves - the
// building block for nesting parent flows over child flows.
func subgraphTask(childURL string) func(context.Context, *workflow.Flow) error {
	return func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph(childURL, nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	}
}

// TestFaultComposition_FanOutBranch pins that a transition-commit fault scoped to ONE cohort branch of a
// fan-out (A -> {X, Y} -> J) recovers cleanly: the faulted branch is reset and re-dispatched by the recovery
// defer, the cohort still converges exactly once (no double-count), and the fan-in sees both branches. This
// is the fault-driven analog of the leasefence cohort test, deterministic via the seam (no zombie timing).
func TestFaultComposition_FanOutBranch(t *testing.T) {
	assert := testarossa.For(t)
	var aCalls, xCalls, yCalls, jCalls atomic.Int64

	proxy := NewTestProxy()
	g := workflow.NewGraph("FC-Fan")
	g.SetEndpoint("A", "fcfan/a")
	g.SetEndpoint("X", "fcfan/x")
	g.SetEndpoint("Y", "fcfan/y")
	g.SetEndpoint("J", "fcfan/j")
	g.SetFanIn("J")
	g.AddTransition("A", "X")
	g.AddTransition("A", "Y")
	g.AddTransition("X", "J")
	g.AddTransition("Y", "J")
	g.AddTransition("J", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("fcfan/g", g)
	proxy.HandleTask("fcfan/a", func(ctx context.Context, f *workflow.Flow) error { aCalls.Add(1); return nil })
	proxy.HandleTask("fcfan/x", func(ctx context.Context, f *workflow.Flow) error {
		xCalls.Add(1)
		f.SetString("x", "ran") // idempotent constant - a re-dispatch yields byte-identical state
		return nil
	})
	proxy.HandleTask("fcfan/y", func(ctx context.Context, f *workflow.Flow) error {
		yCalls.Add(1)
		f.SetString("y", "ran")
		return nil
	})
	proxy.HandleTask("fcfan/j", func(ctx context.Context, f *workflow.Flow) error { jCalls.Add(1); return nil })

	e := NewEngine()
	assert.NoError(e.SetWorkers(4))
	e.SetHost(proxy)
	reader := withManualReader(e)
	e.RunInTest(t)

	// X's transition transaction fails once after X was marked completed: the recovery defer resets X and
	// re-dispatches it, so X runs twice, but the cohort arrival is bumped only by the successful commit.
	e.seams.Inject(faultTransitionCommit, "X")
	fk, out := batteryRun(t, e, "fcfan/g")
	assert.Equal(workflow.StatusCompleted, out.Status)

	// The fan-in saw both branches (merged state carries X's and Y's writes).
	assert.Equal("ran", out.State["x"])
	assert.Equal("ran", out.State["y"])

	assert.Equal(int64(1), aCalls.Load())
	assert.Equal(int64(2), xCalls.Load()) // faulted branch re-dispatched
	assert.Equal(int64(1), yCalls.Load())
	assert.Equal(int64(1), jCalls.Load()) // fan-in fired exactly once - no double-count from the recovery re-run

	_, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	var arrivals, size, jSteps int
	assert.NoError(db.QueryRowContext(context.Background(), "SELECT cohort_arrivals, cohort_size FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&arrivals, &size))
	assert.Equal(2, size)
	assert.Equal(2, arrivals) // both branches arrived exactly once; the recovery re-run did not add a third
	assert.NoError(db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='J'", flowID).Scan(&jSteps))
	assert.Equal(1, jSteps)

	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultComposition_SubgraphChild pins that a transition-commit fault *inside a subgraph child* recovers
// without stranding the parent: the child's failed transition is reset and re-dispatched, the child still
// completes, and completeSurgraphFlow revives the parent caller so the whole tree terminalizes.
func TestFaultComposition_SubgraphChild(t *testing.T) {
	assert := testarossa.For(t)
	var xCalls, yCalls atomic.Int64

	proxy := NewTestProxy()
	parent := workflow.NewGraph("FC-Parent")
	parent.SetEndpoint("Call", "fcsub/call")
	parent.AddTransition("Call", workflow.END)
	proxy.HandleGraph("fcsub/parent", parent)
	child := workflow.NewGraph("FC-Child")
	child.SetEndpoint("X", "fcsub/x")
	child.SetEndpoint("Y", "fcsub/y")
	child.AddTransition("X", "Y")
	child.AddTransition("Y", workflow.END)
	proxy.HandleGraph("fcsub/child", child)
	proxy.HandleTask("fcsub/call", subgraphTask("fcsub/child"))
	proxy.HandleTask("fcsub/x", func(ctx context.Context, f *workflow.Flow) error { xCalls.Add(1); return nil })
	proxy.HandleTask("fcsub/y", func(ctx context.Context, f *workflow.Flow) error { yCalls.Add(1); return nil })

	e := NewEngine()
	e.SetHost(proxy)
	reader := withManualReader(e)
	e.RunInTest(t)

	// X's transition (X->Y) inside the child fails once after X was marked completed: the recovery defer resets
	// X and re-dispatches, so the child proceeds to Y->END and completes, then the parent revives.
	e.seams.Inject(faultTransitionCommit, "X")
	_, out := batteryRun(t, e, "fcsub/parent")
	assert.Equal(workflow.StatusCompleted, out.Status)

	assert.Equal(int64(2), xCalls.Load()) // child branch re-dispatched by the recovery defer
	assert.Equal(int64(1), yCalls.Load())

	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultComposition_RepeatedFault pins that a fault firing repeatedly on the same step still converges: a
// transition-commit fault armed 3x compounds three recovery-defer re-dispatches, so the task runs N+1 times,
// yet the flow reaches a clean terminal state (bounded, no crash-loop) with a final_state byte-identical to a
// no-fault baseline.
func TestFaultComposition_RepeatedFault(t *testing.T) {
	assert := testarossa.For(t)
	e, reader, calls := newFaultBatteryEngine(t, "fcrep", nil)

	// Baseline (no fault): reference final_state and a clean world.
	baseKey, baseOut := batteryRun(t, e, "fcrep/g")
	assert.Equal(workflow.StatusCompleted, baseOut.Status)
	baseFS := readFinalState(t, e, baseKey)
	assert.NotEqual("", baseFS)
	assert.Equal(1, *calls["a"])
	assert.Equal(1, *calls["b"])
	assertFaultRecoveryClean(t, e, reader)

	// A's transition fails 3 times in a row; each failure drives one recovery-defer reset + re-dispatch. A runs
	// N+1 = 4 times; B once; the flow still completes with an identical final_state.
	*calls["a"], *calls["b"] = 0, 0
	e.seams.InjectN(3, faultTransitionCommit, "A")
	fk, out := batteryRun(t, e, "fcrep/g")
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(baseFS, readFinalState(t, e, fk), "final_state diverged from the no-fault baseline")
	assert.Equal(4, *calls["a"]) // 3 faulted attempts + 1 clean
	assert.Equal(1, *calls["b"])
	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultComposition_CompoundWakeLoss pins that when BOTH the dispatch wake (doorbell) and the terminal wake
// (signalStop) are lost, the two independent backstops still cover the flow: pollPendingSteps dispatches the
// stranded pending step, and Await's periodic re-snapshot returns the DB-committed completion.
func TestFaultComposition_CompoundWakeLoss(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("FC-Wake")
	g.SetEndpoint("A", "fcwake/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("fcwake/g", g)
	proxy.HandleTask("fcwake/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.awaitPollInterval = 20 * time.Millisecond // the re-snapshot backstop must fire fast for the test
	reader := withManualReader(e)
	e.RunInTest(t)

	// Drop the create-time doorbell AND the terminal signalStop: neither wake reaches its consumer.
	e.seams.Inject(faultDropDoorbell)
	e.seams.Inject(faultDropSignalStop)
	fk, err := e.Create(ctx, "fcwake/g", nil, nil)
	assert.NoError(err)

	// Await is registered first, so its return can only come from the periodic re-snapshot (the terminal signal
	// is dropped), never the fast wake.
	outCh := make(chan *workflow.FlowOutcome, 1)
	go func() {
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		out, _ := e.Await(awaitCtx, fk)
		outCh <- out
	}()

	// The doorbell was dropped, so the entry step sits pending until this backstop rings the local doorbell.
	e.pollPendingSteps(ctx)

	select {
	case out := <-outCh:
		if assert.NotNil(out) {
			assert.Equal(workflow.StatusCompleted, out.Status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Await did not return despite both backstops")
	}

	assertFaultRecoveryClean(t, e, reader)
}

// TestFaultComposition_DeepSubgraphReviveLoss pins that a revive lost three subgraph levels down is backstopped
// by the wedge sweep, which re-drives the release so it bubbles all the way up to the root. root -> l1 -> l2 ->
// l3(leaf): l3's completion revive is dropped, wedging l2's caller; one sweep pass re-drives it and the normal
// completion cascade carries l2 -> l1 -> root to terminal.
func TestFaultComposition_DeepSubgraphReviveLoss(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()

	root := workflow.NewGraph("FC-Root")
	root.SetEndpoint("Call", "fcdeep/root-call")
	root.AddTransition("Call", workflow.END)
	proxy.HandleGraph("fcdeep/root", root)
	for _, lvl := range []struct{ url, task, child string }{
		{"fcdeep/l1", "fcdeep/l1-call", "fcdeep/l2"},
		{"fcdeep/l2", "fcdeep/l2-call", "fcdeep/l3"},
	} {
		g := workflow.NewGraph("FC-" + lvl.url)
		g.SetEndpoint("Call", lvl.task)
		g.AddTransition("Call", workflow.END)
		proxy.HandleGraph(lvl.url, g)
		proxy.HandleTask(lvl.task, subgraphTask(lvl.child))
	}
	// l3 is the leaf child - a plain one-node flow, no further nesting.
	l3 := workflow.NewGraph("FC-L3")
	l3.SetEndpoint("Leaf", "fcdeep/l3-leaf")
	l3.AddTransition("Leaf", workflow.END)
	proxy.HandleGraph("fcdeep/l3", l3)
	proxy.HandleTask("fcdeep/l3-leaf", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("fcdeep/root-call", subgraphTask("fcdeep/l1"))

	e := NewEngine()
	e.SetHost(proxy)
	reader := withManualReader(e)
	e.RunInTest(t)

	// The first completeSurgraphFlow to run is the deepest (l3 reviving l2's caller): drop that one revive so
	// l2's caller wedges running+parkedSubgraph with a terminal child.
	e.seams.Inject(faultSubgraphReviveLost)
	fk, err := e.Create(ctx, "fcdeep/root", nil, nil)
	assert.NoError(err)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)

	// Wait until the leaf child (l3) has gone terminal - at that point l2's caller is wedged.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id<>0 AND status='"+workflow.StatusCompleted+"'").Scan(&n)
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// One sweep re-drives the wedged l2 caller; the normal completion cascade then carries l2 -> l1 -> root.
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0)
	waitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)

	// The tree ends structurally clean. Unlike the other composition cases in this file, the wedge alarm is *expected* to have
	// fired exactly once here - this test's whole point is that a genuinely-wedged caller was recovered by the
	// sweep (not the normal revive path), so assert the counter is 1 rather than 0.
	assertInvariants(t, e)
	var rm metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &rm))
	unwedged, ok := sumCounter(rm, "dwarf_steps_unwedged", "", "")
	assert.True(ok)
	assert.Equal(int64(1), unwedged) // the sweep re-drove exactly the one lost revive
}
