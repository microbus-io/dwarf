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

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A reusable clean-world battery for the fault tests: the per-fault tests in faults_test.go assert a flow
// reaches its expected terminal status, but not that recovery left a *clean world*. This file adds the two
// reusable end-of-test checks - structural invariants and the always-on wedge alarm - plus the
// fault-transparency property for the faults that cause an in-band re-dispatch: recovery must be
// state-*preserving* (a byte-identical final_state to a no-fault baseline), not merely eventually-terminal.
// That is the property that matters for at-least-once execution of idempotent tasks.

// stateLinear registers a two-node graph A->B->END whose tasks write deterministic, idempotent state (a
// constant, not an increment), so a task that runs twice under recovery still yields a byte-identical
// final_state. calls counts how many times each node ran, so a test can distinguish a re-dispatch (task ran
// twice) from a transparent in-tx retry (task ran once).
func stateLinear(proxy *TestProxy, prefix string, calls map[string]*int) {
	g := workflow.NewGraph("Lin")
	g.SetEndpoint("A", prefix+"/a")
	g.SetEndpoint("B", prefix+"/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	proxy.HandleGraph(prefix+"/g", g)
	proxy.HandleTask(prefix+"/a", func(ctx context.Context, f *workflow.Flow) error {
		if c := calls["a"]; c != nil {
			*c++
		}
		f.SetString("a", "ran")
		return nil
	})
	proxy.HandleTask(prefix+"/b", func(ctx context.Context, f *workflow.Flow) error {
		if c := calls["b"]; c != nil {
			*c++
		}
		f.SetString("b", "ran")
		return nil
	})
}

// readFinalState reads the raw final_state JSON column for a flow, so a test can assert byte-identity across
// two runs of the same graph rather than comparing (map-ordering-nondeterministic) unmarshaled snapshots.
func readFinalState(t *testing.T, e *Engine, flowKey string) string {
	t.Helper()
	assert := testarossa.For(t)
	shard, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	var fs string
	assert.NoError(db.QueryRowContext(context.Background(), "SELECT final_state FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&fs))
	return fs
}

// assertFaultRecoveryClean is the reusable clean-world battery: after a fault test's workload has quiesced, recovery
// must have left a structurally clean world (assertInvariants: no orphan, no terminal-flow-with-non-terminal-
// steps, cohort counters sane, tree links intact) AND must not have tripped the always-on wedge alarm
// (dwarf_steps_unwedged==0). Recovery via the *normal* path must not count as an unwedge; a nonzero value
// means a fault drove a step into a genuinely-wedged state that the sweep had to paper over.
func assertFaultRecoveryClean(t *testing.T, e *Engine, reader sdkmetric.Reader) {
	t.Helper()
	assert := testarossa.For(t)
	assertInvariants(t, e)
	var rm metricdata.ResourceMetrics
	if assert.NoError(reader.Collect(context.Background(), &rm)) {
		if unwedged, ok := sumCounter(rm, "dwarf_steps_unwedged", "", ""); ok {
			assert.Equal(int64(0), unwedged, "dwarf_steps_unwedged fired: a fault drove a step into a genuinely-wedged state")
		}
	}
}

// newFaultBatteryEngine builds an engine wired to a ManualReader (for the unwedged alarm) hosting the
// stateLinear graph under prefix. cfg tweaks the engine before Startup (e.g. shorten the lease).
func newFaultBatteryEngine(t *testing.T, prefix string, cfg func(*Engine)) (*Engine, sdkmetric.Reader, map[string]*int) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	proxy := NewTestProxy()
	calls := map[string]*int{"a": new(int), "b": new(int)}
	stateLinear(proxy, prefix, calls)
	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	e.SetMeterProvider(mp)
	if cfg != nil {
		cfg(e)
	}
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	return e, reader, calls
}

// batteryRun is Create+Await with a ceiling, returning the flow key (for readFinalState) and outcome.
func batteryRun(t *testing.T, e *Engine, url string) (string, *workflow.FlowOutcome) {
	t.Helper()
	assert := testarossa.For(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fk, out, err := e.Run(ctx, url, nil, nil)
	assert.NoError(err)
	return fk, out
}

// TestFault_RecoveryLeavesCleanWorld runs each re-dispatch fault against a no-fault baseline of the same
// graph and asserts three things beyond "eventually completed": a clean invariant world, the wedge alarm
// silent, and a byte-identical final_state (fault-transparency). Each fault is a subtest with its own
// isolated engine, so their leases and DBs never interfere.
func TestFault_RecoveryLeavesCleanWorld(t *testing.T) {
	t.Parallel()
	// The auto-recovering re-dispatch faults: a blocking Run drives recovery entirely in-band.
	cases := []struct {
		label      string
		arm        func(*Engine)
		wantACalls int // task A dispatches expected in the faulted run
		wantBCalls int // task B dispatches expected in the faulted run
	}{
		// A's transition tx fails once after A was marked completed with a NON-contention error: persist retries
		// the transaction in place, so it lands and A runs only ONCE. (It used to run twice - the recovery defer
		// rewound and re-dispatched it, re-executing the task to recover from a database blip.)
		{"transitionCommit", func(e *Engine) { e.seams.Inject(faultTransitionCommit, "A") }, 1, 1},
		// A's transition tx returns a retryable lock-contention error: Transact retries the closure inside the
		// tx, transparently - A runs only once.
		{"contention", func(e *Engine) { e.seams.Inject(faultContention, "A") }, 1, 1},
		// B's flow-completion tx fails once after B was marked completed (B is the terminal node) with a
		// non-contention error: persist retries the transaction in place, so B runs only ONCE. (It used to run
		// twice - the recovery defer rewound and re-dispatched it.)
		{"completeFlowCommit", func(e *Engine) { e.seams.Inject(faultCompleteFlowCommit) }, 1, 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			assert := testarossa.For(t)
			e, reader, calls := newFaultBatteryEngine(t, "fbat", nil)

			// Baseline (no fault): capture the reference final_state and prove a clean world.
			baseKey, baseOut := batteryRun(t, e, "fbat/g")
			assert.Equal(workflow.StatusCompleted, baseOut.Status)
			baseFS := readFinalState(t, e, baseKey)
			assert.NotEqual("", baseFS) // non-empty, so the byte-identity check below is meaningful
			assert.Equal(1, *calls["a"])
			assert.Equal(1, *calls["b"])
			assertFaultRecoveryClean(t, e, reader)

			// Faulted run: same graph, one injected fault, recovered in-band.
			*calls["a"], *calls["b"] = 0, 0
			tc.arm(e)
			fk, out := batteryRun(t, e, "fbat/g")
			assert.Equal(workflow.StatusCompleted, out.Status)

			// Fault-transparency: recovery is state-preserving, not just eventually-terminal.
			assert.Equal(baseFS, readFinalState(t, e, fk), "final_state diverged from the no-fault baseline")
			assert.Equal(tc.wantACalls, *calls["a"])
			assert.Equal(tc.wantBCalls, *calls["b"])
			assertFaultRecoveryClean(t, e, reader)
		})
	}

	// leaseStaleWrite recovers via lease expiry + the poll backstop (not an in-band re-dispatch), so it is
	// driven explicitly with a shortened lease.
	t.Run("leaseStaleWrite", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, reader, calls := newFaultBatteryEngine(t, "fbatlease", func(e *Engine) {
			e.SetTimeBudget(200 * time.Millisecond)
			e.leaseMargin = 100 * time.Millisecond // lease = budget+margin = 300ms, so it expires quickly
		})

		baseKey, baseOut := batteryRun(t, e, "fbatlease/g")
		assert.Equal(workflow.StatusCompleted, baseOut.Status)
		baseFS := readFinalState(t, e, baseKey)
		assert.NotEqual("", baseFS)
		assertFaultRecoveryClean(t, e, reader)

		// A's completion write carries a stale lease generation (a zombie): the fence rejects it, so the step
		// stays claimable and lease recovery re-runs it cleanly - A ran twice, state preserved.
		*calls["a"], *calls["b"] = 0, 0
		e.seams.Inject(faultLeaseStaleWrite, "A")
		fk, err := e.Create(ctx, "fbatlease/g", nil, nil)
		assert.NoError(err)
		// The 300ms lease must lapse before recovery resets the fenced step; drive the backstop until it does
		// (see drivePollBackstop). Anchored on the flow's own status rather than calls["a"], which is a plain *int shared
		// across this battery and would race a read from here.
		drivePollBackstop(t, e, pollBackstopWait, func() bool { return flowStatus(t, e, fk) == workflow.StatusCompleted })
		awaitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)

		assert.Equal(baseFS, readFinalState(t, e, fk), "final_state diverged from the no-fault baseline")
		assert.Equal(2, *calls["a"]) // fenced dispatch + clean re-run
		assert.Equal(1, *calls["b"])
		assertFaultRecoveryClean(t, e, reader)
	})
}
