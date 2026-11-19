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

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
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
func stateLinear(mux *hosts.Mux, prefix string, calls map[string]*int) {
	g := workflow.NewGraph("Lin")
	g.SetURL("A", prefix+"/a")
	g.SetURL("B", prefix+"/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph(prefix+"/g", g)
	mux.HandleTask(prefix+"/a", func(ctx context.Context, f *workflow.Flow) error {
		if c := calls["a"]; c != nil {
			*c++
		}
		f.SetString("a", "ran")
		return nil
	})
	mux.HandleTask(prefix+"/b", func(ctx context.Context, f *workflow.Flow) error {
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
	shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
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
	enginetest.AssertInvariants(t, e)
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
	assert := testarossa.For(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	mux := hosts.NewMux()
	calls := map[string]*int{"a": new(int), "b": new(int)}
	stateLinear(mux, prefix, calls)
	e := NewEngineUnderTest(t.Name())
	e.SetHost(mux)
	e.SetMeterProvider(mp)
	if cfg != nil {
		cfg(e)
	}
	assert.Must(assert.NoError(e.Startup(t.Context())))
	return e, reader, calls
}

// batteryRun is Create+Await with a ceiling, returning the flow key (for readFinalState) and outcome. The minute
// bounds only a hang - a run on a loaded server is unbounded - and a run that returns no outcome stops the test
// here: every caller reads the outcome, and a nil one would panic and take the whole test binary down with it.
func batteryRun(t *testing.T, e *Engine, url string) (string, *workflow.FlowOutcome) {
	t.Helper()
	assert := testarossa.For(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*enginetest.TimeoutScale())
	defer cancel()
	fk, out, err := e.Run(ctx, url, nil, nil)
	if !assert.NoError(err) || !assert.NotNil(out) {
		t.FailNow()
	}
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
		// the transaction in place, so it lands and A runs only ONCE.
		{"transitionCommit", func(e *Engine) { e.diags.Seams().Inject(diagnostics.Join(diagnostics.FaultTransitionCommit, "A")) }, 1, 1},
		// A's transition tx returns a retryable lock-contention error: Transact retries the closure inside the
		// tx, transparently - A runs only once.
		{"contention", func(e *Engine) { e.diags.Seams().Inject(diagnostics.Join(diagnostics.FaultContention, "A")) }, 1, 1},
		// B's flow-completion tx fails once after B was marked completed (B is the terminal node) with a
		// non-contention error: persist retries the transaction in place, so B runs only ONCE.
		{"completeFlowCommit", func(e *Engine) { e.diags.Seams().Inject(diagnostics.FaultCompleteFlowCommit) }, 1, 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			assert := testarossa.For(t)
			e, reader, calls := newFaultBatteryEngine(t, "fbat", func(e *Engine) {
				e.setClaimsTrackerEnabled(false)
				e.persistBackoff = shortPersistBackoff
			})
			defer e.Shutdown(t.Context())

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

	// leaseStaleWrite recovers via lease expiry + lease recovery (not an in-band re-dispatch), so it is
	// driven explicitly: the fenced dispatch's lease is expired and lease recovery driven until it resets the step.
	t.Run("leaseStaleWrite", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, reader, calls := newFaultBatteryEngine(t, "fbatlease", func(e *Engine) {
			e.setClaimsTrackerEnabled(false)
			e.persistBackoff = shortPersistBackoff
		})
		defer e.Shutdown(ctx)

		baseKey, baseOut := batteryRun(t, e, "fbatlease/g")
		assert.Equal(workflow.StatusCompleted, baseOut.Status)
		baseFS := readFinalState(t, e, baseKey)
		assert.NotEqual("", baseFS)
		assertFaultRecoveryClean(t, e, reader)

		// A's completion write carries a stale lease generation (a zombie): the fence rejects it, so the step
		// stays claimable and lease recovery re-runs it cleanly - A ran twice, state preserved.
		*calls["a"], *calls["b"] = 0, 0
		e.diags.Seams().Inject(diagnostics.Join(diagnostics.FaultLeaseStaleWrite, "A"))
		firstDone := e.diags.Seams().Waiter(diagnostics.CheckpointClaimedDispatchDone)
		fk, err := e.Create(ctx, "fbatlease/g", nil, nil)
		assert.NoError(err)
		select {
		case <-firstDone:
		case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
			t.Fatal("A's first dispatch never returned")
		}
		// Lapse the FENCED generation's lease, and only that one. Do not shorten the lease instead: the re-run's
		// claim gets the same short lease, and a re-run slower than it is itself recovered and runs A a third time.
		expireLease(t, e, fk, "A")
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)

		assert.Equal(baseFS, readFinalState(t, e, fk), "final_state diverged from the no-fault baseline")
		assert.Equal(2, *calls["a"]) // fenced dispatch + clean re-run
		assert.Equal(1, *calls["b"])
		assertFaultRecoveryClean(t, e, reader)
	})
}
