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

// Package enginetest holds the engine test helpers that are shared across package boundaries - by the
// white-box tests that stay in package engine AND by the black-box tests in the fixtures package. A helper
// lands here only when it is (a) used from more than one package and (b) expressible through the engine's
// exported test surface alone (the DB and Seams accessors and the public operations). A helper that needs an
// unexported internal (driveLeaseRecovery calling recoverExpiredLeases, startSolo touching testConnCap, the
// peer-row writers) cannot live here and stays in package engine with the white-box tests that use it.
//
// This package must NOT import github.com/microbus-io/dwarf/engine. The package engine white-box tests import
// this package, and if it imported engine back, that pair would be an illegal import cycle in the engine test
// binary (engine-test -> enginetest -> engine). So the helpers take the small Engine interface below, which
// *engine.Engine satisfies structurally, rather than the concrete type - which also lets the same helper serve
// both the package engine tests and the fixtures tests with no duplication.
package enginetest

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/piston"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/testarossa"
)

// Engine is the slice of *engine.Engine's exported test surface these helpers use. Defining it here (rather
// than importing engine) is what keeps this package free of the import cycle described in the package doc;
// *engine.Engine satisfies it structurally.
type Engine interface {
	DB() *database.ShardSet
	Seams() *seamster.Seamster
	Run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (string, *workflow.FlowOutcome, error)
	Await(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error)
}

// checkpointFlowStopped MUST match checkpointFlowStopped. It is duplicated (not imported) only because
// importing engine here would form the cycle the package doc explains; a drift makes AwaitFlowStatus's wait
// time out and the test fail loudly, so it cannot go silently wrong.
const checkpointFlowStopped = "flowStopped"

// AssertInvariants is the reusable end-of-test sweep that proves a workload never *created* any of the states
// the background wedge sweep / orphan detector exist to catch. It runs a fixed set of structural checks by
// direct SQL against every shard and must be called only after the workload has quiesced (all Awaits
// returned), because several checks are transiently violated mid-flight (a straggler sibling still settling, a
// fan-in mid-commit). A clean sweep means the recovery machinery had nothing to do.
func AssertInvariants(t *testing.T, e Engine) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()

	terminal := "('" + workflow.StatusCompleted + "', '" + workflow.StatusFailed + "', '" + workflow.StatusCancelled + "')"
	nonTerminal := "('" + workflow.StatusCreated + "', '" + workflow.StatusPending + "', '" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')"

	for shard := 1; shard <= e.DB().NumShards(); shard++ {
		db, err := e.DB().Shard(shard)
		if !assert.NoError(err) {
			continue
		}

		// Asserted ONCE, against the state as it stands. This used to retry for a few seconds first, on the
		// theory that a violation might be work still in flight (a cohort mid-resolve briefly shows every step
		// terminal while the flow is still running - the orphan shape, transient). That settle was removed
		// deliberately: it could only ever HIDE a real violation, never surface one, and a check that waits for
		// an invariant to come true is not asserting an invariant. The precondition is the caller's to meet -
		// call this only after the workload has quiesced (all Awaits returned).
		check := func(label, query string) {
			var n int
			if !assert.NoError(db.QueryRowContext(ctx, query).Scan(&n)) {
				return
			}
			assert.Equal(0, n, "shard %d: invariant violated: %s (%d offending rows)", shard, label, n)
		}

		// 1. No terminal flow retains a non-terminal step. Delete-marked flows (delete_after_ms>0) are excluded:
		// Delete flips an interrupted flow to cancelled but leaves its interrupted leaf for the reaper to remove
		// with the whole tree (deferred deletion), so a doomed row legitimately carries a non-terminal step
		// until reaped - inert meanwhile (selection/lease-recovery ignore interrupted; Resume 409s the cancelled
		// flow).
		check("terminal flow with a non-terminal step",
			"SELECT COUNT(*) FROM dwarf_steps s JOIN dwarf_flows f ON s.flow_id=f.flow_id"+
				" WHERE f.status IN "+terminal+" AND f.delete_after_ms=0 AND s.status IN "+nonTerminal)

		// 2. No terminal step is left parked.
		check("terminal step with parked<>0",
			"SELECT COUNT(*) FROM dwarf_steps WHERE status IN "+terminal+" AND parked<>0")

		// 3. Every running flow has at least one non-terminal step (else it is the orphan shape).
		check("running flow with zero non-terminal steps",
			"SELECT COUNT(*) FROM dwarf_flows f WHERE f.status='"+workflow.StatusRunning+"'"+
				" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status IN "+nonTerminal+")")

		// 4. Cohort counters never overshoot: arrivals<=size and failures<=arrivals on every spawn step.
		check("cohort counter overshoot",
			"SELECT COUNT(*) FROM dwarf_steps WHERE cohort_size>0 AND (cohort_arrivals>cohort_size OR cohort_failures>cohort_arrivals)")

		// 5. Every subgraph child's surgraph links resolve: the caller step exists, and the caller flow exists
		// and shares this child's root_flow_id (same tree).
		check("dangling surgraph link",
			"SELECT COUNT(*) FROM dwarf_flows f WHERE f.surgraph_step_id>0 AND ("+
				"NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.step_id=f.surgraph_step_id)"+
				" OR NOT EXISTS (SELECT 1 FROM dwarf_flows p WHERE p.flow_id=f.surgraph_flow_id AND p.root_flow_id=f.root_flow_id))")

		// 6. Every flow's root_flow_id resolves to an existing flow that is its own root.
		check("root_flow_id not resolving to a self-root",
			"SELECT COUNT(*) FROM dwarf_flows f WHERE NOT EXISTS ("+
				"SELECT 1 FROM dwarf_flows r WHERE r.flow_id=f.root_flow_id AND r.root_flow_id=r.flow_id)")

		// 7. No DAG edge (successor_id/predecessor_id) crosses into a different flow (0 = no edge, allowed).
		check("cross-flow DAG edge",
			"SELECT COUNT(*) FROM dwarf_steps s WHERE"+
				" (s.successor_id<>0 AND EXISTS (SELECT 1 FROM dwarf_steps t WHERE t.step_id=s.successor_id AND t.flow_id<>s.flow_id))"+
				" OR (s.predecessor_id<>0 AND EXISTS (SELECT 1 FROM dwarf_steps t WHERE t.step_id=s.predecessor_id AND t.flow_id<>s.flow_id))")
	}
}

// AwaitFlowStatus blocks until the given flow has reached the given stop status, failing the test on timeout.
// It rendezvouses with the engine's post-commit flowStopped checkpoint rather than polling the flow row, so a
// return means the status is durable. The order is load-bearing: arm the waiter FIRST, then check Visits - a
// stop before the call is caught by the count, one after by the channel, one landing between the two lines by
// the channel (the waiter is already registered). Reversing the two lines reintroduces the race the split-arm
// waiter exists to remove. Two limits: it only sees stops routed through signalStop
// (completed/failed/cancelled/interrupted), and because Visits is monotonic a REPEATED status is satisfied by
// the earlier occurrence (wait on such a status with a Waiter armed around the specific trigger instead).
func AwaitFlowStatus(t *testing.T, e Engine, flowKey, want string, timeout time.Duration) {
	t.Helper()
	reached := e.Seams().Waiter(checkpointFlowStopped, flowKey, want) // arm first...
	if e.Seams().Visits(checkpointFlowStopped, flowKey, want) > 0 {
		return // ...then catch a stop that beat us to it
	}
	select {
	case <-reached:
	case <-time.After(timeout * testTimeoutScale):
		t.Fatalf("flow %s never reached status %q within %s", flowKey, want, timeout*testTimeoutScale)
	}
}

// BoundedRun runs a flow with a generous ctx bound (scaled under -race) so a wedge fails the test instead of
// hanging the suite.
func BoundedRun(t *testing.T, e Engine, url string) *workflow.FlowOutcome {
	t.Helper()
	assert := testarossa.For(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second*testTimeoutScale)
	defer cancel()
	_, out, err := e.Run(ctx, url, nil, nil)
	assert.NoError(err)
	return out
}

// BoundedAwait awaits a flow with a 30s ctx bound so a wedge fails the test instead of hanging.
func BoundedAwait(t *testing.T, e Engine, flowKey string) (*workflow.FlowOutcome, error) {
	t.Helper()
	awaitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return e.Await(awaitCtx, flowKey)
}

// AwaitAndAssertComplete awaits a flow (bounded) and asserts it completed, returning the outcome.
func AwaitAndAssertComplete(t *testing.T, e Engine, flowKey string) *workflow.FlowOutcome {
	t.Helper()
	assert := testarossa.For(t)
	out, err := BoundedAwait(t, e, flowKey)
	if !assert.NoError(err) || !assert.NotNil(out) {
		return nil
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	return out
}

// TimeoutScale is the multiplier the shared wait helpers apply to their "don't hang" ceilings - 1 normally,
// 5 under -race. Exported for a fixture that must hold its own ceiling rather than route through a helper
// here: -race slows execution ~10x and, with the whole suite parallel, compounds with CPU oversubscription,
// so an unscaled ceiling that passes serially flakes under it. Scale the ceiling, never the assertion - a
// genuine wedge never completes and trips even the stretched bound.
func TimeoutScale() time.Duration { return testTimeoutScale }

// AwaitVisits blocks until the host passes the named checkpoint n FURTHER times, counted from the moment of
// the call, and fails the test if it does not inside the timeout.
//
// The arm-check-block loop is the whole content, and it earns a shared helper because the ways to get it
// wrong are quiet ones. seamster's Waiter is ONE-SHOT and arms for the host's NEXT arrival, while Visits is
// a monotonic count - so waiting for several occurrences means re-arming per occurrence, and the two have to
// be read in the order below: arm FIRST, then read the count, so an arrival landing between the two lines is
// caught by the channel that is already registered rather than lost between them. The deadline is taken once
// rather than per iteration, or each arrival would silently renew the whole budget.
//
// The timeout is a "did it hang" ceiling, never a timing contract, so it stretches under -race like every
// other ceiling here. Waiting for N occurrences of a checkpoint is how a test states "the engine has had its
// chance" without naming a duration: a slow machine makes each occurrence later, not fewer.
//
// Call it from the TEST goroutine, like every other helper here that fails the test: t.Fatalf from a task
// handler or any other engine-owned goroutine is illegal, and here it is also useless - the engine goroutine
// it kills is the one that would have driven the checkpoint, so the suite wedges instead of reporting.
func AwaitVisits(t *testing.T, seams *seamster.Seamster, n int, timeout time.Duration, checkpointName string, scope ...string) {
	t.Helper()
	want := seams.Visits(checkpointName, scope...) + n
	deadline := time.After(timeout * testTimeoutScale)
	for {
		reached := seams.Waiter(checkpointName, scope...) // arm FIRST...
		got := seams.Visits(checkpointName, scope...)     // ...then read
		if got >= want {
			return
		}
		select {
		case <-reached:
		case <-deadline:
			t.Fatalf("checkpoint %q reached %d of the %d times awaited within %s",
				strings.Join(append([]string{checkpointName}, scope...), ":"),
				seams.Visits(checkpointName, scope...), want, timeout*testTimeoutScale)
		}
	}
}

// AwaitShardCycles blocks until every shard's piston has completed `extra` further pushing cycles, counted
// from the moment of the call.
//
// A pushing cycle is the point at which that shard's cache partition has been reconciled against the plan,
// so this is how a test waits for the fleet's cached hints to agree with what the planner actually chose.
// There is no wall-clock stand-in for it, and that is the whole reason the checkpoint exists: each piston
// turns on its own cadence, so one starved or slow shard holds an unreconciled partition for as long as it
// likes while its peers turn normally - the asymmetric case, which no uniform delay reproduces or waits out.
//
// TWO cycles, not one, is the usual ask: a cycle already in flight when the work committed may have scanned
// before it existed, so its push proves nothing about it. The second is the one whose scan is guaranteed to
// have seen it.
func AwaitShardCycles(t *testing.T, e Engine, shards, extra int) {
	t.Helper()
	for shard := 1; shard <= shards; shard++ {
		AwaitVisits(t, e.Seams(), extra, time.Minute, piston.CheckpointCycleDone, strconv.Itoa(shard))
	}
}
