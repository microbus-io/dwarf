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
// public operations and the internal/accessors alone. A helper that needs an
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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/testarossa"
)

// Engine is the slice of *engine.Engine's exported surface these helpers use; the shard set and the seams
// are reached through internal/accessors. Defining it here (rather than importing engine) is what keeps
// this package free of the import cycle described in the package doc; *engine.Engine satisfies it
// structurally.
type Engine interface {
	Run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (string, *workflow.FlowOutcome, error)
	Await(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error)
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
	stopped := diagnostics.Join(diagnostics.CheckpointFlowStopped, flowKey, want)
	reached := accessors.Seams(e).Waiter(stopped) // arm first...
	if accessors.Seams(e).Visits(stopped) > 0 {
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

// BoundedAwait awaits a flow with a 30s ctx bound (scaled under -race) so a wedge fails the test instead of
// hanging.
func BoundedAwait(t *testing.T, e Engine, flowKey string) (*workflow.FlowOutcome, error) {
	t.Helper()
	awaitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second*testTimeoutScale)
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

// Race reports whether the test binary was built with -race, for a fixture whose workload needs a different
// engine configuration there rather than a longer ceiling.
func Race() bool { return raceEnabled }

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
func AwaitVisits(t *testing.T, seams *seamster.Seamster, n int, timeout time.Duration, checkpointName string) {
	t.Helper()
	AwaitVisitCount(t, seams, seams.Visits(checkpointName)+n, timeout, checkpointName)
}

// AwaitVisitCount is AwaitVisits for a test that counts from an earlier moment: it blocks until the checkpoint
// has been reached want times in all. Read the base with Visits before triggering the events, then wait for
// base+n, so an arrival landing before the wait begins still counts.
func AwaitVisitCount(t *testing.T, seams *seamster.Seamster, want int, timeout time.Duration, checkpointName string) {
	t.Helper()
	deadline := time.After(timeout * testTimeoutScale)
	for {
		reached := seams.Waiter(checkpointName) // arm FIRST...
		got := seams.Visits(checkpointName)     // ...then read
		if got >= want {
			return
		}
		select {
		case <-reached:
		case <-deadline:
			t.Fatalf("checkpoint %q reached %d of the %d times awaited within %s",
				checkpointName, seams.Visits(checkpointName), want, timeout*testTimeoutScale)
		}
	}
}

// AwaitShardCycles blocks until every open shard's piston has SCANNED and then PUSHED `extra` further times,
// counted from the moment of the call.
//
// A pushing cycle is the point at which that shard's cache partition has been reconciled against the plan,
// so this is how a test waits for the fleet's cached hints to agree with what the planner actually chose.
// There is no wall-clock stand-in for it, and that is the whole reason the checkpoints exist: each piston
// turns on its own cadence, so one starved or slow shard holds an unreconciled partition for as long as it
// likes while its peers turn normally - the asymmetric case, which no uniform delay reproduces or waits out.
//
// TWO of each, not one: a cycle already in flight when the work committed may have started before it
// existed, so it proves nothing about it. The second is the one guaranteed to have seen it.
//
// IT WAITS ON BOTH LOOPS, IN THAT ORDER, AND COUNTING PUSHES ALONE IS NOT ENOUGH. The piston scans and
// pushes on independent cadences, so a push says only "the partition matches the plan" - and the plan is
// built from whatever tally the planner already held, which may predate the work entirely. N pushes can
// therefore all resolve a stale tally. Waiting for the SCANS first is what puts the work into the plan; the
// pushes that follow are what put the plan into the partition.
func AwaitShardCycles(t *testing.T, e Engine, extra int) {
	t.Helper()
	shards := accessors.DB(e).Indices()
	// Every shard scans before any shard's pushes are awaited, so no shard can satisfy its push count
	// against a plan built before a peer had reported.
	for _, shard := range shards {
		AwaitVisits(t, accessors.Seams(e), extra, time.Minute, diagnostics.Join(diagnostics.CheckpointRefillTallyDone, strconv.Itoa(shard)))
	}
	for _, shard := range shards {
		AwaitVisits(t, accessors.Seams(e), extra, time.Minute, diagnostics.Join(diagnostics.CheckpointRefillCycleDone, strconv.Itoa(shard)))
	}
}
