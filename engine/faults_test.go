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

// White-box tests for the fault-injection seam (debug.go). Each arms a named fault and asserts the engine
// takes the recovery/routing path it exists to guard, without DB forging or timing races.
package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// boundedRun is Create+Await with a ceiling, so a wedge fails the test instead of hanging.
// linear2 registers a two-node graph A->B->END whose tasks succeed, so a fault on A's transition (which
// inserts B) can be exercised. okTasks records how many times each node ran.
func linear2(proxy *TestProxy, prefix string, calls map[string]*int) {
	g := workflow.NewGraph("Lin")
	g.SetEndpoint("A", prefix+"/a")
	g.SetEndpoint("B", prefix+"/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	proxy.HandleGraph(prefix+"/g", g)
	for _, node := range []string{"a", "b"} {
		n := node
		proxy.HandleTask(prefix+"/"+n, func(ctx context.Context, f *workflow.Flow) error {
			if c := calls[n]; c != nil {
				*c++
			}
			return nil
		})
	}
}

// awaitNextStop arms a rendezvous for the NEXT flow stop (completed/failed/cancelled/interrupted) and returns
// the wait half, replacing a status poll / sleep with an exact wait on the (post-commit) signalStop. Call it
// BEFORE the action that triggers the stop, then call the returned func after: the waiter is registered by the
// time awaitNextStop returns, so a stop firing in between is observed rather than lost. That closes both the
// "a background poll drove it first" race and the "slow CI missed the window" flake a poll loop carries.
//
// It used to arm a Break and Resume it after the wait, purely to be race-free; a Waiter is race-free on its own
// without freezing a worker at signalStop, so the breakpoint is gone.
//
// Two constraints. It catches the FIRST stop of ANY flow, so use it only in single-flow-of-interest windows.
// And chaining two of them - wait for stop A, then arm for stop B - is safe only where B genuinely cannot
// happen until the test drives it: nothing holds the engine between the first wait returning and the second
// arming, so an unblocked B could slip through that gap unobserved.
func awaitNextStop(t *testing.T, e *Engine) func() {
	t.Helper()
	assert := testarossa.For(t)
	reached := e.seams.Waiter(CheckpointFlowStopped)
	return func() {
		t.Helper()
		select {
		case <-reached:
		case <-time.After(10 * time.Second):
			assert.True(false, "no flow reached a stop")
			return
		}
	}
}

// driveLeaseRecovery drives lease recovery until `landed` reports the re-dispatch happened, or retryDuration
// elapses and the caller's own assertion reports it.
//
// This DRIVES the engine rather than merely observing it, which is why it is the one helper here that still
// loops: without the e.recoverExpiredLeases call the recovery never happens inside a test's lifetime, measured -
// every run of the lease-fence battery fails on the wait. A loop that makes the thing happen is not the
// spin-wait-on-an-observation that every other wait in this package was converted away from.
//
// Drive it in a loop rather than sleeping past the lease and polling once. recoverExpiredLeases only resets a
// `running` step whose lease has ALREADY expired, so a single fixed-sleep poll has to land between the lease
// lapsing and the caller giving up - and the delay before the lease even starts is variable (the dispatch,
// and for a subgraph the spawn and the child's claim too). A poll that lands early resets nothing, and a
// fenced or abandoned attempt schedules no recovery of its own, so nothing re-polls and the flow never
// advances. Measured on SQL Server: the poll arriving 7ms early, failing 7% idle and 45% under load. Looping
// is timing-independent - whichever poll first sees the lapsed lease does the reset - and cannot
// over-dispatch, since the reset needs `running` plus an expired lease, true only once the previous attempt
// has finished and its lease has run out.
func driveLeaseRecovery(t *testing.T, e *Engine, retryDuration time.Duration, landed func() bool) {
	t.Helper()
	ctx := context.Background()
	for range max(1, int(retryDuration/retryInterval)) {
		if landed() {
			return
		}
		time.Sleep(retryInterval)
		e.recoverExpiredLeases(ctx)
	}
}

// --- Dispatch faults (scoped by task name) ---

func TestFault_ExecuteTask(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	proxy.HandleTask("fexec/work", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("fexec/rescue", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("rescued", "yes")
		return nil
	})
	bare := workflow.NewGraph("Bare")
	bare.SetEndpoint("Work", "fexec/work")
	bare.AddTransition("Work", workflow.END)
	proxy.HandleGraph("fexec/bare", bare)
	handled := workflow.NewGraph("Handled")
	handled.SetEndpoint("Work", "fexec/work")
	handled.SetEndpoint("Rescue", "fexec/rescue")
	handled.AddTransition("Work", workflow.END)
	handled.AddTransitionOnError("Work", "Rescue")
	handled.AddTransition("Rescue", workflow.END)
	proxy.HandleGraph("fexec/handled", handled)

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// No onError -> the injected failure fails the flow.
	e.seams.Inject(seamsJoin(FaultExecuteTask, "Work"))
	if out := enginetest.BoundedRun(t, e, "fexec/bare"); assert.NotNil(out) {
		assert.Equal(workflow.StatusFailed, out.Status)
	}
	// With onError -> routed to the handler and the flow completes. Fault consumed, so re-arm.
	e.seams.Inject(seamsJoin(FaultExecuteTask, "Work"))
	if out := enginetest.BoundedRun(t, e, "fexec/handled"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal("yes", out.State["rescued"])
	}
	// Disarmed: the bare flow now completes.
	if out := enginetest.BoundedRun(t, e, "fexec/bare"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

func TestFault_PanicExecuteTask(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	proxy.HandleTask("fpanic/boom", func(ctx context.Context, f *workflow.Flow) error { return nil })
	g := workflow.NewGraph("Boom")
	g.SetEndpoint("Boom", "fpanic/boom")
	g.AddTransition("Boom", workflow.END)
	proxy.HandleGraph("fpanic/g", g)

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// A panic in the host call is caught at the boundary and fails the step (no wedge/crash-loop).
	e.seams.Inject(seamsJoin(FaultPanicExecuteTask, "Boom"))
	if out := enginetest.BoundedRun(t, e, "fpanic/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusFailed, out.Status)
	}
	if out := enginetest.BoundedRun(t, e, "fpanic/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

func TestFault_LoadGraph(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("G")
	g.SetEndpoint("A", "floadg/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("floadg/g", g)
	proxy.HandleTask("floadg/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// Armed: Create fails (the graph "cannot be loaded"). Disarmed: it creates and completes.
	e.seams.Inject(seamsJoin(FaultLoadGraph, "floadg/g"))
	_, err := e.Create(ctx, "floadg/g", nil, nil)
	assert.Error(err)
	assert.Equal(500, errors.StatusCode(err))

	if out := enginetest.BoundedRun(t, e, "floadg/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

// --- Transaction / recovery faults ---

func TestFault_TransitionCommit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	calls := map[string]*int{"a": new(int), "b": new(int)}
	linear2(proxy, "ftrans", calls)

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// A's transition transaction fails once with a non-contention error, after A was marked completed. It is
	// retried IN PLACE (see persist), so it lands on the next attempt and the flow completes - and A runs ONCE.
	//
	// It used to run twice: the recovery defer rewound A and re-dispatched it, which RE-EXECUTED THE TASK to
	// recover from a database blip the task had nothing to do with. Retrying the write instead of the task is
	// the whole point - a task's side effects must not re-fire because a transaction lost a connection.
	e.seams.Inject(seamsJoin(FaultTransitionCommit, "A"))
	if out := enginetest.BoundedRun(t, e, "ftrans/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(1, *calls["a"]) // the WRITE was retried, not the task
	assert.Equal(1, *calls["b"])
}

func TestFault_Contention(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	calls := map[string]*int{"a": new(int), "b": new(int)}
	linear2(proxy, "fcont", calls)

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// A's transition transaction returns a lock-contention error once: Transact retries the closure to a
	// clean commit, transparently - the flow completes and A ran only once (retry is inside the tx).
	e.seams.Inject(seamsJoin(FaultContention, "A"))
	if out := enginetest.BoundedRun(t, e, "fcont/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(1, *calls["a"])
	assert.Equal(1, *calls["b"])
}

func TestFault_CompleteFlowCommit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "fcfc/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("fcfc/g", g)
	var runs int
	proxy.HandleTask("fcfc/a", func(ctx context.Context, f *workflow.Flow) error { runs++; return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// The flow-completion transaction fails once after the terminal step was marked completed with a
	// non-contention error: persist retries the transaction in place, so the flow still completes (no `running`
	// orphan with all steps terminal) and the task runs ONCE. It used to run twice - the recovery defer rewound
	// the step and re-dispatched it, RE-EXECUTING the task to recover from a database blip the task had nothing
	// to do with.
	e.seams.Inject(FaultCompleteFlowCommit)
	if out := enginetest.BoundedRun(t, e, "fcfc/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(1, runs) // the WRITE was retried, not the task
}

func TestFault_LeaseStaleWrite(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "flease/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("flease/g", g)
	// Atomic: the worker goroutine writes it while the test goroutine waits on it below.
	var runs atomic.Int32
	proxy.HandleTask("flease/a", func(ctx context.Context, f *workflow.Flow) error { runs.Add(1); return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	e.SetTimeBudget(200 * time.Millisecond)
	e.leaseMargin = 100 * time.Millisecond // lease = budget+margin = 300ms, so it expires quickly
	assert.NoError(e.Startup(t.Context()))

	// A's completion write carries a stale lease generation (a zombie): the fence rejects it (no-op), so the
	// step stays claimable and lease recovery re-runs it cleanly - a stale write never corrupts or
	// terminalizes the flow. The first dispatch's write is fenced out; once its lease lapses the poll
	// backstop resets the step and it re-runs (fault consumed) to a clean completion, so A ran twice.
	e.seams.Inject(seamsJoin(FaultLeaseStaleWrite, "A"))
	// Rendezvous on the clean re-run's completion (armed before Create: the fenced first dispatch never stops,
	// so the first - and only - stop is the recovered completion, caught whichever poll drives it).
	waitDone := awaitNextStop(t, e)
	fk, err := e.Create(ctx, "flease/g", nil, nil)
	assert.NoError(err)
	// The 300ms lease must lapse before recovery re-runs A; drive the backstop until it does.
	driveLeaseRecovery(t, e, leaseRecoveryWait, func() bool { return runs.Load() >= 2 })
	waitDone()
	assert.Equal(workflow.StatusCompleted, enginetest.FlowStatus(t, e, fk))
	assert.Equal(int32(2), runs.Load())
}

// --- Signal / doorbell faults (process-wide) ---

func TestFault_DropSignalStop(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "fsig/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("fsig/g", g)
	proxy.HandleTask("fsig/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// The terminal wake is dropped, so Await cannot rely on the signal; it must return via the latch
	// detector reading the DB-committed stop.
	e.seams.Inject(FaultDropSignalStop)
	if out := enginetest.BoundedRun(t, e, "fsig/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

func TestFault_DropDoorbell(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "fdoor/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("fdoor/g", g)
	proxy.HandleTask("fdoor/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// The create-time doorbell is dropped, so nothing hands the entry step to a worker directly. It is
	// `pending` and due, so the shard's next piston cycle selects it on its own - which is the whole
	// backstop, and needs no help from this test. Lease recovery is NOT what covers this (it only resets
	// `running` rows), so the flow simply has to complete within a few cycles.
	e.seams.Inject(FaultDropDoorbell)
	fk, err := e.Create(ctx, "fdoor/g", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)
}

// --- Background-recovery faults ---

func TestFault_SubgraphReviveLost(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "fsub/call")
	parent.AddTransition("Call", workflow.END)
	proxy.HandleGraph("fsub/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "fsub/x")
	child.AddTransition("X", workflow.END)
	proxy.HandleGraph("fsub/child", child)
	proxy.HandleTask("fsub/x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("fsub/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("fsub/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// The child's revive of the parked caller is lost: the caller wedges running+parkedSubgraph with a
	// terminal child. The wedge sweep must re-drive the release. Arm, create, wait for the wedge, then
	// invoke the sweep with minAge 0 (bypassing the steady-state age guard) and the parent completes.
	//
	// Armed with a COUNT and withdrawn below, rather than Inject's single fire, because the rendezvous
	// below lands one statement too early to order the two consults. completeFlow fires the stop
	// checkpoint and only THEN calls completeSurgraphFlow, so waitChild returns before the worker has
	// consulted the fault - and a single fire would then be consumed by whichever of the worker and the
	// sweep got there first. If the sweep won, ITS revive would be the one dropped, the worker would
	// perform the real one, and this test would pass while proving nothing about the sweep.
	e.seams.InjectN(FaultSubgraphReviveLost, 1<<20)
	waitChild := awaitNextStop(t, e) // rendezvous on the child's completion (the first stop; parent is parked)
	fk, err := e.Create(ctx, "fsub/parent", nil, nil)
	assert.NoError(err)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)

	// The child has now gone terminal; its revive is (faultily) lost, so the caller is wedged.
	waitChild()
	// Disarm before the sweep, so the re-drive below is the one revive that is allowed to land.
	e.seams.Withdraw(FaultSubgraphReviveLost)
	// Rendezvous on the parent's completion after the sweep re-drives the release (armed before the sweep).
	waitParent := awaitNextStop(t, e)
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0) // the sweep, no age guard
	waitParent()
	assert.Equal(workflow.StatusCompleted, enginetest.FlowStatus(t, e, fk))
}

func TestFault_ReapMidTree(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "freap/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("freap/g", g)
	proxy.HandleTask("freap/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	shortenDeletion(e, time.Millisecond, time.Hour) // due immediately; the test drives reaps
	assert.NoError(e.Startup(t.Context()))

	waitDone := awaitNextStop(t, e) // rendezvous on completion instead of polling status
	fk, err := e.Create(ctx, "freap/g", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	waitDone()
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	time.Sleep(5 * time.Millisecond) // inherent wall-clock: let the 1ms deletion window elapse

	// Reap aborts mid-tree (after steps, before flows) and rolls back: the tree is left intact, not a
	// half-deleted flow. The next reap pass (fault consumed) removes it cleanly.
	e.seams.Inject(FaultReapMidTree)
	e.reapDueFlows(ctx)
	assert.Equal(1, shardFlowCount(t, e, shard)) // rolled back - still present

	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shard)) // clean removal
}

// --- Poll / refill clamp faults (process-wide) ---

func TestFault_RefillScanErr(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "frefill/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("frefill/g", g)
	proxy.HandleTask("frefill/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// The refiller's band scan errors once. Instead of refilling empty and idling every worker forever, it
	// logs and shortens the next poll, so the flow is still dispatched and completes (just after a re-scan).
	e.seams.Inject(FaultRefillScanErr)
	if out := enginetest.BoundedRun(t, e, "frefill/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

// TestFault_RefillScanErrPreservesCache pins the scan-error policy THROUGH THE ENGINE'S WIRING: the fault
// is armed on the engine's seams, which must have reached the piston for this to fire at all. Refill is a
// wholesale partition replace that HONORS an empty batch (an empty scan means nothing is due, so the
// cached hints are dead), so a FAILED scan must not be routed into it: an error means "unknown", not
// "nothing is due", and replacing a healthy partition with nothing on a transient DB blip would idle its
// workers in Pop. The hints cost nothing to keep - a worker popping a stale one just loses its claim CAS.
func TestFault_RefillScanErrPreservesCache(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngineUnderTest(t)
	e.SetHost(NewTestProxy())
	assert.NoError(e.SetWorkers(0)) // no workers, so nothing drains the cache underneath the assertion
	assert.NoError(e.Startup(t.Context()))

	// Seed the cache as a healthy cycle would have, then make every scan fail (sticky, so a background
	// cycle cannot slip a legitimate empty refill in and wipe the cache on its own).
	e.cache.Refill(1, []candidates.Job{{StepID: 101, Shard: 1}, {StepID: 102, Shard: 1}}, 5)
	assert.Equal(2, e.cache.Len())
	e.seams.InjectN(FaultRefillScanErr, 1<<20)

	_, _, err := e.pistons[1].ScanBand(ctx, 1)
	assert.Error(err, "the engine's seams must reach its pistons")
	assert.Equal(2, e.cache.Len(), "a failed scan does not discard the healthy candidates")
}
