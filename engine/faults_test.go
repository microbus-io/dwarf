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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// boundedRun is Create+Await with a ceiling, so a wedge fails the test instead of hanging.
func boundedRun(t *testing.T, e *Engine, url string) *workflow.FlowOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, out, err := e.Run(ctx, url, nil, nil)
	testarossa.For(t).NoError(err)
	return out
}

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

// --- Dispatch faults (scoped by task name) ---

func TestFault_ExecuteTask(t *testing.T) {
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

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// No onError -> the injected failure fails the flow.
	e.injectFault(faultKey(faultExecuteTask, "Work"))
	if out := boundedRun(t, e, "fexec/bare"); assert.NotNil(out) {
		assert.Equal(workflow.StatusFailed, out.Status)
	}
	// With onError -> routed to the handler and the flow completes. Fault consumed, so re-arm.
	e.injectFault(faultKey(faultExecuteTask, "Work"))
	if out := boundedRun(t, e, "fexec/handled"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal("yes", out.State["rescued"])
	}
	// Disarmed: the bare flow now completes.
	if out := boundedRun(t, e, "fexec/bare"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

func TestFault_PanicExecuteTask(t *testing.T) {
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	proxy.HandleTask("fpanic/boom", func(ctx context.Context, f *workflow.Flow) error { return nil })
	g := workflow.NewGraph("Boom")
	g.SetEndpoint("Boom", "fpanic/boom")
	g.AddTransition("Boom", workflow.END)
	proxy.HandleGraph("fpanic/g", g)

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// A panic in the host call is caught at the boundary and fails the step (no wedge/crash-loop).
	e.injectFault(faultKey(faultPanicExecuteTask, "Boom"))
	if out := boundedRun(t, e, "fpanic/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusFailed, out.Status)
	}
	if out := boundedRun(t, e, "fpanic/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

func TestFault_LoadGraph(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("G")
	g.SetEndpoint("A", "floadg/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("floadg/g", g)
	proxy.HandleTask("floadg/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// Armed: Create fails (the graph "cannot be loaded"). Disarmed: it creates and completes.
	e.injectFault(faultKey(faultLoadGraph, "floadg/g"))
	_, err := e.Create(ctx, "floadg/g", nil, nil)
	assert.Error(err)
	assert.Equal(500, errors.StatusCode(err))

	if out := boundedRun(t, e, "floadg/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

// --- Transaction / recovery faults ---

func TestFault_TransitionCommit(t *testing.T) {
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	calls := map[string]*int{"a": new(int), "b": new(int)}
	linear2(proxy, "ftrans", calls)

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// A's transition transaction fails once, after A was marked completed: the recovery defer must reset A
	// and re-dispatch, so the flow still completes and A ran twice.
	e.injectFault(faultKey(faultTransitionCommit, "A"))
	if out := boundedRun(t, e, "ftrans/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(2, *calls["a"]) // re-dispatched after the failed transition
	assert.Equal(1, *calls["b"])
}

func TestFault_Contention(t *testing.T) {
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	calls := map[string]*int{"a": new(int), "b": new(int)}
	linear2(proxy, "fcont", calls)

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// A's transition transaction returns a lock-contention error once: Transact retries the closure to a
	// clean commit, transparently - the flow completes and A ran only once (retry is inside the tx).
	e.injectFault(faultKey(faultContention, "A"))
	if out := boundedRun(t, e, "fcont/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(1, *calls["a"])
	assert.Equal(1, *calls["b"])
}

func TestFault_CompleteFlowCommit(t *testing.T) {
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "fcfc/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("fcfc/g", g)
	var runs int
	proxy.HandleTask("fcfc/a", func(ctx context.Context, f *workflow.Flow) error { runs++; return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// The flow-completion transaction fails once after the terminal step was marked completed: the recovery
	// defer resets it and re-dispatches, so the flow still completes (no `running` orphan with all steps
	// terminal).
	e.injectFault(faultCompleteFlowCommit)
	if out := boundedRun(t, e, "fcfc/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(2, runs)
}

func TestFault_LeaseStaleWrite(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "flease/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("flease/g", g)
	var runs int
	proxy.HandleTask("flease/a", func(ctx context.Context, f *workflow.Flow) error { runs++; return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.SetTimeBudget(200 * time.Millisecond)
	e.leaseMargin = 100 * time.Millisecond // lease = budget+margin = 300ms, so it expires quickly
	e.RunInTest(t)

	// A's completion write carries a stale lease generation (a zombie): the fence rejects it (no-op), so the
	// step stays claimable and lease recovery re-runs it cleanly - a stale write never corrupts or
	// terminalizes the flow. The first dispatch's write is fenced out; once its lease lapses the poll
	// backstop resets the step and it re-runs (fault consumed) to a clean completion, so A ran twice.
	e.injectFault(faultKey(faultLeaseStaleWrite, "A"))
	fk, err := e.Create(ctx, "flease/g", nil, nil)
	assert.NoError(err)
	time.Sleep(400 * time.Millisecond) // > lease (300ms): the fenced dispatch ran, its lease has lapsed
	e.pollPendingSteps(ctx)            // the lease-recovery backstop
	waitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)
	assert.Equal(2, runs)
}

// --- Signal / doorbell faults (process-wide) ---

func TestFault_DropSignalStop(t *testing.T) {
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "fsig/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("fsig/g", g)
	proxy.HandleTask("fsig/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.awaitPollInterval = 20 * time.Millisecond // the backstop must fire fast for the test
	e.RunInTest(t)

	// The terminal wake is dropped, so Await cannot rely on the signal; it must return via its periodic
	// re-snapshot of the DB-committed stop.
	e.injectFault(faultDropSignalStop)
	if out := boundedRun(t, e, "fsig/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

func TestFault_DropDoorbell(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "fdoor/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("fdoor/g", g)
	proxy.HandleTask("fdoor/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// The create-time doorbell is dropped, so the entry step sits pending until the poll backstop rings the
	// local doorbell. Drive the backstop directly, then the flow completes.
	e.injectFault(faultDropDoorbell)
	fk, err := e.Create(ctx, "fdoor/g", nil, nil)
	assert.NoError(err)
	e.pollPendingSteps(ctx) // the backstop that recovers a lost doorbell
	waitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)
}

// --- Background-recovery faults ---

func TestFault_SubgraphReviveLost(t *testing.T) {
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

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// The child's revive of the parked caller is lost: the caller wedges running+parkedSubgraph with a
	// terminal child. The wedge sweep must re-drive the release. Arm, create, wait for the wedge, then
	// invoke the sweep with minAge 0 (bypassing the steady-state age guard) and the parent completes.
	e.injectFault(faultSubgraphReviveLost)
	fk, err := e.Create(ctx, "fsub/parent", nil, nil)
	assert.NoError(err)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)

	// Wait until the child has gone terminal (the caller is now wedged).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id<>0 AND status='"+workflow.StatusCompleted+"'").Scan(&n)
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0) // the sweep, no age guard
	waitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)
}

func TestFault_ReapMidTree(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "freap/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("freap/g", g)
	proxy.HandleTask("freap/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	shortenDeletion(e, time.Millisecond, time.Hour) // due immediately; the test drives reaps
	e.RunInTest(t)

	fk, err := e.Create(ctx, "freap/g", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	waitFlowStatus(t, e, fk, workflow.StatusCompleted, 5*time.Second)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	time.Sleep(5 * time.Millisecond) // let the 1ms window elapse

	// Reap aborts mid-tree (after steps, before flows) and rolls back: the tree is left intact, not a
	// half-deleted flow. The next reap pass (fault consumed) removes it cleanly.
	e.injectFault(faultReapMidTree)
	e.reapDueFlows(ctx)
	assert.Equal(1, shardFlowCount(t, e, shard)) // rolled back - still present

	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shard)) // clean removal
}

// --- Poll / refill clamp faults (process-wide) ---

func TestFault_RefillScanErr(t *testing.T) {
	assert := testarossa.For(t)
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "frefill/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("frefill/g", g)
	proxy.HandleTask("frefill/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// The refiller's band scan errors once. Instead of refilling empty and idling every worker forever, it
	// logs and shortens the next poll, so the flow is still dispatched and completes (just after a re-scan).
	e.injectFault(faultRefillScanErr)
	if out := boundedRun(t, e, "frefill/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

func TestFault_PollSizingErr(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// A sizing-query failure must clamp the next poll to pollErrorRetryInterval (re-poll soon) rather than
	// sleeping maxPollInterval on an unknown backlog. Drive one poll with the fault armed and assert the
	// scheduled wake is near-term, not minutes out.
	e.injectFault(faultPollSizingErr)
	e.pollPendingSteps(ctx)

	e.nextPollLock.Lock()
	delay := time.Until(e.nextPoll)
	e.nextPollLock.Unlock()
	assert.True(delay <= 3*time.Second, "expected a clamped near-term re-poll, got %s", delay)
}
