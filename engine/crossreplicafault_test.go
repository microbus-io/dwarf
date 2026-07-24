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

// Cross-replica fault tests: fault injection is per-engine, which is exactly what's needed to test hand-off
// between replicas. Two engines share one database; a fault deliberately breaks a path on one replica and the
// test proves the other replica's backstop covers it.
//   - a lost terminal wake on the executing replica is backstopped by a remote awaiter's poll;
//   - a step a replica claimed but never completed (a fenced/zombie write, then the replica "crashes") is
//     recovered and re-dispatched to a clean completion by a peer replica's lease recovery.
package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCrossReplica_LostTerminalWake_AwaiterPolls pins that when the replica that runs a flow's final step has
// its terminal wake fully dropped (FaultDropSignalStop drops both the local waiter wake AND the peer
// statusChange broadcast), an Await on a *different* replica still returns - via its own periodic
// re-snapshot, the only wake path left once the signal is gone. The cross-replica analog of
// TestFault_DropSignalStop.
func TestCrossReplica_LostTerminalWake_AwaiterPolls(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	// A one-step graph. The awaiter (0 workers) must never execute it; the worker does.
	buildProxy := func(onRun func()) *TestProxy {
		p := NewTestProxy()
		g := workflow.NewGraph("Wake")
		g.SetEndpoint("W", "xrwake/w")
		g.AddTransition("W", workflow.END)
		assert.NoError(g.Validate())
		p.HandleGraph("xrwake/g", g)
		p.HandleTask("xrwake/w", func(ctx context.Context, f *workflow.Flow) error {
			onRun()
			return nil
		})
		return p
	}

	dsn := "file:xrwake%d?mode=memory&cache=shared"
	proxyAwaiter := buildProxy(func() { t.Error("the 0-worker awaiter must never execute the task") })
	var workerRuns atomic.Int64
	proxyWorker := buildProxy(func() { workerRuns.Add(1) })

	awaiter := NewEngine()
	awaiter.SetHost(proxyAwaiter)
	assert.NoError(awaiter.SetShard(ShardSpec{Index: 1, DSN: dsn}))
	assert.NoError(awaiter.SetWorkers(0))
	awaiter.awaitPollInterval = 100 * time.Millisecond // the backstop must be prompt for the test

	worker := NewEngine()
	worker.SetHost(proxyWorker)
	assert.NoError(worker.SetShard(ShardSpec{Index: 1, DSN: dsn}))
	assert.NoError(worker.SetWorkers(2))

	proxyAwaiter.AddPeer(worker) // awaiter's create-doorbell reaches the worker
	proxyWorker.AddPeer(awaiter) // worker's (dropped) terminal wake would reach the awaiter

	assert.NoError(awaiter.Startup(ctx))
	t.Cleanup(func() { awaiter.Shutdown(ctx) })
	assert.NoError(worker.Startup(ctx))
	t.Cleanup(func() { worker.Shutdown(ctx) })

	// Break the worker's terminal wake for the rest of the test: every signalStop on the worker (local notify
	// + peer broadcast) delivers nothing, so the awaiter can learn the outcome only by re-snapshotting the DB.
	worker.seams.InjectN(1<<20, FaultDropSignalStop)

	fk, err := awaiter.Create(ctx, "xrwake/g", nil, nil)
	if !assert.NoError(err) {
		return
	}

	// The only path by which this Await can return is the awaiter's own poll backstop - no signal survives.
	out, err := enginetest.BoundedAwait(t, awaiter, fk)
	if assert.NoError(err) && assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int64(1), workerRuns.Load()) // the worker replica executed it exactly once
}

// TestCrossReplica_ClaimedStepRecoveredByPeer pins cross-replica lease recovery: replica A claims and runs a
// step but its completion write is fenced out (FaultLeaseStaleWrite - a zombie/late write), leaving the step
// `running` under A's lease; A then "crashes" (Shutdown). Replica B's lease recovery resets the expired lease
// and re-dispatches the step to a clean completion. The task body runs on both replicas (execution is
// at-least-once), but the persisted outcome is state-correct and the invariant sweep is clean - the property
// that matters for at-least-once + idempotent tasks, proven across a replica boundary.
func TestCrossReplica_ClaimedStepRecoveredByPeer(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	dir := t.TempDir() // a file DB so the flow survives replica A's shutdown

	var runs atomic.Int64
	buildProxy := func() *TestProxy {
		p := NewTestProxy()
		g := workflow.NewGraph("XRRec")
		g.SetEndpoint("A", "xrrec/a")
		g.AddTransition("A", workflow.END)
		assert.NoError(g.Validate())
		p.HandleGraph("xrrec/g", g)
		// Idempotent: writes a constant (not an increment), so a re-run yields the same final_state.
		p.HandleTask("xrrec/a", func(ctx context.Context, f *workflow.Flow) error {
			runs.Add(1)
			f.SetString("done", "yes")
			return nil
		})
		return p
	}
	dsn := fmt.Sprintf("file:%s/db%%d.sqlite?_pragma=busy_timeout(5000)", dir)

	// Replica A: claims and executes the step, but its completion write is fenced (stale lease_seq), so the
	// step stays `running`. A short budget makes the lease expire quickly for B to recover.
	repA := NewEngine()
	repA.SetHost(buildProxy())
	assert.NoError(repA.SetShard(ShardSpec{Index: 1, DSN: dsn}))
	assert.NoError(repA.SetWorkers(2))
	assert.NoError(repA.SetTimeBudget(300 * time.Millisecond))
	repA.leaseMargin = 100 * time.Millisecond // lease = budget+margin = 400ms
	assert.NoError(repA.Startup(ctx))

	repA.seams.Inject(FaultLeaseStaleWrite, "A")
	fk, err := repA.Create(ctx, "xrrec/g", nil, nil)
	if !assert.NoError(err) {
		repA.Shutdown(ctx)
		return
	}

	// Wait until A has executed the task (its write is then fenced), then "crash" A. Shutdown drains A's
	// worker, so the fenced completion write has landed and the step rests `running` under A's lease.
	deadline := time.Now().Add(10 * time.Second)
	for runs.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.NoError(repA.Shutdown(ctx))
	assert.Equal(int64(1), runs.Load()) // A ran it once; the write was fenced

	// Replica B joins on the same DB. Its lease recovery is what will re-drive the step.
	repB := NewEngine()
	repB.SetHost(buildProxy())
	assert.NoError(repB.SetShard(ShardSpec{Index: 1, DSN: dsn}))
	assert.NoError(repB.SetWorkers(2))
	assert.NoError(repB.Startup(ctx))
	t.Cleanup(func() { repB.Shutdown(ctx) })

	// While A's lease is still valid (just claimed), the flow is stranded `running` - B cannot yet recover a
	// live lease, so this read is stable.
	dbB, err := repB.db.Shard(1)
	if assert.NoError(err) {
		var status string
		assert.NoError(dbB.QueryRowContext(ctx, "SELECT status FROM dwarf_flows").Scan(&status))
		assert.Equal(workflow.StatusRunning, status)
	}

	// Wait past A's 400ms lease, then drive B's lease-recovery poll; B resets the expired lease to pending and
	// re-dispatches the step to completion.
	time.Sleep(500 * time.Millisecond)
	repB.pollPendingSteps(ctx) // B's lease-recovery backstop

	out := enginetest.AwaitAndAssertComplete(t, repB, fk)
	if assert.NotNil(out) {
		assert.Equal("yes", out.State["done"]) // state-correct despite the fenced first attempt
	}
	assert.Equal(int64(2), runs.Load())  // ran once on A (fenced) + once on B (recovery): at-least-once
	enginetest.AssertInvariants(t, repB) // the recovered world is structurally clean
}
