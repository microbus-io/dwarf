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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// shortPersistBackoff is the retry schedule tests use so they do not sit out the production 1+2+4 seconds.
// Set on the engine (e.persistBackoff = shortPersistBackoff) before Startup - it is a read-only value, not
// a mutable global, so parallel tests each shorten their own engine without racing.
var shortPersistBackoff = []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}

// TestPersist_TransientWriteErrorIsAbsorbedWithoutReExecution is the payoff of retrying the WRITE rather than
// the task. The step-completion write fails once with a non-contention database error - a failover, a dropped
// connection, a momentary connection-limit rejection. The retry loop lands it on the next attempt, and the task
// is NOT re-executed: its side effects fire exactly once.
//
// Re-dispatching instead (which is what happens with no retry: the step is left `running` and lease recovery
// takes it) would have re-run the task. That is the difference this test exists to pin.
func TestPersist_TransientWriteErrorIsAbsorbedWithoutReExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	// A one-task flow whose task counts its executions - the number the whole design turns on, since an
	// execution is where side effects fire.
	var runs atomic.Int32
	proxy := NewTestProxy()
	g := workflow.NewGraph("Persist")
	g.SetEndpoint("A", "p/transient/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("p/transient/wf", g)
	proxy.HandleTask("p/transient/a", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		f.SetString("out", "done")
		return nil
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	e.persistBackoff = shortPersistBackoff
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	e.seams.InjectN(1, faultPersistErr, "A") // ONE failing attempt, then the database is fine again

	_, outcome, err := e.Run(ctx, "p/transient/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, "a transient write error must not fail the flow")
	assert.Equal("done", outcome.State["out"])
	assert.Equal(int32(1), runs.Load(), "the WRITE is retried, never the task - the task must run exactly once")
}

// TestPersist_PermanentWriteErrorFailsTheStepInsteadOfLoopingForever is the bug this closes.
//
// A write that will NEVER land (an unstorable payload, a value past a column limit, a constraint violation) used
// to leave the step `running` with error=” and attempt=0 - reading as perfectly healthy - while lease recovery
// re-dispatched it every `budget + leaseMargin`, RE-EXECUTING THE TASK and re-firing its side effects, forever.
// Silent and eternal: detectOrphanedFlows cannot see it, because a non-terminal step does exist.
//
// Now the retries exhaust and the classifier asks the database itself: the CLEAN write (failStep - a status and
// an error message, none of the payload) lands, which proves the database is reachable and therefore that the
// PAYLOAD is at fault. The failure is permanent, so the step fails, naming the driver's error - and the task has
// run exactly ONCE.
func TestPersist_PermanentWriteErrorFailsTheStepInsteadOfLoopingForever(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs atomic.Int32
	proxy := NewTestProxy()
	g := workflow.NewGraph("Persist")
	g.SetEndpoint("A", "p/permanent/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("p/permanent/wf", g)
	proxy.HandleTask("p/permanent/a", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		f.SetString("out", "done")
		return nil
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	e.persistBackoff = shortPersistBackoff
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	e.seams.InjectN(1000, faultPersistErr, "A") // every attempt fails: the payload, not the database

	_, outcome, err := e.Run(ctx, "p/permanent/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status, "a write that will never land must terminalize the flow, not loop")
	assert.Contains(outcome.Error, faultPersistErr, "the flow's error must name the driver's actual failure")

	// The whole point: ONE execution. Every extra run here is a side effect fired again.
	assert.Equal(int32(1), runs.Load(), "the task must execute exactly once, not once per lease expiry forever")

	// And it is genuinely terminal - not a step left `running` for lease recovery to pick up again.
	db, err := e.db.Shard(1)
	assert.NoError(err)
	var status string
	var errMsg string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, error FROM dwarf_steps WHERE task_name='A'").Scan(&status, &errMsg))
	// `status` is CHAR(16), which Postgres and SQL Server blank-pad on retrieval while SQLite/MySQL do not,
	// so a raw column read is trimmed at the boundary - the same convention the engine itself follows.
	assert.Equal(workflow.StatusFailed, strings.TrimSpace(status))
	assert.NotEqual("", errMsg, "the step records why it could not be persisted")

	// Give lease recovery a chance to prove it has nothing to recover.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(int32(1), runs.Load(), "nothing re-dispatches a terminalized step")
}

// TestPersist_LeaseExtensionStatusGuard pins that persist's lease-extension UPDATE is guarded on
// `status IN ('running','completed')` - the two states a worker legitimately holds a step in - so it extends
// the lease of a step we still own but NEVER stamps a future lease_expires onto one that lease recovery has
// already reset to `pending`. Recovery resets an expired step running->pending WITHOUT bumping lease_seq, so
// the generation fence alone still matches it; a `pending` row with a future lease_expires is un-claimable
// (every claim/sizing predicate needs lease_expires<=NOW) and cannot wake the timer (which schedules only on a
// `running` future lease_expires), so a step claimable in 30s could sleep up to maxPollInterval (5m).
//
// Both directions are pinned, because the guard must be neither too wide (fence the pending reset) nor too
// narrow (running-only would wrongly fence the transition retry, which runs while the step is `completed`).
func TestPersist_LeaseExtensionStatusGuard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// setup inserts one running flow (flow_id=1) and its single step (step_id=1) in the given status under
	// generation 5, with not_before/lease_expires far in the future so the engine's own candidate scan (a
	// pending step needs both <= NOW) leaves it alone for the test.
	setup := func(t *testing.T, status string) (*Engine, *sequel.DB) {
		assert := testarossa.For(t)
		e := NewEngineUnderTest(t)
		e.SetHost(NewTestProxy())
		e.persistBackoff = shortPersistBackoff
		if err := e.Startup(t.Context()); err != nil {
			t.Fatal(err)
		}
		db, err := e.db.Shard(1)
		assert.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			"ftok", "u", "W", []byte("{}"), workflow.StatusRunning, 1, 1, 1000,
		)
		assert.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, lease_seq, not_before, lease_expires)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000), DATE_ADD_MILLIS(NOW_UTC(), 999000))",
			1, 1, "stok", "T", "u", status, 1000, 5,
		)
		assert.NoError(err)
		return e, db
	}
	leaseMsOut := func(t *testing.T, db *sequel.DB) float64 {
		var ms float64
		testarossa.For(t).NoError(db.QueryRowContext(ctx, "SELECT DATE_DIFF_MILLIS(lease_expires, NOW_UTC()) FROM dwarf_steps WHERE step_id=1").Scan(&ms))
		return ms
	}

	// A step recovery reset to `pending` (same generation) is no longer ours: the extension must match zero rows,
	// persist must return errPersistFenced without starting the retry loop, and lease_expires must be untouched.
	t.Run("pending_is_fenced_and_lease_untouched", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusPending)

		before := leaseMsOut(t, db)
		assert.True(before > 100000)

		var writeCalls int
		err := e.persist(ctx, db, 1, 1, 5, func() error {
			writeCalls++
			return errors.New("injected non-contention write error")
		})
		assert.True(errors.Is(err, errPersistFenced), "a recovery-reset pending step must fence the extension, got: %v", err)
		assert.Equal(1, writeCalls, "the write runs once; the retry loop must not start after the fence")
		assert.True(leaseMsOut(t, db) > 100000, "lease_expires must be untouched on the reclaimed pending step (was %.0fms out)", before)
	})

	// A `completed` step whose transition write is retrying IS still ours (that is the second persist site): the
	// extension must land, the retry loop must run, and lease_expires must move out to ~persistLeaseExtensionMs.
	t.Run("completed_is_extended_not_fenced", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusCompleted)

		var writeCalls int
		err := e.persist(ctx, db, 1, 1, 5, func() error {
			writeCalls++
			return errors.New("injected non-contention write error")
		})
		assert.False(errors.Is(err, errPersistFenced), "a completed step we own (transition retry) must NOT be fenced")
		assert.True(writeCalls > 1, "the retry loop must run for a step we still own, got %d write(s)", writeCalls)
		after := leaseMsOut(t, db)
		assert.True(after > 20000 && after < 60000, "lease_expires must be extended to ~persistLeaseExtensionMs (30s), got %.0fms", after)
	})
}

// TestPersist_DrainReleasesTheLeaseInsteadOfSleepingItOut pins the shutdown path. A worker sitting out a
// persistence backoff must notice the drain, hand the step back immediately (fenced), and let Shutdown proceed -
// rather than making the drain wait out a window nobody is watching, or holding the step until its lease lapses.
//
// The released step goes back to `pending` with an expired lease, so a peer (or this engine on restart) claims it
// at once. That DOES re-execute the task - it is the at-least-once contract, and it is what would have happened
// at lease expiry anyway, only sooner.
func TestPersist_DrainReleasesTheLeaseInsteadOfSleepingItOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs atomic.Int32
	proxy := NewTestProxy()
	g := workflow.NewGraph("Drain")
	g.SetEndpoint("A", "p/drain/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("p/drain/wf", g)
	proxy.HandleTask("p/drain/a", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		return nil
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	// A long backoff, so the worker is provably ASLEEP in the retry loop when the drain lands. If it did not
	// select on drainStop, Shutdown would block for this long.
	e.persistBackoff = []time.Duration{30 * time.Second}
	assert.NoError(e.Startup(ctx))
	e.seams.InjectN(1000, faultPersistErr, "A")

	_, err := e.Create(ctx, "p/drain/wf", nil, nil)
	assert.NoError(err)

	// Wait until the task has run and the worker is therefore inside the retry backoff.
	deadline := time.Now().Add(5 * time.Second)
	for runs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	assert.Equal(int32(1), runs.Load())

	start := time.Now()
	assert.NoError(e.Shutdown(ctx))
	assert.True(time.Since(start) < 10*time.Second,
		"a worker asleep in a persistence backoff must wake on the drain, not sleep it out (Shutdown took %v)", time.Since(start))
}
