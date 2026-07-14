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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// shortPersistBackoff shrinks the retry window so a test does not sit out 1+2+4 seconds. It restores the
// production values on cleanup.
func shortPersistBackoff(t *testing.T) {
	t.Helper()
	saved := persistBackoff
	persistBackoff = []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}
	t.Cleanup(func() { persistBackoff = saved })
}

// persistFlow builds a one-task flow and counts how many times the task actually EXECUTES - which is the number
// the whole design turns on, because an execution is where side effects fire.
func persistFlow(t *testing.T, name string) (*Engine, *TestProxy, *atomic.Int32) {
	t.Helper()
	var runs atomic.Int32

	proxy := NewTestProxy()
	g := workflow.NewGraph("Persist")
	g.SetEndpoint("A", "p/"+name+"/a")
	g.AddTransition("A", workflow.END)
	testarossa.For(t).NoError(g.Validate())
	proxy.HandleGraph("p/"+name+"/wf", g)
	proxy.HandleTask("p/"+name+"/a", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		f.SetString("out", "done")
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)
	return e, proxy, &runs
}

// TestPersist_TransientWriteErrorIsAbsorbedWithoutReExecution is the payoff of retrying the WRITE rather than
// the task. The step-completion write fails once with a non-contention database error - a failover, a dropped
// connection, a momentary connection-limit rejection. The retry loop lands it on the next attempt, and the task
// is NOT re-executed: its side effects fire exactly once.
//
// Re-dispatching instead (which is what happens with no retry: the step is left `running` and lease recovery
// takes it) would have re-run the task. That is the difference this test exists to pin.
func TestPersist_TransientWriteErrorIsAbsorbedWithoutReExecution(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)
	shortPersistBackoff(t)

	e, _, runs := persistFlow(t, "transient")
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
	ctx := context.Background()
	assert := testarossa.For(t)
	shortPersistBackoff(t)

	e, _, runs := persistFlow(t, "permanent")
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
	assert.Equal(workflow.StatusFailed, status)
	assert.NotEqual("", errMsg, "the step records why it could not be persisted")

	// Give lease recovery a chance to prove it has nothing to recover.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(int32(1), runs.Load(), "nothing re-dispatches a terminalized step")
}

// TestPersist_DrainReleasesTheLeaseInsteadOfSleepingItOut pins the shutdown path. A worker sitting out a
// persistence backoff must notice the drain, hand the step back immediately (fenced), and let Shutdown proceed -
// rather than making the drain wait out a window nobody is watching, or holding the step until its lease lapses.
//
// The released step goes back to `pending` with an expired lease, so a peer (or this engine on restart) claims it
// at once. That DOES re-execute the task - it is the at-least-once contract, and it is what would have happened
// at lease expiry anyway, only sooner.
func TestPersist_DrainReleasesTheLeaseInsteadOfSleepingItOut(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	// A long backoff, so the worker is provably ASLEEP in the retry loop when the drain lands. If it did not
	// select on drainStop, Shutdown would block for this long.
	saved := persistBackoff
	persistBackoff = []time.Duration{30 * time.Second}
	t.Cleanup(func() { persistBackoff = saved })

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

	e := NewEngine()
	e.SetHost(proxy)
	assert.NoError(e.SetInTest(t.Name()))
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
