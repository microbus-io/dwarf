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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestRestartSurvival is the headline durability proof: a flow survives the death of
// its engine. "Restart" is modelled as a genuine full Shutdown of engine 1 followed by a fresh Startup of
// engine 2 against the *same on-disk database* (a file-backed SQLite DSN, so the data outlives the gap with
// no overlap between the engines). Each variant rests the flow in a non-running state before the restart
// (pending, sleeping, parked-on-a-sleeping-child, interrupted) - the states a clean Shutdown leaves intact,
// since Shutdown drains in-flight workers by contract (a hard kill of a running worker is the lease-recovery
// path, pinned separately by TestLeaseRecovery_EndToEnd and the leasefence tests). Every variant asserts
// exactly-once execution per step (shared counters across both engines), the correct final state, and a clean
// invariant sweep.
func TestRestartSurvival(t *testing.T) {
	ctx := context.Background()

	// mkEngine builds an engine bound to a file DSN under dir (so two engines resolve to the same on-disk DB)
	// with its own worker count. Not test mode: SetShard uses the DSN directly and migrations run at Startup
	// (idempotent, so engine 2 re-opening the migrated DB is a no-op).
	mkEngine := func(t *testing.T, proxy *TestProxy, dir string, workers int) *Engine {
		e := NewEngine()
		e.SetHost(proxy)
		testarossa.For(t).NoError(e.SetShard(ShardSpec{Index: 1, DSN: fmt.Sprintf("file:%s/db%%d.sqlite?_pragma=busy_timeout(5000)", dir)}))
		testarossa.For(t).NoError(e.SetWorkers(workers))
		return e
	}

	// waitFor polls a COUNT(*) query on e's shard 1 until it returns >0 or the deadline passes.
	waitFor := func(t *testing.T, e *Engine, query string, args ...any) bool {
		t.Helper()
		db, err := e.db.Shard(1)
		if !testarossa.For(t).NoError(err) {
			return false
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			if err := db.QueryRowContext(ctx, query, args...).Scan(&n); err == nil && n > 0 {
				return true
			}
			time.Sleep(10 * time.Millisecond)
		}
		return false
	}

	t.Run("pending_step_lost_doorbell", func(t *testing.T) {
		assert := testarossa.For(t)
		dir := t.TempDir()
		var aCalls atomic.Int64

		proxy := NewTestProxy()
		g := workflow.NewGraph("R1")
		g.SetEndpoint("A", "r1/a")
		g.AddTransition("A", workflow.END)
		assert.NoError(g.Validate())
		proxy.HandleGraph("r1/g", g)
		proxy.HandleTask("r1/a", func(ctx context.Context, f *workflow.Flow) error { aCalls.Add(1); return nil })

		// Engine 1 has ZERO workers: Create inserts a pending entry step and rings a doorbell nobody consumes,
		// then dies. The step must be recovered by engine 2's poll with no doorbell of its own.
		e1 := mkEngine(t, proxy, dir, 0)
		assert.NoError(e1.Startup(ctx))
		flowKey, err := e1.Create(ctx, "r1/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.NoError(e1.Shutdown(ctx))
		assert.Equal(int64(0), aCalls.Load()) // engine 1 never ran it (no workers)

		e2 := mkEngine(t, proxy, dir, 2)
		assert.NoError(e2.Startup(ctx))
		defer func() { assert.NoError(e2.Shutdown(ctx)) }()

		awaitAndAssertComplete(t, e2, flowKey)
		assert.Equal(int64(1), aCalls.Load()) // executed exactly once, on engine 2
		assertInvariants(t, e2)
	})

	t.Run("sleeping_step", func(t *testing.T) {
		assert := testarossa.For(t)
		dir := t.TempDir()
		var aCalls, bCalls atomic.Int64

		proxy := NewTestProxy()
		g := workflow.NewGraph("R2")
		g.SetEndpoint("A", "r2/a")
		g.SetEndpoint("B", "r2/b")
		g.AddTransition("A", "B")
		g.AddTransition("B", workflow.END)
		assert.NoError(g.Validate())
		proxy.HandleGraph("r2/g", g)
		// A sleeps before the transition, so successor B is inserted pending with a future not_before - a
		// durable timer that must fire on engine 2 after the restart.
		proxy.HandleTask("r2/a", func(ctx context.Context, f *workflow.Flow) error {
			aCalls.Add(1)
			f.Sleep(1500 * time.Millisecond)
			return nil
		})
		proxy.HandleTask("r2/b", func(ctx context.Context, f *workflow.Flow) error { bCalls.Add(1); return nil })

		e1 := mkEngine(t, proxy, dir, 2)
		assert.NoError(e1.Startup(ctx))
		flowKey, err := e1.Create(ctx, "r2/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
		_, flowID, _, err := keys.ParseFlowKey(flowKey)
		if !assert.NoError(err) {
			return
		}
		// Wait until the sleeping successor B exists (A has run and transitioned), then restart mid-sleep.
		if !assert.True(waitFor(t, e1, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='B'", flowID)) {
			return
		}
		assert.NoError(e1.Shutdown(ctx))

		e2 := mkEngine(t, proxy, dir, 2)
		assert.NoError(e2.Startup(ctx))
		defer func() { assert.NoError(e2.Shutdown(ctx)) }()

		awaitAndAssertComplete(t, e2, flowKey)
		assert.Equal(int64(1), aCalls.Load()) // A ran once on engine 1
		assert.Equal(int64(1), bCalls.Load()) // B ran once on engine 2 after the sleep elapsed
		assertInvariants(t, e2)
	})

	t.Run("parked_on_sleeping_subgraph", func(t *testing.T) {
		assert := testarossa.For(t)
		dir := t.TempDir()
		var caCalls, cbCalls, parentDone atomic.Int64

		proxy := NewTestProxy()
		parent := workflow.NewGraph("R3P")
		parent.SetEndpoint("P0", "r3/p0")
		parent.AddTransition("P0", workflow.END)
		assert.NoError(parent.Validate())
		proxy.HandleGraph("r3/parent", parent)
		proxy.HandleTask("r3/p0", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph("r3/child", nil, nil)
			if yield || err != nil {
				return err
			}
			parentDone.Add(1)
			return nil
		})

		child := workflow.NewGraph("R3C")
		child.SetEndpoint("CA", "r3/ca")
		child.SetEndpoint("CB", "r3/cb")
		child.AddTransition("CA", "CB")
		child.AddTransition("CB", workflow.END)
		assert.NoError(child.Validate())
		proxy.HandleGraph("r3/child", child)
		// CA sleeps before transitioning, so the child rests with CB pending (future not_before) and the parent
		// stays parked - a clean rest state to restart through.
		proxy.HandleTask("r3/ca", func(ctx context.Context, f *workflow.Flow) error {
			caCalls.Add(1)
			f.Sleep(1500 * time.Millisecond)
			return nil
		})
		proxy.HandleTask("r3/cb", func(ctx context.Context, f *workflow.Flow) error { cbCalls.Add(1); return nil })

		e1 := mkEngine(t, proxy, dir, 2)
		assert.NoError(e1.Startup(ctx))
		flowKey, err := e1.Create(ctx, "r3/parent", nil, nil)
		if !assert.NoError(err) {
			return
		}
		// Wait until the child's sleeping CB exists (parent parked, child spawned, CA slept), then restart.
		if !assert.True(waitFor(t, e1, "SELECT COUNT(*) FROM dwarf_steps WHERE task_name='CB'")) {
			return
		}
		assert.NoError(e1.Shutdown(ctx))

		e2 := mkEngine(t, proxy, dir, 2)
		assert.NoError(e2.Startup(ctx))
		defer func() { assert.NoError(e2.Shutdown(ctx)) }()

		awaitAndAssertComplete(t, e2, flowKey)
		assert.Equal(int64(1), caCalls.Load())    // CA ran once on engine 1
		assert.Equal(int64(1), cbCalls.Load())    // CB ran once on engine 2 after the sleep
		assert.Equal(int64(1), parentDone.Load()) // the parent was revived and completed on engine 2
		assertInvariants(t, e2)
	})

	t.Run("interrupted_resume", func(t *testing.T) {
		assert := testarossa.For(t)
		dir := t.TempDir()
		var gateCalls, doneCalls atomic.Int64

		proxy := NewTestProxy()
		g := workflow.NewGraph("R4")
		g.SetEndpoint("Gate", "r4/gate")
		g.SetEndpoint("Done", "r4/done")
		g.AddTransition("Gate", "Done")
		g.AddTransition("Done", workflow.END)
		assert.NoError(g.Validate())
		proxy.HandleGraph("r4/g", g)
		proxy.HandleTask("r4/gate", func(ctx context.Context, f *workflow.Flow) error {
			gateCalls.Add(1)
			var out map[string]any
			yield, err := f.Interrupt(nil, &out)
			if yield || err != nil {
				return err
			}
			f.SetString("resumed", "yes")
			return nil
		})
		proxy.HandleTask("r4/done", func(ctx context.Context, f *workflow.Flow) error { doneCalls.Add(1); return nil })

		e1 := mkEngine(t, proxy, dir, 2)
		assert.NoError(e1.Startup(ctx))
		flowKey, err := e1.Create(ctx, "r4/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
		out, err := boundedAwait(t, e1, flowKey)
		if !assert.NoError(err) || !assert.NotNil(out) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, out.Status)
		assert.NoError(e1.Shutdown(ctx))

		e2 := mkEngine(t, proxy, dir, 2)
		assert.NoError(e2.Startup(ctx))
		defer func() { assert.NoError(e2.Shutdown(ctx)) }()

		// Resume on the fresh engine drives the flow to completion.
		assert.NoError(e2.Resume(ctx, flowKey, nil))
		final := awaitAndAssertComplete(t, e2, flowKey)
		if final != nil {
			assert.Equal("yes", final.State["resumed"])
		}
		assert.Equal(int64(2), gateCalls.Load()) // Gate: once to interrupt (e1), once resumed (e2)
		assert.Equal(int64(1), doneCalls.Load()) // Done ran once on engine 2
		assertInvariants(t, e2)
	})
}

// boundedAwait awaits a flow with a 30s ctx bound so a wedge fails the test instead of hanging.
func boundedAwait(t *testing.T, e *Engine, flowKey string) (*workflow.FlowOutcome, error) {
	t.Helper()
	awaitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return e.Await(awaitCtx, flowKey)
}

// awaitAndAssertComplete awaits a flow (bounded) and asserts it completed, returning the outcome.
func awaitAndAssertComplete(t *testing.T, e *Engine, flowKey string) *workflow.FlowOutcome {
	t.Helper()
	assert := testarossa.For(t)
	out, err := boundedAwait(t, e, flowKey)
	if !assert.NoError(err) || !assert.NotNil(out) {
		return nil
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	return out
}
