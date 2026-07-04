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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// waitFlowStatus polls the flow row until it reaches want, failing the test on timeout. Used where the
// settled status is reached after a transient one (e.g. interrupted -> failed) so Await is unsuitable.
func waitFlowStatus(t *testing.T, e *Engine, flowKey, want string, timeout time.Duration) {
	t.Helper()
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	testarossa.For(t).NoError(err)
	db, err := e.db.Shard(shardNum)
	testarossa.For(t).NoError(err)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var s string
		db.QueryRowContext(context.Background(), "SELECT status FROM dwarf_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&s)
		if strings.TrimSpace(s) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("flow %s did not reach status %q within %s", flowKey, want, timeout)
}

// shardFlowCount returns the number of flows on a shard.
func shardFlowCount(t *testing.T, e *Engine, shardNum int) int {
	t.Helper()
	db, err := e.db.Shard(shardNum)
	testarossa.For(t).NoError(err)
	var n int
	db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_flows").Scan(&n)
	return n
}

// waitFlowDeleted polls until the flow's row (and steps) are gone, failing the test on timeout.
func waitFlowDeleted(t *testing.T, e *Engine, flowKey string, timeout time.Duration) {
	t.Helper()
	shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
	testarossa.For(t).NoError(err)
	db, err := e.db.Shard(shardNum)
	testarossa.For(t).NoError(err)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&n)
		if n == 0 {
			var steps int
			db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&steps)
			testarossa.For(t).Equal(0, steps) // steps deleted with the flow
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("flow %s was not deleted within %s", flowKey, timeout)
}

// shortenDeletion overrides the deletion grace / reaper cadence for a test and restores them after. Must be
// called before RunInTest (the reaper reads reapInterval at startup; completeFlow reads deletionGrace at
// completion). Engine tests run sequentially, so mutating these package vars is safe.
func shortenDeletion(t *testing.T, grace, interval time.Duration) {
	t.Helper()
	og, oi := deletionGrace, reapInterval
	deletionGrace, reapInterval = grace, interval
	t.Cleanup(func() { deletionGrace, reapInterval = og, oi })
}

// TestDeleteOnCompletion_ReaperDeletesOnSuccess asserts a flow created with DeleteOnCompletion is removed (row
// and steps) by the background reaper once its grace window elapses.
func TestDeleteOnCompletion_ReaperDeletesOnSuccess(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	shortenDeletion(t, time.Millisecond, 20*time.Millisecond)

	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "doc/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/solo", g)
	proxy.HandleTask("doc/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "doc/solo", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)

	waitFlowDeleted(t, e, fk, 5*time.Second)
}

// TestDeleteOnCompletion_OutcomeObservableThenReaped asserts the new deferred-deletion contract: a disposable
// flow stays `completed` for the grace window so Await/Snapshot serve its outcome, then the reaper removes it
// and reads 404. A long reapInterval keeps the background reaper out; the test forces one reap pass itself.
func TestDeleteOnCompletion_OutcomeObservableThenReaped(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	shortenDeletion(t, time.Millisecond, time.Hour) // due ~immediately, but only the manual reap fires

	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "doc/aw-a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/await", g)
	proxy.HandleTask("doc/aw-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "doc/await", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)

	// The outcome is observable during the grace window (Await returns completed, not 404).
	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(true, out.State["done"])
	}

	// Force a reap pass (the flow is due), then it is gone: Await/Snapshot 404, uniformly.
	time.Sleep(5 * time.Millisecond)
	e.reapDueFlows(ctx)

	_, err = e.Await(ctx, fk)
	assert.Equal(404, errors.StatusCode(err))
	_, err = e.Snapshot(ctx, fk)
	assert.Equal(404, errors.StatusCode(err))
}

// TestDeleteOnCompletion_RunReturnsOutcome asserts Run on a disposable flow returns the completed outcome
// (observable during the grace window), not a 404.
func TestDeleteOnCompletion_RunReturnsOutcome(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	shortenDeletion(t, time.Millisecond, time.Hour)

	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "doc/run-a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/run", g)
	proxy.HandleTask("doc/run-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	_, out, err := e.Run(ctx, "doc/run", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(true, out.State["done"])
	}
}

// TestPurge_ReaperRemovesTree pins the operator path: Purge marks a root (it does not delete inline), and the
// reaper then removes the root AND its subgraph child (keyed on root_flow_id) - the no-orphan guarantee. A
// long reapInterval keeps the background reaper out; the test forces one reap pass.
func TestPurge_ReaperRemovesTree(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	shortenDeletion(t, time.Millisecond, time.Hour)

	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Run", "pr/run")
	parent.AddTransition("Run", workflow.END)
	proxy.HandleGraph("pr/parent", parent)
	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("X", "pr/x")
	inner.AddTransition("X", workflow.END)
	proxy.HandleGraph("pr/inner", inner)
	proxy.HandleTask("pr/x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("pr/run", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("pr/inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, _, err := e.Run(ctx, "pr/parent", nil, nil)
	assert.NoError(err)
	shardNum, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	assert.Equal(2, shardFlowCount(t, e, shardNum)) // root + subgraph child

	n, err := e.Purge(ctx, workflow.Query{WorkflowURL: "pr/parent"})
	assert.NoError(err)
	assert.Equal(1, n) // one root marked

	time.Sleep(5 * time.Millisecond) // let the 1ms delete_after_ms elapse (in-memory SQLite is sub-ms)
	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shardNum)) // root and child both removed - no orphan
}

// TestDeleteOnCompletion_KeepsFailedFlow asserts a failed flow is retained even with DeleteOnCompletion set
// - failures stay available for diagnosis / Fork.
func TestDeleteOnCompletion_KeepsFailedFlow(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Failing")
	g.SetEndpoint("A", "doc/fail-a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/failing", g)
	proxy.HandleTask("doc/fail-a", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("boom")
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "doc/failing", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	waitFlowStatus(t, e, fk, workflow.StatusFailed, 5*time.Second)

	// The failed flow row is still present (not auto-deleted).
	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	var n int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&n)
	assert.Equal(1, n)
}

// TestDeleteOnCompletion_ReaperCascadesSubgraph asserts that when a disposable root flow completes, the reaper
// removes it AND its subgraph descendants (keyed on root_flow_id; the child carries no flag of its own).
func TestDeleteOnCompletion_ReaperCascadesSubgraph(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	shortenDeletion(t, time.Millisecond, 20*time.Millisecond)

	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("A", "doc/sg-a")
	parent.SetEndpoint("RunInner", "doc/sg-run-inner")
	parent.AddTransitionChain("A", "RunInner", workflow.END)
	proxy.HandleGraph("doc/sg-parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("X", "doc/sg-x")
	inner.AddTransition("X", workflow.END)
	proxy.HandleGraph("doc/sg-inner", inner)

	proxy.HandleTask("doc/sg-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("doc/sg-x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("doc/sg-run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("doc/sg-inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "doc/sg-parent", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	shardNum, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	// Root completes; the reaper then removes it plus the subgraph child - no flows remain.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if shardFlowCount(t, e, shardNum) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(0, shardFlowCount(t, e, shardNum))
}

// TestDeleteOnCompletion_OutcomeObservableUnderConcurrency hammers many disposable flows concurrently to pin
// the deferred-deletion contract under contention: every Await must return the completed outcome cleanly (the
// flow stays observable during its grace window). A long reapInterval keeps the reaper from removing any flow
// mid-test, so a missing/errored outcome would signal a real defect (torn completion write, lost outcome).
func TestDeleteOnCompletion_OutcomeObservableUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	shortenDeletion(t, deletionGrace, time.Hour) // reaper effectively off for the test's duration

	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "doc/conc-a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/conc", g)
	proxy.HandleTask("doc/conc-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.SetWorkers(8)
	e.RunInTest(t)

	const workers = 8
	const perWorker = 25
	var wg sync.WaitGroup
	var bad atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				fk, err := e.Create(ctx, "doc/conc", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
				if err != nil {
					bad.Add(1)
					continue
				}
				out, err := e.Await(ctx, fk)
				if err != nil || out == nil || out.Status != workflow.StatusCompleted || out.State["done"] != true {
					bad.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	testarossa.For(t).Equal(int64(0), bad.Load(),
		"every disposable Await must return the completed outcome during the grace window")
}
