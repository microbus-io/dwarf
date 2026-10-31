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
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// shardFlowCount returns the number of flows on a shard.
func shardFlowCount(t *testing.T, e *Engine, shardNum int) int {
	t.Helper()
	assert := testarossa.For(t)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	n := -1
	assert.NoError(db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_flows").Scan(&n))
	return n
}

// waitFlowDeleted polls until the flow's row (and steps) are gone, failing the test on timeout. Call it once the
// flow has stopped (enginetest.AwaitFlowStatus), so the timeout covers only the reaper's tick behind it, not the
// flow's run as well - a run on a loaded server is unbounded.
//
// The timeout is a "did it hang" ceiling, not a timing contract, so it stretches under -race the same way the
// shared enginetest helpers do.
func waitFlowDeleted(t *testing.T, e *Engine, flowKey string, timeout time.Duration) {
	t.Helper()
	assert := testarossa.For(t)
	shardNum, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	deadline := time.Now().Add(timeout * enginetest.TimeoutScale())
	for time.Now().Before(deadline) {
		n := -1 // a failed count must not read as deleted
		assert.NoError(db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&n))
		if n == 0 {
			steps := -1
			assert.NoError(db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&steps))
			assert.Equal(0, steps) // steps deleted with the flow
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("flow %s was not deleted within %s", flowKey, timeout*enginetest.TimeoutScale())
}

// shortenDeletion overrides the engine's deletion grace / reaper cadence. Must be called after
// NewEngineUnderTest and before Startup (the reaper reads reapInterval at Startup; completeFlow reads deletionGrace at
// completion). Per-engine fields, so there is nothing to restore.
func shortenDeletion(e *Engine, grace, interval time.Duration) {
	e.deletionGrace = grace
	e.reapInterval = interval
}

// TestDeleteOnCompletion_ReaperDeletesOnSuccess asserts a flow created with DeleteOnCompletion is removed (row
// and steps) by the background reaper once its grace window elapses.
func TestDeleteOnCompletion_ReaperDeletesOnSuccess(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "doc/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("doc/solo", g)
	mux.HandleTask("doc/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, 20*time.Millisecond)
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "doc/solo", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)

	// The run is unbounded on a loaded server, so the minute bounds only a hang; the reap behind it is one tick.
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	waitFlowDeleted(t, e, fk, 5*time.Second)
}

// TestDeleteOnCompletion_OutcomeObservableThenReaped asserts the new deferred-deletion contract: a disposable
// flow stays `completed` for the grace window so Await/Snapshot serve its outcome, then the reaper removes it
// and reads 404. A long reapInterval keeps the background reaper out; the test forces one reap pass itself.
func TestDeleteOnCompletion_OutcomeObservableThenReaped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "doc/aw-a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("doc/await", g)
	mux.HandleTask("doc/aw-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, time.Hour) // due ~immediately, but only the manual reap fires
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "doc/await", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)

	// The outcome is observable during the grace window (Await returns completed, not 404).
	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(true, stateVal(out.State, "done"))
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
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "doc/run-a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("doc/run", g)
	mux.HandleTask("doc/run-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, time.Hour)
	assert.NoError(e.Startup(t.Context()))

	_, out, err := e.Run(ctx, "doc/run", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(true, stateVal(out.State, "done"))
	}
}

// TestPurge_ReaperRemovesTree pins the operator path: Purge marks a root (it does not delete inline), and the
// reaper then removes the root AND its subgraph child (keyed on root_flow_id) - the no-orphan guarantee. A
// long reapInterval keeps the background reaper out; the test forces one reap pass.
func TestPurge_ReaperRemovesTree(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Run", "pr/run")
	parent.AddTransition("Run", workflow.End)
	mux.HandleGraph("pr/parent", parent)
	inner := workflow.NewGraph("Inner")
	inner.SetURL("X", "pr/x")
	inner.AddTransition("X", workflow.End)
	mux.HandleGraph("pr/inner", inner)
	mux.HandleTask("pr/x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("pr/run", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("pr/inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, time.Hour)
	assert.NoError(e.Startup(t.Context()))

	fk, _, err := e.Run(ctx, "pr/parent", nil, nil)
	assert.NoError(err)
	shardNum, _, _, _, err := keys.ParseFlowKey(fk)
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
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Failing")
	g.SetURL("A", "doc/fail-a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("doc/failing", g)
	mux.HandleTask("doc/fail-a", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("boom")
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "doc/failing", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusFailed, time.Minute) // the minute bounds only a hang

	// The failed flow is never scheduled for deletion, so no reaper pass - now or later - can remove it.
	shardNum, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	assert.Equal(1, enginetest.CountRows(t, e, shardNum, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND delete_after_ms=0", flowID))
}

// TestDeleteOnCompletion_ReaperCascadesSubgraph asserts that when a disposable root flow completes, the reaper
// removes it AND its subgraph descendants (keyed on root_flow_id; the child carries no flag of its own). A
// long reapInterval keeps the background reaper out; the test forces one reap pass once the root completes.
func TestDeleteOnCompletion_ReaperCascadesSubgraph(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("A", "doc/sg-a")
	parent.SetURL("RunInner", "doc/sg-run-inner")
	parent.AddTransitionChain("A", "RunInner", workflow.End)
	mux.HandleGraph("doc/sg-parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetURL("X", "doc/sg-x")
	inner.AddTransition("X", workflow.End)
	mux.HandleGraph("doc/sg-inner", inner)

	mux.HandleTask("doc/sg-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("doc/sg-x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("doc/sg-run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("doc/sg-inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, time.Hour) // due ~immediately, but only the manual reap fires
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "doc/sg-parent", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	shardNum, _, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	// The tree's run is four dispatches and a subgraph spawn and revive, which a loaded server stretches past
	// any short ceiling, so the minute bounds only a hang.
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	assert.Equal(2, shardFlowCount(t, e, shardNum)) // root + subgraph child
	// Only the root is stamped; the child goes with it because the reap keys on root_flow_id.
	assert.Equal(1, enginetest.CountRows(t, e, shardNum, "SELECT COUNT(*) FROM dwarf_flows WHERE delete_after_ms>0"))

	time.Sleep(5 * time.Millisecond) // let the 1ms delete_after_ms elapse on the database clock
	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shardNum)) // root and child both removed - no orphan
}

// TestDeleteOnCompletion_HistoryOfAChildHidesWithItsRoot pins that History of a subgraph child's key 404s during
// its root's deletion window, as History of the root does: only the root is stamped, so the child's own row
// would let its history through.
func TestDeleteOnCompletion_HistoryOfAChildHidesWithItsRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("RunInner", "doc/hc-run-inner")
	parent.AddTransition("RunInner", workflow.End)
	mux.HandleGraph("doc/hc-parent", parent)
	inner := workflow.NewGraph("Inner")
	inner.SetURL("X", "doc/hc-x")
	inner.AddTransition("X", workflow.End)
	mux.HandleGraph("doc/hc-inner", inner)
	mux.HandleTask("doc/hc-x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("doc/hc-run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		_, err := f.Subgraph("doc/hc-inner", map[string]any{}, &out)
		return err
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Hour, time.Hour) // the grace window outlasts the test
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "doc/hc-parent", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	shardNum, _, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)

	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	var childID, childHour int
	var childToken string
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT flow_id, created_hour, flow_token FROM dwarf_flows WHERE surgraph_flow_id>0").Scan(&childID, &childHour, &childToken))
	childKey := keys.New(shardNum, childHour, childID, childToken)

	_, err = e.History(ctx, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	_, err = e.History(ctx, childKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}

// TestDeleteOnCompletion_OutcomeObservableUnderConcurrency hammers many disposable flows concurrently to pin
// the deferred-deletion contract under contention: every Await must return the completed outcome cleanly (the
// flow stays observable during its grace window). A long reapInterval keeps the reaper from removing any flow
// mid-test, so a missing/errored outcome would signal a real defect (torn completion write, lost outcome).
func TestDeleteOnCompletion_OutcomeObservableUnderConcurrency(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "doc/conc-a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("doc/conc", g)
	mux.HandleTask("doc/conc-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.SetWorkers(8)
	// Keep grace at the production default and the reaper effectively off, so no flow is removed mid-test
	// and a missing/errored outcome would signal a real defect.
	shortenDeletion(e, time.Minute, time.Hour)
	assert.NoError(e.Startup(t.Context()))

	const workers = 8
	const perWorker = 25
	var wg sync.WaitGroup
	var bad atomic.Int64
	for range workers {
		wg.Go(func() {
			for range perWorker {
				fk, err := e.Create(ctx, "doc/conc", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
				if err != nil {
					bad.Add(1)
					continue
				}
				out, err := e.Await(ctx, fk)
				if err != nil || out == nil || out.Status != workflow.StatusCompleted || stateVal(out.State, "done") != true {
					bad.Add(1)
				}
			}
		})
	}
	wg.Wait()
	assert.Equal(int64(0), bad.Load(),
		"every disposable Await must return the completed outcome during the grace window")
}
