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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// registerCountedGate is registerGate with a count of the task's runs and the attempt its last run saw.
func registerCountedGate(mux *hosts.Mux, prefix string, runs, lastAttempt *atomic.Int32) {
	g := workflow.NewGraph("Gate")
	g.SetURL("Gate", prefix+"/gate")
	g.AddTransition("Gate", workflow.End)
	mux.HandleGraph(prefix+"/g", g)
	mux.HandleTask(prefix+"/gate", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		lastAttempt.Store(int32(f.Attempt()))
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return f.Set("done", true)
	})
}

// flowRows returns a one-step flow's shard database, its flow id and its step id.
func flowRows(t *testing.T, e *Engine, fk string) (*sequel.DB, int, int) {
	t.Helper()
	assert := testarossa.For(t)
	shard, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	var stepID int
	assert.NoError(db.QueryRowContext(t.Context(), "SELECT step_id FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&stepID))
	return db, flowID, stepID
}

// setFlowPayloadVer stands in for a flow created by a build of another format.
func setFlowPayloadVer(t *testing.T, db *sequel.DB, flowID, ver int) {
	t.Helper()
	assert := testarossa.For(t)
	_, err := db.ExecContext(t.Context(), "UPDATE dwarf_flows SET payload_ver=? WHERE flow_id=?", ver, flowID)
	assert.NoError(err)
}

// TestPayloadVer_NewerFlowIsDeferredUnrun pins the dispatch half: a step of a flow in a format newer than this build
// reads goes back to pending, backed off, without its task running and without touching attempt; once a replica that
// reads it is in place (simulated by lowering the flow's payload_ver), it runs as if nothing happened.
func TestPayloadVer_NewerFlowIsDeferredUnrun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs, lastAttempt atomic.Int32
	mux := hosts.NewMux()
	registerCountedGate(mux, "newerpayload", &runs, &lastAttempt)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk := createInterruptedFlow(t, e, "newerpayload/g")
	assert.Equal(int32(1), runs.Load())
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, flowID, stepID := flowRows(t, e, fk)
	setFlowPayloadVer(t, db, flowID, payloadVer+1)

	dispatched := e.seams.Waiter(engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(stepID)))
	assert.NoError(e.Resume(ctx, fk, nil))
	select {
	case <-dispatched:
	case <-time.After(10 * time.Second):
		t.Fatal("the resumed step was never dispatched")
	}

	var status string
	var deferrals, attempt int
	var dueInMs float64
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT status, payload_deferrals, attempt, DATE_DIFF_MILLIS(not_before, NOW_UTC()) FROM dwarf_steps WHERE step_id=?", stepID,
	).Scan(&status, &deferrals, &attempt, &dueInMs))
	assert.Equal(workflow.StatusPending, status)
	assert.Equal(1, deferrals)
	assert.Equal(0, attempt, "a deferral must not count as an attempt")
	assert.True(dueInMs > 0, "the deferral must back off, due in %vms", dueInMs)
	assert.Equal(int32(1), runs.Load(), "a deferred step's task must not run")

	// The upgrade: a replica that reads the flow claims the step.
	setFlowPayloadVer(t, db, flowID, payloadVer)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=NOW_UTC() WHERE step_id=?", stepID)
	assert.NoError(err)
	awaitCtx, cancel := context.WithTimeout(ctx, 15*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status, out.Error)
		assert.True(out.State.GetBool("done"))
	}
	assert.Equal(int32(2), runs.Load())
	assert.Equal(int32(0), lastAttempt.Load(), "the task must see no attempts from the deferrals")
}

// TestPayloadVer_DeferralBoundFailsTheStep pins the bound: a step deferred maxPayloadDeferrals times fails, with an
// error naming the format, rather than being deferred forever. Failing the flow reads its tails for the final state,
// which refuses the format too, so the flow ends with an empty one.
func TestPayloadVer_DeferralBoundFailsTheStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs, lastAttempt atomic.Int32
	mux := hosts.NewMux()
	registerCountedGate(mux, "payloadbound", &runs, &lastAttempt)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	// A flow with state of its own, so an empty final state shows the tails were refused rather than empty.
	fk, err := e.Create(ctx, "payloadbound/g", map[string]any{"kept": 1}, nil)
	assert.NoError(err)
	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusInterrupted, out.Status)
	db, flowID, stepID := flowRows(t, e, fk)
	setFlowPayloadVer(t, db, flowID, payloadVer+1)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET payload_deferrals=? WHERE step_id=?", maxPayloadDeferrals, stepID)
	assert.NoError(err)

	assert.NoError(e.Resume(ctx, fk, nil))
	awaitCtx, cancel := context.WithTimeout(ctx, 15*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err = e.Await(awaitCtx, fk)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusFailed, out.Status)
		assert.Contains(out.Error, "payload is in format "+strconv.Itoa(payloadVer+1))
		assert.Equal(0, out.State.Len())
	}
	assert.Equal(int32(1), runs.Load(), "a step past the bound fails without running")
}

// TestPayloadVer_ReadersRefuseNewerFlows pins the reads that are not a dispatch: Snapshot and Step of a newer flow
// error rather than decode its rows, and a Terminate still stops it - with an empty final state, since the merge
// refuses the rows rather than misreading them.
func TestPayloadVer_ReadersRefuseNewerFlows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs, lastAttempt atomic.Int32
	mux := hosts.NewMux()
	registerCountedGate(mux, "payloadreads", &runs, &lastAttempt)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	// A flow with state of its own, so an empty final state shows the tails were refused rather than empty.
	fk, err := e.Create(ctx, "payloadreads/g", map[string]any{"kept": 1}, nil)
	assert.NoError(err)
	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusInterrupted, out.Status)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, flowID, stepID := flowRows(t, e, fk)
	var stepToken string
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_token FROM dwarf_steps WHERE step_id=?", stepID).Scan(&stepToken))
	setFlowPayloadVer(t, db, flowID, payloadVer+1)

	_, err = e.Snapshot(ctx, fk)
	if assert.Error(err) {
		assert.Contains(err.Error(), "payload is in format")
		assert.Equal(http.StatusNotImplemented, errors.StatusCode(err))
	}
	_, err = e.Step(ctx, keys.New(shard, stepID, stepToken))
	if assert.Error(err) {
		assert.Contains(err.Error(), "payload is in format")
	}

	assert.NoError(e.Terminate(ctx, fk, "upgrade pending"))
	out, err = e.Snapshot(ctx, fk)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusTerminated, out.Status)
		assert.Equal(0, out.State.Len())
	}
}

// TestPayloadVer_ForkRefusesANewerFlow pins that Fork refuses a flow it cannot read before it decodes any of it.
func TestPayloadVer_ForkRefusesANewerFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("ForkNewer")
	g.SetURL("A", "forknewer/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("forknewer/g", g)
	mux.HandleTask("forknewer/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, out, err := e.Run(ctx, "forknewer/g", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, out.Status)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, flowID, stepID := flowRows(t, e, fk)
	var stepToken string
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_token FROM dwarf_steps WHERE step_id=?", stepID).Scan(&stepToken))
	setFlowPayloadVer(t, db, flowID, payloadVer+1)
	// A refs value this build cannot parse stands in for the newer format, so the error shows which check ran first.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET state_refs=? WHERE step_id=?", []byte(`{"doc":{"v":9}}`), stepID)
	assert.NoError(err)

	_, err = e.Fork(ctx, keys.New(shard, stepID, stepToken), nil)
	if assert.Error(err) {
		assert.Contains(err.Error(), "payload is in format")
	}
}

// TestPayloadVer_ForkRefusesANewerSubgraph pins that Fork checks every flow it clones, not only the fork step's: a
// subgraph child is created in its creator's format, so a tree can mix formats, and cloning the child copies rows
// and remaps refs this build may not read.
func TestPayloadVer_ForkRefusesANewerSubgraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	parent := workflow.NewGraph("ForkNewerParent")
	parent.SetURL("Call", "forknewersub/call")
	parent.SetURL("Done", "forknewersub/done")
	parent.AddTransitionChain("Call", "Done", workflow.End)
	mux.HandleGraph("forknewersub/parent", parent)
	child := workflow.NewGraph("ForkNewerChild")
	child.SetURL("X", "forknewersub/x")
	child.AddTransition("X", workflow.End)
	mux.HandleGraph("forknewersub/child", child)
	mux.HandleTask("forknewersub/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("forknewersub/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("forknewersub/done", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("forknewersub/x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, out, err := e.Run(ctx, "forknewersub/parent", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, out.Status, out.Error)
	shard, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	var childID, doneID int
	var doneToken string
	assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id=?", flowID).Scan(&childID))
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id, step_token FROM dwarf_steps WHERE flow_id=? AND task_name='Done'", flowID).Scan(&doneID, &doneToken))
	setFlowPayloadVer(t, db, childID, payloadVer+1)

	// Forking at Done keeps Call, whose child is cloned whole.
	_, err = e.Fork(ctx, keys.New(shard, doneID, doneToken), nil)
	if assert.Error(err) {
		assert.Contains(err.Error(), "payload is in format")
	}
}

// TestPayloadVer_CreateStampsForkCopies pins where a flow's format comes from: Create stamps the format this build
// writes, and Fork - which copies the origin's rows - keeps the origin's. The origin is set to an older format (0)
// this build still reads, so a copy is distinguishable from a fresh stamp.
func TestPayloadVer_CreateStampsForkCopies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("ForkCopies")
	g.SetURL("A", "forkcopies/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("forkcopies/g", g)
	mux.HandleTask("forkcopies/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, out, err := e.Run(ctx, "forkcopies/g", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, out.Status)
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, flowID, stepID := flowRows(t, e, fk)
	var ver int
	assert.NoError(db.QueryRowContext(ctx, "SELECT payload_ver FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&ver))
	assert.Equal(payloadVer, ver)

	setFlowPayloadVer(t, db, flowID, 0)
	var stepToken string
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_token FROM dwarf_steps WHERE step_id=?", stepID).Scan(&stepToken))
	forkKey, err := e.Fork(ctx, keys.New(shard, stepID, stepToken), nil)
	assert.NoError(err)
	_, forkID, _, err := keys.ParseFlowKey(forkKey)
	assert.NoError(err)
	assert.NoError(db.QueryRowContext(ctx, "SELECT payload_ver FROM dwarf_flows WHERE flow_id=?", forkID).Scan(&ver))
	assert.Equal(0, ver, "a fork keeps its origin's format")
}

// TestPayloadVer_DeferralSchedule pins the arithmetic the bound is sized on: a doubling backoff from a second to
// a minute, adding up to at least an hour over the deferrals that precede the failure.
func TestPayloadVer_DeferralSchedule(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	assert.Equal(time.Second, payloadDeferralBackoff(0))
	assert.Equal(32*time.Second, payloadDeferralBackoff(5))
	assert.Equal(time.Minute, payloadDeferralBackoff(6))
	assert.Equal(time.Minute, payloadDeferralBackoff(maxPayloadDeferrals))
	var total time.Duration
	for n := range maxPayloadDeferrals {
		total += payloadDeferralBackoff(n)
	}
	assert.True(total >= time.Hour, "deferrals add up to %v", total)
	assert.True(total < time.Hour+time.Minute, "deferrals add up to %v", total)
}
