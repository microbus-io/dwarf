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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The tests in this file commit a transaction and then lose its acknowledgement: the server has the writes, and the
// engine is told the connection was lost. Each asserts what a lost reply may cost and nothing more - the operation's
// outcome as without the fault, each task run as often as its contract allows, one durable advancement per obligation
// (a single successor, a single convergence, a single child), no outcome that moves once it closed, and a structurally
// clean world after every recovery sweep has had its turn.

// newLostCommitEngine starts an engine on mux with persist's retry ladder shortened to milliseconds.
func newLostCommitEngine(t *testing.T, mux *hosts.Mux) (*Engine, sdkmetric.Reader) {
	t.Helper()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(mux)
	e.persistBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	reader := withManualReader(e)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	return e, reader
}

// assertFired fails the test when an armed fault was never consulted, so a test cannot pass without exercising the
// lost acknowledgement it is named for. It consumes the fault if it is still armed.
func assertFired(t *testing.T, e *Engine, fault string) {
	t.Helper()
	testarossa.For(t).False(e.diags.Seams().IsFault(fault), "the fault %s never fired", fault)
}

// sweepEveryDetector runs every background recovery detector once on every shard, the park detectors without their
// age guard, as a replica's recovery jobs would over time. A detector's error is logged by the detector; the
// assertions that follow read the rows.
func sweepEveryDetector(t *testing.T, e *Engine) {
	t.Helper()
	ctx := context.Background()
	e.recoverLeases(ctx)
	for _, shard := range e.db.Indices() {
		sh, err := e.db.Shard(shard)
		if !testarossa.For(t).NoError(err) {
			continue
		}
		db := sh.DB()
		_ = e.recoverOrphanedFlows(ctx, db, shard)
		_ = e.alarmStrandedFlows(ctx, db, shard)
		_ = e.recoverWedgedSubgraphParks(ctx, db, shard, 0)
		_ = e.recoverOrphanedSubgraphChildren(ctx, db, shard, 0)
	}
}

// assertSettledClean is the end of every test here, run once the workload has quiesced: it records every closed
// outcome, lets every recovery detector run, and requires the outcomes unchanged, nothing removed, the world clean and
// - given the engine's metric reader - the wedge alarm silent.
func assertSettledClean(t *testing.T, e *Engine, reader sdkmetric.Reader) {
	t.Helper()
	before := enginetest.CaptureOutcomes(t, e)
	sweepEveryDetector(t, e)
	removed := enginetest.AssertOutcomesKept(t, e, before)
	testarossa.For(t).Len(removed, 0, "a closed flow left the live tables")
	if reader == nil {
		enginetest.AssertInvariants(t, e)
		return
	}
	assertFaultRecoveryClean(t, e, reader)
}

// gate holds a task until it is opened, or until the task's context ends. Open is idempotent, so a test opens it in a
// defer as well as at the moment it means to.
type gate struct {
	ch   chan struct{}
	once sync.Once
}

func newGate() *gate { return &gate{ch: make(chan struct{})} }

func (g *gate) open() { g.once.Do(func() { close(g.ch) }) }

func (g *gate) wait(ctx context.Context) error {
	select {
	case <-g.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// countSteps counts a flow's step rows of one task.
func countSteps(t *testing.T, e *Engine, flowKey, taskName string) int {
	t.Helper()
	shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	testarossa.For(t).NoError(err)
	return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name=?", flowID, taskName)
}

// awaitChildFlow waits for the subgraph child of the flow to exist and returns its flow id.
func awaitChildFlow(t *testing.T, e *Engine, flowKey string) int {
	t.Helper()
	assert := testarossa.For(t)
	shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	var childID int
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
		childID = enginetest.CountRows(t, e, shard, "SELECT COALESCE(MAX(flow_id), 0) FROM dwarf_flows WHERE surgraph_flow_id=?", flowID)
		return childID > 0
	}), "the subgraph child was never created")
	return childID
}

// awaitBounded awaits a flow with a hang ceiling and fails the test now if it returns no outcome.
func awaitBounded(t *testing.T, e *Engine, flowKey string) *workflow.FlowOutcome {
	t.Helper()
	assert := testarossa.For(t)
	out, err := enginetest.BoundedAwait(t, e, flowKey)
	if !assert.NoError(err) || !assert.NotNil(out) {
		t.FailNow()
	}
	return out
}

// lostCommitLinear registers A -> B -> END under prefix, each task writing a constant and counting its runs. A waits on
// hold when one is given.
func lostCommitLinear(mux *hosts.Mux, prefix string, hold *gate) (a, b *atomic.Int32) {
	a, b = new(atomic.Int32), new(atomic.Int32)
	g := workflow.NewGraph("Linear")
	g.SetURL("A", prefix+"/a")
	g.SetURL("B", prefix+"/b")
	g.AddTransitionChain("A", "B", workflow.End)
	mux.HandleGraph(prefix+"/g", g)
	mux.HandleTask(prefix+"/a", func(ctx context.Context, f *workflow.Flow) error {
		if hold != nil {
			if err := hold.wait(ctx); err != nil {
				return err
			}
		}
		a.Add(1)
		f.SetString("a", "ran")
		return nil
	})
	mux.HandleTask(prefix+"/b", func(ctx context.Context, f *workflow.Flow) error {
		b.Add(1)
		f.SetString("b", "ran")
		return nil
	})
	return a, b
}

// TestLostCommit_Transition loses the acknowledgement of a linear transition: B was inserted and A's advancing flag
// cleared. Persist retries the transaction, and the retry must find A already advanced - not insert B again, and not
// hand A back to be run.
func TestLostCommit_Transition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	a, b := lostCommitLinear(mux, "lctransition", nil)
	e, reader := newLostCommitEngine(t, mux)

	fault := diagnostics.Join(diagnostics.FaultAdvanceCommitLost, "A")
	e.diags.Seams().Inject(fault)
	fk, out := batteryRun(t, e, "lctransition/g")
	assertFired(t, e, fault)
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal("ran", out.State.GetString("a"))
	assert.Equal("ran", out.State.GetString("b"))
	assert.Equal(int32(1), a.Load(), "A ran again after its transition committed")
	assert.Equal(int32(1), b.Load())
	assert.Equal(1, countSteps(t, e, fk, "B"), "the transition inserted its successor twice")
	assertSettledClean(t, e, reader)
}

// lostCommitCohort registers Seed -> forEach(items) Branch -> Join (fan-in) -> END, the branches appending their
// element to "got". Seed fans out over width elements; the branch of element 1 waits on hold.
func lostCommitCohort(mux *hosts.Mux, prefix string, width int, hold *gate) (branches, joins *atomic.Int32) {
	branches, joins = new(atomic.Int32), new(atomic.Int32)
	g := workflow.NewGraph("Cohort")
	g.SetURL("Seed", prefix+"/seed")
	g.SetURL("Branch", prefix+"/branch")
	g.SetURL("Join", prefix+"/join")
	g.SetFanIn("Join")
	g.SetReducer("got", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Branch", "items", "item")
	g.AddTransitionChain("Branch", "Join", workflow.End)
	mux.HandleGraph(prefix+"/g", g)
	mux.HandleTask(prefix+"/seed", func(ctx context.Context, f *workflow.Flow) error {
		items := make([]int, width)
		for i := range items {
			items[i] = i
		}
		return f.Set("items", items)
	})
	mux.HandleTask(prefix+"/branch", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("item") == 1 && hold != nil {
			if err := hold.wait(ctx); err != nil {
				return err
			}
		}
		branches.Add(1)
		return f.Set("got", []int{f.GetInt("item")})
	})
	mux.HandleTask(prefix+"/join", func(ctx context.Context, f *workflow.Flow) error {
		joins.Add(1)
		return nil
	})
	return branches, joins
}

// TestLostCommit_CohortArrival loses the acknowledgement of a cohort arrival, once for an arrival that leaves the
// cohort open and once for the arrival that resolves it and inserts the fan-in. The retry must count neither arrival a
// second time, nor converge the cohort twice.
func TestLostCommit_CohortArrival(t *testing.T) {
	t.Parallel()
	for _, resolving := range []bool{false, true} {
		name := "not_resolving"
		if resolving {
			name = "resolving"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := context.Background()
			mux := hosts.NewMux()
			hold := newGate()
			defer hold.open()
			branches, joins := lostCommitCohort(mux, "lcarrival", 2, hold)
			e, reader := newLostCommitEngine(t, mux)

			fault := diagnostics.Join(diagnostics.FaultAdvanceCommitLost, "Branch")
			if !resolving {
				e.diags.Seams().Inject(fault) // the first arrival, element 0's: element 1 is held
			}
			fk, err := e.Create(ctx, "lcarrival/g", nil, nil)
			assert.Must(assert.NoError(err))
			// Seed's dispatch and element 0's have returned, so the first arrival and its retry are behind us
			enginetest.AwaitVisitCount(t, e.diags.Seams(), 2, 10*time.Second, diagnostics.CheckpointClaimedDispatchDone)
			if resolving {
				e.diags.Seams().Inject(fault) // the second arrival, element 1's, resolves the cohort
			}
			hold.open()
			out := awaitBounded(t, e, fk)
			assertFired(t, e, fault)
			if assert.Equal(workflow.StatusCompleted, out.Status) {
				var got []int
				_, err := out.State.Get("got", &got)
				assert.NoError(err)
				assert.Equal([]int{0, 1}, got, "an arrival folded twice or was lost")
			}
			assert.Equal(int32(2), branches.Load(), "a branch ran again after its arrival committed")
			assert.Equal(int32(1), joins.Load(), "the cohort converged twice")
			assert.Equal(1, countSteps(t, e, fk, "Join"))
			shard, _, flowID, _, _ := keys.ParseFlowKey(fk)
			assert.Equal(2, enginetest.CountRows(t, e, shard, "SELECT cohort_arrivals FROM dwarf_steps WHERE flow_id=? AND task_name='Seed'", flowID),
				"an arrival was counted twice or lost")
			assertSettledClean(t, e, reader)
		})
	}
}

// TestLostCommit_FanInDirect loses the acknowledgement of an empty forEach's straight route to its fan-in. The retry
// must find the fan-in already inserted.
func TestLostCommit_FanInDirect(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	branches, joins := lostCommitCohort(mux, "lcfanindirect", 0, nil)
	e, reader := newLostCommitEngine(t, mux)

	fault := diagnostics.Join(diagnostics.FaultFanInDirectCommitLost, "Seed")
	e.diags.Seams().Inject(fault)
	fk, out := batteryRun(t, e, "lcfanindirect/g")
	assertFired(t, e, fault)
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(int32(0), branches.Load())
	assert.Equal(int32(1), joins.Load(), "the fan-in ran twice")
	assert.Equal(1, countSteps(t, e, fk, "Join"), "the fan-in was inserted twice")
	assertSettledClean(t, e, reader)
}

// lostCommitParent registers a parent Call -> END whose Call runs the child X -> END as a subgraph, counting both
// tasks' runs. X waits on hold when one is given and fails when childErr is set.
func lostCommitParent(mux *hosts.Mux, prefix string, hold *gate, childErr error) (calls, xs *atomic.Int32) {
	calls, xs = new(atomic.Int32), new(atomic.Int32)
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", prefix+"/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph(prefix+"/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("X", prefix+"/x")
	child.AddTransition("X", workflow.End)
	mux.HandleGraph(prefix+"/child", child)
	callChild := subgraphTask(prefix + "/child")
	mux.HandleTask(prefix+"/call", func(ctx context.Context, f *workflow.Flow) error {
		calls.Add(1)
		return callChild(ctx, f)
	})
	mux.HandleTask(prefix+"/x", func(ctx context.Context, f *workflow.Flow) error {
		if hold != nil {
			if err := hold.wait(ctx); err != nil {
				return err
			}
		}
		xs.Add(1)
		if childErr != nil {
			return childErr
		}
		f.SetString("x", "ran")
		return nil
	})
	return calls, xs
}

// TestLostCommit_FlowCompletion loses the acknowledgement of a flow's completion: the flow is completed and its final
// state frozen. Persist retries the transaction, which must find the flow already completed - and for a subgraph
// child, still revive its caller once, which the lost reply skipped.
func TestLostCommit_FlowCompletion(t *testing.T) {
	t.Parallel()

	t.Run("root", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		ctx := context.Background()
		mux := hosts.NewMux()
		hold := newGate()
		defer hold.open()
		g := workflow.NewGraph("Single")
		g.SetURL("A", "lccomplete/a")
		g.AddTransition("A", workflow.End)
		mux.HandleGraph("lccomplete/g", g)
		var a atomic.Int32
		mux.HandleTask("lccomplete/a", func(ctx context.Context, f *workflow.Flow) error {
			if err := hold.wait(ctx); err != nil {
				return err
			}
			a.Add(1)
			f.SetString("a", "ran")
			return nil
		})
		e, reader := newLostCommitEngine(t, mux)

		fk, err := e.Create(ctx, "lccomplete/g", nil, nil)
		assert.Must(assert.NoError(err))
		_, _, flowID, _, _ := keys.ParseFlowKey(fk)
		fault := diagnostics.Join(diagnostics.FaultCompleteFlowCommitLost, strconv.Itoa(flowID))
		e.diags.Seams().Inject(fault)
		hold.open()
		out := awaitBounded(t, e, fk)
		assertFired(t, e, fault)
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal("ran", out.State.GetString("a"))
		assert.Equal(int32(1), a.Load(), "the task ran again after its flow completed")
		assertSettledClean(t, e, reader)
	})

	t.Run("child", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		ctx := context.Background()
		mux := hosts.NewMux()
		hold := newGate()
		defer hold.open()
		calls, xs := lostCommitParent(mux, "lccompletechild", hold, nil)
		e, reader := newLostCommitEngine(t, mux)

		fk, err := e.Create(ctx, "lccompletechild/parent", nil, nil)
		assert.Must(assert.NoError(err))
		childID := awaitChildFlow(t, e, fk)
		fault := diagnostics.Join(diagnostics.FaultCompleteFlowCommitLost, strconv.Itoa(childID))
		e.diags.Seams().Inject(fault)
		hold.open()
		out := awaitBounded(t, e, fk)
		assertFired(t, e, fault)
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(int32(2), calls.Load(), "the caller ran other than to spawn and to take its child's result")
		assert.Equal(int32(1), xs.Load(), "the child's task ran again after its flow completed")
		shard, _, flowID, _, _ := keys.ParseFlowKey(fk)
		assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id=?", flowID))
		assertSettledClean(t, e, reader)
	})
}

// TestLostCommit_FailStep loses the acknowledgement of failStep's transaction: the step and its flow are failed, and
// for a subgraph child the failure is delivered to its parked caller. Nothing may run again, and the caller must take
// the failure once.
func TestLostCommit_FailStep(t *testing.T) {
	t.Parallel()

	t.Run("trunk", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		g := workflow.NewGraph("Failing")
		g.SetURL("A", "lcfail/a")
		g.AddTransition("A", workflow.End)
		mux.HandleGraph("lcfail/g", g)
		var a atomic.Int32
		mux.HandleTask("lcfail/a", func(ctx context.Context, f *workflow.Flow) error {
			a.Add(1)
			return errors.New("boom")
		})
		e, reader := newLostCommitEngine(t, mux)

		fault := diagnostics.Join(diagnostics.FaultFailStepCommitLost, "A")
		e.diags.Seams().Inject(fault)
		_, out := batteryRun(t, e, "lcfail/g")
		assertFired(t, e, fault)
		assert.Equal(workflow.StatusFailed, out.Status)
		assert.Contains(out.Error, "boom")
		assert.Equal(int32(1), a.Load(), "the failed task ran again")
		assertSettledClean(t, e, reader)
	})

	t.Run("child", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		calls, xs := lostCommitParent(mux, "lcfailchild", nil, errors.New("child boom"))
		e, reader := newLostCommitEngine(t, mux)

		fault := diagnostics.Join(diagnostics.FaultFailStepCommitLost, "X")
		e.diags.Seams().Inject(fault)
		fk, out := batteryRun(t, e, "lcfailchild/parent")
		assertFired(t, e, fault)
		assert.Equal(workflow.StatusFailed, out.Status)
		assert.Contains(out.Error, "child boom")
		assert.Equal(int32(1), xs.Load(), "the child's failed task ran again")
		assert.Equal(int32(2), calls.Load(), "the caller ran other than to spawn and to take its child's failure")
		shard, _, flowID, _, _ := keys.ParseFlowKey(fk)
		assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id=?", flowID))
		assertSettledClean(t, e, reader)
	})
}

// TestLostCommit_Revive loses the acknowledgement of the delivery reviving a completed child's parked caller: the
// caller is pending with the child's result. Its doorbell is skipped, so the shard's next cycle must find it, and the
// retry of the child's completion must not revive it a second time.
func TestLostCommit_Revive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	hold := newGate()
	defer hold.open()
	calls, xs := lostCommitParent(mux, "lcrevive", hold, nil)
	e, reader := newLostCommitEngine(t, mux)

	fk, err := e.Create(ctx, "lcrevive/parent", nil, nil)
	assert.Must(assert.NoError(err))
	childID := awaitChildFlow(t, e, fk)
	fault := diagnostics.Join(diagnostics.FaultReviveCommitLost, strconv.Itoa(childID))
	e.diags.Seams().Inject(fault)
	hold.open()
	out := awaitBounded(t, e, fk)
	assertFired(t, e, fault)
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(int32(2), calls.Load(), "the caller ran other than to spawn and to take its child's result")
	assert.Equal(int32(1), xs.Load())
	assertSettledClean(t, e, reader)
}

// TestLostCommit_DeliverError loses the acknowledgement of the wedge sweep's delivery of a failed child's error to its
// parked caller, whose own live delivery was lost first. The caller is re-armed with the error; a second sweep must find
// nothing to deliver, and the parent fails once.
func TestLostCommit_DeliverError(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	calls, xs := lostCommitParent(mux, "lcdelivererr", nil, errors.New("child boom"))
	e, _ := newLostCommitEngine(t, mux)

	e.diags.Seams().Inject(diagnostics.FaultDeliverFailureErr)
	fk, err := e.Create(ctx, "lcdelivererr/parent", nil, nil)
	assert.Must(assert.NoError(err))
	shard, _, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	childID := awaitChildFlow(t, e, fk)
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+workflow.StatusFailed+"'", childID) == 1
	}), "the child never failed")
	assertFired(t, e, diagnostics.FaultDeliverFailureErr)

	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	fault := diagnostics.Join(diagnostics.FaultDeliverErrorCommitLost, strconv.Itoa(childID))
	e.diags.Seams().Inject(fault)
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0)
	assertFired(t, e, fault)
	out := awaitBounded(t, e, fk)
	assert.Equal(workflow.StatusFailed, out.Status)
	assert.Contains(out.Error, "child boom")
	assert.Equal(int32(1), xs.Load())
	assert.Equal(int32(2), calls.Load(), "the caller ran other than to spawn and to take its child's failure")

	before := enginetest.CaptureOutcomes(t, e)
	sweepEveryDetector(t, e)
	assert.Len(enginetest.AssertOutcomesKept(t, e, before), 0)
	enginetest.AssertInvariants(t, e)
}

// TestLostCommit_Create loses the acknowledgement of a flow's creation: the flow and its entry step are in, its
// doorbell is not rung, and Create reports the lost connection - an uncertain outcome the caller cannot tell from a
// flow that was never made. The flow that was made must still run, once.
func TestLostCommit_Create(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	a, b := lostCommitLinear(mux, "lccreate", nil)
	e, reader := newLostCommitEngine(t, mux)

	fault := diagnostics.Join(diagnostics.FaultCreateCommitLost, "lccreate/g")
	e.diags.Seams().Inject(fault)
	_, err := e.Create(ctx, "lccreate/g", nil, nil)
	assertFired(t, e, fault)
	assert.Error(err)
	assert.True(sequel.IsConnectionError(err), "a lost acknowledgement reads as a lost connection: %v", err)

	shard := e.db.Indices()[0]
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	var flowID, hour int
	var token string
	assert.Must(assert.NoError(sh.DB().QueryRowContext(ctx, "SELECT flow_id, created_hour, flow_token FROM dwarf_flows").Scan(&flowID, &hour, &token)))
	out := awaitBounded(t, e, keys.NewFlowKey(shard, hour, flowID, token))
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(int32(1), a.Load())
	assert.Equal(int32(1), b.Load())
	assertSettledClean(t, e, reader)
}

// TestLostCommit_Resume loses the acknowledgement of a Resume: the leaf is pending with the resume data and the flow
// running, and Resume reports the lost connection. The flow must run on once; a retried Resume finds nothing to resume.
func TestLostCommit_Resume(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	g := workflow.NewGraph("Resumable")
	g.SetURL("A", "lcresume/a")
	g.SetURL("B", "lcresume/b")
	g.AddTransitionChain("A", "B", workflow.End)
	mux.HandleGraph("lcresume/g", g)
	var a, b atomic.Int32
	mux.HandleTask("lcresume/a", func(ctx context.Context, f *workflow.Flow) error {
		a.Add(1)
		var answer struct{ Answer string }
		yield, err := f.Interrupt(nil, &answer)
		if yield || err != nil {
			return err
		}
		f.SetString("answer", answer.Answer)
		return nil
	})
	mux.HandleTask("lcresume/b", func(ctx context.Context, f *workflow.Flow) error {
		b.Add(1)
		return nil
	})
	e, reader := newLostCommitEngine(t, mux)

	fk := createInterruptedFlow(t, e, "lcresume/g")
	_, _, flowID, _, _ := keys.ParseFlowKey(fk)
	fault := diagnostics.Join(diagnostics.FaultResumeCommitLost, strconv.Itoa(flowID))
	e.diags.Seams().Inject(fault)
	err := e.Resume(ctx, fk, map[string]any{"Answer": "yes"})
	assertFired(t, e, fault)
	assert.True(sequel.IsConnectionError(err), "a lost acknowledgement reads as a lost connection: %v", err)
	out := awaitBounded(t, e, fk)
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal("yes", out.State.GetString("answer"))
	assert.Equal(int32(2), a.Load(), "A ran other than to interrupt and to take its resume")
	assert.Equal(int32(1), b.Load())
	assert.Equal(http.StatusConflict, errors.StatusCode(e.Resume(ctx, fk, map[string]any{"Answer": "again"})),
		"a retried Resume resumed a flow that was no longer interrupted")
	assertSettledClean(t, e, reader)
}

// TestLostCommit_Terminate loses the acknowledgement of a Terminate: the flow and its running step are terminated, and
// Terminate reports the lost connection. An Await must still see the flow terminated, a retried Terminate must find it
// closed, and the step's late completion must change nothing.
func TestLostCommit_Terminate(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	hold := newGate()
	defer hold.open()
	a, b := lostCommitLinear(mux, "lcterminate", hold)
	e, reader := newLostCommitEngine(t, mux)

	fk, err := e.Create(ctx, "lcterminate/g", nil, nil)
	assert.Must(assert.NoError(err))
	shard, _, flowID, _, _ := keys.ParseFlowKey(fk)
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND status='"+workflow.StatusRunning+"'", flowID) == 1
	}), "A never started")
	fault := diagnostics.Join(diagnostics.FaultTerminateCommitLost, strconv.Itoa(flowID))
	e.diags.Seams().Inject(fault)
	err = e.Terminate(ctx, fk, "stop")
	assertFired(t, e, fault)
	assert.True(sequel.IsConnectionError(err), "a lost acknowledgement reads as a lost connection: %v", err)
	out := awaitBounded(t, e, fk)
	assert.Equal(workflow.StatusTerminated, out.Status)
	assert.Equal("stop", out.TerminateReason)
	assert.NoError(e.Terminate(ctx, fk, "again"), "a retried Terminate of a terminated flow")

	dispatched := e.diags.Seams().Visits(diagnostics.CheckpointClaimedDispatchDone)
	hold.open()
	enginetest.AwaitVisitCount(t, e.diags.Seams(), dispatched+1, 10*time.Second, diagnostics.CheckpointClaimedDispatchDone)
	assert.Equal(int32(1), a.Load())
	assert.Equal(int32(0), b.Load(), "a terminated flow advanced")
	out = awaitBounded(t, e, fk)
	assert.Equal(workflow.StatusTerminated, out.Status)
	assert.Equal("stop", out.TerminateReason, "the retried Terminate rewrote the closed flow's reason")
	assertSettledClean(t, e, reader)
}

// TestLostCommit_Fork loses the acknowledgement of a Fork: the clone is in and live, and Fork reports the lost
// connection. The clone must run, once; a retried Fork is a second fork, as a retried Create is a second flow.
func TestLostCommit_Fork(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	a, b := lostCommitLinear(mux, "lcfork", nil)
	e, reader := newLostCommitEngine(t, mux)

	fk, out := batteryRun(t, e, "lcfork/g")
	assert.Must(assert.Equal(workflow.StatusCompleted, out.Status))
	history, err := e.History(ctx, fk)
	assert.Must(assert.NoError(err))
	var stepB string
	for _, s := range history {
		if s.TaskName == "B" {
			stepB = s.StepKey
		}
	}
	assert.Must(assert.NotEqual("", stepB))

	shard, _, flowID, _, _ := keys.ParseFlowKey(fk)
	fault := diagnostics.Join(diagnostics.FaultForkCommitLost, strconv.Itoa(flowID))
	e.diags.Seams().Inject(fault)
	_, err = e.Fork(ctx, stepB, nil)
	assertFired(t, e, fault)
	assert.True(sequel.IsConnectionError(err), "a lost acknowledgement reads as a lost connection: %v", err)

	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	var forkID, hour int
	var token string
	assert.Must(assert.NoError(sh.DB().QueryRowContext(ctx, "SELECT flow_id, created_hour, flow_token FROM dwarf_flows WHERE forked_from_step<>0").Scan(&forkID, &hour, &token)))
	out = awaitBounded(t, e, keys.NewFlowKey(shard, hour, forkID, token))
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(int32(1), a.Load(), "the fork re-ran a step before its fork point")
	assert.Equal(int32(2), b.Load(), "the fork ran its fork step other than once")
	assertSettledClean(t, e, reader)
}
