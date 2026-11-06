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

package fixtures

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphCohortFailWaitflow is the completing-last-arriver companion of TestSubgraphErrorWaitflow: a
// subgraph child with an internal fan-out where one branch fails first and the OTHER branch's completion
// resolves the cohort (cohort_failures > 0), failing the child via the transition path rather than failStep.
// That path too must raise signalStop for the child key as failed, and an Await already blocked on the
// (read-only) child key must then return the child's failure rather than hang.
func TestSubgraphCohortFailWaitflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	parent := workflow.NewGraph("Parent")
	parent.SetURL("RunInner", "subgraphcohortfailwait.verify:0/run-inner")
	parent.AddTransition("RunInner", workflow.End)
	mux.HandleGraph("subgraphcohortfailwait.verify:0/parent", parent)

	// Inner: Split fans out to {Boom, Slow}, both converging on Join. Boom fails immediately (no onError);
	// Slow blocks until released, so its completion is the last arrival that resolves the failed cohort.
	inner := workflow.NewGraph("Inner")
	inner.SetURL("Split", "subgraphcohortfailwait.verify:0/split")
	inner.SetURL("Boom", "subgraphcohortfailwait.verify:0/boom")
	inner.SetURL("Slow", "subgraphcohortfailwait.verify:0/slow")
	inner.SetURL("Join", "subgraphcohortfailwait.verify:0/join")
	inner.AddTransition("Split", "Boom")
	inner.AddTransition("Split", "Slow")
	inner.AddTransition("Boom", "Join")
	inner.AddTransition("Slow", "Join")
	inner.AddTransition("Join", workflow.End)
	inner.SetFanIn("Join")
	mux.HandleGraph("subgraphcohortfailwait.verify:0/inner", inner)

	// Slow is held until the cohort-failing arrival is wanted. The release is deferred below the Shutdown defer
	// so it unwinds first: an early return otherwise leaves Slow holding a worker the drain waits on forever.
	release := make(chan struct{})
	releaseSlow := sync.OnceFunc(func() { close(release) })
	defer releaseSlow()
	mux.HandleTask("subgraphcohortfailwait.verify:0/split", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// Every execution of Boom reports its child key and a waiter on that dispatch finishing, armed here, before
	// it returns, so the dispatch cannot finish unobserved. A dispatch ends after failStep commits the branch
	// failure - or after a recovery reset rewinds Boom to pending, in which case the next execution reports again.
	type boomed struct {
		child string
		done  <-chan struct{}
	}
	boomCh := make(chan boomed, 16)
	mux.HandleTask("subgraphcohortfailwait.verify:0/boom", func(ctx context.Context, f *workflow.Flow) error {
		shard, _, _, stepID, _, err := keys.ParseStepKey(f.StepKey())
		if err != nil {
			return err
		}
		done := engineseams.Seams(eng).Waiter(engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(stepID)))
		select {
		case boomCh <- boomed{child: f.FlowKey(), done: done}: // a subgraph child sees its own (child) flow key
		default:
		}
		return errors.New("branch boom", http.StatusInternalServerError)
	})
	mux.HandleTask("subgraphcohortfailwait.verify:0/slow", func(ctx context.Context, f *workflow.Flow) error {
		<-release // completes last, resolving the cohort with cohort_failures>0 (the transition path)
		return nil
	})
	mux.HandleTask("subgraphcohortfailwait.verify:0/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil // never reached: the cohort fails
	})
	mux.HandleTask("subgraphcohortfailwait.verify:0/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("subgraphcohortfailwait.verify:0/inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// Run the parent in the background; Run blocks until the (root) flow stops.
	type outcome struct {
		out *workflow.FlowOutcome
		err error
	}
	rootCh := make(chan outcome, 1)
	go func() {
		_, o, e := eng.Run(ctx, "subgraphcohortfailwait.verify:0/parent", map[string]any{}, nil)
		rootCh <- outcome{o, e}
	}()

	// Wait until the Boom branch's failure has COMMITTED (its failStep committed without failing the flow - the
	// cohort is not fully arrived while Slow blocks). Only then does releasing Slow make its completion the last
	// arrival, deterministically driving the transition-path cohort failure under test. Each dispatch's end is
	// waited for as an event, since it is database work, and then read: one that ended without failing Boom was
	// rewound, and the next execution reports again. The ceiling is a hang guard.
	var child string
	hang := time.After(30 * time.Second * enginetest.TimeoutScale())
	for boomFailed := false; !boomFailed; {
		var boom boomed
		select {
		case boom = <-boomCh:
		case <-hang:
			assert.True(false, "the Boom branch never failed")
			return
		}
		select {
		case <-boom.done:
		case <-hang:
			assert.True(false, "the Boom branch's dispatch never finished")
			return
		}
		child = boom.child
		steps, err := eng.History(ctx, child)
		if !assert.NoError(err) {
			return
		}
		for _, s := range steps {
			if s.TaskName == "Boom" && s.Status == workflow.StatusFailed {
				boomFailed = true
			}
		}
	}

	// The stop is waited for as an event, not a duration: everything between the release and the wake - Slow
	// returning, the transition committing the cohort's failure, the Await's read - is database work, and a 3s
	// bound on it fails under a database stall. The targeted checkpoint fires at the top of signalStop,
	// post-commit, so its arrival proves the transition path raised signalStop for the child's key as failed.
	// That is the half of the wake the Await's return cannot show, because the latch detector would also find the
	// committed stop. Delivery from signalStop to a parked caller is internal/latch's
	// TestRelease_WakesWithoutASweep, and the detector-only case is
	// TestSubgraphChildStopSignal_DroppedThenBackstopped. Every wait below is a hang ceiling and nothing else.
	// Armed before anything can fail the child.
	stopped := engineseams.Seams(eng).Waiter(engineseams.Join(engineseams.CheckpointFlowStopped, child, workflow.StatusFailed))

	// Register an Await on the child key while the child is still running (Slow is blocked). The ctx outlasts
	// the hang ceilings below, so it is never what returns the Await.
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 60*time.Second*enginetest.TimeoutScale())
	defer cancelAwait()
	awaitCh := make(chan outcome, 1)
	parked := engineseams.Seams(eng).Waiter(engineseams.Join(engineseams.CheckpointAwaitParked, child)) // armed before the goroutine that trips it
	go func() {
		o, e := eng.Await(awaitCtx, child)
		awaitCh <- outcome{o, e}
	}()

	// The Await must be ON the board before the child settles, or the test proves nothing: `await` reads once
	// before parking, so an Await that registers after the cohort resolved is answered by its own read and the
	// wake path under test never runs - silently, with every assertion below still passing.
	select {
	case <-parked:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "the Await never parked on the child, so no blocked waiter was there to be woken")
		return
	}
	releaseSlow() // Slow completes last; the cohort resolves with failures via the transition path
	select {
	case <-stopped:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "the child's cohort failure never raised signalStop for a failed stop on its key")
		return
	}
	select {
	case aw := <-awaitCh:
		assert.NoError(aw.err, "Await(childKey) must return the child's cohort failure, not time out")
		if assert.NotNil(aw.out) {
			assert.Equal(workflow.StatusFailed, aw.out.Status)
		}
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await(childKey) never returned, though signalStop was raised for the child's failed stop")
		return
	}

	// The parent observes the child failure through its flow.Subgraph call and fails in turn.
	select {
	case ro := <-rootCh:
		assert.NoError(ro.err)
		if assert.NotNil(ro.out) {
			assert.Equal(workflow.StatusFailed, ro.out.Status)
		}
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "parent flow did not stop")
		return
	}
}
