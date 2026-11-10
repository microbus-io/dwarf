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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// terminateBeforeTransition runs a flow A -> B -> END whose A task is task, freezes the worker after A's completion
// write and before its transition, Terminates the flow there, runs arm, and releases the worker. It returns once
// A's dispatch has returned, with A's status and error and B's step count.
func terminateBeforeTransition(t *testing.T, task func(ctx context.Context, f *workflow.Flow) error, arm func(e *Engine)) (aStatus, aError string, bSteps int) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Fence")
	g.SetURL("A", "tfence/a")
	g.SetURL("B", "tfence/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("tfence/g", g)
	mux.HandleTask("tfence/a", task)
	mux.HandleTask("tfence/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	e.seams.Break(engineseams.CheckpointBeforeTransitionTx)
	fk, err := e.Create(ctx, "tfence/g", nil, nil)
	assert.NoError(err)
	assert.Must(assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeTransitionTx, 10*time.Second), "A never reached its transition"))
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	var aStepID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND status='"+
		workflow.StatusCompleted+"' AND advancing=1", flowID).Scan(&aStepID), "A's completion write did not land")
	aDone := engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(aStepID))
	aDoneBase := e.seams.Visits(aDone)

	assert.NoError(e.Terminate(ctx, fk, "test"))
	arm(e)
	e.seams.Resume(engineseams.CheckpointBeforeTransitionTx)
	enginetest.AwaitVisitCount(t, e.seams, aDoneBase+1, 15*time.Second, aDone)

	assert.NoError(db.QueryRowContext(ctx, "SELECT status, COALESCE(error, '') FROM dwarf_steps WHERE step_id=?", aStepID).Scan(&aStatus, &aError))
	bSteps = enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='B'", flowID)
	out, err := e.Snapshot(ctx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusTerminated, out.Status)
	}
	enginetest.AssertInvariants(t, e)
	return aStatus, aError, bSteps
}

// TestTerminate_FencesAFailedTransition pins that a step whose completion write landed before a Terminate keeps its
// recorded outcome when its transition then fails: the failure's write is fenced, so the closed tree's step is not
// rewritten `failed` with an error, which History, Step and Fingerprint would show. The transition fails on a Goto
// with no goto edge from A.
func TestTerminate_FencesAFailedTransition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	status, errText, bSteps := terminateBeforeTransition(t,
		func(ctx context.Context, f *workflow.Flow) error { f.Goto("B"); return nil },
		func(e *Engine) {},
	)
	assert.Equal(workflow.StatusCompleted, status, "the terminated flow's completed step was rewritten")
	assert.Equal("", errText)
	assert.Zero(bSteps)
}

// TestTerminate_FencesAnErroredTransition pins that a step whose completion write landed before a Terminate keeps its
// recorded outcome when its transition errors out: the recovery reset is fenced, so the step is not put back to
// `pending` and then reaped as `terminated` - recording a task that completed as one that never finished, or leaving
// a pending step in a closed tree. The transition errors on lock contention past Transact's retries.
func TestTerminate_FencesAnErroredTransition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	status, errText, bSteps := terminateBeforeTransition(t,
		func(ctx context.Context, f *workflow.Flow) error { return nil },
		func(e *Engine) { e.seams.InjectN(engineseams.Join(engineseams.FaultContention, "A"), 1000) },
	)
	assert.Equal(workflow.StatusCompleted, status, "the terminated flow's completed step was reset")
	assert.Equal("", errText)
	assert.Zero(bSteps)
}

// TestTerminate_FencesACancelRoute pins that a step Cancel marked while it ran, whose completion write landed before
// a Terminate, keeps its recorded outcome when its worker goes on to route the cancellation. With an onError
// handler the route adds onErr to the step's changes; without one it fails the step as cancelled. Both are fenced,
// so the closed tree's step stays completed with the changes its task wrote.
func TestTerminate_FencesACancelRoute(t *testing.T) {
	t.Parallel()
	for _, withHandler := range []bool{true, false} {
		name := "no_handler"
		if withHandler {
			name = "on_error_handler"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := context.Background()

			mux := hosts.NewMux()
			g := workflow.NewGraph("CancelFence")
			g.SetURL("A", "tcfence/a")
			g.SetURL("B", "tcfence/b")
			g.AddTransition("A", "B")
			g.AddTransition("B", workflow.End)
			if withHandler {
				g.SetURL("H", "tcfence/h")
				g.AddTransitionOnError("A", "H")
				g.AddTransition("H", workflow.End)
				mux.HandleTask("tcfence/h", func(ctx context.Context, f *workflow.Flow) error { return nil })
			}
			assert.NoError(g.Validate())
			mux.HandleGraph("tcfence/g", g)
			started, release := make(chan struct{}, 1), make(chan struct{})
			mux.HandleTask("tcfence/a", func(ctx context.Context, f *workflow.Flow) error {
				started <- struct{}{}
				<-release
				f.Set("a", 1)
				return nil
			})
			mux.HandleTask("tcfence/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

			e := NewEngineUnderTest(t.Name())
			defer e.Shutdown(ctx)
			var releaseOnce sync.Once
			releaseA := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseA() // below the Shutdown defer, so an early exit frees the held worker before the drain waits on it
			e.SetHost(mux)
			assert.Must(assert.NoError(e.Startup(t.Context())))

			fk, err := e.Create(ctx, "tcfence/g", nil, nil)
			assert.NoError(err)
			select {
			case <-started:
			case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
				t.Fatal("A never started")
			}
			// Cancel marks A while it runs; A's completion write then finds the mark.
			assert.NoError(e.Cancel(ctx, fk, "withdrawn"))
			e.seams.Break(engineseams.CheckpointBeforeCancelRoute)
			releaseA()
			assert.Must(assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeCancelRoute, 10*time.Second), "A never reached its cancel route"))

			shard, _, flowID, _, err := keys.ParseFlowKey(fk)
			assert.NoError(err)
			db, err := e.db.Shard(shard)
			assert.Must(assert.NoError(err))
			var aStepID int
			assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND status='"+
				workflow.StatusCompleted+"' AND advancing=1", flowID).Scan(&aStepID), "A's completion write did not land")
			aDone := engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(aStepID))
			aDoneBase := e.seams.Visits(aDone)

			assert.NoError(e.Terminate(ctx, fk, "test"))
			e.seams.Resume(engineseams.CheckpointBeforeCancelRoute)
			enginetest.AwaitVisitCount(t, e.seams, aDoneBase+1, 15*time.Second, aDone)

			var status, changes string
			assert.NoError(db.QueryRowContext(ctx, "SELECT status, changes FROM dwarf_steps WHERE step_id=?", aStepID).Scan(&status, &changes))
			assert.Equal(workflow.StatusCompleted, status, "the terminated flow's completed step was failed")
			assert.False(strings.Contains(changes, "onErr"), "the terminated flow's completed step was routed: %s", changes)
			assert.True(strings.Contains(changes, `"a"`), "A's own changes were lost: %s", changes)
			assert.Zero(enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name<>'A'", flowID))
			out, err := e.Snapshot(ctx, fk)
			if assert.NoError(err) {
				assert.Equal(workflow.StatusTerminated, out.Status)
			}
			enginetest.AssertInvariants(t, e)
		})
	}
}
