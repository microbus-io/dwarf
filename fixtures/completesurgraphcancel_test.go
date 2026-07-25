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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCompleteSurgraph_vs_CancelRoot_BothOrders is the subgraph-child variant of
// TestCompleteFlowVsCancel_BothOrders (which raced a top-level completeFlow against Cancel). Here the flow
// completing at engine.CheckpointBeforeCompleteFlowWrite is a subgraph CHILD, so its completion additionally drives
// completeSurgraphFlow's revive of the parked caller - and the racing Cancel is a whole-tree teardown from the
// root. The window: the child's terminal step is already marked completed and completeFlow is about to flip the
// child flow to completed (which would then revive the caller) when a Cancel(root) arrives.
//
//   - completion_first: the child completes, completeSurgraphFlow revives the (running+parkedSubgraph) caller to
//     pending and it re-dispatches; the later Cancel then tears the now-running caller down cleanly.
//   - cancel_first: Cancel terminalizes the whole tree (caller + still-running child) first; on release the
//     child's completeFlow status-gate matches zero rows (child already cancelled), so it no-ops - it does NOT
//     complete the child and never reaches completeSurgraphFlow, so the cancelled caller is not resurrected.
//
// This composes the checkpoint seam with the revive guard TestReviveVsCancel_Deterministic pins, but across the
// child's completion boundary and in both orders.
func TestCompleteSurgraph_vs_CancelRoot_BothOrders(t *testing.T) {
	t.Parallel()
	// newEngine builds Parent(Call -> subgraph Child(X)). On the caller's post-subgraph re-dispatch the Call task
	// signals callResumed then blocks on callBlock, so a revived caller rests `running` (not racing to completion)
	// while the test drives the Cancel. callBlock is closed at cleanup to release the parked worker goroutine.
	newEngine := func(t *testing.T, prefix string) (e *engine.Engine, url string, callResumed chan struct{}, callBlock chan struct{}) {
		assert := testarossa.For(t)
		callResumed = make(chan struct{}, 1)
		callBlock = make(chan struct{})
		proxy := engine.NewTestProxy()

		parent := workflow.NewGraph("Parent")
		parent.SetEndpoint("Call", prefix+"/call")
		parent.AddTransition("Call", workflow.END)
		assert.NoError(parent.Validate())
		proxy.HandleGraph(prefix+"/parent", parent)

		child := workflow.NewGraph("Child")
		child.SetEndpoint("X", prefix+"/x")
		child.AddTransition("X", workflow.END)
		assert.NoError(child.Validate())
		proxy.HandleGraph(prefix+"/child", child)

		proxy.HandleTask(prefix+"/call", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph(prefix+"/child", nil, nil)
			if yield || err != nil {
				return err
			}
			// Resumed after the child completed: signal, then hold the flow running so Cancel deterministically
			// sees a running caller.
			select {
			case callResumed <- struct{}{}:
			default:
			}
			<-callBlock
			return nil
		})
		proxy.HandleTask(prefix+"/x", func(ctx context.Context, f *workflow.Flow) error { return nil })

		e = engine.NewEngineUnderTest(t)
		e.SetHost(proxy)
		assert.NoError(e.Startup(t.Context()))
		t.Cleanup(func() { close(callBlock) })
		return e, prefix + "/parent", callResumed, callBlock
	}

	// callStatus reads the Parent's Call step status.
	callStatus := func(t *testing.T, e *engine.Engine, fk string) string {
		t.Helper()
		assert := testarossa.For(t)
		shard, flowID, _, err := keys.ParseFlowKey(fk)
		assert.NoError(err)
		db, err := e.DB().Shard(shard)
		assert.NoError(err)
		var s string
		assert.NoError(db.QueryRowContext(context.Background(), "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='Call'", flowID).Scan(&s))
		return s
	}

	t.Run("completion_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, url, callResumed, _ := newEngine(t, "csvc1")

		// Freeze the child's worker just before its completeFlow transaction (X is already marked completed, the
		// caller is running+parkedSubgraph).
		e.Seams().Break(engine.CheckpointBeforeCompleteFlowWrite)
		fk, err := e.Create(ctx, url, nil, nil)
		assert.NoError(err)
		assert.True(e.Seams().WaitTimeout(ctx, 10*time.Second, engine.CheckpointBeforeCompleteFlowWrite), "engine never reached checkpoint engine.CheckpointBeforeCompleteFlowWrite")

		// Release completion FIRST: the child completes and completeSurgraphFlow revives the caller, which
		// re-dispatches and blocks (running).
		e.Seams().Resume(engine.CheckpointBeforeCompleteFlowWrite)
		select {
		case <-callResumed:
		case <-time.After(10 * time.Second):
			assert.True(false, "caller never revived after the child completed")
			return
		}
		assert.Equal(workflow.StatusRunning, callStatus(t, e, fk)) // caller revived and running

		// Cancel now tears the running caller (and its root flow) down cleanly.
		assert.NoError(e.Cancel(ctx, fk, "test"))
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCancelled, 10*time.Second)
		assert.Equal(workflow.StatusCancelled, enginetest.FlowStatus(t, e, fk))
		assert.Equal(workflow.StatusCancelled, callStatus(t, e, fk))
		enginetest.AssertInvariants(t, e)
	})

	t.Run("cancel_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, url, _, _ := newEngine(t, "csvc2")

		// Freeze the child at the same window.
		e.Seams().Break(engine.CheckpointBeforeCompleteFlowWrite)
		fk, err := e.Create(ctx, url, nil, nil)
		assert.NoError(err)
		assert.True(e.Seams().WaitTimeout(ctx, 10*time.Second, engine.CheckpointBeforeCompleteFlowWrite), "engine never reached checkpoint engine.CheckpointBeforeCompleteFlowWrite")

		// Cancel wins while the child's completion is held: the whole tree (root, its parked Call caller, and the
		// still-running child) is terminalized under the cancel transaction.
		assert.NoError(e.Cancel(ctx, fk, "test"))
		assert.Equal(workflow.StatusCancelled, callStatus(t, e, fk)) // caller cancelled, not parked/revived

		// Release completion: the child's status-gate (status NOT IN terminal) matches zero rows - a clean no-op.
		// completeFlow returns completed=false, so completeSurgraphFlow never runs and the cancelled caller is not
		// resurrected to pending.
		e.Seams().Resume(engine.CheckpointBeforeCompleteFlowWrite)

		// Give the released worker a moment to (wrongly) revive the caller, then confirm nothing did: the flow
		// stays cancelled, the caller stays cancelled, and no orphan/wedge shape was created.
		time.Sleep(200 * time.Millisecond)
		assert.Equal(workflow.StatusCancelled, enginetest.FlowStatus(t, e, fk))
		assert.Equal(workflow.StatusCancelled, callStatus(t, e, fk))
		enginetest.AssertInvariants(t, e)
	})
}
