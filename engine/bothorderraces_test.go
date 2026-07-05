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

// CP-E: both-order interleavings. A race with two legal outcomes is pinned in BOTH directions
// deterministically by freezing the racing operations at checkpoints and releasing them in a chosen order -
// where a probabilistic loop only ever samples "whichever won this time". These use the checkpoint seam's
// setBreakpoint on one or two sites to drive each order.
package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// cpWaitFor blocks until the engine reaches (or is already frozen at) the named checkpoint, failing the test
// on timeout rather than hanging to the suite deadline.
func cpWaitFor(t *testing.T, e *Engine, name string, timeout time.Duration) {
	t.Helper()
	reached := make(chan struct{})
	go func() { e.waitFor(name); close(reached) }()
	select {
	case <-reached:
	case <-time.After(timeout):
		t.Fatalf("engine never reached checkpoint %q", name)
	}
}

// flowStatus reads a flow's current status by key.
func flowStatus(t *testing.T, e *Engine, flowKey string) string {
	t.Helper()
	assert := testarossa.For(t)
	shard, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	var status string
	assert.NoError(db.QueryRowContext(context.Background(), "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status))
	return strings.TrimSpace(status)
}

// TestCompleteFlowVsCancel_BothOrders pins the completeFlow-vs-Cancel race in both directions. The window:
// the terminal step is already marked completed, and completeFlow is about to flip the flow to completed when
// a Cancel arrives. Released completion-first the flow completes and the later Cancel 409s (terminal);
// released Cancel-first the flow cancels and completeFlow's status-gate (status NOT IN terminal) no-ops.
func TestCompleteFlowVsCancel_BothOrders(t *testing.T) {
	newEngine := func(t *testing.T, prefix string) (*Engine, string) {
		proxy := NewTestProxy()
		g := workflow.NewGraph("Solo")
		g.SetEndpoint("A", prefix+"/a")
		g.AddTransition("A", workflow.END)
		testarossa.For(t).NoError(g.Validate())
		proxy.HandleGraph(prefix+"/g", g)
		proxy.HandleTask(prefix+"/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
		e := NewEngine()
		e.SetHost(proxy)
		e.RunInTest(t)
		return e, prefix + "/g"
	}

	t.Run("cancel_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, url := newEngine(t, "cfvc1")

		// Freeze the worker just before completeFlow's transaction (A is already marked completed).
		e.setBreakpoint(checkpointBeforeCompleteFlowWrite)
		fk, err := e.Create(ctx, url, nil, nil)
		assert.NoError(err)
		cpWaitFor(t, e, checkpointBeforeCompleteFlowWrite, 10*time.Second)

		// Cancel wins while completion is held: the flow goes cancelled under the flow-row lock.
		assert.NoError(e.Cancel(ctx, fk, "test"))

		// Release completion: its status-gate write (status NOT IN terminal) matches zero rows - a clean no-op,
		// the flow stays cancelled.
		e.clearBreakpoint(checkpointBeforeCompleteFlowWrite)
		waitFlowStatus(t, e, fk, workflow.StatusCancelled, 10*time.Second)
		assert.Equal(workflow.StatusCancelled, flowStatus(t, e, fk))
		assertInvariants(t, e)
	})

	t.Run("completion_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, url := newEngine(t, "cfvc2")

		// Freeze at the same window, then release completion FIRST so the flow completes.
		e.setBreakpoint(checkpointBeforeCompleteFlowWrite)
		fk, err := e.Create(ctx, url, nil, nil)
		assert.NoError(err)
		cpWaitFor(t, e, checkpointBeforeCompleteFlowWrite, 10*time.Second)
		e.clearBreakpoint(checkpointBeforeCompleteFlowWrite)
		waitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)

		// Cancel now arrives on a terminal flow: it 409s and the flow stays completed.
		err = e.Cancel(ctx, fk, "test")
		assert.Error(err)
		assert.Equal(409, errors.StatusCode(err))
		assert.Equal(workflow.StatusCompleted, flowStatus(t, e, fk))
		assertInvariants(t, e)
	})
}

// TestDeleteVsResume_BothOrders pins the Delete-vs-Resume race in both directions with BOTH operations frozen
// at their pre-transaction checkpoints, released in a chosen order. Both gate on the flow being `interrupted`,
// so exactly one wins the CAS: released Resume-first the flow revives (running) and Delete 409s (running
// flow); released Delete-first the flow cancels and Resume's gate write finds it no longer interrupted and
// rolls back with an honest 409. The Delete-wins direction mirrors the existing (single-freeze) DD-1 pin; the
// Resume-wins direction is the untested mirror.
func TestDeleteVsResume_BothOrders(t *testing.T) {
	// newGate builds an interrupted flow whose gate task, on resume, BLOCKS on gateBlock - so a resumed flow
	// rests `running` (not racing to completion) while the test inspects the Delete outcome.
	newGate := func(t *testing.T, prefix string) (*Engine, chan struct{}) {
		gateBlock := make(chan struct{})
		proxy := NewTestProxy()
		g := workflow.NewGraph("Gate")
		g.SetEndpoint("Gate", prefix+"/gate")
		g.AddTransition("Gate", workflow.END)
		testarossa.For(t).NoError(g.Validate())
		proxy.HandleGraph(prefix+"/g", g)
		proxy.HandleTask(prefix+"/gate", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Interrupt(nil, nil)
			if yield || err != nil {
				return err
			}
			<-gateBlock // on resume, hold the flow running so Delete deterministically sees a running flow
			return nil
		})
		e := NewEngine()
		e.SetHost(proxy)
		e.RunInTest(t)
		t.Cleanup(func() { close(gateBlock) })
		return e, gateBlock
	}

	createInterrupted := func(t *testing.T, e *Engine, url string) string {
		assert := testarossa.For(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		fk, err := e.Create(ctx, url, nil, nil)
		assert.NoError(err)
		out, err := e.Await(ctx, fk)
		assert.NoError(err)
		if assert.NotNil(out) {
			assert.Equal(workflow.StatusInterrupted, out.Status)
		}
		return fk
	}

	t.Run("resume_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, _ := newGate(t, "dvr1")
		fk := createInterrupted(t, e, "dvr1/g")

		// Freeze both operations before their transactions, then launch them.
		e.setBreakpoint(checkpointResumeBeforeFlowWrite)
		e.setBreakpoint(checkpointBeforeDeleteWrite)
		resumeDone := make(chan error, 1)
		deleteDone := make(chan error, 1)
		go func() { resumeDone <- e.Resume(ctx, fk, nil) }()
		go func() { deleteDone <- e.Delete(ctx, fk) }()
		cpWaitFor(t, e, checkpointResumeBeforeFlowWrite, 10*time.Second)
		cpWaitFor(t, e, checkpointBeforeDeleteWrite, 10*time.Second)

		// Resume wins: released first, it flips interrupted->running and returns cleanly (the gate re-dispatches
		// and blocks, so the flow rests running).
		e.clearBreakpoint(checkpointResumeBeforeFlowWrite)
		assert.NoError(<-resumeDone)

		// Delete now sees a running flow -> 409, and never stamps deletion.
		e.clearBreakpoint(checkpointBeforeDeleteWrite)
		delErr := <-deleteDone
		assert.Error(delErr)
		assert.Equal(409, errors.StatusCode(delErr))
		assert.NotEqual(workflow.StatusCancelled, flowStatus(t, e, fk)) // Resume won; not cancelled
		assertInvariants(t, e)
	})

	t.Run("delete_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, _ := newGate(t, "dvr2")
		fk := createInterrupted(t, e, "dvr2/g")

		e.setBreakpoint(checkpointResumeBeforeFlowWrite)
		e.setBreakpoint(checkpointBeforeDeleteWrite)
		resumeDone := make(chan error, 1)
		deleteDone := make(chan error, 1)
		go func() { resumeDone <- e.Resume(ctx, fk, nil) }()
		go func() { deleteDone <- e.Delete(ctx, fk) }()
		cpWaitFor(t, e, checkpointResumeBeforeFlowWrite, 10*time.Second)
		cpWaitFor(t, e, checkpointBeforeDeleteWrite, 10*time.Second)

		// Delete wins: released first, it flips interrupted->cancelled and stamps deletion, returning cleanly.
		e.clearBreakpoint(checkpointBeforeDeleteWrite)
		assert.NoError(<-deleteDone)

		// Resume now finds the flow no longer interrupted: its gate write matches zero rows, the whole
		// transaction rolls back, and it returns an honest 409 (not a silent success).
		e.clearBreakpoint(checkpointResumeBeforeFlowWrite)
		resErr := <-resumeDone
		assert.Error(resErr)
		assert.Equal(409, errors.StatusCode(resErr))
		assert.Equal(workflow.StatusCancelled, flowStatus(t, e, fk)) // Delete won
		assertInvariants(t, e)
	})
}
