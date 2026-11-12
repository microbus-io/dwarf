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

// Both-order interleavings. A race with two legal outcomes is pinned in BOTH directions
// deterministically by freezing the racing operations at checkpoints and releasing them in a chosen order -
// where a probabilistic loop only ever samples "whichever won this time". These use the checkpoint seam's
// setBreakpoint on one or two sites to drive each order.
package fixtures

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// The checkpoint rendezvous is engineseams.Seams(e).WaitTimeout; awaitFlowStatus (checkpointhelpers_test.go) is the
// flow-status wait shared by every checkpoint-driven test.

// TestCompleteFlowVsTerminate_BothOrders pins the completeFlow-vs-Terminate race in both directions. The window:
// the terminal step is already marked completed, and completeFlow is about to flip the flow to completed when
// a Terminate arrives. Released completion-first the flow completes and the later Terminate is a no-op;
// released Terminate-first the flow terminates and completeFlow's status-gate (status NOT IN terminal) no-ops.
func TestCompleteFlowVsTerminate_BothOrders(t *testing.T) {
	t.Parallel()
	newEngine := func(t *testing.T, prefix string) (*engine.Engine, string) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		g := workflow.NewGraph("Solo")
		g.SetURL("A", prefix+"/a")
		g.AddTransition("A", workflow.End)
		assert.NoError(g.Validate())
		mux.HandleGraph(prefix+"/g", g)
		mux.HandleTask(prefix+"/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
		e := engine.NewEngineUnderTest(t.Name())
		e.SetHost(mux)
		assert.Must(assert.NoError(e.Startup(t.Context())))
		return e, prefix + "/g"
	}

	t.Run("terminate_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, url := newEngine(t, "cfvc1")
		defer e.Shutdown(ctx)

		// Freeze the worker just before completeFlow's transaction (A is already marked completed).
		engineseams.Seams(e).Break(engineseams.CheckpointBeforeCompleteFlowWrite)
		fk, err := e.Create(ctx, url, nil, nil)
		assert.NoError(err)
		assert.True(engineseams.Seams(e).WaitTimeout(ctx, engineseams.CheckpointBeforeCompleteFlowWrite, 10*time.Second), "engine never reached checkpoint engineseams.CheckpointBeforeCompleteFlowWrite")

		// Terminate wins while completion is held: the flow goes terminated under the flow-row lock.
		assert.NoError(e.Terminate(ctx, fk, "test"))

		// Release completion: its status-gate write (status NOT IN terminal) matches zero rows - a clean no-op,
		// the flow stays terminated.
		engineseams.Seams(e).Resume(engineseams.CheckpointBeforeCompleteFlowWrite)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusTerminated, 10*time.Second)
		assert.Equal(workflow.StatusTerminated, enginetest.FlowStatus(t, e, fk))
		enginetest.AssertInvariants(t, e)
	})

	t.Run("completion_first", func(t *testing.T) {
		assert := testarossa.For(t)
		ctx := context.Background()
		e, url := newEngine(t, "cfvc2")
		defer e.Shutdown(ctx)

		// Freeze at the same window, then release completion FIRST so the flow completes.
		engineseams.Seams(e).Break(engineseams.CheckpointBeforeCompleteFlowWrite)
		fk, err := e.Create(ctx, url, nil, nil)
		assert.NoError(err)
		assert.True(engineseams.Seams(e).WaitTimeout(ctx, engineseams.CheckpointBeforeCompleteFlowWrite, 10*time.Second), "engine never reached checkpoint engineseams.CheckpointBeforeCompleteFlowWrite")
		engineseams.Seams(e).Resume(engineseams.CheckpointBeforeCompleteFlowWrite)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)

		// Terminate now arrives on a closed flow: a no-op, and the flow stays completed.
		assert.NoError(e.Terminate(ctx, fk, "test"))
		assert.Equal(workflow.StatusCompleted, enginetest.FlowStatus(t, e, fk))
		enginetest.AssertInvariants(t, e)
	})
}

// TestConcurrentInterrupt_FirstWriterWins pins the interrupt-payload first-writer-wins guard: when two
// fan-out siblings inside a subgraph both interrupt, their propagations up the surgraph chain both target the
// SHARED ancestor (the parent's subgraph-caller step), and the guard (interrupt_payload='{}') keeps the
// second from clobbering the payload the first already wrote there.
//
// Unlike the two races above, the two interrupts hit the *same* payload-write site, and Resume releases every
// arrival at that site together - so a breakpoint cannot order two arrivals there. The order is imposed at the
// task level instead: branch IX interrupts immediately, branch IY holds on a channel until the test has seen
// IX's propagation land, then interrupts - so IX is always the first writer and IY always the guarded no-op
// writer. A held task is the right tool for that; what must NOT stand in for it is a wall-clock poll on the
// ancestor row, so both waits here rendezvous with the propagation itself via awaitPropagatedInterrupts.
func TestConcurrentInterrupt_FirstWriterWins(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	yGate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(yGate) }) }

	mux := hosts.NewMux()
	// Parent: one subgraph caller. Its Call step is the shared ancestor both child branches interrupt up to.
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "cip/call")
	parent.AddTransition("Call", workflow.End)
	assert.NoError(parent.Validate())
	mux.HandleGraph("cip/parent", parent)
	// Child: fan-out Split -> {IX, IY} -> J. Both branches interrupt with distinct payloads.
	child := workflow.NewGraph("Child")
	child.SetURL("Split", "cip/split")
	child.SetURL("IX", "cip/ix")
	child.SetURL("IY", "cip/iy")
	child.SetURL("J", "cip/j")
	child.SetFanIn("J")
	child.AddTransition("Split", "IX")
	child.AddTransition("Split", "IY")
	child.AddTransition("IX", "J")
	child.AddTransition("IY", "J")
	child.AddTransition("J", workflow.End)
	assert.NoError(child.Validate())
	mux.HandleGraph("cip/child", child)

	mux.HandleTask("cip/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("cip/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("cip/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	// IX interrupts immediately - the first writer.
	mux.HandleTask("cip/ix", func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(map[string]any{"branch": "X"}, nil)
		return err
	})
	// IY interrupts only after the test releases it (once IX has committed) - the guarded no-op writer.
	mux.HandleTask("cip/iy", func(ctx context.Context, f *workflow.Flow) error {
		<-yGate
		_, err := f.Interrupt(map[string]any{"branch": "Y"}, nil)
		return err
	})
	mux.HandleTask("cip/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	// Release IY here, AFTER the Shutdown defer, so it unwinds FIRST: Shutdown drains the workers, and one
	// still parked on yGate would never return.
	defer release()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk, err := e.Create(ctx, "cip/parent", nil, nil)
	assert.NoError(err)
	shard, _, parentFlowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	sh, err := engineseams.DB(e).Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	callPayload := func() string {
		var p string
		db.QueryRowContext(ctx, "SELECT interrupt_payload FROM dwarf_steps WHERE flow_id=? AND task_name='Call'", parentFlowID).Scan(&p)
		return strings.TrimSpace(p)
	}

	// IX's interrupt propagates its payload onto the shared Call step (the first writer wins it). The stop
	// checkpoint fires POST-COMMIT, so one propagation counted on the parent means the payload is durable
	// there - the fact the poll this replaces was spinning to sample.
	awaitPropagatedInterrupts(t, e, fk, 1)
	assert.True(strings.Contains(callPayload(), "X"), "first writer (IX) should own the ancestor payload")

	// Release IY: its interrupt now reaches the payload write, but the guard (interrupt_payload='{}') matches
	// zero rows on the already-written Call step - a no-op. Its propagation still signals a stop, so the
	// second arrival on the parent is what says IY has been all the way through the write.
	release()
	awaitPropagatedInterrupts(t, e, fk, 2)

	// The guard held: the shared ancestor still carries IX's payload, uncorrupted by IY.
	p := callPayload()
	assert.True(strings.Contains(p, "X"), "ancestor payload should still be IX's, got %q", p)
	assert.True(!strings.Contains(p, "Y"), "IY must not have clobbered the shared ancestor payload, got %q", p)
	enginetest.AssertInvariants(t, e)
}
