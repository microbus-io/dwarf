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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestCancelSpawnflow pins that a subgraph asked for by a caller a Cancel covered is born cancelled: its entry step
// never runs, the cancellation reaches the caller as its child's error, and a handler - the child's own or the
// caller's - still recovers the flow, a subgraph the caller's handler spawns running as any other.
func TestCancelSpawnflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// held is a caller whose first run waits inside its task until released, so a Cancel lands while it runs, and
	// which then asks for its child.
	type held struct {
		started, release chan struct{}
		runs             atomic.Int32
		once             sync.Once
	}
	newHeld := func() *held { return &held{started: make(chan struct{}), release: make(chan struct{})} }
	// call is the caller's task: the first run waits, then spawns the child; a later run hands onward what
	// onReturn makes of the child's outcome.
	call := func(h *held, childURL string, onReturn func(f *workflow.Flow, out workflow.State, err error) error) func(context.Context, *workflow.Flow) error {
		return func(ctx context.Context, f *workflow.Flow) error {
			if h.runs.Add(1) == 1 {
				close(h.started)
				<-h.release
			}
			var out workflow.State
			yield, err := f.Subgraph(childURL, nil, &out)
			if yield {
				return nil
			}
			return onReturn(f, out, err)
		}
	}
	returnErr := func(f *workflow.Flow, out workflow.State, err error) error { return err }

	t.Run("an_interrupting_child_never_runs", func(t *testing.T) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		child := workflow.NewGraph("Ask")
		child.SetURL("Ask", "cancelspawnflow.verify:428/ask")
		child.AddTransition("Ask", workflow.End)
		mux.HandleGraph("cancelspawnflow.verify:428/ask-child", child)
		var asked atomic.Int32
		mux.HandleTask("cancelspawnflow.verify:428/ask", func(ctx context.Context, f *workflow.Flow) error {
			asked.Add(1)
			_, err := f.Interrupt(map[string]any{"ask": "proceed?"}, nil)
			return err
		})
		// A step before the caller, so the child flow's id and its entry step's differ.
		g := workflow.NewGraph("Caller")
		g.SetURL("Start", "cancelspawnflow.verify:428/start")
		g.SetURL("Call", "cancelspawnflow.verify:428/call-ask")
		g.AddTransitionChain("Start", "Call", workflow.End)
		mux.HandleGraph("cancelspawnflow.verify:428/caller-ask", g)
		mux.HandleTask("cancelspawnflow.verify:428/start", func(ctx context.Context, f *workflow.Flow) error { return nil })
		h := newHeld()
		mux.HandleTask("cancelspawnflow.verify:428/call-ask", call(h, "cancelspawnflow.verify:428/ask-child", returnErr))

		e := engine.NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		defer h.once.Do(func() { close(h.release) })
		e.SetHost(mux)
		assert.Must(assert.NoError(e.Startup(t.Context())))

		flowKey, err := e.Create(ctx, "cancelspawnflow.verify:428/caller-ask", nil, nil)
		assert.Must(assert.NoError(err))
		<-h.started
		assert.Must(assert.NoError(e.Cancel(ctx, flowKey, "stop")))
		h.once.Do(func() { close(h.release) })

		outcome, err := enginetest.BoundedAwait(t, e, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCancelled, outcome.Status, "not interrupted on a prompt from cancelled work")
		assert.Equal("stop", outcome.CancelReason)
		assert.Equal(int32(0), asked.Load(), "the child's entry step never ran")
		childKey := subgraphKeyOf(t, e, flowKey, "Call")
		childOutcome, err := e.Snapshot(ctx, childKey)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCancelled, childOutcome.Status)
			assert.Equal("stop", childOutcome.CancelReason, "the child carries the Cancel's reason")
		}
		// The child is cancelled once, at its entry step: its watermark is that step's id, not the flow's.
		childFlowID := enginetest.CountRows(t, e, 1, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id<>0")
		entryStepID := enginetest.CountRows(t, e, 1, "SELECT MIN(step_id) FROM dwarf_steps WHERE flow_id=?", childFlowID)
		assert.NotEqual(childFlowID, entryStepID, "the ids differ, so binding the wrong one shows")
		assert.Equal(entryStepID, enginetest.CountRows(t, e, 1, "SELECT cancel_watermark FROM dwarf_flows WHERE flow_id=?", childFlowID))
		enginetest.AssertInvariants(t, e)
	})

	t.Run("the_childs_handler_recovers_it", func(t *testing.T) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		child := workflow.NewGraph("Work")
		child.SetURL("Work", "cancelspawnflow.verify:428/work")
		child.SetURL("Recover", "cancelspawnflow.verify:428/recover")
		child.AddTransition("Work", workflow.End)
		child.AddTransitionOnError("Work", "Recover")
		child.AddTransition("Recover", workflow.End)
		mux.HandleGraph("cancelspawnflow.verify:428/work-child", child)
		var worked, recovered atomic.Int32
		mux.HandleTask("cancelspawnflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
			worked.Add(1)
			return nil
		})
		mux.HandleTask("cancelspawnflow.verify:428/recover", func(ctx context.Context, f *workflow.Flow) error {
			var onErr errors.TracedError
			_, _ = f.Get("onErr", &onErr)
			if workflow.IsCancelled(&onErr) {
				recovered.Add(1)
			}
			f.SetInt("childOut", 7)
			return nil
		})
		g := workflow.NewGraph("Caller")
		g.SetURL("Call", "cancelspawnflow.verify:428/call-work")
		g.AddTransition("Call", workflow.End)
		mux.HandleGraph("cancelspawnflow.verify:428/caller-work", g)
		h := newHeld()
		// The caller sees the recovered child's result, and keeps it: a covered completion's changes stand.
		mux.HandleTask("cancelspawnflow.verify:428/call-work", call(h, "cancelspawnflow.verify:428/work-child",
			func(f *workflow.Flow, out workflow.State, err error) error {
				if err != nil {
					return err
				}
				f.SetInt("fromChild", out.GetInt("childOut"))
				return nil
			}))

		e := engine.NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		defer h.once.Do(func() { close(h.release) })
		e.SetHost(mux)
		assert.Must(assert.NoError(e.Startup(t.Context())))

		flowKey, err := e.Create(ctx, "cancelspawnflow.verify:428/caller-work", nil, nil)
		assert.Must(assert.NoError(err))
		<-h.started
		assert.Must(assert.NoError(e.Cancel(ctx, flowKey, "stop")))
		h.once.Do(func() { close(h.release) })

		outcome, err := enginetest.BoundedAwait(t, e, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(int32(0), worked.Load(), "the child's entry step never ran")
		assert.Equal(int32(1), recovered.Load(), "the child's handler caught the cancellation")
		childOutcome, err := e.Snapshot(ctx, subgraphKeyOf(t, e, flowKey, "Call"))
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, childOutcome.Status, "the child recovered")
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status, "the caller's own completion is still covered")
		assert.Equal(7, outcome.State.GetInt("fromChild"), "the caller saw what the recovered child returned")
		enginetest.AssertInvariants(t, e)
	})

	t.Run("the_callers_handler_spawns_an_ordinary_child", func(t *testing.T) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		for _, name := range []string{"first", "second"} {
			child := workflow.NewGraph("Child")
			child.SetURL("Child", "cancelspawnflow.verify:428/"+name)
			child.AddTransition("Child", workflow.End)
			mux.HandleGraph("cancelspawnflow.verify:428/"+name+"-child", child)
		}
		var firstRan, secondRan atomic.Int32
		mux.HandleTask("cancelspawnflow.verify:428/first", func(ctx context.Context, f *workflow.Flow) error {
			firstRan.Add(1)
			return nil
		})
		mux.HandleTask("cancelspawnflow.verify:428/second", func(ctx context.Context, f *workflow.Flow) error {
			secondRan.Add(1)
			return nil
		})
		g := workflow.NewGraph("Caller")
		g.SetURL("Call", "cancelspawnflow.verify:428/call-first")
		g.SetURL("Handle", "cancelspawnflow.verify:428/handle")
		g.AddTransition("Call", workflow.End)
		g.AddTransitionOnError("Call", "Handle")
		g.AddTransition("Handle", workflow.End)
		mux.HandleGraph("cancelspawnflow.verify:428/caller-first", g)
		h := newHeld()
		mux.HandleTask("cancelspawnflow.verify:428/call-first", call(h, "cancelspawnflow.verify:428/first-child", returnErr))
		// The handler recovers by running a subgraph of its own: it is inserted after the Cancel, so its child is born
		// as any other.
		mux.HandleTask("cancelspawnflow.verify:428/handle", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph("cancelspawnflow.verify:428/second-child", nil, nil)
			if yield || err != nil {
				return err
			}
			f.SetBool("recovered", true)
			return nil
		})

		e := engine.NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		defer h.once.Do(func() { close(h.release) })
		e.SetHost(mux)
		assert.Must(assert.NoError(e.Startup(t.Context())))

		flowKey, err := e.Create(ctx, "cancelspawnflow.verify:428/caller-first", nil, nil)
		assert.Must(assert.NoError(err))
		<-h.started
		assert.Must(assert.NoError(e.Cancel(ctx, flowKey, "stop")))
		h.once.Do(func() { close(h.release) })

		outcome, err := enginetest.BoundedAwait(t, e, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCompleted, outcome.Status, "the caller's handler recovered the flow")
		assert.True(outcome.State.GetBool("recovered"))
		assert.Equal(int32(0), firstRan.Load(), "the covered caller's child was born cancelled")
		assert.Equal(int32(1), secondRan.Load(), "the handler's child ran")
		enginetest.AssertInvariants(t, e)
	})

	t.Run("the_caller_is_marked_before_the_cancels_mark_pass", func(t *testing.T) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		child := workflow.NewGraph("Quick")
		child.SetURL("Quick", "cancelspawnflow.verify:428/quick")
		child.AddTransition("Quick", workflow.End)
		mux.HandleGraph("cancelspawnflow.verify:428/quick-child", child)
		var quickRan atomic.Int32
		mux.HandleTask("cancelspawnflow.verify:428/quick", func(ctx context.Context, f *workflow.Flow) error {
			quickRan.Add(1)
			return nil
		})
		g := workflow.NewGraph("Caller")
		g.SetURL("Call", "cancelspawnflow.verify:428/call-quick")
		g.AddTransition("Call", workflow.End)
		mux.HandleGraph("cancelspawnflow.verify:428/caller-quick", g)
		h := newHeld()
		var consumed atomic.Bool
		// The caller handles its child's cancellation itself and completes: only its own mark can still cover it.
		mux.HandleTask("cancelspawnflow.verify:428/call-quick", call(h, "cancelspawnflow.verify:428/quick-child",
			func(f *workflow.Flow, out workflow.State, err error) error {
				consumed.Store(workflow.IsCancelled(err))
				return nil
			}))

		e := engine.NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		seams := accessors.Seams(e)
		defer seams.Resume(diagnostics.CheckpointCancelBeforeMark)
		defer h.once.Do(func() { close(h.release) })
		e.SetHost(mux)
		assert.Must(assert.NoError(e.Startup(t.Context())))

		flowKey, err := e.Create(ctx, "cancelspawnflow.verify:428/caller-quick", nil, nil)
		assert.Must(assert.NoError(err))
		<-h.started
		// Cancel commits its watermark and is held before its mark pass.
		seams.Break(diagnostics.CheckpointCancelBeforeMark)
		cancelled := make(chan error, 1)
		go func() { cancelled <- e.Cancel(ctx, flowKey, "stop") }()
		assert.Must(assert.True(seams.WaitTimeout(ctx, diagnostics.CheckpointCancelBeforeMark, 10*time.Second*enginetest.TimeoutScale()),
			"Cancel never reached its mark pass"))
		// The caller spawns, its child is cancelled at once, and the caller is revived and completes - all before
		// the mark pass.
		h.once.Do(func() { close(h.release) })
		enginetest.AwaitFlowStatus(t, e, flowKey, workflow.StatusCancelled, 10*time.Second)
		seams.Resume(diagnostics.CheckpointCancelBeforeMark)
		assert.NoError(<-cancelled)

		outcome, err := enginetest.BoundedAwait(t, e, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCancelled, outcome.Status, "the spawn marked the caller")
		assert.True(consumed.Load(), "the caller saw its child's cancellation")
		assert.Equal(int32(0), quickRan.Load())
		enginetest.AssertInvariants(t, e)
	})
}

// subgraphKeyOf returns the key of the child the flow's step of task called.
func subgraphKeyOf(t *testing.T, e *engine.Engine, flowKey, task string) string {
	t.Helper()
	steps, err := e.History(context.Background(), flowKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range steps {
		if s.TaskName == task && s.SubgraphKey != "" {
			return s.SubgraphKey
		}
	}
	t.Fatalf("no child of %s in the history of %s", task, flowKey)
	return ""
}
