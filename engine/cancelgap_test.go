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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCancelInTransitionGap pins the one window Cancel's per-step mark cannot reach: a step whose completion
// write has landed (so it is terminal, never marked) but whose successor is not inserted yet (so there is
// nothing to mark). Without the flow-level cancel_watermark the successor is inserted unmarked and the flow
// runs to completed although Cancel returned nil; with it, the successor inherits the mark and is preempted.
// A and the Cancel routinely land in the same millisecond here, which is what a timestamp comparison lost.
func TestCancelInTransitionGap(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var bRan atomic.Int32
	proxy := NewTestProxy()
	g := workflow.NewGraph("CancelGap")
	g.SetEndpoint("A", "cgap/a")
	g.SetEndpoint("B", "cgap/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("cgap/g", g)
	proxy.HandleTask("cgap/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("cgap/b", func(ctx context.Context, f *workflow.Flow) error { bRan.Add(1); return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// Freeze A after its completion write, before the transition inserts B.
	e.seams.Break(CheckpointBeforeTransitionTx)
	fk, err := e.Create(ctx, "cgap/g", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, CheckpointBeforeTransitionTx, 10*time.Second), "engine never reached checkpoint CheckpointBeforeTransitionTx")

	// Cancel lands in the gap: A is completed and B does not exist, so the per-step mark finds nothing.
	assert.NoError(e.Cancel(ctx, fk, "gap reason"))
	e.seams.Resume(CheckpointBeforeTransitionTx)

	outcome, err := e.Await(ctx, fk)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCancelled, outcome.Status, "B must inherit the cancellation, not run the flow to completion")
	assert.Equal("gap reason", outcome.CancelReason)
	assert.Equal(int32(0), bRan.Load(), "B must be preempted, not run")
}

// TestCancelMarkSparesAHandlerInsertedAfterTheWatermark pins that Cancel's step mark stops at the watermark.
// The mark runs after the watermark commits, and in between a step can be preempted and route to its onError
// handler: the handler is inserted above the watermark, unmarked on purpose, since its source was covered.
// An unbounded mark then caught the handler and delivered the cancellation a second time - to the step
// written to catch it - and the flow settled cancelled instead of recovering.
func TestCancelMarkSparesAHandlerInsertedAfterTheWatermark(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	aStarted, aGo := make(chan struct{}), make(chan struct{})
	hStarted, hGo := make(chan struct{}), make(chan struct{})
	var bRan atomic.Int32
	proxy := NewTestProxy()
	g := workflow.NewGraph("CancelMarkWindow")
	g.SetEndpoint("A", "cmark/a")
	g.SetEndpoint("B", "cmark/b")
	g.SetEndpoint("H", "cmark/h")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	g.AddTransitionOnError("B", "H")
	g.AddTransition("H", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("cmark/g", g)
	proxy.HandleTask("cmark/a", func(ctx context.Context, f *workflow.Flow) error {
		close(aStarted)
		<-aGo
		return nil
	})
	proxy.HandleTask("cmark/b", func(ctx context.Context, f *workflow.Flow) error { bRan.Add(1); return nil })
	proxy.HandleTask("cmark/h", func(ctx context.Context, f *workflow.Flow) error {
		close(hStarted)
		<-hGo
		f.SetBool("recovered", true)
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "cmark/g", nil, nil)
	assert.NoError(err)
	<-aStarted

	// Cancel commits its watermark while A is running, and is held before it marks anything.
	e.seams.Break(CheckpointCancelBeforeMark)
	cancelled := make(chan error, 1)
	go func() { cancelled <- e.Cancel(ctx, fk, "window") }()
	assert.True(e.seams.WaitTimeout(ctx, CheckpointCancelBeforeMark, 10*time.Second), "Cancel never reached its mark")

	// A finishes unmarked; B inherits the cancellation (A is at or below the watermark and was not covered),
	// is preempted, and routes to H - inserted above the watermark, unmarked.
	close(aGo)
	select {
	case <-hStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never started")
	}

	// Now the mark runs, with H in flight.
	e.seams.Resume(CheckpointCancelBeforeMark)
	assert.NoError(<-cancelled)
	close(hGo)

	outcome, err := e.Await(ctx, fk)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(int32(0), bRan.Load(), "B was preempted")
	assert.Equal(workflow.StatusCompleted, outcome.Status, "the handler that caught the cancellation recovers the flow")
	assert.True(outcome.State.GetBool("recovered"))
}

// TestCancelOncePerFlow pins that a flow is cancelled once. A handler catches the first cancellation and the
// flow carries on; a second Cancel - a host retrying a call whose response it lost - must not reach the
// recovered flow, so it neither re-marks the step now running nor overwrites the recorded reason. Re-marking
// was what a repeat used to do, and it cancelled a flow its author had recovered.
func TestCancelOncePerFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	aStarted, aGo := make(chan struct{}), make(chan struct{})
	cStarted, cGo := make(chan struct{}), make(chan struct{})
	proxy := NewTestProxy()
	g := workflow.NewGraph("CancelOnce")
	g.SetEndpoint("A", "conce/a")
	g.SetEndpoint("H", "conce/h")
	g.SetEndpoint("C", "conce/c")
	g.AddTransition("A", workflow.END)
	g.AddTransitionOnError("A", "H")
	g.AddTransition("H", "C")
	g.AddTransition("C", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("conce/g", g)
	proxy.HandleTask("conce/a", func(ctx context.Context, f *workflow.Flow) error {
		close(aStarted)
		<-aGo
		return nil
	})
	proxy.HandleTask("conce/h", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("conce/c", func(ctx context.Context, f *workflow.Flow) error {
		close(cStarted)
		<-cGo
		f.SetBool("carriedOn", true)
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "conce/g", nil, nil)
	assert.NoError(err)
	<-aStarted
	assert.NoError(e.Cancel(ctx, fk, "first"))
	close(aGo) // A finishes covered and routes to H, which recovers the flow into C
	select {
	case <-cStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("the recovered flow never reached C")
	}

	assert.NoError(e.Cancel(ctx, fk, "second"), "a repeat is a no-op, not an error")
	close(cGo)

	outcome, err := e.Await(ctx, fk)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status, "the second Cancel must not reach the recovered flow")
	assert.True(outcome.State.GetBool("carriedOn"))
}
