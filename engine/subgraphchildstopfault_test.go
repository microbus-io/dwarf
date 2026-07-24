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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphChildStopSignal_DroppedThenBackstopped is the dropped-wake variant of
// TestSubgraphErrorWaitflow. That fixture proves a failing subgraph child's signalStop wakes an Await blocked
// on its (read-only) child key promptly, well under awaitPollInterval. Here we arm FaultDropSignalStop so the
// child's terminal wake is LOST (the first signalStop consult is the child's failure; the fault fires once and
// is consumed there, so the root's later wake is delivered normally). The child-key Await must STILL return -
// via the periodic re-snapshot backstop - bounding the hang to one awaitPollInterval rather than the caller's
// ctx deadline. This proves the child-key introspection contract survives a lost wake, not just a delivered one.
func TestSubgraphChildStopSignal_DroppedThenBackstopped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("RunInner", "scss/run-inner")
	parent.AddTransition("RunInner", workflow.END)
	assert.NoError(parent.Validate())
	proxy.HandleGraph("scss/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("Boom", "scss/boom")
	inner.AddTransition("Boom", workflow.END)
	assert.NoError(inner.Validate())
	proxy.HandleGraph("scss/inner", inner)

	var mu sync.Mutex
	var childKey string
	captured := make(chan struct{})
	release := make(chan struct{})
	proxy.HandleTask("scss/boom", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		childKey = f.FlowKey() // a subgraph child sees its own (child) flow key
		mu.Unlock()
		close(captured)
		<-release // stay running until the test has an Await registered on the child key
		return errors.New("child boom", http.StatusInternalServerError)
	})
	proxy.HandleTask("scss/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("scss/inner", map[string]any{}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	e.awaitPollInterval = 20 * time.Millisecond // the re-snapshot backstop must fire fast for the test
	assert.NoError(e.Startup(t.Context()))

	// Run the parent (root) in the background; Run blocks until the root flow stops.
	rootDone := make(chan struct{})
	go func() {
		defer close(rootDone)
		e.Run(ctx, "scss/parent", map[string]any{}, nil)
	}()

	// Wait until the child captured its key and is blocked (still running).
	select {
	case <-captured:
	case <-time.After(10 * time.Second):
		assert.True(false, "inner (child) task never ran")
		return
	}
	mu.Lock()
	child := childKey
	mu.Unlock()
	assert.True(child != "", "expected the inner task to capture the child flow key")

	// Register an Await on the child key while the child is still running, so it blocks and then must be woken.
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 30*time.Second)
	defer cancelAwait()
	type outcome struct {
		out *workflow.FlowOutcome
		err error
	}
	awaitCh := make(chan outcome, 1)
	go func() {
		o, err := e.Await(awaitCtx, child)
		awaitCh <- outcome{o, err}
	}()

	// Let the Await goroutine register its waiter and block on the still-running child.
	time.Sleep(100 * time.Millisecond)

	// Drop the child's terminal wake: with FaultDropSignalStop armed, the child's failStep signalStop delivers
	// nothing. The fault fires once and is consumed on the child's (first) stop, so the root's later wake is
	// unaffected.
	e.seams.Inject(FaultDropSignalStop)
	close(release) // the child now fails via failStep's subgraph-child path, whose signalStop is dropped

	// Despite the lost signal, Await(childKey) must still return - the only remaining wake path is the periodic
	// re-snapshot backstop. A generous bound (far above awaitPollInterval, well below the 30s Await ctx) proves
	// it was the backstop, not a ctx-deadline timeout, that unblocked it.
	select {
	case aw := <-awaitCh:
		assert.NoError(aw.err, "Await(childKey) must be recovered by the re-snapshot backstop, not time out")
		if assert.NotNil(aw.out) {
			assert.Equal(workflow.StatusFailed, aw.out.Status)
			assert.Equal("child boom", aw.out.Error)
		}
	case <-time.After(10 * time.Second):
		assert.True(false, "Await(childKey) not woken after a dropped signalStop — the re-snapshot backstop did not recover it")
		return
	}

	// The root flow fails too (the child error surfaces through flow.Subgraph); its wake was not dropped.
	select {
	case <-rootDone:
	case <-time.After(10 * time.Second):
		assert.True(false, "root Run never returned")
		return
	}
	enginetest.AssertInvariants(t, e)
}
