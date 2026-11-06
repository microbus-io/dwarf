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
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphErrorWaitflow pins that a failing subgraph child raises signalStop for its (read-only) child key
// and that an Await already blocked on that key returns the failure. A subgraph child is a legal introspection
// target, so its flow-stop must be signalled to its Await waiters. The child here captures its own key, then
// blocks so the test can register an Await while the child is still running; releasing it fails the child. The
// Await must return that failure rather than be left to time out against its context deadline.
func TestSubgraphErrorWaitflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	parent := workflow.NewGraph("Parent")
	parent.SetURL("RunInner", "subgrapherrwait.verify:0/run-inner")
	parent.AddTransition("RunInner", workflow.End)
	mux.HandleGraph("subgrapherrwait.verify:0/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetURL("Boom", "subgrapherrwait.verify:0/boom")
	inner.AddTransition("Boom", workflow.End)
	mux.HandleGraph("subgrapherrwait.verify:0/inner", inner)

	// Execution is at-least-once, so the capture is once-only: a re-executed Boom must not close captured twice.
	var mu sync.Mutex
	var childKey string
	captured := make(chan struct{})
	var captureOnce sync.Once
	// The release is deferred below the Shutdown defer so it unwinds first: an early return otherwise leaves Boom
	// holding a worker the drain waits on forever.
	release := make(chan struct{})
	releaseBoom := sync.OnceFunc(func() { close(release) })
	defer releaseBoom()
	mux.HandleTask("subgrapherrwait.verify:0/boom", func(ctx context.Context, f *workflow.Flow) error {
		captureOnce.Do(func() {
			mu.Lock()
			childKey = f.FlowKey() // a subgraph child sees its own (child) flow key
			mu.Unlock()
			close(captured)
		})
		<-release // stay running until the test has an Await registered on the child key
		return errors.New("child boom", http.StatusInternalServerError)
	})
	mux.HandleTask("subgrapherrwait.verify:0/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("subgrapherrwait.verify:0/inner", map[string]any{}, &out)
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
		_, o, e := eng.Run(ctx, "subgrapherrwait.verify:0/parent", map[string]any{}, nil)
		rootCh <- outcome{o, e}
	}()

	// Wait until the child has captured its key and is blocked (so it is still running).
	select {
	case <-captured:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "inner (child) task never ran")
		return
	}
	mu.Lock()
	child := childKey
	mu.Unlock()
	if !assert.True(child != "", "expected the inner task to capture the child flow key") {
		return
	}

	// The stop is waited for as an event, not a duration: everything between the release and the wake - the task
	// returning, failStep committing, the Await's read - is database work, and a 2s bound on it was outlasted by
	// a loaded SQL Server. The targeted checkpoint fires at the top of signalStop, post-commit, so its arrival
	// proves failStep's subgraph-child path raised signalStop for the child's key as failed. That is the half of
	// the wake the Await's return cannot show, because the latch detector would also find the committed stop.
	// Delivery from signalStop to a parked caller is internal/latch's TestRelease_WakesWithoutASweep, and the
	// detector-only case is TestSubgraphChildStopSignal_DroppedThenBackstopped. Every wait below is a hang
	// ceiling and nothing else. Armed before anything can fail the child.
	stopped := engineseams.Seams(eng).Waiter(engineseams.Join(engineseams.CheckpointFlowStopped, child, workflow.StatusFailed))

	// Register an Await on the child key while the child is still running: it must block until the child fails.
	// The ctx outlasts the hang ceilings below, so it is never what returns the Await.
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 60*time.Second*enginetest.TimeoutScale())
	defer cancelAwait()
	awaitCh := make(chan outcome, 1)
	parked := engineseams.Seams(eng).Waiter(engineseams.Join(engineseams.CheckpointAwaitParked, child)) // armed before the goroutine that trips it
	go func() {
		o, e := eng.Await(awaitCtx, child)
		awaitCh <- outcome{o, e}
	}()

	// The Await must be ON the board before the child fails, or the test proves nothing: `await` reads once
	// before parking, so an Await that registers after the failure is answered by its own read and the wake
	// path under test never runs - silently, with every assertion below still passing.
	select {
	case <-parked:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "the Await never parked on the child, so no blocked waiter was there to be woken")
		return
	}
	releaseBoom() // the child now fails via failStep's subgraph-child path, which must signalStop the child
	select {
	case <-stopped:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "the child's failure never raised signalStop for a failed stop on its key")
		return
	}
	select {
	case aw := <-awaitCh:
		assert.NoError(aw.err, "Await(childKey) must return the child's failure, not time out")
		if assert.NotNil(aw.out) {
			assert.Equal(workflow.StatusFailed, aw.out.Status)
			assert.Equal("child boom", aw.out.Error)
		}
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await(childKey) never returned, though signalStop was raised for the child's failed stop")
		return
	}

	// The parent flow (root) fails too, since flow.Subgraph surfaces the child error and the caller returns it.
	select {
	case root := <-rootCh:
		assert.NoError(root.err)
		if assert.NotNil(root.out) {
			assert.Equal(workflow.StatusFailed, root.out.Status)
		}
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "parent flow did not stop")
	}
}
