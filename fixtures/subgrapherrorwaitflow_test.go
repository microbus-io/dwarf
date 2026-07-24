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
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphErrorWaitflow pins that a failing subgraph child wakes an Await already blocked on its (read-only)
// child key. A subgraph child is a legal introspection target, so a flow-stop must broadcast to its Await
// waiters. The child here captures its own key,
// then blocks so the test can register an Await while the child is still running; releasing it fails the child.
// The Await must be woken by the failure's signalStop, not left to time out against its context deadline.
func TestSubgraphErrorWaitflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("RunInner", "subgrapherrwait.verify:0/run-inner")
	parent.AddTransition("RunInner", workflow.END)
	proxy.HandleGraph("subgrapherrwait.verify:0/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("Boom", "subgrapherrwait.verify:0/boom")
	inner.AddTransition("Boom", workflow.END)
	proxy.HandleGraph("subgrapherrwait.verify:0/inner", inner)

	var mu sync.Mutex
	var childKey string
	captured := make(chan struct{})
	release := make(chan struct{})
	proxy.HandleTask("subgrapherrwait.verify:0/boom", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		childKey = f.FlowKey() // a subgraph child sees its own (child) flow key
		mu.Unlock()
		close(captured)
		<-release // stay running until the test has an Await registered on the child key
		return errors.New("child boom", http.StatusInternalServerError)
	})
	proxy.HandleTask("subgrapherrwait.verify:0/run-inner", func(ctx context.Context, f *workflow.Flow) error {
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
	case <-time.After(5 * time.Second):
		assert.True(false, "inner (child) task never ran")
		return
	}
	mu.Lock()
	child := childKey
	mu.Unlock()
	assert.True(child != "", "expected the inner task to capture the child flow key")

	// Register an Await on the child key while the child is still running: it must block, then be woken by the
	// failure's signalStop. The ctx is generous so it never gates the outcome; the wake bound below is what the
	// test asserts on.
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 30*time.Second)
	defer cancelAwait()
	awaitCh := make(chan outcome, 1)
	go func() {
		o, e := eng.Await(awaitCtx, child)
		awaitCh <- outcome{o, e}
	}()

	// Give the Await goroutine time to register its waiter and block on the (still-running) child.
	time.Sleep(100 * time.Millisecond)
	close(release) // the child now fails via failStep's subgraph-child path, which must signalStop the child

	// The wake bound is deliberately well below awaitPollInterval (5s): signalStop returns in ms, so this proves
	// the *signal* woke Await, not the 5s lost-wake poll backstop that would mask a missing signalStop.
	select {
	case aw := <-awaitCh:
		assert.NoError(aw.err, "Await(childKey) must be woken by the failure's signalStop, not time out")
		if assert.NotNil(aw.out) {
			assert.Equal(workflow.StatusFailed, aw.out.Status)
			assert.Equal("child boom", aw.out.Error)
		}
	case <-time.After(2 * time.Second):
		assert.True(false, "Await(childKey) not woken within 2s — a failing subgraph child did not signalStop its Await waiters")
		return
	}

	// The parent flow (root) fails too, since flow.Subgraph surfaces the child error and the caller returns it.
	root := <-rootCh
	assert.NoError(root.err)
	if assert.NotNil(root.out) {
		assert.Equal(workflow.StatusFailed, root.out.Status)
	}
}
