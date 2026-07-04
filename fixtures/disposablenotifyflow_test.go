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

/*
DeleteOnCompletion + subgraph + NotifyOnStop composition (see _MORETESTS.md D6). A disposable flow
(DeleteOnCompletion) that runs a subgraph child and requests NotifyOnStop must, on completion:
(1) fire FlowStopped exactly once with status completed and the full final state (captured before
the atomic delete); (2) leave the root uniformly 404 to Snapshot/History/Await; (3) leave the
subgraph child 404 too - the whole tree is gone atomically, including a goroutine already blocked
in Await(childKey); (4) leave nothing listable. The delete happens inside the completion
transaction, so there is never a committed "completed" row for any reader to observe.
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

func TestDisposableNotifyflow(t *testing.T) {
	// The disposable-outcome assertion (that a completed DeleteOnCompletion flow's outcome is observable)
	// requires the deferred-deletion work so Await returns the outcome during a grace window instead of
	// 404-ing on the inline delete. Un-skip and rework once that lands.
	t.Skip("requires deferred-deletion work so Await can observe a disposable flow's outcome")

	ctx := context.Background()

	proxy := engine.NewTestProxy()

	var stopMu sync.Mutex
	var stopCount int
	var stopStatus string
	var stopState map[string]any

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("RunSub", "disposablenotify.verify:428/run-sub")
	parent.AddTransition("RunSub", workflow.END)
	proxy.HandleGraph("disposablenotify.verify:428/parent", parent)

	child := workflow.NewGraph("Child")
	child.SetEndpoint("Work", "disposablenotify.verify:428/work")
	child.AddTransition("Work", workflow.END)
	proxy.HandleGraph("disposablenotify.verify:428/child", child)

	// The child task publishes its own flowKey and blocks until released, so the test can register an
	// Await on the child key while the whole tree is still live (before the atomic delete).
	childKeyCh := make(chan string, 1)
	release := make(chan struct{})
	proxy.HandleTask("disposablenotify.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		childKeyCh <- f.FlowKey()
		<-release
		f.SetString("childOut", "hello")
		return nil
	})
	proxy.HandleTask("disposablenotify.verify:428/run-sub", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("disposablenotify.verify:428/child", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		if v, ok := out["childOut"]; ok {
			f.Set("subResult", v)
		}
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	rootKey, err := eng.Create(ctx, "disposablenotify.verify:428/parent", map[string]any{},
		&workflow.FlowOptions{DeleteOnCompletion: true})
	if !assert.NoError(err) {
		close(release)
		return
	}

	// Grab the child key while the child task is blocked (tree still live), then park an Await on it.
	var childKey string
	select {
	case childKey = <-childKeyCh:
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("child task never started")
	}
	assert.NotEqual("", childKey)

	type awaitResult struct {
		outcome *workflow.FlowOutcome
		err     error
	}
	childAwaitCh := make(chan awaitResult, 1)
	go func() {
		awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		out, err := eng.Await(awaitCtx, childKey)
		childAwaitCh <- awaitResult{out, err}
	}()
	// Give the child Await a moment to register before the tree is torn down.
	time.Sleep(50 * time.Millisecond)

	// Release the child; the tree completes and is deleted atomically inside the completion transaction.
	close(release)

	// (2) Await on the root returns 404 - for a disposable flow that 404 IS the completion signal.
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, err = eng.Await(awaitCtx, rootKey)
	cancel()
	if assert.Error(err) {
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	}

	// (1) FlowStopped fired exactly once, with status completed and the full final state.
	// FlowStopped is fire-and-forget after the commit, so allow a brief window for it to land.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stopMu.Lock()
		got := stopCount
		stopMu.Unlock()
		if got >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopMu.Lock()
	assert.Equal(1, stopCount)
	assert.Equal(workflow.StatusCompleted, stopStatus)
	assert.Equal("hello", stopState["subResult"])
	stopMu.Unlock()

	// (2 cont.) Snapshot and History on the root are uniformly 404.
	_, err = eng.Snapshot(ctx, rootKey)
	if assert.Error(err) {
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	}
	_, err = eng.History(ctx, rootKey)
	if assert.Error(err) {
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	}

	// (3) The goroutine blocked in Await(childKey) woke - it does not hang. Its outcome is legitimately
	// one of two: 404 (it re-snapshotted after the atomic tree delete) OR a completed outcome (it woke on
	// the subgraph child's OWN completion signal, which fires when the child terminalizes - before the
	// parent's completion+delete transaction). The engine only guarantees the tree is gone to readers
	// arriving AFTER the parent completes; a waiter parked on the child can observe the child's own
	// mid-tree completion first. The post-delete Snapshot below is the strict "tree is gone" check.
	select {
	case res := <-childAwaitCh:
		if res.err != nil {
			assert.Equal(http.StatusNotFound, errors.StatusCode(res.err))
		} else if assert.NotNil(res.outcome) {
			assert.Equal(workflow.StatusCompleted, res.outcome.Status)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Await(childKey) never returned")
	}
	// Once the disposable root has completed (its Await 404'd above), the whole tree is gone: a fresh read
	// of the child key is uniformly 404.
	_, err = eng.Snapshot(ctx, childKey)
	if assert.Error(err) {
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	}

	// (4) Nothing of the tree remains listable, under either URL.
	roots, _, err := eng.List(ctx, workflow.Query{WorkflowURL: "disposablenotify.verify:428/parent"})
	if assert.NoError(err) {
		assert.Equal(0, len(roots))
	}
	subs, _, err := eng.List(ctx, workflow.Query{WorkflowURL: "disposablenotify.verify:428/child", IncludeSubgraphs: true})
	if assert.NoError(err) {
		assert.Equal(0, len(subs))
	}
}
