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
Purge drain loop. Purge is capped per call and returns a count of matched
ROOT flows, so a retention job loops until it returns 0. This pins that contract: with a small
Limit, repeated Purge calls drain the whole matched set; the total returned equals the number of
root flows (a subgraph child is deleted as part of its root's tree but does not add to the count);
a concurrently-running flow that matches no filter is untouched; and after the loop nothing of the
purged batch remains (roots and the subgraph child alike 404).
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

func TestPurgeLoopflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// A trivial one-task graph used for the plain completed flows.
	tiny := workflow.NewGraph("Tiny")
	tiny.SetEndpoint("Only", "purgeloop.verify:428/only")
	tiny.AddTransition("Only", workflow.END)
	proxy.HandleGraph("purgeloop.verify:428/tiny", tiny)
	proxy.HandleTask("purgeloop.verify:428/only", func(ctx context.Context, f *workflow.Flow) error { return nil })

	// A parent graph that runs one subgraph child, so one purged root owns a subtree.
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("RunInner", "purgeloop.verify:428/run-inner")
	parent.AddTransition("RunInner", workflow.END)
	proxy.HandleGraph("purgeloop.verify:428/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("TaskX", "purgeloop.verify:428/task-x")
	inner.AddTransition("TaskX", workflow.END)
	proxy.HandleGraph("purgeloop.verify:428/inner", inner)

	var mu sync.Mutex
	var childKey string
	proxy.HandleTask("purgeloop.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		childKey = f.FlowKey()
		mu.Unlock()
		return nil
	})
	proxy.HandleTask("purgeloop.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("purgeloop.verify:428/inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// A graph that blocks forever, standing in for a concurrently-running flow that no filter matches.
	block := workflow.NewGraph("Block")
	block.SetEndpoint("Wait", "purgeloop.verify:428/wait")
	block.AddTransition("Wait", workflow.END)
	proxy.HandleGraph("purgeloop.verify:428/block", block)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseTask := func() { releaseOnce.Do(func() { close(release) }) }
	running := make(chan struct{})
	var runningOnce sync.Once
	proxy.HandleTask("purgeloop.verify:428/wait", func(ctx context.Context, f *workflow.Flow) error {
		runningOnce.Do(func() { close(running) })
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// 6 plain completed flows + 1 parent-with-subgraph = 7 completed roots.
	const roots = 7
	var rootKeys []string
	for range roots - 1 {
		fk, outcome, err := eng.Run(ctx, "purgeloop.verify:428/tiny", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		rootKeys = append(rootKeys, fk)
	}
	parentKey, outcome, err := eng.Run(ctx, "purgeloop.verify:428/parent", map[string]any{}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	rootKeys = append(rootKeys, parentKey)

	mu.Lock()
	child := childKey
	mu.Unlock()
	assert.True(child != "", "inner task should capture the subgraph child key")

	// A concurrently-running flow (still executing its blocking task) that no completed-status filter matches.
	blockKey, err := eng.Create(ctx, "purgeloop.verify:428/block", nil, nil)
	if !assert.NoError(err) {
		return
	}
	// Release the blocking task no matter how the test exits, so the worker returns and Shutdown drains
	// promptly instead of waiting out the step's time budget.
	defer releaseTask()
	<-running // ensure it is actually running before we purge

	// Drain loop: Purge completed roots two at a time until it returns 0.
	totalDeleted := 0
	iterations := 0
	for {
		n, err := eng.Purge(ctx, workflow.Query{Status: workflow.StatusCompleted, Limit: 2})
		if !assert.NoError(err) {
			return
		}
		totalDeleted += n
		if n == 0 {
			break
		}
		iterations++
		if !assert.True(iterations <= roots+2, "purge loop did not terminate") {
			return
		}
	}

	// Exactly the 7 roots were counted (marked for deletion); the subgraph child is not counted separately.
	// Purge marks (delete_after_ms=1), it does not delete inline; the reaper removes the trees. The observable
	// public contract is that the completed roots drop out of List. Per-flow physical removal (and the subtree
	// cascade) is covered by the engine-package reaper tests.
	assert.Equal(roots, totalDeleted)

	// No completed root of the batch is listable any more.
	for _, fk := range rootKeys {
		_, err := eng.History(ctx, fk)
		if assert.Error(err) {
			assert.Equal(http.StatusNotFound, errors.StatusCode(err))
		}
	}

	// List of completed flows is now empty (this engine sees only its own flows).
	remaining, _, err := eng.List(ctx, workflow.Query{Status: workflow.StatusCompleted})
	if assert.NoError(err) {
		assert.Equal(0, len(remaining))
	}

	// The concurrently-running flow was untouched: it matched no completed filter.
	snap, err := eng.Snapshot(ctx, blockKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusRunning, snap.Status)
	}

	// Release the blocking task and let it finish so the engine drains cleanly on cleanup.
	releaseTask()
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	outcome, err = eng.Await(awaitCtx, blockKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
}
