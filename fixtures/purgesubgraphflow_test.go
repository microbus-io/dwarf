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

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestPurgeSubgraphflow pins that Purge deletes a matched root's whole subgraph subtree - the child flow and
// its steps, not just the root - and that Purge rejects Query.IncludeSubgraphs with 400 (a subgraph child is
// never purged independently).
func TestPurgeSubgraphflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("TaskA", "purgesub.verify:428/task-a")
	parent.SetEndpoint("RunInner", "purgesub.verify:428/run-inner")
	parent.AddTransitionChain("TaskA", "RunInner", workflow.END)
	proxy.HandleGraph("purgesub.verify:428/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("TaskX", "purgesub.verify:428/task-x")
	inner.AddTransitionChain("TaskX", workflow.END)
	proxy.HandleGraph("purgesub.verify:428/inner", inner)

	var mu sync.Mutex
	var childKey string
	proxy.HandleTask("purgesub.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("purgesub.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		childKey = f.FlowKey()
		mu.Unlock()
		return nil
	})
	proxy.HandleTask("purgesub.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("purgesub.verify:428/inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	rootKey, outcome, err := eng.Run(ctx, "purgesub.verify:428/parent", map[string]any{}, nil)
	assert := testarossa.For(t)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	mu.Lock()
	child := childKey
	mu.Unlock()
	assert.True(child != "", "inner task should capture the child flow key")

	// The child exists before the purge.
	includeSubs := workflow.Query{WorkflowURL: "purgesub.verify:428/inner", IncludeSubgraphs: true}
	subs, _, err := eng.List(ctx, includeSubs)
	assert.NoError(err)
	assert.Equal(1, len(subs))

	t.Run("purge_rejects_include_subgraphs", func(t *testing.T) {
		assert := testarossa.For(t)
		_, err := eng.Purge(ctx, workflow.Query{WorkflowURL: "purgesub.verify:428/inner", IncludeSubgraphs: true})
		assert.Error(err)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	})

	t.Run("purge_root_deletes_whole_tree", func(t *testing.T) {
		assert := testarossa.For(t)
		deleted, err := eng.Purge(ctx, workflow.Query{WorkflowURL: "purgesub.verify:428/parent"})
		assert.NoError(err)
		assert.Equal(1, deleted) // the count is the root flow, not its descendants

		// The root is gone.
		_, err = eng.Snapshot(ctx, rootKey)
		assert.Error(err)
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))

		// The subgraph child (flow and steps) is gone too - the bug was that it was left orphaned.
		_, err = eng.Snapshot(ctx, child)
		assert.Error(err)
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
		_, err = eng.History(ctx, child)
		assert.Error(err)
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))

		// Nothing remains under either URL.
		subs, _, err := eng.List(ctx, includeSubs)
		assert.NoError(err)
		assert.Equal(0, len(subs))
		roots, _, err := eng.List(ctx, workflow.Query{WorkflowURL: "purgesub.verify:428/parent"})
		assert.NoError(err)
		assert.Equal(0, len(roots))
	})
}
