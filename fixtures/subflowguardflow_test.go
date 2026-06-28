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

// TestSubflowGuardflow pins pure Option 1: a subgraph child flow's key is read-only. Lifecycle mutations
// (Resume/Cancel/Delete/Continue) reject it with 400 - they act on the whole flow/tree and must be addressed
// by the root key - while introspection (Snapshot/History) still works on the child key.
func TestSubflowGuardflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	// Parent: A -> RunInner. Inner: X (captures its own - the child's - flow key).
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("TaskA", "subflowguard.verify:428/task-a")
	parent.SetEndpoint("RunInner", "subflowguard.verify:428/run-inner")
	parent.AddTransitionChain("TaskA", "RunInner", workflow.END)
	proxy.HandleGraph("subflowguard.verify:428/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("TaskX", "subflowguard.verify:428/task-x")
	inner.AddTransitionChain("TaskX", workflow.END)
	proxy.HandleGraph("subflowguard.verify:428/inner", inner)

	var mu sync.Mutex
	var childKey string
	proxy.HandleTask("subflowguard.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("subflowguard.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		childKey = f.FlowKey() // a subgraph child sees its own (child) flow key
		mu.Unlock()
		f.SetString("inner", "done")
		return nil
	})
	proxy.HandleTask("subflowguard.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("subflowguard.verify:428/inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	rootKey, outcome, err := eng.Run(ctx, "subflowguard.verify:428/parent", map[string]any{}, nil)
	assert := testarossa.For(t)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	mu.Lock()
	child := childKey
	mu.Unlock()
	assert.True(child != "", "expected the inner task to capture the child flow key")
	assert.True(child != rootKey, "child key must differ from the root key")

	t.Run("introspection_allowed_on_child", func(t *testing.T) {
		assert := testarossa.For(t)
		snap, err := eng.Snapshot(ctx, child)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, snap.Status)
		steps, err := eng.History(ctx, child)
		assert.NoError(err)
		assert.True(len(steps) > 0, "child history should have steps")
	})

	t.Run("mutations_rejected_on_child", func(t *testing.T) {
		assert := testarossa.For(t)
		err := eng.Resume(ctx, child, nil)
		assert.Error(err)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

		err = eng.Cancel(ctx, child, "nope")
		assert.Error(err)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

		err = eng.Delete(ctx, child)
		assert.Error(err)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

		_, err = eng.Continue(ctx, child, nil)
		assert.Error(err)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	})

	t.Run("child_survives_rejected_mutations", func(t *testing.T) {
		assert := testarossa.For(t)
		// The child was neither deleted nor cancelled by the rejected calls above.
		snap, err := eng.Snapshot(ctx, child)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, snap.Status)
	})

	t.Run("list_locates_subgraphs_by_url_and_marks_them", func(t *testing.T) {
		assert := testarossa.For(t)

		// Default List by the inner URL finds nothing - subgraph children are excluded by default.
		roots, _, err := eng.List(ctx, workflow.Query{WorkflowURL: "subflowguard.verify:428/inner"})
		assert.NoError(err)
		assert.Equal(0, len(roots))

		// IncludeSubgraphs + the inner URL (which only ever runs as a subgraph) locates the child, marked
		// Subgraph=true.
		subs, _, err := eng.List(ctx, workflow.Query{WorkflowURL: "subflowguard.verify:428/inner", IncludeSubgraphs: true})
		assert.NoError(err)
		assert.Equal(1, len(subs))
		if len(subs) == 1 {
			assert.Equal(child, subs[0].FlowKey)
			assert.True(subs[0].Subgraph, "child should be marked Subgraph=true")
		}

		// The parent (root) lists by default and is marked Subgraph=false.
		parents, _, err := eng.List(ctx, workflow.Query{WorkflowURL: "subflowguard.verify:428/parent"})
		assert.NoError(err)
		assert.Equal(1, len(parents))
		if len(parents) == 1 {
			assert.Equal(rootKey, parents[0].FlowKey)
			assert.True(!parents[0].Subgraph, "root should be marked Subgraph=false")
		}
	})
}
