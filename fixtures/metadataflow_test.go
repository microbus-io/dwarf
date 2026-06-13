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
The opaque metadata map captured at Create is stored on the flow row and passed,
unchanged, to every GraphLoader and TaskExecutor call for the flow's lifetime —
including subgraph flows, which inherit the parent's metadata. The engine never
interprets it. This wires capturing loader/executor shims to assert metadata
reaches every call site identically, across a subgraph boundary.
*/
package fixtures

import (
	"context"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestMetadataflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Parent: A -> runInner -> END; Inner: X -> END.
	parent := workflow.NewGraph("metadataflow.verify:428/parent")
	parent.AddTask("taskA", "metadataflow.verify:428/task-a")
	parent.AddTask("runInner", "metadataflow.verify:428/run-inner")
	parent.AddTransition("taskA", "runInner")
	parent.AddTransition("runInner", workflow.END)
	proxy.HandleGraph("metadataflow.verify:428/parent", parent)

	inner := workflow.NewGraph("metadataflow.verify:428/inner")
	inner.AddTask("taskX", "metadataflow.verify:428/task-x")
	inner.AddTransition("taskX", workflow.END)
	proxy.HandleGraph("metadataflow.verify:428/inner", inner)

	proxy.HandleTask("metadataflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		return nil
	})
	proxy.HandleTask("metadataflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		return nil
	})
	proxy.HandleTask("metadataflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		_, yield, err := f.Subgraph("metadataflow.verify:428/inner", nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// Capturing shims record the metadata seen at each loader/executor call.
	var mu sync.Mutex
	seenLoad := map[string]map[string]any{}
	seenTask := map[string]map[string]any{}
	loader := func(ctx context.Context, name string, md map[string]any) (*workflow.Graph, error) {
		mu.Lock()
		seenLoad[name] = md
		mu.Unlock()
		return proxy.LoadGraph(ctx, name, md)
	}
	executor := func(ctx context.Context, taskName string, f *workflow.Flow, md map[string]any) error {
		mu.Lock()
		seenTask[taskName] = md
		mu.Unlock()
		return proxy.ExecuteTask(ctx, taskName, f, md)
	}

	eng := engine.NewEngine().
		WithGraphLoader(loader).
		WithTaskExecutor(executor)
	eng.RunInTest(t)

	t.Run("metadata_reaches_loader_and_every_task_including_subgraph", func(t *testing.T) {
		assert := testarossa.For(t)

		md := map[string]any{"actor": "alice", "scope": "s1"}
		flowKey, err := eng.Create(ctx, "metadataflow.verify:428/parent", nil, md, nil)
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(eng.Start(ctx, flowKey)) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		mu.Lock()
		defer mu.Unlock()

		// Loader saw metadata for the parent graph and the subgraph.
		assert.Equal("alice", seenLoad["metadataflow.verify:428/parent"]["actor"])
		assert.Equal("alice", seenLoad["metadataflow.verify:428/inner"]["actor"])

		// Every task — parent tasks and the inherited subgraph task — saw it.
		for _, task := range []string{
			"metadataflow.verify:428/task-a",
			"metadataflow.verify:428/run-inner",
			"metadataflow.verify:428/task-x",
		} {
			got := seenTask[task]
			if !assert.NotNil(got, "task %s never received metadata", task) {
				continue
			}
			assert.Equal("alice", got["actor"], "task %s", task)
			assert.Equal("s1", got["scope"], "task %s", task)
		}
	})
}
