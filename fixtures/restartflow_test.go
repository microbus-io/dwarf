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
Restart re-runs a terminal flow from its entry point as a fresh attempt: every
step below the entry is deleted, the entry step is reset to pending, and the flow
transitions straight to running (no Start needed). State overrides are merged onto
the entry step's snapshot. This covers the full Restart operation (distinct from
RestartFrom, which only rewinds a chosen subtree).
*/
package fixtures

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestRestartflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("restartflow.verify:428/restart")
	graph.AddTask("taskA", "restartflow.verify:428/task-a")
	graph.AddTask("taskB", "restartflow.verify:428/task-b")
	graph.AddTransition("taskA", "taskB")
	graph.AddTransition("taskB", workflow.END)
	proxy.HandleGraph("restartflow.verify:428/restart", graph)

	// Counts how many times the entry task body runs across the whole flow lifetime.
	var entryRuns atomic.Int64

	proxy.HandleTask("restartflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		entryRuns.Add(1)
		f.SetString("path", "A")
		return nil
	})
	proxy.HandleTask("restartflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetString("path", f.GetString("path")+"B")
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("restart_reruns_from_entry_with_override", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "restartflow.verify:428/restart", map[string]any{"seed": "first"}, nil, nil)
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
		assert.Equal("AB", outcome.State["path"])
		assert.Equal(int64(1), entryRuns.Load())
		// Original entry seed survives the first run.
		assert.Equal("first", outcome.State["seed"])

		// Restart with a state override merged onto the entry snapshot.
		if !assert.NoError(eng.Restart(ctx, flowKey, map[string]any{"seed": "second"})) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("AB", outcome.State["path"])
		// The entry task ran again -> a true re-run from the entry point.
		assert.Equal(int64(2), entryRuns.Load())
		// The override replaced the entry seed.
		assert.Equal("second", outcome.State["seed"])
	})

	t.Run("restart_rejects_non_terminal_flow", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "restartflow.verify:428/restart", nil, nil, nil)
		if !assert.NoError(err) {
			return
		}
		// A created (never-started, non-terminal) flow cannot be restarted.
		err = eng.Restart(ctx, flowKey, nil)
		assert.Error(err)
		assert.Equal(http.StatusConflict, errors.StatusCode(err))
	})
}
