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
When a task inside a subgraph fails, the failure is delivered back to the parent's
flow.Subgraph call as the err return (carried in the subgraph_error column), rather
than silently failing the parent flow. The parent task can then handle it: here the
parent returns the error and an onError transition routes to a recovery task, so the
parent flow completes. A second variant lets the error propagate and asserts the
parent flow fails with the inner error text.
*/
package fixtures

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestSubgraphfailflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Inner: X -> boom (fails)
	inner := workflow.NewGraph("subgraphfailflow.verify:428/inner")
	inner.AddTask("taskX", "subgraphfailflow.verify:428/task-x")
	inner.AddTask("boom", "subgraphfailflow.verify:428/boom")
	inner.AddTransition("taskX", "boom")
	inner.AddTransition("boom", workflow.END)
	proxy.HandleGraph("subgraphfailflow.verify:428/inner", inner)

	proxy.HandleTask("subgraphfailflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		return nil
	})
	proxy.HandleTask("subgraphfailflow.verify:428/boom", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		return errors.New("inner exploded", http.StatusInternalServerError)
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("parent_recovers_from_subgraph_error_via_on_error", func(t *testing.T) {
		assert := testarossa.For(t)

		// Parent: A -> runInner -> Z, with runInner --onError--> recover.
		parent := workflow.NewGraph("subgraphfailflow.verify:428/recoverparent")
		parent.AddTask("taskA", "subgraphfailflow.verify:428/task-a")
		parent.AddTask("runInner", "subgraphfailflow.verify:428/run-inner-recover")
		parent.AddTask("taskZ", "subgraphfailflow.verify:428/task-z")
		parent.AddTask("recover", "subgraphfailflow.verify:428/recover")
		parent.AddTransition("taskA", "runInner")
		parent.AddTransition("runInner", "taskZ")
		parent.AddTransition("taskZ", workflow.END)
		parent.AddTransitionOnError("runInner", "recover")
		parent.AddTransition("recover", workflow.END)
		proxy.HandleGraph("subgraphfailflow.verify:428/recoverparent", parent)

		proxy.HandleTask("subgraphfailflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, baggage any) error {
			return nil
		})
		proxy.HandleTask("subgraphfailflow.verify:428/run-inner-recover", func(ctx context.Context, f *workflow.Flow, baggage any) error {
			out, yield, err := f.Subgraph("subgraphfailflow.verify:428/inner", nil)
			if yield {
				return nil
			}
			if err != nil {
				// The child's failure reached us as the err return; propagate it so
				// the onError transition can route to the recovery task.
				return err
			}
			_ = out
			return nil
		})
		proxy.HandleTask("subgraphfailflow.verify:428/task-z", func(ctx context.Context, f *workflow.Flow, baggage any) error {
			f.SetString("result", "Z-should-not-run")
			return nil
		})
		proxy.HandleTask("subgraphfailflow.verify:428/recover", func(ctx context.Context, f *workflow.Flow, baggage any) error {
			var onErr errors.TracedError
			_ = f.Get("onErr", &onErr)
			f.SetString("result", "recovered: "+onErr.Error())
			return nil
		})

		flowKey, err := eng.Create(ctx, "subgraphfailflow.verify:428/recoverparent", nil, nil, nil)
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
		result, _ := outcome.State["result"].(string)
		assert.True(strings.HasPrefix(result, "recovered:"), "got %q", result)
		assert.True(strings.Contains(result, "inner exploded"), "got %q", result)
	})

	t.Run("unhandled_subgraph_error_fails_parent", func(t *testing.T) {
		assert := testarossa.For(t)

		// Parent: A -> runInner -> Z, no onError handler.
		parent := workflow.NewGraph("subgraphfailflow.verify:428/failparent")
		parent.AddTask("taskA", "subgraphfailflow.verify:428/task-a2")
		parent.AddTask("runInner", "subgraphfailflow.verify:428/run-inner-fail")
		parent.AddTask("taskZ", "subgraphfailflow.verify:428/task-z2")
		parent.AddTransition("taskA", "runInner")
		parent.AddTransition("runInner", "taskZ")
		parent.AddTransition("taskZ", workflow.END)
		proxy.HandleGraph("subgraphfailflow.verify:428/failparent", parent)

		proxy.HandleTask("subgraphfailflow.verify:428/task-a2", func(ctx context.Context, f *workflow.Flow, baggage any) error {
			return nil
		})
		proxy.HandleTask("subgraphfailflow.verify:428/run-inner-fail", func(ctx context.Context, f *workflow.Flow, baggage any) error {
			_, yield, err := f.Subgraph("subgraphfailflow.verify:428/inner", nil)
			if yield {
				return nil
			}
			return err
		})
		proxy.HandleTask("subgraphfailflow.verify:428/task-z2", func(ctx context.Context, f *workflow.Flow, baggage any) error {
			return nil
		})

		flowKey, err := eng.Create(ctx, "subgraphfailflow.verify:428/failparent", nil, nil, nil)
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
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.True(strings.Contains(outcome.Error, "inner exploded"), "got %q", outcome.Error)
	})
}
