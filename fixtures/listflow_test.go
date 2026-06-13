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
List queries flows by status / workflow name, newest first, with opaque cursor
pagination. This covers the List operation and the FlowSummary it returns, plus
walking a multi-page result to exhaustion without overlap.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestListflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("listflow.verify:428/list")
	graph.AddTask("only", "listflow.verify:428/only")
	graph.AddTransition("only", workflow.END)
	proxy.HandleGraph("listflow.verify:428/list", graph)

	proxy.HandleTask("listflow.verify:428/only", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		f.SetString("done", "yes")
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	const total = 5
	created := make(map[string]bool, total)
	for range total {
		flowKey, err := eng.Create(ctx, "listflow.verify:428/list", nil, nil, nil)
		testarossa.NoError(t, err)
		testarossa.NoError(t, eng.Start(ctx, flowKey))
		outcome, err := eng.Await(ctx, flowKey)
		testarossa.NoError(t, err)
		testarossa.Equal(t, workflow.StatusCompleted, outcome.Status)
		created[flowKey] = true
	}

	t.Run("list_by_workflow_name_returns_all", func(t *testing.T) {
		assert := testarossa.For(t)

		flows, _, err := eng.List(ctx, workflow.Query{WorkflowName: "listflow.verify:428/list"})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(total, len(flows))
		for _, fs := range flows {
			assert.Equal("listflow.verify:428/list", fs.WorkflowName)
			assert.Equal(workflow.StatusCompleted, fs.Status)
			assert.Equal("only", fs.TaskName)
			assert.True(created[fs.FlowKey])
			assert.False(fs.CreatedAt.IsZero())
		}
	})

	t.Run("list_by_status_completed", func(t *testing.T) {
		assert := testarossa.For(t)

		flows, _, err := eng.List(ctx, workflow.Query{
			WorkflowName: "listflow.verify:428/list",
			Status:       workflow.StatusCompleted,
		})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(total, len(flows))
	})

	t.Run("cursor_pagination_walks_all_pages_without_overlap", func(t *testing.T) {
		assert := testarossa.For(t)

		seen := map[string]bool{}
		cursor := ""
		pages := 0
		for {
			flows, next, err := eng.List(ctx, workflow.Query{
				WorkflowName: "listflow.verify:428/list",
				Limit:        2,
				Cursor:       cursor,
			})
			if !assert.NoError(err) {
				return
			}
			pages++
			for _, fs := range flows {
				assert.False(seen[fs.FlowKey], "flow %s appeared on two pages", fs.FlowKey)
				seen[fs.FlowKey] = true
			}
			if next == "" {
				break
			}
			cursor = next
			if !assert.True(pages <= total+2, "pagination did not terminate") {
				return
			}
		}
		// Every created flow surfaced exactly once across the pages.
		assert.Equal(total, len(seen))
		for fk := range created {
			assert.True(seen[fk], "flow %s missing from paginated results", fk)
		}
	})

	// Runs last: it adds flows under the same workflow name, which would change the by-workflow-name
	// counts the earlier subtests assert.
	t.Run("filter_by_fairness_key_and_priority", func(t *testing.T) {
		assert := testarossa.For(t)

		// Two flows in fairness key "tenantA" at priority 7, one in "tenantB" at priority 9.
		for range 2 {
			fk, err := eng.Create(ctx, "listflow.verify:428/list", nil, nil, &workflow.FlowOptions{FairnessKey: "tenantA", Priority: 7})
			if !assert.NoError(err) {
				return
			}
			assert.NoError(eng.Start(ctx, fk))
			_, err = eng.Await(ctx, fk)
			assert.NoError(err)
		}
		fkB, err := eng.Create(ctx, "listflow.verify:428/list", nil, nil, &workflow.FlowOptions{FairnessKey: "tenantB", Priority: 9})
		if !assert.NoError(err) {
			return
		}
		assert.NoError(eng.Start(ctx, fkB))
		_, err = eng.Await(ctx, fkB)
		assert.NoError(err)

		// FairnessKey narrows to that key only.
		flows, _, err := eng.List(ctx, workflow.Query{FairnessKey: "tenantA"})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(2, len(flows))

		// Priority combines with FairnessKey for an exact slice.
		flows, _, err = eng.List(ctx, workflow.Query{FairnessKey: "tenantB", Priority: 9})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(1, len(flows))
		assert.Equal(fkB, flows[0].FlowKey)

		// A key/priority combination with no flows yields nothing.
		flows, _, err = eng.List(ctx, workflow.Query{FairnessKey: "tenantA", Priority: 9})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(0, len(flows))
	})
}
