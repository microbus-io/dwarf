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
List queries flows by status / workflow name, newest first. This covers the List
operation and the FlowSummary it returns.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestListflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()

	graph := workflow.NewGraph("List")
	graph.SetURL("Only", "listflow.verify:428/only")
	graph.AddTransition("Only", workflow.End)
	mux.HandleGraph("listflow.verify:428/list", graph)

	mux.HandleTask("listflow.verify:428/only", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("done", "yes")
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	const total = 5
	created := make(map[string]bool, total)
	for range total {
		flowKey, err := eng.Create(ctx, "listflow.verify:428/list", nil, nil)
		assert.NoError(err)
		outcome, err := eng.Await(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		created[flowKey] = true
	}

	t.Run("list_by_workflow_url_returns_all", func(t *testing.T) {
		assert := testarossa.For(t)

		flows, err := enginetest.ListAll(ctx, eng, workflow.Query{WorkflowURL: "listflow.verify:428/list"})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(total, len(flows))
		for _, fs := range flows {
			assert.Equal("listflow.verify:428/list", fs.WorkflowURL)
			assert.Equal(workflow.StatusCompleted, fs.Status)
			assert.Equal("Only", fs.StoppedTaskName)
			assert.True(created[fs.FlowKey])
			assert.False(fs.CreatedAt.IsZero())
		}
	})

	t.Run("list_by_status_completed", func(t *testing.T) {
		assert := testarossa.For(t)

		flows, err := enginetest.ListAll(ctx, eng, workflow.Query{
			WorkflowURL: "listflow.verify:428/list",
			Status:      workflow.StatusCompleted,
		})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(total, len(flows))
	})

	// Runs last: it adds flows under the same workflow name, which would change the by-workflow-name
	// counts the earlier subtests assert.
	t.Run("filter_by_fairness_key_and_priority", func(t *testing.T) {
		assert := testarossa.For(t)

		// Two flows in fairness key "tenantA" at priority 2, one in "tenantB" at priority 4.
		for range 2 {
			fk, err := eng.Create(ctx, "listflow.verify:428/list", nil, &workflow.FlowOptions{FairnessKey: "tenantA", Priority: 2})
			if !assert.NoError(err) {
				return
			}
			_, err = eng.Await(ctx, fk)
			assert.NoError(err)
		}
		fkB, err := eng.Create(ctx, "listflow.verify:428/list", nil, &workflow.FlowOptions{FairnessKey: "tenantB", Priority: 4})
		if !assert.NoError(err) {
			return
		}
		_, err = eng.Await(ctx, fkB)
		assert.NoError(err)

		// FairnessKey narrows to that key only.
		flows, err := enginetest.ListAll(ctx, eng, workflow.Query{FairnessKey: "tenantA"})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(2, len(flows))

		// Priority combines with FairnessKey for an exact slice.
		flows, err = enginetest.ListAll(ctx, eng, workflow.Query{FairnessKey: "tenantB", Priority: 4})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(1, len(flows))
		assert.Equal(fkB, flows[0].FlowKey)

		// A key/priority combination with no flows yields nothing.
		flows, err = enginetest.ListAll(ctx, eng, workflow.Query{FairnessKey: "tenantA", Priority: 4})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(0, len(flows))
	})
}
