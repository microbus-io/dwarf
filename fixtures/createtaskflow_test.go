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
CreateTask wraps a single task in a trivial one-node graph and runs it. It accepts
baggage and FlowOptions like Create: this asserts the task executes with the supplied
baggage, and that the scheduling options (priority + fairness key) are persisted on the
flow row — proven by querying them back through Query.FairnessKey / Query.Priority.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestCreatetaskflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	var seenBaggage any
	proxy.HandleTask("createtaskflow.verify:428/only", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		seenBaggage = baggage
		f.SetString("ran", "yes")
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("runs_with_baggage_and_options", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.CreateTask(ctx, "createtaskflow.verify:428/only",
			map[string]any{"in": 1},
			map[string]any{"actor": "alice"},
			&workflow.FlowOptions{Priority: 3, FairnessKey: "tk1"},
		)
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
		assert.Equal("yes", outcome.State["ran"])

		// The task saw the baggage.
		got, _ := seenBaggage.(map[string]any)
		assert.Equal("alice", got["actor"])

		// The FlowOptions persisted on the flow row, surfaced on the FlowSummary.
		flows, _, err := eng.List(ctx, workflow.Query{FairnessKey: "tk1"})
		if !assert.NoError(err) {
			return
		}
		if !assert.Equal(1, len(flows)) {
			return
		}
		assert.Equal(flowKey, flows[0].FlowKey)
		assert.Equal(3, flows[0].Priority)
		assert.Equal("tk1", flows[0].FairnessKey)

		// The Priority filter also matches the stored band, and a different band does not.
		flows, _, err = eng.List(ctx, workflow.Query{FairnessKey: "tk1", Priority: 4})
		if !assert.NoError(err) {
			return
		}
		assert.Equal(0, len(flows))
	})

	t.Run("nil_options_use_defaults", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.CreateTask(ctx, "createtaskflow.verify:428/only", nil, nil, nil)
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
	})
}
