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
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestStepnavflow verifies that History and Step report a step's DAG edges as step keys, and that Step
// populates the PrevKey/NextKey navigation links. For a linear A->B->C flow, the middle step B must
// resolve to A as its previous and C as its next, so a UI can offer ?step= links across the execution DAG.
func TestStepnavflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("Flow")
	graph.SetEndpoint("TaskA", "stepnavflow.verify:428/task-a")
	graph.SetEndpoint("TaskB", "stepnavflow.verify:428/task-b")
	graph.SetEndpoint("TaskC", "stepnavflow.verify:428/task-c")
	graph.AddTransitionChain("TaskA", "TaskB", "TaskC", workflow.End)
	proxy.HandleGraph("stepnavflow.verify:428/flow", graph)
	noop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	proxy.HandleTask("stepnavflow.verify:428/task-a", noop)
	proxy.HandleTask("stepnavflow.verify:428/task-b", noop)
	proxy.HandleTask("stepnavflow.verify:428/task-c", noop)

	flowKey, outcome, err := eng.Run(ctx, "stepnavflow.verify:428/flow", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	steps, err := eng.History(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	if !assert.Len(steps, 3) {
		return
	}
	// History records the A->B->C edges as step keys.
	assert.Equal(steps[0].StepKey, steps[1].PredecessorKey)
	assert.Equal(steps[2].StepKey, steps[1].SuccessorKey)

	mid, err := eng.Step(ctx, steps[1].StepKey)
	if !assert.NoError(err) {
		return
	}
	// Step reports the same edges, and in a flow with no subgraph the navigation neighbors match them.
	assert.Equal(steps[1].PredecessorKey, mid.PredecessorKey)
	assert.Equal(steps[1].SuccessorKey, mid.SuccessorKey)
	assert.Equal(steps[0].StepKey, mid.PrevKey)
	assert.Equal(steps[2].StepKey, mid.NextKey)
}
