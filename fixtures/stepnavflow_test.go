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

// TestStepnavflow verifies that the Step endpoint populates PrevKey/NextKey navigation links. For a
// linear A->B->C flow, the middle step B must resolve to A as its previous and C as its next, so a UI
// can offer ?step= links across the execution DAG.
func TestStepnavflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Flow", "stepnavflow.verify:428/flow")
	graph.AddTask("TaskA", "stepnavflow.verify:428/task-a")
	graph.AddTask("TaskB", "stepnavflow.verify:428/task-b")
	graph.AddTask("TaskC", "stepnavflow.verify:428/task-c")
	graph.AddTransition("TaskA", "TaskB")
	graph.AddTransition("TaskB", "TaskC")
	graph.AddTransition("TaskC", workflow.END)
	proxy.HandleGraph("stepnavflow.verify:428/flow", graph)
	noop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	proxy.HandleTask("stepnavflow.verify:428/task-a", noop)
	proxy.HandleTask("stepnavflow.verify:428/task-b", noop)
	proxy.HandleTask("stepnavflow.verify:428/task-c", noop)

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	outcome, err := eng.Run(ctx, "stepnavflow.verify:428/flow", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	steps, err := eng.History(ctx, outcome.FlowKey)
	if !assert.NoError(err) {
		return
	}
	// Locate the middle step: the one with both a predecessor and a successor in the DAG.
	var midKey string
	for _, s := range steps {
		if s.PredecessorID != 0 && s.SuccessorID != 0 {
			midKey = s.StepKey
		}
	}
	if !assert.NotEqual("", midKey) {
		return
	}

	mid, err := eng.Step(ctx, midKey)
	if !assert.NoError(err) {
		return
	}
	// The Step endpoint must resolve both navigation neighbors.
	assert.NotEqual("", mid.PrevKey)
	assert.NotEqual("", mid.NextKey)
}
