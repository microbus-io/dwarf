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
	"fmt"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestSubtaskflow exercises flow.Subtask: a parent task invokes a single task as an isolated child flow.
// Unlike Subgraph, the child task has NO graph registered - the engine synthesizes a trivial one-node
// graph around its URL. Only the explicit input crosses into the child and only the explicit out crosses
// back, so the child's internal state does not leak into the parent.
func TestSubtaskflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	// Parent graph: Seed -> RunSub -> Done. There is no graph for the child task; Subtask synthesizes one.
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Seed", "subtaskflow.verify:428/seed")
	parent.SetEndpoint("RunSub", "subtaskflow.verify:428/run-sub")
	parent.SetEndpoint("Done", "subtaskflow.verify:428/done")
	parent.AddTransitionChain("Seed", "RunSub", "Done", workflow.END)
	proxy.HandleGraph("subtaskflow.verify:428/parent", parent)

	proxy.HandleTask("subtaskflow.verify:428/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("seed", "s")
		return nil
	})
	// The child task is reached only via flow.Subtask - it is registered as a bare task handler with no
	// graph. It reads its explicit input "in" and writes its output "out".
	proxy.HandleTask("subtaskflow.verify:428/transform", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("out", fmt.Sprintf("T(%s)", f.GetString("in")))
		return nil
	})
	// RunSub runs the child task as a subtask: only {in: seed} crosses in, only out.Out crosses back.
	proxy.HandleTask("subtaskflow.verify:428/run-sub", func(ctx context.Context, f *workflow.Flow) error {
		var out struct {
			Out string `json:"out"`
		}
		yield, err := f.Subtask("Transform", "subtaskflow.verify:428/transform", map[string]any{"in": f.GetString("seed")}, &out)
		if yield || err != nil {
			return err
		}
		f.SetString("result", out.Out)
		return nil
	})
	proxy.HandleTask("subtaskflow.verify:428/done", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	flowKey, outcome, err := eng.Run(ctx, "subtaskflow.verify:428/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	// The subtask transformed "s" -> "T(s)" and the result crossed back via the out pointer.
	assert.Equal("T(s)", outcome.State["result"])
	// Isolation: the child's "in"/"out" stayed in the child flow; only the explicit "result" is in the parent.
	_, leakedIn := outcome.State["in"]
	_, leakedOut := outcome.State["out"]
	assert.False(leakedIn)
	assert.False(leakedOut)

	// The parent's history renders the subtask as a one-node child subtree named "Transform".
	hist, err := eng.History(ctx, flowKey)
	if assert.NoError(err) {
		var sawSubtask bool
		for _, step := range hist {
			if step.Subgraph {
				for _, sub := range step.SubHistory {
					if sub.TaskName == "Transform" {
						sawSubtask = true
					}
				}
			}
		}
		assert.True(sawSubtask)
	}
}
