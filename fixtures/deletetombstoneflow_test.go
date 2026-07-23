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

// TestDeletetombstoneflow pins that a field cleared with flow.Del is absent from the flow's final_state /
// FlowOutcome.State - not carried forever as a JSON null tombstone. The host sees the key gone, matching
// Flow.Del's contract ("the following merge drops it") rather than a leaked "drop": null.
func TestDeletetombstoneflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	graph := workflow.NewGraph("DeleteTombstone")
	graph.SetEndpoint("TaskA", "deletetombstoneflow.verify:428/task-a")
	graph.SetEndpoint("TaskB", "deletetombstoneflow.verify:428/task-b")
	graph.AddTransitionChain("TaskA", "TaskB", workflow.END)
	proxy.HandleGraph("deletetombstoneflow.verify:428/delete-tombstone", graph)

	// TaskA deletes "drop" (present in initial state) and keeps "keep".
	proxy.HandleTask("deletetombstoneflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		f.Del("drop")
		return nil
	})
	// TaskB does nothing - it just carries state to the terminal step so the delete must have survived
	// the state materialization across a step boundary, not merely within TaskA's own changes.
	proxy.HandleTask("deletetombstoneflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	initialState := map[string]any{"keep": 1, "drop": 2}
	_, outcome, err := eng.Run(ctx, "deletetombstoneflow.verify:428/delete-tombstone", initialState, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal(1.0, outcome.State["keep"])
	_, dropPresent := outcome.State["drop"]
	assert.False(dropPresent) // gone, not present as null
}
