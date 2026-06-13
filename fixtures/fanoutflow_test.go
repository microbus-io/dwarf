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

func TestFanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("fanoutflow.verify:428/fan-out")
	graph.AddTask("taskA", "fanoutflow.verify:428/task-a")
	graph.AddTask("taskB", "fanoutflow.verify:428/task-b")
	graph.AddTask("taskC", "fanoutflow.verify:428/task-c")
	graph.AddTask("taskD", "fanoutflow.verify:428/task-d")
	graph.AddTask("taskE", "fanoutflow.verify:428/task-e")
	graph.SetFanIn("taskE")
	graph.AddTransition("taskA", "taskB")
	graph.AddTransition("taskA", "taskC")
	graph.AddTransition("taskA", "taskD")
	graph.AddTransition("taskB", "taskE")
	graph.AddTransition("taskC", "taskE")
	graph.AddTransition("taskD", "taskE")
	graph.AddTransition("taskE", workflow.END)
	proxy.HandleGraph("fanoutflow.verify:428/fan-out", graph)

	proxy.HandleTask("fanoutflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetBool("markA", true)
		return nil
	})
	proxy.HandleTask("fanoutflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetBool("markB", f.GetBool("markA"))
		return nil
	})
	proxy.HandleTask("fanoutflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetBool("markC", f.GetBool("markA"))
		return nil
	})
	proxy.HandleTask("fanoutflow.verify:428/task-d", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetBool("markD", f.GetBool("markA"))
		return nil
	})
	proxy.HandleTask("fanoutflow.verify:428/task-e", func(ctx context.Context, f *workflow.Flow, baggage map[string]any) error {
		f.SetBool("allMarked", f.GetBool("markB") && f.GetBool("markC") && f.GetBool("markD"))
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("static_fan_out_and_fan_in", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "fanoutflow.verify:428/fan-out", nil, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, outcome.State["allMarked"])
	})
}
