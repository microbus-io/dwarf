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
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestFanouterrorflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("FanOutError", "fanouterrorflow.verify:428/fan-out-error")
	graph.AddTask("TaskA", "fanouterrorflow.verify:428/task-a")
	graph.AddTask("TaskB", "fanouterrorflow.verify:428/task-b")
	graph.AddTask("TaskC", "fanouterrorflow.verify:428/task-c")
	graph.AddTask("TaskD", "fanouterrorflow.verify:428/task-d")
	graph.AddTask("Handler", "fanouterrorflow.verify:428/handler")
	graph.AddTask("TaskE", "fanouterrorflow.verify:428/task-e")
	graph.SetFanIn("TaskE")
	graph.AddTransition("TaskA", "TaskB")
	graph.AddTransition("TaskA", "TaskC")
	graph.AddTransition("TaskA", "TaskD")
	graph.AddTransitionOnError("TaskB", "Handler")
	graph.AddTransition("TaskB", "TaskE")
	graph.AddTransition("TaskC", "TaskE")
	graph.AddTransition("TaskD", "TaskE")
	graph.AddTransition("Handler", "TaskE")
	graph.AddTransition("TaskE", workflow.END)
	proxy.HandleGraph("fanouterrorflow.verify:428/fan-out-error", graph)

	proxy.HandleTask("fanouterrorflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("fanouterrorflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("triggered failure in TaskB")
	})
	proxy.HandleTask("fanouterrorflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("markC", true)
		return nil
	})
	proxy.HandleTask("fanouterrorflow.verify:428/task-d", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("markD", true)
		return nil
	})
	proxy.HandleTask("fanouterrorflow.verify:428/handler", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("handled", true)
		return nil
	})
	proxy.HandleTask("fanouterrorflow.verify:428/task-e", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("recovered", f.GetBool("handled") && !f.GetBool("markB"))
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("flow_does_not_fail", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "fanouterrorflow.verify:428/fan-out-error", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	})

	t.Run("handler_runs_and_state_reaches_taskE", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "fanouterrorflow.verify:428/fan-out-error", nil, nil)
		assert.NoError(err)
		assert.Equal(true, outcome.State["recovered"])
	})
}
