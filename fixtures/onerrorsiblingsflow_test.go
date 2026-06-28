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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestOnerrorsiblingsflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	graph := workflow.NewGraph("FanOutError")
	graph.SetEndpoint("TaskA", "onerrorsiblingsflow.verify:428/task-a")
	graph.SetEndpoint("TaskB", "onerrorsiblingsflow.verify:428/task-b")
	graph.SetEndpoint("TaskC", "onerrorsiblingsflow.verify:428/task-c")
	graph.SetEndpoint("TaskD", "onerrorsiblingsflow.verify:428/task-d")
	graph.SetEndpoint("Handler", "onerrorsiblingsflow.verify:428/handler")
	graph.SetEndpoint("TaskE", "onerrorsiblingsflow.verify:428/task-e")
	graph.SetFanIn("TaskE")
	graph.AddTransitionFanOut("TaskA", "TaskB", "TaskC", "TaskD")
	graph.AddTransitionOnError("TaskB", "Handler")
	graph.AddTransition("TaskB", "TaskE")
	graph.AddTransition("TaskC", "TaskE")
	graph.AddTransition("TaskD", "TaskE")
	graph.AddTransitionChain("Handler", "TaskE", workflow.END)
	commonProxy.HandleGraph("onerrorsiblingsflow.verify:428/fan-out-error", graph)

	commonProxy.HandleTask("onerrorsiblingsflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	commonProxy.HandleTask("onerrorsiblingsflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("triggered failure in TaskB")
	})
	commonProxy.HandleTask("onerrorsiblingsflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("markC", true)
		return nil
	})
	commonProxy.HandleTask("onerrorsiblingsflow.verify:428/task-d", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("markD", true)
		return nil
	})
	commonProxy.HandleTask("onerrorsiblingsflow.verify:428/handler", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("handled", true)
		return nil
	})
	commonProxy.HandleTask("onerrorsiblingsflow.verify:428/task-e", func(ctx context.Context, f *workflow.Flow) error {
		recovered := f.GetBool("handled") && !f.GetBool("markB")
		siblingsRan := f.GetBool("markC") && f.GetBool("markD")
		f.SetBool("recovered", recovered)
		f.SetBool("siblingsRan", siblingsRan)
		return nil
	})

	t.Run("flow_completes_with_handler_and_siblings", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := commonEngine.Run(ctx, "onerrorsiblingsflow.verify:428/fan-out-error", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, outcome.State["recovered"])
		assert.Equal(true, outcome.State["siblingsRan"])
	})
}
