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

func TestBasicflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Basic", "basicflow.verify:428/basic")
	graph.AddTask("TaskA", "basicflow.verify:428/task-a")
	graph.AddTask("TaskB", "basicflow.verify:428/task-b")
	graph.AddTask("TaskC", "basicflow.verify:428/task-c")
	graph.AddTransition("TaskA", "TaskB")
	graph.AddTransition("TaskB", "TaskC")
	graph.AddTransition("TaskC", workflow.END)
	proxy.HandleGraph("basicflow.verify:428/basic", graph)

	proxy.HandleTask("basicflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("path", "A")
		return nil
	})
	proxy.HandleTask("basicflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("path", f.GetString("path")+"B")
		return nil
	})
	proxy.HandleTask("basicflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("path", f.GetString("path")+"C")
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("sequential_a_b_c", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "basicflow.verify:428/basic", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("ABC", outcome.State["path"])
	})
}
