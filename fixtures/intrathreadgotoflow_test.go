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

func TestIntrathreadgotoflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("IntraThreadGoto")
	graph.SetEndpoint("TaskA", "intrathreadgotoflow.verify:428/task-a")
	graph.SetEndpoint("LoopTask", "intrathreadgotoflow.verify:428/loop-task")
	graph.SetEndpoint("NormalC", "intrathreadgotoflow.verify:428/normal-c")
	graph.SetEndpoint("TaskD", "intrathreadgotoflow.verify:428/task-d")
	graph.SetFanIn("TaskD")
	graph.AddTransition("TaskA", "LoopTask")
	graph.AddTransition("TaskA", "NormalC")
	graph.AddTransitionGoto("LoopTask", "LoopTask")
	graph.AddTransition("LoopTask", "TaskD")
	graph.AddTransition("NormalC", "TaskD")
	graph.AddTransition("TaskD", workflow.END)
	proxy.HandleGraph("intrathreadgotoflow.verify:428/intra-thread-goto", graph)

	proxy.HandleTask("intrathreadgotoflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("intrathreadgotoflow.verify:428/loop-task", func(ctx context.Context, f *workflow.Flow) error {
		loops := f.GetInt("loops") + 1
		f.SetInt("loops", loops)
		if loops < f.GetInt("target") {
			f.Goto("LoopTask")
		}
		return nil
	})
	proxy.HandleTask("intrathreadgotoflow.verify:428/normal-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("stamp", "stamped")
		return nil
	})
	proxy.HandleTask("intrathreadgotoflow.verify:428/task-d", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", fmt.Sprintf("%s/%d", f.GetString("stamp"), f.GetInt("loops")))
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("loops_branch_converges_with_normal_branch_at_fan_in", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"target": 3}
		_, outcome, err := eng.Run(ctx, "intrathreadgotoflow.verify:428/intra-thread-goto", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("stamped/3", outcome.State["result"])
	})
}
