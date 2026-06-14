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

func TestGotoflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Goto", "gotoflow.verify:428/goto")
	graph.AddTask("TaskA", "gotoflow.verify:428/task-a")
	graph.AddTask("TaskB", "gotoflow.verify:428/task-b")
	graph.AddTask("TaskC", "gotoflow.verify:428/task-c")
	graph.AddTransition("TaskA", "TaskB")
	graph.AddTransitionGoto("TaskB", "TaskA")
	graph.AddTransition("TaskB", "TaskC")
	graph.AddTransition("TaskC", workflow.END)
	proxy.HandleGraph("gotoflow.verify:428/goto", graph)

	proxy.HandleTask("gotoflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("loops", f.GetInt("loops")+1)
		return nil
	})
	proxy.HandleTask("gotoflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("loops") < f.GetInt("target") {
			f.Goto("TaskA")
		}
		f.SetBool("visited", true)
		return nil
	})
	proxy.HandleTask("gotoflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("finalLoops", f.GetInt("loops"))
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("loops_one_then_falls_through", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"target": 1}
		outcome, err := eng.Run(ctx, "gotoflow.verify:428/goto", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(1.0, outcome.State["finalLoops"])
	})

	t.Run("loops_three_times_via_goto", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"target": 3}
		outcome, err := eng.Run(ctx, "gotoflow.verify:428/goto", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3.0, outcome.State["finalLoops"])
	})
}

func TestGotoflow_BadGoto(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("BadGoto", "gotoflow.verify:428/bad-goto")
	graph.AddTask("BadGotoer", "gotoflow.verify:428/bad-gotoer")
	graph.AddTransition("BadGotoer", workflow.END)
	proxy.HandleGraph("gotoflow.verify:428/bad-goto", graph)

	proxy.HandleTask("gotoflow.verify:428/bad-gotoer", func(ctx context.Context, f *workflow.Flow) error {
		f.Goto("NoSuchTask")
		f.SetBool("stamp", true)
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("goto_to_unregistered_target_fails_flow", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "gotoflow.verify:428/bad-goto", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, outcome.Status)
	})
}
