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

func TestInterruptedfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("InterruptedFanOut", "interruptedfanoutflow.verify:428/interrupted-fan-out")
	graph.AddTask("Src", "interruptedfanoutflow.verify:428/src")
	graph.AddTask("A", "interruptedfanoutflow.verify:428/a")
	graph.AddTask("B", "interruptedfanoutflow.verify:428/b")
	graph.AddTask("C", "interruptedfanoutflow.verify:428/c")
	graph.AddTask("J", "interruptedfanoutflow.verify:428/j")
	graph.SetFanIn("J")
	graph.SetReducer("executed", workflow.ReducerAdd)
	graph.AddTransition("Src", "A")
	graph.AddTransition("Src", "B")
	graph.AddTransition("Src", "C")
	graph.AddTransition("A", "J")
	graph.AddTransition("B", "J")
	graph.AddTransition("C", "J")
	graph.AddTransition("J", workflow.END)
	proxy.HandleGraph("interruptedfanoutflow.verify:428/interrupted-fan-out", graph)

	proxy.HandleTask("interruptedfanoutflow.verify:428/src", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("interruptedfanoutflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("executed", 1)
		return nil
	})
	proxy.HandleTask("interruptedfanoutflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"branch": "B"}, nil)
		if yield || err != nil {
			return err
		}
		f.SetInt("executed", 1)
		return nil
	})
	proxy.HandleTask("interruptedfanoutflow.verify:428/c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("executed", 1)
		return nil
	})
	proxy.HandleTask("interruptedfanoutflow.verify:428/j", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("totalExecuted", f.GetInt("executed"))
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("interrupt_then_resume_completes_with_sum_3", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "interruptedfanoutflow.verify:428/interrupted-fan-out", nil, nil)
		if !assert.NoError(err) {
			return
		}
		err = eng.Start(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)

		err = eng.Resume(ctx, flowKey, map[string]any{"resumed": true})
		if !assert.NoError(err) {
			return
		}

		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3.0, outcome.State["totalExecuted"])
	})
}
