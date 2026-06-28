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
	"github.com/microbus-io/testarossa"
)

func TestInterruptedfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	graph := workflow.NewGraph("InterruptedFanOut")
	graph.SetEndpoint("Src", "interruptedfanoutflow.verify:428/src")
	graph.SetEndpoint("A", "interruptedfanoutflow.verify:428/a")
	graph.SetEndpoint("B", "interruptedfanoutflow.verify:428/b")
	graph.SetEndpoint("C", "interruptedfanoutflow.verify:428/c")
	graph.SetEndpoint("J", "interruptedfanoutflow.verify:428/j")
	graph.SetFanIn("J")
	graph.SetReducer("executed", workflow.ReducerAdd)
	graph.AddTransition("Src", "A")
	graph.AddTransition("Src", "B")
	graph.AddTransition("Src", "C")
	graph.AddTransition("A", "J")
	graph.AddTransition("B", "J")
	graph.AddTransitionChain("C", "J", workflow.END)
	commonProxy.HandleGraph("interruptedfanoutflow.verify:428/interrupted-fan-out", graph)

	commonProxy.HandleTask("interruptedfanoutflow.verify:428/src", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	commonProxy.HandleTask("interruptedfanoutflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("executed", 1)
		return nil
	})
	commonProxy.HandleTask("interruptedfanoutflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"branch": "B"}, nil)
		if yield || err != nil {
			return err
		}
		f.SetInt("executed", 1)
		return nil
	})
	commonProxy.HandleTask("interruptedfanoutflow.verify:428/c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("executed", 1)
		return nil
	})
	commonProxy.HandleTask("interruptedfanoutflow.verify:428/j", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("totalExecuted", f.GetInt("executed"))
		return nil
	})

	t.Run("interrupt_then_resume_completes_with_sum_3", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := commonEngine.Create(ctx, "interruptedfanoutflow.verify:428/interrupted-fan-out", nil, nil)
		if !assert.NoError(err) {
			return
		}

		outcome, err := commonEngine.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)

		err = commonEngine.Resume(ctx, flowKey, map[string]any{"resumed": true})
		if !assert.NoError(err) {
			return
		}

		outcome, err = commonEngine.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3.0, outcome.State["totalExecuted"])
	})
}
