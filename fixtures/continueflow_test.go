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

func TestContinueflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Counting")
	graph.SetEndpoint("Increment", "continueflow.verify:428/increment")
	graph.AddTransition("Increment", workflow.END)
	proxy.HandleGraph("continueflow.verify:428/counting", graph)

	proxy.HandleTask("continueflow.verify:428/increment", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("counter", f.GetInt("counter")+1)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("counter_persists_across_continue_turns", func(t *testing.T) {
		assert := testarossa.For(t)

		// Turn 1: create + start a flow starting from counter=0.
		flowKey, err := eng.Create(ctx, "continueflow.verify:428/counting", map[string]any{"counter": 0}, nil)
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
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(1.0, outcome.State["counter"]) // JSON round-trip: int -> float64

		// Turn 2: continue from the thread, no additional state.
		flowKey2, err := eng.Continue(ctx, flowKey, map[string]any{}, nil)
		if !assert.NoError(err) {
			return
		}
		err = eng.Start(ctx, flowKey2)
		if !assert.NoError(err) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey2)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(2.0, outcome.State["counter"])
	})
}
