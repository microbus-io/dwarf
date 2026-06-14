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

/*
Fan-in ordering by fan_out_ordinal keeps results in input-array order despite
random per-branch retry latency scrambling completion order.
*/
package fixtures

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestRetryfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("retryfanoutflow.verify:428/retry-fan-out")
	graph.AddTask("enter", "retryfanoutflow.verify:428/enter")
	graph.AddTask("increment", "retryfanoutflow.verify:428/increment")
	graph.AddTask("join", "retryfanoutflow.verify:428/join")
	graph.SetFanIn("join")
	graph.SetReducer("results", workflow.ReducerAppend)
	graph.AddTransitionForEach("enter", "increment", "elements", "element")
	graph.AddTransition("increment", "join")
	graph.AddTransition("join", workflow.END)
	proxy.HandleGraph("retryfanoutflow.verify:428/retry-fan-out", graph)

	proxy.HandleTask("retryfanoutflow.verify:428/enter", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("retryfanoutflow.verify:428/increment", func(ctx context.Context, f *workflow.Flow) error {
		if rand.Float64() < 0.10 {
			f.Retry(math.MaxInt32, 0, 0, 0)
			return nil
		}
		f.Set("results", []int{f.GetInt("element") + 1})
		return nil
	})
	proxy.HandleTask("retryfanoutflow.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("ordered_despite_random_retries", func(t *testing.T) {
		assert := testarossa.For(t)

		elements := make([]int, 100)
		for i := range elements {
			elements[i] = i
		}
		initialState := map[string]any{"elements": elements}
		outcome, err := eng.Run(ctx, "retryfanoutflow.verify:428/retry-fan-out", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		raw, ok := outcome.State["results"].([]any)
		if !assert.True(ok) {
			return
		}
		assert.Equal(100, len(raw))
		for i, v := range raw {
			assert.Equal(float64(i+1), v)
		}
	})
}
