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

// TestEmptyforeachreducerflow pins that an empty forEach converging on a fan-in applies the graph's reducers
// to the spawn task's own delta, exactly like a non-empty forEach. The spawn task (TaskA) writes a delta (5)
// to "sum", wired to ReducerAdd, over an accumulated base (10) in initial state. Both the empty and non-empty
// runs must fold that delta onto the base at the fan-in (sum -> 15) - the result must not depend on cohort
// size. Before the fix, the empty-cohort path (fireFanInDirect) merged with nil reducers, so the delta
// replaced the base (sum -> 5) past an empty array while a non-empty array reduced correctly.
func TestEmptyforeachreducerflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("EmptyForEachReducer")
	graph.SetEndpoint("TaskA", "emptyforeachreducerflow.verify:428/task-a")
	graph.SetEndpoint("TaskB", "emptyforeachreducerflow.verify:428/task-b")
	graph.SetEndpoint("TaskC", "emptyforeachreducerflow.verify:428/task-c")
	graph.SetFanIn("TaskC")
	graph.SetReducer("sum", workflow.ReducerAdd)
	graph.SetReducer("processed", workflow.ReducerAdd)
	graph.AddTransitionForEach("TaskA", "TaskB", "items", "item")
	graph.AddTransitionChain("TaskB", "TaskC", workflow.END)
	// Validate requires the fan-out to converge on a SetFanIn node, which is what makes the empty-cohort path
	// route to the fan-in (fireFanInDirect) rather than fall into its complete-at-the-source fallback. (The
	// routing map itself is derived engine-side at dispatch, not stored on the graph by Validate.)
	assert.NoError(graph.Validate())
	proxy.HandleGraph("emptyforeachreducerflow.verify:428/empty-for-each-reducer", graph)

	// The spawn (fan-out source) writes a delta to the reducer-managed "sum" field, then fans out.
	proxy.HandleTask("emptyforeachreducerflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("sum", 5)
		return nil
	})
	// A branch contributes only to "processed", never to "sum", so the fan-in's "sum" is the spawn base+delta
	// alone - identical regardless of how many branches ran.
	proxy.HandleTask("emptyforeachreducerflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("processed", 1)
		return nil
	})
	proxy.HandleTask("emptyforeachreducerflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("sumOut", f.GetInt("sum"))
		f.SetInt("processedCount", f.GetInt("processed"))
		return nil
	})

	t.Run("non_empty_reduces", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"items": []string{"x", "y"}, "sum": 10}
		_, outcome, err := eng.Run(ctx, "emptyforeachreducerflow.verify:428/empty-for-each-reducer", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(15.0, outcome.State.Value("sumOut"))        // 10 base + 5 delta, folded by ReducerAdd
		assert.Equal(2.0, outcome.State.Value("processedCount")) // both branches ran through the fan-in
	})

	t.Run("empty_reduces_identically", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"items": []string{}, "sum": 10}
		_, outcome, err := eng.Run(ctx, "emptyforeachreducerflow.verify:428/empty-for-each-reducer", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(15.0, outcome.State.Value("sumOut"))        // same 10+5 as the non-empty run, not 5 (replace)
		assert.Equal(0.0, outcome.State.Value("processedCount")) // no branches ran, but the fan-in still fired
	})
}
