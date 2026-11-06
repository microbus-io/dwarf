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
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestEmptyforeachreducerflow pins what a fan-out source's own write to a reducer-managed field means: a
// contribution folded onto its input, exactly as a linear step's write is. The spawn task (TaskA) writes 5 to
// "sum", wired to ReducerAdd, over 10 in the initial state; every branch is handed 15, and the fan-in sees 15
// plus the branches' contributions. An empty forEach must agree: no branches, so the fan-in sees 15.
func TestEmptyforeachreducerflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	graph := workflow.NewGraph("EmptyForEachReducer")
	graph.SetURL("TaskA", "emptyforeachreducerflow.verify:428/task-a")
	graph.SetURL("TaskB", "emptyforeachreducerflow.verify:428/task-b")
	graph.SetURL("TaskC", "emptyforeachreducerflow.verify:428/task-c")
	graph.SetFanIn("TaskC")
	graph.SetReducer("sum", workflow.ReducerAdd)
	graph.SetReducer("processed", workflow.ReducerAdd)
	graph.AddTransitionForEach("TaskA", "TaskB", "items", "item")
	graph.AddTransitionChain("TaskB", "TaskC", workflow.End)
	// Validate requires the fan-out to converge on a SetFanIn node, which is what makes the empty-cohort path
	// route to the fan-in (fireFanInDirect) rather than fall into its complete-at-the-source fallback. (The
	// routing map itself is derived engine-side at dispatch, not stored on the graph by Validate.)
	assert.NoError(graph.Validate())
	mux.HandleGraph("emptyforeachreducerflow.verify:428/empty-for-each-reducer", graph)

	// The spawn (fan-out source) sets the reducer-managed "sum" field, then fans out.
	mux.HandleTask("emptyforeachreducerflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("sum", 5)
		return nil
	})
	// A branch records the sum it was handed, then contributes 1 to it and 1 to "processed".
	var handed sync.Map
	mux.HandleTask("emptyforeachreducerflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		handed.Store(f.GetString("item"), f.GetInt("sum"))
		f.SetInt("sum", 1)
		f.SetInt("processed", 1)
		return nil
	})
	mux.HandleTask("emptyforeachreducerflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("sumOut", f.GetInt("sum"))
		f.SetInt("processedCount", f.GetInt("processed"))
		return nil
	})

	t.Run("non_empty_folds_the_branches", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"items": []string{"x", "y"}, "sum": 10}
		_, outcome, err := eng.Run(ctx, "emptyforeachreducerflow.verify:428/empty-for-each-reducer", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		for _, item := range []string{"x", "y"} {
			v, _ := handed.Load(item)
			assert.Equal(15, v, "branch %s is handed the spawn's result", item)
		}
		assert.Equal(17.0, stateVal(outcome.State, "sumOut"))        // 10, the spawn's 5, and 1 from each branch
		assert.Equal(2.0, stateVal(outcome.State, "processedCount")) // both branches ran through the fan-in
	})

	t.Run("empty_keeps_the_spawn_value", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"items": []string{}, "sum": 10}
		_, outcome, err := eng.Run(ctx, "emptyforeachreducerflow.verify:428/empty-for-each-reducer", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(15.0, stateVal(outcome.State, "sumOut"))        // 10 and the spawn's 5, with no branch to add
		assert.Equal(0.0, stateVal(outcome.State, "processedCount")) // no branches ran, but the fan-in still fired
	})
}
