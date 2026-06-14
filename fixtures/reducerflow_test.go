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
Each fan-out branch writes only its delta (e.g. total=10, not a running total).
The engine applies the reducer at fan-in.
*/
package fixtures

import (
	"context"
	"sort"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestReducerflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Reducer", "reducerflow.verify:428/reducer")
	graph.AddTask("TaskA", "reducerflow.verify:428/task-a")
	graph.AddTask("TaskB", "reducerflow.verify:428/task-b")
	graph.AddTask("TaskC", "reducerflow.verify:428/task-c")
	graph.AddTask("TaskD", "reducerflow.verify:428/task-d")
	graph.AddTask("TaskE", "reducerflow.verify:428/task-e")
	graph.SetFanIn("TaskE")
	graph.SetReducer("total", workflow.ReducerAdd)
	graph.SetReducer("tags", workflow.ReducerAppend)
	graph.SetReducer("seen", workflow.ReducerUnion)
	graph.AddTransition("TaskA", "TaskB")
	graph.AddTransition("TaskA", "TaskC")
	graph.AddTransition("TaskA", "TaskD")
	graph.AddTransition("TaskB", "TaskE")
	graph.AddTransition("TaskC", "TaskE")
	graph.AddTransition("TaskD", "TaskE")
	graph.AddTransition("TaskE", workflow.END)
	proxy.HandleGraph("reducerflow.verify:428/reducer", graph)

	proxy.HandleTask("reducerflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("reducerflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("total", 10)
		f.SetStrings("tags", []string{"b"})
		f.SetStrings("seen", []string{"x"})
		return nil
	})
	proxy.HandleTask("reducerflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("total", 20)
		f.SetStrings("tags", []string{"c"})
		f.SetStrings("seen", []string{"y", "x"})
		return nil
	})
	proxy.HandleTask("reducerflow.verify:428/task-d", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("total", 30)
		f.SetStrings("tags", []string{"d"})
		f.SetStrings("seen", []string{"z"})
		return nil
	})
	proxy.HandleTask("reducerflow.verify:428/task-e", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("finalSum", f.GetInt("total"))
		f.SetStrings("finalList", f.GetStrings("tags"))
		f.SetStrings("finalSet", f.GetStrings("seen"))
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("sum_list_and_set_reducers_apply", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "reducerflow.verify:428/reducer", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(60.0, outcome.State["finalSum"])

		var list []string
		for _, v := range outcome.State["finalList"].([]any) {
			list = append(list, v.(string))
		}
		sort.Strings(list)
		assert.Equal([]string{"b", "c", "d"}, list)

		var set []string
		for _, v := range outcome.State["finalSet"].([]any) {
			set = append(set, v.(string))
		}
		sort.Strings(set)
		assert.Equal([]string{"x", "y", "z"}, set)
	})
}
