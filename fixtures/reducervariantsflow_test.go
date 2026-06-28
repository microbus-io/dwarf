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
reducerflow covers add/append/union. This covers the remaining fan-in reducers:
min, max, and, or, concat, and merge. Each fan-out branch writes only its own
delta; the engine folds them at the fan-in step in fan_out_ordinal order
(declaration order here: B, C, D), which matters for concat and merge.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestReducervariantsflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	graph := workflow.NewGraph("Reducer")
	graph.SetEndpoint("TaskA", "reducervariantsflow.verify:428/task-a")
	graph.SetEndpoint("TaskB", "reducervariantsflow.verify:428/task-b")
	graph.SetEndpoint("TaskC", "reducervariantsflow.verify:428/task-c")
	graph.SetEndpoint("TaskD", "reducervariantsflow.verify:428/task-d")
	graph.SetEndpoint("Join", "reducervariantsflow.verify:428/join")
	graph.SetFanIn("Join")
	graph.SetReducer("lo", workflow.ReducerMin)
	graph.SetReducer("hi", workflow.ReducerMax)
	graph.SetReducer("allOk", workflow.ReducerAnd)
	graph.SetReducer("anyOk", workflow.ReducerOr)
	graph.SetReducer("word", workflow.ReducerConcat)
	graph.SetReducer("obj", workflow.ReducerMerge)
	graph.AddTransition("TaskA", "TaskB")
	graph.AddTransition("TaskA", "TaskC")
	graph.AddTransition("TaskA", "TaskD")
	graph.AddTransition("TaskB", "Join")
	graph.AddTransition("TaskC", "Join")
	graph.AddTransitionChain("TaskD", "Join", workflow.END)
	proxy.HandleGraph("reducervariantsflow.verify:428/reducer", graph)

	proxy.HandleTask("reducervariantsflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("reducervariantsflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("lo", 5)
		f.SetInt("hi", 5)
		f.SetBool("allOk", true)
		f.SetBool("anyOk", false)
		f.SetString("word", "a")
		f.Set("obj", map[string]any{"k1": 1.0})
		return nil
	})
	proxy.HandleTask("reducervariantsflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("lo", 2)
		f.SetInt("hi", 8)
		f.SetBool("allOk", true)
		f.SetBool("anyOk", false)
		f.SetString("word", "b")
		f.Set("obj", map[string]any{"k2": 2.0})
		return nil
	})
	proxy.HandleTask("reducervariantsflow.verify:428/task-d", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("lo", 9)
		f.SetInt("hi", 3)
		f.SetBool("allOk", false)
		f.SetBool("anyOk", true)
		f.SetString("word", "c")
		f.Set("obj", map[string]any{"k1": 9.0})
		return nil
	})
	proxy.HandleTask("reducervariantsflow.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		// Copy merged values forward under stable result keys.
		f.SetFloat("rLo", f.GetFloat("lo"))
		f.SetFloat("rHi", f.GetFloat("hi"))
		f.SetBool("rAll", f.GetBool("allOk"))
		f.SetBool("rAny", f.GetBool("anyOk"))
		f.SetString("rWord", f.GetString("word"))
		var obj map[string]any
		_ = f.Get("obj", &obj)
		f.Set("rObj", obj)
		return nil
	})

	t.Run("min_max_and_or_concat_merge", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "reducervariantsflow.verify:428/reducer", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		assert.Equal(2.0, outcome.State["rLo"])     // min(5,2,9)
		assert.Equal(8.0, outcome.State["rHi"])     // max(5,8,3)
		assert.Equal(false, outcome.State["rAll"])  // true AND true AND false
		assert.Equal(true, outcome.State["rAny"])   // false OR false OR true
		assert.Equal("abc", outcome.State["rWord"]) // concat in B,C,D order

		// merge: later contribution (D) wins on key collisions; union of keys.
		obj, ok := outcome.State["rObj"].(map[string]any)
		if !assert.True(ok) {
			return
		}
		assert.Equal(9.0, obj["k1"]) // D overwrote B's k1=1
		assert.Equal(2.0, obj["k2"]) // C's k2 carried through
	})
}
