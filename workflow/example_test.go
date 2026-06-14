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

package workflow_test

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/microbus-io/dwarf/workflow"
)

// Build a linear workflow graph and validate it.
func ExampleGraph() {
	g := workflow.NewGraph("Checkout", "checkout")
	g.AddTask("Reserve", "inventory.reserve")
	g.AddTask("Charge", "billing.charge")
	g.AddTransition("Reserve", "Charge")
	g.AddTransition("Charge", workflow.END)

	fmt.Println("name:", g.URL())
	fmt.Println("entry:", g.EntryPoint())
	fmt.Println("valid:", g.Validate() == nil)
	// Output:
	// name: checkout
	// entry: Reserve
	// valid: true
}

// Reducers merge the changes of parallel branches at a fan-in. MergeState applies them to fold a delta
// onto a base state.
func ExampleMergeState() {
	reducers := map[string]workflow.Reducer{
		"items": workflow.ReducerAppend, // concatenate arrays
		"total": workflow.ReducerAdd,    // sum numbers
	}
	base := map[string]any{"items": []any{"a"}, "total": 1}
	delta := map[string]any{"items": []any{"b"}, "total": 2}

	// Reducer outputs are raw JSON, ready to be re-serialized as the next step's state.
	merged, _ := workflow.MergeState(base, delta, reducers)
	out, _ := json.Marshal(merged)
	fmt.Println(string(out))
	// Output: {"items":["a","b"],"total":3}
}

// A host's ExecuteTask wraps its transport's "can't serve" signal so the engine trips the task's breaker
// instead of failing the flow. The engine classifies with IsBreakerTrip.
func ExampleErrBreakerTrip() {
	err := workflow.ErrBreakerTrip(errors.New("503 from billing"), "unavailable")

	if cause, ok := workflow.IsBreakerTrip(err); ok {
		fmt.Println("trip breaker, cause:", cause)
	}
	// The wrapped error is preserved.
	fmt.Println("underlying:", err.Error())
	// Output:
	// trip breaker, cause: unavailable
	// underlying: 503 from billing
}

// A Flow carries state to and from a task. Tasks read inputs and write outputs with typed accessors.
func ExampleFlow() {
	f := workflow.NewFlow()
	f.SetString("name", "ada")
	f.SetInt("count", 3)

	fmt.Println(f.GetString("name"), f.GetInt("count"))
	// Output: ada 3
}
