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
	"fmt"

	"github.com/microbus-io/dwarf/workflow"
)

// Build a linear workflow graph and validate it.
func ExampleGraph() {
	g := workflow.NewGraph("Checkout")
	g.SetEndpoint("Reserve", "inventory.reserve")
	g.SetEndpoint("Charge", "billing.charge")
	g.AddTransitionChain("Reserve", "Charge", workflow.END)

	fmt.Println("name:", g.Name())
	fmt.Println("entry:", g.EntryPoint())
	fmt.Println("valid:", g.Validate() == nil)
	// Output:
	// name: Checkout
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

// A Flow carries state to and from a task. Tasks read inputs and write outputs with typed accessors.
func ExampleFlow() {
	f := workflow.NewFlow()
	f.SetString("name", "ada")
	f.SetInt("count", 3)

	fmt.Println(f.GetString("name"), f.GetInt("count"))
	// Output: ada 3
}
