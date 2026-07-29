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
Reducers compare and dedupe on the MARSHALLED bytes of their operands, which are canonical only for a
value that came back decoded from the database (Go sorts a map's keys, but marshals a STRUCT's fields in
declaration order). Continue's additionalState is the one reducer input that skips the database - it is
the caller's raw Go value - so it is canonicalized at the door. Without that, a caller passing a struct
whose fields are not declared alphabetically folds a second, byte-different spelling of an element that
is already in the thread's accumulated state, and a union reducer keeps both.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// tagStruct declares its fields out of alphabetical order on purpose: Go marshals a struct in DECLARATION
// order, so this yields {"z":…,"a":…} while the same object decoded from the database yields {"a":…,"z":…}.
type tagStruct struct {
	Z string `json:"z"`
	A string `json:"a"`
}

func TestContinuecanonicalflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Tagging")
	graph.SetEndpoint("Tag", "continuecanonical.verify:428/tag")
	graph.AddTransition("Tag", workflow.END)
	graph.SetReducer("tags", workflow.ReducerUnion)
	proxy.HandleGraph("continuecanonical.verify:428/g", graph)

	// The task is a pass-through: the interesting merge is Continue's, between the thread's accumulated
	// state and the caller's additionalState.
	proxy.HandleTask("continuecanonical.verify:428/tag", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// Turn 1 establishes the element in state, via the ordinary (decoded) path.
	flowKey, outcome, err := eng.Run(ctx, "continuecanonical.verify:428/g",
		map[string]any{"tags": []any{map[string]any{"z": "1", "a": "2"}}}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Turn 2 re-contributes the SAME element, but as a struct - the byte-different spelling. The union
	// reducer must recognize it as the element already present, not append a duplicate.
	nextKey, err := eng.Continue(ctx, flowKey, map[string]any{"tags": []tagStruct{{Z: "1", A: "2"}}})
	if !assert.NoError(err) {
		return
	}
	outcome, err = eng.Await(ctx, nextKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	tags, ok := stateVal(outcome.State, "tags").([]any)
	if !assert.True(ok, "tags should be an array, got %#v", stateVal(outcome.State, "tags")) {
		return
	}
	assert.Equal(1, len(tags), "the same element in a different spelling must dedupe, got %#v", tags)

	// And a genuinely new element still unions in.
	thirdKey, err := eng.Continue(ctx, nextKey, map[string]any{"tags": []tagStruct{{Z: "9", A: "8"}}})
	if !assert.NoError(err) {
		return
	}
	outcome, err = eng.Await(ctx, thirdKey)
	if !assert.NoError(err) {
		return
	}
	tags, _ = stateVal(outcome.State, "tags").([]any)
	assert.Equal(2, len(tags), "a distinct element must still be added, got %#v", tags)
}
