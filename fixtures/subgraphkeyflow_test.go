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
	"net/http"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphKeyflow pins History's SubgraphKey: the caller step names its child flow, a Snapshot of that key
// returns what the child returned, and the key is a read handle - every operation that changes a flow rejects
// it.
func TestSubgraphKeyflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	parent := workflow.NewGraph("KeyParent")
	parent.SetURL("Call", "subgraphkey.verify:428/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("subgraphkey.verify:428/parent", parent)

	child := workflow.NewGraph("KeyChild")
	child.SetURL("Make", "subgraphkey.verify:428/make")
	child.AddTransition("Make", workflow.End)
	mux.HandleGraph("subgraphkey.verify:428/child", child)

	mux.HandleTask("subgraphkey.verify:428/call", func(ctx context.Context, f *workflow.Flow) error {
		var out struct {
			Made string `json:"made"`
		}
		yield, err := f.Subgraph("subgraphkey.verify:428/child", map[string]any{"seed": "abc"}, &out)
		if yield || err != nil {
			return err
		}
		f.SetString("got", out.Made)
		return nil
	})
	mux.HandleTask("subgraphkey.verify:428/make", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("made", f.GetString("seed")+"-made")
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, out, err := eng.Run(ctx, "subgraphkey.verify:428/parent", nil, nil)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
		return
	}
	assert.Equal("abc-made", out.State.GetString("got"))

	history, err := eng.History(ctx, flowKey)
	if !assert.NoError(err) || !assert.Len(history, 1) {
		return
	}
	caller := history[0]
	assert.True(caller.Subgraph)
	if !assert.NotEqual("", caller.SubgraphKey) {
		return
	}
	assert.NotEqual(flowKey, caller.SubgraphKey)

	// The child's result, read by its key.
	childOut, err := eng.Snapshot(ctx, caller.SubgraphKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, childOut.Status)
		assert.Equal("abc-made", childOut.State.GetString("made"))
	}
	awaited, err := eng.Await(ctx, caller.SubgraphKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, awaited.Status)
	}

	// A read handle: every mutation takes the root's key.
	assert.Equal(http.StatusBadRequest, errors.StatusCode(eng.Terminate(ctx, caller.SubgraphKey, "no")))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(eng.Cancel(ctx, caller.SubgraphKey, "no")))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(eng.Delete(ctx, caller.SubgraphKey)))
	_, err = eng.Continue(ctx, caller.SubgraphKey, nil)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
}
