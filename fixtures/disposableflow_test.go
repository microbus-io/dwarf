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
DeleteOnCompletion + subgraph: the deferred-deletion contract. A disposable flow that runs a subgraph child
completes, then lingers for a grace window (~1 min) before the reaper removes the whole tree. During the
window its OUTCOME is observable - Run/Await/Snapshot return the completed FlowOutcome (including state merged
back from the subgraph) - which is how a caller learns a disposable flow's result now that there is no
FlowStopped callback. The flow is nonetheless logically gone: it is excluded from List and History 404s. The
physical reap (and its cascade to the subgraph child, keyed on root_flow_id) is pinned in the engine-package
reaper tests (which can shorten the grace and force a pass); a fixture using the public API and the default
1-min grace verifies the observable window.
*/
package fixtures

import (
	"context"
	"net/http"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestDisposableflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("RunSub", "disposable.verify:428/run-sub")
	parent.AddTransition("RunSub", workflow.END)
	proxy.HandleGraph("disposable.verify:428/parent", parent)

	child := workflow.NewGraph("Child")
	child.SetEndpoint("Work", "disposable.verify:428/work")
	child.AddTransition("Work", workflow.END)
	proxy.HandleGraph("disposable.verify:428/child", child)

	proxy.HandleTask("disposable.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("childOut", "hello")
		return nil
	})
	proxy.HandleTask("disposable.verify:428/run-sub", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("disposable.verify:428/child", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		if v, ok := out["childOut"]; ok {
			f.Set("subResult", v)
		}
		return nil
	})

	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// Run a disposable flow. Its outcome is observable during the grace window - Run returns the completed
	// outcome with the subgraph's result merged in, NOT a 404. This is how a caller learns the result.
	rootKey, out, err := eng.Run(ctx, "disposable.verify:428/parent", map[string]any{},
		&workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal("hello", out.State.Value("subResult"))
	}

	// Snapshot still serves the outcome during the window.
	snap, err := eng.Snapshot(ctx, rootKey)
	assert.NoError(err)
	if assert.NotNil(snap) {
		assert.Equal(workflow.StatusCompleted, snap.Status)
		assert.Equal("hello", snap.State.Value("subResult"))
	}

	// It is nonetheless logically gone: History 404s (the full step detail is what a disposable flow discards)
	// and it is excluded from List.
	_, err = eng.History(ctx, rootKey)
	if assert.Error(err) {
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	}
	roots, _, err := eng.List(ctx, workflow.Query{WorkflowURL: "disposable.verify:428/parent"})
	if assert.NoError(err) {
		assert.Equal(0, len(roots))
	}
}
