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

package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestFork_SubgraphChildGetsThreadTokenAndStepID pins the non-root branch of cloneOneFlow: a cloned
// subgraph child must carry its own thread_token and current step_id, exactly as every other creation
// path does via insertFlowTx. When it did not, the child row kept the schema defaults (thread_token=”,
// step_id=0), so List built a malformed ThreadKey ("{shard}-{id}-") whose empty token then resolved to
// zero rows in a ThreadKey-filtered query - a 404 on a key the API itself handed back.
//
// The graph is A -> RunInner -> Z, where RunInner launches a subgraph child; the fork rewinds Z, so the
// completed prefix (A, RunInner, and RunInner's child) is cloned - the child as an OFF-path flow
// (rewind==0), which is exactly the shape whose thread_token=” was permanent.
func TestFork_SubgraphChildGetsThreadTokenAndStepID(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("A", "fktk/a")
	parent.SetEndpoint("RunInner", "fktk/run-inner")
	parent.SetEndpoint("Z", "fktk/z")
	parent.AddTransitionChain("A", "RunInner", "Z", workflow.END)
	assert.NoError(parent.Validate())
	proxy.HandleGraph("fktk/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("X", "fktk/x")
	inner.AddTransitionChain("X", workflow.END)
	assert.NoError(inner.Validate())
	proxy.HandleGraph("fktk/inner", inner)

	proxy.HandleTask("fktk/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("fktk/x", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("innerResult", "X!")
		return nil
	})
	proxy.HandleTask("fktk/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("fktk/inner", nil, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("fktk/z", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	eng := NewEngineUnderTest(t)
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	originKey, outcome, err := eng.Run(ctx, "fktk/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Fork at Z so the subgraph child (under RunInner, in the completed prefix) is cloned off-path.
	var zKey string
	hist, err := eng.History(ctx, originKey)
	if !assert.NoError(err) {
		return
	}
	for _, s := range hist {
		if s.TaskName == "Z" {
			zKey = s.StepKey
		}
	}
	if !assert.NotEqual("", zKey) {
		return
	}
	forkKey, err := eng.Fork(ctx, zKey, nil)
	if !assert.NoError(err) {
		return
	}
	if _, err := eng.Await(ctx, forkKey); !assert.NoError(err) {
		return
	}

	shardNum, forkRootID, _, err := keys.ParseFlowKey(forkKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(shardNum)
	if !assert.NoError(err) {
		return
	}

	// The fork's subgraph child: a member of the fork tree (root_flow_id) that has a caller (surgraph_flow_id).
	var childFlowID, threadID, childStepID int
	var childFlowToken, threadToken string
	err = db.QueryRowContext(ctx,
		"SELECT flow_id, flow_token, thread_id, thread_token, step_id FROM dwarf_flows WHERE root_flow_id=? AND surgraph_flow_id<>0",
		forkRootID,
	).Scan(&childFlowID, &childFlowToken, &threadID, &threadToken, &childStepID)
	if !assert.NoError(err) {
		return
	}

	// The core of the fix: neither column may take its schema default on a cloned non-root flow.
	assert.NotEqual("", threadToken)    // was '' - the permanent, API-visible defect
	assert.NotEqual(0, childStepID)     // was 0 - flow no longer points at its current step
	assert.Equal(childFlowID, threadID) // a subgraph flow is its own thread
	assert.Equal(childFlowToken, threadToken)

	// End-to-end: the ThreadKey List builds for the child must resolve, not 404. Before the fix its token
	// half was empty, so queryClauses' `WHERE flow_id=? AND flow_token=?` matched no row.
	childThreadKey := fmt.Sprintf("%d-%d-%s", shardNum, threadID, threadToken)
	byThread, _, err := eng.List(ctx, workflow.Query{ThreadKey: childThreadKey, IncludeSubgraphs: true})
	if !assert.NoError(err) {
		return
	}
	assert.True(len(byThread) >= 1)
	found := false
	for _, s := range byThread {
		if s.Subgraph {
			found = true
		}
	}
	assert.True(found)

	assertInvariants(t, eng)
}
