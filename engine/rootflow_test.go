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
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// rootFlowIDOf reads the denormalized root_flow_id column for a flow id.
func rootFlowIDOf(t *testing.T, e *Engine, shard, flowID int) int {
	t.Helper()
	db, err := e.shard(shard)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	var root int
	if err := db.QueryRowContext(context.Background(),
		"SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?", flowID,
	).Scan(&root); err != nil {
		t.Fatalf("read root_flow_id: %v", err)
	}
	return root
}

// TestRootFlowID_CreateAndSubgraph asserts a top-level flow is its own root and a subgraph child inherits
// the parent's root_flow_id (the whole tree shares one root pointer).
func TestRootFlowID_CreateAndSubgraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("P", "rootid.verify:0/p")
	parent.AddTransition("P", workflow.END)
	proxy.HandleGraph("rootid.verify:0/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("K", "rootid.verify:0/k")
	child.AddTransition("K", workflow.END)
	proxy.HandleGraph("rootid.verify:0/child", child)

	proxy.HandleTask("rootid.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("rootid.verify:0/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("rootid.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	flowKey, out, err := e.Run(ctx, "rootid.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)

	shard, parentFlowID, _, err := parseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}

	// A top-level flow is its own root.
	assert.Equal(parentFlowID, rootFlowIDOf(t, e, shard, parentFlowID))

	// The subgraph child inherits the parent's root_flow_id.
	db, _ := e.shard(shard)
	var childFlowID, childRoot int
	err = db.QueryRowContext(ctx,
		"SELECT flow_id, root_flow_id FROM dwarf_flows WHERE surgraph_flow_id=?", parentFlowID,
	).Scan(&childFlowID, &childRoot)
	if !assert.NoError(err) {
		return
	}
	assert.NotEqual(0, childFlowID)
	assert.Equal(parentFlowID, childRoot)
}

// TestRootFlowID_ForkIsItsOwnRoot asserts a forked flow is its own root - it does not inherit the origin's
// root_flow_id - so the clone is a self-contained tree.
func TestRootFlowID_ForkIsItsOwnRoot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	g := workflow.NewGraph("Lin")
	g.SetEndpoint("A", "rootidfork.verify:0/a")
	g.SetEndpoint("B", "rootidfork.verify:0/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	proxy.HandleGraph("rootidfork.verify:0/lin", g)
	proxy.HandleTask("rootidfork.verify:0/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("rootidfork.verify:0/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	flowKey, out, err := e.Run(ctx, "rootidfork.verify:0/lin", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)

	// Fork from the first step.
	steps, err := e.History(ctx, flowKey)
	if !assert.NoError(err) || !assert.True(len(steps) > 0) {
		return
	}
	forkKey, err := e.Fork(ctx, steps[0].StepKey, nil)
	if !assert.NoError(err) {
		return
	}
	_, err = e.Await(ctx, forkKey)
	if !assert.NoError(err) {
		return
	}

	shard, forkFlowID, _, err := parseFlowKey(forkKey)
	if !assert.NoError(err) {
		return
	}
	// The fork is its own root, distinct from the origin.
	_, originFlowID, _, _ := parseFlowKey(flowKey)
	assert.Equal(forkFlowID, rootFlowIDOf(t, e, shard, forkFlowID))
	assert.NotEqual(originFlowID, rootFlowIDOf(t, e, shard, forkFlowID))
}

// TestRootFlowID_ContinueStartsFreshRoot asserts a Continue turn starts its own root rather than inheriting
// the prior turn's.
func TestRootFlowID_ContinueStartsFreshRoot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	g := workflow.NewGraph("Turn")
	g.SetEndpoint("T", "rootidcont.verify:0/t")
	g.AddTransition("T", workflow.END)
	proxy.HandleGraph("rootidcont.verify:0/turn", g)
	proxy.HandleTask("rootidcont.verify:0/t", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	firstKey, out, err := e.Run(ctx, "rootidcont.verify:0/turn", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)

	nextKey, err := e.Continue(ctx, firstKey, nil)
	if !assert.NoError(err) {
		return
	}
	_, err = e.Await(ctx, nextKey)
	if !assert.NoError(err) {
		return
	}

	shard, nextFlowID, _, err := parseFlowKey(nextKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(nextFlowID, rootFlowIDOf(t, e, shard, nextFlowID))
}
