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
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// shardFlowCount returns the number of flows on a shard.
func shardFlowCount(t *testing.T, e *Engine, shardNum int) int {
	t.Helper()
	db, err := e.shard(shardNum)
	testarossa.For(t).NoError(err)
	var n int
	db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_flows").Scan(&n)
	return n
}

// waitFlowDeleted polls until the flow's row (and steps) are gone, failing the test on timeout.
func waitFlowDeleted(t *testing.T, e *Engine, flowKey string, timeout time.Duration) {
	t.Helper()
	shardNum, flowID, _, err := parseFlowKey(flowKey)
	testarossa.For(t).NoError(err)
	db, err := e.shard(shardNum)
	testarossa.For(t).NoError(err)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&n)
		if n == 0 {
			var steps int
			db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&steps)
			testarossa.For(t).Equal(0, steps) // steps deleted with the flow
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("flow %s was not deleted within %s", flowKey, timeout)
}

// TestDeleteOnCompletion_DeletesOnSuccess asserts a flow created with DeleteOnCompletion deletes itself
// (and its steps) once it completes successfully.
func TestDeleteOnCompletion_DeletesOnSuccess(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "doc/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/solo", g)
	proxy.HandleTask("doc/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "doc/solo", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	assert.NoError(e.Start(ctx, fk))

	waitFlowDeleted(t, e, fk, 5*time.Second)
}

// TestDeleteOnCompletion_AwaitReturns404 asserts Await on a disposable flow blocks until it finishes and
// then returns 404 - the flow is gone, and that 404 is the completion signal (uniform regardless of timing).
func TestDeleteOnCompletion_AwaitReturns404(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "doc/aw-a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/await", g)
	proxy.HandleTask("doc/aw-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "doc/await", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	assert.NoError(e.Start(ctx, fk))

	_, err = e.Await(ctx, fk)
	assert.Error(err)
	assert.Equal(404, errors.StatusCode(err))

	// A second Await yields the same 404 - uniform, not timing-dependent.
	_, err = e.Await(ctx, fk)
	assert.Error(err)
	assert.Equal(404, errors.StatusCode(err))
}

// TestDeleteOnCompletion_RunReturns404 asserts Run on a disposable flow returns 404 once it completes.
func TestDeleteOnCompletion_RunReturns404(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "doc/run-a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/run", g)
	proxy.HandleTask("doc/run-a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	_, _, err := e.Run(ctx, "doc/run", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.Error(err)
	assert.Equal(404, errors.StatusCode(err))
}

// TestDeleteOnCompletion_KeepsFailedFlow asserts a failed flow is retained even with DeleteOnCompletion set
// - failures stay available for diagnosis / Restart / Recover.
func TestDeleteOnCompletion_KeepsFailedFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Failing")
	g.SetEndpoint("A", "doc/fail-a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("doc/failing", g)
	proxy.HandleTask("doc/fail-a", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("boom")
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "doc/failing", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	assert.NoError(e.Start(ctx, fk))
	waitFlowStatus(t, e, fk, workflow.StatusFailed, 5*time.Second)

	// The failed flow row is still present (not auto-deleted).
	shardNum, flowID, _, err := parseFlowKey(fk)
	assert.NoError(err)
	db, err := e.shard(shardNum)
	assert.NoError(err)
	var n int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&n)
	assert.Equal(1, n)
}

// TestDeleteOnCompletion_CascadesSubgraph asserts that when a disposable root flow completes, the delete
// cascades into its subgraph descendants - the child is swept by the root's cascade (it carries no flag of
// its own).
func TestDeleteOnCompletion_CascadesSubgraph(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("A", "doc/sg-a")
	parent.SetEndpoint("RunInner", "doc/sg-run-inner")
	parent.AddTransitionChain("A", "RunInner", workflow.END)
	proxy.HandleGraph("doc/sg-parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("X", "doc/sg-x")
	inner.AddTransition("X", workflow.END)
	proxy.HandleGraph("doc/sg-inner", inner)

	proxy.HandleTask("doc/sg-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("doc/sg-x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("doc/sg-run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("doc/sg-inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "doc/sg-parent", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	shardNum, _, _, err := parseFlowKey(fk)
	assert.NoError(err)
	assert.NoError(e.Start(ctx, fk))

	// Root completes and deletes itself plus the subgraph child - no flows remain.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if shardFlowCount(t, e, shardNum) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(0, shardFlowCount(t, e, shardNum))
}
