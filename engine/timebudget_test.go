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
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// flowBudgetMs reads the frozen per-flow budget off the flow row.
func flowBudgetMs(t *testing.T, e *Engine, flowKey string) int {
	t.Helper()
	shardNum, flowID, _, err := parseFlowKey(flowKey)
	testarossa.For(t).NoError(err)
	db, err := e.shard(shardNum)
	testarossa.For(t).NoError(err)
	var ms int
	err = db.QueryRowContext(context.Background(), "SELECT time_budget_ms FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&ms)
	testarossa.For(t).NoError(err)
	return ms
}

// entryStepBudgetMs reads the entry step's denormalized budget.
func entryStepBudgetMs(t *testing.T, e *Engine, flowKey string) int {
	t.Helper()
	shardNum, flowID, _, err := parseFlowKey(flowKey)
	testarossa.For(t).NoError(err)
	db, err := e.shard(shardNum)
	testarossa.For(t).NoError(err)
	var ms int
	err = db.QueryRowContext(context.Background(), "SELECT time_budget_ms FROM dwarf_steps WHERE flow_id=? ORDER BY step_id LIMIT_OFFSET(1, 0)", flowID).Scan(&ms)
	testarossa.For(t).NoError(err)
	return ms
}

// TestTimeBudget_FrozenAtCreate asserts FlowOptions.TimeBudget is resolved at Create, frozen onto the flow
// row, and denormalized onto the entry step - while a flow with no override freezes the engine default, and
// a later default change does not retro-edit an existing flow.
func TestTimeBudget_FrozenAtCreate(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("Task", "timebudget/task")
	g.AddTransition("Task", workflow.END)
	proxy.HandleGraph("timebudget/graph", g)
	proxy.HandleTask("timebudget/task", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)
	assert.NoError(e.SetTimeBudget(30 * time.Second))

	// Explicit override: frozen on the flow row and seeded onto the entry step.
	fk, err := e.Create(ctx, "timebudget/graph", nil, &workflow.FlowOptions{TimeBudget: 45 * time.Second})
	assert.NoError(err)
	assert.Equal(45000, flowBudgetMs(t, e, fk))
	assert.Equal(45000, entryStepBudgetMs(t, e, fk))

	// No override: the engine default at Create time is frozen.
	fkDefault, err := e.Create(ctx, "timebudget/graph", nil, nil)
	assert.NoError(err)
	assert.Equal(30000, flowBudgetMs(t, e, fkDefault))

	// A later default change does not retro-edit the already-created flow.
	assert.NoError(e.SetTimeBudget(10 * time.Second))
	assert.Equal(30000, flowBudgetMs(t, e, fkDefault))
}

// TestTimeBudget_LeaseSizedFromRow proves the crash-recovery lease is sized from the step's own
// time_budget_ms, not the engine default. With a tiny default and a large per-flow override, the running
// step's lease must reflect the override (else a long task would be falsely reclaimed mid-flight).
func TestTimeBudget_LeaseSizedFromRow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var once sync.Once
	started := make(chan struct{})
	release := make(chan struct{})

	proxy := NewTestProxy()
	g := workflow.NewGraph("Blocker")
	g.SetEndpoint("Task", "timebudget/blocker")
	g.AddTransition("Task", workflow.END)
	proxy.HandleGraph("timebudget/blockergraph", g)
	proxy.HandleTask("timebudget/blocker", func(ctx context.Context, f *workflow.Flow) error {
		once.Do(func() { close(started) })
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)
	assert.NoError(e.SetTimeBudget(1 * time.Second)) // tiny default

	// Override well above the default; the lease must follow the override, not the 1s default.
	fk, err := e.Create(ctx, "timebudget/blockergraph", nil, &workflow.FlowOptions{TimeBudget: 120 * time.Second})
	assert.NoError(err)

	<-started // the entry step is now running and leased

	shardNum, flowID, _, err := parseFlowKey(fk)
	assert.NoError(err)
	db, err := e.shard(shardNum)
	assert.NoError(err)
	var leaseRemainingMs float64
	var budgetMs int
	err = db.QueryRowContext(ctx,
		"SELECT DATE_DIFF_MILLIS(lease_expires, NOW_UTC()), time_budget_ms FROM dwarf_steps WHERE flow_id=? AND status=?",
		flowID, workflow.StatusRunning,
	).Scan(&leaseRemainingMs, &budgetMs)
	assert.NoError(err)
	assert.Equal(120000, budgetMs)
	// Sized from the 120s budget (+margin), not the 1s default (which would leave ~31s). A generous floor
	// keeps the assertion robust against the few seconds elapsed since the claim.
	assert.True(leaseRemainingMs > 60000)

	close(release)
	_, err = e.Await(ctx, fk)
	assert.NoError(err)
}

// TestTimeBudget_InheritedBySubgraph asserts a subgraph child flow inherits the parent's frozen budget via
// the same rail priority/fairness use.
func TestTimeBudget_InheritedBySubgraph(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("RunInner", "timebudget/run-inner")
	parent.AddTransition("RunInner", workflow.END)
	proxy.HandleGraph("timebudget/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("TaskX", "timebudget/task-x")
	inner.AddTransition("TaskX", workflow.END)
	proxy.HandleGraph("timebudget/inner", inner)

	proxy.HandleTask("timebudget/task-x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("timebudget/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("timebudget/inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)
	assert.NoError(e.SetTimeBudget(30 * time.Second))

	fk, outcome, err := e.Run(ctx, "timebudget/parent", nil, &workflow.FlowOptions{TimeBudget: 45 * time.Second})
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// The child flow (surgraph_flow_id > 0) carries the parent's 45s budget, not the 30s default.
	shardNum, _, _, err := parseFlowKey(fk)
	assert.NoError(err)
	db, err := e.shard(shardNum)
	assert.NoError(err)
	var childBudgetMs int
	err = db.QueryRowContext(ctx, "SELECT time_budget_ms FROM dwarf_flows WHERE surgraph_flow_id > 0 ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)").Scan(&childBudgetMs)
	assert.NoError(err)
	assert.Equal(45000, childBudgetMs)
}
