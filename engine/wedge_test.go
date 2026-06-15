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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestWedgeSweep_SubgraphCallerRevived manufactures a wedged parkedSubgraph caller (child reached terminal
// but the revive was lost) and asserts the sweep re-drives completeSurgraphFlow so the caller resumes and
// the flow completes, adopting the child's output. The wedge can't arise naturally (the park-before-spawn
// fix prevents it), so the test forges the DB state the bug would leave and backdates the caller past
// parkWedgeThreshold, then calls the recovery directly (bypassing the time gate).
func TestWedgeSweep_SubgraphCallerRevived(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})

	parent := workflow.NewGraph("Parent", "wedgesub.verify:0/parent")
	parent.AddTask("P", "wedgesub.verify:0/p")
	parent.AddTransition("P", workflow.END)
	proxy.HandleGraph("wedgesub.verify:0/parent", parent)
	child := workflow.NewGraph("Child", "wedgesub.verify:0/child")
	child.AddTask("K", "wedgesub.verify:0/k")
	child.AddTransition("K", workflow.END)
	proxy.HandleGraph("wedgesub.verify:0/child", child)

	proxy.HandleTask("wedgesub.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		out, yield, err := f.Subgraph("wedgesub.verify:0/child", nil)
		if yield || err != nil {
			return err
		}
		v, _ := out["v"].(float64)
		f.SetInt("got", int(v))
		return nil
	})
	// The child blocks so it stays running and the caller stays parked while we forge the wedge.
	proxy.HandleTask("wedgesub.verify:0/k", func(ctx context.Context, f *workflow.Flow) error {
		<-release
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)
	defer close(release)

	flowKey, err := e.Create(ctx, "wedgesub.verify:0/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	if !assert.NoError(e.Start(ctx, flowKey)) {
		return
	}
	shard, parentFlowID, _, err := parseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.shard(shard)
	if !assert.NoError(err) {
		return
	}

	// Wait until P has armed the subgraph and parked.
	var parentStepID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db.QueryRowContext(ctx,
			"SELECT step_id FROM dwarf_steps WHERE flow_id=? AND parked=? AND status=?",
			parentFlowID, parkedSubgraph, workflow.StatusRunning,
		).Scan(&parentStepID) == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !assert.NotEqual(0, parentStepID, "caller never parked on the subgraph") {
		return
	}

	// Forge the wedge: the child reached terminal (completed, final_state {"v":7}) but the caller was never
	// revived.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status=?, final_state=? WHERE surgraph_step_id=?",
		workflow.StatusCompleted, `{"v":7}`, parentStepID)
	assert.NoError(err)

	// Recover (minAge=0 bypasses the age guard for the test) and confirm the flow resumes and adopts the
	// child's output.
	e.recoverWedgedSubgraphParks(ctx, db, shard, 0)

	out, err := e.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	got, _ := out.State["got"].(float64)
	assert.Equal(7, int(got))
}

// TestWedgeSweep_BreakerProbeReElected manufactures a breaker backlog that has lost its probe (all steps
// parked=2, none dispatchable) and asserts the sweep re-elects a probe so the backlog recovers. The
// downstream is made healthy just before recovery, so the re-elected probe succeeds, breakerClose unparks
// the shard, and every flow completes.
func TestWedgeSweep_BreakerProbeReElected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	const taskURL = "wedgebrk.verify:0/task"
	var down atomic.Bool
	down.Store(true)

	proxy := NewTestProxy()
	g := workflow.NewGraph("Down", "wedgebrk.verify:0/down")
	g.AddTask("Down", taskURL)
	g.AddTransition("Down", workflow.END)
	proxy.HandleGraph("wedgebrk.verify:0/down", g)
	proxy.HandleTask(taskURL, func(ctx context.Context, f *workflow.Flow) error {
		if down.Load() {
			return workflow.ErrUnavailable(errors.New("downstream unavailable"), "unavailable")
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// Several flows for the same task, so the breaker trips with a backlog beyond the single probe.
	const n = 4
	keys := make([]string, 0, n)
	for range n {
		k, err := e.Create(ctx, "wedgebrk.verify:0/down", nil, nil)
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(e.Start(ctx, k)) {
			return
		}
		keys = append(keys, k)
	}

	db, err := e.shard(1) // RunInTest defaults to a single shard
	if !assert.NoError(err) {
		return
	}

	// Wait until the breaker has tripped (at least one step parked=2 for the task).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var parked int
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE task_url=? AND parked=?", taskURL, parkedBreaker).Scan(&parked)
		if parked >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Forge the lost-probe wedge: force every step of the task to parked=2 pending (no probe). Leave
	// updated_at at its existing (past) value so the minAge=0 sweep flags it.
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET parked=?, status=?, lease_expires=NOW_UTC() WHERE task_url=?",
		parkedBreaker, workflow.StatusPending, taskURL,
	)
	assert.NoError(err)
	var probes int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE task_url=? AND parked=? AND status IN (?,?)",
		taskURL, parkedNone, workflow.StatusPending, workflow.StatusRunning).Scan(&probes)
	if !assert.Equal(0, probes, "test setup should leave no probe") {
		return
	}

	// Downstream recovers, then the sweep re-elects a probe; the probe succeeds, breakerClose unparks the
	// rest, and all flows complete.
	down.Store(false)
	e.recoverWedgedBreakerParks(ctx, db, 1, 0)

	for _, k := range keys {
		out, err := e.Await(ctx, k)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}
