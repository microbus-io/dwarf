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
	"github.com/microbus-io/testarossa"
)

// TestWedgeSweep_SubgraphCallerRevived manufactures a wedged parkedSubgraph caller (child reached terminal
// but the revive was lost) and asserts the sweep re-drives completeSurgraphFlow so the caller resumes and
// the flow completes, adopting the child's output. The wedge can't arise naturally (the park-before-spawn
// fix prevents it), so the test forges the DB state the bug would leave and backdates the caller past
// parkWedgeThreshold, then calls the recovery directly (bypassing the time gate).
func TestWedgeSweep_SubgraphCallerRevived(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("P", "wedgesub.verify:0/p")
	parent.AddTransition("P", workflow.END)
	proxy.HandleGraph("wedgesub.verify:0/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("K", "wedgesub.verify:0/k")
	child.AddTransition("K", workflow.END)
	proxy.HandleGraph("wedgesub.verify:0/child", child)

	proxy.HandleTask("wedgesub.verify:0/p", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("wedgesub.verify:0/child", nil, &out)
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
	shard, parentFlowID, _, err := parseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.shard(shard)
	if !assert.NoError(err) {
		return
	}

	// Wait until the subgraph child is fully started (status=running), not merely until the caller parks.
	// The launch is three sequential steps - park the caller (parkedSubgraph), create the child (created),
	// then start the child (running) - and the park commits and is visible before the child is started. If
	// the test forged the child to completed in that window, the engine's start(child) would find it
	// non-created and fail the caller with "flow is already started". A running child implies start(child)
	// has run and the caller is parked. (SQLite serializes these writes so the window never opens; MySQL and
	// Postgres expose it.)
	var parentStepID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db.QueryRowContext(ctx,
			"SELECT surgraph_step_id FROM dwarf_flows WHERE surgraph_flow_id=? AND status=?",
			parentFlowID, workflow.StatusRunning,
		).Scan(&parentStepID) == nil && parentStepID != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !assert.NotEqual(0, parentStepID, "subgraph child never started") {
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
