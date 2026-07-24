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
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestFanInDirectCancel_NoExtendCancelledFlow pins the empty-cohort direct fan-in path
// (fireFanInDirect) must not extend a flow a concurrent Cancel already terminalized. The window is the
// completed-step-then-cancel race - the spawn step is marked `completed` by processStep, then the flow is
// cancelled, then the worker reaches fireFanInDirect against the now-cancelled flow. Without the terminal-status
// guard on fireFanInDirect's opening lock-grab it would insert a `pending` fan-in step and overwrite the flow's
// `step_id` on the cancelled flow (orphan work reaped only later by the claim-time terminal check).
//
// Timing is made deterministic by blocking the spawn task on a channel: while it is blocked (step `running`
// under a valid lease), the flow row is cancelled by SQL (leaving the spawn step non-cancelled so its
// completion UPDATE still succeeds), then the task is released with an empty forEach array so processStep
// completes the step and reaches fireFanInDirect.
func TestFanInDirectCancel_NoExtendCancelledFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	proxy := NewTestProxy()
	g := workflow.NewGraph("FIDC")
	g.SetEndpoint("Spawn", "fidc/spawn")
	g.SetEndpoint("Work", "fidc/work")
	g.SetEndpoint("Join", "fidc/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Spawn", "Work", "items", "item")
	g.AddTransition("Work", "Join")
	g.AddTransition("Join", workflow.END)
	// The engine derives the fan-in routing at dispatch so an empty forEach routes to Join (fireFanInDirect)
	// rather than completing at the source. Validate here just asserts the graph is well-formed.
	assert.NoError(g.Validate())
	proxy.HandleGraph("fidc/g", g)

	proxy.HandleTask("fidc/spawn", func(ctx context.Context, f *workflow.Flow) error {
		started <- struct{}{}
		<-release // hold the spawn step `running` until the test has cancelled the flow
		return nil
	})
	proxy.HandleTask("fidc/work", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("fidc/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Empty forEach source: the spawn produces no branches, so processStep reaches the empty-cohort fan-in.
	flowKey, err := e.Create(ctx, "fidc/g", map[string]any{"items": []string{}}, nil)
	if !assert.NoError(err) {
		return
	}
	shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shardNum)
	if !assert.NoError(err) {
		return
	}

	// Wait until the spawn task is executing (step definitively `running`).
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("spawn task never started")
	}

	// Cancel the flow row only (mirrors the completed-step-then-cancel window: the spawn step is left
	// non-cancelled so its own completion UPDATE succeeds and the worker reaches fireFanInDirect).
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET status=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=?",
		workflow.StatusCancelled, flowID,
	)
	if !assert.NoError(err) {
		return
	}
	var stepIDBefore int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&stepIDBefore))

	// Release the spawn task: it completes, then processStep reaches fireFanInDirect against the cancelled flow.
	close(release)

	// Wait until the spawn step has been marked completed (line before fireFanInDirect), then a settle window so
	// a broken guard would have inserted the fan-in step within it (matches the leasefence tests' 1s settle).
	deadline := time.Now().Add(5 * time.Second)
	for {
		var st string
		assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='Spawn'", flowID).Scan(&st))
		if strings.TrimSpace(st) == workflow.StatusCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("spawn step never completed after release")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)

	// The guard held: no pending fan-in step was inserted, and the flow's step_id was not overwritten.
	var joinSteps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='Join'", flowID).Scan(&joinSteps))
	assert.Equal(0, joinSteps)

	var stepIDAfter int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&stepIDAfter))
	assert.Equal(stepIDBefore, stepIDAfter)

	// The flow stays cancelled (terminal, immutable); the completed spawn step is a harmless tail on it.
	var flowStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&flowStatus))
	assert.Equal(workflow.StatusCancelled, strings.TrimSpace(flowStatus))
}
