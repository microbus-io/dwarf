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
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphCohortFail_NoStrandOnBranchFailure pins that a subgraph child with an internal fan-out does
// NOT terminalize on the first unhandled branch error while a sibling branch is still live. The child runs
// the same cohort accounting a top-level flow does: the failing branch only records cohort_failures, and the
// child fails (delivering the error to the parent's flow.Subgraph call) once the cohort fully resolves. The
// old eager short-circuit failed the child immediately, stranding the live sibling and any subgraph it had
// parked on - unreachable by every tree walk, recoverable only by Delete. The key assertion is that the
// child is still `running` after one branch has failed and the sibling is parked on a grandchild subgraph;
// the whole tree then converges to a clean terminal state with the branch error surfaced to the root.
func TestSubgraphCohortFail_NoStrandOnBranchFailure(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})
	var once sync.Once
	releaseGC := func() { once.Do(func() { close(release) }) }
	defer releaseGC()

	const (
		parentURL = "s2cohort.verify:0/parent"
		childURL  = "s2cohort.verify:0/child"
		gcURL     = "s2cohort.verify:0/grandchild"
	)

	// Parent: one task that runs the child subgraph and propagates its error (no onError -> parent fails).
	parent := workflow.NewGraph("S2Parent")
	parent.SetEndpoint("Call", "s2cohort.verify:0/call")
	parent.AddTransition("Call", workflow.END)
	proxy.HandleGraph(parentURL, parent)

	// Child: Split fans out to {Boom, Slow}, both converging at the Join fan-in. Boom fails; Slow parks on a
	// grandchild subgraph that blocks, so Boom's failure lands while Slow is still live.
	child := workflow.NewGraph("S2Child")
	child.SetEndpoint("Split", "s2cohort.verify:0/split")
	child.SetEndpoint("Boom", "s2cohort.verify:0/boom")
	child.SetEndpoint("Slow", "s2cohort.verify:0/slow")
	child.SetEndpoint("Join", "s2cohort.verify:0/join")
	child.SetFanIn("Join")
	child.AddTransition("Split", "Boom")
	child.AddTransition("Split", "Slow")
	child.AddTransition("Boom", "Join")
	child.AddTransition("Slow", "Join")
	child.AddTransition("Join", workflow.END)
	proxy.HandleGraph(childURL, child)

	// Grandchild: a single task that blocks until released.
	gc := workflow.NewGraph("S2Grandchild")
	gc.SetEndpoint("Wait", "s2cohort.verify:0/wait")
	gc.AddTransition("Wait", workflow.END)
	proxy.HandleGraph(gcURL, gc)

	proxy.HandleTask("s2cohort.verify:0/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph(childURL, nil, nil)
		if yield {
			return nil
		}
		return err
	})
	proxy.HandleTask("s2cohort.verify:0/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("s2cohort.verify:0/boom", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("boom-branch-failed", http.StatusInternalServerError)
	})
	proxy.HandleTask("s2cohort.verify:0/slow", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph(gcURL, nil, nil)
		if yield {
			return nil
		}
		return err
	})
	proxy.HandleTask("s2cohort.verify:0/join", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("s2cohort.verify:0/wait", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	flowKey, err := e.Create(ctx, parentURL, nil, nil)
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

	// Wait until Boom has failed AND the grandchild is running (so Slow is parked on it) - the moment the old
	// eager short-circuit would have terminalized the child.
	var childFlowID, childReady int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var boomFailed, gcRunning int
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE task_name=? AND status=?",
			"Boom", workflow.StatusFailed).Scan(&boomFailed)
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE workflow_url=? AND status=?",
			gcURL, workflow.StatusRunning).Scan(&gcRunning)
		if boomFailed > 0 && gcRunning > 0 {
			childReady = 1
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !assert.Equal(1, childReady, "Boom never failed while the grandchild was running") {
		releaseGC()
		return
	}

	// The prevention property: the child flow is still running, NOT failed, even though one of its branches
	// has already failed - because the sibling branch (parked on the grandchild) has not yet arrived.
	var childStatus string
	err = db.QueryRowContext(ctx, "SELECT flow_id, status FROM dwarf_flows WHERE workflow_url=?", childURL).
		Scan(&childFlowID, &childStatus)
	assert.NoError(err)
	assert.Equal(workflow.StatusRunning, strings.TrimSpace(childStatus),
		"child must stay running until its cohort resolves, not fail on the first branch error")

	// Release the grandchild: Slow arrives, the cohort resolves with a failure, the child fails, and the
	// error is delivered to the parent's flow.Subgraph call, failing the parent.
	releaseGC()

	out, err := e.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, out.Status)
	assert.True(strings.Contains(out.Error, "boom-branch-failed"), "got %q", out.Error)

	// No stranding: every flow in the tree reached a terminal status.
	rows, err := db.QueryContext(ctx,
		"SELECT workflow_url, status FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)",
		parentFlowID)
	if !assert.NoError(err) {
		return
	}
	defer rows.Close()
	nonTerminal := 0
	for rows.Next() {
		var url, status string
		rows.Scan(&url, &status)
		switch strings.TrimSpace(status) {
		case workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled:
		default:
			nonTerminal++
			t.Logf("non-terminal flow left in tree: %s status=%s", url, status)
		}
	}
	assert.Equal(0, nonTerminal, "no flow in the tree may be left non-terminal (stranded)")
}
