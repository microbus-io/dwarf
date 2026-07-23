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
	"testing"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestFork_RejectsInterruptedKeptStep pins the cloneOneFlow guard: a KEPT interrupted step (one off the
// fork's rewind path) must make Fork reject loudly rather than clone the interrupted step verbatim into a
// running fork and wedge it (unresumable, and - as a cohort member - unable to ever fan-in).
//
// A terminal flow can never legitimately hold an interrupted step (interrupt forces the root non-terminal,
// and Fork already rejects a non-terminal root), so the test manufactures the broken invariant directly by
// doctoring a completed cohort member to `interrupted`, then forks a sibling.
func TestFork_RejectsInterruptedKeptStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	g := workflow.NewGraph("FanForkGuard")
	g.SetEndpoint("Src", "forkguard.verify:0/src")
	g.SetEndpoint("A", "forkguard.verify:0/a")
	g.SetEndpoint("B", "forkguard.verify:0/b")
	g.SetEndpoint("J", "forkguard.verify:0/j")
	g.SetFanIn("J")
	g.AddTransition("Src", "A")
	g.AddTransition("Src", "B")
	g.AddTransition("A", "J")
	g.AddTransitionChain("B", "J", workflow.END)
	proxy.HandleGraph("forkguard.verify:0/g", g)
	proxy.HandleTask("forkguard.verify:0/src", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("forkguard.verify:0/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("forkguard.verify:0/b", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("forkguard.verify:0/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	flowKey, out, err := e.Run(ctx, "forkguard.verify:0/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)

	shard, _, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}

	// Manufacture the invariant break: cohort member A -> interrupted (a state a terminal flow can't hold).
	res, err := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, interrupt_done=1 WHERE flow_id=(SELECT flow_id FROM dwarf_flows WHERE flow_token=?) AND task_name=?",
		workflow.StatusInterrupted, mustFlowToken(t, flowKey), "A",
	)
	if !assert.NoError(err) {
		return
	}
	if n, _ := res.RowsAffected(); !assert.Equal(int64(1), n) {
		return
	}

	// Fork at sibling B. A is a kept (non-descendant) step, so the guard must reject with 409 instead of
	// cloning the interrupted A into the running fork.
	bKey := stepKeyByTaskName(t, e, flowKey, "B")
	if !assert.NotEqual("", bKey) {
		return
	}
	forkKey, err := e.Fork(ctx, bKey, nil)
	assert.Error(err)
	assert.Equal("", forkKey)
	assert.Equal(http.StatusConflict, errors.StatusCode(err))
	assert.Contains(err.Error(), "interrupted step")
}

// mustFlowToken extracts the flow token component of a flow key.
func mustFlowToken(t *testing.T, flowKey string) string {
	t.Helper()
	_, _, token, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		t.Fatalf("parseFlowKey: %v", err)
	}
	return token
}

// stepKeyByTaskName returns the key of the first step in the flow's history whose task matches taskName.
func stepKeyByTaskName(t *testing.T, e *Engine, flowKey, taskName string) string {
	t.Helper()
	hist, err := e.History(context.Background(), flowKey)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	for _, s := range hist {
		if s.TaskName == taskName {
			return s.StepKey
		}
	}
	return ""
}
