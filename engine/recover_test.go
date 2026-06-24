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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// waitFlowStatus polls the flow row until it reaches want, failing the test on timeout. Used where the
// settled status is reached after a transient one (e.g. interrupted -> failed) so Await is unsuitable.
func waitFlowStatus(t *testing.T, e *Engine, flowKey, want string, timeout time.Duration) {
	t.Helper()
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	testarossa.For(t).NoError(err)
	db, err := e.shard(shardNum)
	testarossa.For(t).NoError(err)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var s string
		db.QueryRowContext(context.Background(), "SELECT status FROM dwarf_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&s)
		if strings.TrimSpace(s) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("flow %s did not reach status %q within %s", flowKey, want, timeout)
}

// countStepsByStatus returns how many steps of a flow are in the given status.
func countStepsByStatus(t *testing.T, e *Engine, flowKey, status string) int {
	t.Helper()
	shardNum, flowID, _, err := parseFlowKey(flowKey)
	testarossa.For(t).NoError(err)
	db, err := e.shard(shardNum)
	testarossa.For(t).NoError(err)
	var n int
	db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status=?", flowID, status).Scan(&n)
	return n
}

// TestRecover_LinearFailedStep recovers a single failed step in a linear flow: the step fails once, Recover
// restarts it, and the re-run completes the flow.
func TestRecover_LinearFailedStep(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var recovered atomic.Bool
	proxy := NewTestProxy()
	g := workflow.NewGraph("Linear")
	g.SetEndpoint("A", "recover/lin-a")
	g.SetEndpoint("B", "recover/lin-b")
	g.AddTransitionChain("A", "B", workflow.END)
	proxy.HandleGraph("recover/linear", g)
	proxy.HandleTask("recover/lin-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("recover/lin-b", func(ctx context.Context, f *workflow.Flow) error {
		if !recovered.Load() {
			return errors.New("b failed")
		}
		f.SetBool("done", true)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, outcome, err := e.Run(ctx, "recover/linear", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)

	recovered.Store(true)
	assert.NoError(e.Recover(ctx, fk, nil))

	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(true, out.State["done"])
}

// TestRecover_FanOutMultiFailedLeaf recovers a fan-out where two of three branches fail. Recover restarts
// exactly the two failed branches (not the completed sibling), and the re-run drains through fan-in.
func TestRecover_FanOutMultiFailedLeaf(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var recovered atomic.Bool
	proxy := NewTestProxy()
	g := workflow.NewGraph("FanOut")
	g.SetEndpoint("A", "recover/fo-a")
	g.SetEndpoint("B", "recover/fo-b")
	g.SetEndpoint("C", "recover/fo-c")
	g.SetEndpoint("D", "recover/fo-d")
	g.SetEndpoint("J", "recover/fo-j")
	g.SetFanIn("J")
	g.AddTransition("A", "B")
	g.AddTransition("A", "C")
	g.AddTransition("A", "D")
	g.AddTransition("B", "J")
	g.AddTransition("C", "J")
	g.AddTransitionChain("D", "J", workflow.END)
	proxy.HandleGraph("recover/fanout", g)

	var bCalls, cCalls, dCalls atomic.Int32
	proxy.HandleTask("recover/fo-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("recover/fo-b", func(ctx context.Context, f *workflow.Flow) error {
		bCalls.Add(1)
		f.SetBool("markB", true)
		return nil
	})
	failingBranch := func(mark string, calls *atomic.Int32) TaskHandler {
		return func(ctx context.Context, f *workflow.Flow) error {
			calls.Add(1)
			if !recovered.Load() {
				return errors.New("branch failed")
			}
			f.SetBool(mark, true)
			return nil
		}
	}
	proxy.HandleTask("recover/fo-c", failingBranch("markC", &cCalls))
	proxy.HandleTask("recover/fo-d", failingBranch("markD", &dCalls))
	proxy.HandleTask("recover/fo-j", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("allMarked", f.GetBool("markB") && f.GetBool("markC") && f.GetBool("markD"))
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, outcome, err := e.Run(ctx, "recover/fanout", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)

	recovered.Store(true)
	assert.NoError(e.Recover(ctx, fk, nil))

	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(true, out.State["allMarked"])
	// Only the failed branches were restarted; the completed B was not re-run.
	assert.Equal(int32(1), bCalls.Load())
	assert.Equal(int32(2), cCalls.Load())
	assert.Equal(int32(2), dCalls.Load())
}

// TestRecover_SubgraphCallerFailure recovers a flow whose failure originated in a subgraph child: the
// failure surfaces as a failed caller step in the parent, so Recover restarts that one step and the
// re-run spawns a fresh child that succeeds.
func TestRecover_SubgraphCallerFailure(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var recovered atomic.Bool
	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("A", "recover/sg-a")
	parent.SetEndpoint("RunInner", "recover/sg-run-inner")
	parent.AddTransitionChain("A", "RunInner", workflow.END)
	proxy.HandleGraph("recover/sg-parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("X", "recover/sg-x")
	inner.AddTransition("X", workflow.END)
	proxy.HandleGraph("recover/sg-inner", inner)

	proxy.HandleTask("recover/sg-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("recover/sg-x", func(ctx context.Context, f *workflow.Flow) error {
		if !recovered.Load() {
			return errors.New("x failed")
		}
		f.SetBool("innerRan", true)
		return nil
	})
	proxy.HandleTask("recover/sg-run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("recover/sg-inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		f.SetBool("innerOK", out["innerRan"] == true)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, outcome, err := e.Run(ctx, "recover/sg-parent", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)

	recovered.Store(true)
	assert.NoError(e.Recover(ctx, fk, nil)) // the RunInner caller step

	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(true, out.State["innerOK"])
}

// TestRecover_RefusesInterruptedFlowWithFailedSibling pins the failed-vs-interrupted fan-out interaction:
// a lone branch failure does not resolve the cohort (arrivals < size while the sibling is parked), so the
// flow settles as interrupted - not failed - and Recover refuses it. The operator resumes the interrupt
// first; only once the cohort resolves to a failure does Recover apply.
func TestRecover_RefusesInterruptedFlowWithFailedSibling(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("FanOutInt")
	g.SetEndpoint("A", "recover/fi-a")
	g.SetEndpoint("IntBranch", "recover/fi-int")
	g.SetEndpoint("FailBranch", "recover/fi-fail")
	g.SetEndpoint("J", "recover/fi-j")
	g.SetFanIn("J")
	g.AddTransition("A", "IntBranch")
	g.AddTransition("A", "FailBranch")
	g.AddTransition("IntBranch", "J")
	g.AddTransitionChain("FailBranch", "J", workflow.END)
	proxy.HandleGraph("recover/fanoutint", g)

	proxy.HandleTask("recover/fi-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("recover/fi-int", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"need": "input"}, nil)
		if yield {
			return nil
		}
		return err
	})
	proxy.HandleTask("recover/fi-fail", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("fail branch failed")
	})
	proxy.HandleTask("recover/fi-j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, err := e.Create(ctx, "recover/fanoutint", nil, nil)
	assert.NoError(err)
	assert.NoError(e.Start(ctx, fk))

	// Wait until both branches have acted: one failed, one interrupted.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if countStepsByStatus(t, e, fk, workflow.StatusFailed) == 1 && countStepsByStatus(t, e, fk, workflow.StatusInterrupted) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(1, countStepsByStatus(t, e, fk, workflow.StatusFailed))
	assert.Equal(1, countStepsByStatus(t, e, fk, workflow.StatusInterrupted))

	// The lone failure did not resolve the 2-branch cohort, so the flow is interrupted, not failed.
	waitFlowStatus(t, e, fk, workflow.StatusInterrupted, time.Second)

	// Recover refuses a non-failed flow - the operator must Resume the interrupt first.
	err = e.Recover(ctx, fk, nil)
	assert.Error(err)
	assert.Equal(409, errors.StatusCode(err))
}

// TestRecover_ConcurrentCallsDoNotCorrupt fires two Recover calls at once on the same failed fan-out and
// asserts the flow still completes - the per-step CAS must let each failed step be rewound exactly once,
// so the cohort counters are not double-undone.
func TestRecover_ConcurrentCallsDoNotCorrupt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var recovered atomic.Bool
	proxy := NewTestProxy()
	g := workflow.NewGraph("FanOut")
	g.SetEndpoint("A", "recover/cc-a")
	g.SetEndpoint("B", "recover/cc-b")
	g.SetEndpoint("C", "recover/cc-c")
	g.SetEndpoint("J", "recover/cc-j")
	g.SetFanIn("J")
	g.AddTransition("A", "B")
	g.AddTransition("A", "C")
	g.AddTransition("B", "J")
	g.AddTransitionChain("C", "J", workflow.END)
	proxy.HandleGraph("recover/concurrent", g)

	branch := func(mark string) TaskHandler {
		return func(ctx context.Context, f *workflow.Flow) error {
			if !recovered.Load() {
				return errors.New("branch failed")
			}
			f.SetBool(mark, true)
			return nil
		}
	}
	proxy.HandleTask("recover/cc-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("recover/cc-b", branch("markB"))
	proxy.HandleTask("recover/cc-c", branch("markC"))
	proxy.HandleTask("recover/cc-j", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("allMarked", f.GetBool("markB") && f.GetBool("markC"))
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, outcome, err := e.Run(ctx, "recover/concurrent", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)

	recovered.Store(true)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Recover(ctx, fk, nil) // both may partially win; the engine arbitrates per step
		}()
	}
	wg.Wait()

	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal(true, out.State["allMarked"])
}

// TestRecover_RejectsNonFailedFlow asserts Recover refuses a flow that is not in failed status.
func TestRecover_RejectsNonFailedFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Done")
	g.SetEndpoint("A", "recover/done-a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("recover/done", g)
	proxy.HandleTask("recover/done-a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	fk, outcome, err := e.Run(ctx, "recover/done", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	err = e.Recover(ctx, fk, nil)
	assert.Error(err)
	assert.Equal(409, errors.StatusCode(err))
}
