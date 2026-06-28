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

package fixtures

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// interruptedStepCount returns how many of a flow's steps are currently interrupted.
func interruptedStepCount(t *testing.T, eng *engine.Engine, flowKey string) int {
	t.Helper()
	hist, err := eng.History(context.Background(), flowKey)
	testarossa.For(t).NoError(err)
	n := 0
	for _, s := range hist {
		if s.Status == workflow.StatusInterrupted {
			n++
		}
	}
	return n
}

// TestInterruptSnapshotMatchesResume pins that Snapshot reports the same interrupted step Resume will
// resolve next - selected by updated_at (earliest), NOT by step_depth/step_id. Two fan-out siblings
// interrupt; sibling A interrupts first (earliest updated_at) while B is gated. Snapshot must report A; the
// removed `ORDER BY step_depth DESC, step_id DESC` selection would have reported B.
func TestInterruptSnapshotMatchesResume(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	g := workflow.NewGraph("SnapResume")
	g.SetEndpoint("Src", "snapresume.verify:428/src")
	g.SetEndpoint("A", "snapresume.verify:428/a")
	g.SetEndpoint("B", "snapresume.verify:428/b")
	g.SetEndpoint("J", "snapresume.verify:428/j")
	g.SetFanIn("J")
	g.AddTransition("Src", "A")
	g.AddTransition("Src", "B")
	g.AddTransition("A", "J")
	g.AddTransition("B", "J")
	g.AddTransition("J", workflow.END)
	proxy.HandleGraph("snapresume.verify:428/g", g)

	gateB := make(chan struct{})
	proxy.HandleTask("snapresume.verify:428/src", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("snapresume.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"branch": "A"}, nil)
		if yield || err != nil {
			return err
		}
		f.SetBool("aDone", true)
		return nil
	})
	proxy.HandleTask("snapresume.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		// Block until A has interrupted (the test closes the gate), so A's interrupt has the earlier
		// updated_at and is the unambiguous "earliest" pick.
		select {
		case <-gateB:
		case <-ctx.Done():
			return ctx.Err()
		}
		yield, err := f.Interrupt(map[string]any{"branch": "B"}, nil)
		if yield || err != nil {
			return err
		}
		f.SetBool("bDone", true)
		return nil
	})
	proxy.HandleTask("snapresume.verify:428/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngine()
	// Several workers so B's gate-block doesn't starve A on a single worker.
	testarossa.For(t).NoError(eng.SetWorkers(4))
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	flowKey, err := eng.Create(ctx, "snapresume.verify:428/g", nil, nil)
	assert.NoError(err)

	waitInterrupted := func(want int) bool {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if interruptedStepCount(t, eng, flowKey) == want {
				return true
			}
			time.Sleep(10 * time.Millisecond)
		}
		return false
	}

	// A interrupts; B is still gated (running, not interrupted).
	assert.True(waitInterrupted(1))
	close(gateB)
	// Now B interrupts too - both interrupted, A's updated_at strictly earlier.
	assert.True(waitInterrupted(2))

	// Snapshot must report A (earliest updated_at = the next Resume's target), not B.
	out, err := eng.Snapshot(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusInterrupted, out.Status)
	assert.Equal("A", out.InterruptPayload["branch"])

	// Resume resolves exactly that step (A); afterward only B remains interrupted, and Snapshot reports B -
	// proving Snapshot tracked Resume's selection.
	assert.NoError(eng.Resume(ctx, flowKey, nil))
	assert.True(waitInterrupted(1))
	out, err = eng.Snapshot(ctx, flowKey)
	assert.NoError(err)
	assert.Equal("B", out.InterruptPayload["branch"])

	// Resume B; the flow completes.
	assert.NoError(eng.Resume(ctx, flowKey, nil))
	final, err := eng.Await(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, final.Status)
}

// TestInterruptParallelSubgraphResume exercises the surgraph_step_id-based interrupt descent: a parent
// fans out to two parallel subgraph callers at the SAME depth, each child interrupts, and Resume must
// descend into the child belonging to the caller it walked (depth-matching would be ambiguous across the
// two same-depth callers). Resuming each turn delivers a distinct token to exactly one child; the flow
// completes with both children's own results, proving no cross-delivery / double-resume.
func TestInterruptParallelSubgraphResume(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	parent := workflow.NewGraph("PSParent")
	parent.SetEndpoint("Seed", "psub.verify:428/seed")
	parent.SetEndpoint("Call", "psub.verify:428/call")
	parent.SetEndpoint("Join", "psub.verify:428/join")
	parent.SetFanIn("Join")
	parent.SetReducer("results", workflow.ReducerAppend)
	parent.AddTransitionForEach("Seed", "Call", "items", "n")
	parent.AddTransitionChain("Call", "Join", workflow.END)
	proxy.HandleGraph("psub.verify:428/parent", parent)

	child := workflow.NewGraph("PSChild")
	child.SetEndpoint("Inner", "psub.verify:428/inner")
	child.AddTransitionChain("Inner", workflow.END)
	proxy.HandleGraph("psub.verify:428/child", child)

	proxy.HandleTask("psub.verify:428/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("items", []int{1, 2})
		return nil
	})
	proxy.HandleTask("psub.verify:428/inner", func(ctx context.Context, f *workflow.Flow) error {
		var resume map[string]any
		yield, err := f.Interrupt(map[string]any{"n": f.GetInt("n")}, &resume)
		if yield || err != nil {
			return err
		}
		// Result tags this child's own n with the resume token it received - so a mis-descended resume
		// (wrong child / wrong token) would show up as a wrong or duplicated tag.
		f.SetString("childResult", fmt.Sprintf("n%d-%v", f.GetInt("n"), resume["token"]))
		return nil
	})
	proxy.HandleTask("psub.verify:428/call", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("psub.verify:428/child", map[string]any{"n": f.GetInt("n")}, &out)
		if yield || err != nil {
			return err
		}
		f.Set("results", []string{fmt.Sprint(out["childResult"])}) // append reducer: delta only
		return nil
	})
	proxy.HandleTask("psub.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("joined", true)
		return nil
	})

	eng := engine.NewEngine()
	testarossa.For(t).NoError(eng.SetWorkers(4))
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	flowKey, err := eng.Create(ctx, "psub.verify:428/parent", nil, nil)
	assert.NoError(err)

	// Wait until BOTH parallel callers have propagated their child's interrupt up to the parent before
	// snapshotting. Snapshotting after only one has landed races the selection: the not-yet-visible
	// sibling can carry an EARLIER updated_at (the engine stamps it via the database clock, and on
	// Postgres NOW_UTC() is the transaction-start time, which is not ordered by commit/visibility), so the
	// earliest-updated pick - which both Snapshot and Resume use - can shift between the Snapshot and the
	// Resume. With both settled, the set is frozen and the two selections agree (the invariant under test).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && interruptedStepCount(t, eng, flowKey) < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(2, interruptedStepCount(t, eng, flowKey))

	// snapshotN returns the n of the child Snapshot says will resolve next (the propagated interrupt payload
	// of the parent's earliest-updated interrupted caller).
	snapshotN := func() int {
		out, err := eng.Await(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(workflow.StatusInterrupted, out.Status)
		n, _ := out.InterruptPayload["n"].(float64)
		return int(n)
	}

	// Snapshot identifies the next child; Resume must deliver the token to THAT child (not the other
	// same-depth caller's child). The token-to-n pairing is the assertion: a depth-matching descent would
	// route the token to the wrong child while Snapshot pointed at this one.
	first := snapshotN()
	assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"token": "R1"}))
	second := snapshotN()
	assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"token": "R2"}))

	final, err := eng.Await(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, final.Status)

	results := map[string]bool{}
	for _, r := range final.State["results"].([]any) {
		results[fmt.Sprint(r)] = true
	}
	// The child Snapshot reported first got R1; the second got R2 - the token reached the child Snapshot
	// pointed at, proving the descent matched the selected caller.
	assert.True(results[fmt.Sprintf("n%d-R1", first)], "expected n%d-R1 in %v", first, results)
	assert.True(results[fmt.Sprintf("n%d-R2", second)], "expected n%d-R2 in %v", second, results)
	assert.Equal(2, len(results))
	assert.NotEqual(first, second)
}
