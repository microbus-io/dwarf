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
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// awaitPropagatedInterrupts blocks until `want` interrupts have propagated up to the given root flow.
//
// handleInterrupt signalStops every flow in the surgraph chain - the root included - once per interrupting
// step, so the stop checkpoint scoped to (root key, interrupted) counts exactly the propagations that have
// landed. A wall-clock poll cannot stand in for it here: the path from Create to two children interrupting is a
// dozen round trips, and on SQL Server two parallel subgraph callers interrupting at once genuinely DEADLOCK
// (moving a step running->interrupted deletes keys from the status-filtered indexes), so the write is rolled
// back and retried by Transact. That is correct engine behaviour and it is not fast - measured under a loaded
// parallel suite, ZERO of the two had landed inside a 5s bound on a run whose remaining assertions all passed.
//
// Arm FIRST, then read Visits: a propagation that landed before the call is caught by the count, a later one by
// the channel, and one landing between the two lines by the channel, since the waiter is already registered.
// Waiter is one-shot, hence the loop - it is a rendezvous per propagation, not a spin on an observation.
//
// The minute is a "did it hang" ceiling, not a timing contract: an interrupt that never propagates never will,
// so nothing is traded away by leaving generous room for a loaded server and for -race.
func awaitPropagatedInterrupts(t *testing.T, eng *engine.Engine, flowKey string, want int) {
	t.Helper()
	seams := engineseams.Seams(eng)
	for {
		reached := seams.Waiter(engineseams.Join(engineseams.CheckpointFlowStopped, flowKey, workflow.StatusInterrupted))
		got := seams.Visits(engineseams.Join(engineseams.CheckpointFlowStopped, flowKey, workflow.StatusInterrupted))
		if got >= want {
			return
		}
		select {
		case <-reached:
		case <-time.After(time.Minute * enginetest.TimeoutScale()):
			// The flow's own state tells a stalled tree from one that ended before its interrupts arrived.
			snap, err := eng.Snapshot(context.Background(), flowKey)
			if err != nil {
				t.Fatalf("only %d of %d interrupts propagated to the root flow; snapshot: %v", got, want, err)
			}
			t.Fatalf("only %d of %d interrupts propagated to the root flow, which is %s %s", got, want, snap.Status, snap.Error)
		}
	}
}

// interruptedStepCount returns how many of a flow's steps are currently interrupted.
func interruptedStepCount(t *testing.T, eng *engine.Engine, flowKey string) int {
	t.Helper()
	assert := testarossa.For(t)
	hist, err := eng.History(context.Background(), flowKey)
	assert.NoError(err)
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
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("SnapResume")
	g.SetURL("Src", "snapresume.verify:428/src")
	g.SetURL("A", "snapresume.verify:428/a")
	g.SetURL("B", "snapresume.verify:428/b")
	g.SetURL("J", "snapresume.verify:428/j")
	g.SetFanIn("J")
	g.AddTransition("Src", "A")
	g.AddTransition("Src", "B")
	g.AddTransition("A", "J")
	g.AddTransition("B", "J")
	g.AddTransition("J", workflow.End)
	mux.HandleGraph("snapresume.verify:428/g", g)

	gateB := make(chan struct{})
	mux.HandleTask("snapresume.verify:428/src", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("snapresume.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"branch": "A"}, nil)
		if yield || err != nil {
			return err
		}
		f.SetBool("aDone", true)
		return nil
	})
	mux.HandleTask("snapresume.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
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
	mux.HandleTask("snapresume.verify:428/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	// Several workers so B's gate-block doesn't starve A on a single worker.
	assert.NoError(eng.SetWorkers(4))
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	flowKey, err := eng.Create(ctx, "snapresume.verify:428/g", nil, nil)
	assert.NoError(err)

	// A interrupts; B is still gated (running, not interrupted).
	awaitPropagatedInterrupts(t, eng, flowKey, 1)
	assert.Equal(1, interruptedStepCount(t, eng, flowKey))
	close(gateB)
	// Now B interrupts too - both interrupted, A's updated_at strictly earlier.
	awaitPropagatedInterrupts(t, eng, flowKey, 2)
	assert.Equal(2, interruptedStepCount(t, eng, flowKey))

	// Snapshot must report A (earliest updated_at = the next Resume's target), not B.
	out, err := eng.Snapshot(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusInterrupted, out.Status)
	assert.Equal("A", stateVal(out.InterruptPayload, "branch"))

	// Resume resolves exactly that step (A); afterward only B remains interrupted, and Snapshot reports B -
	// proving Snapshot tracked Resume's selection.
	// Resume resets the leaf to `pending` inside its own transaction, so B is the only interrupted step the
	// moment it returns - this is an assertion, not a wait.
	assert.NoError(eng.Resume(ctx, flowKey, nil))
	assert.Equal(1, interruptedStepCount(t, eng, flowKey))
	out, err = eng.Snapshot(ctx, flowKey)
	assert.NoError(err)
	assert.Equal("B", stateVal(out.InterruptPayload, "branch"))

	// Resume B; the flow completes.
	assert.NoError(eng.Resume(ctx, flowKey, nil))
	final, err := eng.Await(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, final.Status)
}

// TestInterruptSnapshotFollowsTheRecordedInterrupt pins that Snapshot, Resume and StoppedTaskName agree when the
// step rows' order and the flow row's record disagree. The flow row records the interrupt whose flow-row write
// committed first; a sibling can write its step earlier yet commit later (or in the same millisecond). The test
// forges that shape by backdating B's step past A's, which A's record must outrank: ordering by updated_at alone
// reports B while StoppedTaskName says A.
func TestInterruptSnapshotFollowsTheRecordedInterrupt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("SnapRecord")
	g.SetURL("Src", "snaprecord.verify:428/src")
	g.SetURL("A", "snaprecord.verify:428/a")
	g.SetURL("B", "snaprecord.verify:428/b")
	g.SetURL("J", "snaprecord.verify:428/j")
	g.SetFanIn("J")
	g.AddTransition("Src", "A")
	g.AddTransition("Src", "B")
	g.AddTransition("A", "J")
	g.AddTransition("B", "J")
	g.AddTransition("J", workflow.End)
	mux.HandleGraph("snaprecord.verify:428/g", g)

	gateB := make(chan struct{})
	interrupt := func(branch string, gate chan struct{}) func(ctx context.Context, f *workflow.Flow) error {
		return func(ctx context.Context, f *workflow.Flow) error {
			if gate != nil {
				select {
				case <-gate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			yield, err := f.Interrupt(map[string]any{"branch": branch}, nil)
			if yield || err != nil {
				return err
			}
			return nil
		}
	}
	mux.HandleTask("snaprecord.verify:428/src", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("snaprecord.verify:428/a", interrupt("A", nil))
	mux.HandleTask("snaprecord.verify:428/b", interrupt("B", gateB))
	mux.HandleTask("snaprecord.verify:428/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(4))
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	flowKey, err := eng.Create(ctx, "snaprecord.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	awaitPropagatedInterrupts(t, eng, flowKey, 1) // A's interrupt is the flow's record
	close(gateB)
	awaitPropagatedInterrupts(t, eng, flowKey, 2)

	shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	db, err := engineseams.DB(eng).Shard(shard)
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE flow_id=? AND task_name='B'", flowID)
	assert.NoError(err)

	summaries, err := enginetest.ListAll(ctx, eng, workflow.Query{WorkflowURL: "snaprecord.verify:428/g"})
	if assert.NoError(err) && assert.Len(summaries, 1) {
		assert.Equal("A", summaries[0].StoppedTaskName)
	}
	out, err := eng.Snapshot(ctx, flowKey)
	assert.NoError(err)
	assert.Equal("A", stateVal(out.InterruptPayload, "branch"), "Snapshot reports the recorded interrupt")

	// Resume resolves that same step, so B is the one left.
	assert.NoError(eng.Resume(ctx, flowKey, nil))
	out, err = eng.Snapshot(ctx, flowKey)
	assert.NoError(err)
	assert.Equal("B", stateVal(out.InterruptPayload, "branch"))
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
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()

	parent := workflow.NewGraph("PSParent")
	parent.SetURL("Seed", "psub.verify:428/seed")
	parent.SetURL("Call", "psub.verify:428/call")
	parent.SetURL("Join", "psub.verify:428/join")
	parent.SetFanIn("Join")
	parent.SetReducer("results", workflow.ReducerAppend)
	parent.AddTransitionForEach("Seed", "Call", "items", "n")
	parent.AddTransitionChain("Call", "Join", workflow.End)
	mux.HandleGraph("psub.verify:428/parent", parent)

	child := workflow.NewGraph("PSChild")
	child.SetURL("Inner", "psub.verify:428/inner")
	child.AddTransitionChain("Inner", workflow.End)
	mux.HandleGraph("psub.verify:428/child", child)

	mux.HandleTask("psub.verify:428/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("items", []int{1, 2})
		return nil
	})
	mux.HandleTask("psub.verify:428/inner", func(ctx context.Context, f *workflow.Flow) error {
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
	mux.HandleTask("psub.verify:428/call", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("psub.verify:428/child", map[string]any{"n": f.GetInt("n")}, &out)
		if yield || err != nil {
			return err
		}
		f.Set("results", []string{fmt.Sprint(out["childResult"])}) // append reducer: delta only
		return nil
	})
	mux.HandleTask("psub.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("joined", true)
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(4))
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	flowKey, err := eng.Create(ctx, "psub.verify:428/parent", nil, nil)
	assert.NoError(err)

	// Wait until BOTH parallel callers have propagated their child's interrupt up to the parent before
	// snapshotting. Snapshotting after only one has landed races the selection: the not-yet-visible
	// sibling can carry an EARLIER updated_at (the engine stamps it via the database clock, and on
	// Postgres NOW_UTC() is the transaction-start time, which is not ordered by commit/visibility), so the
	// earliest-updated pick - which both Snapshot and Resume use - can shift between the Snapshot and the
	// Resume. With both settled, the set is frozen and the two selections agree (the invariant under test).
	awaitPropagatedInterrupts(t, eng, flowKey, 2)
	assert.Equal(2, interruptedStepCount(t, eng, flowKey))

	// snapshotN returns the n of the child Snapshot says will resolve next (the propagated interrupt payload
	// of the parent's earliest-updated interrupted caller).
	snapshotN := func() int {
		out, err := eng.Await(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(workflow.StatusInterrupted, out.Status)
		n, _ := stateVal(out.InterruptPayload, "n").(float64)
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
	for _, r := range stateVal(final.State, "results").([]any) {
		results[fmt.Sprint(r)] = true
	}
	// The child Snapshot reported first got R1; the second got R2 - the token reached the child Snapshot
	// pointed at, proving the descent matched the selected caller.
	assert.True(results[fmt.Sprintf("n%d-R1", first)], "expected n%d-R1 in %v", first, results)
	assert.True(results[fmt.Sprintf("n%d-R2", second)], "expected n%d-R2 in %v", second, results)
	assert.Equal(2, len(results))
	assert.NotEqual(first, second)
}
