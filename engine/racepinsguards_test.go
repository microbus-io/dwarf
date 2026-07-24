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

// Deterministic pins for three concurrency guards that were previously testable only by a probabilistic
// race loop. Each freezes the engine at a checkpoint in the exact race window, drives the racing operation
// to completion while the engine is held, then releases and asserts the single guaranteed outcome:
//   - Cancel vs the transition transaction (the write-first terminal-status guard).
//   - Cancel vs subgraph spawn (the orphaned-child residue + its wedge-sweep recovery).
//   - flow.Retry rewind vs Cancel (the status='running' rewind guard).
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

// TestCancelVsTransition_Deterministic pins the transition tx's write-first terminal-status guard: a Cancel
// that terminalizes a flow after its step was marked completed but before the transition transaction runs
// must make the transition a clean no-op - no successor step is inserted into the cancelled flow, and no
// orphan results. The checkpoint makes the Cancel-wins ordering deterministic.
func TestCancelVsTransition_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var bRan int
	proxy := NewTestProxy()
	g := workflow.NewGraph("Lin")
	g.SetEndpoint("A", "cvt/a")
	g.SetEndpoint("B", "cvt/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("cvt/g", g)
	proxy.HandleTask("cvt/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("cvt/b", func(ctx context.Context, f *workflow.Flow) error { bRan++; return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Freeze A after it is marked completed, before it inserts B.
	e.seams.Break(checkpointBeforeTransitionTx)
	fk, err := e.Create(ctx, "cvt/g", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, 10*time.Second, checkpointBeforeTransitionTx), "engine never reached checkpoint checkpointBeforeTransitionTx")

	// Cancel wins while A's transition is held: the flow goes cancelled under the flow-row lock.
	assert.NoError(e.Cancel(ctx, fk, "test"))

	// Release A: its transition tx's guard (status NOT IN terminal) matches zero rows and inserts nothing.
	e.seams.Resume(checkpointBeforeTransitionTx)
	awaitFlowStatus(t, e, fk, workflow.StatusCancelled, 10*time.Second)

	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	var bSteps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='B'", flowID).Scan(&bSteps))
	assert.Equal(0, bSteps) // no successor inserted into the cancelled flow
	assert.Equal(0, bRan)   // B never dispatched
	assertInvariants(t, e)  // no orphan
}

// TestCancelVsSubgraphSpawn_Deterministic pins the orphaned-child recovery: a Cancel that terminalizes the
// tree in the window after the caller step parked but before the child flow was inserted leaves a live child
// under a terminal parent - an orphan no lifecycle op reaches. The wedge sweep's recoverOrphanedSubgraphChildren
// must cancel it. The checkpoint manufactures this residue deterministically (vs. the Cancel-vs-spawn timing race).
func TestCancelVsSubgraphSpawn_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	xRelease := make(chan struct{})
	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "cvs/call")
	parent.AddTransition("Call", workflow.END)
	assert.NoError(parent.Validate())
	proxy.HandleGraph("cvs/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "cvs/x")
	child.AddTransition("X", workflow.END)
	assert.NoError(child.Validate())
	proxy.HandleGraph("cvs/child", child)
	proxy.HandleTask("cvs/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("cvs/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	// X blocks so the orphaned child stays non-terminal (running) when the sweep runs; released at cleanup.
	proxy.HandleTask("cvs/x", func(ctx context.Context, f *workflow.Flow) error {
		<-xRelease
		return nil
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Release X AFTER Startup so this cleanup runs BEFORE the engine's Shutdown cleanup (LIFO): Shutdown drains
	// workers, so a worker still blocked in X must be unblocked first or the drain deadlocks.
	t.Cleanup(func() { close(xRelease) })

	// Freeze the caller after it parked, before createSubgraphFlow inserts the child.
	e.seams.Break(checkpointAfterCallerPark)
	fk, err := e.Create(ctx, "cvs/parent", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, 10*time.Second, checkpointAfterCallerPark), "engine never reached checkpoint checkpointAfterCallerPark")

	// Cancel the tree while the child does not yet exist: teardown works from a scan taken before the child.
	assert.NoError(e.Cancel(ctx, fk, "test"))

	// Release the caller: createSubgraphFlow now inserts the child under the already-cancelled parent - orphan.
	e.seams.Resume(checkpointAfterCallerPark)

	shardNum, parentFlowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)

	// Wait for the orphaned child flow to be inserted (surgraph_flow_id -> the cancelled parent).
	var childFlowID int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		db.QueryRowContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id=?", parentFlowID).Scan(&childFlowID)
		if childFlowID != 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !assert.True(childFlowID != 0, "orphaned child flow was never inserted") {
		return
	}

	// The sweep (minAge 0, bypassing the steady-state age guard) must cancel the orphaned child's subtree.
	e.recoverOrphanedSubgraphChildren(ctx, db, shardNum, 0)

	var childStatus string
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", childFlowID).Scan(&childStatus)
		if strings.TrimSpace(childStatus) == workflow.StatusCancelled {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.Equal(workflow.StatusCancelled, strings.TrimSpace(childStatus)) // orphan shares the parent's terminal fate
	assertInvariants(t, e)
}

// TestRetryRewindVsCancel_Deterministic pins the flow.Retry rewind's status='running' guard: a Cancel that
// terminalizes the running step before the rewind must stop the rewind from reviving the cancelled (immutable)
// step to pending and from reaping the cancelled tree's children. The checkpoint makes the Cancel-wins ordering
// deterministic.
func TestRetryRewindVsCancel_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aRuns int
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "rrc/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("rrc/g", g)
	proxy.HandleTask("rrc/a", func(ctx context.Context, f *workflow.Flow) error {
		aRuns++
		f.Retry(0, 1.0, 0, time.Hour) // arm a retry so processStep reaches the rewind
		return nil
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Freeze A before its retry rewind.
	e.seams.Break(checkpointBeforeRetryRewind)
	fk, err := e.Create(ctx, "rrc/g", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, 10*time.Second, checkpointBeforeRetryRewind), "engine never reached checkpoint checkpointBeforeRetryRewind")

	// Cancel wins: A's running step is flipped cancelled under the cancel transaction.
	assert.NoError(e.Cancel(ctx, fk, "test"))

	// Release A: the rewind's status='running' guard matches zero rows, so the cancelled step is not revived.
	e.seams.Resume(checkpointBeforeRetryRewind)
	awaitFlowStatus(t, e, fk, workflow.StatusCancelled, 10*time.Second)

	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shardNum)
	assert.NoError(err)
	var stepStatus string
	var attempt int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, attempt FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&stepStatus, &attempt))
	assert.Equal(workflow.StatusCancelled, strings.TrimSpace(stepStatus)) // not revived to pending
	assert.Equal(0, attempt)                                              // rewind did not bump the attempt
	assert.Equal(1, aRuns)                                                // no re-dispatch
	assertInvariants(t, e)
}
