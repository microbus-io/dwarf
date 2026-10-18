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

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestLockOrder_AscendingCopy pins the two halves of lockOrder: the chunked lock passes see the ids ascending, and
// the caller's list keeps its order, since Terminate keeps it aligned with each flow's key for the post-commit
// signals.
func TestLockOrder_AscendingCopy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	ids := []any{7, 3, 1025, 512, 3, 1}
	assert.Equal([]any{1, 3, 3, 7, 512, 1025}, lockOrder(ids))
	assert.Equal([]any{7, 3, 1025, 512, 3, 1}, ids)
}

// TestLockOrder_TreeOperationsLockAscending pins lockOrder at its call sites: Cancel's and Terminate's chunked
// lock passes bind the tree's flow ids ascending. The tree is built so the tree walk does not already yield them
// in that order: two branches of the root each call a subgraph, and the first child spawns a grandchild before
// the second child exists, so the walk lists the second child (a higher id) ahead of the grandchild.
func TestLockOrder_TreeOperationsLockAscending(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	grandchildRan := make(chan struct{})
	var grandchildOnce sync.Once
	mux := hosts.NewMux()
	root := workflow.NewGraph("LockOrderRoot")
	root.SetURL("Seed", "lockorder/seed")
	root.SetURL("Call", "lockorder/call")
	root.SetURL("Join", "lockorder/join")
	root.SetFanIn("Join")
	root.AddTransitionForEach("Seed", "Call", "branches", "n")
	root.AddTransitionChain("Call", "Join", workflow.End)
	mux.HandleGraph("lockorder/root", root)
	first := workflow.NewGraph("LockOrderFirst")
	first.SetURL("Spawn", "lockorder/spawn")
	first.AddTransition("Spawn", workflow.End)
	mux.HandleGraph("lockorder/first", first)
	for _, url := range []string{"lockorder/second", "lockorder/grandchild"} {
		g := workflow.NewGraph("LockOrderAsk")
		g.SetURL("Ask", url+"/ask")
		g.AddTransition("Ask", workflow.End)
		mux.HandleGraph(url, g)
	}
	mux.HandleTask("lockorder/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("branches", []int{0, 1})
		return nil
	})
	mux.HandleTask("lockorder/call", func(ctx context.Context, f *workflow.Flow) error {
		child := "lockorder/first"
		if f.GetInt("n") == 1 {
			// The second child is created only once the grandchild exists, so its id is the higher one.
			select {
			case <-grandchildRan:
			case <-ctx.Done():
				return ctx.Err()
			}
			child = "lockorder/second"
		}
		_, err := f.Subgraph(child, nil, nil)
		return err
	})
	mux.HandleTask("lockorder/spawn", func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Subgraph("lockorder/grandchild", nil, nil)
		return err
	})
	ask := func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(nil, nil)
		return err
	}
	mux.HandleTask("lockorder/second/ask", ask)
	mux.HandleTask("lockorder/grandchild/ask", func(ctx context.Context, f *workflow.Flow) error {
		grandchildOnce.Do(func() { close(grandchildRan) })
		return ask(ctx, f)
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "lockorder/root", nil, nil)
	assert.NoError(err)
	// Both leaves interrupted, so all four flows exist and rest non-terminal. handleInterrupt fires the root's
	// interrupted stop once per interrupting step.
	enginetest.AwaitVisitCount(t, e.seams, 2, 10*time.Second,
		engineseams.Join(engineseams.CheckpointFlowStopped, flowKey, workflow.StatusInterrupted))

	shard, rootID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	descendants, _, _, err := e.allSubgraphFlows(ctx, db, shard, rootID)
	assert.NoError(err)
	walked := append([]any{rootID}, descendants...)
	if !assert.Len(walked, 4) {
		return
	}
	assert.NotEqual(lockOrder(walked), walked, "the tree walk must list the ids out of order, or the test pins nothing")

	captured := func(op string) []any {
		v, ok := e.seams.Capture(engineseams.Join(engineseams.VariableLockOrder, op))
		if !ok {
			t.Fatalf("%s never recorded its lock order", op)
		}
		return v.([]any)
	}
	assert.NoError(e.Cancel(ctx, flowKey, "lock order"))
	assert.Equal(lockOrder(walked), captured("cancel"))
	assert.NoError(e.Terminate(ctx, flowKey, "lock order"))
	assert.Equal(lockOrder(walked), captured("terminate"))
}
