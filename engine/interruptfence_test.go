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
	"github.com/microbus-io/testarossa"
)

// TestFault_InterruptStaleWriteRollback pins the interrupt lease-fence — the ONLY fence in the engine that
// rolls its whole transaction back (via errLeaseFenced) instead of no-op'ing on a zero-row match. A subgraph
// child's interrupt re-parks the ancestor caller (running+parkedSubgraph → interrupted) in the SAME combined
// UPDATE that interrupts the leaf; if a zombie holding a stale lease_seq were let through, it would flip the
// caller out of parkedSubgraph and strand the parent revive — so the fence must undo the re-park, which only a
// full rollback can do.
//
// faultInterruptStaleWrite forces the in-tx leaf lease_seq to look re-granted on the FIRST interrupt attempt
// (exactly as a real peer re-claim would). The transaction rolls back, the worker abandons, and the child step
// stays running-and-leased; once its short lease lapses the background poll re-dispatches it and the SECOND
// attempt (fault consumed) drives the real interrupt up the whole chain with no strand. A root Resume then
// threads back down to the leaf and the tree completes — proving the rollback left the ancestor park intact.
func TestFault_InterruptStaleWriteRollback(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "fisw/call")
	parent.AddTransition("Call", workflow.END)
	proxy.HandleGraph("fisw/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "fisw/x")
	child.AddTransition("X", workflow.END)
	proxy.HandleGraph("fisw/child", child)

	proxy.HandleTask("fisw/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("fisw/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	// Atomic: the worker goroutine writes it while the test goroutine waits on it below.
	var xCalls atomic.Int32
	proxy.HandleTask("fisw/x", func(ctx context.Context, f *workflow.Flow) error {
		xCalls.Add(1)
		yield, err := f.Interrupt(map[string]any{"ask": "approve?"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	e.SetTimeBudget(200 * time.Millisecond)
	e.leaseMargin = 100 * time.Millisecond // lease = budget+margin = 300ms, so lease recovery re-dispatches fast
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	e.seams.Inject(faultInterruptStaleWrite)
	fk, err := e.Create(ctx, "fisw/parent", nil, nil)
	assert.NoError(err)

	// The fenced first attempt rolls back and returns nil (abandon quietly), leaving the child step
	// running-and-leased — it does not schedule its own recovery, so drive the lease-recovery backstop until
	// the re-dispatch lands (see drivePollBackstop; this is the site whose fixed-sleep version flaked).
	drivePollBackstop(t, e, pollBackstopWait, func() bool { return xCalls.Load() >= 2 })

	// The flow reaches interrupted only via that recovered second attempt — the fenced first attempt rolled
	// back, so the interrupt could not take on the first try.
	awaitFlowStatus(t, e, fk, workflow.StatusInterrupted, 10*time.Second)
	assert.Equal(int32(2), xCalls.Load()) // fenced attempt + recovered real attempt: proof the fence forced a re-dispatch
	assertInvariants(t, e)                // no strand: the rollback undid the ancestor re-park, so no terminal-flow-with-live-step

	// The rollback left a clean, resumable tree: Resume threads down to the leaf and the whole tree completes.
	err = e.Resume(ctx, fk, map[string]any{"answer": "yes"})
	assert.NoError(err)
	awaitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)
	assertInvariants(t, e)
}
