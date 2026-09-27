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

// TestCancelInTransitionGap pins the one window Cancel's per-step mark cannot reach: a step whose completion
// write has landed (so it is terminal, never marked) but whose successor is not inserted yet (so there is
// nothing to mark). Without the flow-level cancelled_at the successor is inserted unmarked and the flow runs
// to completed although Cancel returned nil; with it, the successor inherits the mark and is preempted.
func TestCancelInTransitionGap(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var bRan atomic.Int32
	proxy := NewTestProxy()
	g := workflow.NewGraph("CancelGap")
	g.SetEndpoint("A", "cgap/a")
	g.SetEndpoint("B", "cgap/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("cgap/g", g)
	proxy.HandleTask("cgap/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("cgap/b", func(ctx context.Context, f *workflow.Flow) error { bRan.Add(1); return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// Freeze A after its completion write, before the transition inserts B.
	e.seams.Break(CheckpointBeforeTransitionTx)
	fk, err := e.Create(ctx, "cgap/g", nil, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, CheckpointBeforeTransitionTx, 10*time.Second), "engine never reached checkpoint CheckpointBeforeTransitionTx")

	// Cancel lands in the gap: A is completed and B does not exist, so the per-step mark finds nothing.
	assert.NoError(e.Cancel(ctx, fk, "gap reason"))
	e.seams.Resume(CheckpointBeforeTransitionTx)

	outcome, err := e.Await(ctx, fk)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCancelled, outcome.Status, "B must inherit the cancellation, not run the flow to completion")
	assert.Equal("gap reason", outcome.CancelReason)
	assert.Equal(int32(0), bRan.Load(), "B must be preempted, not run")
}
