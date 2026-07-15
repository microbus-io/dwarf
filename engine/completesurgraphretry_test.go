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

// TestCompleteFlow_TransientReviveErrorIsRetriedNotLost pins the revive re-drive: completeFlow's write closure is
// idempotent for persist's retry, INCLUDING its post-commit surgraph revive. completeFlow is two phases - a
// transaction that terminalizes the flow row, then a post-commit completeSurgraphFlow that re-dispatches the
// parked parent caller. If the transaction commits but completeSurgraphFlow then hits a transient DB error,
// persist re-runs the closure; on that retry the status UPDATE no-ops (flow already completed), so the revive
// must be re-driven from the !completed branch rather than silently skipped - otherwise the parent's caller
// step strands running+parkedSubgraph until the ~10m parked-step wedge sweep (with a false wedge alarm).
//
// faultCompleteSurgraphErr fails the FIRST revive with a non-contention error; the retry (fault consumed) must
// re-drive it and the whole parent+child tree must complete promptly - not wait out the wedge sweep.
func TestCompleteFlow_TransientReviveErrorIsRetriedNotLost(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	shortPersistBackoff(t)

	proxy := NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "csr/call")
	parent.AddTransition("Call", workflow.END)
	assert.NoError(parent.Validate())
	proxy.HandleGraph("csr/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "csr/x")
	child.AddTransition("X", workflow.END)
	assert.NoError(child.Validate())
	proxy.HandleGraph("csr/child", child)

	var callRuns, xRuns atomic.Int32
	proxy.HandleTask("csr/call", func(ctx context.Context, f *workflow.Flow) error {
		callRuns.Add(1)
		yield, err := f.Subgraph("csr/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil // re-entry after the child completed and the caller was revived
	})
	proxy.HandleTask("csr/x", func(ctx context.Context, f *workflow.Flow) error {
		xRuns.Add(1)
		return nil
	})

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// The first completeSurgraphFlow (driven by the child's completion) errors; persist must re-drive it.
	e.seams.InjectN(1, faultCompleteSurgraphErr)

	fk, err := e.Create(ctx, "csr/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}

	// With the fix the retry re-drives the revive within a backoff, so the tree completes promptly. Without it
	// the parent strands running+parkedSubgraph and only the ~10m wedge sweep would recover it - so this wait
	// times out, which is the failure this test pins.
	waitFlowStatus(t, e, fk, workflow.StatusCompleted, 15*time.Second)

	assert.Equal(int32(1), xRuns.Load(), "the child task runs once; persist retries the WRITE, not the task")
	assert.Equal(int32(2), callRuns.Load(), "the caller parks, then re-enters exactly once after the revive lands")
	assertInvariants(t, e) // no strand: no terminal flow left with a live parked caller
}
