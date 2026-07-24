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
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// statusChangeRecorder wraps a TestProxy and records every statusChange broadcast the engine hands to
// SignalPeers, keyed by flow key, so a test can assert which flow stops were (not) broadcast to peers.
type statusChangeRecorder struct {
	*TestProxy
	mu    sync.Mutex
	byKey map[string][]string // flowKey -> statuses broadcast
}

func (r *statusChangeRecorder) SignalPeers(ctx context.Context, op string, payload []byte) {
	if signalOp(op) == signalOpStatusChange {
		var p statusChangePayload
		if json.Unmarshal(payload, &p) == nil {
			r.mu.Lock()
			if r.byKey == nil {
				r.byKey = map[string][]string{}
			}
			r.byKey[p.FlowKey] = append(r.byKey[p.FlowKey], p.Status)
			r.mu.Unlock()
		}
	}
	r.TestProxy.SignalPeers(ctx, op, payload)
}

func (r *statusChangeRecorder) statusesFor(flowKey string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.byKey[flowKey]))
	copy(out, r.byKey[flowKey])
	return out
}

// TestAwaited_GatesStatusChangeBroadcast pins the awaited broadcast gate: the statusChange peer signal's
// only purpose is to wake remote Await/Poll callers, so a flow that was never awaited stops without
// broadcasting, while a flow some caller awaited (its `awaited` column stamped 1) broadcasts its stop.
func TestAwaited_GatesStatusChangeBroadcast(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	base := NewTestProxy()
	rec := &statusChangeRecorder{TestProxy: base}

	release := make(chan struct{})
	gBlock := workflow.NewGraph("AwaitedBlock")
	gBlock.SetEndpoint("Blocker", "awaited/blocker")
	gBlock.AddTransition("Blocker", workflow.END)
	assert.NoError(gBlock.Validate())
	base.HandleGraph("awaited/gblock", gBlock)
	base.HandleTask("awaited/blocker", func(ctx context.Context, f *workflow.Flow) error {
		<-release
		return nil
	})

	gFast := workflow.NewGraph("AwaitedFast")
	gFast.SetEndpoint("Fast", "awaited/fast")
	gFast.AddTransition("Fast", workflow.END)
	assert.NoError(gFast.Validate())
	base.HandleGraph("awaited/gfast", gFast)
	base.HandleTask("awaited/fast", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetWorkers(2))
	eng.SetHost(rec)
	assert.NoError(eng.Startup(t.Context()))

	// Awaited flow: Poll stamps awaited=1 while the entry task is still blocked, so the completion
	// deterministically reads the flag and broadcasts the stop.
	awaitedKey, err := eng.Create(ctx, "awaited/gblock", nil, nil)
	if !assert.NoError(err) {
		return
	}
	pollCtx, pollCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	outcome, err := eng.Poll(pollCtx, awaitedKey)
	pollCancel()
	if !assert.NoError(err) {
		return
	}
	assert.False(outcome.Stopped())
	close(release)
	outcome, err = eng.Await(ctx, awaitedKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Contains(rec.statusesFor(awaitedKey), workflow.StatusCompleted,
		"an awaited flow's stop must be broadcast to peers")

	// Never-awaited flow: observe completion via Snapshot only (which does not stamp awaited), and
	// assert no statusChange broadcast was sent for it.
	fireForgetKey, err := eng.Create(ctx, "awaited/gfast", nil, nil)
	if !assert.NoError(err) {
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		out, err := eng.Snapshot(ctx, fireForgetKey)
		if !assert.NoError(err) {
			return
		}
		if out.Stopped() {
			assert.Equal(workflow.StatusCompleted, out.Status)
			break
		}
		if !assert.True(time.Now().Before(deadline), "flow did not complete in time") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	// signalStop runs just after the terminal commit Snapshot observed; give it a beat before asserting.
	time.Sleep(300 * time.Millisecond)
	assert.Len(rec.statusesFor(fireForgetKey), 0,
		"a never-awaited flow's stop must not be broadcast to peers")
}
