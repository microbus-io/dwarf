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

// enqueueRecorder wraps a TestProxy and records every enqueue payload the engine hands to SignalPeers, so a
// test can replay a genuine, engine-serialized doorbell rather than hand-rolling the wire format.
type enqueueRecorder struct {
	*TestProxy
	mu       sync.Mutex
	payloads [][]byte
}

func (r *enqueueRecorder) SignalPeers(ctx context.Context, op string, payload []byte) {
	if signalOp(op) == signalOpEnqueue {
		r.mu.Lock()
		r.payloads = append(r.payloads, append([]byte(nil), payload...))
		r.mu.Unlock()
	}
	r.TestProxy.SignalPeers(ctx, op, payload)
}

func (r *enqueueRecorder) all() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.payloads))
	copy(out, r.payloads)
	return out
}

// TestDeliverSignal_IdempotentAndSpoofSafe pins the documented trust-boundary claim (see
// DeliverSignal's godoc): duplicate and spoofed peer signals are harmless. A doorbell is a hint, never
// an ownership grant - the claim CAS arbitrates - so replaying a real enqueue 100x, firing enqueues for
// completed/nonexistent steps and invalid shards, and a statusChange for an unknown key must not re-execute
// any task nor panic. DeliverSignal returns an error only for genuinely malformed input (bad JSON, unknown
// op), and the engine keeps processing new work afterward.
func TestDeliverSignal_IdempotentAndSpoofSafe(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	var calls int64
	var callsMu sync.Mutex
	base := NewTestProxy()
	rec := &enqueueRecorder{TestProxy: base}

	g := workflow.NewGraph("DS")
	g.SetEndpoint("A", "ds/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	base.HandleGraph("ds/g", g)
	base.HandleTask("ds/a", func(ctx context.Context, f *workflow.Flow) error {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		return nil
	})

	eng := NewEngine()
	assert.NoError(eng.SetWorkers(2))
	eng.SetHost(rec)
	eng.RunInTest(t)

	// Run a flow to completion, capturing a real enqueue payload for its (now-completed) entry step.
	_, outcome, err := eng.Run(ctx, "ds/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	var legit []byte
	for _, p := range rec.all() {
		var ep enqueuePayload
		if json.Unmarshal(p, &ep) == nil && ep.StepID != 0 {
			legit = p // a genuine doorbell for a real step (now completed)
			break
		}
	}
	if !assert.NotNil(legit, "should have captured a real enqueue payload") {
		return
	}

	callsMu.Lock()
	callsBefore := calls
	callsMu.Unlock()

	// 1. The same legit enqueue (now for a COMPLETED step) 100x - the CAS rejects every re-dispatch.
	for range 100 {
		assert.NoError(eng.DeliverSignal(ctx, string(signalOpEnqueue), legit))
	}
	// 2. Boundary / spoofed enqueues: sentinel {0,0}, an out-of-range shard, and a nonexistent step. None
	//    dispatch anything (bad shard is skipped; a bogus step id loses the claim CAS) and none panic.
	for _, ep := range []enqueuePayload{{Shard: 0, StepID: 0}, {Shard: 99, StepID: 1}, {Shard: 1, StepID: 999999}} {
		b, _ := json.Marshal(ep)
		assert.NoError(eng.DeliverSignal(ctx, string(signalOpEnqueue), b))
	}
	// 3. statusChange for an unknown key - only wakes local Await waiters (there are none), harmless.
	sc, _ := json.Marshal(statusChangePayload{FlowKey: "1-424242-deadbeefdeadbeef", Status: workflow.StatusCompleted})
	assert.NoError(eng.DeliverSignal(ctx, string(signalOpStatusChange), sc))
	// 4. Malformed input is the ONLY case that errors: garbage JSON and an unknown op.
	assert.Error(eng.DeliverSignal(ctx, string(signalOpEnqueue), []byte("{not json")))
	assert.Error(eng.DeliverSignal(ctx, "bogusOp", []byte("{}")))

	// No signal re-executed the already-completed step.
	time.Sleep(300 * time.Millisecond)
	callsMu.Lock()
	assert.Equal(callsBefore, calls, "no spoofed/duplicate signal re-executed the completed step")
	callsMu.Unlock()

	// The engine still processes NEW work after the spoof storm.
	_, outcome2, err := eng.Run(ctx, "ds/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome2.Status)
	callsMu.Lock()
	assert.Equal(callsBefore+1, calls)
	callsMu.Unlock()
}
