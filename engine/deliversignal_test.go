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

// enqueueRecorder wraps a TestProxy and records every signal the engine hands to SignalPeers, so a
// test can replay a genuine, engine-serialized doorbell rather than hand-rolling the wire format, and
// can assert on what a given engine state does (or does not) broadcast.
type enqueueRecorder struct {
	*TestProxy
	mu       sync.Mutex
	payloads [][]byte
	ops      []string
}

func (r *enqueueRecorder) SignalPeers(ctx context.Context, op string, payload []byte) {
	r.mu.Lock()
	r.payloads = append(r.payloads, append([]byte(nil), payload...))
	r.ops = append(r.ops, op)
	r.mu.Unlock()
	r.TestProxy.SignalPeers(ctx, op, payload)
}

// all returns the enqueue payloads only - the doorbells a replay test needs.
func (r *enqueueRecorder) all() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]byte
	for i, p := range r.payloads {
		if signalOp(r.ops[i]) == signalOpEnqueue {
			out = append(out, p)
		}
	}
	return out
}

// count returns how many signals of any kind have been emitted.
func (r *enqueueRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// TestDeliverSignal_IdempotentAndSpoofSafe pins the documented trust-boundary claim (see
// DeliverSignal's godoc): duplicate and spoofed peer signals are harmless. A doorbell is a hint, never
// an ownership grant - the claim CAS arbitrates - so replaying a real enqueue 100x, firing enqueues for
// completed/nonexistent steps and invalid shards, and a statusChange for an unknown key must not re-execute
// any task nor panic. DeliverSignal returns an error only for genuinely malformed input (bad JSON, unknown
// op), and the engine keeps processing new work afterward.
func TestDeliverSignal_IdempotentAndSpoofSafe(t *testing.T) {
	t.Parallel()
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

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetWorkers(2))
	eng.SetHost(rec)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

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
			// Rewrite the Origin to a foreign instance: a replay of this engine's own payload would be
			// discarded by the echo-suppression check before ever reaching the claim CAS this test pins.
			ep.Origin = "some-peer"
			legit, _ = json.Marshal(ep)
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

// TestDeliverSignal_OfflineEngineIgnoresSignals pins the offline contract: an engine that is not
// running (never started, or already shut down) discards every inbound signal. It must not enqueue
// work (there is no cache), must not wake a waiter, and must not re-read the peer registry (its shards
// are closed) - a peersChanged nudge to a dead replica is inert.
func TestDeliverSignal_OfflineEngineIgnoresSignals(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	// A host that records every outbound signal, so we can prove a shut-down engine emits nothing more.
	base := NewTestProxy()
	rec := &enqueueRecorder{TestProxy: base}

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetHost(rec))
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	assert.NoError(eng.Shutdown(ctx)) // the post-shutdown engine is the target

	emittedByShutdown := rec.count() // the shutdown deregister nudge is legitimate; anything after it is not

	pc, _ := json.Marshal(peerPayload{Origin: "peer-x"})
	enq, _ := json.Marshal(enqueuePayload{Origin: "peer-x", Shard: 1, StepID: 1})
	sc, _ := json.Marshal(statusChangePayload{Origin: "peer-x", FlowKey: "1-1-aaaaaaaaaaaaaaaa", Status: workflow.StatusCompleted})

	// Every op is accepted (fire-and-forget hints never error) and every one is a no-op.
	assert.NoError(eng.DeliverSignal(ctx, string(signalOpPeersChanged), pc))
	assert.NoError(eng.DeliverSignal(ctx, string(signalOpEnqueue), enq))
	assert.NoError(eng.DeliverSignal(ctx, string(signalOpStatusChange), sc))

	// The peersChanged nudge triggered no registry re-read (which would fail on closed shards) and no
	// broadcast, and the offline engine emitted nothing beyond its own shutdown deregister nudge.
	assert.Equal(emittedByShutdown, rec.count(), "a shut-down engine must not broadcast or recount")
}

// TestDeliverSignal_IgnoresOwnEcho pins the echo-suppression contract: SignalPeers asks the host to
// deliver only to OTHER replicas, but a broadcast transport may echo the signal back to the sender, so
// every outbound payload carries the engine's random instanceID as Origin and DeliverSignal silently
// discards a payload whose Origin is its own. A payload with a foreign or absent Origin (an older
// build's signal) is processed normally.
func TestDeliverSignal_IgnoresOwnEcho(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetHost(noopHost{}))
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// A statusChange echo observability hook: register a waiter and see whether a signal wakes it.
	// The waiters map is created lazily by the first Await, so initialize it here.
	woke := func(payload statusChangePayload) bool {
		ch := make(chan string, 1)
		eng.waitersLock.Lock()
		if eng.waiters == nil {
			eng.waiters = make(map[string][]chan string)
		}
		eng.waiters[payload.FlowKey] = append(eng.waiters[payload.FlowKey], ch)
		eng.waitersLock.Unlock()
		b, _ := json.Marshal(payload)
		assert.NoError(eng.DeliverSignal(ctx, string(signalOpStatusChange), b))
		select {
		case <-ch:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}

	key := "1-424242-deadbeefdeadbeef"
	assert.False(woke(statusChangePayload{Origin: eng.instanceID, FlowKey: key, Status: workflow.StatusCompleted}),
		"the engine's own echoed signal must be discarded")
	assert.True(woke(statusChangePayload{Origin: "some-peer", FlowKey: key, Status: workflow.StatusCompleted}),
		"a peer's signal must be processed")
	assert.True(woke(statusChangePayload{FlowKey: key, Status: workflow.StatusCompleted}),
		"an origin-less signal (older build) must be processed")
}
