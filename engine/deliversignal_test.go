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

// signalRecorder wraps a TestProxy and records every signal the engine hands to SignalPeers, so a test
// can assert on what a given engine state does (or does not) broadcast.
type signalRecorder struct {
	*TestProxy
	mu       sync.Mutex
	payloads [][]byte
	opsSeen  []string
}

func (r *signalRecorder) SignalPeers(ctx context.Context, op string, payload []byte) {
	r.mu.Lock()
	r.payloads = append(r.payloads, append([]byte(nil), payload...))
	r.opsSeen = append(r.opsSeen, op)
	r.mu.Unlock()
	r.TestProxy.SignalPeers(ctx, op, payload)
}

// count returns how many signals of any kind have been emitted.
func (r *signalRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.opsSeen)
}

// ops returns a copy of the op routing keys emitted so far, in order.
func (r *signalRecorder) ops() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.opsSeen...)
}

// TestDeliverSignal_IdempotentAndSpoofSafe pins the documented trust-boundary claim (see
// DeliverSignal's godoc): duplicate and spoofed peer signals are harmless. Replaying a statusChange for
// an unknown key, and hammering the entry point with malformed and unknown-op input, must not re-execute
// any task nor panic. DeliverSignal returns an error only for genuinely malformed input (bad JSON,
// unknown op), and the engine keeps processing new work afterward.
//
// It also pins that `enqueue` is now an UNKNOWN op: the per-step work doorbell was removed, so a peer
// running an older build that still broadcasts one gets a clean error rather than silent acceptance, and
// no code path re-admits it.
func TestDeliverSignal_IdempotentAndSpoofSafe(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var calls int64
	var callsMu sync.Mutex
	base := NewTestProxy()
	rec := &signalRecorder{TestProxy: base}

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
	assert.NoError(eng.Startup(t.Context()))

	// Run a flow to completion; its steps are what a spoofed signal would try to re-execute.
	_, outcome, err := eng.Run(ctx, "ds/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	callsMu.Lock()
	callsBefore := calls
	callsMu.Unlock()

	// 1. statusChange for an unknown key, 100x - only wakes local Await waiters (there are none).
	sc, _ := json.Marshal(statusChangePayload{FlowKey: "1-424242-deadbeefdeadbeef", Status: workflow.StatusCompleted})
	for range 100 {
		assert.NoError(eng.DeliverSignal(ctx, string(signalOpStatusChange), sc))
	}
	// 2. A peersChanged nudge is a pure registry re-read; spoofing it cannot dispatch work.
	pc, _ := json.Marshal(peerPayload{Origin: "some-peer"})
	assert.NoError(eng.DeliverSignal(ctx, string(signalOpPeersChanged), pc))
	// 3. Malformed input errors: garbage JSON, an unknown op, and - now - the RETIRED enqueue op, whose
	//    removal must be a clean rejection rather than a silently-ignored payload.
	assert.Error(eng.DeliverSignal(ctx, string(signalOpStatusChange), []byte("{not json")))
	assert.Error(eng.DeliverSignal(ctx, "bogusOp", []byte("{}")))
	assert.Error(eng.DeliverSignal(ctx, "enqueue", []byte(`{"Shard":1,"StepID":1}`)),
		"the retired per-step work doorbell must be an unknown op, not a live path")

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
	rec := &signalRecorder{TestProxy: base}

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetHost(rec))
	assert.NoError(eng.Startup(t.Context()))
	assert.NoError(eng.Shutdown(ctx)) // the post-shutdown engine is the target

	emittedByShutdown := rec.count() // the shutdown deregister nudge is legitimate; anything after it is not

	pc, _ := json.Marshal(peerPayload{Origin: "peer-x"})
	sc, _ := json.Marshal(statusChangePayload{Origin: "peer-x", FlowKey: "1-1-aaaaaaaaaaaaaaaa", Status: workflow.StatusCompleted})

	// Every live op is accepted (fire-and-forget hints never error) and every one is a no-op. The offline
	// check runs BEFORE the op switch, so even a retired op is absorbed rather than reported unknown.
	assert.NoError(eng.DeliverSignal(ctx, string(signalOpPeersChanged), pc))
	assert.NoError(eng.DeliverSignal(ctx, string(signalOpStatusChange), sc))
	assert.NoError(eng.DeliverSignal(ctx, "enqueue", []byte(`{"Shard":1,"StepID":1}`)))

	// The peersChanged nudge triggered no registry re-read (which would fail on closed shards) and no
	// broadcast, and the offline engine emitted nothing beyond its own shutdown deregister nudge.
	assert.Equal(emittedByShutdown, rec.count(), "a shut-down engine must not broadcast or recount")
}

// TestDeliverSignal_IgnoresOwnEcho pins the echo-suppression contract: SignalPeers asks the host to
// deliver only to OTHER replicas, but a broadcast transport may echo the signal back to the sender, so
// every outbound payload carries the engine's random engineIDBase36 as Origin and DeliverSignal silently
// discards a payload whose Origin is its own. A payload with a foreign or absent Origin (an older
// build's signal) is processed normally.
func TestDeliverSignal_IgnoresOwnEcho(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetHost(noopHost{}))
	assert.NoError(eng.Startup(t.Context()))

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
	assert.False(woke(statusChangePayload{Origin: eng.engineIDBase36, FlowKey: key, Status: workflow.StatusCompleted}),
		"the engine's own echoed signal must be discarded")
	assert.True(woke(statusChangePayload{Origin: "some-peer", FlowKey: key, Status: workflow.StatusCompleted}),
		"a peer's signal must be processed")
	assert.True(woke(statusChangePayload{FlowKey: key, Status: workflow.StatusCompleted}),
		"an origin-less signal (older build) must be processed")
}

// TestSignals_VolumeDoesNotScaleWithSteps pins the property that removing the per-step work doorbell
// bought: outbound peer-signal volume is O(flows), not O(steps).
//
// The `enqueue` broadcast fired at every step-origination site - every created, completed, retried,
// fanned-out and fanned-in step - so a D-step flow emitted ~D signals per peer, and completeFlow emitted
// one more with a {0,0} sentinel payload on top. Under load that is the dominant cross-replica cost while
// buying no dispatch latency the receiver's own refiller scan would not have covered (and it cost every
// receiver a PK lookup plus an unpartitioned head-insert racing the residue class's owner).
//
// Asserting the op names rather than a count is what makes this durable: it fails loudly if any per-step
// broadcast is reintroduced under a new op name, and it survives Phase 2 removing statusChange too (the
// remaining assertion - no op scales with D - still holds).
func TestSignals_VolumeDoesNotScaleWithSteps(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	base := NewTestProxy()
	rec := &signalRecorder{TestProxy: base}

	// A 6-step chain: if anything still broadcasts per step, the count scales with this length.
	g := workflow.NewGraph("Chain")
	names := []string{"A", "B", "C", "D", "E", "F"}
	for _, n := range names {
		g.SetEndpoint(n, "sigvol/nop")
	}
	g.AddTransitionChain(append(names, workflow.END)...)
	assert.NoError(g.Validate())
	base.HandleGraph("sigvol/g", g)
	base.HandleTask("sigvol/nop", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetHost(rec))
	assert.NoError(eng.Startup(t.Context()))

	// Startup's own peersChanged join nudge is legitimate and not per-step; count from after it.
	baseline := rec.count()

	_, outcome, err := eng.Run(ctx, "sigvol/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	var perStep []string
	for _, op := range rec.ops()[baseline:] {
		// statusChange is per-FLOW (one terminal stop, gated on the awaited flag) - Run awaits, so exactly
		// one is expected here. peersChanged is per-deployment-event. Anything else is per-step by
		// elimination, since a step transition is the only other thing that happened.
		if signalOp(op) != signalOpStatusChange && signalOp(op) != signalOpPeersChanged {
			perStep = append(perStep, op)
		}
	}
	assert.Zero(len(perStep), "no per-step op may be broadcast; got %v", perStep)
	// And the total stays in flow-scale territory rather than step-scale: 6 tasks ran, so a per-step
	// broadcast would put this at >= 6 regardless of which op carried it.
	assert.True(rec.count()-baseline < len(names),
		"signal volume must not scale with step count: %d signals for %d steps", rec.count()-baseline, len(names))
}
