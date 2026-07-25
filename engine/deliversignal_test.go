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
	"github.com/microbus-io/sequel"
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
// DeliverSignal's godoc): duplicate and spoofed peer signals are harmless. Replaying the live op, and
// hammering the entry point with malformed and unimplemented-op input, must not re-execute any task nor
// panic. DeliverSignal errors only for genuinely malformed input (bad JSON, an op it does not
// implement), and the engine keeps processing new work afterward.
//
// The two unimplemented ops it names are the shapes peers.go forbids - a per-step work doorbell and a
// per-flow stop broadcast - so nothing can quietly re-admit either by accepting its op name.
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

	// 1. A peersChanged nudge, 100x - a pure registry re-read; spoofing it cannot dispatch work.
	pc, _ := json.Marshal(peerPayload{Origin: "some-peer"})
	for range 100 {
		assert.NoError(eng.DeliverSignal(ctx, string(signalOpPeersChanged), pc))
	}
	// 2. Malformed input errors: garbage JSON and an unknown op.
	assert.Error(eng.DeliverSignal(ctx, string(signalOpPeersChanged), []byte("{not json")))
	assert.Error(eng.DeliverSignal(ctx, "bogusOp", []byte("{}")))
	// 3. An op this engine does not implement is rejected rather than silently absorbed. The two named
	//    here are the ones a peer on an older build can actually send, so they are the concrete cases:
	//    a per-step work doorbell and a per-flow stop broadcast (see peers.go for why neither exists).
	assert.Error(eng.DeliverSignal(ctx, "enqueue", []byte(`{"Shard":1,"StepID":1}`)),
		"a per-step work doorbell is not a live path")
	assert.Error(eng.DeliverSignal(ctx, "statusChange", []byte(`{"FlowKey":"1-424242-deadbeefdeadbeef","Status":"completed"}`)),
		"a per-flow stop broadcast is not a live path")

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

	// The live op is accepted (fire-and-forget hints never error) and is a no-op. Offline is checked
	// BEFORE the op is looked at, so an op this engine does not implement is absorbed here rather than
	// reported unknown - a dead replica has nothing to say about what it would have done.
	assert.NoError(eng.DeliverSignal(ctx, string(signalOpPeersChanged), pc))
	assert.NoError(eng.DeliverSignal(ctx, "enqueue", []byte(`{"Shard":1,"StepID":1}`)))
	assert.NoError(eng.DeliverSignal(ctx, "statusChange", []byte(`{"FlowKey":"1-1-aaaaaaaaaaaaaaaa"}`)))

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

	// peersChanged is the observable op: it re-reads the registry, so planting peer rows WITHOUT forcing a
	// recount leaves the engine's count stale until a signal is actually acted on. R is therefore the
	// readout for "was this signal processed or discarded".
	plantPeers := func(ids ...int64) {
		for _, id := range ids {
			err := eng.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
				_, err := db.ExecContext(ctx,
					"INSERT INTO dwarf_peers (engine_id, seen_at, dispatched_at) VALUES (?, NOW_UTC(), NOW_UTC())", id)
				return err
			})
			assert.NoError(err)
		}
	}
	deliver := func(origin string) {
		b, _ := json.Marshal(peerPayload{Origin: origin})
		assert.NoError(eng.DeliverSignal(ctx, string(signalOpPeersChanged), b))
	}

	assert.Equal(1, eng.observedReplicas(), "solo to start with")
	plantPeers(918001, 918002)

	deliver(eng.engineIDBase36)
	assert.Equal(1, eng.observedReplicas(), "the engine's own echoed signal must be discarded, so no recount")

	deliver("some-peer")
	assert.Equal(3, eng.observedReplicas(), "a peer's signal must be processed")

	plantPeers(918003)
	deliver("") // an older build's signal carries no Origin
	assert.Equal(4, eng.observedReplicas(), "an origin-less signal (older build) must be processed")
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
		// peersChanged is per-deployment-event and is the only surviving op. Anything else is per-step or
		// per-flow by elimination, since running this graph is the only thing that happened.
		if signalOp(op) != signalOpPeersChanged {
			perStep = append(perStep, op)
		}
	}
	assert.Zero(len(perStep), "no per-step or per-flow op may be broadcast; got %v", perStep)
	// And the total stays in flow-scale territory rather than step-scale: 6 tasks ran, so a per-step
	// broadcast would put this at >= 6 regardless of which op carried it.
	assert.True(rec.count()-baseline < len(names),
		"signal volume must not scale with step count: %d signals for %d steps", rec.count()-baseline, len(names))
}
