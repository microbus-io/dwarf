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
	"time"

	"github.com/microbus-io/errors"
)

// signalOp identifies which internal handler an inbound peer signal targets. It is the op routing key
// passed across the host boundary as a plain string.
type signalOp string

const (
	signalOpEnqueue      signalOp = "enqueue"
	signalOpStatusChange signalOp = "statusChange"
	signalOpHello        signalOp = "hello"   // a replica came online; receivers reply with a ping
	signalOpPing         signalOp = "ping"    // periodic liveness heartbeat
	signalOpGoodbye      signalOp = "goodbye" // graceful shutdown; receivers drop the sender immediately
)

// Per-op payload bodies. The engine marshals these in emitSignal and unmarshals the received bytes in
// DeliverSignal. Origin carries the sending engine's random instanceID: SignalPeers' contract asks the
// host to deliver only to OTHER replicas, but a broadcast transport may echo the signal back to the
// sender - DeliverSignal discards a payload whose Origin matches its own instanceID rather than rely
// on the host. (An empty Origin - e.g. a signal from an older build - is never discarded.)
type (
	enqueuePayload struct {
		Origin        string
		Shard, StepID int
	}
	statusChangePayload struct {
		Origin          string
		FlowKey, Status string
	}
	peerPayload struct {
		Origin string
	}
)

// emitSignal serializes a signal body and hands (op, bytes) to the host for delivery to OTHER replicas.
func (e *Engine) emitSignal(ctx context.Context, op signalOp, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		e.logger.ErrorContext(ctx, "Marshaling peer signal", "op", string(op), "error", err)
		return
	}
	err = errors.CatchPanic(func() error {
		// faultSignalPeersPanic simulates the host's SignalPeers panicking, so the test can prove the
		// boundary CatchPanic swallows it (logged below) and flow completion / Await are unaffected. Scoped
		// by op so a test targets the terminal statusChange wake without also tripping enqueue doorbells.
		if e.seams.IsFault(faultSignalPeersPanic, string(op)) {
			panic("injected fault: " + faultSignalPeersPanic + " " + string(op))
		}
		e.host.SignalPeers(ctx, string(op), data)
		return nil
	})
	if err != nil {
		e.logger.ErrorContext(ctx, "SignalPeers callback panicked", "op", string(op), "error", err)
	}
}

func (e *Engine) signalEnqueue(ctx context.Context, shard, stepID int) {
	e.emitSignal(ctx, signalOpEnqueue, enqueuePayload{Origin: e.instanceID, Shard: shard, StepID: stepID})
}

func (e *Engine) signalStatusChange(ctx context.Context, flowKey, status string) {
	e.emitSignal(ctx, signalOpStatusChange, statusChangePayload{Origin: e.instanceID, FlowKey: flowKey, Status: status})
}

// DeliverSignal processes an inbound peer signal. The host calls it with the op routing key and the
// payload bytes it received from a peer (the JSON encoding of what the engine handed that peer's
// SignalPeers). It delegates by op to the matching internal handler. op and payload are opaque to the
// host; only the engine interprets them.
//
// Trust boundary: the host MUST authenticate the peer channel; a signal admitted here is trusted.
//
// An engine that is not running (never started, or already shut down) discards every signal: there is
// no cache to ring a doorbell into, no waiter to wake, and no pool to resize, and a hello answered by
// a dead replica would advertise it to the fleet - inflating every peer's observed R and shrinking
// their connection budgets for a replica that will never claim a step. A host that keeps feeding a
// shut-down engine (a slow bus drain, a late redelivery) is therefore harmless. Returns nil, not an
// error: peer signals are fire-and-forget hints, and a dropped one is always recoverable by the
// receiver's own backstop (the poll for doorbells, the Await re-snapshot for status changes, the next
// ping for peer discovery).
func (e *Engine) DeliverSignal(ctx context.Context, op string, payload []byte) error {
	if !e.started.Load() {
		return nil
	}
	switch signalOp(op) {
	case signalOpEnqueue:
		var p enqueuePayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		if p.Origin == e.instanceID {
			return nil // the host echoed this engine's own broadcast back; nothing new to learn
		}
		e.handleEnqueue(ctx, p.Shard, p.StepID)
	case signalOpStatusChange:
		var p statusChangePayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		if p.Origin == e.instanceID {
			return nil
		}
		e.notifyStatusChange(p.FlowKey, p.Status)
	case signalOpHello, signalOpPing, signalOpGoodbye:
		var p peerPayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		if p.Origin == e.instanceID || p.Origin == "" {
			return nil // own echo, or a malformed origin that must not pollute the peer map
		}
		e.handlePeerSignal(ctx, signalOp(op), p.Origin)
	default:
		return errors.New("unknown peer signal op: %q", op)
	}
	return nil
}

// --- Peer discovery (observed replica count R) ---
//
// The engine discovers how many replicas share its shards' databases from the peer signals alone:
// "hello" on startup (receivers reply with an immediate "ping" so the joiner converges in one round
// trip), "ping" re-announced every pingInterval, "goodbye" on graceful shutdown. Each replica keeps
// map[instanceID]lastSeen (self included, refreshed by its own loop); entries not heard from for
// 3 x pingInterval are pruned by the loop, which covers crashed peers - their goodbye never comes.
// R = len(map). R divides the derived per-shard connection pools (see recomputePools): the budget is
// a property of the shard's DATABASE, and n replicas each holding the full budget would overshoot the
// measured knee n times over.
//
// This is deliberately a LOOKUP, not a control loop: the count is exact and discrete, and it is
// independent of the actuation (a shrunk pool still pings). R is a tuning number - a wrong count
// mis-sizes pools and degrades performance but corrupts nothing - which is why, unlike the doorbell
// and statusChange signals, it rides best-effort transport with no database backstop. Asymmetry by
// construction: a new peer shrinks pools at once (hello/ping handled immediately - overshoot is what
// harms); a vanished peer grows them lazily (only when the loop prunes it - undershoot merely slows).

// handlePeerSignal updates the peer map for an inbound hello/ping/goodbye and recomputes the pools
// when the fleet changed. A hello is answered with an immediate ping so the joiner learns this
// replica without waiting a full heartbeat.
func (e *Engine) handlePeerSignal(ctx context.Context, op signalOp, origin string) {
	e.peersLock.Lock()
	before := len(e.peers)
	switch op {
	case signalOpHello, signalOpPing:
		e.peers[origin] = time.Now()
	case signalOpGoodbye:
		delete(e.peers, origin)
	}
	changed := len(e.peers) != before
	e.peersLock.Unlock()
	if op == signalOpHello {
		// Answer the joiner so it learns this replica within one round trip rather than a full
		// heartbeat. Only a RUNNING engine ever gets here (DeliverSignal discards signals otherwise),
		// so a shut-down replica never advertises itself into a peer's replica count.
		e.emitSignal(ctx, signalOpPing, peerPayload{Origin: e.instanceID})
	}
	if changed {
		e.recomputePools()
	}
}

// observedReplicas returns the observed replica count R: the peers heard from recently, self included.
// Stale entries are pruned by peersLoop, so this is a plain len under the lock.
func (e *Engine) observedReplicas() int {
	e.peersLock.Lock()
	defer e.peersLock.Unlock()
	return max(1, len(e.peers))
}

// runPeersLoop is the heartbeat: broadcast a ping every pingInterval, refresh self, prune peers not
// heard from for 3 intervals (a crashed peer's goodbye never comes), and recompute pools when the
// prune shrank the fleet. Started by initRuntime; stopped via peersStop in drainRuntime.
func (e *Engine) runPeersLoop() {
	ticker := time.NewTicker(e.pingInterval)
	defer ticker.Stop()
	// The peer-discovery window closes on a TIMER, not on a signal, and it lives here because this loop already
	// owns peer state and is already drained by peersLoop.Wait().
	//
	// It cannot be a timestamp consulted lazily, because nothing would consult it: a SOLO replica receives no peer
	// signals at all (a host does not echo a hello back to its sender), and it is exactly the replica that must
	// eventually take its full budget. Something has to fire on its own.
	//
	// Nor can it be the first reply. Replies land at different latencies, so mid-window the count is still climbing
	// (1 -> 2 -> ... -> R): sizing on the first one would pick R=2 when the truth may be 8, over-connecting
	// fourfold - the very failure the startup cap prevents. Peers register throughout; the pools stay capped until
	// the window closes, and then ONE recompute runs on the count we converged on.
	// A nil channel blocks forever in a select, so when there is nothing to wait for (an explicit
	// SetMaxOpenConns pins the pools, or a test disabled the window) no timer is armed at all.
	var graceC <-chan time.Time
	if !e.poolGraceDone.Load() {
		grace := time.NewTimer(startupPoolGrace)
		defer grace.Stop()
		graceC = grace.C
	}
	for {
		select {
		case <-e.peersStop:
			return
		case <-graceC:
			e.poolGraceDone.Store(true)
			e.recomputePools()
			continue
		case <-ticker.C:
		}
		e.emitSignal(e.lifetimeCtx, signalOpPing, peerPayload{Origin: e.instanceID})
		cutoff := time.Now().Add(-3 * e.pingInterval)
		e.peersLock.Lock()
		e.peers[e.instanceID] = time.Now()
		pruned := false
		for id, seen := range e.peers {
			if id != e.instanceID && seen.Before(cutoff) {
				delete(e.peers, id)
				pruned = true
			}
		}
		e.peersLock.Unlock()
		if pruned {
			e.recomputePools()
		}
	}
}
