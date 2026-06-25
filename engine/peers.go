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

	"github.com/microbus-io/errors"
)

// Cross-replica coordination is funneled through a single host method, Host.SignalPeers, and a single
// inbound entry point, Engine.DeliverSignal. The op is a routing key the host may use as a topic/
// subject; the payload is opaque bytes the engine has already serialized. The host is a pure pipe - it
// ships (op, bytes) to peers and hands inbound (op, bytes) back, never branching on the signal kind nor
// touching the encoding. Adding a new signal kind in the future requires no host change.

// signalOp identifies which internal handler an inbound peer signal targets. It is the op routing key
// passed across the host boundary as a plain string.
type signalOp string

const (
	signalOpEnqueue      signalOp = "enqueue"
	signalOpStatusChange signalOp = "statusChange"
)

// Per-op payload bodies. The engine marshals these in emitSignal and unmarshals the received bytes in
// DeliverSignal.
type (
	enqueuePayload      struct{ Shard, StepID int }
	statusChangePayload struct{ FlowKey, Status string }
)

// emitSignal serializes a signal body and hands (op, bytes) to the host for delivery to OTHER replicas.
func (e *Engine) emitSignal(ctx context.Context, op signalOp, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		e.logger.ErrorContext(ctx, "Marshaling peer signal", "op", string(op), "error", err)
		return
	}
	e.host.SignalPeers(ctx, string(op), data)
}

func (e *Engine) signalEnqueue(ctx context.Context, shard, stepID int) {
	e.emitSignal(ctx, signalOpEnqueue, enqueuePayload{Shard: shard, StepID: stepID})
}

func (e *Engine) signalStatusChange(ctx context.Context, flowKey, status string) {
	e.emitSignal(ctx, signalOpStatusChange, statusChangePayload{FlowKey: flowKey, Status: status})
}

// DeliverSignal processes an inbound peer signal. The host calls it with the op routing key and the
// payload bytes it received from a peer (the JSON encoding of what the engine handed that peer's
// SignalPeers). It delegates by op to the matching internal handler. op and payload are opaque to the
// host; only the engine interprets them.
func (e *Engine) DeliverSignal(ctx context.Context, op string, payload []byte) error {
	switch signalOp(op) {
	case signalOpEnqueue:
		var p enqueuePayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		e.handleEnqueue(ctx, p.Shard, p.StepID)
	case signalOpStatusChange:
		var p statusChangePayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		e.notifyStatusChange(p.FlowKey, p.Status)
	default:
		return errors.New("unknown peer signal op: %q", op)
	}
	return nil
}
