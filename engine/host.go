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

	"github.com/microbus-io/dwarf/workflow"
)

// Host is the contract between the dwarf engine and the surrounding host application (the "foreman"
// adapter). The engine owns no transport of its own; it reaches workflow graphs and tasks, reports flow
// stops, and carries cross-replica coordination signals exclusively through the host. Register it once
// via Engine.WithHost.
//
// A host MUST implement LoadGraph and ExecuteTask. The remaining five methods are optional: an
// implementation may do nothing in them when it has no flow-stop notification need (FlowStopped) or runs
// single-replica with no cross-replica gossip (Enqueue, SyncValve, TripBreaker, NotifyStatusChange).
//
// Cross-replica signal contract (SignalPeers): the engine funnels all of its coordination signals
// (work doorbells, valve-rate gossip, breaker trips, flow-stop wakes) through this one method. op is a
// routing key the host may use as a topic/subject; payload is opaque bytes the engine already serialized.
// The host delivers (op, payload) to OTHER replicas, EXCLUDING the calling replica, and on the receiving
// side hands them back via Engine.DeliverSignal(ctx, op, payload) - it is a pure pipe that never
// inspects either. The engine always applies a signal's effect locally before calling SignalPeers, so an
// implementation that echoes the signal back to the sender would cause it to be processed twice on the
// originating replica (a doubled enqueue, valve cut, breaker trip, or status-change wake); if the
// transport delivers published messages to the publisher, the implementation must filter out
// self-delivery. Because the host never branches on op or inspects payload, adding a new engine signal
// kind requires no host change.
type Host interface {
	// LoadGraph fetches a workflow graph definition by its URL (the addressable resolve key passed to
	// Create). The flow's opaque baggage rides on ctx; read it with workflow.BaggageFrom(ctx) if loading
	// is identity-dependent (authz, per-actor graphs).
	LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error)

	// ExecuteTask executes a single task within a workflow. taskURL is the task's dispatch URL (the real
	// downstream address), not the graph node name. The flow carrier has its state pre-populated; the
	// executor should call the task and let it write changes to the flow. The flow's opaque baggage rides
	// on ctx - read it with workflow.BaggageFrom(ctx) (e.g. to mint a token).
	ExecuteTask(ctx context.Context, taskURL string, flow *workflow.Flow) error

	// FlowStopped is fired when a flow stops (completed, failed, cancelled, interrupted), but only for a
	// flow created with FlowOptions.NotifyOnStop=true. The flow's opaque baggage rides on ctx (read it
	// with workflow.BaggageFrom(ctx)); the host decides where/how to deliver the notification from it -
	// the engine traffics in no delivery address. Optional: a host with no notification need does nothing.
	FlowStopped(ctx context.Context, outcome *workflow.FlowOutcome)

	// SignalPeers delivers a cross-replica coordination signal to the other replicas. op is an opaque
	// routing key (usable as a topic); payload is opaque bytes the engine already serialized. The host
	// ships (op, payload) to peers and on the receiving side calls Engine.DeliverSignal(ctx, op,
	// payload). See the cross-replica signal contract above. A single-replica host does nothing here.
	SignalPeers(ctx context.Context, op string, payload []byte)
}

// noopHost is a Host whose methods all do nothing (LoadGraph/ExecuteTask return nil). Used by tests that
// only exercise schema/lifecycle, not dispatch.
type noopHost struct{}

func (noopHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) { return nil, nil }
func (noopHost) ExecuteTask(ctx context.Context, name string, flow *workflow.Flow) error {
	return nil
}
func (noopHost) FlowStopped(context.Context, *workflow.FlowOutcome) {}
func (noopHost) SignalPeers(context.Context, string, []byte)        {}
