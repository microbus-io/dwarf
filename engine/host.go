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
	"time"

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
// Cross-replica signal contract (Enqueue, SyncValve, TripBreaker, NotifyStatusChange): all four are
// fire-and-forget and must be delivered to OTHER replicas only, EXCLUDING the calling replica. The
// engine always applies a signal's effect locally before invoking the corresponding method (e.g.
// startNotify rings the local doorbell via handleEnqueue and then calls Enqueue; valveRegulate mutates
// the local valve and then calls SyncValve). An implementation that echoes the signal back to the sender
// would cause it to be processed twice on the originating replica — a doubled enqueue, valve cut, breaker
// trip, or status-change wake. If the underlying transport delivers published messages to the publisher,
// the implementation is responsible for filtering out self-delivery.
type Host interface {
	// LoadGraph fetches a workflow graph definition by name. The flow's opaque baggage rides on ctx;
	// read it with workflow.BaggageFrom(ctx) if loading is identity-dependent (authz, per-actor graphs).
	LoadGraph(ctx context.Context, workflowName string) (*workflow.Graph, error)

	// ExecuteTask executes a single task within a workflow. The flow carrier has its state
	// pre-populated; the executor should call the task and let it write changes to the flow. The flow's
	// opaque baggage rides on ctx - read it with workflow.BaggageFrom(ctx) (e.g. to mint a token).
	ExecuteTask(ctx context.Context, taskName string, flow *workflow.Flow) error

	// FlowStopped is fired when a flow stops (completed, failed, cancelled, interrupted). The hostname
	// is the notify_hostname stored on the flow via StartNotify. Optional: a host with no notification
	// need does nothing here.
	FlowStopped(ctx context.Context, hostname string, outcome *workflow.FlowOutcome)

	// Enqueue rings the work doorbell on peer replicas (a step is pending). See the cross-replica
	// signal contract above.
	Enqueue(ctx context.Context, shard, stepID int)

	// SyncValve gossips an adaptive per-task dispatch-rate cut to peer replicas. See the contract above.
	SyncValve(ctx context.Context, taskName string, wCong int, tCong time.Time)

	// TripBreaker gossips a per-task circuit-breaker trip to peer replicas. See the contract above.
	TripBreaker(ctx context.Context, taskName string)

	// NotifyStatusChange tells peer replicas a flow reached a stopped status so their Await callers wake
	// and re-check. Without it, an Await on the replica that did not run the flow's final step blocks
	// until its context deadline. See the contract above.
	NotifyStatusChange(ctx context.Context, flowKey string, status string)
}

// noopHost is a Host whose methods all do nothing (LoadGraph/ExecuteTask return nil). Used by tests that
// only exercise schema/lifecycle, not dispatch.
type noopHost struct{}

func (noopHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) { return nil, nil }
func (noopHost) ExecuteTask(ctx context.Context, name string, flow *workflow.Flow) error {
	return nil
}
func (noopHost) FlowStopped(context.Context, string, *workflow.FlowOutcome) {}
func (noopHost) Enqueue(context.Context, int, int)                          {}
func (noopHost) SyncValve(context.Context, string, int, time.Time)          {}
func (noopHost) TripBreaker(context.Context, string)                        {}
func (noopHost) NotifyStatusChange(context.Context, string, string)         {}
