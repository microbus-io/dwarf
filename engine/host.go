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

// Host is the contract between the dwarf engine and the surrounding host application. The engine owns no
// transport of its own; it reaches workflow graphs and tasks exclusively through the host. Register it
// once via Engine.SetHost.
//
// THE ENGINE SENDS NOTHING TO ITS PEERS, and a host therefore wires no inter-replica transport for it.
// Replicas sharing a database coordinate entirely by reading it: pending work, flow outcomes and fleet
// membership are all discovered by polling, on cadences the engine sets, so a fleet converges with no
// message passing between replicas at any volume - not per step, not per flow, not per deployment event.
// What a host must supply is exactly what only it can: the graphs and the task dispatch below.
type Host interface {
	// LoadGraph fetches a workflow graph definition by its URL (the addressable resolve key passed to
	// Create). The flow's opaque baggage rides on ctx; read it with workflow.BaggageFrom(ctx) if loading
	// is identity-dependent (authz, per-actor graphs). The engine validates the returned graph
	// (graph.Validate) at Create and at subgraph spawn, so the host need not: returning (nil, nil) yields a
	// 404 and a structurally invalid graph a 400, rather than a later dispatch-time failure.
	LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error)

	// ExecuteTask executes a single task within a workflow. taskURL is the task's dispatch URL (the real
	// downstream address), not the graph node name. The flow carrier has its state pre-populated; the
	// executor should call the task and let it write changes to the flow. The flow's opaque baggage rides
	// on ctx - read it with workflow.BaggageFrom(ctx) (e.g. to mint a token).
	//
	// Execution is at-least-once and may be concurrent: a task can run more than once, and if a worker's
	// lease is lost while it is still running (a task that overruns its ctx deadline, or a forward DB-clock
	// step past the lease) a second worker re-runs it in parallel. The engine guarantees the flow's
	// persisted state reflects exactly one execution, but exactly-once side effects are the task's
	// responsibility - tasks must be idempotent. Honor the ctx deadline (it bounds the step's time budget);
	// a task that ignores it can only be recovered by lease expiry, not cancelled.
	ExecuteTask(ctx context.Context, taskURL string, flow *workflow.Flow) error
}

// noopHost is a Host whose methods all do nothing (LoadGraph/ExecuteTask return nil). Used by tests that
// only exercise schema/lifecycle, not dispatch.
type noopHost struct{}

func (noopHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) { return nil, nil }
func (noopHost) ExecuteTask(ctx context.Context, name string, flow *workflow.Flow) error {
	return nil
}
