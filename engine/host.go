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
// A host supplies only the graphs and the task dispatch below. Replicas sharing a database coordinate by
// reading it: pending work, flow outcomes and fleet membership are all discovered by polling, on cadences the
// engine sets.
type Host interface {
	// LoadGraph fetches a workflow graph definition by its URL (the addressable resolve key passed to
	// Create). The flow's opaque baggage rides on ctx; read it with workflow.BaggageFrom(ctx) if loading
	// is identity-dependent (authz, per-actor graphs). The engine validates the returned graph
	// (graph.Validate) at Create and at subgraph spawn, so the host need not: returning (nil, nil) yields a
	// 404 and a structurally invalid graph a 400, rather than a later dispatch-time failure.
	//
	// Return (nil, nil) for a graph that does not exist, and an error with status 503 when the graph's source
	// cannot be reached right now. At Create the error is returned to the caller; at a subgraph spawn it is
	// returned to the calling task from flow.Subgraph, status code included, so the task can retry a 503 and
	// give up on a 404. The engine itself reads no status code.
	LoadGraph(ctx context.Context, workflowURL string) (graph *workflow.Graph, err error)

	// ExecuteTask executes a single task within a workflow. taskURL is the task's dispatch URL (the real
	// downstream address), not the task's name in the graph. The flow carrier has its state pre-populated; the
	// executor should call the task and let it write changes to the flow. The flow's opaque baggage rides
	// on ctx - read it with workflow.BaggageFrom(ctx) (e.g. to mint a token).
	//
	// Execution is at-least-once and may be concurrent: a task can run more than once, and if a worker's
	// lease is lost while it is still running (a task that overruns its ctx deadline, or a forward DB-clock
	// step past the lease) a second worker re-runs it in parallel. The engine guarantees the flow's
	// persisted state reflects exactly one execution, but exactly-once side effects are the task's
	// responsibility - tasks must be idempotent. Honor the ctx deadline (it bounds the step's time budget);
	// a task that ignores it can only be recovered by lease expiry, not cancelled.
	//
	// A host that cannot reach the task's worker may wait for it with flow.Defer, up to a horizon of its own:
	//
	//	if yield, err := flow.Defer(time.Second, 2, 30*time.Second, horizon); yield || err != nil {
	//		return err // nil while it waits; the flow's cancellation, which the host must return as it is
	//	}
	//	return errors.New("no worker for %s", taskURL, http.StatusServiceUnavailable)
	//
	// The step comes back as it stands - its resume data, its subgraph's result, its attempt - and the wait counts
	// toward flow.Deferrals. A host that drops Defer's error and returns its own fails a cancelled flow's step
	// instead of cancelling it.
	//
	// After ExecuteTask returns, the engine reads only the flow's changes and its control signals. A host that
	// reaches its task across a process boundary carries the flow as JSON - workflow.RequestCarrier going out,
	// workflow.ResultCarrier coming back - and the reply need not carry the state or the reducers; whatever
	// else it carries is not read.
	ExecuteTask(ctx context.Context, taskURL string, flow *workflow.Flow) error
}

// noopHost is a Host whose methods all do nothing (LoadGraph/ExecuteTask return nil). Used by tests that
// only exercise schema/lifecycle, not dispatch.
type noopHost struct{}

func (noopHost) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	return nil, nil
}
func (noopHost) ExecuteTask(ctx context.Context, taskURL string, flow *workflow.Flow) error {
	return nil
}
