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

package workflow

import "time"

// FlowSummary is a flow's metadata, as the lists and Engine.Summary return it.
type FlowSummary struct {
	// FlowKey is the flow's key, which the operations on one flow take.
	FlowKey string `json:"flowKey,omitzero"`
	// WorkflowURL is the URL the flow's graph was loaded from, as Create was given it.
	WorkflowURL string `json:"workflowURL,omitzero"`
	// WorkflowName is the graph's name, as workflow.NewGraph was given it.
	WorkflowName string `json:"workflowName,omitzero"`
	// Status is the flow's status: running or interrupted while it is open; completed, failed, terminated or cancelled
	// once it has closed.
	Status string `json:"status,omitzero"`
	// StoppedTaskName is the task the flow stopped at: the step that completed, failed or cancelled it, or the
	// step it is interrupted at, which a Terminate of an interrupted flow keeps. It is empty while the flow is
	// running, and for a stop with no single step.
	StoppedTaskName string `json:"stoppedTaskName,omitzero"`
	// Error is the task error string when Status is "failed". A message over 16 KB is cut to that size and ends
	// with " ... [truncated]".
	Error string `json:"error,omitzero"`
	// TerminateReason is the reason passed to Terminate, or the one the engine gives when a flow's expiration
	// terminates it. Populated when Status is "terminated".
	TerminateReason string `json:"terminateReason,omitzero"`
	// CancelReason is the reason string passed to Cancel(flowKey, reason). Populated when Status is "cancelled".
	CancelReason string `json:"cancelReason,omitzero"`
	// CreatedAt is when the flow was created, by the database's clock.
	CreatedAt time.Time `json:"createdAt,omitzero"`
	// StartedAt is when the flow began running: the same as CreatedAt for a flow Create, Continue or a subgraph made,
	// and for a Fork's new flow when its copy of the original was complete. Measure a flow's duration from it.
	StartedAt time.Time `json:"startedAt,omitzero"`
	// UpdatedAt is when the flow last changed status - for a closed flow, when it closed. ListOpen and
	// ListRecentlyClosed window and order on it.
	UpdatedAt time.Time `json:"updatedAt,omitzero"`
	// ArchivedAt is when the flow moved to the archive, normally about a minute after it closed, by the database's
	// clock; zero for a flow not archived. ListClosed orders by it.
	ArchivedAt time.Time `json:"archivedAt,omitzero"`
	// Priority is the flow's scheduling priority (>= 1, lower runs first), resolved at Create.
	Priority int `json:"priority,omitzero"`
	// FairnessKey is the flow's scheduling fairness bucket, resolved at Create.
	FairnessKey string `json:"fairnessKey,omitzero"`
	// TraceID is the flow's distributed-trace id (the 32-hex trace-id parsed from its stored W3C
	// traceparent), or empty when no tracer was configured at Create. Surfaced for correlating a listed
	// flow with its trace backend; it is a token-free correlation value, not a capability.
	TraceID string `json:"traceID,omitzero"`
	// Subgraph is true when this flow is a subgraph child (it has a parent caller step), false for a
	// top-level/root flow. A list returns roots only unless Query.IncludeSubgraphs opts subgraph children in.
	Subgraph bool `json:"subgraph,omitzero"`
	// ParentStepKey is the key of the step that called this flow as its subgraph, in the parent flow, and empty for a
	// root flow. Pass it to Engine.Step or Engine.Fork.
	ParentStepKey string `json:"parentStepKey,omitzero"`
	// RootFlowKey is the key of the root of this flow's tree: its own key for a root flow. Resume, Cancel, Terminate,
	// Delete and Continue take the root's key and reject a subgraph child's.
	RootFlowKey string `json:"rootFlowKey,omitzero"`
}
