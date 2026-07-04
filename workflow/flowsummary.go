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

// FlowSummary is a summary of a flow for listing purposes.
type FlowSummary struct {
	FlowKey      string    `json:"flowKey,omitzero"`
	ThreadKey    string    `json:"threadKey,omitzero"`
	WorkflowURL  string    `json:"workflowURL,omitzero"`
	WorkflowName string    `json:"workflowName,omitzero"`
	Status       string    `json:"status,omitzero"`
	TaskName     string    `json:"taskName,omitzero"`
	Error        string    `json:"error,omitzero"`
	CancelReason string    `json:"cancelReason,omitzero"`
	CreatedAt    time.Time `json:"createdAt,omitzero"`
	// Use StartedAt for duration metrics; CreatedAt for when the flow first appeared. StartedAt is
	// when this attempt began dispatching, distinct from CreatedAt, when the row was first created.
	StartedAt time.Time `json:"startedAt,omitzero"`
	UpdatedAt time.Time `json:"updatedAt,omitzero"`
	// Priority is the flow's scheduling priority (>= 1, lower runs first), resolved at Create.
	Priority int `json:"priority,omitzero"`
	// FairnessKey is the flow's scheduling fairness bucket, resolved at Create.
	FairnessKey string `json:"fairnessKey,omitzero"`
	// TraceID is the flow's distributed-trace id (the 32-hex trace-id parsed from its stored W3C
	// traceparent), or empty when no tracer was configured at Create. Surfaced for correlating a listed
	// flow with its trace backend; it is a token-free correlation value, not a capability.
	TraceID string `json:"traceID,omitzero"`
	// Subgraph is true when this flow is a subgraph child (it has a parent caller step), false for a
	// top-level/root flow. A list returns roots only unless Query.Subgraph opts subgraph children in.
	Subgraph bool `json:"subgraph,omitzero"`
}

// Duration is the wall-clock time from StartedAt to UpdatedAt.
func (f FlowSummary) Duration() time.Duration {
	if f.StartedAt.IsZero() || f.UpdatedAt.IsZero() {
		return 0
	}
	d := f.UpdatedAt.Sub(f.StartedAt)
	if d < 0 {
		return 0
	}
	return d
}
