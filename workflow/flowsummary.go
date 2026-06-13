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
	WorkflowName string    `json:"workflowName,omitzero"`
	Status       string    `json:"status,omitzero"`
	TaskName     string    `json:"taskName,omitzero"`
	Error        string    `json:"error,omitzero"`
	CancelReason string    `json:"cancelReason,omitzero"`
	CreatedAt    time.Time `json:"createdAt,omitzero"`
	// StartedAt is when this attempt began dispatching (Start, or a Restart/RestartFrom
	// rewind). Distinct from CreatedAt, which is the row's INSERT moment and is only
	// reset on full Restart. Use StartedAt for duration metrics; CreatedAt for "when did
	// this flow first appear."
	StartedAt time.Time `json:"startedAt,omitzero"`
	UpdatedAt time.Time `json:"updatedAt,omitzero"`
	// Priority is the flow's scheduling priority (>= 1, lower runs first), resolved at Create.
	Priority int `json:"priority,omitzero"`
	// FairnessKey is the flow's scheduling fairness bucket, resolved at Create.
	FairnessKey string `json:"fairnessKey,omitzero"`
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
