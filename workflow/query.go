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

// Query specifies filtering and pagination options for listing or purging flows.
type Query struct {
	Status      string `json:"status,omitzero"`
	WorkflowURL string `json:"workflowURL,omitzero"`
	// WorkflowName filters to flows whose graph display name (the human-friendly name set via
	// NewGraph and denormalized onto the flow row) equals this value. Distinct from WorkflowURL,
	// which matches the resolve key. Empty disables the filter; composes with WorkflowURL.
	WorkflowName string `json:"workflowName,omitzero"`
	ThreadKey    string `json:"threadKey,omitzero"`
	// TaskName filters to flows whose current step is on the named task.
	TaskName string `json:"taskName,omitzero"`
	// FairnessKey filters to flows with this scheduling fairness key. The host typically sets the
	// fairness key to the tenant, so this is how "list flows for tenant X" is expressed. Empty
	// disables the filter.
	FairnessKey string `json:"fairnessKey,omitzero"`
	// Priority filters to flows at this scheduling priority band. Zero disables the filter
	// (valid priorities are >= 1).
	Priority int `json:"priority,omitzero"`
	// OlderThan filters to flows whose updated_at is older than this duration relative to now.
	// Zero disables the filter.
	OlderThan time.Duration `json:"olderThan,omitzero"`
	// NewerThan filters to flows whose updated_at is within this duration of now.
	// Zero disables the filter. Composes with OlderThan to express "between X and Y ago."
	NewerThan time.Duration `json:"newerThan,omitzero"`
	// IncludeSubgraphs adds subgraph-child flows to the results alongside roots. They are excluded by default -
	// a subgraph child is an internal execution detail most callers never want in a list. Pair it with
	// WorkflowURL (a graph that runs only as a subgraph has no root flows under that URL) to locate every run
	// of a graph that executed as a subgraph; FlowSummary.Subgraph marks which kind each returned flow is.
	// Purge ignores this flag and always targets roots only (deleting a subgraph child directly would strand
	// its parent).
	IncludeSubgraphs bool `json:"includeSubgraphs,omitzero"`
	// Shard restricts the query to a single 1-based shard. Zero queries all shards.
	Shard int `json:"shard,omitzero"`
	// Cursor is the opaque pagination cursor returned as NextCursor by the previous List call.
	Cursor string `json:"cursor,omitzero"`
	// Search is a case-insensitive substring matched against workflow_url, workflow_name, current
	// task_name, error, and cancel_reason. SQL LIKE wildcards (%, _) in the value are honored.
	Search string `json:"search,omitzero"`
	Limit  int    `json:"limit,omitzero"`
}
