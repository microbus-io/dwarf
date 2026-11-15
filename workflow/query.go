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

// Query specifies the filters, the time window and the limit for listing or purging flows.
//
// The window is FromTime, inclusive, to ToTime, exclusive. ListClosed and Purge need both; for ListOpen and
// ListRecentlyClosed each is optional. Times compare to the millisecond.
// ListOpen's and ListRecentlyClosed's window is on when a flow last changed status (FlowSummary.UpdatedAt);
// ListClosed's and Purge's on when a flow was archived (FlowSummary.ArchivedAt), and may reach at most 31 days.
// A text filter must be valid UTF-8 without NUL characters.
type Query struct {
	Status      string `json:"status,omitzero"`
	WorkflowURL string `json:"workflowURL,omitzero"`
	// WorkflowName filters to flows whose graph display name (the human-friendly name set via
	// NewGraph) equals this value. Distinct from WorkflowURL, which matches the resolve key. Empty
	// disables the filter; composes with WorkflowURL.
	WorkflowName string `json:"workflowName,omitzero"`
	// StoppedTaskName filters to flows that stopped at the named task: the step that completed, failed or
	// cancelled the flow, or the step it is interrupted at (which a Terminate of an interrupted flow keeps). It
	// matches no running flow.
	StoppedTaskName string `json:"stoppedTaskName,omitzero"`
	// FairnessKey filters to flows with this scheduling fairness key. The host typically sets the
	// fairness key to the tenant, so this is how "list flows for tenant X" is expressed. Empty
	// disables the filter.
	FairnessKey string `json:"fairnessKey,omitzero"`
	// Priority filters to flows at this scheduling priority band. Zero disables the filter
	// (valid priorities are >= 1).
	Priority int `json:"priority,omitzero"`
	// FromTime is the window's lower bound, inclusive: ListOpen and ListRecentlyClosed list flows last updated at or
	// after it, ListClosed flows archived at or after it. Zero disables it.
	FromTime time.Time `json:"fromTime,omitzero"`
	// ToTime is the window's upper bound, exclusive: ListOpen and ListRecentlyClosed list flows last updated before it,
	// ListClosed flows archived before it. Zero disables it.
	ToTime time.Time `json:"toTime,omitzero"`
	// IncludeSubgraphs adds subgraph-child flows to the results alongside roots. They are excluded by default -
	// a subgraph child is an internal execution detail most callers never want in a list. Pair it with
	// WorkflowURL (a graph that runs only as a subgraph has no root flows under that URL) to locate every run
	// of a graph that executed as a subgraph; FlowSummary.Subgraph marks which kind each returned flow is.
	// Purge rejects this flag with a 400 (a subgraph child is purged only as part of its root's subtree,
	// never directly - doing so would strand its parent); Purge always targets roots only.
	IncludeSubgraphs bool `json:"includeSubgraphs,omitzero"`
	// Shard restricts the query to the shard with this index, as declared in the engine's shard spec.
	// Zero queries all shards.
	Shard int `json:"shard,omitzero"`
	// Search is a case-insensitive substring matched against the flow's WorkflowURL, WorkflowName,
	// StoppedTaskName (so a running flow never matches on a task), Error, TerminateReason, CancelReason, and
	// the flow key. Any '%' or '_' in the value is matched literally, not as a wildcard, so "a_b" matches only
	// "a_b" (not "axb") and "50%" matches only a literal "50%".
	Search string `json:"search,omitzero"`
	// Limit is the most flows a list returns, and the most roots Purge removes: required, at most
	// 65,536. The result is the first Limit in the call's order across every shard queried. A search over many days
	// wants a low Limit; a walk through a time range wants short windows.
	Limit int `json:"limit,omitzero"`
}
