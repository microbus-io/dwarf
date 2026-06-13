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
	Status       string `json:"status,omitzero"`
	WorkflowName string `json:"workflowName,omitzero"`
	ThreadKey    string `json:"threadKey,omitzero"`
	// TaskName filters to flows whose current step is on the named task.
	TaskName string `json:"taskName,omitzero"`
	// TenantID filters to flows belonging to a specific tenant.
	// Zero disables the filter.
	TenantID int `json:"tenantID,omitzero"`
	// OlderThan filters to flows whose updated_at is older than this duration relative to now.
	// Zero disables the filter.
	OlderThan time.Duration `json:"olderThan,omitzero"`
	// NewerThan filters to flows whose updated_at is within this duration of now.
	// Zero disables the filter. Composes with OlderThan to express "between X and Y ago."
	NewerThan time.Duration `json:"newerThan,omitzero"`
	// Shard restricts the query to a single 1-based shard. Zero queries all shards.
	Shard int `json:"shard,omitzero"`
	// Cursor is the opaque pagination cursor returned as NextCursor by the previous List call.
	Cursor string `json:"cursor,omitzero"`
	// Search is a case-insensitive substring matched against workflow_name, current task_name,
	// error, and cancel_reason. SQL LIKE wildcards (%, _) in the value are honored.
	Search string `json:"search,omitzero"`
	Limit  int    `json:"limit,omitzero"`
}
