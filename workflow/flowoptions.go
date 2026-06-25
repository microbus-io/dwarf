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

// FlowOptions sets flow-level properties at Create or Run: scheduling (priority, fairness, start
// time) plus the opaque host Baggage. A nil *FlowOptions, or any zero field, uses the engine's
// defaults.
type FlowOptions struct {
	// Priority orders flows competing for workers; an explicit priority is >= 1,
	// lower runs first. Zero means "unset" and uses the engine's
	// DefaultPriority config.
	Priority int `json:"priority,omitzero"`
	// FairnessKey groups flows for fair scheduling, typically a tenant.
	// Empty derives it from baggage, else the "" bucket.
	FairnessKey string `json:"fairnessKey,omitzero"`
	// FairnessWeight is the relative dispatch share of the fairness key.
	// Zero uses a weight of 1.
	FairnessWeight float64 `json:"fairnessWeight,omitzero"`
	// StartAt delays execution of the flow's entry step until the given UTC time.
	// Zero or a past time means run as soon as the flow is started. The flow can
	// still be created and started immediately, but no worker picks the entry
	// step up before StartAt.
	StartAt time.Time `json:"startAt,omitzero"`
	// TimeBudget overrides the engine's default per-task time budget for this flow, bounding every
	// ExecuteTask call's context deadline. Subgraph descendants inherit it. Zero uses the engine's
	// SetTimeBudget default. Frozen at Create and immutable for the flow's life; a per-task default, not a
	// flow-wide deadline.
	TimeBudget time.Duration `json:"timeBudget,omitzero"`
	// NotifyOnStop requests that the host's FlowStopped callback fire when this flow stops
	// (completed/failed/cancelled/interrupted). The engine persists the intent and, at stop time,
	// invokes FlowStopped with the flow's Baggage on the context - the host decides where/how to deliver
	// the notification from that baggage. When false (the default) FlowStopped is never called for the
	// flow. Set at Create; plain Start then runs the flow.
	NotifyOnStop bool `json:"notifyOnStop,omitzero"`
	// DeleteOnCompletion deletes the flow and its subgraph descendants as soon as it completes
	// successfully. Failed, cancelled, and interrupted flows are not deleted (they stay available for
	// Restart/Recover/Resume). Honored on the root flow only. Await and Run reject a flow created with this
	// set (its outcome is deleted, not observable); use NotifyOnStop to receive the result.
	DeleteOnCompletion bool `json:"deleteOnCompletion,omitzero"`
	// Baggage is opaque, host-defined context (identity/claims, tenant, locale, ...) carried with the
	// flow. The engine never interprets it: it is set once here, stored on the flow, inherited by
	// subgraphs and Continue, and delivered to every Host LoadGraph/ExecuteTask call via the dispatch
	// context - read it with BaggageFrom(ctx). Any JSON-marshalable value; the host receives the
	// JSON-decoded form (typically map[string]any), exactly like flow state.
	Baggage any `json:"baggage,omitzero"`
}
