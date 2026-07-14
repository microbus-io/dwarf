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

// FlowOptions sets a flow's policy at genesis - Create or Run only. Derived operations (Continue, Fork)
// do not take FlowOptions; they inherit policy from their source, so the operation is the
// inherit-vs-default selector. A nil *FlowOptions, or any zero field, uses the engine's defaults.
type FlowOptions struct {
	// Priority orders flows competing for workers; an explicit priority is >= 1,
	// lower runs first. Zero means "unset" and uses the engine's
	// DefaultPriority config.
	Priority int `json:"priority,omitzero"`
	// FairnessKey groups flows for fair scheduling, typically a tenant.
	// Empty uses the "" bucket.
	FairnessKey string `json:"fairnessKey,omitzero"`
	// FairnessWeight is the relative dispatch share of the fairness key.
	// Zero uses a weight of 1.
	FairnessWeight float64 `json:"fairnessWeight,omitzero"`
	// TimeBudget overrides the engine's default per-task time budget for this flow, bounding every
	// ExecuteTask call's context deadline. Subgraph descendants inherit it. Zero uses the engine's
	// SetTimeBudget default. Frozen at Create and immutable for the flow's life; a per-task default, not a
	// flow-wide deadline.
	//
	// Create rejects a negative value, or a positive one below 1ms - the budget is persisted in
	// milliseconds, so a sub-millisecond value would truncate to zero and dispatch every task with a
	// deadline that has already passed.
	TimeBudget time.Duration `json:"timeBudget,omitzero"`
	// DeleteOnCompletion marks the flow (and its subgraph subtree) for deletion once it completes
	// successfully - for fire-and-forget jobs whose output is not retained. Failed and cancelled flows are
	// kept. Deletion is deferred: the flow lingers for a short grace window during which its outcome stays
	// observable (Await/Snapshot return the completed FlowOutcome), then a background reaper removes it and
	// reads return "flow not found". During the window the flow is excluded from List and History 404s.
	DeleteOnCompletion bool `json:"deleteOnCompletion,omitzero"`
	// Baggage is opaque, host-defined context (identity/claims, tenant, locale, ...) carried with the
	// flow. The engine never interprets it: it is set once here, stored on the flow, inherited by
	// subgraphs and Continue, and delivered to every Host LoadGraph/ExecuteTask call via the dispatch
	// context - read it with BaggageFrom(ctx). Any JSON-marshalable value; the host receives the
	// JSON-decoded form (typically map[string]any), exactly like flow state.
	Baggage any `json:"baggage,omitzero"`
	// ThreadKey places the new flow into an existing thread (multi-turn conversation) instead of starting
	// its own. It is any FlowKey in that thread; the engine reads its thread id and routes the new flow to
	// the thread's shard. Empty starts a fresh thread. This is the explicit-policy way to add a turn to a
	// thread (Continue is the inherit-everything convenience); the host can mix the two.
	ThreadKey string `json:"threadKey,omitzero"`
}
