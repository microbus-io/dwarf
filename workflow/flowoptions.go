/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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

// FlowOptions sets flow-level scheduling properties at Create or Run.
// A nil *FlowOptions, or any zero field, uses the engine's defaults.
type FlowOptions struct {
	// Priority orders flows competing for workers; an explicit priority is >= 1,
	// lower runs first. Zero means "unset" and uses the engine's
	// DefaultPriority config.
	Priority int `json:"priority,omitzero"`
	// FairnessKey groups flows for fair scheduling, typically a tenant.
	// Empty derives it from metadata, else the "" bucket.
	FairnessKey string `json:"fairnessKey,omitzero"`
	// FairnessWeight is the relative dispatch share of the fairness key.
	// Zero uses a weight of 1.
	FairnessWeight float64 `json:"fairnessWeight,omitzero"`
	// StartAt delays execution of the flow's entry step until the given UTC time.
	// Zero or a past time means run as soon as the flow is started. Sets the
	// entry step's not_before column; the flow can still be created and started
	// immediately, but no worker will pick the step up before StartAt.
	StartAt time.Time `json:"startAt,omitzero"`
}
