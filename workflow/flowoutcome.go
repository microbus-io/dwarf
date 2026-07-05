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

// FlowOutcome carries the status and side-channel signals of a flow at a moment in time.
// Returned by Snapshot, Await, and Run. Side-channel fields are populated only for the matching
// Status; for example InterruptPayload is populated only when Status is "interrupted".
type FlowOutcome struct {
	// Status is the flow's current lifecycle status: created, running, interrupted, completed, failed, or cancelled.
	Status string `json:"status,omitzero"`
	// State is the flow's accumulated state. For terminal statuses this is the final_state; for an
	// interrupted flow it is the merged snapshot of the interrupted step. For a running flow it is
	// deliberately empty - the live in-flight merged state is not reconstructed.
	State map[string]any `json:"state,omitzero"`
	// Error is the task error string. Populated when Status is "failed".
	Error string `json:"error,omitzero"`
	// InterruptPayload is the raw payload from flow.Interrupt(payload). Populated when Status is "interrupted".
	InterruptPayload map[string]any `json:"interruptPayload,omitzero"`
	// CancelReason is the reason string passed to Cancel(flowKey, reason). Populated when Status is "cancelled".
	CancelReason string `json:"cancelReason,omitzero"`
}

// Stopped reports whether the flow has stopped - reached any status other than not-yet-started (created,
// pending) or actively running. A running outcome returned by Poll (Stopped() == false) means the flow has
// not stopped yet and the caller should poll again.
func (o *FlowOutcome) Stopped() bool {
	if o == nil {
		return false
	}
	switch o.Status {
	case "", StatusCreated, StatusPending, StatusRunning:
		return false
	default:
		return true
	}
}
