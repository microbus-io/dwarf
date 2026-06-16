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

import (
	"maps"
	"time"
)

// RawFlow wraps Flow with additional methods used by the orchestrator.
// Task endpoints should use Flow directly; RawFlow is for internal orchestration use only.
type RawFlow struct {
	Flow
}

// NewRawFlow creates a new RawFlow with initialized maps.
func NewRawFlow() *RawFlow {
	return &RawFlow{
		Flow: *NewFlow(),
	}
}

// --- Raw state access (for orchestrator use) ---

// RawState returns a copy of the raw state map.
func (f *RawFlow) RawState() map[string]any {
	result := make(map[string]any, len(f.state))
	maps.Copy(result, f.state)
	return result
}

// RawChanges returns a copy of the raw changes map.
func (f *RawFlow) RawChanges() map[string]any {
	result := make(map[string]any, len(f.changes))
	maps.Copy(result, f.changes)
	return result
}

// SetRawState replaces the entire state with the given raw map, without tracking changes.
func (f *RawFlow) SetRawState(state map[string]any) {
	f.state = make(map[string]any, len(state))
	maps.Copy(f.state, state)
}

// SetRawChanges replaces the entire changes map with the given raw map.
func (f *RawFlow) SetRawChanges(changes map[string]any) {
	f.changes = make(map[string]any, len(changes))
	maps.Copy(f.changes, changes)
}

// ClearChanges resets the changes map. Called by the orchestrator after persisting changes.
func (f *RawFlow) ClearChanges() {
	f.changes = make(map[string]any)
}

// ClearControl resets all control signals. Called by the orchestrator after processing them.
func (f *RawFlow) ClearControl() {
	f.gotoNext = ""
	f.retry = false
	f.sleepDuration = 0
	f.interrupt = false
	f.interruptPayload = nil
	f.attempt = 0
	f.backoffMaxAttempts = 0
	f.backoffInitialDelay = 0
	f.backoffDelayMultiplier = 0
	f.backoffMaxDelay = 0
}

// SetAttempt sets the attempt counter on the flow. Called by the orchestrator before dispatching
// a task so that Retry can check whether attempts are exhausted.
func (f *RawFlow) SetAttempt(attempt int) {
	f.attempt = attempt
}

// SetTimestamps records the flow row's createdAt and updatedAt. Called by the orchestrator
// before dispatching a task so the task can read them via Flow.CreatedAt() and Flow.UpdatedAt().
func (f *RawFlow) SetTimestamps(createdAt, updatedAt time.Time) {
	f.createdAt = createdAt
	f.updatedAt = updatedAt
}

// SetFlowKey records the external key of the flow being dispatched, so the task can read it via
// Flow.FlowKey(). Called by the orchestrator before dispatching a task.
func (f *RawFlow) SetFlowKey(flowKey string) {
	f.flowKey = flowKey
}

// SetStepKey records the external key of the step being dispatched, so the task can read it via
// Flow.StepKey(). Called by the orchestrator before dispatching a task.
func (f *RawFlow) SetStepKey(stepKey string) {
	f.stepKey = stepKey
}

// SetInterruptResolution records that an interrupt park has resolved, with the resume data
// materialized from the step row's resume_data column, so flow.Interrupt returns it (with yield=false)
// on re-entry instead of re-arming. The orchestrator calls this only when the step row's interrupt_done
// is set; an un-resumed step leaves the flow's default (not resolved).
func (f *RawFlow) SetInterruptResolution(resumeData map[string]any) {
	f.interruptDone = true
	f.resumeData = resumeData
}

// SetSubgraphResolution records that a subgraph park has resolved, with the child's final_state
// (result) and error materialized from the step row's subgraph_result / subgraph_error columns, so
// flow.Subgraph returns them (with yield=false) on re-entry instead of re-arming. The orchestrator
// calls this only when the step row's subgraph_done is set.
func (f *RawFlow) SetSubgraphResolution(result map[string]any, errStr string) {
	f.subgraphDone = true
	f.subgraphResult = result
	f.subgraphError = errStr
}
