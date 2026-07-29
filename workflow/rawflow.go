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
	"bytes"
	"encoding/json"
	"time"

	"github.com/microbus-io/errors"
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

// RawState returns a copy of the raw state.
func (f *RawFlow) RawState() State {
	return f.state.Clone()
}

// RawChanges returns a copy of the raw changes.
func (f *RawFlow) RawChanges() State {
	return f.changes.Clone()
}

// SetRawState replaces the entire state with a copy of the given state, without tracking changes.
func (f *RawFlow) SetRawState(state State) {
	f.state = state.Clone()
}

// SetRawChanges replaces the entire changes with a copy of the given changes.
func (f *RawFlow) SetRawChanges(changes State) {
	f.changes = changes.Clone()
}

// SetAttempt sets the attempt counter on the flow. Called by the orchestrator before dispatching
// a task so that Retry can check whether attempts are exhausted.
func (f *RawFlow) SetAttempt(attempt int) {
	f.attempt = attempt
}

// SetCreatedAt records the flow row's createdAt. Called by the orchestrator before dispatching a
// task so the task can read it via Flow.CreatedAt().
func (f *RawFlow) SetCreatedAt(createdAt time.Time) {
	f.createdAt = createdAt
}

// SetUpdatedAt records the flow row's updatedAt. Called by the orchestrator before dispatching a
// task so the task can read it via Flow.UpdatedAt().
func (f *RawFlow) SetUpdatedAt(updatedAt time.Time) {
	f.updatedAt = updatedAt
}

// SetStepCreatedAt records the step row's createdAt, preserved across retries. Called by the
// orchestrator before dispatching a task so Retry can measure its giveUpAfter horizon and the task
// can read it via Flow.StepCreatedAt().
func (f *RawFlow) SetStepCreatedAt(stepCreatedAt time.Time) {
	f.stepCreatedAt = stepCreatedAt
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
func (f *RawFlow) SetInterruptResolution(resumeData State) {
	f.interruptDone = true
	f.resumeData = resumeData
}

// SetSubgraphResolution records that a subgraph park has resolved, with the child's final_state
// (result) and error materialized from the step row's subgraph_result / subgraph_error columns, so
// flow.Subgraph returns them (with yield=false) on re-entry instead of re-arming. The orchestrator
// calls this only when the step row's subgraph_done is set.
func (f *RawFlow) SetSubgraphResolution(result State, errStr string) {
	f.subgraphDone = true
	f.subgraphResult = result
	f.subgraphError = errStr
}

// StateFromCanonicalJSON builds a State whose fields are held as RAW JSON, decoded only if and when
// something reads them. It is the engine's constructor for a payload column it wrote itself, and it is what
// makes carrying a large field across a task cost its byte length rather than the several times that a
// decoded Go value occupies.
//
// The bytes must be CANONICAL - the output of marshalling decoded values, which every payload column the
// engine writes is, at every level of nesting. This is a warranty the caller gives, not something that can
// be checked cheaply, and it is the whole reason this is separate from NewState: NewState decodes precisely
// so that caller-supplied JSON is canonicalized, because reducers compare marshalled bytes and Go marshals
// a map with sorted keys but a struct in declaration order. Feed this anything that did not come from the
// engine's own storage and two spellings of one value can coexist, which a union reducer will faithfully
// keep both of.
//
// An empty or "null" payload yields an empty State, matching NewState. A non-object is an error.
func StateFromCanonicalJSON(data []byte) (State, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return State{d: map[string]any{}}, nil
	}
	if trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return State{}, errors.New("workflow: state must be a JSON object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return State{}, errors.Trace(err)
	}
	s := State{d: make(map[string]any, len(fields))}
	for k, v := range fields {
		s.d[k] = v
	}
	return s, nil
}
