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

// FlowStep is a single step in a flow's execution history.
type FlowStep struct {
	StepKey   string `json:"stepKey,omitzero"`
	StepID    int    `json:"stepID,omitzero"`
	StepDepth int    `json:"stepDepth,omitzero"`
	TaskName  string `json:"taskName,omitzero"`
	Attempt   int    `json:"attempt,omitzero"`
	// PredecessorID and SuccessorID are the shard-local step ids of this step's neighbors in
	// the execution DAG. 0 means no such edge (entry / exit step).
	PredecessorID int `json:"predecessorID,omitzero"`
	SuccessorID   int `json:"successorID,omitzero"`
	// PrevKey and NextKey are the external step keys of the resolved navigation neighbors,
	// ready for use as ?step= links. Populated only by the Step endpoint.
	PrevKey          string         `json:"prevKey,omitzero"`
	NextKey          string         `json:"nextKey,omitzero"`
	Subgraph         bool           `json:"subgraph,omitzero"`
	SubWorkflowName  string         `json:"subWorkflowName,omitzero"`
	SubHistory       []FlowStep     `json:"subHistory,omitzero"`
	State            map[string]any `json:"state,omitzero"`
	Changes          map[string]any `json:"changes,omitzero"`
	InterruptPayload map[string]any `json:"interruptPayload,omitzero"`
	Status           string         `json:"status,omitzero"`
	Error            string         `json:"error,omitzero"`
	CreatedAt        time.Time      `json:"createdAt,omitzero"`
	// StartedAt is when the worker first dispatched the current attempt of this step.
	// Use HasStarted to gate reads; on a not-yet-leased row it carries the INSERT-time default.
	StartedAt time.Time `json:"startedAt,omitzero"`
	UpdatedAt time.Time `json:"updatedAt,omitzero"`
}

// HasStarted reports whether StartedAt is a real dispatch timestamp rather than the INSERT-time
// default. True for running and any terminal status; false for created/pending.
func (s FlowStep) HasStarted() bool {
	switch s.Status {
	case StatusRunning, StatusCompleted, StatusFailed, StatusCancelled, StatusInterrupted:
		return true
	}
	return false
}

// Duration is the wall-clock time from CreatedAt to UpdatedAt. Returns zero when either
// timestamp is missing or the delta is negative.
func (s FlowStep) Duration() time.Duration {
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
		return 0
	}
	d := s.UpdatedAt.Sub(s.CreatedAt)
	if d < 0 {
		return 0
	}
	return d
}
