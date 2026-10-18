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

// FlowStep is a single step in a flow's execution history.
//
// Engine.History returns every step of a flow without its data: State, Changes and InterruptPayload are
// empty there, as are PrevKey and NextKey. Engine.Step returns one step with its data and navigation keys,
// and leaves StartedAt, EngineID, Parked and the subgraph fields empty.
type FlowStep struct {
	// StepKey identifies the step; pass it to Engine.Step or Engine.Fork.
	StepKey string `json:"stepKey,omitzero"`
	// StepDepth orders the steps along the flow: each step is one deeper than the step it follows, so the
	// parallel branches of a fan-out share a depth, and a subgraph's steps continue from their caller's depth.
	// It is informational; PredecessorKey and SuccessorKey record the execution graph.
	StepDepth int `json:"stepDepth,omitzero"`
	// TaskName is the graph task the step ran.
	TaskName string `json:"taskName,omitzero"`
	// Attempt is the step's zero-based retry counter, as the task reads it with Flow.Attempt.
	Attempt int `json:"attempt,omitzero"`
	// EngineID identifies the engine replica that handled this step: the replica that executed
	// its most recent attempt once dispatched, or created it before it ran. It is a random
	// per-process value that changes when a replica restarts, so it is a correlation and
	// work-distribution signal (which replica ran this step), not a stable address. Zero when unset.
	EngineID int64 `json:"engineID,omitzero"`
	// PredecessorKey and SuccessorKey are the keys of this step's neighbors in its flow's execution
	// graph, empty where none is recorded. Not every edge is recorded on both ends: a fan-out step's
	// SuccessorKey names only its first branch, while every branch names the fan-out step as
	// PredecessorKey; and each step joining a fan-in names the fan-in step as SuccessorKey. A flow's full
	// edge set is the union of the pairs of all its steps.
	PredecessorKey string `json:"predecessorKey,omitzero"`
	SuccessorKey   string `json:"successorKey,omitzero"`
	// PrevKey and NextKey are the step keys of the previous and next steps on a linear walk of the
	// execution timeline. Unlike PredecessorKey and SuccessorKey, the walk passes through subgraphs:
	// it skips the subgraph caller step, entering the child flow at its first step and leaving it to
	// the caller's neighbor in the parent flow. Set only by Engine.Step; empty in History.
	PrevKey string `json:"prevKey,omitzero"`
	NextKey string `json:"nextKey,omitzero"`
	// Subgraph reports that the step called a subgraph. SubgraphKey is the child flow's key, SubgraphURL and
	// SubgraphName are the child workflow's URL and display name, and SubHistory is the child flow's own
	// history, nested the same way. When the step called its subgraph more than once (a Retry), they describe
	// the latest call. Set only by Engine.History; empty from Engine.Step.
	//
	// SubgraphKey is a read handle: pass it to Engine.Snapshot for the child's result, or to Await, Poll,
	// Fingerprint or History. Operations that change a flow (Resume, Terminate, Cancel, Delete, Continue) reject
	// it; they take the root's key.
	Subgraph     bool       `json:"subgraph,omitzero"`
	SubgraphKey  string     `json:"subgraphKey,omitzero"`
	SubgraphURL  string     `json:"subgraphURL,omitzero"`
	SubgraphName string     `json:"subgraphName,omitzero"`
	SubHistory   []FlowStep `json:"subHistory,omitzero"`
	// State is the step's input: the state the task received.
	State State `json:"state,omitzero"`
	// Changes is what the task wrote. A field it overwrote holds its new value, and a field it deleted
	// holds null. On a field with a reducer, a contribution is the task's own addition, not the field's new
	// value. The new value is in what follows the step: the next step's State in a linear chain (a forEach
	// branch's also holds its element), the fan-in's State after a branch's last step, which combines every
	// branch's writes, and the flow's final state after a terminal step.
	Changes State `json:"changes,omitzero"`
	// InterruptPayload is the payload the task passed to Flow.Interrupt, if it interrupted.
	InterruptPayload State `json:"interruptPayload,omitzero"`
	// Status is the step's status: one of the Status constants.
	Status string `json:"status,omitzero"`
	// Parked reports whether the step is currently held out of dispatch (a subgraph caller waiting on
	// its child). A terminal step is never parked.
	Parked bool `json:"parked,omitzero"`
	// Error is the step's error message, empty when it has none.
	Error string `json:"error,omitzero"`
	// CreatedAt is when the step was created.
	CreatedAt time.Time `json:"createdAt,omitzero"`
	// StartedAt is when the worker first dispatched the current attempt of this step.
	// Use HasStarted to gate reads.
	StartedAt time.Time `json:"startedAt,omitzero"`
	// UpdatedAt is when the step was last updated.
	UpdatedAt time.Time `json:"updatedAt,omitzero"`
}

// HasStarted reports whether StartedAt is a real dispatch timestamp rather than a placeholder recorded
// when the step was created. True for running, interrupted and any terminal status; false for
// created/pending.
func (s FlowStep) HasStarted() bool {
	switch s.Status {
	case StatusRunning, StatusCompleted, StatusFailed, StatusTerminated, StatusCancelled, StatusInterrupted:
		return true
	}
	return false
}
