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

// RawState returns a copy of the raw state.
func (f *RawFlow) RawState() State {
	return f.state.Clone()
}

// RawChanges returns a copy of what the task wrote.
func (f *RawFlow) RawChanges() Changes {
	return f.changes.Clone()
}

// SetRawState replaces the entire state with a copy of the given state, without tracking changes.
func (f *RawFlow) SetRawState(state State) {
	f.state = state.Clone()
}

// SetReducers sets the field reducers the task's writes combine through - the graph's.
func (f *RawFlow) SetReducers(reducers map[string]Reducer) {
	f.reducers = reducers
}

// SetInBranch records whether the step runs inside a fan-out branch, where overwriting a reduced field is not
// allowed.
func (f *RawFlow) SetInBranch(inBranch bool) {
	f.inBranch = inBranch
}

// SetRawChanges replaces what the task has written with a copy of the given changes.
func (f *RawFlow) SetRawChanges(changes Changes) {
	f.changes = changes.Clone()
}

// SetAttempt sets the attempt counter on the flow. Called by the orchestrator before dispatching
// a task so that Retry can check whether attempts are exhausted.
func (f *RawFlow) SetAttempt(attempt int) {
	f.attempt = attempt
}

// SetCreatedAt records when the flow was created. Called by the orchestrator before dispatching a
// task so the task can read it via Flow.CreatedAt().
func (f *RawFlow) SetCreatedAt(createdAt time.Time) {
	f.createdAt = createdAt
}

// SetUpdatedAt records when the flow last changed status. Called by the orchestrator before dispatching a
// task so the task can read it via Flow.UpdatedAt().
func (f *RawFlow) SetUpdatedAt(updatedAt time.Time) {
	f.updatedAt = updatedAt
}

// SetStepCreatedAt records when the step was created, preserved across retries. Called by the
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

// SetInterruptResolution records that an interrupt park has resolved, with the resume data the caller
// supplied, so flow.Interrupt returns it (with yield=false) on re-entry instead of re-arming. The
// orchestrator calls this only once the interrupt has actually been resumed; an un-resumed step leaves
// the flow's default (not resolved).
func (f *RawFlow) SetInterruptResolution(resumeData State) {
	f.interruptDone = true
	f.resumeData = resumeData
}

// SetSubgraphResolution records that a subgraph park has resolved, with the child's final state (result)
// and error, so flow.Subgraph returns them (with yield=false) on re-entry instead of re-arming. The
// orchestrator calls this only once the child flow has actually completed.
func (f *RawFlow) SetSubgraphResolution(result State, errStr string) {
	f.subgraphDone = true
	f.subgraphResult = result
	f.subgraphError = errStr
}

// --- Control signal inspection (what the task asked of the orchestrator) ---

// GotoRequested returns the task name set by Goto, or empty if not set.
func (f *RawFlow) GotoRequested() string {
	return f.gotoNext
}

// RetryRequested returns the backoff parameters (initialDelay, multiplier, maxDelay) and true if Retry
// was called.
func (f *RawFlow) RetryRequested() (initialDelay time.Duration, multiplier float64, maxDelay time.Duration, ok bool) {
	if !f.retry {
		return 0, 0, 0, false
	}
	return f.backoffInitialDelay, f.backoffDelayMultiplier, f.backoffMaxDelay, true
}

// RetryDelay returns the backoff before the next attempt of a step that called Retry on the given attempt
// (0-based): min(initialDelay * multiplier^attempt, maxDelay), or zero if Retry was not called. It is the same
// delay Retry's give-up decision assumes.
func (f *RawFlow) RetryDelay(attempt int) time.Duration {
	if !f.retry {
		return 0
	}
	return backoffDelay(f.backoffInitialDelay, f.backoffDelayMultiplier, f.backoffMaxDelay, attempt)
}

// backoffDelay is min(initialDelay * multiplier^attempt, maxDelay); a non-positive multiplier or maxDelay
// disables that term. It clamps in float space and stops multiplying at the cap, so it never overflows int64 ns.
func backoffDelay(initialDelay time.Duration, multiplier float64, maxDelay time.Duration, attempt int) time.Duration {
	delay := float64(initialDelay)
	if multiplier > 0 {
		for range attempt {
			if maxDelay > 0 && delay >= float64(maxDelay) {
				break
			}
			delay *= multiplier
		}
	}
	if maxDelay > 0 && delay > float64(maxDelay) {
		delay = float64(maxDelay)
	}
	return time.Duration(delay)
}

// SleepRequested returns the duration set by Sleep, or zero if none was set. A negative duration - which
// Sleep accepts as a no-op, and which a Flow decoded off the wire from a remote task may carry - reads as zero.
func (f *RawFlow) SleepRequested() time.Duration {
	return max(f.sleepDuration, 0)
}

// InterruptRequested returns the interrupt payload and true if Interrupt was called.
func (f *RawFlow) InterruptRequested() (State, bool) {
	return f.interruptPayload, f.interrupt
}

// SubgraphRequested returns the request URL, input state, and true if Subgraph was called. The engine
// loads the graph by URL.
func (f *RawFlow) SubgraphRequested() (url string, input State, ok bool) {
	if f.subgraphURL == "" {
		return "", State{}, false
	}
	return f.subgraphURL, f.subgraphInput, true
}
