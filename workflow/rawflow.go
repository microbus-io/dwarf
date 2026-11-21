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
	"math"
	"time"

	"github.com/microbus-io/errors"
)

// RawFlow is the side of a Flow the engine drives: it sets what a task reads and reads back what a task
// requested. Task code uses Flow.
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

// SetDeferrals sets the count of the step's deferrals since it last started over. Called by the orchestrator before
// dispatching a task so that Defer can check whether its bound is reached.
func (f *RawFlow) SetDeferrals(deferrals int) {
	f.deferrals = deferrals
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
// orchestrator before dispatching a task so the task can read it via Flow.StepCreatedAt().
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
// and error (nil for none), so flow.Subgraph returns them (with yield=false) on re-entry instead of re-arming.
// The orchestrator calls this only once the child flow has actually completed. The error is kept as an
// *errors.TracedError without its stack: its message, status code, trace ID and properties reach the task.
func (f *RawFlow) SetSubgraphResolution(result State, err error) {
	f.subgraphDone = true
	f.subgraphResult = result
	f.subgraphError = nil
	if err != nil {
		delivered := errors.TracedError{Err: err}
		if traced, ok := err.(*errors.TracedError); ok {
			delivered = *traced
		}
		delivered.Stack = nil
		f.subgraphError = &delivered
	}
}

// SetCancellation records that the step runs after its flow was cancelled, so Retry and Defer return err instead of
// arming. As for SetSubgraphResolution, the error's stack is dropped.
func (f *RawFlow) SetCancellation(err error) {
	f.cancelledErr = nil
	if err != nil {
		delivered := errors.TracedError{Err: err}
		if traced, ok := err.(*errors.TracedError); ok {
			delivered = *traced
		}
		delivered.Stack = nil
		f.cancelledErr = &delivered
	}
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

// DeferRequested returns the backoff parameters (initialDelay, multiplier, maxDelay) and true if Defer was
// called.
func (f *RawFlow) DeferRequested() (initialDelay time.Duration, multiplier float64, maxDelay time.Duration, ok bool) {
	if !f.deferred {
		return 0, 0, 0, false
	}
	return f.backoffInitialDelay, f.backoffDelayMultiplier, f.backoffMaxDelay, true
}

// DeferDelay returns the backoff before the next run of a step that called Defer after the given number of
// deferrals: min(initialDelay * multiplier^deferrals, maxDelay), or zero if Defer was not called. It is the same
// delay Defer's give-up decision assumes.
func (f *RawFlow) DeferDelay(deferrals int) time.Duration {
	if !f.deferred {
		return 0
	}
	return backoffDelay(f.backoffInitialDelay, f.backoffDelayMultiplier, f.backoffMaxDelay, deferrals)
}

// backoffDelay is min(initialDelay * multiplier^attempt, maxDelay); a non-positive multiplier or maxDelay
// disables that term. A negative initialDelay, which Retry refuses but a carrier decoded off the wire may hold,
// comes back as it is, for the engine to refuse. Without a maxDelay the product saturates at the largest Duration:
// converting a float64 past int64's range is implementation-defined, and a huge delay came out negative on amd64.
//
// The loop stops early once the product reaches the ceiling only for a multiplier that cannot bring it back down:
// a multiplier below 1 decays a delay that starts above the cap, so stopping there returned the cap at every
// attempt. A decaying product stops once it reaches zero.
func backoffDelay(initialDelay time.Duration, multiplier float64, maxDelay time.Duration, attempt int) time.Duration {
	ceiling := time.Duration(math.MaxInt64)
	if maxDelay > 0 {
		ceiling = maxDelay
	}
	delay := float64(initialDelay)
	if multiplier > 0 && multiplier != 1 && delay > 0 {
		for range attempt {
			if (multiplier >= 1 && delay >= float64(ceiling)) || delay == 0 {
				break
			}
			delay *= multiplier
		}
	}
	if delay >= float64(ceiling) {
		return ceiling
	}
	return time.Duration(delay)
}

// backoffFits checks delays 0..attempt against budget, with inputs validated by Retry or Defer. The unrounded product
// advances once per delay so the sum matches backoffDelay's per-attempt rounding without replaying every prefix.
// Constant and capped tails use division to avoid overflowing either the sum or attempt+1.
func backoffFits(initialDelay time.Duration, multiplier float64, maxDelay time.Duration, attempt int, budget time.Duration) bool {
	if attempt < 0 || initialDelay == 0 {
		return true
	}
	ceiling := time.Duration(math.MaxInt64)
	if maxDelay > 0 {
		ceiling = maxDelay
	}
	product := float64(initialDelay)
	for remaining := attempt; ; remaining-- {
		delay := ceiling
		if product < float64(ceiling) {
			delay = time.Duration(product)
		}
		if delay == 0 {
			return true // a decaying product below one nanosecond contributes no more delay
		}
		next := product * multiplier
		if multiplier == 0 || next == product || (multiplier > 1 && delay == ceiling) {
			return int64(remaining) < int64(budget/delay)
		}
		if delay > budget {
			return false
		}
		budget -= delay
		if remaining == 0 {
			return true
		}
		product = next
	}
}

// SleepRequested returns the duration set by Sleep, or zero if none was set. A negative duration - which
// Sleep accepts as a no-op, and which a Flow decoded off the wire from a remote task may carry - reads as zero.
func (f *RawFlow) SleepRequested() time.Duration {
	return max(f.sleepDuration, 0)
}

// InterruptRequested returns the interrupt payload and true if Interrupt was called.
func (f *RawFlow) InterruptRequested() (payload State, requested bool) {
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
