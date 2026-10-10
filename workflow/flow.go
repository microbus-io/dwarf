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
	"encoding/json"
	"time"

	"github.com/microbus-io/errors"
)

// Flow is the carrier object passed to tasks. It holds the state and control
// signals for a single step in a workflow execution.
//
// # A Flow is NOT safe for concurrent use
//
// A task owns its Flow exclusively for the duration of its execution, and the Flow carries no lock. A
// task that fans out internally must therefore collect results in its own goroutines and write them to
// the Flow from a single goroutine:
//
//	// WRONG - concurrent map writes
//	var g errgroup.Group
//	for _, id := range f.GetStrings("ids") {
//	    g.Go(func() error { f.SetString("r_"+id, lookup(id)); return nil })
//	}
//	return g.Wait()
//
//	// RIGHT - fan out, then write from one goroutine
//	results := make([]string, len(ids))
//	var g errgroup.Group
//	for i, id := range ids {
//	    g.Go(func() error { results[i] = lookup(id); return nil })
//	}
//	if err := g.Wait(); err != nil {
//	    return err
//	}
//	for i, id := range ids {
//	    f.SetString("r_"+id, results[i])
//	}
//
// This matters more than the usual "not thread-safe" caveat. Two goroutines writing a Flow trip the Go
// runtime's concurrent-map-write detector, which is a *throw*, not a panic: it cannot be recovered, so
// the engine's panic isolation around a task cannot contain it. One task that does this takes down the
// whole replica, and every unrelated flow in flight on it.
//
// Fanning work out across STEPS (a forEach transition) is the first-class way to parallelize, and each
// branch gets its own Flow.
type Flow struct {
	// State
	state   State
	changes State

	// Control
	gotoNext         string
	retry            bool
	sleepDuration    time.Duration
	interrupt        bool
	interruptPayload State

	// Dynamic subgraph request. subgraphURL holds the graph URL the engine LoadGraph's; subgraphInput is
	// the child's initial state.
	subgraphURL   string
	subgraphInput State

	// Park resolution (inbound), set by the orchestrator on dispatch from the step row.
	// interruptDone/resumeData resolve an Interrupt park; subgraphDone/subgraphResult/
	// subgraphError resolve a Subgraph park. A parker returns its resolved value once the
	// matching *Done flag is set, instead of re-arming.
	interruptDone  bool
	resumeData     State
	subgraphDone   bool
	subgraphResult State
	subgraphError  string

	// Backoff retry
	attempt                int
	backoffInitialDelay    time.Duration
	backoffDelayMultiplier float64
	backoffMaxDelay        time.Duration

	// Flow lifecycle timestamps, populated by the orchestrator on dispatch.
	// Useful to a task implementing its own elapsed-time / lifetime guard.
	createdAt     time.Time
	updatedAt     time.Time
	stepCreatedAt time.Time

	// Identity of the dispatch, populated by the orchestrator. The task's own flow and step keys,
	// for correlating logs/traces or calling back into the engine (History/Step) with its own keys.
	flowKey string
	stepKey string
}

// NewFlow creates a new Flow with initialized state.
func NewFlow() *Flow {
	state, _ := NewState()
	changes, _ := NewState()
	return &Flow{
		state:   state,
		changes: changes,
	}
}

// --- State access ---

// The typed getters below return the zero value for a field that is absent or cleared, and PANIC on a
// field that is present but holds a value of the wrong type - reading a string as an int is a bug in the
// workflow, and returning 0 for it would let the task proceed on a value it never had (a GetInt of a 1.5
// retry delay yielding 0 is a hot loop against the downstream, not a slow retry).
//
// A panic here is a clean failure, not a crash: the orchestrator catches panics at the task-call boundary,
// so a mistyped read fails the step - routed to the graph's onError handler if it has one - with the stack
// trace attached. Use Get to HANDLE a mistyped field instead of failing on it: it returns the error.

// GetString returns a state field as a string. It returns "" if the field is absent, and panics if the
// field holds a non-string.
func (f *Flow) GetString(name string) string { return f.state.GetString(name) }

// GetStrings returns a state field as a string slice. It returns nil if the field is absent, and panics
// if the field holds anything but an array of strings.
func (f *Flow) GetStrings(name string) []string { return f.state.GetStrings(name) }

// GetInt returns a state field as an int. It returns 0 if the field is absent, and panics if the field
// holds a non-integer (a fractional number included).
func (f *Flow) GetInt(name string) int { return f.state.GetInt(name) }

// GetFloat returns a state field as a float64. It returns 0 if the field is absent, and panics if the
// field holds a non-number.
func (f *Flow) GetFloat(name string) float64 { return f.state.GetFloat(name) }

// GetBool returns a state field as a bool. It returns false if the field is absent, and panics if the
// field holds a non-boolean.
func (f *Flow) GetBool(name string) bool { return f.state.GetBool(name) }

// GetDuration returns a state field as a time.Duration. It returns 0 if the field is absent, and panics
// if the field holds anything but a duration in nanoseconds.
func (f *Flow) GetDuration(name string) time.Duration { return f.state.GetDuration(name) }

// Get unmarshals the state field named name into target, a non-nil pointer, and reports whether the field
// was present. It reads the step's input state together with anything this task has already set.
//
// It coerces through JSON, so a stored number reads into an int or a float64 and a stored object into a
// struct or a map. Use it for those, and wherever a type mismatch should be handled rather than fail the
// step: Get returns the mismatch as an error, where the typed getters (GetInt, ...) panic.
//
// An absent or cleared field returns (false, nil) and leaves target untouched, so a default set beforehand
// survives:
//
//	limit := 10
//	if _, err := f.Get("limit", &limit); err != nil {
//		return err
//	}
//
// A present value that cannot be unmarshalled into target's type returns (false, err).
func (f *Flow) Get(name string, target any) (ok bool, err error) {
	return f.state.Get(name, target)
}

// Has reports whether a state field exists. A cleared slot (JSON null) reads
// as absent.
func (f *Flow) Has(name string) bool {
	return f.state.Has(name)
}

// Parse unmarshals the flow's state into target, typically a pointer to a struct whose fields are matched by
// their JSON tags. It reads the step's input state together with anything this task has already set. State
// fields with no matching struct field are ignored, cleared fields are skipped, and struct fields with no
// matching state field keep their values.
//
// Parse only reads. A task writes its output with Set and the typed setters.
func (f *Flow) Parse(target any) error {
	return f.state.Parse(target)
}

// CreatedAt returns the wall-clock time at which the flow was created. Useful for tasks that
// want to implement their own elapsed-time guard (e.g. "if time.Since(flow.CreatedAt()) > 24h
// then return an error to fail the workflow"). Zero when called outside a dispatched task or
// when the orchestrator has not populated it.
func (f *Flow) CreatedAt() time.Time {
	return f.createdAt
}

// UpdatedAt returns the wall-clock time of the flow row's last status transition. Useful for
// tasks that want to gate on "how long since the flow last advanced." Zero when called outside
// a dispatched task or when the orchestrator has not populated it.
func (f *Flow) UpdatedAt() time.Time {
	return f.updatedAt
}

// StepCreatedAt returns the wall-clock time at which this step was first created, preserved across
// retries of the step. It anchors Retry's giveUpAfter horizon, and a task can read it directly to
// implement a custom elapsed-time guard. Zero when called outside a dispatched task.
func (f *Flow) StepCreatedAt() time.Time {
	return f.stepCreatedAt
}

// Attempt returns the zero-based retry attempt counter for the current step: 0 on the first
// execution, incremented by the orchestrator on each Retry. A task can gate on it to bound retries
// by count (e.g. "if flow.Attempt() < 3") instead of, or alongside, Retry's time-based horizon.
func (f *Flow) Attempt() int {
	return f.attempt
}

// FlowKey returns the external key of the flow this task is executing in, in the form
// "{shard}-{flowID}-{token}". Useful for correlating logs/traces or calling back into the engine
// (e.g. History, Snapshot) for the task's own flow. Empty when called outside a dispatched task.
func (f *Flow) FlowKey() string {
	return f.flowKey
}

// StepKey returns the external key of the step this task is executing, in the form
// "{shard}-{stepID}-{token}". Useful for correlating logs/traces or calling back into the engine
// (e.g. Step) for the task's own step. Empty when called outside a dispatched task.
func (f *Flow) StepKey() string {
	return f.stepKey
}

// --- State mutation ---

// A task's output is its changes; state is the in-task working copy. So every setter writes to BOTH: state
// (so a later read in the same task sees it) and changes (which is what the orchestrator persists). Each
// delegates to the matching State method on the two fields; State normalizes the value through JSON.

// Set sets a state field and tracks the change. Use this for complex types (structs, maps, etc.).
// It returns an error only if the value cannot be marshalled to JSON (a NaN, an +Inf, a channel).
func (f *Flow) Set(name string, value any) error {
	if err := f.state.Set(name, value); err != nil {
		return err
	}
	return f.changes.Set(name, value)
}

// SetString sets a state string field and tracks the change.
func (f *Flow) SetString(name string, value string) {
	f.state.SetString(name, value)
	f.changes.SetString(name, value)
}

// SetStrings sets a state string slice field and tracks the change.
func (f *Flow) SetStrings(name string, value []string) {
	f.state.SetStrings(name, value)
	f.changes.SetStrings(name, value)
}

// SetInt sets a state int field and tracks the change.
func (f *Flow) SetInt(name string, value int) {
	f.state.SetInt(name, value)
	f.changes.SetInt(name, value)
}

// SetFloat sets a state float64 field and tracks the change.
func (f *Flow) SetFloat(name string, value float64) {
	f.state.SetFloat(name, value)
	f.changes.SetFloat(name, value)
}

// SetBool sets a state bool field and tracks the change.
func (f *Flow) SetBool(name string, value bool) {
	f.state.SetBool(name, value)
	f.changes.SetBool(name, value)
}

// SetDuration sets a state time.Duration field and tracks the change (nanoseconds).
func (f *Flow) SetDuration(name string, value time.Duration) {
	f.state.SetDuration(name, value)
	f.changes.SetDuration(name, value)
}

// Del removes the listed state fields. Each is recorded as a cleared value
// (JSON null) in changes so the following merge drops it, and is removed from the
// local state map so later reads in this task see it as absent.
func (f *Flow) Del(names ...string) {
	for _, n := range names {
		f.deleteOne(n)
	}
}

// Clear removes every state field. Equivalent to Del on every current field: each is recorded as a cleared
// value (JSON null) in changes so the following merge drops it, and state is emptied. Useful at workflow
// boundaries or anywhere a task wants a blank slate before populating it.
func (f *Flow) Clear() {
	if f.changes.IsZero() {
		f.changes, _ = NewState()
	}
	for k := range f.state.d {
		f.changes.d[k] = nil // tombstone so the delete propagates into the task's persisted changes
	}
	f.state.Clear()
}

// deleteOne is the shared worker: writes a cleared (Go nil) tombstone to changes so the following merge
// drops the field, and removes it from state so later reads in this task see it as absent.
func (f *Flow) deleteOne(name string) {
	if f.changes.IsZero() {
		f.changes, _ = NewState()
	}
	f.changes.d[name] = nil // tombstone; isCleared treats a Go nil (or JSON null) as cleared
	f.state.Del(name)
}

// Snapshot captures an independent copy of the flow's current state (including any changes applied so far) -
// for instance, to pass a subgraph the flow's whole state as its input.
//
// The copy is independent of the flow, so writing to the flow afterwards does not disturb it. It does not
// materialize field values, so snapshotting a large carried payload costs a map entry rather than the payload.
func (f *Flow) Snapshot() State {
	return f.state.Clone()
}

// SetChanges sets each field of source exactly as Set would, one by one. source is any value that marshals to a
// JSON object - typically a struct, or a pointer to one - and its fields are those encoding/json emits, under the
// names it emits them: a json tag's name, else the Go name, with an untagged embedded struct's fields promoted, a
// "-" or unexported field skipped, and an omitempty or omitzero field skipped when empty. A field that marshals
// to null deletes, as Set(name, nil) does. It returns an error if source cannot be marshalled or is not an
// object; on an error, no field is set.
func (f *Flow) SetChanges(source any) error {
	data, err := json.Marshal(source)
	if err != nil {
		return errors.Trace(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return errors.New("SetChanges: source must marshal to a JSON object")
	}
	if f.changes.IsZero() {
		f.changes, _ = NewState()
	}
	if f.state.IsZero() {
		f.state, _ = NewState()
	}
	for name, value := range fields {
		if err := f.Set(name, value); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// --- Control ---

// Goto overrides transition routing. The orchestrator skips condition evaluation and follows the specified
// task instead.
//
// The target must be wired with graph.AddTransitionGoto(from, target) from this task. An unmatched target
// FAILS the step - it does not fall through to normal routing - because a goto that silently did nothing
// would send the flow down the very path the task was trying to override.
func (f *Flow) Goto(taskName string) {
	f.gotoNext = taskName
}

/*
Interrupt parks the flow to await external input, or returns the resume data once it has arrived.

On the first call (not yet resumed) it records the interrupt request with the given payload - surfaced to the
awaiting caller so it can see what input the task needs - and returns yield=true. The task must return
immediately.

On re-entry after Resume it unmarshals the resume data into out with yield=false and does not re-arm; the task
proceeds. The payload is any JSON-marshalable value (a struct or a map[string]any); out is a pointer (a *struct or
*map[string]any) the resume data is unmarshaled into by JSON tag, or nil to ignore it. The returned err is non-nil
if the payload cannot be marshalled, if out fails to unmarshal, or if this step already parked: a step parks at
most once, so a second Interrupt in one dispatch, or an Interrupt after a Subgraph, is rejected. A rejected call
does not arm the interrupt. The payload is copied, so
mutating it after the call does not change what is persisted. Symmetric with Subgraph: any in, pointer out.

	var resume ResumeData
	yield, err := flow.Interrupt(map[string]any{"request": "userInput"}, &resume)
	if yield {
	    return nil // parked, awaiting Resume
	}
	// proceed with resume
*/
func (f *Flow) Interrupt(payload any, out any) (yield bool, err error) {
	if f.interruptDone {
		if out != nil {
			err := f.resumeData.Parse(out)
			if err != nil {
				return false, errors.Trace(err)
			}
		}
		return false, nil
	}
	// Single-park guard: a step parks at most once - interrupt XOR subgraph, and at most once per kind.
	// Reject arming an interrupt when this step already parked for a subgraph (resolved subgraphDone, or
	// armed earlier in this same dispatch via subgraphURL)...
	if f.subgraphDone || f.subgraphURL != "" {
		return false, errors.New("cannot interrupt: step already parked for a subgraph")
	}
	// ...or when an interrupt was already armed earlier in this same dispatch: a second flow.Interrupt
	// call before the task returns would otherwise silently overwrite the payload.
	if f.interrupt {
		return false, errors.New("cannot interrupt: step already armed an interrupt this dispatch")
	}
	payloadState, err := NewState(payload)
	if err != nil {
		return false, errors.Trace(err)
	}
	f.interrupt = true
	f.interruptPayload = payloadState
	return true, nil
}

/*
Subgraph runs a child workflow and unmarshals its result once it completes, parking the step in between.

Semantically a function call: only the explicit in argument crosses the boundary into the child, and only the explicit
out crosses back. The parent's state does NOT auto-cross either direction. in is any JSON-marshalable value (a
struct or a map[string]any) and becomes the child's initial state field-by-field; a nil in means "no arguments"
(the child starts with empty state). A caller that wants the parent's full state to cross can pass flow.Snapshot() as
in. The out argument is a pointer (a *struct or *map[string]any) into which the child's final_state is unmarshaled
by JSON tag; pass nil to ignore the result. Using a typed struct reads only the fields you declare, with type safety.

On the first call (child not yet run) it arms the subgraph park with the child workflow URL and in and returns
yield=true; the task must return immediately.

On re-entry after the child terminates it unmarshals the child's final_state into out, returns yield=false, and sets
err if the child failed. Does not re-arm on re-entry. Like Interrupt, it returns an error without arming if in cannot
be marshalled or this step already parked.

	var out ChildOut
	yield, err := flow.Subgraph(childURL, ChildIn{Value: value}, &out)
	if yield {
	    return nil // parked, child running
	}
	if err != nil {
	    if flow.Retry(time.Second, 2.0, 30*time.Second, time.Hour) {
	        return nil
	    }
	    return err
	}
	// read fields from out
*/
func (f *Flow) Subgraph(workflowURL string, in any, out any) (yield bool, err error) {
	if f.subgraphDone {
		if out != nil {
			err := f.subgraphResult.Parse(out)
			if err != nil {
				return false, errors.Trace(err)
			}
		}
		if f.subgraphError != "" {
			return false, decodeSubgraphError(f.subgraphError)
		}
		return false, nil
	}
	// Single-park guard: a step parks at most once - interrupt XOR subgraph, and at most once per kind.
	// Reject arming a subgraph when this step already parked for an interrupt (resolved interruptDone, or
	// armed earlier in this same dispatch via interrupt)...
	if f.interruptDone || f.interrupt {
		return false, errors.New("cannot start subgraph: step already parked for an interrupt")
	}
	// ...or when a subgraph was already armed earlier in this same dispatch: a second flow.Subgraph call
	// before the task returns would otherwise silently overwrite the child workflow/input.
	if f.subgraphURL != "" {
		return false, errors.New("cannot start subgraph: step already armed a subgraph this dispatch")
	}
	inputState, err := NewState(in)
	if err != nil {
		return false, errors.Trace(err)
	}
	f.subgraphURL = workflowURL
	f.subgraphInput = inputState
	return true, nil
}

// decodeSubgraphError rebuilds the error a child flow delivered. The engine stores it as the error's
// TracedError JSON, so its status code and properties - a cancellation marker among them - survive the hop.
// A value that is not such JSON is taken as a bare message, and passed as an argument rather than as the
// pattern, which would read any % in it as a format verb.
func decodeSubgraphError(s string) error {
	var te errors.TracedError
	if json.Unmarshal([]byte(s), &te) == nil && te.Err != nil && te.Error() != "" {
		return &te
	}
	return errors.New("%s", s)
}

/*
Retry requests the orchestrator to re-execute this task with exponential backoff. The bound is
wall-clock, not a count: Retry returns true (the caller should return nil) while the next attempt would
still land within giveUpAfter of the step's first creation, and false (the caller should return its
error) once the horizon is reached - including when the next backoff delay alone would overshoot it, so
a wait we already know is doomed is not parked before failing. Pass giveUpAfter <= 0 for unlimited retry.

The delay before attempt N is min(initialDelay * delayMultiplier^N, maxIntervalDelay); pass a zero
initialDelay for immediate retries, and a zero maxIntervalDelay for no per-interval cap. To hold the
delay constant (e.g. honoring a provider's Retry-After carried in initialDelay), pass delayMultiplier
1.0. Sleep, if also set, is added on top as a floor.

Retry carries no condition of its own - it is the single retry primitive, called inside whatever
error branch the task decides is retryable. Keeping the condition explicit at the call site avoids
the "retry on every error" trap (most errors - validation, bad input, business rejections - should
not be retried). Gate it on whatever your task considers transient:

	result, err := callExternalAPI(ctx)
	if err != nil {
	    if isTransient(err) && flow.Retry(1*time.Second, 2.0, 30*time.Second, 1*time.Hour) {
	        return result, nil // transient failure: retry scheduled, don't report error
	    }
	    return result, err // non-retryable, or horizon exceeded
	}

To bound by count instead of (or in addition to) time, gate on Attempt: pass giveUpAfter 0 and check
flow.Attempt() at the call site.
*/
func (f *Flow) Retry(initialDelay time.Duration, delayMultiplier float64, maxIntervalDelay time.Duration, giveUpAfter time.Duration) bool {
	if giveUpAfter > 0 && !f.stepCreatedAt.IsZero() {
		// Give up if we've crossed the horizon, or if the next attempt's delay alone would overshoot it -
		// no point parking for a wait we already know lands past the deadline. The delay is the one the
		// engine will apply (see backoffDelay).
		nextDelay := backoffDelay(initialDelay, delayMultiplier, maxIntervalDelay, f.attempt)
		if time.Since(f.stepCreatedAt)+nextDelay >= giveUpAfter {
			f.retry = false
			return false
		}
	}
	f.retry = true
	f.backoffInitialDelay = initialDelay
	f.backoffDelayMultiplier = delayMultiplier
	f.backoffMaxDelay = maxIntervalDelay
	return true
}

// Sleep tells the orchestrator to wait for the given duration before the next execution. A non-positive
// duration is a no-op (no sleep is requested), not an error - "wait for no time" and "do not wait" are the
// same instruction, so a computed delay that lands at or below zero needs no guard at the call site.
func (f *Flow) Sleep(duration time.Duration) {
	f.sleepDuration = duration
}

// --- Internal helpers ---

// --- JSON serialization ---

// flowJSON is the wire format for Flow.
type flowJSON struct {
	FlowKey                string        `json:"flowKey,omitzero"`
	StepKey                string        `json:"stepKey,omitzero"`
	State                  State         `json:"state,omitzero"`
	Changes                State         `json:"changes,omitzero"`
	Goto                   string        `json:"goto,omitzero"`
	Retry                  bool          `json:"retry,omitzero"`
	SleepDuration          time.Duration `json:"sleepDuration,omitzero"`
	Interrupt              bool          `json:"interrupt,omitzero"`
	InterruptPayload       State         `json:"interruptPayload,omitzero"`
	SubgraphURL            string        `json:"subgraphURL,omitzero"`
	SubgraphInput          State         `json:"subgraphInput,omitzero"`
	InterruptDone          bool          `json:"interruptDone,omitzero"`
	ResumeData             State         `json:"resumeData,omitzero"`
	SubgraphDone           bool          `json:"subgraphDone,omitzero"`
	SubgraphResult         State         `json:"subgraphResult,omitzero"`
	SubgraphError          string        `json:"subgraphError,omitzero"`
	Attempt                int           `json:"attempt,omitzero"`
	BackoffInitialDelay    time.Duration `json:"backoffInitialDelay,omitzero"`
	BackoffDelayMultiplier float64       `json:"backoffDelayMultiplier,omitzero"`
	BackoffMaxDelay        time.Duration `json:"backoffMaxDelay,omitzero"`
	CreatedAt              time.Time     `json:"createdAt,omitzero"`
	UpdatedAt              time.Time     `json:"updatedAt,omitzero"`
	StepCreatedAt          time.Time     `json:"stepCreatedAt,omitzero"`
}

// MarshalJSON serializes the Flow including private fields.
func (f *Flow) MarshalJSON() ([]byte, error) {
	return json.Marshal(flowJSON{
		FlowKey:                f.flowKey,
		StepKey:                f.stepKey,
		State:                  f.state,
		Changes:                f.changes,
		Goto:                   f.gotoNext,
		Retry:                  f.retry,
		SleepDuration:          f.sleepDuration,
		Interrupt:              f.interrupt,
		InterruptPayload:       f.interruptPayload,
		SubgraphURL:            f.subgraphURL,
		SubgraphInput:          f.subgraphInput,
		InterruptDone:          f.interruptDone,
		ResumeData:             f.resumeData,
		SubgraphDone:           f.subgraphDone,
		SubgraphResult:         f.subgraphResult,
		SubgraphError:          f.subgraphError,
		Attempt:                f.attempt,
		BackoffInitialDelay:    f.backoffInitialDelay,
		BackoffDelayMultiplier: f.backoffDelayMultiplier,
		BackoffMaxDelay:        f.backoffMaxDelay,
		CreatedAt:              f.createdAt,
		UpdatedAt:              f.updatedAt,
		StepCreatedAt:          f.stepCreatedAt,
	})
}

// UnmarshalJSON deserializes the Flow including private fields.
func (f *Flow) UnmarshalJSON(data []byte) error {
	var wire flowJSON
	err := json.Unmarshal(data, &wire)
	if err != nil {
		return err
	}
	f.flowKey = wire.FlowKey
	f.stepKey = wire.StepKey
	f.state = wire.State
	f.changes = wire.Changes
	// Either may be absent from the wire; the setters write to both, so neither may stay nil.
	if f.state.IsZero() {
		f.state, _ = NewState()
	}
	if f.changes.IsZero() {
		f.changes, _ = NewState()
	}
	f.gotoNext = wire.Goto
	f.retry = wire.Retry
	f.sleepDuration = wire.SleepDuration
	f.interrupt = wire.Interrupt
	f.interruptPayload = wire.InterruptPayload
	f.subgraphURL = wire.SubgraphURL
	f.subgraphInput = wire.SubgraphInput
	f.interruptDone = wire.InterruptDone
	f.resumeData = wire.ResumeData
	f.subgraphDone = wire.SubgraphDone
	f.subgraphResult = wire.SubgraphResult
	f.subgraphError = wire.SubgraphError
	f.attempt = wire.Attempt
	f.backoffInitialDelay = wire.BackoffInitialDelay
	f.backoffDelayMultiplier = wire.BackoffDelayMultiplier
	f.backoffMaxDelay = wire.BackoffMaxDelay
	f.createdAt = wire.CreatedAt
	f.updatedAt = wire.UpdatedAt
	f.stepCreatedAt = wire.StepCreatedAt
	return nil
}
