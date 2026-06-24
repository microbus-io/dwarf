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
	"maps"
	"reflect"
	"time"

	"github.com/microbus-io/errors"
)

// Flow is the carrier object passed to tasks. It holds the state and control
// signals for a single step in a workflow execution.
type Flow struct {
	// State
	state   map[string]any
	changes map[string]any

	// Control
	gotoNext         string
	retry            bool
	sleepDuration    time.Duration
	interrupt        bool
	interruptPayload map[string]any

	// Dynamic subgraph / subtask request. subgraphURL holds the URL (a graph URL for Subgraph, a task
	// URL for Subtask); subgraphTaskName is both the discriminator and the synthesized graph's name:
	// non-empty => subtask (the engine builds a single-task graph named subgraphTaskName), empty =>
	// regular subgraph (the engine LoadGraph's the URL).
	subgraphURL      string
	subgraphInput    map[string]any
	subgraphTaskName string

	// Park resolution (inbound), set by the orchestrator on dispatch from the step row.
	// interruptDone/resumeData resolve an Interrupt park; subgraphDone/subgraphResult/
	// subgraphError resolve a Subgraph park. A parker returns its resolved value once the
	// matching *Done flag is set, instead of re-arming.
	interruptDone  bool
	resumeData     map[string]any
	subgraphDone   bool
	subgraphResult map[string]any
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

// NewFlow creates a new Flow with initialized maps.
func NewFlow() *Flow {
	return &Flow{
		state:   make(map[string]any),
		changes: make(map[string]any),
	}
}

// --- State access ---

// GetString returns a state field as a string.
func (f *Flow) GetString(key string) string {
	var v string
	getFromMap(f.state, key, &v)
	return v
}

// GetStrings returns a state field as a string slice.
func (f *Flow) GetStrings(key string) []string {
	var v []string
	getFromMap(f.state, key, &v)
	return v
}

// GetInt returns a state field as an int.
func (f *Flow) GetInt(key string) int {
	var v int
	getFromMap(f.state, key, &v)
	return v
}

// GetFloat returns a state field as a float64.
func (f *Flow) GetFloat(key string) float64 {
	var v float64
	getFromMap(f.state, key, &v)
	return v
}

// GetBool returns a state field as a bool.
func (f *Flow) GetBool(key string) bool {
	var v bool
	getFromMap(f.state, key, &v)
	return v
}

// GetDuration returns a state field as a time.Duration.
func (f *Flow) GetDuration(key string) time.Duration {
	var v time.Duration
	getFromMap(f.state, key, &v)
	return v
}

// Get unmarshals a state field into the target. Use this for complex types (structs, maps, etc.).
func (f *Flow) Get(key string, target any) error {
	return getFromMap(f.state, key, target)
}

// Has reports whether a state field exists. A cleared slot (JSON null) reads
// as absent.
func (f *Flow) Has(key string) bool {
	v, ok := f.state[key]
	return ok && !isCleared(v)
}

// ParseState unmarshals state fields into the target struct.
// Fields are matched by their JSON tag names. Fields in state that are not in the struct are ignored.
func (f *Flow) ParseState(target any) error {
	return parseMapInto(f.state, target)
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

// Set sets a state field and tracks the change. Use this for complex types (structs, maps, etc.).
func (f *Flow) Set(key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if f.state == nil {
		f.state = make(map[string]any)
	}
	if f.changes == nil {
		f.changes = make(map[string]any)
	}
	raw := json.RawMessage(data)
	f.state[key] = raw
	f.changes[key] = raw
	return nil
}

// SetString sets a state string field and tracks the change.
func (f *Flow) SetString(key string, value string) {
	f.set(key, value)
}

// SetStrings sets a state string slice field and tracks the change.
func (f *Flow) SetStrings(key string, value []string) {
	f.set(key, value)
}

// SetInt sets a state int field and tracks the change.
func (f *Flow) SetInt(key string, value int) {
	f.set(key, value)
}

// SetFloat sets a state float64 field and tracks the change.
func (f *Flow) SetFloat(key string, value float64) {
	f.set(key, value)
}

// SetBool sets a state bool field and tracks the change.
func (f *Flow) SetBool(key string, value bool) {
	f.set(key, value)
}

// SetDuration sets a state time.Duration field and tracks the change.
func (f *Flow) SetDuration(key string, value time.Duration) {
	f.set(key, value)
}

// set is an internal helper that marshals a value and records it in state and changes.
func (f *Flow) set(key string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err) // should never happen for primitive types
	}
	if f.state == nil {
		f.state = make(map[string]any)
	}
	if f.changes == nil {
		f.changes = make(map[string]any)
	}
	raw := json.RawMessage(data)
	f.state[key] = raw
	f.changes[key] = raw
}

// Delete removes the listed state fields. Each is recorded as a cleared value
// (JSON null) in changes so the following merge drops it, and is removed from the
// local state map so later reads in this task see it as absent.
func (f *Flow) Delete(keys ...string) {
	for _, k := range keys {
		f.deleteOne(k)
	}
}

// Clear removes every state field. Equivalent to Delete on every current key.
// Useful at workflow boundaries (e.g. a task that builds a fresh subgraph input
// from a curated subset of parent state) or anywhere a task wants a blank slate
// before populating it.
func (f *Flow) Clear() {
	for k := range f.state {
		f.deleteOne(k)
	}
}

// Transform clears all state, then re-introduces the listed fields under new
// names. Arguments are (newKey, oldKey) pairs; the value previously stored
// under oldKey is captured before the clear and re-set under newKey. Old keys
// that were absent or already null are skipped (the new key is not introduced
// as null). Panics on an odd number of arguments.
//
// Typical use: a small task immediately upstream of a subgraph node that
// reshapes parent state into the subgraph's expected input.
//
//	flow.Transform("subInput1", "parentVarA", "subInput2", "parentVarB")
func (f *Flow) Transform(pairs ...string) {
	if len(pairs)%2 != 0 {
		panic("workflow: Transform requires an even number of arguments (newKey, oldKey, ...)")
	}
	n := len(pairs) / 2
	captured := make([]any, n)
	for i := range n {
		captured[i] = f.state[pairs[i*2+1]]
	}
	f.Clear()
	for i := range n {
		v := captured[i]
		if isCleared(v) {
			continue
		}
		if f.state == nil {
			f.state = make(map[string]any)
		}
		newKey := pairs[i*2]
		if raw, ok := v.(json.RawMessage); ok {
			f.state[newKey] = raw
			f.changes[newKey] = raw
		} else {
			_ = f.Set(newKey, v)
		}
	}
}

// deleteOne is the shared worker: writes JSON null to changes, drops from state.
func (f *Flow) deleteOne(key string) {
	if f.changes == nil {
		f.changes = make(map[string]any)
	}
	f.changes[key] = json.RawMessage("null")
	delete(f.state, key)
}

// Snapshot captures a read-only copy of the flow's current state
// (including any changes applied so far). Pass the returned snapshot to SetChanges
// to record only the fields that differ.
func (f *Flow) Snapshot() map[string]any {
	snap := make(map[string]any, len(f.state))
	maps.Copy(snap, f.state)
	return snap
}

// SetState marshals the source struct fields into state without tracking changes.
// Fields are matched by their JSON tag names.
func (f *Flow) SetState(source any) error {
	if f.state == nil {
		f.state = make(map[string]any)
	}
	return f.applyFields(source, f.state)
}

// SetChanges marshals the source struct back to state, comparing against the provided snapshot.
// Only fields whose JSON value differs from the snapshot are recorded as changes.
// Changed fields are written to both the state and changes maps, so that subsequent reads
// (including transition condition evaluation) see the updated values.
func (f *Flow) SetChanges(source any, snap map[string]any) error {
	if f.changes == nil {
		f.changes = make(map[string]any)
	}
	if f.state == nil {
		f.state = make(map[string]any)
	}
	return f.diffAndApply(source, snap, f.state, f.changes)
}

// --- Control ---

// Goto overrides transition routing. The orchestrator skips condition evaluation
// and follows the specified task instead.
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
only if the payload fails to marshal (or out fails to unmarshal); interrupt itself has no failure mode, so err is
otherwise always nil. Symmetric with Subgraph: any in, pointer out.

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
			if err := parseMapInto(f.resumeData, out); err != nil {
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
	payloadMap, err := toStateMap(payload)
	if err != nil {
		return false, errors.Trace(err)
	}
	f.interrupt = true
	f.interruptPayload = payloadMap
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
err if the child failed. Does not re-arm on re-entry.

	var out ChildOut
	yield, err := flow.Subgraph(childURL, ChildIn{Value: value}, &out)
	if yield {
	    return nil // parked, child running
	}
	if err != nil {
	    if flow.Retry(time.Second, 2.0, 30*time.Second, time.Hour) {
	        return nil
	    }
	    return errors.Trace(err)
	}
	// read fields from out
*/
func (f *Flow) Subgraph(workflowURL string, in any, out any) (yield bool, err error) {
	if f.subgraphDone {
		if out != nil {
			if err := parseMapInto(f.subgraphResult, out); err != nil {
				return false, errors.Trace(err)
			}
		}
		if f.subgraphError != "" {
			return false, errors.New(f.subgraphError)
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
	inputMap, err := toStateMap(in)
	if err != nil {
		return false, errors.Trace(err)
	}
	f.subgraphURL = workflowURL
	f.subgraphInput = inputMap
	return true, nil
}

// Subtask launches a single task as an isolated child flow, the task-level sibling of Subgraph. The
// engine synthesizes a trivial one-node graph (named name) around taskURL, so any task endpoint can be
// invoked without a graph definition; everything after launch - parking, re-entry, the out-pointer
// result, cancel/interrupt propagation - is identical to Subgraph. name is required and non-empty (the
// node's display name in diagrams/history).
//
// Pass a task URL, not a graph URL: a graph URL would be wrapped as a one-node graph and dispatched as a
// task, failing at dispatch. (Symmetrically, Subgraph with a task URL fails in LoadGraph.)
func (f *Flow) Subtask(name, taskURL string, in any, out any) (yield bool, err error) {
	if name == "" {
		return false, errors.New("subtask name is required")
	}
	yield, err = f.Subgraph(taskURL, in, out)
	if yield && err == nil {
		// A fresh request was armed this dispatch; mark it a subtask. (On re-entry Subgraph returns
		// yield=false and nothing is armed, so this is correctly skipped.)
		f.subgraphTaskName = name
	}
	return yield, err
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
		// no point parking for a wait we already know lands past the deadline. nextDelay mirrors the delay
		// the engine computes for this attempt (min(initialDelay * delayMultiplier^attempt, maxIntervalDelay)).
		nextDelay := float64(initialDelay)
		if delayMultiplier > 0 {
			for range f.attempt {
				nextDelay *= delayMultiplier
			}
		}
		if maxIntervalDelay > 0 && time.Duration(nextDelay) > maxIntervalDelay {
			nextDelay = float64(maxIntervalDelay)
		}
		if time.Since(f.stepCreatedAt)+time.Duration(nextDelay) >= giveUpAfter {
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

// Sleep tells the orchestrator to wait for the given duration before the next execution.
func (f *Flow) Sleep(duration time.Duration) {
	if duration >= 0 {
		f.sleepDuration = duration
	}
}

// --- Control signal inspection ---

// GotoRequested returns the task URL set by Goto, or empty if not set.
func (f *Flow) GotoRequested() string {
	return f.gotoNext
}

// RetryRequested returns the backoff parameters (initialDelay, multiplier, maxDelay) and true if Retry
// was called.
func (f *Flow) RetryRequested() (initialDelay time.Duration, multiplier float64, maxDelay time.Duration, ok bool) {
	if !f.retry {
		return 0, 0, 0, false
	}
	return f.backoffInitialDelay, f.backoffDelayMultiplier, f.backoffMaxDelay, true
}

// SleepRequested returns the duration set by Sleep, or zero if not set.
func (f *Flow) SleepRequested() time.Duration {
	return max(f.sleepDuration, 0)
}

// InterruptRequested returns the interrupt payload and true if Interrupt was called.
func (f *Flow) InterruptRequested() (map[string]any, bool) {
	return f.interruptPayload, f.interrupt
}

// SubgraphRequested returns the request URL, input state, the subtask name, and true if Subgraph or
// Subtask was called. taskName discriminates: non-empty means Subtask (the engine synthesizes a
// single-task graph named taskName), empty means a regular Subgraph (the engine loads the graph by URL).
func (f *Flow) SubgraphRequested() (url string, input map[string]any, taskName string, ok bool) {
	if f.subgraphURL == "" {
		return "", nil, "", false
	}
	return f.subgraphURL, f.subgraphInput, f.subgraphTaskName, true
}

// --- Internal helpers ---

// applyFields marshals each field of source into the target map.
func (f *Flow) applyFields(source any, target map[string]any) error {
	v := reflect.ValueOf(source)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		tag := jsonTagName(field)
		if tag == "" || tag == "-" {
			continue
		}
		data, err := json.Marshal(v.Field(i).Interface())
		if err != nil {
			return err
		}
		target[tag] = json.RawMessage(data)
	}
	return nil
}

// diffAndApply marshals each field of source, compares against the snapshot,
// and writes changed fields to both state and changes.
func (f *Flow) diffAndApply(source any, snapshot, state, changes map[string]any) error {
	v := reflect.ValueOf(source)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		tag := jsonTagName(field)
		if tag == "" || tag == "-" {
			continue
		}
		data, err := json.Marshal(v.Field(i).Interface())
		if err != nil {
			return err
		}
		// Only record as change if different from snapshot
		if snapshot != nil {
			if prev, ok := snapshot[tag]; ok {
				prevData, _ := marshalAny(prev)
				if string(prevData) == string(data) {
					continue
				}
			}
		}
		raw := json.RawMessage(data)
		state[tag] = raw
		changes[tag] = raw
	}
	return nil
}

// --- JSON serialization ---

// flowJSON is the wire format for Flow.
type flowJSON struct {
	FlowKey                string         `json:"flowKey,omitzero"`
	StepKey                string         `json:"stepKey,omitzero"`
	WorkflowURL            string         `json:"workflowURL"`
	TaskName               string         `json:"taskName"`
	StepNum                int            `json:"stepNum"`
	State                  map[string]any `json:"state,omitzero"`
	Changes                map[string]any `json:"changes,omitzero"`
	Goto                   string         `json:"goto,omitzero"`
	Retry                  bool           `json:"retry,omitzero"`
	SleepDuration          time.Duration  `json:"sleepDuration,omitzero"`
	Interrupt              bool           `json:"interrupt,omitzero"`
	InterruptPayload       map[string]any `json:"interruptPayload,omitzero"`
	SubgraphURL            string         `json:"subgraphURL,omitzero"`
	SubgraphInput          map[string]any `json:"subgraphInput,omitzero"`
	SubgraphTaskName       string         `json:"subgraphTaskName,omitzero"`
	InterruptDone          bool           `json:"interruptDone,omitzero"`
	ResumeData             map[string]any `json:"resumeData,omitzero"`
	SubgraphDone           bool           `json:"subgraphDone,omitzero"`
	SubgraphResult         map[string]any `json:"subgraphResult,omitzero"`
	SubgraphError          string         `json:"subgraphError,omitzero"`
	Attempt                int            `json:"attempt,omitzero"`
	BackoffInitialDelay    time.Duration  `json:"backoffInitialDelay,omitzero"`
	BackoffDelayMultiplier float64        `json:"backoffDelayMultiplier,omitzero"`
	BackoffMaxDelay        time.Duration  `json:"backoffMaxDelay,omitzero"`
	CreatedAt              time.Time      `json:"createdAt,omitzero"`
	UpdatedAt              time.Time      `json:"updatedAt,omitzero"`
	StepCreatedAt          time.Time      `json:"stepCreatedAt,omitzero"`
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
		SubgraphTaskName:       f.subgraphTaskName,
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
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	f.flowKey = wire.FlowKey
	f.stepKey = wire.StepKey
	f.state = wire.State
	f.changes = wire.Changes
	f.gotoNext = wire.Goto
	f.retry = wire.Retry
	f.sleepDuration = wire.SleepDuration
	f.interrupt = wire.Interrupt
	f.interruptPayload = wire.InterruptPayload
	f.subgraphURL = wire.SubgraphURL
	f.subgraphInput = wire.SubgraphInput
	f.subgraphTaskName = wire.SubgraphTaskName
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
