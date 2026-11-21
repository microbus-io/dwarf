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
	"net/http"
	"slices"
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
	// State: the task's view (input combined with what it wrote) and what it wrote. reducers are the graph's
	// field reducers, which every write combines through; inBranch says the step runs inside a fan-out branch.
	state    State
	changes  Changes
	reducers map[string]Reducer
	inBranch bool

	// Control
	gotoNext         string
	retry            bool
	deferred         bool
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
	subgraphError  *errors.TracedError

	// cancelledErr is the flow's cancellation, set by the orchestrator when it runs a step of a cancelled flow;
	// Retry and Defer return it instead of arming.
	cancelledErr *errors.TracedError

	// Backoff of a Retry or a Defer. attempt counts the step's Retries, deferrals its Defers since the last Retry.
	attempt                int
	deferrals              int
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

	// carrierVersion is the carrier format version the Flow was decoded with, which every encoding of it repeats;
	// 0 if it was never decoded.
	carrierVersion int
}

// NewFlow creates a new Flow with initialized state. The zero Flow is ready to use as well.
func NewFlow() *Flow {
	state, _ := NewState()
	return &Flow{
		state:   state,
		changes: NewChanges(),
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

// GetDuration returns a state field, an integer number of milliseconds as SetDuration stores it, as a
// time.Duration. It returns 0 if the field is absent, and panics if the field holds anything but an integer.
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

// UpdatedAt returns the wall-clock time of the flow's last status change, such as its start or its latest
// Resume; it does not move as steps run. Zero when called outside a dispatched task or when the orchestrator
// has not populated it.
func (f *Flow) UpdatedAt() time.Time {
	return f.updatedAt
}

// StepCreatedAt returns the wall-clock time at which this step was first created, preserved across
// retries of the step. Retry's giveUpAfter does not use it; a task that wants an elapsed-time bound reads it
// and gates Retry on it. Zero when called outside a dispatched task.
func (f *Flow) StepCreatedAt() time.Time {
	return f.stepCreatedAt
}

// Attempt returns the zero-based retry attempt counter for the current step: 0 on the first
// execution, incremented by the orchestrator on each Retry. A task can gate on it to bound retries
// by count (e.g. "if flow.Attempt() < 3") instead of, or alongside, Retry's time-based horizon.
// A Defer leaves it as it is.
func (f *Flow) Attempt() int {
	return f.attempt
}

// Deferrals returns how many times the current step was deferred since it last started over or got its answer: 0 on
// its first execution, after a Retry, after a Resume of its Interrupt and when its Subgraph returns, incremented by
// the orchestrator on each Defer. A host that cannot reach the task defers too, so the count includes deferrals the
// task never saw.
func (f *Flow) Deferrals() int {
	return f.deferrals
}

// FlowKey returns the external key of the flow this task is executing in, in the form
// "{shard}-{hour}-{flowID}-{token}". Useful for correlating logs/traces or calling back into the engine
// (e.g. History, Snapshot) for the task's own flow. Empty when called outside a dispatched task.
func (f *Flow) FlowKey() string {
	return f.flowKey
}

// StepKey returns the external key of the step this task is executing, in the form
// "{shard}-{hour}-{flowID}-{stepID}-{token}". Useful for correlating logs/traces or calling back into the engine
// (e.g. Step) for the task's own step. Empty when called outside a dispatched task.
//
// The key is the same on every execution of the step - a Retry, a Defer, or a re-run after a crash or an expired claim -
// so it is the idempotency key for a side effect the step makes: pass it to a downstream that deduplicates by it,
// or record it in the same transaction as the effect. A new step (a loop through Goto), a Fork, a Continue and a
// new flow each have a new key, and their effects happen again. See
// https://docs.dwarf.run/building-workflows/tasks/#idempotency.
func (f *Flow) StepKey() string {
	return f.stepKey
}

// --- State mutation ---

// A task's output is its changes; state is the in-task working view. Every setter writes BOTH: changes (which is
// what the orchestrator persists) and state (so a later read in the same task sees the field's combined value). A
// Set on a field with no reducer folds nothing, so its encoded bytes go into both directly (setPlain); every other
// write is a one-field Changes, composed into changes and applied to state. On a field the graph declares a
// reducer for, a write is a contribution: Set adds to the field rather than replacing it, and two Sets in one task
// both count. Overwrite and Del replace instead.

// ready makes a zero Flow writable: every write goes into both state and changes.
func (f *Flow) ready() {
	if f.state.IsZero() {
		f.state, _ = NewState()
	}
	if f.changes.IsZero() {
		f.changes = NewChanges()
	}
}

// write applies a one-field Changes to the view and composes it into changes, leaving both untouched on error.
func (f *Flow) write(name string, one Changes) error {
	f.ready()
	prev, had := f.state.d[name]
	if err := f.state.Apply(one, f.reducers); err != nil {
		return errors.Trace(err)
	}
	if err := f.changes.Compose(one, f.reducers); err != nil {
		if had {
			f.state.d[name] = prev
		} else {
			delete(f.state.d, name)
		}
		return errors.Trace(err)
	}
	return nil
}

// Set writes a state field. Use this for complex types (structs, maps, etc.).
//
// On a field with no reducer, the value replaces the field, and nil deletes it. On a field the graph declares
// a reducer for, the value is a contribution, combined through the reducer with what the field already holds:
// under ReducerAppend, Set("messages", []any{m}) adds m to the history. Write only your contribution, never the
// value you read, which would add it again. A nil contributes nothing there; use Del or Overwrite to clear or
// replace such a field.
//
// It returns an error if the value cannot be marshalled to JSON (a NaN, an +Inf, a channel) or if the reducer
// cannot combine it with the field's value (a string under ReducerAdd). It panics if name is not a valid state
// field name (IsValidFieldName); the orchestrator turns the panic into a failure of the step.
func (f *Flow) Set(name string, value any) error {
	mustBeFieldName(name)
	if r := f.reducers[name]; r == "" || r == ReducerReplace {
		// No reducer, so nothing to fold: the bytes go straight into the view and the changes.
		raw, err := marshalWrite(name, value)
		if err != nil {
			return errors.Trace(err)
		}
		f.ready()
		f.changes.setPlain(name, raw)
		if isCleared(raw) {
			delete(f.state.d, name)
		} else {
			f.state.d[name] = raw
		}
		return nil
	}
	one := NewChanges()
	if err := one.Set(name, value, f.reducers); err != nil {
		return errors.Trace(err)
	}
	return f.write(name, one)
}

// mustSet is Set for the typed setters, which have no error to return. The panic is for a value the field's
// reducer cannot combine, or SetFloat's NaN or infinity, which does not marshal (an invalid name panics in Set
// itself) - either way the orchestrator turns it into a failure of the step, naming the field.
func (f *Flow) mustSet(name string, value any) {
	if err := f.Set(name, value); err != nil {
		panic(errors.New("state field %q: %v", name, err))
	}
}

// SetString writes a state string field, as Set does.
func (f *Flow) SetString(name string, value string) { f.mustSet(name, value) }

// SetStrings writes a state string slice field, as Set does.
func (f *Flow) SetStrings(name string, value []string) { f.mustSet(name, value) }

// SetInt writes a state int field, as Set does.
func (f *Flow) SetInt(name string, value int) { f.mustSet(name, value) }

// SetFloat writes a state float64 field, as Set does. It panics on NaN or an infinity, which JSON cannot
// represent.
func (f *Flow) SetFloat(name string, value float64) { f.mustSet(name, value) }

// SetBool writes a state bool field, as Set does.
func (f *Flow) SetBool(name string, value bool) { f.mustSet(name, value) }

// SetDuration writes a state time.Duration field as an integer number of milliseconds, rounded to the nearest
// one, as Set does. Every duration dwarf writes as JSON is in milliseconds, so a task in another language reads it
// as a plain integer, and a reducer or a when expression treats it as a number. Set stores a time.Duration as Go
// encodes it, in nanoseconds, which GetDuration does not read.
func (f *Flow) SetDuration(name string, value time.Duration) { f.mustSet(name, millisRounded(value)) }

// Overwrite replaces a state field with value, whatever reducer the field has; a nil value deletes it. Later
// Sets in the same task combine onto the new value. Use it to reset or trim a reduced field, such as a
// conversation history.
//
// Overwriting a reduced field is not allowed inside a fan-out branch, at any depth: the branches are parallel,
// and a reset in one of them has no defined meaning for the others. There it returns an error and records
// nothing, so a task that ignores the error keeps the field as it was. A field with no reducer can be overwritten
// anywhere. It also returns an error if the value cannot be marshalled, and panics, like Set, if name is not a
// valid state field name.
func (f *Flow) Overwrite(name string, value any) error {
	mustBeFieldName(name)
	if r := f.reducers[name]; f.inBranch && r != "" && r != ReducerReplace {
		return errors.New("workflow: cannot overwrite reduced field %q inside a fan-out branch", name)
	}
	one := NewChanges()
	if err := one.Overwrite(name, value); err != nil {
		return errors.Trace(err)
	}
	return f.write(name, one)
}

// Del deletes the listed state fields, whatever reducer they have: it is Overwrite with nil. Later reads in the
// task see each field as absent. Deleting a reduced field inside a fan-out branch fails the step, as Overwrite
// does. It panics, like Set, if a name is not a valid state field name.
func (f *Flow) Del(names ...string) {
	for _, n := range names {
		mustBeFieldName(n)
		one := NewChanges()
		if err := one.Overwrite(n, nil); err != nil {
			panic(errors.Trace(err))
		}
		_ = f.write(n, one) // a delete combines with nothing, so it cannot fail
	}
}

// Clear deletes every state field, as Del on each, so it panics if the state holds a name that is not a valid
// state field name. Useful at workflow boundaries or anywhere a task wants a blank slate before populating it.
func (f *Flow) Clear() {
	for _, k := range slices.Collect(f.state.Names()) {
		f.Del(k)
	}
}

// Snapshot captures an independent copy of the flow's current state (including any changes applied so far) -
// for instance, to pass a subgraph the flow's whole state as its input.
//
// The copy is independent of the flow, so writing to the flow afterwards does not disturb it. It does not
// materialize field values, so snapshotting a large carried payload costs a map entry rather than the payload.
func (f *Flow) Snapshot() State {
	return f.state.Clone()
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
in. The out argument is a pointer (a *struct or *map[string]any) into which the child's final state is unmarshaled
by JSON tag; pass nil to ignore the result. Using a typed struct reads only the fields you declare, with type safety.

On the first call (child not yet run) it arms the subgraph park with the child workflow URL and in and returns
yield=true; the task must return immediately.

On re-entry after the child terminates it unmarshals the child's final state into out, returns yield=false, and sets
err if the child failed. Does not re-arm on re-entry. Like Interrupt, it returns an error without arming if in cannot
be marshalled or this step already parked, and a 400 if workflowURL is not valid (IsValidURL).

A child that could not be started comes back the same way, with no child flow created and nothing in out: err
carries the host's error when the child's graph could not be loaded (a 503 when its source is unreachable), a 404
when the graph does not exist, or a 400 when it is invalid. Read the code with errors.StatusCode. Returning err takes
the task's onError transition, if the graph has one; flow.Retry runs the task again, and its next Subgraph call
starts a new child.

	var out ChildOut
	yield, err := flow.Subgraph(childURL, ChildIn{Value: value}, &out)
	if yield {
	    return nil // parked, child running
	}
	if errors.StatusCode(err) == http.StatusServiceUnavailable {
	    if yield, retryErr := flow.Retry(time.Second, 2.0, 30*time.Second, time.Hour); yield || retryErr != nil {
	        return retryErr // nil: spawn the child again later
	    }
	}
	if err != nil {
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
		if f.subgraphError != nil {
			// A copy, so a task that traces or annotates the error leaves the carrier's as delivered.
			delivered := *f.subgraphError
			return false, &delivered
		}
		return false, nil
	}
	// An empty URL is also how "no subgraph requested" reads (SubgraphRequested), so arming one would answer
	// yield=true for a request the engine never sees: the step would complete with no child.
	if !IsValidURL(workflowURL) {
		return false, errors.New("cannot start subgraph: invalid workflow URL %q: %s", workflowURL, nameRule, http.StatusBadRequest)
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

/*
Retry asks the orchestrator to run this task again with exponential backoff, starting the step over: a resolved
Interrupt asks again and a returned Subgraph runs a new child. To come back later and keep what the step resolved,
use Defer.

Retry carries no condition of its own - it is the single retry primitive, called inside whatever error branch the
task decides is retryable. Keeping the condition explicit at the call site avoids the "retry on every error" trap
(most errors - validation, bad input, business rejections - should not be retried):

	result, err := callExternalAPI(ctx)
	if isTransient(err) {
	    if yield, retryErr := flow.Retry(1*time.Second, 2.0, 30*time.Second, 1*time.Hour); yield || retryErr != nil {
	        return retryErr // nil: the retry is scheduled
	    }
	}
	if err != nil {
	    return err // not transient, or past the bound
	}
	flow.Set("result", result)

Retry returns yield=true while the retry is within its bound; the task returns nil and runs again after the delay.
It returns yield=false and a nil err once the bound is reached; the task returns its own error. It arms nothing and
returns an error in two cases, and the task returns that error:
  - The step runs after its flow was cancelled: the cancellation, which IsCancelled recognizes, so the step's onError
    handler receives it or the step ends cancelled.
  - An argument is invalid, with status 400: initialDelay and maxIntervalDelay must not be negative, and
    delayMultiplier must be a finite number that is not negative.

giveUpAfter bounds the total backoff: yield is true while the backoff delays of every attempt so far, the next one
included, add up to at most giveUpAfter. Pass giveUpAfter <= 0 for unlimited retry. The bound counts only the
delays Retry itself asks for, computed from this call's arguments and Attempt, never the clock: the engine cannot
promise when an attempt runs, and time a step spends queued behind higher-priority work, or running, must not use
up its retries. A Sleep is the task's own wait, whichever is called first, and is not counted either; a task that
wants its sleeps bounded keeps its own budget, for example in a state field, and gates Retry on it. With a zero
initialDelay the total stays zero and never reaches the bound, so bound immediate retries by Attempt: pass
giveUpAfter 0 and check flow.Attempt() at the call site.

The delay before attempt N is min(initialDelay * delayMultiplier^N, maxIntervalDelay); pass a zero initialDelay for
immediate retries, and a zero maxIntervalDelay for no per-interval cap. To hold the delay constant (e.g. honoring a
provider's Retry-After carried in initialDelay), pass delayMultiplier 1.0. Sleep, if also set, is added on top as a
floor.
*/
func (f *Flow) Retry(initialDelay time.Duration, delayMultiplier float64, maxIntervalDelay time.Duration, giveUpAfter time.Duration) (yield bool, err error) {
	yield, err = f.backoff("Retry", initialDelay, delayMultiplier, maxIntervalDelay, giveUpAfter, f.attempt)
	f.retry = yield
	return yield, err
}

/*
Defer asks the orchestrator to run this step again later, as it stands. Where Retry starts the step over, Defer
keeps what it resolved: the next run's Interrupt returns the same resume data, its Subgraph the same result of the
same child, and Attempt stays as it was. Either way the run's writes are kept and the next run adds to them.

Defer is for a step that cannot go on yet although nothing about it failed: a dependency rate-limits it after a
human has answered its Interrupt, say, or a host cannot reach the worker that runs the task.

	var approval Approval
	if yield, err := flow.Interrupt(request, &approval); yield || err != nil {
	    return err
	}
	err := charge(ctx, approval)
	if isRateLimited(err) {
	    if yield, deferErr := flow.Defer(1*time.Second, 2.0, 30*time.Second, 1*time.Hour); yield || deferErr != nil {
	        return deferErr // nil: later, with the same approval; a Retry would ask for it again
	    }
	}
	return err

Its results, arguments, backoff and bound are Retry's, the bound counted by Deferrals instead of Attempt: yield is
true while the backoff delays of every deferral so far, the next one included, add up to at most giveUpAfter. A
Retry resets Deferrals, and so does the step resolving an Interrupt or a Subgraph, so the bound covers the deferrals
since the step last started over or got its answer. A host waiting for an absent worker defers the step too, and
its waits and the task's own deferrals share the one count.
*/
func (f *Flow) Defer(initialDelay time.Duration, delayMultiplier float64, maxIntervalDelay time.Duration, giveUpAfter time.Duration) (yield bool, err error) {
	yield, err = f.backoff("Defer", initialDelay, delayMultiplier, maxIntervalDelay, giveUpAfter, f.deferrals)
	f.deferred = yield
	return yield, err
}

// backoff records the backoff of a Retry or a Defer, named by signal, whose n earlier runs have backed off, and
// reports whether it is within giveUpAfter. A cancelled flow or an invalid argument records nothing and returns the
// error.
func (f *Flow) backoff(signal string, initialDelay time.Duration, delayMultiplier float64, maxIntervalDelay time.Duration, giveUpAfter time.Duration, n int) (yield bool, err error) {
	if f.cancelledErr != nil {
		// A copy, so a task that traces or annotates the error leaves the carrier's as delivered.
		delivered := *f.cancelledErr
		return false, &delivered
	}
	switch {
	case initialDelay < 0:
		return false, errors.New("%s: initialDelay %v is negative", signal, initialDelay, http.StatusBadRequest)
	case maxIntervalDelay < 0:
		return false, errors.New("%s: maxIntervalDelay %v is negative", signal, maxIntervalDelay, http.StatusBadRequest)
	case !(delayMultiplier >= 0) || math.IsInf(delayMultiplier, 1):
		return false, errors.New("%s: delayMultiplier %v is not a finite number that is not negative", signal, delayMultiplier, http.StatusBadRequest)
	}
	if giveUpAfter > 0 && !backoffFits(initialDelay, delayMultiplier, maxIntervalDelay, n, giveUpAfter) {
		return false, nil
	}
	f.backoffInitialDelay = initialDelay
	f.backoffDelayMultiplier = delayMultiplier
	f.backoffMaxDelay = maxIntervalDelay
	return true, nil
}

// Sleep tells the orchestrator to wait for the given duration before the next execution. A non-positive
// duration is a no-op (no sleep is requested), not an error - "wait for no time" and "do not wait" are the
// same instruction, so a computed delay that lands at or below zero needs no guard at the call site.
func (f *Flow) Sleep(duration time.Duration) {
	f.sleepDuration = duration
}
