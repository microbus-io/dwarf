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
	"math"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestFlow_GetSetString(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("name", "Alice")
	assert.NoError(err)
	assert.Equal("Alice", f.GetString("name"))
}

func TestFlow_GetSetInt(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("count", 42)
	assert.NoError(err)
	assert.Equal(42, f.GetInt("count"))
}

func TestFlow_GetSetFloat(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("score", 3.14)
	assert.NoError(err)
	assert.Equal(3.14, f.GetFloat("score"))
}

func TestFlow_GetSetBool(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("valid", true)
	assert.NoError(err)
	assert.True(f.GetBool("valid"))
}

func TestFlow_GetSetStrings(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("tags", []string{"a", "b"})
	assert.NoError(err)
	got := f.GetStrings("tags")
	assert.Equal(2, len(got))
	assert.Equal("a", got[0])
	assert.Equal("b", got[1])
}

func TestFlow_GetSetDuration(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	f.SetDuration("timeout", 5*time.Second)
	assert.Equal(5*time.Second, f.GetDuration("timeout"))
	assert.Equal(5000, f.GetInt("timeout"), "stored as whole milliseconds")
}

func TestFlow_GetComplex(t *testing.T) {
	assert := testarossa.For(t)
	type Order struct {
		ID    string `json:"id"`
		Total int    `json:"total"`
	}
	f := NewFlow()
	err := f.Set("order", &Order{ID: "abc", Total: 100})
	assert.NoError(err)
	var got Order
	_, err = f.Get("order", &got)
	assert.NoError(err)
	assert.Equal("abc", got.ID)
	assert.Equal(100, got.Total)
}

func TestFlow_GetMissing(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	assert.Equal("", f.GetString("missing"))
	assert.Equal(0, f.GetInt("missing"))
}

func TestFlow_SetTracksChanges(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	f.Set("a", "1")
	f.Set("b", "2")
	assert.Equal(2, f.changes.Contributions().Len())
	assert.Equal("1", stateVal(f.changes.Contributions(), "a")) // stored decoded, not as raw JSON bytes
}

func TestFlow_Has(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	assert.False(f.Has("missing"))
	f.Set("name", "Alice")
	assert.True(f.Has("name"))
}

func TestFlow_Delete(t *testing.T) {
	assert := testarossa.For(t)

	f := NewFlow()
	f.SetString("name", "Alice")
	f.SetInt("score", 42)
	f.SetBool("active", true)
	f.changes = NewChanges() // reset changes from typed setters

	f.Del("name", "score")
	// State reads see the deleted fields as absent.
	assert.False(f.Has("name"))
	assert.False(f.Has("score"))
	assert.Equal("", f.GetString("name"))
	assert.Equal(0, f.GetInt("score"))
	// Changes records a cleared tombstone for each deleted key.
	for _, k := range []string{"name", "score"} {
		assert.True(f.changes.IsDeleted(k), "changes must record %q as a delete, not drop it", k)
		assert.False(f.changes.Contributions().Has(k), "a delete holds no value")
	}
	// Unlisted field is unaffected.
	assert.True(f.Has("active"))
	assert.Equal(true, f.GetBool("active"))
}

func TestFlow_Clear(t *testing.T) {
	assert := testarossa.For(t)

	f := NewFlow()
	f.SetString("name", "Alice")
	f.SetInt("score", 42)
	f.changes = NewChanges()

	f.Clear()
	// Every field reads as absent.
	assert.False(f.Has("name"))
	assert.False(f.Has("score"))
	// Every prior field is recorded as a delete in changes.
	assert.Len(slices.Collect(f.changes.Names()), 2)
	for _, k := range []string{"name", "score"} {
		assert.True(f.changes.IsDeleted(k), "changes must record %q as a delete, not drop it", k)
		assert.False(f.changes.Contributions().Has(k), "a delete holds no value")
	}
}

func TestFlow_GetTreatsJSONNullAsAbsent(t *testing.T) {
	assert := testarossa.For(t)

	f := NewFlow()
	f.state.d["x"] = nil // a cleared (null) slot: state stores a decoded JSON null as Go nil
	f.state.d["y"] = nil
	assert.False(f.Has("x"))
	assert.False(f.Has("y"))
	assert.Equal("", f.GetString("x"))
	assert.Equal(0, f.GetInt("y"))

	// Parse skips null-valued fields rather than overwriting the target's zero.
	var view struct {
		X string `json:"x"`
		Y int    `json:"y"`
	}
	view.X = "preset"
	view.Y = 99
	err := f.Parse(&view)
	assert.NoError(err)
	assert.Equal("preset", view.X)
	assert.Equal(99, view.Y)
}

func TestFlow_Goto(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	f.Goto("next-task")
	assert.Equal("next-task", f.gotoNext)
}

func TestFlow_Interrupt(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	f.Interrupt(map[string]any{"request": "ssn"}, nil)
	assert.True(f.interrupt)
	assert.Equal("ssn", stateVal(f.interruptPayload, "request"))
}

func TestFlow_SingleParkGuard(t *testing.T) {
	assert := testarossa.For(t)

	// Subgraph armed this dispatch blocks a subsequent interrupt.
	f := NewFlow()
	yield, err := f.Subgraph("https://x/wf", nil, nil)
	assert.True(yield)
	assert.NoError(err)
	yield, err = f.Interrupt(map[string]any{"request": "ssn"}, nil)
	assert.False(yield)
	assert.Error(err)
	assert.False(f.interrupt)

	// Interrupt armed this dispatch blocks a subsequent subgraph.
	f = NewFlow()
	yield, err = f.Interrupt(map[string]any{"request": "ssn"}, nil)
	assert.True(yield)
	assert.NoError(err)
	yield, err = f.Subgraph("https://x/wf", nil, nil)
	assert.False(yield)
	assert.Error(err)
	assert.Equal("", f.subgraphURL)

	// A resolved interrupt slot blocks arming a subgraph on re-entry.
	f = NewFlow()
	f.interruptDone = true
	yield, err = f.Subgraph("https://x/wf", nil, nil)
	assert.False(yield)
	assert.Error(err)
	assert.Equal("", f.subgraphURL)

	// A resolved subgraph slot blocks arming an interrupt on re-entry.
	f = NewFlow()
	f.subgraphDone = true
	yield, err = f.Interrupt(map[string]any{"request": "ssn"}, nil)
	assert.False(yield)
	assert.Error(err)
	assert.False(f.interrupt)

	// A second interrupt this dispatch is rejected and does not overwrite the first payload.
	f = NewFlow()
	yield, err = f.Interrupt(map[string]any{"request": "first"}, nil)
	assert.True(yield)
	assert.NoError(err)
	yield, err = f.Interrupt(map[string]any{"request": "second"}, nil)
	assert.False(yield)
	assert.Error(err)
	assert.Equal("first", stateVal(f.interruptPayload, "request"))

	// A second subgraph this dispatch is rejected and does not overwrite the first workflow URL.
	f = NewFlow()
	yield, err = f.Subgraph("https://x/first", nil, nil)
	assert.True(yield)
	assert.NoError(err)
	yield, err = f.Subgraph("https://x/second", nil, nil)
	assert.False(yield)
	assert.Error(err)
	assert.Equal("https://x/first", f.subgraphURL)
}

// TestFlow_SubgraphRejectsAnEmptyURL pins that Subgraph refuses an empty workflow URL without arming anything.
// The empty URL is also what "no subgraph requested" reads as, so arming one returned yield=true for a request
// the engine never saw: the task returned, and the engine completed the step and moved on with no child.
func TestFlow_SubgraphRejectsAnEmptyURL(t *testing.T) {
	assert := testarossa.For(t)
	rf := NewRawFlow()
	f := &rf.Flow
	yield, err := f.Subgraph("", map[string]any{"x": 1}, nil)
	assert.False(yield)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	_, _, requested := rf.SubgraphRequested()
	assert.False(requested)

	// Nothing was armed, so a valid call still can be.
	yield, err = f.Subgraph("https://x/wf", nil, nil)
	assert.True(yield)
	assert.NoError(err)
}

// TestFlow_SubgraphRejectsAPaddedURL pins that Subgraph refuses a workflow URL that begins or ends with whitespace,
// as Create does, without arming anything.
func TestFlow_SubgraphRejectsAPaddedURL(t *testing.T) {
	assert := testarossa.For(t)
	for _, url := range []string{" https://x/wf", "https://x/wf ", "https://x/wf\n"} {
		rf := NewRawFlow()
		f := &rf.Flow
		yield, err := f.Subgraph(url, nil, nil)
		assert.False(yield, "%q", url)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%q", url)
		_, _, requested := rf.SubgraphRequested()
		assert.False(requested, "%q", url)
	}
}

func TestFlow_Retry(t *testing.T) {
	assert := testarossa.For(t)

	// Within the horizon: should retry, and the backoff shape is recorded.
	rf := NewRawFlow()
	f := &rf.Flow
	f.stepCreatedAt = time.Now()
	ok := f.Retry(time.Second, 2.0, 10*time.Second, time.Hour)
	assert.True(ok)
	assert.True(f.retry)
	initialDelay, multiplier, maxDelay, requested := rf.RetryRequested()
	assert.True(requested)
	assert.Equal(time.Second, initialDelay)
	assert.Equal(2.0, multiplier)
	assert.Equal(10*time.Second, maxDelay)

	// giveUpAfter <= 0 means unlimited: retries regardless of how old the step is.
	f2 := NewFlow()
	f2.stepCreatedAt = time.Now().Add(-72 * time.Hour)
	ok = f2.Retry(time.Second, 2.0, 10*time.Second, 0)
	assert.True(ok)
	assert.True(f2.retry)

	// The bound is the total backoff, not the clock: a step created two hours ago still retries under a one-hour
	// bound while its delays (1s, 2s, 4s, 8s, then 10s each) sum within it, and stops on the attempt that would pass
	// it: through attempt n the total is 15s + (n-3) x 10s, 3595s at attempt 361 and 3605s at attempt 362.
	f3 := NewFlow()
	f3.stepCreatedAt = time.Now().Add(-2 * time.Hour)
	f3.attempt = 0
	assert.True(f3.Retry(time.Second, 2.0, 10*time.Second, time.Hour), "the clock does not count")
	f3.attempt = 361
	assert.True(f3.Retry(time.Second, 2.0, 10*time.Second, time.Hour))
	f3.attempt = 362
	assert.False(f3.Retry(time.Second, 2.0, 10*time.Second, time.Hour))
	assert.False(f3.retry)

	// The next delay alone overshoots the bound: give up now rather than park a doomed wait.
	f4 := NewFlow()
	ok = f4.Retry(time.Hour, 1.0, 0, time.Minute)
	assert.False(ok)
	assert.False(f4.retry)

	// A zero initialDelay never adds to the total, so it never reaches the bound.
	f4b := NewFlow()
	f4b.attempt = 1_000_000
	assert.True(f4b.Retry(0, 2.0, 0, time.Minute))

	// A delay that saturates still gives up: added to the running total, it would overflow to a negative.
	f5 := NewFlow()
	f5.attempt = 1
	ok = f5.Retry(time.Second, 1e10, 0, time.Hour)
	assert.False(ok)
	assert.False(f5.retry)

	// Arguments outside the allowed values arm nothing.
	for _, args := range []struct {
		initialDelay time.Duration
		multiplier   float64
		maxDelay     time.Duration
	}{
		{-time.Second, 2, 0},
		{time.Second, 2, -time.Second},
		{time.Second, -2, 0},
		{time.Second, math.NaN(), 0},
		{time.Second, math.Inf(1), 0},
	} {
		f6 := NewFlow()
		ok = f6.Retry(args.initialDelay, args.multiplier, args.maxDelay, 0)
		assert.False(ok, args)
		assert.False(f6.retry, args)
	}
}

func TestFlow_Sleep(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()
	f.Sleep(10 * time.Second)
	assert.Equal(10*time.Second, f.sleepDuration)
}

func TestFlow_MarshalUnmarshal(t *testing.T) {
	assert := testarossa.For(t)
	original := NewFlow()
	original.Set("name", "Alice")
	original.Goto("next")
	original.Retry(time.Second, 2.0, 30*time.Second, 0)
	original.Sleep(5 * time.Second)
	original.Interrupt(map[string]any{"request": "ssn"}, nil)
	original.attempt = 2

	data, err := json.Marshal(original)
	assert.NoError(err)

	restored := NewFlow()
	err = json.Unmarshal(data, restored)
	assert.NoError(err)

	assert.Equal("Alice", restored.GetString("name"))
	assert.Equal("next", restored.gotoNext)
	assert.True(restored.retry)
	assert.Equal(5*time.Second, restored.sleepDuration)
	assert.True(restored.interrupt)
	assert.Equal("ssn", stateVal(restored.interruptPayload, "request"))
	assert.Equal(2, restored.attempt)
	assert.Equal(time.Second, restored.backoffInitialDelay)
	assert.Equal(2.0, restored.backoffDelayMultiplier)
	assert.Equal(30*time.Second, restored.backoffMaxDelay)
}

// TestFlow_TypedGetterPanicsOnTypeMismatch pins the disposition of a present-but-mistyped state field. The
// single-return getters have no channel to report one through, and returning the zero value silently is how
// a task ends up acting on a value it never had (a GetInt of a 1.5 retry delay reading as 0 is a hot loop
// against the downstream, not a slow retry). Absent and cleared fields are NOT a mismatch - an optional
// field must still read as its zero value.
func TestFlow_TypedGetterPanicsOnTypeMismatch(t *testing.T) {
	assert := testarossa.For(t)

	f := NewFlow()
	assert.NoError(f.Set("retryAfter", 1.5))
	assert.NoError(f.Set("name", "Alice"))
	assert.NoError(f.Set("cleared", nil))

	mustPanic := func(label string, read func()) {
		defer func() {
			assert.NotNil(recover(), "%s: expected a panic on the type mismatch", label)
		}()
		read()
	}
	mustPanic("GetInt of a fractional number", func() { f.GetInt("retryAfter") })
	mustPanic("GetInt of a string", func() { f.GetInt("name") })
	mustPanic("GetBool of a string", func() { f.GetBool("name") })
	mustPanic("GetStrings of a string", func() { f.GetStrings("name") })

	// Absent, cleared, and well-typed fields are untouched.
	assert.Equal(0, f.GetInt("missing"))
	assert.Equal("", f.GetString("cleared"))
	assert.Equal("Alice", f.GetString("name"))
	assert.Equal(1.5, f.GetFloat("retryAfter"))

	// Get reports the mismatch as an error instead - the escape hatch for a task that wants to handle it.
	var n int
	_, err := f.Get("retryAfter", &n)
	assert.Error(err)
}

// TestFlow_GetReportsPresence pins Get's three outcomes as its godoc states them: a present field is
// assigned and reports ok, an absent or cleared one reports !ok and leaves a default in place, and a type
// mismatch is returned as an error rather than panicking.
func TestFlow_GetReportsPresence(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	f.SetInt("limit", 25)
	f.SetString("name", "ada")
	f.Del("gone")

	limit := 10
	ok, err := f.Get("limit", &limit)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal(25, limit)

	fallback := 10
	ok, err = f.Get("missing", &fallback)
	assert.NoError(err)
	assert.False(ok, "an absent field is not reported present")
	assert.Equal(10, fallback, "and the default survives")
	ok, err = f.Get("gone", &fallback)
	assert.NoError(err)
	assert.False(ok, "a cleared field reads as absent")
	assert.Equal(10, fallback)

	var n int
	ok, err = f.Get("name", &n)
	assert.Error(err, "a mismatch is an error, not a panic")
	assert.False(ok)
}

// TestFlow_DecodedWithoutChangesTakesWrites pins that a Flow decoded from a wire form missing its state or
// changes still accepts the setters, which write to both.
func TestFlow_DecodedWithoutChangesTakesWrites(t *testing.T) {
	assert := testarossa.For(t)

	for _, wire := range []string{`{"v":1}`, `{"v":1,"state":{"a":1}}`, `{"v":1,"changes":{"a":1}}`} {
		var f Flow
		if !assert.NoError(json.Unmarshal([]byte(wire), &f), wire) {
			continue
		}
		f.SetString("b", "v")
		assert.Equal("v", f.GetString("b"), wire)
	}
}

// TestFlow_ZeroValueTakesEveryWrite pins that every writer works on a zero Flow and records the write in both
// the view and the changes - not only the ones that happened to initialize it.
func TestFlow_ZeroValueTakesEveryWrite(t *testing.T) {
	assert := testarossa.For(t)

	writes := map[string]func(f *Flow) error{
		"Set":       func(f *Flow) error { return f.Set("x", 1) },
		"SetString": func(f *Flow) error { f.SetString("x", "v"); return nil },
		"Overwrite": func(f *Flow) error { return f.Overwrite("x", 1) },
		"reduced Set": func(f *Flow) error {
			f.reducers = map[string]Reducer{"x": ReducerAppend}
			return f.Set("x", []any{1})
		},
	}
	for name, write := range writes {
		var f Flow
		if assert.NoError(write(&f), name) {
			assert.True(f.Has("x"), name)
			assert.True(f.changes.Mentions("x"), name)
		}
	}

	var f Flow
	f.Del("x")
	assert.False(f.Has("x"))
	assert.True(f.changes.IsDeleted("x"))
}

func TestFlow_ReducersAndChangesCrossTheWire(t *testing.T) {
	assert := testarossa.For(t)

	f := reducedFlow(true)
	assert.NoError(f.Set("log", []string{"b"}))
	f.Del("note")

	// A task reached remotely receives the reducers and the branch flag, and returns its changes with the
	// overwrite marked by the suffix.
	data, err := json.Marshal(f)
	assert.NoError(err)
	assert.Contains(string(data), `"reducers":{"log":"append"}`)
	assert.Contains(string(data), `"changes":{"log":["b"],"note!":null}`)
	assert.Contains(string(data), `"inBranch":true`)
	var remote Flow
	assert.NoError(json.Unmarshal(data, &remote))
	assert.Equal(map[string]Reducer{"log": ReducerAppend}, remote.reducers)
	assert.True(remote.changes.IsDeleted("note"))
	assert.NoError(remote.Set("log", []string{"c"}))
	assert.Equal([]string{"a", "b", "c"}, remote.GetStrings("log"))
	assert.Equal([]string{"b", "c"}, remote.changes.Contributions().GetStrings("log"))
}

// reducedFlow returns a carrier whose input state holds log=[a] on ReducerAppend and note="n" with no reducer,
// as the orchestrator hands it to a task.
func reducedFlow(inBranch bool) *RawFlow {
	state, _ := NewState("log", []string{"a"}, "note", "n")
	f := NewRawFlow()
	f.SetRawState(state)
	f.SetReducers(map[string]Reducer{"log": ReducerAppend})
	f.SetInBranch(inBranch)
	return f
}

func TestFlow_SetOnAReducedFieldContributes(t *testing.T) {
	assert := testarossa.For(t)

	f := reducedFlow(false)
	assert.NoError(f.Set("log", []string{"b"}))
	assert.NoError(f.Set("log", "c")) // a single value under append is one element
	// The task reads the combined value; its changes hold its contribution alone.
	assert.Equal([]string{"a", "b", "c"}, f.GetStrings("log"))
	assert.Equal([]string{"b", "c"}, f.RawChanges().Contributions().GetStrings("log"))
	assert.False(f.RawChanges().IsOverwritten("log"))

	// A nil contributes nothing to a reduced field, and deletes a plain one.
	var none []string
	assert.NoError(f.Set("log", none))
	assert.Equal([]string{"a", "b", "c"}, f.GetStrings("log"))
	assert.NoError(f.Set("note", nil))
	assert.False(f.Has("note"))
	assert.True(f.RawChanges().IsDeleted("note"))

	// A value the reducer cannot combine is an error, and changes nothing.
	add := NewRawFlow()
	st, _ := NewState("n", 1)
	add.SetRawState(st)
	add.SetReducers(map[string]Reducer{"n": ReducerAdd})
	assert.Error(add.Set("n", "x"))
	assert.Equal(1, add.GetInt("n"))
	assert.False(add.RawChanges().Mentions("n"))
}

// TestFlow_InvalidNamePanicsInEveryWriter pins that every Flow writer panics on a name that is not a valid state
// field name, the overwrite and addition suffixes included, and records nothing: a returned error the task ignores
// would drop the write silently. The orchestrator turns the panic into a failure of the step.
func TestFlow_InvalidNamePanicsInEveryWriter(t *testing.T) {
	assert := testarossa.For(t)

	panics := func(write func()) (panicked bool) {
		defer func() { panicked = recover() != nil }()
		write()
		return false
	}
	for _, name := range []string{"x!", "x+", "a.b", "1x", "-x", "a b", "é", ""} {
		var f Flow
		writers := map[string]func(){
			"Set":         func() { _ = f.Set(name, 1) },
			"Overwrite":   func() { _ = f.Overwrite(name, 1) },
			"Del":         func() { f.Del(name) },
			"SetString":   func() { f.SetString(name, "v") },
			"SetStrings":  func() { f.SetStrings(name, []string{"v"}) },
			"SetInt":      func() { f.SetInt(name, 1) },
			"SetFloat":    func() { f.SetFloat(name, 1.5) },
			"SetBool":     func() { f.SetBool(name, true) },
			"SetDuration": func() { f.SetDuration(name, time.Second) },
		}
		for writer, write := range writers {
			assert.True(panics(write), "%s(%q)", writer, name)
		}
		assert.Equal(0, f.changes.Len(), "%q", name)
	}

	// A valid name, hyphen included, writes.
	var f Flow
	assert.NoError(f.Set("line-item", 1))
	assert.Equal(1, f.GetInt("line-item"))

	// Clear deletes every field of the state, so a state holding an invalid name panics there too.
	bad := NewRawFlow()
	st, _ := NewState(map[string]any{"ok": 1, "a.b": 2})
	bad.SetRawState(st)
	assert.True(panics(func() { bad.Clear() }), "Clear of a state holding an invalid name")
}

func TestFlow_OverwriteAndDelThenSet(t *testing.T) {
	assert := testarossa.For(t)

	// Del then Sets: the field starts fresh, and the step's delta replaces it.
	f := reducedFlow(false)
	f.Del("log")
	assert.False(f.Has("log"))
	assert.NoError(f.Set("log", []string{"d1"}))
	assert.NoError(f.Set("log", []string{"d2"}))
	assert.Equal([]string{"d1", "d2"}, f.GetStrings("log"))
	changes := f.RawChanges()
	assert.True(changes.IsOverwritten("log"))
	assert.Equal([]string{"d1", "d2"}, changes.Overwrites().GetStrings("log"))

	// Overwrite then Set: the Set folds onto the overwritten value.
	f = reducedFlow(false)
	assert.NoError(f.Overwrite("log", []string{"x"}))
	assert.NoError(f.Set("log", []string{"y"}))
	assert.Equal([]string{"x", "y"}, f.GetStrings("log"))
	assert.True(f.RawChanges().IsOverwritten("log"))

	// Applying the delta to the input reproduces the task's view.
	input, _ := NewState("log", []string{"a"}, "note", "n")
	assert.NoError(input.Apply(f.RawChanges(), map[string]Reducer{"log": ReducerAppend}))
	assert.Equal([]string{"x", "y"}, input.GetStrings("log"))
}

func TestFlow_OverwriteOfAReducedFieldInABranch(t *testing.T) {
	assert := testarossa.For(t)

	f := reducedFlow(true)
	assert.Error(f.Overwrite("log", []string{"x"}))
	assert.Equal([]string{"a"}, f.GetStrings("log"), "a refused overwrite changes nothing")
	assert.NoError(f.Overwrite("note", "m"), "a field with no reducer may be overwritten anywhere")
	assert.NoError(f.Set("log", []string{"b"}), "a contribution is what a branch writes")

	// The flag survives the wire, so a remote task refuses the same call.
	data, err := json.Marshal(&f.Flow)
	assert.NoError(err)
	var remote Flow
	assert.NoError(json.Unmarshal(data, &remote))
	assert.Error(remote.Overwrite("log", nil))
}

// TestFlow_RetryHorizonIgnoresSleep pins that giveUpAfter bounds Retry's own backoff and not the run's Sleep,
// whichever is called first: a Sleep is the task's explicit wait, and the engine adds it on top of the backoff. A task
// that wants its sleeps bounded keeps its own budget.
func TestFlow_RetryHorizonIgnoresSleep(t *testing.T) {
	assert := testarossa.For(t)

	before := NewRawFlow()
	before.Sleep(time.Hour)
	assert.True(before.Retry(time.Millisecond, 1, 0, time.Minute))
	assert.Equal(time.Hour, before.SleepRequested())

	after := NewRawFlow()
	assert.True(after.Retry(time.Millisecond, 1, 0, time.Minute))
	after.Sleep(time.Hour)
	_, _, _, requested := after.RetryRequested()
	assert.True(requested, "a Sleep after Retry does not withdraw the retry")
}

func TestFlow_RetryBudgetBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		initial    time.Duration
		multiplier float64
		cap        time.Duration
		attempt    int
		total      time.Duration
	}{
		{"constant", 10 * time.Millisecond, 1, 0, 100_000, 100_001 * 10 * time.Millisecond},
		{"disabled multiplier", 10 * time.Millisecond, 0, 0, 3, 40 * time.Millisecond},
		{"growing capped", 3 * time.Nanosecond, 1.5, 10 * time.Nanosecond, 4, 33 * time.Nanosecond},
		{"shrinking capped", 10 * time.Second, 0.1, time.Second, 2, 2100 * time.Millisecond},
		{"fractional decay", 3 * time.Nanosecond, 0.75, 0, math.MaxInt, 7 * time.Nanosecond},
		{"maximum constant count", time.Nanosecond, 1, 0, math.MaxInt - 1, time.Duration(math.MaxInt)},
		{"maximum capped count", time.Hour, 2, time.Nanosecond, math.MaxInt - 1, time.Duration(math.MaxInt)},
		{"duration ceiling", time.Duration(math.MaxInt64), 1, 0, 0, time.Duration(math.MaxInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := NewRawFlow()
			f.SetAttempt(tc.attempt)
			if !f.Retry(tc.initial, tc.multiplier, tc.cap, tc.total) {
				t.Fatal("retry refused at the exact cumulative budget")
			}
			if f.Retry(tc.initial, tc.multiplier, tc.cap, tc.total-time.Nanosecond) {
				t.Fatal("retry accepted one nanosecond beyond the cumulative budget")
			}
		})
	}

	f := NewRawFlow()
	f.SetAttempt(math.MaxInt)
	if !f.Retry(0, 2, 0, time.Nanosecond) {
		t.Fatal("zero delay spent a positive budget")
	}
	if f.Retry(time.Nanosecond, 1, 0, time.Duration(math.MaxInt)) {
		t.Fatal("attempt count overflow admitted a retry beyond the budget")
	}
	f.SetAttempt(1)
	if f.Retry(time.Second, 1e300, 0, time.Duration(math.MaxInt64)) {
		t.Fatal("saturated delay overflow admitted a retry beyond the budget")
	}
}
