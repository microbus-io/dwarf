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
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

func TestFlow_GetSetString(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("name", "Alice")
	assert.NoError(err)
	assert.Equal("Alice", f.GetString("name"))
}

func TestFlow_GetSetInt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("count", 42)
	assert.NoError(err)
	assert.Equal(42, f.GetInt("count"))
}

func TestFlow_GetSetFloat(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("score", 3.14)
	assert.NoError(err)
	assert.Equal(3.14, f.GetFloat("score"))
}

func TestFlow_GetSetBool(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("valid", true)
	assert.NoError(err)
	assert.True(f.GetBool("valid"))
}

func TestFlow_GetSetStrings(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	err := f.Set("timeout", 5*time.Second)
	assert.NoError(err)
	assert.Equal(5*time.Second, f.GetDuration("timeout"))
}

func TestFlow_GetComplex(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	type Order struct {
		ID    string `json:"id"`
		Total int    `json:"total"`
	}
	f := NewFlow()
	err := f.Set("order", &Order{ID: "abc", Total: 100})
	assert.NoError(err)
	var got Order
	err = f.Get("order", &got)
	assert.NoError(err)
	assert.Equal("abc", got.ID)
	assert.Equal(100, got.Total)
}

func TestFlow_GetMissing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	assert.Equal("", f.GetString("missing"))
	assert.Equal(0, f.GetInt("missing"))
}

func TestFlow_SetTracksChanges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	f.Set("a", "1")
	f.Set("b", "2")
	assert.Equal(2, len(f.changes))
	assert.Equal(`"1"`, string(f.changes["a"].(json.RawMessage)))
}

func TestFlow_ParseStateAndSetChanges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	f.Set("name", "Alice")
	f.Set("score", 10)
	f.Set("extra", "untouched")
	f.changes = make(map[string]any) // reset changes from Set calls
	snap := f.Snapshot()
	var view struct {
		Name  string `json:"name"`
		Score int    `json:"score"`
	}
	err := f.ParseState(&view)
	assert.NoError(err)
	assert.Equal("Alice", view.Name)
	assert.Equal(10, view.Score)

	// Modify only score
	view.Score = 25
	err = f.SetChanges(view, snap)
	assert.NoError(err)

	// Only score should be in changes
	assert.Equal(1, len(f.changes))
	assert.Equal("25", string(f.changes["score"].(json.RawMessage)))
	// Extra field should be untouched
	assert.Equal(`"untouched"`, string(f.state["extra"].(json.RawMessage)))
}

func TestFlow_SetChangesNoChanges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	f.Set("name", "Alice")
	f.changes = make(map[string]any) // reset changes from Set calls
	snap := f.Snapshot()
	var view struct {
		Name string `json:"name"`
	}
	err := f.ParseState(&view)
	assert.NoError(err)
	// No modifications
	err = f.SetChanges(view, snap)
	assert.NoError(err)
	assert.Equal(0, len(f.changes))
}

func TestFlow_Has(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	assert.False(f.Has("missing"))
	f.Set("name", "Alice")
	assert.True(f.Has("name"))
}

func TestFlow_Delete(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	f := NewFlow()
	f.SetString("name", "Alice")
	f.SetInt("score", 42)
	f.SetBool("active", true)
	f.changes = make(map[string]any) // reset changes from typed setters

	f.Delete("name", "score")
	// State reads see the deleted fields as absent.
	assert.False(f.Has("name"))
	assert.False(f.Has("score"))
	assert.Equal("", f.GetString("name"))
	assert.Equal(0, f.GetInt("score"))
	// Changes records JSON null for each deleted key.
	for _, k := range []string{"name", "score"} {
		raw, ok := f.changes[k].(json.RawMessage)
		assert.True(ok)
		assert.Equal("null", string(raw))
	}
	// Unlisted field is unaffected.
	assert.True(f.Has("active"))
	assert.Equal(true, f.GetBool("active"))
}

func TestFlow_Clear(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	f := NewFlow()
	f.SetString("name", "Alice")
	f.SetInt("score", 42)
	f.changes = make(map[string]any)

	f.Clear()
	// Every field reads as absent.
	assert.False(f.Has("name"))
	assert.False(f.Has("score"))
	// Every prior field has a null contribution in changes.
	assert.Equal(2, len(f.changes))
	for _, k := range []string{"name", "score"} {
		raw, ok := f.changes[k].(json.RawMessage)
		assert.True(ok)
		assert.Equal("null", string(raw))
	}
}

func TestFlow_Transform(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	f := NewFlow()
	f.SetString("parentVarA", "alpha")
	f.SetInt("parentVarB", 7)
	f.SetString("scratch", "drop me")
	f.changes = make(map[string]any)

	f.Transform("subInput1", "parentVarA", "subInput2", "parentVarB")

	// New names hold the captured values.
	assert.Equal("alpha", f.GetString("subInput1"))
	assert.Equal(7, f.GetInt("subInput2"))
	// Old names and uninvolved fields are gone.
	assert.False(f.Has("parentVarA"))
	assert.False(f.Has("parentVarB"))
	assert.False(f.Has("scratch"))
	// Changes: new keys carry their values, dropped old keys carry null.
	assert.Equal(`"alpha"`, string(f.changes["subInput1"].(json.RawMessage)))
	assert.Equal("7", string(f.changes["subInput2"].(json.RawMessage)))
	for _, k := range []string{"parentVarA", "parentVarB", "scratch"} {
		raw, ok := f.changes[k].(json.RawMessage)
		assert.True(ok)
		assert.Equal("null", string(raw))
	}
}

func TestFlow_TransformSkipsMissingAndNull(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	f := NewFlow()
	f.SetString("present", "value")
	f.state["alreadyNull"] = json.RawMessage("null")
	f.changes = make(map[string]any)

	f.Transform("kept", "present", "skipped1", "absent", "skipped2", "alreadyNull")

	assert.True(f.Has("kept"))
	assert.False(f.Has("skipped1"))
	assert.False(f.Has("skipped2"))
}

func TestFlow_TransformPanicsOnOddArgs(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	f := NewFlow()
	defer func() {
		r := recover()
		assert.True(r != nil)
	}()
	f.Transform("only-one")
}

func TestFlow_GetTreatsJSONNullAsAbsent(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	f := NewFlow()
	f.state["x"] = json.RawMessage("null")
	f.state["y"] = nil
	assert.False(f.Has("x"))
	assert.False(f.Has("y"))
	assert.Equal("", f.GetString("x"))
	assert.Equal(0, f.GetInt("y"))

	// ParseState skips null-valued fields rather than overwriting the target's zero.
	var view struct {
		X string `json:"x"`
		Y int    `json:"y"`
	}
	view.X = "preset"
	view.Y = 99
	err := f.ParseState(&view)
	assert.NoError(err)
	assert.Equal("preset", view.X)
	assert.Equal(99, view.Y)
}

func TestFlow_Goto(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	f.Goto("next-task")
	assert.Equal("next-task", f.gotoNext)
}

func TestFlow_Interrupt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	f.Interrupt(map[string]any{"request": "ssn"})
	assert.True(f.interrupt)
	assert.Equal("ssn", f.interruptPayload["request"])
}

func TestFlow_SingleParkGuard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Subgraph armed this dispatch blocks a subsequent interrupt.
	f := NewFlow()
	_, yield, err := f.Subgraph("https://x/wf", nil)
	assert.True(yield)
	assert.NoError(err)
	_, yield, err = f.Interrupt(map[string]any{"request": "ssn"})
	assert.False(yield)
	assert.Error(err)
	assert.False(f.interrupt)

	// Interrupt armed this dispatch blocks a subsequent subgraph.
	f = NewFlow()
	_, yield, err = f.Interrupt(map[string]any{"request": "ssn"})
	assert.True(yield)
	assert.NoError(err)
	_, yield, err = f.Subgraph("https://x/wf", nil)
	assert.False(yield)
	assert.Error(err)
	assert.Equal("", f.subgraphWorkflow)

	// A resolved interrupt slot blocks arming a subgraph on re-entry.
	f = NewFlow()
	f.interruptDone = true
	_, yield, err = f.Subgraph("https://x/wf", nil)
	assert.False(yield)
	assert.Error(err)
	assert.Equal("", f.subgraphWorkflow)

	// A resolved subgraph slot blocks arming an interrupt on re-entry.
	f = NewFlow()
	f.subgraphDone = true
	_, yield, err = f.Interrupt(map[string]any{"request": "ssn"})
	assert.False(yield)
	assert.Error(err)
	assert.False(f.interrupt)

	// A second interrupt this dispatch is rejected and does not overwrite the first payload.
	f = NewFlow()
	_, yield, err = f.Interrupt(map[string]any{"request": "first"})
	assert.True(yield)
	assert.NoError(err)
	_, yield, err = f.Interrupt(map[string]any{"request": "second"})
	assert.False(yield)
	assert.Error(err)
	assert.Equal("first", f.interruptPayload["request"])

	// A second subgraph this dispatch is rejected and does not overwrite the first workflow URL.
	f = NewFlow()
	_, yield, err = f.Subgraph("https://x/first", nil)
	assert.True(yield)
	assert.NoError(err)
	_, yield, err = f.Subgraph("https://x/second", nil)
	assert.False(yield)
	assert.Error(err)
	assert.Equal("https://x/first", f.subgraphWorkflow)
}

func TestFlow_Retry(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Attempt 0: should retry
	f := NewFlow()
	f.attempt = 0
	ok := f.Retry(3, time.Second, 2.0, 10*time.Second)
	assert.True(ok)
	assert.True(f.retry)
	maxAttempts, initialDelay, multiplier, maxDelay, requested := f.RetryRequested()
	assert.True(requested)
	assert.Equal(3, maxAttempts)
	assert.Equal(time.Second, initialDelay)
	assert.Equal(2.0, multiplier)
	assert.Equal(10*time.Second, maxDelay)

	// Attempt 2: should retry (still under max of 3)
	f2 := NewFlow()
	f2.attempt = 2
	ok = f2.Retry(3, time.Second, 2.0, 10*time.Second)
	assert.True(ok)
	assert.True(f2.retry)

	// Attempt 3: exhausted
	f3 := NewFlow()
	f3.attempt = 3
	ok = f3.Retry(3, time.Second, 2.0, 10*time.Second)
	assert.False(ok)
	assert.False(f3.retry)
}

func TestFlow_Sleep(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	f := NewFlow()
	f.Sleep(10 * time.Second)
	assert.Equal(10*time.Second, f.sleepDuration)
}

func TestFlow_MarshalUnmarshal(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	original := NewFlow()
	original.Set("name", "Alice")
	original.Goto("next")
	original.Retry(5, time.Second, 2.0, 30*time.Second)
	original.Sleep(5 * time.Second)
	original.Interrupt(map[string]any{"request": "ssn"})
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
	assert.Equal("ssn", restored.interruptPayload["request"])
	assert.Equal(2, restored.attempt)
	assert.Equal(5, restored.backoffMaxAttempts)
	assert.Equal(time.Second, restored.backoffInitialDelay)
	assert.Equal(2.0, restored.backoffDelayMultiplier)
	assert.Equal(30*time.Second, restored.backoffMaxDelay)
}

