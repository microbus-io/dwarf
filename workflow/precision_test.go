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
	"testing"

	"github.com/microbus-io/testarossa"
)

// An integer beyond 2^53 does not survive state's JSON round trip - it comes back rounded - so every
// authoring path rejects it rather than store a value the flow can never read back. The typed setters
// panic (no error to return; the orchestrator turns the panic into a clean step failure) and the paths
// that already return an error report it.
func TestFlow_SetRejectsIntegerBeyondFloat64Precision(t *testing.T) {
	const snowflake = 1234567890123456789 // a 64-bit id; also the shape of time.Now().UnixNano()

	t.Run("SetInt panics", func(t *testing.T) {
		assert := testarossa.For(t)
		f := NewFlow()
		func() {
			defer func() {
				r := recover()
				if !assert.NotNil(r, "SetInt must panic rather than silently round") {
					return
				}
				assert.Contains(r.(error).Error(), "orderID")
				assert.Contains(r.(error).Error(), "carry it as a string")
			}()
			f.SetInt("orderID", snowflake)
		}()
		// Nothing was recorded: the flow does not carry a half-written field.
		assert.False(f.Has("orderID"))
	})

	t.Run("SetDuration panics beyond ~104 days", func(t *testing.T) {
		assert := testarossa.For(t)
		f := NewFlow()
		defer func() { assert.NotNil(recover()) }()
		f.SetDuration("ttl", 1<<53+1) // nanoseconds
	})

	t.Run("Set returns the error", func(t *testing.T) {
		assert := testarossa.For(t)
		f := NewFlow()
		err := f.Set("order", map[string]any{"id": int64(snowflake)})
		if !assert.Error(err) {
			return
		}
		assert.Contains(err.Error(), `"order"`) // the field being set
		assert.Contains(err.Error(), "'id'")    // and the path within it, not just the top-level key
		assert.False(f.Has("order"))
	})

	t.Run("SetChanges returns the error", func(t *testing.T) {
		assert := testarossa.For(t)
		f := NewFlow()
		snap := f.Snapshot()
		err := f.SetChanges(struct {
			OrderID int64 `json:"orderID"`
		}{OrderID: snowflake}, snap)
		if !assert.Error(err) {
			return
		}
		assert.Contains(err.Error(), "orderID")
		assert.False(f.Has("orderID"))
	})

	t.Run("Interrupt payload returns the error", func(t *testing.T) {
		assert := testarossa.For(t)
		f := NewFlow()
		yield, err := f.Interrupt(map[string]any{"orderID": int64(snowflake)}, nil)
		assert.Error(err)
		assert.False(yield)
		_, armed := f.InterruptRequested()
		assert.False(armed, "a rejected payload must not park the step")
	})

	t.Run("Subgraph input returns the error", func(t *testing.T) {
		assert := testarossa.For(t)
		f := NewFlow()
		yield, err := f.Subgraph("some/graph", map[string]any{"orderID": int64(snowflake)}, nil)
		assert.Error(err)
		assert.False(yield)
		_, _, armed := f.SubgraphRequested()
		assert.False(armed, "a rejected input must not park the step")
	})
}

// The guard is on the exactly-unrepresentable integer, not on large values as such: everything a
// float64 carries exactly still goes through, including the 2^53 boundary itself and any float.
func TestFlow_SetAcceptsWhatFloat64CarriesExactly(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()

	f.SetInt("boundary", 1<<53)
	assert.Equal(1<<53, f.GetInt("boundary"))

	f.SetInt("millis", 1752459999999) // UnixMilli: ~1.7e12, three orders of magnitude clear
	assert.Equal(1752459999999, f.GetInt("millis"))

	f.SetFloat("huge", 1e300) // float-domain: exact round trip at any magnitude
	assert.Equal(1e300, f.GetFloat("huge"))

	f.SetString("orderID", "1234567890123456789") // the prescribed workaround
	assert.Equal("1234567890123456789", f.GetString("orderID"))

	assert.NoError(f.Set("order", map[string]any{"total": 19.99, "lines": []int{1, 2, 3}}))
}

// toStateMap round-trips EVERY payload through JSON, including a map that could have been passed through
// as-is. The short-circuit it replaced forfeited three things at once, and all three are pinned here.
func TestFlow_PayloadIsValidatedCopiedAndUniform(t *testing.T) {
	t.Run("a map payload is validated, exactly like a struct one", func(t *testing.T) {
		assert := testarossa.For(t)

		// A NaN cannot be marshalled. Before, this was rejected from a struct but ACCEPTED from a map, and
		// only surfaced later as an opaque step failure from inside the orchestrator.
		f := NewFlow()
		yield, err := f.Interrupt(map[string]any{"score": math.NaN()}, nil)
		assert.Error(err)
		assert.False(yield)
		_, armed := f.InterruptRequested()
		assert.False(armed, "a rejected payload must not arm the interrupt")

		// Same value, struct shape: same outcome.
		f2 := NewFlow()
		_, err = f2.Interrupt(struct {
			Score float64 `json:"score"`
		}{Score: math.Inf(1)}, nil)
		assert.Error(err)

		// And the storability rules apply to a map payload too (see CheckStorable).
		f3 := NewFlow()
		_, err = f3.Subgraph("some/graph", map[string]any{"id": int64(1) << 54}, nil)
		assert.Error(err)
		_, _, armed = f3.SubgraphRequested()
		assert.False(armed)
	})

	t.Run("the payload is copied, not aliased", func(t *testing.T) {
		assert := testarossa.For(t)

		// The caller's map is live. Without the copy, mutating it after the call would edit the payload the
		// orchestrator is about to persist, from under it.
		payload := map[string]any{"question": "approve?"}
		f := NewFlow()
		yield, err := f.Interrupt(payload, nil)
		assert.NoError(err)
		assert.True(yield)

		payload["question"] = "MUTATED"
		payload["injected"] = true

		got, armed := f.InterruptRequested()
		assert.True(armed)
		assert.Equal("approve?", got["question"])
		_, injected := got["injected"]
		assert.False(injected)
	})
}
