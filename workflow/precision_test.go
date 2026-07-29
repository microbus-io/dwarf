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

// Everything a float64 carries exactly round-trips through state, including the 2^53 boundary itself and
// any float. State number precision beyond ±2^53 is not enforced (a large id is carried as a string);
// this pins the values that do go through cleanly.
func TestFlow_SetAcceptsWhatFloat64CarriesExactly(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()

	f.SetInt("boundary", 1<<53)
	assert.Equal(1<<53, f.GetInt("boundary"))

	f.SetInt("millis", 1752459999999) // UnixMilli: ~1.7e12, three orders of magnitude clear
	assert.Equal(1752459999999, f.GetInt("millis"))

	f.SetFloat("huge", 1e300) // float-domain: exact round trip at any magnitude
	assert.Equal(1e300, f.GetFloat("huge"))

	f.SetString("orderID", "1234567890123456789") // the prescribed workaround for a >2^53 id
	assert.Equal("1234567890123456789", f.GetString("orderID"))

	assert.NoError(f.Set("order", map[string]any{"total": 19.99, "lines": []int{1, 2, 3}}))
}

// toStateMap round-trips EVERY payload through JSON, including a map that could have been passed through
// as-is. The short-circuit it replaced forfeited two things, and both are pinned here.
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
		assert.Equal("approve?", got.Value("question"))
		_, injected := got.Lookup("injected")
		assert.False(injected)
	})
}
