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
	"errors"
	"fmt"
	"testing"

	"github.com/microbus-io/testarossa"
)

func TestDisposition(t *testing.T) {
	assert := testarossa.For(t)

	t.Run("backpressure classifies and carries its cause", func(t *testing.T) {
		err := ErrBackpressure(errors.New("rate limited"), "quota")
		cause, ok := IsBackpressure(err)
		assert.True(ok)
		assert.Equal("quota", cause)
		_, ok = IsBreakerTrip(err)
		assert.False(ok, "a backpressure error is not a breaker trip")
	})

	t.Run("breaker trip classifies and carries its cause", func(t *testing.T) {
		err := ErrBreakerTrip(errors.New("down"), "unavailable")
		cause, ok := IsBreakerTrip(err)
		assert.True(ok)
		assert.Equal("unavailable", cause)
		_, ok = IsBackpressure(err)
		assert.False(ok, "a breaker trip is not backpressure")
	})

	t.Run("undecorated error is neither", func(t *testing.T) {
		err := errors.New("ordinary failure")
		_, ok := IsBackpressure(err)
		assert.False(ok)
		_, ok = IsBreakerTrip(err)
		assert.False(ok)
	})

	t.Run("nil error is allowed and still classifies", func(t *testing.T) {
		err := ErrBackpressure(nil, "self")
		cause, ok := IsBackpressure(err)
		assert.True(ok)
		assert.Equal("self", cause)
		assert.Equal("backpressure", err.Error())
	})

	t.Run("the wrapped error is preserved through Unwrap", func(t *testing.T) {
		sentinel := errors.New("sentinel")
		err := ErrBreakerTrip(fmt.Errorf("context: %w", sentinel), "overloaded")
		assert.True(errors.Is(err, sentinel), "errors.Is should see through the disposition wrapper")
		cause, ok := IsBreakerTrip(err)
		assert.True(ok)
		assert.Equal("overloaded", cause)
	})

	t.Run("classification sees through an outer wrap", func(t *testing.T) {
		err := fmt.Errorf("outer: %w", ErrBackpressure(errors.New("inner"), "c"))
		cause, ok := IsBackpressure(err)
		assert.True(ok)
		assert.Equal("c", cause)
	})
}
