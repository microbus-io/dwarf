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

	t.Run("rate-limited classifies and carries its cause", func(t *testing.T) {
		err := ErrRateLimited(errors.New("rate limited"), "quota")
		cause, ok := IsRateLimited(err)
		assert.True(ok)
		assert.Equal("quota", cause)
		_, ok = IsUnavailable(err)
		assert.False(ok, "a rate-limited error is not unavailable")
	})

	t.Run("unavailable classifies and carries its cause", func(t *testing.T) {
		err := ErrUnavailable(errors.New("down"), "unavailable")
		cause, ok := IsUnavailable(err)
		assert.True(ok)
		assert.Equal("unavailable", cause)
		_, ok = IsRateLimited(err)
		assert.False(ok, "an unavailable error is not rate-limited")
	})

	t.Run("undecorated error is neither", func(t *testing.T) {
		err := errors.New("ordinary failure")
		_, ok := IsRateLimited(err)
		assert.False(ok)
		_, ok = IsUnavailable(err)
		assert.False(ok)
	})

	t.Run("nil error is allowed and still classifies", func(t *testing.T) {
		err := ErrRateLimited(nil, "self")
		cause, ok := IsRateLimited(err)
		assert.True(ok)
		assert.Equal("self", cause)
		assert.Equal("rate limited", err.Error())
	})

	t.Run("the wrapped error is preserved through Unwrap", func(t *testing.T) {
		sentinel := errors.New("sentinel")
		err := ErrUnavailable(fmt.Errorf("context: %w", sentinel), "overloaded")
		assert.True(errors.Is(err, sentinel), "errors.Is should see through the disposition wrapper")
		cause, ok := IsUnavailable(err)
		assert.True(ok)
		assert.Equal("overloaded", cause)
	})

	t.Run("classification sees through an outer wrap", func(t *testing.T) {
		err := fmt.Errorf("outer: %w", ErrRateLimited(errors.New("inner"), "c"))
		cause, ok := IsRateLimited(err)
		assert.True(ok)
		assert.Equal("c", cause)
	})
}
