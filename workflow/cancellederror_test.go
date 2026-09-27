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
	"testing"

	"github.com/microbus-io/dwarf/internal/cancelmarker"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// The engine builds the marker and this package detects it from its own copy of the constants, so the test
// drives one through the other - including the JSON round trip onErr takes - and fails if the copies drift.
func TestIsCancelled(t *testing.T) {
	assert := testarossa.For(t)
	assert.True(IsCancelled(cancelmarker.New("reason")))
	pct := cancelmarker.New("50% off")
	assert.True(IsCancelled(pct), "a %% in the operator's reason must not consume the marker")
	assert.Equal("flow cancelled: 50% off", pct.Error())
	assert.False(IsCancelled(errors.New("flow cancelled: reason")), "detection is by marker, never by message")
	assert.False(IsCancelled(nil))
	var onErr *errors.TracedError
	assert.False(IsCancelled(onErr), "a nil *TracedError - a host handler's typed onErr argument - is not a cancellation")

	s, _ := NewState()
	assert.NoError(s.Set("onErr", errors.Convert(cancelmarker.New("reason"))))
	var te errors.TracedError
	_, err := s.Get("onErr", &te)
	assert.NoError(err)
	assert.True(IsCancelled(&te))
}
