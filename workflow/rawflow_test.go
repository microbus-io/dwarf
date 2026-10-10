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
	"time"

	"github.com/microbus-io/testarossa"
)

// TestRawFlow_RetryDelay pins the backoff the engine applies after a Retry: exponential in the attempt, capped,
// and zero when Retry was not called.
func TestRawFlow_RetryDelay(t *testing.T) {
	assert := testarossa.For(t)

	rf := NewRawFlow()
	assert.Equal(time.Duration(0), rf.RetryDelay(3))
	assert.True(rf.Retry(time.Second, 2, 5*time.Second, 0))
	assert.Equal(time.Second, rf.RetryDelay(0))
	assert.Equal(4*time.Second, rf.RetryDelay(2))
	assert.Equal(5*time.Second, rf.RetryDelay(3))
	assert.Equal(5*time.Second, rf.RetryDelay(1000))
}
