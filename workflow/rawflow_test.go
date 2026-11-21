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
	"time"

	"github.com/microbus-io/testarossa"
)

// yieldOf is the yield of a Retry or a Defer that must not return an error.
func yieldOf(yield bool, err error) bool {
	if err != nil {
		panic(err)
	}
	return yield
}

// TestRawFlow_DeferDelay pins the backoff the engine applies after a Defer: Retry's, in the number of deferrals,
// and zero when Defer was not called - a Retry included.
func TestRawFlow_DeferDelay(t *testing.T) {
	assert := testarossa.For(t)

	rf := NewRawFlow()
	assert.True(yieldOf(rf.Retry(time.Second, 2, 5*time.Second, 0)))
	assert.Equal(time.Duration(0), rf.DeferDelay(3))

	rf = NewRawFlow()
	assert.True(yieldOf(rf.Defer(time.Second, 2, 5*time.Second, 0)))
	assert.Equal(time.Duration(0), rf.RetryDelay(0))
	assert.Equal(time.Second, rf.DeferDelay(0))
	assert.Equal(4*time.Second, rf.DeferDelay(2))
	assert.Equal(5*time.Second, rf.DeferDelay(3))
}

// TestRawFlow_RetryDelay pins the backoff the engine applies after a Retry: exponential in the attempt, capped,
// and zero when Retry was not called.
func TestRawFlow_RetryDelay(t *testing.T) {
	assert := testarossa.For(t)

	rf := NewRawFlow()
	assert.Equal(time.Duration(0), rf.RetryDelay(3))
	assert.True(yieldOf(rf.Retry(time.Second, 2, 5*time.Second, 0)))
	assert.Equal(time.Second, rf.RetryDelay(0))
	assert.Equal(4*time.Second, rf.RetryDelay(2))
	assert.Equal(5*time.Second, rf.RetryDelay(3))
	assert.Equal(5*time.Second, rf.RetryDelay(1000))

	// A multiplier below 1 decays a delay that starts above the cap: min(10s x 0.1^N, 1s). Stopping at the cap returned
	// 1s at every attempt.
	rf = NewRawFlow()
	assert.True(yieldOf(rf.Retry(10*time.Second, 0.1, time.Second, 0)))
	assert.Equal(time.Second, rf.RetryDelay(0))
	assert.Equal(time.Second, rf.RetryDelay(1))
	assert.Equal(100*time.Millisecond, rf.RetryDelay(2))
	assert.Equal(10*time.Millisecond, rf.RetryDelay(3))
	// It decays to zero and stops there, however many attempts.
	assert.Equal(time.Duration(0), backoffDelay(time.Second, 0.5, 0, 1_000_000))
}

// TestBackoffDelay_Saturates pins that a delay past the largest Duration saturates rather than converting a float64
// out of int64's range, which is implementation-defined and came out negative on amd64; and that a negative
// initialDelay comes back as it is, for the engine to refuse.
func TestBackoffDelay_Saturates(t *testing.T) {
	assert := testarossa.For(t)

	const largest = time.Duration(math.MaxInt64)
	assert.Equal(largest, backoffDelay(time.Second, 1e10, 0, 1))
	assert.Equal(largest, backoffDelay(time.Second, 1e300, 0, 3))
	assert.Equal(largest, backoffDelay(time.Second, math.Inf(1), 0, 1))
	assert.Equal(largest, backoffDelay(largest, 1, 0, 0))
	assert.Equal(time.Hour, backoffDelay(time.Second, 1e300, time.Hour, 3))
	assert.Equal(time.Hour, backoffDelay(2*time.Hour, 2, time.Hour, 0))
	assert.Equal(time.Duration(0), backoffDelay(0, math.Inf(1), 0, 3))
	assert.Equal(time.Second, backoffDelay(time.Second, math.NaN(), 0, 3))
	assert.Equal(-time.Second, backoffDelay(-time.Second, 1e300, 0, 3))
}
