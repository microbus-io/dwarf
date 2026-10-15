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

package memlimit

import (
	"math"
	"runtime/debug"
	"testing"

	"github.com/microbus-io/testarossa"
)

// TestMemlimit_GoMemLimitWins pins the order: a soft memory limit set on the runtime is the answer whatever
// the system reports. Not parallel: it sets a process-wide limit.
func TestMemlimit_GoMemLimitWins(t *testing.T) {
	assert := testarossa.For(t)

	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	debug.SetMemoryLimit(3 << 30)
	limit, source := Detect()
	assert.Equal(int64(3<<30), limit)
	assert.Equal(SourceGoMemLimit, source)

	// Unset, the system answers: some positive limit on Linux, nothing elsewhere.
	debug.SetMemoryLimit(math.MaxInt64)
	limit, source = Detect()
	assert.NotEqual(SourceGoMemLimit, source)
	if source == SourceNone {
		assert.Equal(int64(0), limit)
	} else {
		assert.True(limit > 0)
	}
}
