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

package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// TestShardSummary_JSONLatencyIsMilliseconds pins ShardSummary's JSON: Latency is "latencyMillis", whole
// milliseconds rounded to the nearest one.
func TestShardSummary_JSONLatencyIsMilliseconds(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	b, err := json.Marshal(ShardSummary{Index: 2, Latency: 12600 * time.Microsecond})
	assert.NoError(err)
	assert.Equal(`{"index":2,"latencyMillis":13}`, string(b))
	var back ShardSummary
	assert.NoError(json.Unmarshal(b, &back))
	assert.Equal(ShardSummary{Index: 2, Latency: 13 * time.Millisecond}, back)

	b, err = json.Marshal([]ShardSummary{{Index: 1, Error: "down", Latency: 300 * time.Microsecond}})
	assert.NoError(err)
	assert.Equal(`[{"index":1,"error":"down"}]`, string(b), "under half a millisecond is omitted as zero")
}
