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

// TestFlowOptions_JSONTimeBudgetIsMilliseconds pins FlowOptions' JSON: TimeBudget is "timeBudgetMillis", whole
// milliseconds with a positive fraction rounded up so it never reads back as the default, and every other field
// keeps its own key.
func TestFlowOptions_JSONTimeBudgetIsMilliseconds(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	opts := FlowOptions{Priority: 2, FairnessKey: "t1", TimeBudget: 1500 * time.Millisecond, DeleteOnCompletion: true}
	b, err := json.Marshal(opts)
	assert.NoError(err)
	assert.Equal(`{"priority":2,"fairnessKey":"t1","deleteOnCompletion":true,"timeBudgetMillis":1500}`, string(b))
	var back FlowOptions
	assert.NoError(json.Unmarshal(b, &back))
	assert.Equal(opts, back)

	b, err = json.Marshal(&FlowOptions{TimeBudget: 300 * time.Microsecond})
	assert.NoError(err)
	assert.Equal(`{"timeBudgetMillis":1}`, string(b), "a positive fraction rounds up")
	b, err = json.Marshal(FlowOptions{})
	assert.NoError(err)
	assert.Equal(`{}`, string(b), "zero is omitted")

	back = FlowOptions{TimeBudget: time.Minute}
	assert.NoError(json.Unmarshal([]byte(`{"priority":1}`), &back))
	assert.Equal(time.Minute, back.TimeBudget, "an absent key leaves the value")
	assert.Equal(1, back.Priority)
	assert.Error(json.Unmarshal([]byte(`{"timeBudgetMillis":1.5}`), &back), "not a whole number")
}
