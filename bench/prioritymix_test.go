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

package main

import (
	"testing"

	"github.com/microbus-io/testarossa"
)

// TestParsePriorityMix pins the -priority-mix grammar: levels sorted best first, an empty flag meaning every flow
// at the engine's default, and the mistakes that would otherwise run a day at the wrong mix rejected up front.
func TestParsePriorityMix(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	mix, err := parsePriorityMix(" 3:0.55, 1:0.03,5:0.1 ,2:0.12,4:0.2")
	assert.NoError(err)
	assert.Equal(priorityMix{{1, 0.03}, {2, 0.12}, {3, 0.55}, {4, 0.2}, {5, 0.1}}, mix, "sorted best band first")

	mix, err = parsePriorityMix("")
	assert.NoError(err)
	assert.Equal(priorityMix{{realDefaultPriority, 1}}, mix)

	for _, bad := range []string{"3", "0:1", "6:1", "x:1", "3:0", "3:-1", "3:0.5,3:0.5", "3:0.5,4:0.4"} {
		_, err := parsePriorityMix(bad)
		assert.Error(err, bad)
	}
}

// TestPriorityMixSample pins the draw: each level takes its weight's share of the unit interval, in order.
func TestPriorityMixSample(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	mix, err := parsePriorityMix("1:0.1,3:0.6,5:0.3")
	assert.NoError(err)
	counts := map[int]int{}
	const n = 10000
	for i := range n {
		counts[mix.sample((float64(i)+0.5)/n)]++
	}
	assert.Equal(map[int]int{1: 1000, 3: 6000, 5: 3000}, counts)
	assert.Equal(5, mix.sample(0.9999999999))
}
