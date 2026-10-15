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
	"time"

	"github.com/microbus-io/testarossa"
)

func TestDelayDist_Parse(t *testing.T) {
	assert := testarossa.For(t)

	dist, err := parseDelayDist(" 0.80:5ms, 0.15:200ms ,0.05:8s")
	if assert.NoError(err) {
		assert.Equal(delayDist{{0.80, 5 * time.Millisecond}, {0.15, 200 * time.Millisecond}, {0.05, 8 * time.Second}}, dist)
		assert.Equal(8*time.Second, dist.longest())
		assert.Equal(434*time.Millisecond, dist.mean().Round(time.Millisecond)) // 4 + 30 + 400
	}

	// Seven classes whose decimal weights do not sum to exactly 1 in floating point.
	_, err = parseDelayDist("0.459:1ms,0.15:2ms,0.20:3ms,0.10:4ms,0.05:5ms,0.04:6ms,0.001:7ms")
	assert.NoError(err)

	dist, err = parseDelayDist("")
	assert.NoError(err)
	assert.Len(dist, 0)

	for _, bad := range []string{
		"0.8:5ms,0.15:200ms,0.5:8s", // a typo that shifts the mix must not normalize away
		"0.5:5ms",                   // short of 1
		"1:5ms,0:8s",                // a zero-weight class
		"1.5:5ms,-0.5:8s",           // a negative weight
		"1:-5ms",                    // a negative duration
		"1:5",                       // no unit
		"5ms",                       // no weight
		"x:5ms",
		"NaN:5ms",
	} {
		_, err = parseDelayDist(bad)
		assert.Error(err, "%q", bad)
	}
}

// TestDelayDist_SampleBoundaries pins the class edges: each class owns [cumulative, cumulative+weight), so
// a uniform u lands in each with exactly its weight.
func TestDelayDist_SampleBoundaries(t *testing.T) {
	assert := testarossa.For(t)
	dist := delayDist{{0.80, 5 * time.Millisecond}, {0.15, 200 * time.Millisecond}, {0.05, 8 * time.Second}}

	cases := []struct {
		u    float64
		want time.Duration
	}{
		{0, 5 * time.Millisecond},
		{0.7999, 5 * time.Millisecond},
		{0.80, 200 * time.Millisecond},
		{0.9499, 200 * time.Millisecond},
		{0.9501, 8 * time.Second},
		{0.999999, 8 * time.Second},
		{1, 8 * time.Second}, // past the last edge, as rounding can leave it
	}
	for _, c := range cases {
		assert.Equal(c.want, dist.sample(c.u), "u=%v", c.u)
	}
}
