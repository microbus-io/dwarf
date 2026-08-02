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

// TestTaskProfile_ConstantUnlessBothHalvesAreSet pins that a profile only alternates when it has both a
// burst and a quiet half. Half a cycle is not a cycle, and silently treating it as one would change what
// every existing -task-delay arm measures.
func TestTaskProfile_ConstantUnlessBothHalvesAreSet(t *testing.T) {
	assert := testarossa.For(t)
	start := time.Now()

	for _, p := range []taskProfile{
		{delay: time.Second, start: start},                        // no cycle at all
		{delay: time.Second, on: 10 * time.Second, start: start},  // burst but no quiet half
		{delay: time.Second, off: 10 * time.Second, start: start}, // quiet half but no burst
	} {
		assert.False(p.bursting(), "on=%v off=%v is not a cycle", p.on, p.off)
		for _, at := range []time.Duration{0, 5 * time.Second, 15 * time.Second, time.Hour} {
			assert.Equal(time.Second, p.delayAt(start.Add(at)), "constant at +%v", at)
		}
	}
}

// TestTaskProfile_AlternatesOnItsCycle walks a full period plus a repeat, because the property that matters
// is that it CYCLES: a schedule that ran one burst and then stayed quiet would pass a single-period check
// and measure nothing after it.
func TestTaskProfile_AlternatesOnItsCycle(t *testing.T) {
	assert := testarossa.For(t)
	start := time.Now()
	p := taskProfile{delay: 2 * time.Second, on: 30 * time.Second, off: 90 * time.Second, start: start}
	assert.True(p.bursting())

	cases := []struct {
		at   time.Duration
		want time.Duration
	}{
		{0, 2 * time.Second},                 // the run opens in a burst
		{29 * time.Second, 2 * time.Second},  // last instant of it
		{30 * time.Second, 0},                // quiet begins exactly at the boundary
		{119 * time.Second, 0},               // ...and holds to the end of the period
		{120 * time.Second, 2 * time.Second}, // second cycle: the burst returns
		{150 * time.Second, 0},               // and alternates again
		{10 * time.Minute, 2 * time.Second},  // five periods on: still cycling
		{10*time.Minute + 30*time.Second, 0}, //
	}
	for _, c := range cases {
		assert.Equal(c.want, p.delayAt(start.Add(c.at)), "delay at +%v", c.at)
	}
}

// TestTaskProfile_BeforeTheAnchorIsABurst covers a task dispatched fractionally before the anchor - the
// engine is started before the profile's clock is read, so it is reachable. It must not return a NEGATIVE
// modulo's worth of nonsense; opening in a burst is the same thing every run does at t=0.
func TestTaskProfile_BeforeTheAnchorIsABurst(t *testing.T) {
	assert := testarossa.For(t)
	start := time.Now()
	p := taskProfile{delay: time.Second, on: time.Minute, off: time.Minute, start: start}
	assert.Equal(time.Second, p.delayAt(start.Add(-time.Millisecond)))
	assert.Equal(time.Second, p.delayAt(start.Add(-time.Hour)))
}
