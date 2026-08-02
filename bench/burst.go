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

import "time"

// taskProfile is the per-task delay the host applies, as a function of WHEN the task runs.
//
// A CONSTANT delay cannot see either of the two things worth knowing about a grown crew, and that is the
// whole reason this type exists. The crew grows while tasks are long and only comes back down once they are
// not, so an arm that holds task duration fixed for its entire window grows to a plateau and reports the
// plateau - whatever the engine would have done next. Both questions need load to change WITHIN one run:
//
//   - does the crew come back DOWN after a burst, or is it a ratchet that holds the worst load ever seen,
//   - and does throughput recover to what the same rig does with no delay at all.
//
// The quiet half is not idle: flows keep arriving at the same rate, tasks simply stop sleeping. That is
// what makes the comparison a controlled one - only the exec term moves.
type taskProfile struct {
	// delay is the long-task sleep applied during a burst.
	delay time.Duration
	// on and off are the burst and quiet halves of one cycle. Either at zero means no alternation, and the
	// delay is constant for the run - the historical behaviour, and still the default.
	on, off time.Duration
	// start anchors the cycle. Every worker derives its phase from this, so there is nothing to synchronize.
	start time.Time
}

// delayAt is how long a task starting at now should sleep.
//
// Pure in (now - start), which is what keeps this free of a driver goroutine, a shared mutable delay, and
// any synchronization around it: every worker computes the same answer from the same clock, and adding a
// worker cannot perturb the schedule. A profile that alternated by having something flip a variable would
// have all three, and its phase would depend on scheduler latency under exactly the load being measured.
func (p taskProfile) delayAt(now time.Time) time.Duration {
	if p.on <= 0 || p.off <= 0 {
		return p.delay
	}
	elapsed := now.Sub(p.start)
	if elapsed < 0 {
		return p.delay // before the anchor: the run opens in a burst
	}
	if elapsed%(p.on+p.off) < p.on {
		return p.delay
	}
	return 0
}

// bursting reports whether this profile alternates, for labelling and for the artifact.
func (p taskProfile) bursting() bool { return p.delay > 0 && p.on > 0 && p.off > 0 }
