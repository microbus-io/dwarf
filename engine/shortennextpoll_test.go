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
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// TestShortenNextPoll_PastDeadlineReplace is the seam-free regression tripwire for the sleep/retry wedge fixed
// by the "nextPoll already in the past -> replace" branch (the `|| e.nextPoll.Before(time.Now())` clause of the
// shortenNextPoll predicate). The wedge: a worker arms a FUTURE not_before and calls shortenNextPoll(T) while
// nextPoll is transiently in the past (a just-fired deadline the timer is mid-poll on). A strictly-lower-only
// update (tm.Before(nextPoll) alone) drops that write - a future T is not "before" a past deadline - and the
// in-flight pollPendingSteps then clobbers nextPoll with its far maxPollInterval default, parking the timer for
// minutes while a due step waits. The past-branch is what keeps the worker's future wake from being lost.
//
// This pins the predicate directly on a bare (never-started) engine, so there is no background timer/poll
// goroutine racing nextPoll and no timing hammer - the branch is exercised deterministically by construction.
// (nudgeTimer is safe here: wakeTimer is nil until Startup, so its non-blocking select takes the default.)
//
// Scope note: this tripwire locks the *decision logic* the wedge fix hinges on. The full end-to-end concurrent
// interleaving - an in-flight pollPendingSteps' far-default write racing a worker's shortenNextPoll around the
// shared read-modify-write - is a timer-internal race the current pre-transaction checkpoint/fault seams can't
// express; pinning it deterministically would need a checkpoint inside timerLoop/pollPendingSteps around the
// nextPoll RMW. That seam is consciously deferred (low regression risk, small documented branch), so this
// predicate-level pin stands in for it as the cheap, deterministic guard against re-simplifying the branch away.
func TestShortenNextPoll_PastDeadlineReplace(t *testing.T) {
	assert := testarossa.For(t)
	e := NewEngine() // bare engine: no Startup, so no timer/poll goroutine mutates nextPoll under us

	// 1. Normal lower: an earlier tm replaces a later nextPoll.
	future1h := time.Now().Add(time.Hour)
	e.nextPoll = future1h
	sooner := time.Now().Add(time.Minute)
	e.shortenNextPoll(sooner)
	assert.True(e.nextPoll.Equal(sooner), "an earlier tm must lower nextPoll")

	// 2. No raise: a later tm must NOT push a still-future nextPoll out.
	e.nextPoll = future1h
	later := time.Now().Add(2 * time.Hour)
	e.shortenNextPoll(later)
	assert.True(e.nextPoll.Equal(future1h), "a later tm must not raise a still-future nextPoll")

	// 3. Past-deadline replace (the wedge-fix branch): nextPoll lies in the past (a fired deadline the timer is
	// mid-poll on), and a worker arms a FUTURE wake. Even though the future tm is NOT before the past nextPoll,
	// the past-branch must replace it - otherwise the future wake is dropped and the in-flight poll's far
	// default wedges the timer. A strictly-lower-only predicate would leave nextPoll at the stale past value
	// and fail this assertion.
	e.nextPoll = time.Now().Add(-time.Hour) // a fired, past deadline
	futureWake := time.Now().Add(30 * time.Minute)
	e.shortenNextPoll(futureWake)
	assert.True(e.nextPoll.Equal(futureWake),
		"a past nextPoll must be replaced by a future wake request (the sleep/retry wedge fix), got %s", e.nextPoll)
}
