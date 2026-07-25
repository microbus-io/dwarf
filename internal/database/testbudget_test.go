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

package database

import (
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
	"golang.org/x/sync/semaphore"
)

// TestBudget_BlocksThenReleases is the deadlock-critical case: once the budget is exhausted, the next
// acquirer BLOCKS until a release frees a slot, then proceeds. This is what keeps the sum of live test
// pools under the server cap - a non-blocking or leaky implementation would let the (cap+1)th engine open
// a connection the server rejects.
func TestBudget_BlocksThenReleases(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	sem := semaphore.NewWeighted(4)

	release := acquireFrom(sem, 4, 4) // take the whole budget

	// A second acquire cannot proceed while the budget is fully held.
	got := make(chan func(), 1)
	go func() { got <- acquireFrom(sem, 4, 1) }()
	select {
	case <-got:
		assert.True(false, "acquire succeeded while the budget was fully reserved")
		return
	case <-time.After(100 * time.Millisecond):
		// still blocked, as required
	}

	release() // free the budget

	select {
	case release2 := <-got:
		release2() // the blocked acquire proceeded
	case <-time.After(2 * time.Second):
		assert.True(false, "acquire did not proceed after the budget was released")
		return
	}
}

// TestBudget_ClampsOversizeRequest pins that a request larger than the whole budget is clamped rather than
// blocking forever (semaphore.Acquire(n > size) with a never-cancelled context never returns). A single
// engine bigger than the server just serializes against everything else.
func TestBudget_ClampsOversizeRequest(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	sem := semaphore.NewWeighted(4)

	done := make(chan func(), 1)
	go func() { done <- acquireFrom(sem, 4, 999) }() // asks for far more than the budget
	select {
	case release := <-done:
		release()
	case <-time.After(2 * time.Second):
		assert.True(false, "an oversize request blocked forever instead of clamping to the budget")
		return
	}

	// Clamped to 4, so after release the full budget is free again: a fresh full acquire succeeds at once.
	got := make(chan struct{}, 1)
	go func() { acquireFrom(sem, 4, 4)(); got <- struct{}{} }()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		assert.True(false, "the clamped request over-reserved: the budget was not fully released")
		return
	}
}

// TestBudget_ReleaseIsIdempotent pins that calling release twice frees the weight ONCE. A double-release
// would inflate the semaphore's capacity and let live pools exceed the cap. Open's failure path and Close
// can both fire the release, so this must hold.
func TestBudget_ReleaseIsIdempotent(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	sem := semaphore.NewWeighted(2)

	release := acquireFrom(sem, 2, 2) // whole budget held
	release()
	release() // second call must be a no-op, not a second +2

	// If the double-release had leaked capacity, this would grab MORE than the real budget. Take the full
	// budget, then prove a further acquire blocks (the capacity is still exactly 2).
	held := acquireFrom(sem, 2, 2)
	defer held()
	blocked := make(chan struct{}, 1)
	go func() { acquireFrom(sem, 2, 1)(); blocked <- struct{}{} }()
	select {
	case <-blocked:
		assert.True(false, "budget capacity leaked: an acquire past the cap succeeded after a double-release")
		return
	case <-time.After(100 * time.Millisecond):
		// correctly blocked - capacity was not inflated
	}
}

// TestBudget_DriverCaps pins the per-driver budgets and that unknown drivers get the effectively-unbounded
// default (so the SQLite in-memory suite, and any unrecognized driver, never blocks).
func TestBudget_DriverCaps(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.Equal(int64(80), driverConnCap("pgx"))
	assert.Equal(int64(120), driverConnCap("mysql"))
	assert.Equal(int64(120), driverConnCap("mssql"))
	assert.Equal(int64(1<<20), driverConnCap("sqlite"))
	assert.Equal(int64(1<<20), driverConnCap("something-else"))
}
