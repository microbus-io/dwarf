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
	"context"
	"slices"
	"sync"
	"time"

	"github.com/microbus-io/sequel"
)

// cohortStripe is one stripe of e.cohortLocks: a FIFO lock whose waiters can run a callback periodically while they
// wait without losing their place in line, and whose unlock hands the stripe to the longest waiter. The zero value
// is an unlocked stripe. The uncontended path allocates nothing; a waiter allocates a channel and a timer.
type cohortStripe struct {
	mu    sync.Mutex
	held  bool
	queue []chan struct{} // waiters in arrival order; unlock hands the stripe to the head by closing its channel
}

// lock acquires the stripe. While it waits it calls tick every interval, keeping its place in line; tick returning
// false abandons the wait, and lock then returns false without the stripe. A panic in tick withdraws the waiter
// before it propagates, so the stripe is never handed to a goroutine that is gone.
func (s *cohortStripe) lock(interval time.Duration, tick func() bool) bool {
	s.mu.Lock()
	if !s.held {
		s.held = true
		s.mu.Unlock()
		return true
	}
	turn := make(chan struct{})
	s.queue = append(s.queue, turn)
	s.mu.Unlock()

	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-turn:
			return true
		case <-timer.C:
			if !s.tick(turn, tick) {
				s.abandon(turn)
				return false
			}
			timer.Reset(interval)
		}
	}
}

// tick runs one tick for the waiter holding turn, withdrawing it if the callback panics.
func (s *cohortStripe) tick(turn chan struct{}, tick func() bool) bool {
	defer func() {
		if r := recover(); r != nil {
			s.abandon(turn)
			panic(r)
		}
	}()
	return tick()
}

// abandon withdraws a waiter. If the stripe was handed to it while it was out ticking, it now holds the stripe, and
// passes it on.
func (s *cohortStripe) abandon(turn chan struct{}) {
	s.mu.Lock()
	if i := slices.Index(s.queue, turn); i >= 0 {
		s.queue = slices.Delete(s.queue, i, i+1)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.unlock()
}

// unlock releases the stripe, handing it to the longest waiter if there is one.
func (s *cohortStripe) unlock() {
	s.mu.Lock()
	if len(s.queue) == 0 {
		s.held = false
		s.queue = nil
	} else {
		next := s.queue[0]
		s.queue[0] = nil
		s.queue = s.queue[1:]
		close(next)
	}
	s.mu.Unlock()
}

// lockCohortStripe parks a step's worker on its cohort's stripe, keeping the step's lease alive while it waits: every
// half extension window (or half leaseMargin, if shorter) it extends the lease on a turn of its own. It returns false
// if an extension found the step taken back - re-claimed, rewound or terminalized - in which case the worker no longer
// owns it and must abandon. A database error on an extension keeps waiting on the lease already held; the fence
// still protects the step if that lapses.
//
// The caller parks holding NO turn: the stripe exists so a losing sibling waits without occupying a connection, and
// a turn is that occupancy one level up.
func (e *Engine) lockCohortStripe(ctx context.Context, stripe *cohortStripe, db *sequel.DB, shardNum, stepID, leaseSeq int) bool {
	interval := min(e.leaseMargin, e.persistLeaseExtension) / 2
	return stripe.lock(interval, func() bool {
		tctx, doneTurn := e.dbTurn(ctx, shardNum)
		defer doneTurn()
		held, err := e.extendLease(tctx, db, stepID, leaseSeq)
		return held || err != nil
	})
}
