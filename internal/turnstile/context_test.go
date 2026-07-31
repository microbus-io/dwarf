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

package turnstile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// TestSet_ShardsAreIndependent pins that a shard's turnstile is its own. A set is a lookup table, not a
// shared counter: a shard exhausted and queueing must be invisible to every other shard, or one slow
// resource would hold up callers bound for an idle one.
func TestSet_ShardsAreIndependent(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := NewSet()
	s.Resize(1, 1)
	s.Resize(2, 1)

	ctx1 := s.ContextWithPriority(context.Background(), 1, 1)
	ctx2 := s.ContextWithPriority(context.Background(), 2, 1)

	held := WaitTurn(ctx1) // shard 1 is now exhausted
	avail, _ := s.Snapshot()
	assert.Equal(0, avail[1])
	assert.Equal(1, avail[2])

	served := make(chan struct{})
	go func() {
		p := WaitTurn(ctx2)
		close(served)
		p.Return()
	}()
	select {
	case <-served:
	case <-time.After(2 * time.Second):
		assert.True(false, "an exhausted shard must not hold up another")
	}

	held.Return()
	s.Close()
}

// TestSet_ContextKeepsTheJobsAgeAcrossShards pins the rule that makes a cross-shard operation orderable at
// all. Re-pointing a job at another shard must carry its ORIGINAL age and job number: re-stamping instead
// would make a job that has been running for a while look newly arrived at every hop, so it would keep
// losing to work that had only just started - the exact inversion the age ordering exists to prevent.
func TestSet_ContextKeepsTheJobsAgeAcrossShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := NewSet()
	s.Resize(1, 1)
	s.Resize(2, 1)

	ctx := s.ContextWithPriority(context.Background(), 1, 7)
	first, ok := ClaimFrom(ctx)
	assert.True(ok)

	time.Sleep(2 * time.Millisecond) // so a re-stamp would be visibly younger
	next := s.ContextWithPriority(ctx, 2, 3)
	second, ok := ClaimFrom(next)
	assert.True(ok)

	assert.True(first.Since.Equal(second.Since), "the job keeps its age when it moves to another shard")
	assert.Equal(first.Seq, second.Seq, "and its identity")
	assert.Equal(3, second.Priority, "only the band and the shard are re-pointed")
	assert.Equal(7, first.Priority, "and the original context is left alone")

	// The re-pointed context really does address the other shard's turnstile.
	held := WaitTurn(next)
	avail, _ := s.Snapshot()
	assert.Equal(1, avail[1])
	assert.Equal(0, avail[2])
	held.Return()
	s.Close()
}

// TestSet_JobSeqOrdersClaimsTheClockCannotSeparate pins what the job number is for. A clock coarser than
// the arrival rate hands many jobs the same timestamp; without the tiebreak they order arbitrarily, so two
// tied jobs interleave their turns instead of one finishing and leaving.
func TestSet_JobSeqOrdersClaimsTheClockCannotSeparate(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	ts := New(1)
	held, ok := ts.WaitTurn(context.Background(), Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)

	tied := time.Now() // one timestamp, several jobs - what a coarse clock produces
	served := make(chan uint32, 3)
	var wg sync.WaitGroup
	// Queued highest-numbered first, so arrival order is the reverse of the order they must be served in.
	for _, seq := range []uint32{30, 20, 10} {
		wg.Go(func() {
			p, ok := ts.WaitTurn(context.Background(), Claim{Priority: 1, Since: tied, Seq: seq})
			if ok {
				served <- seq
				p.Return()
			}
		})
		assert.True(queued(ts, int(40-seq)/10))
	}

	held.Return()
	wg.Wait()
	assert.Equal(uint32(10), <-served, "the job number orders what the clock could not")
	assert.Equal(uint32(20), <-served)
	assert.Equal(uint32(30), <-served)
	ts.Close()
}

// TestWaitTurn_PassesThroughWhenUnordered pins the fail-open direction, which is what lets call sites be
// converted one at a time and lets a drain finish work whose outcome exists nowhere else. None of these
// callers may be blocked, and every pass they get back must be safe to return.
func TestWaitTurn_PassesThroughWhenUnordered(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := NewSet()
	s.Resize(1, 1)

	done := make(chan string, 4)
	go func() {
		// A context nobody stamped.
		WaitTurn(context.Background()).Return()
		done <- "unstamped"

		// A shard that was never sized, so the set has no turnstile for it.
		WaitTurn(s.ContextWithPriority(context.Background(), 99, 1)).Return()
		done <- "unsized shard"

		// An expired deadline, with the only pass held elsewhere.
		ctx, cancel := context.WithTimeout(s.ContextWithPriority(context.Background(), 1, 1), time.Millisecond)
		defer cancel()
		WaitTurn(ctx).Return()
		done <- "expired ctx"

		// A closed set.
		closed := NewSet()
		closed.Resize(1, 1)
		cctx := closed.ContextWithPriority(context.Background(), 1, 1)
		closed.Close()
		WaitTurn(cctx).Return()
		done <- "closed"
	}()

	held := WaitTurn(s.ContextWithPriority(context.Background(), 1, 1)) // holds shard 1's only pass throughout
	for _, want := range []string{"unstamped", "unsized shard", "expired ctx", "closed"} {
		select {
		case got := <-done:
			assert.Equal(want, got)
		case <-time.After(5 * time.Second):
			assert.True(false, "%s must pass straight through rather than block", want)
		}
	}

	held.Return()
	avail, _ := s.Snapshot()
	assert.Equal(1, avail[1], "a passed-through caller returns nothing, so the ceiling is untouched")
	s.Close()
}

// TestSet_ResizeKeepsTheShardsTurnstile pins that re-sizing a live shard moves the existing turnstile's
// ceiling rather than replacing it. A replacement would strand every caller queued on the old one and hand
// its held passes out a second time.
func TestSet_ResizeKeepsTheShardsTurnstile(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := NewSet()
	s.Resize(1, 2)
	first := s.Turnstile(1)
	ctx := s.ContextWithPriority(context.Background(), 1, 1)
	held := WaitTurn(ctx)

	s.Resize(1, 4)
	assert.Equal(first, s.Turnstile(1), "the shard keeps its turnstile across a resize")
	avail, _ := s.Snapshot()
	assert.Equal(3, avail[1], "and the resize moves by the delta, leaving the held pass held")

	held.Return()
	avail, _ = s.Snapshot()
	assert.Equal(4, avail[1])
	s.Close()
}
