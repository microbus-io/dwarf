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
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// TestTurnstile_TryWaitTurnNeverBarges pins that a non-blocking take only ever takes a pass nobody is waiting
// for: a pass returned while a caller is queued goes to that caller, not to a try that arrives after it.
func TestTurnstile_TryWaitTurnNeverBarges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(1)
	held, ok := ts.TryWaitTurn()
	assert.True(ok, "a free pass is taken")
	_, ok = ts.TryWaitTurn()
	assert.False(ok, "no pass is free")

	granted := make(chan Pass, 1)
	go func() {
		p, _ := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
		granted <- p
	}()
	assert.True(queued(ts, 1))
	held.Return()
	_, ok = ts.TryWaitTurn()
	assert.False(ok, "the returned pass belongs to the queued caller")
	p := <-granted
	p.Return()

	_, ok = ts.TryWaitTurn()
	assert.True(ok, "with nobody waiting, a returned pass is free again")
	ts.Close()
	_, ok = ts.TryWaitTurn()
	assert.False(ok, "a closed turnstile issues nothing")
}

// TestGate_TryAcquire pins the non-blocking acquire's three outcomes: a turn taken on a stamped context, no
// turn with nothing held, and a closed set reported as a stop.
func TestGate_TryAcquire(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	s := NewSet()
	s.Resize(1, 1)
	g := s.Gate(10)

	gated, release, acquired, ok := g.TryAcquire(ctx, 1)
	assert.True(acquired && ok)
	claim, stamped := ClaimFrom(gated)
	assert.True(stamped, "the context carries the job's claim")
	assert.Equal(10, claim.Priority)

	_, release2, acquired, ok := g.TryAcquire(ctx, 1)
	assert.True(ok)
	assert.False(acquired, "the shard's one turn is taken")
	release2() // nothing held; must be safe
	avail, _ := s.Turnstile(1).Snapshot()
	assert.Equal(0, avail, "a failed try holds nothing and releases nothing")

	release()
	_, release, acquired, _ = g.TryAcquire(ctx, 1)
	assert.True(acquired, "the returned turn is free again")
	release()

	_, _, acquired, ok = g.TryAcquire(ctx, 2)
	assert.True(acquired && ok, "a shard with no turnstile admits, as WaitTurn does")

	s.Close()
	_, _, acquired, ok = g.TryAcquire(ctx, 1)
	assert.False(ok, "a closed set is a stop")
	assert.False(acquired)
}
