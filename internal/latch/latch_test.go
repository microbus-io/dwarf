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

package latch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// resolver is a scripted Resolve that records what it was asked.
type resolver struct {
	lock   sync.Mutex
	asked  [][]string
	answer map[string]string
	err    error
}

func (r *resolver) resolve(ctx context.Context, keys []string) (map[string]string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.asked = append(r.asked, keys)
	return r.answer, r.err
}

func (r *resolver) calls() [][]string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.asked
}

func (r *resolver) reply(answer map[string]string, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.answer, r.err = answer, err
}

// parked is one caller blocked in Latch, and the outcome it eventually got.
type parked struct {
	status string
	err    error
	done   chan struct{}
}

// park calls Latch on its own goroutine and hands back a handle to its outcome. It returns once the
// caller is registered on the board, so a test can sweep or release without racing the registration.
func park(t *testing.T, ctx context.Context, b *Board, key string) *parked {
	t.Helper()
	before := waiters(b, key)
	p := &parked{done: make(chan struct{})}
	go func() {
		defer close(p.done)
		p.status, p.err = b.Latch(ctx, key)
	}()
	waitFor(t, func() bool { return waiters(b, key) == before+1 }, "caller never latched onto "+key)
	return p
}

// wait blocks until the parked caller returned, failing the test if it never does.
func (p *parked) wait(t *testing.T) *parked {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("parked caller never woke")
	}
	return p
}

// stuck reports whether the caller is still parked after a moment. It is the negative assertion, so it
// pays a real wait rather than sampling once.
func (p *parked) stuck() bool {
	select {
	case <-p.done:
		return false
	case <-time.After(50 * time.Millisecond):
		return true
	}
}

func waiters(b *Board, key string) int {
	return b.Waiting(key)
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(time.Millisecond)
	}
}

func newBoard(t *testing.T, r *resolver) *Board {
	t.Helper()
	b, err := New(r.resolve)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

func TestNew_RequiresAResolver(t *testing.T) {
	assert := testarossa.For(t)
	_, err := New(nil)
	assert.Error(err, "a board with no way to resolve keys is a wiring mistake")
}

func TestLatch_RequiresAKey(t *testing.T) {
	assert := testarossa.For(t)
	b := newBoard(t, &resolver{})
	_, err := b.Latch(context.Background(), "")
	assert.Error(err, "an empty key would be swept forever and never reported")
}

func TestSweep_ReleasesAReportedKey(t *testing.T) {
	assert := testarossa.For(t)
	r := &resolver{answer: map[string]string{"1-7-abc": "completed"}}
	b := newBoard(t, r)

	p := park(t, context.Background(), b, "1-7-abc")
	res := b.Sweep(context.Background())

	assert.Equal(1, res.Latched)
	assert.Equal(1, res.Reported)
	assert.Equal(1, res.Released)
	assert.NoError(res.Err)

	p.wait(t)
	assert.NoError(p.err)
	assert.Equal("completed", p.status)
	assert.Equal(0, b.Latched(), "a caller that returned must leave nothing behind")
}

func TestSweep_AsksNothingWithAnEmptyBoard(t *testing.T) {
	assert := testarossa.For(t)
	r := &resolver{}
	b := newBoard(t, r)

	res := b.Sweep(context.Background())

	assert.Equal(0, len(r.calls()), "an idle board must cost no query at all")
	assert.Equal(0, res.Latched)
	assert.NoError(res.Err)
}

func TestSweep_AsksForEachKeyOnceInOrder(t *testing.T) {
	assert := testarossa.For(t)
	r := &resolver{}
	b := newBoard(t, r)
	ctx := context.Background()

	park(t, ctx, b, "1-9-c")
	park(t, ctx, b, "1-3-a")
	park(t, ctx, b, "1-9-c") // a second caller on a key already latched
	park(t, ctx, b, "1-5-b")

	b.Sweep(ctx)

	assert.Equal(1, len(r.calls()))
	// Deduped (two callers on 1-9-c ask once) and sorted, so a resolver batching these sees the same
	// boundaries from one sweep to the next.
	assert.Equal([]string{"1-3-a", "1-5-b", "1-9-c"}, r.calls()[0])
}

func TestSweep_AnOmittedKeyStaysLatched(t *testing.T) {
	assert := testarossa.For(t)
	r := &resolver{answer: map[string]string{"1-2-b": "failed"}}
	b := newBoard(t, r)
	ctx := context.Background()

	still := park(t, ctx, b, "1-1-a")
	gone := park(t, ctx, b, "1-2-b")

	res := b.Sweep(ctx)
	assert.Equal(1, res.Released)

	gone.wait(t)
	assert.Equal("failed", gone.status)
	assert.True(still.stuck(), "a key the resolver did not report must stay parked")
	assert.Equal(1, b.Latched())
}

func TestSweep_AppliesWhatItGotDespiteAnError(t *testing.T) {
	assert := testarossa.For(t)
	// The shape a partly-failed fan-out produces: some keys resolved, an error for the rest.
	r := &resolver{answer: map[string]string{"1-1-a": "completed"}, err: errors.New("shard 2 unreachable")}
	b := newBoard(t, r)
	ctx := context.Background()

	resolved := park(t, ctx, b, "1-1-a")
	unresolved := park(t, ctx, b, "2-1-a")

	res := b.Sweep(ctx)
	assert.Error(res.Err, "the failure is reported for the log line")
	assert.Equal(1, res.Released, "a partial answer is still an answer")

	resolved.wait(t)
	assert.Equal("completed", resolved.status)
	assert.True(unresolved.stuck(), "the unresolved key waits for the next sweep")
}

func TestSweep_RetriesAnUnresolvedKeyOnTheNextPass(t *testing.T) {
	assert := testarossa.For(t)
	r := &resolver{err: errors.New("database is down")}
	b := newBoard(t, r)
	ctx := context.Background()

	p := park(t, ctx, b, "1-1-a")
	assert.Error(b.Sweep(ctx).Err)
	assert.True(p.stuck())

	r.reply(map[string]string{"1-1-a": "cancelled"}, nil)
	assert.NoError(b.Sweep(ctx).Err)

	p.wait(t)
	assert.Equal("cancelled", p.status)
}

func TestRelease_WakesWithoutASweep(t *testing.T) {
	assert := testarossa.For(t)
	r := &resolver{}
	b := newBoard(t, r)

	p := park(t, context.Background(), b, "1-1-a")
	assert.Equal(1, b.Release("1-1-a", "interrupted"))

	p.wait(t)
	assert.Equal("interrupted", p.status)
	assert.Equal(0, len(r.calls()), "the direct path must not touch the resolver")
}

func TestRelease_OfAnUnheldKeyIsANoOp(t *testing.T) {
	assert := testarossa.For(t)
	b := newBoard(t, &resolver{})
	assert.Equal(0, b.Release("1-1-a", "completed"))
}

func TestRelease_WakesEveryCallerOnTheKey(t *testing.T) {
	assert := testarossa.For(t)
	b := newBoard(t, &resolver{})
	ctx := context.Background()

	all := make([]*parked, 8)
	for i := range all {
		all[i] = park(t, ctx, b, "1-1-a")
	}
	assert.Equal(1, b.Latched(), "one key, however many callers")
	assert.Equal(8, b.Waiting("1-1-a"))
	assert.Equal(0, b.Waiting("1-2-b"), "a key nobody holds")
	assert.Equal(8, b.Release("1-1-a", "completed"))

	for _, p := range all {
		p.wait(t)
		assert.Equal("completed", p.status)
	}
	assert.Equal(0, b.Latched())
}

func TestLatch_ContextEndsTheWaitAndUnlatches(t *testing.T) {
	assert := testarossa.For(t)
	r := &resolver{}
	b := newBoard(t, r)
	ctx, cancel := context.WithCancel(context.Background())

	p := park(t, ctx, b, "1-1-a")
	cancel()
	p.wait(t)

	assert.Error(p.err)
	assert.Equal("", p.status)
	waitFor(t, func() bool { return b.Latched() == 0 }, "a departed caller left its key on the board")

	// The key it left with must not be swept for any longer.
	b.Sweep(context.Background())
	assert.Equal(0, len(r.calls()))
}

func TestLatch_OneDeparturePreservesItsPeers(t *testing.T) {
	assert := testarossa.For(t)
	b := newBoard(t, &resolver{})
	ctx, cancel := context.WithCancel(context.Background())

	staying := park(t, context.Background(), b, "1-1-a")
	leaving := park(t, ctx, b, "1-1-a")
	cancel()
	leaving.wait(t)
	waitFor(t, func() bool { return waiters(b, "1-1-a") == 1 }, "the departed caller was not dropped")

	assert.Equal(1, b.Release("1-1-a", "completed"))
	staying.wait(t)
	assert.Equal("completed", staying.status)
}

func TestClose_WakesEveryParkedCaller(t *testing.T) {
	assert := testarossa.For(t)
	b := newBoard(t, &resolver{})
	ctx := context.Background()

	a := park(t, ctx, b, "1-1-a")
	c := park(t, ctx, b, "1-2-b")
	b.Close()

	for _, p := range []*parked{a, c} {
		p.wait(t)
		assert.True(errors.Is(p.err, ErrClosed), "a closed board must say so, not hang to the caller's deadline")
	}
}

func TestClose_RejectsALaterLatch(t *testing.T) {
	assert := testarossa.For(t)
	b := newBoard(t, &resolver{})
	b.Close()
	b.Close() // idempotent

	_, err := b.Latch(context.Background(), "1-1-a")
	assert.True(errors.Is(err, ErrClosed))
}

func TestClose_CannotDisplaceADeliveredRelease(t *testing.T) {
	assert := testarossa.For(t)
	b := newBoard(t, &resolver{})

	// The waiter is placed by hand rather than parked on a goroutine, because the window this pins is
	// precisely the one where the caller has NOT been scheduled yet: a real goroutine consumes its status
	// the instant Release delivers it, so Close finds an empty buffer and the case never arises.
	ch := make(chan wake, 1)
	b.waiters["1-1-a"] = []chan wake{ch}

	b.Release("1-1-a", "completed")
	b.Close()

	w := <-ch
	assert.False(w.closed, "shutdown must not overwrite an answer the caller had already been given")
	assert.Equal("completed", w.status)
}

func TestBoard_SweepsWhileCallersComeAndGo(t *testing.T) {
	assert := testarossa.For(t)
	// Under -race this is the assertion: registration, departure, release and sweep all touch the same
	// map, and only the lock keeps them apart.
	answer := map[string]string{}
	for i := range 32 {
		answer[fmt.Sprintf("1-%d-x", i)] = "completed"
	}
	r := &resolver{answer: answer}
	b := newBoard(t, r)

	sweeping := make(chan struct{})
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		for {
			select {
			case <-sweeping:
				return
			default:
			}
			b.Sweep(context.Background())
		}
	}()

	var group sync.WaitGroup
	for i := range 32 {
		group.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Half the callers are resolvable and half are not, so departures and releases interleave.
			key := fmt.Sprintf("1-%d-x", i)
			if i%2 == 1 {
				key = fmt.Sprintf("2-%d-x", i)
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
			}
			_, _ = b.Latch(ctx, key)
		})
	}
	group.Wait()
	close(sweeping)
	<-swept

	assert.Equal(0, b.Latched(), "every caller returned, so the board must be empty")
}
