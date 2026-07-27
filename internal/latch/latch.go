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

// Package latch parks callers on a key until something reports that key done.
//
// A Board is the set of keys callers are waiting on. A caller parks with Latch and gets back the status
// that ended its wait. The owner drives Sweep on its own ticker: each pass hands every latched key to
// the StatusResolver callback and wakes the callers whose keys come back done.
//
//	board, err := latch.New(func(ctx context.Context, keys []string) (map[string]string, error) {
//		// Look up keys however you like; report ONLY the ones whose wait is over.
//		return done, err
//	})
//
//	go func() {
//		for range ticker.C {
//			observe(board.Sweep(ctx))
//		}
//	}()
//
//	status, err := board.Latch(ctx, key) // blocks until the key is reported done
//
// Two things wake a parked caller: a sweep, and Release for an owner that learns a key is done by some
// other means. Close ends every wait at shutdown.
//
// The board holds no database handle, spawns no goroutine, picks no cadence, and has no opinion on what
// a status string means - StatusResolver decides which keys are done, and the ticker decides how
// often. Nothing needs arming before a key can settle: a key already done when Latch is called is
// reported by the next sweep, so registration order costs latency at worst.
//
// Latch, Release, Latched and Close are safe to call from anywhere at any time. Sweep is not: drive it
// from one goroutine.
package latch

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/errors"
)

// StatusResolver reports which of the given keys are done waiting, and with what status.
//
// A key it OMITS stays latched, so reporting nothing is how to say "still waiting". It may report a key
// that is not in keys; that release simply finds no waiter.
//
// It is called with at least one key and never concurrently with itself, and is free to chunk, pad,
// group or route the keys however it needs. On error it may still return whatever it did resolve: those
// releases are applied, and the keys it did not report are asked about again on the next sweep.
type StatusResolver func(ctx context.Context, keys []string) (map[string]string, error)

// ErrClosed is what Latch returns once the board is closed. It carries no status code; a caller mapping
// errors onto a transport translates it there.
var ErrClosed = errors.New("latch board is closed")

// Result is one sweep's outcome, for logging and metrics. Nothing in it is a control signal: read it and
// carry on.
type Result struct {
	// Latched is how many distinct keys the sweep asked about, Reported how many of those came back done,
	// and Released how many parked callers were woken (one key can hold several).
	Latched  int
	Reported int
	Released int

	// Duration spans the whole pass, StatusResolver included.
	Duration time.Duration

	// Err is what StatusResolver returned. Any releases it reported alongside the error have already been
	// applied; this is for the log line.
	Err error
}

// wake is what a parked caller receives. Closing travels the same one-slot channel as a status, which is
// what makes their precedence structural rather than a race: a delivered status leaves no room for the
// close.
type wake struct {
	status string
	closed bool
}

// Board is the set of keys callers are parked on.
//
// Latch, Release, Latched and Close are safe to call from anywhere at any time. Sweep is NOT safe for
// concurrent use - drive it from one goroutine, which also means StatusResolver is never re-entered.
type Board struct {
	resolve StatusResolver
	onPark  atomic.Pointer[func(key string)]

	lock    sync.Mutex
	waiters map[string][]chan wake
	closed  bool
}

// SetOnPark registers a callback fired by [Board.Latch] once a caller is ON the board and about to block,
// or clears it when nil. It is the observation point a caller outside this package cannot reach for
// itself: registering and blocking are one call here - deliberately, since a gap between them is exactly
// the race Close is careful not to leave - so "the caller is parked" has no other moment.
//
// It exists for the one thing a duration cannot state. Registration order is not a correctness question
// (the board is polled, so a key that settles before its caller arrives is reported by the next sweep),
// but it IS the premise of any test about a caller that was ALREADY blocked when the key settled - and
// left to a sleep, that premise silently degrades into its opposite on a slow machine: the caller
// registers late, its own first read answers it, and the test passes having exercised nothing.
//
// The callback runs on the parking goroutine while it holds no lock of this package's, so it may do as it
// likes; a board with none set pays one atomic load per Latch. It is NOT called for a Latch turned away
// by a closed board, which never parks.
func (b *Board) SetOnPark(fn func(key string)) {
	if fn == nil {
		b.onPark.Store(nil)
		return
	}
	b.onPark.Store(&fn)
}

// New returns an empty board over the callback that reports which keys are done.
func New(resolve StatusResolver) (*Board, error) {
	if resolve == nil {
		return nil, errors.New("resolve is required")
	}
	return &Board{
		resolve: resolve,
		waiters: map[string][]chan wake{},
	}, nil
}

// Latch parks the caller on one key and returns the status that ended its wait.
//
// It returns when the key is reported done - by a sweep or by a direct Release - and otherwise on ctx,
// or with ErrClosed once the board is closed. A key already done when Latch is called is reported by the
// next sweep, so a caller wanting to skip that wait checks the key itself first.
func (b *Board) Latch(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", errors.New("key is required")
	}
	ch := make(chan wake, 1)
	b.lock.Lock()
	// Registering under the same lock Close holds is what leaves no gap between the two: a caller is
	// either on the board in time for Close to wake it, or turned away here. Never neither.
	if b.closed {
		b.lock.Unlock()
		return "", errors.Trace(ErrClosed)
	}
	b.waiters[key] = append(b.waiters[key], ch)
	b.lock.Unlock()
	defer b.unlatch(key, ch)

	// After the registration and outside the lock: the caller is now on the board, so a release from any
	// source will reach it, and an observer told so here cannot be told too early. See SetOnPark.
	if fn := b.onPark.Load(); fn != nil {
		(*fn)(key)
	}

	select {
	case w := <-ch:
		if w.closed {
			return "", errors.Trace(ErrClosed)
		}
		return w.status, nil
	case <-ctx.Done():
		return "", errors.Trace(ctx.Err())
	}
}

// Release wakes every caller parked on one key and reports how many there were. It is the direct path,
// for a caller that learns a key is done by some means other than a sweep; a sweep releases through it
// too. Releasing a key nobody holds is a no-op.
func (b *Board) Release(key string, status string) int {
	b.lock.Lock()
	parked := slices.Clone(b.waiters[key])
	b.lock.Unlock()
	// Sent outside the lock and never blocking: the channels are buffered, and a caller that has already
	// been released (or has gone home on its ctx) must not hold up the rest.
	for _, ch := range parked {
		select {
		case ch <- wake{status: status}:
		default:
		}
	}
	return len(parked)
}

// Sweep runs one detector pass: it asks StatusResolver about every latched key and releases the ones
// that come back done. With nothing latched it does not call StatusResolver at all, so an idle board is
// free.
//
// It never returns an error, only a Result carrying one - a failed pass needs no decision from the
// caller, since the next sweep asks again.
func (b *Board) Sweep(ctx context.Context) Result {
	started := time.Now()
	keys := b.keys()
	r := Result{Latched: len(keys)}
	if len(keys) == 0 {
		r.Duration = time.Since(started)
		return r
	}
	done, err := b.resolve(ctx, keys)
	// Applied before the error is examined, on purpose: a partial answer is still an answer, and a
	// transport that resolved three of four keys should not hold the three back over the fourth.
	for key, status := range done {
		r.Released += b.Release(key, status)
	}
	r.Reported = len(done)
	if err != nil {
		r.Err = errors.Trace(err)
	}
	r.Duration = time.Since(started)
	return r
}

// Latched is how many distinct keys currently have someone parked on them.
func (b *Board) Latched() int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return len(b.waiters)
}

// Waiting is how many callers are parked on one key.
func (b *Board) Waiting(key string) int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return len(b.waiters[key])
}

// Close wakes every parked caller with ErrClosed and rejects later Latch calls. It is idempotent, and it
// does not stop a Sweep already in flight - the owner drives that and stops driving it.
//
// A caller already released keeps its status: shutdown never overwrites an answer that was delivered.
func (b *Board) Close() {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, parked := range b.waiters {
		for _, ch := range parked {
			select {
			case ch <- wake{closed: true}:
			default:
			}
		}
	}
}

// keys snapshots what is latched, sorted so that a caller batching them sees stable boundaries from one
// sweep to the next rather than a fresh split of the same keys on every pass.
func (b *Board) keys() []string {
	b.lock.Lock()
	defer b.lock.Unlock()
	keys := make([]string, 0, len(b.waiters))
	for key := range b.waiters {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// unlatch drops one parked caller's channel, and the key with it once it holds none. Leaving an empty
// key behind would keep asking StatusResolver about something nobody waits for.
func (b *Board) unlatch(key string, ch chan wake) {
	b.lock.Lock()
	defer b.lock.Unlock()
	parked := b.waiters[key]
	for i, c := range parked {
		if c == ch {
			// Order among a key's waiters means nothing - they all get the same status - so the last one
			// fills the hole rather than shifting the rest.
			parked[i] = parked[len(parked)-1]
			b.waiters[key] = parked[:len(parked)-1]
			break
		}
	}
	if len(b.waiters[key]) == 0 {
		delete(b.waiters, key)
	}
}
