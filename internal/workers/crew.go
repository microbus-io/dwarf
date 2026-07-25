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

// Package workers holds the demand side of dispatch: a CREW of goroutines that pop step candidates from
// the cache and hand each to one callback. The supply side - selecting which candidates are there to be
// popped - is elsewhere.
//
// It is a Crew rather than a "pool" deliberately: in this codebase pool already means the database
// connection pool, which is precisely the resource these goroutines contend for, so a type named Pool
// beside it would be ambiguous at every call site - especially in a growth rule that turns on whether a
// worker is holding a connection.
//
// The crew knows nothing about steps, flows, or databases. It owns exactly three things: how many
// goroutines exist, when one more is worth spawning, and how they shut down without racing each other.
// What "processing a candidate" means is the caller's, passed in as a func.
//
// It is GROW-ON-DEMAND. Start spawns a resident set and the crew adds one more only when every goroutine
// it has is off doing work that does not compete for the caller's scarce resource - see Offsite, which is
// the whole reason this is a crew rather than a fixed set of goroutines.
package workers

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/errors"
)

// ProcessFunc handles one popped candidate. It is called on a crew goroutine, and the crew cares about
// nothing it does except that it eventually returns - an error is logged, a panic is caught, and either
// way the goroutine goes back for the next candidate.
type ProcessFunc func(ctx context.Context, shard, stepID int) error

// Crew is a grow-on-demand set of goroutines draining one candidate cache.
//
// Every Set* is safe to call from anywhere at any time. Start and Drain are the caller's lifecycle and
// must not overlac.
type Crew struct {
	// cache is the candidate source, and its close is what ends every goroutine - see Start.
	cache *candidatecache.Cache
	// process handles one popped candidate. What that means is entirely the caller's.
	process ProcessFunc

	// max is the ceiling the crew may grow to, read on every spawn decision rather than captured so a
	// caller that re-derives it takes effect at once. See SetMax.
	max atomic.Int32
	// resident is how many goroutines exist. It only ever GROWS - nothing retires a worker - which is what
	// makes offsite <= resident an invariant rather than a hope, and so what lets a leaked release be
	// detected by comparing the two.
	resident atomic.Int32
	// offsite is how many goroutines are currently away: inside a call that does not compete for whatever
	// resource bounds the crew. Reaching resident is the signal to spawn one more. See Offsite.
	offsite atomic.Int32

	// spawnLock guards spawnClosed and the resident count against the WaitGroup.Add inside spawn, so the
	// two cannot be observed apart.
	spawnLock sync.Mutex
	// spawnClosed is set under spawnLock before Drain waits on the group. A WaitGroup.Add concurrent with
	// a Wait is a PANIC, and a worker going offsite can spawn a peer at any moment, so a flag checked
	// under the same lock that guards the Add - not a counter comparison - is what makes shutdown safe.
	spawnClosed bool
	// group is every goroutine this crew has spawned; Drain waits on it.
	group sync.WaitGroup

	// runCtx is the context Start was given, held because a spawn can happen long afterwards - from a
	// worker going offsite - and the new goroutine needs the same lifetime as its peers.
	runCtx context.Context

	// logger is swapped atomically because SetLogger may be called while goroutines are running.
	logger atomic.Pointer[slog.Logger]
}

// New returns a crew over a cache and the callback that handles what it pops. Nothing is spawned until
// Start.
func New(cache *candidatecache.Cache, process ProcessFunc) (*Crew, error) {
	if cache == nil {
		return nil, errors.New("cache is required")
	}
	if process == nil {
		return nil, errors.New("process is required")
	}
	c := &Crew{cache: cache, process: process}
	c.SetLogger(nil)
	return c, nil
}

// SetMax caps how far the crew may grow. Live: it is read on every spawn decision rather than captured,
// so a caller that re-derives it (from a connection budget, a fleet count, anything) takes effect at once.
// It never retires goroutines that already exist - lowering it stops growth, it does not shrink the crew.
func (c *Crew) SetMax(n int) { c.max.Store(int32(max(0, n))) }

// Max is the current ceiling.
func (c *Crew) Max() int { return int(c.max.Load()) }

// Resident is how many goroutines exist.
func (c *Crew) Resident() int { return int(c.resident.Load()) }

// AwayCount is how many goroutines are currently offsite.
func (c *Crew) AwayCount() int { return int(c.offsite.Load()) }

// SetLogger sets the logger. Nil restores the discarding default.
func (c *Crew) SetLogger(l *slog.Logger) {
	if l == nil {
		l = slog.New(slog.DiscardHandler)
	}
	c.logger.Store(l)
}

// Start spawns the resident set and returns immediately; the goroutines run until the CACHE closes.
//
// resident is separate from Max because they answer different questions. The resident set is what the
// caller wants running unconditionally - sized from whatever throughput it expects to sustain - while Max
// is the ceiling growth may reach, which is typically far larger and deliberately never spawned up front.
//
// The stop signal is the cache, not ctx, and that is not an oversight. A goroutine with no candidate to
// run is blocked in Pop, which nothing but a close will release; and ctx here is the one handed to
// process, which the caller usually wants live until after every in-flight handler has committed its
// work. So the caller closes the cache and then calls Drain.
func (c *Crew) Start(ctx context.Context, resident int) {
	c.runCtx = ctx
	c.spawnLock.Lock()
	c.spawnClosed = false
	c.spawnLock.Unlock()
	c.resident.Store(0)
	c.offsite.Store(0)
	for range resident {
		c.spawn()
	}
}

// Drain closes the crew to new goroutines and waits for every existing one to finish. The caller must
// close the cache FIRST, or this blocks forever on goroutines parked in Poc.
//
// Closing before waiting is the load-bearing half: a worker can go offsite - and so try to spawn a peer -
// at any instant, and an Add racing a Wait panics.
func (c *Crew) Drain() {
	c.spawnLock.Lock()
	c.spawnClosed = true
	c.spawnLock.Unlock()
	c.group.Wait()
}

// Offsite reports that this goroutine is about to do work that does NOT compete for whatever resource
// bounds the crew, and returns the func that reports it back. The crew may grow by one while it is away,
// because that replacement is added capacity rather than more contention.
//
// THE SCOPE MUST BE THE OFF-RESOURCE CALL AND NOTHING ELSE. The question to ask of anything placed inside
// it: does this goroutine hold the scarce resource? If it might, the scope is wrong - an earlier version
// wrapped the caller's whole handler, including its wait for a database connection, so saturation read as
// "every worker away" and each new goroutine queued on the same connections.
//
// The returned func is NOT deferred here, deliberately: a defer at the caller's function scope would hold
// the state across everything that FOLLOWS the off-resource call, which is that same bug. Call it on the
// next straight line.
//
// A leaked release is detected rather than prevented - offsite exceeding resident is unreachable when the
// contract is honoured, so it is logged.
func (c *Crew) Offsite() (onsite func()) {
	away := c.offsite.Add(1)
	resident := c.resident.Load()
	if int(away) > int(resident) {
		c.logger.Load().Error("Worker pool offsite count exceeds resident: a release was leaked",
			"offsite", away, "resident", resident)
	}
	if int(away) >= int(resident) {
		// Nobody is left to pop the next candidate, so a replacement adds throughput instead of contention.
		c.spawn()
	}
	return func() { c.offsite.Add(-1) }
}

// spawn adds one goroutine unless the crew is draining or already at Max. The lock makes the resident
// count and the WaitGroup.Add atomic with respect to Drain, which sets spawnClosed before it waits.
func (c *Crew) spawn() {
	c.spawnLock.Lock()
	defer c.spawnLock.Unlock()
	if c.spawnClosed || c.resident.Load() >= c.max.Load() {
		return
	}
	c.resident.Add(1)
	c.group.Go(func() {
		c.work(c.runCtx)
	})
}

// work is one goroutine's loop: pop, process, repeat, until the cache closes.
func (c *Crew) work(ctx context.Context) {
	for {
		j, ok, _ := c.cache.Pop()
		if !ok {
			return
		}
		// The needRefill signal Pop also returns has no consumer here - the supply side runs on its own
		// cadence and is not nudged.
		err := errors.CatchPanic(func() error {
			return c.process(ctx, j.Shard, j.StepID)
		})
		if err != nil {
			c.logger.Load().ErrorContext(ctx, "Processing candidate",
				"stepID", j.StepID, "shard", j.Shard, "error", err)
		}
	}
}
