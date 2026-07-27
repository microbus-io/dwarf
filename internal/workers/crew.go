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
// The crew knows nothing about steps, flows, or databases. It owns three things and no more: how many
// goroutines exist, when one more is worth spawning, and how they shut down without racing each other.
// What "processing a candidate" means is the caller's, passed in as a func.
//
//	crew, err := workers.New(cache, gate, process)
//	crew.SetMax(ceiling)
//	crew.Start(ctx, resident)
//	...
//	cache.Close() // releases the goroutines parked waiting for work
//	gate.Close()  // releases the goroutines waiting on a permit
//	crew.Drain()
//
// It is GROW-ON-DEMAND, and the growth rule is a question about IDLENESS rather than about what any worker
// happens to be doing: one more goroutine is added while NOBODY is idle, work is waiting, and the gate
// reports the bounded resource still has room. That is tested on two paths - by a worker that has just taken
// a candidate (fast, and it cascades) and on a fixed cadence (reliable, and it is what covers work arriving
// while every worker is already committed, which fires no edge at all). See considerGrowth and growLoop.
//
// The gate is what makes so easy a trigger safe, and it belongs to the caller: the crew holds *a* gate
// without knowing what it gates.
//
// The cache and the gate, not ctx, are the stop signals: close them, then Drain. Every Set* is safe to
// call from anywhere at any time; Start and Drain are the caller's lifecycle and must not overlap.
package workers

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/errors"
)

// growCheckInterval is how often the crew re-tests the growth condition independently of any worker
// activity. It bounds only the STANDING START - the delay before a fully-committed crew reacts to work that
// arrived while nobody was free - because the edge trigger cascades from the first spawn. Short enough that
// a long-task workload reaches its working set inside a warmup, long enough that an idle engine's cost is a
// few atomic loads a second.
const growCheckInterval = 5 * time.Millisecond

// ProcessFunc handles one popped candidate. It is called on a crew goroutine, and the crew cares about
// nothing it does except that it eventually returns - an error is logged, a panic is caught, and either
// way the goroutine goes back for the next candidate.
//
// release hands the gate's permit back. The HANDLER owns when, because only it knows the point in its own
// work at which the bounded resource stops being held - typically just before a long call that holds
// nothing. It is safe to call more than once and safe never to call: the crew wraps it in sync.OnceFunc and
// calls it unconditionally on the way out, so no early return can leak a permit.
type ProcessFunc func(ctx context.Context, shard, stepID int, release func()) error

// Gate bounds how many goroutines may be working against whatever resource actually binds. The crew treats
// it opaquely: take a permit before removing work from the cache, hand it back when the handler says so.
//
// Acquire blocks, and reports !ok only when the gate CLOSES - one of the two stop signals. Available is a
// hint for the growth decision, where a stale answer costs one goroutine that parks harmlessly.
type Gate interface {
	Acquire(shard int) (release func(), ok bool)
	Available(shard int) bool
}

// Crew is a grow-on-demand set of goroutines draining one candidate cache through one gate.
//
// Every Set* is safe to call from anywhere at any time. Start and Drain are the caller's lifecycle and
// must not overlap.
type Crew struct {
	// cache is the candidate source, and its close is what ends every goroutine - see Start.
	cache *candidatecache.Cache
	// gate bounds concurrent work against the resource the caller cares about.
	gate Gate
	// process handles one popped candidate. What that means is entirely the caller's.
	process ProcessFunc

	// max is the ceiling the crew may grow to, read on every spawn decision rather than captured so a
	// caller that re-derives it takes effect at once. See SetMax.
	max atomic.Int32
	// resident is how many goroutines exist. It only ever GROWS - nothing retires a worker. It is
	// BOOKKEEPING plus the cap check, not a decision input: what governs growth is idleness and the gate.
	resident atomic.Int32
	// idle is how many goroutines are NOT currently holding a candidate. Zero means the crew is fully
	// committed - nobody is left to take the next one - which is the growth signal.
	//
	// "Not holding a candidate" is deliberately wider than "parked waiting for work", and each of the three
	// states it covers is a worker that will take the next candidate without help, so none is a reason to
	// add another goroutine:
	//   - parked in WaitForWork,
	//   - blocked in the gate's Acquire (it proceeds the moment a permit frees),
	//   - SPAWNED BUT NOT YET RUNNING. spawn increments this BEFORE the goroutine exists, and it must:
	//     counting only the park makes a freshly-started crew read as fully committed for the instant
	//     before its workers reach it, so Start itself trips the trigger and over-spawns.
	//
	// DO NOT move this to a counter the CALLER maintains around its own off-resource call. Correctness then
	// rests on where the caller puts a brace, and one line too many - wrapping the wait for a database
	// connection - makes saturation read as idleness: measured at ~20% throughput on a saturated 8-vCPU
	// shard (2,902 vs 3,523 steps/s), with ~1,300 goroutines where ~512 sufficed.
	idle atomic.Int32
	// spawnLock guards spawnClosed and the resident count against the WaitGroup.Add inside spawn, so the
	// two cannot be observed apart.
	spawnLock sync.Mutex
	// spawnClosed is set under spawnLock before Drain waits on the group. A WaitGroup.Add concurrent with
	// a Wait is a PANIC, and a worker can spawn a peer at any moment, so a flag checked under the same
	// lock that guards the Add - not a counter comparison - is what makes shutdown safe.
	spawnClosed bool
	// group is every goroutine this crew has spawned; Drain waits on it.
	group sync.WaitGroup
	// growStop ends the grower; growWorker is what Drain waits on before closing the crew to spawns.
	growStop   chan struct{}
	growWorker sync.WaitGroup

	// runCtx is the context Start was given, held because a spawn can happen long afterwards - from a
	// worker taking the last free candidate - and the new goroutine needs the same lifetime as its peers.
	runCtx context.Context

	// logger is swapped atomically because SetLogger may be called while goroutines are running.
	logger atomic.Pointer[slog.Logger]
}

// New returns a crew over a cache, the gate that bounds its concurrency, and the callback that handles what
// it pops. Nothing is spawned until Start.
func New(cache *candidatecache.Cache, gate Gate, process ProcessFunc) (*Crew, error) {
	if cache == nil {
		return nil, errors.New("cache is required")
	}
	if gate == nil {
		return nil, errors.New("gate is required")
	}
	if process == nil {
		return nil, errors.New("process is required")
	}
	c := &Crew{cache: cache, gate: gate, process: process}
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

// Idle is how many goroutines are not currently holding a candidate.
func (c *Crew) Idle() int { return int(c.idle.Load()) }

// SetLogger sets the logger. Nil restores the discarding default.
func (c *Crew) SetLogger(l *slog.Logger) {
	if l == nil {
		l = slog.New(slog.DiscardHandler)
	}
	c.logger.Store(l)
}

// Start spawns the resident set and returns immediately; the goroutines run until the cache closes.
//
// resident is separate from Max because they answer different questions. The resident set is what the
// caller wants running unconditionally - sized from whatever throughput it expects to sustain - while Max
// is the ceiling growth may reach, which is typically far larger and deliberately never spawned up front.
//
// The stop signals are the cache and the gate, not ctx, and that is not an oversight. A goroutine with no
// candidate to run is blocked in one of them, which nothing but a close will release; and ctx here is the
// one handed to process, which the caller usually wants live until after every in-flight handler has
// committed its work. So the caller closes both and then calls Drain.
func (c *Crew) Start(ctx context.Context, resident int) {
	c.runCtx = ctx
	c.spawnLock.Lock()
	c.spawnClosed = false
	c.spawnLock.Unlock()
	c.resident.Store(0)
	c.idle.Store(0)
	c.growStop = make(chan struct{})
	stop := c.growStop
	c.growWorker.Go(func() { c.growLoop(stop) })
	for range resident {
		c.spawn()
	}
}

// Drain closes the crew to new goroutines and waits for every existing one to finish. The caller must
// close the cache AND the gate FIRST, or this blocks forever on goroutines parked in one of them.
//
// Closing before waiting is the load-bearing half: a worker can try to spawn a peer at any instant, and an
// Add racing a Wait panics.
func (c *Crew) Drain() {
	// The grower stops FIRST and is waited on before the flag is set, so it cannot be mid-spawn when Drain
	// closes the crew - the same Add-racing-Wait hazard the flag exists for, arriving from a goroutine that
	// is not one of the workers.
	if c.growStop != nil {
		close(c.growStop)
		c.growStop = nil
	}
	c.growWorker.Wait()
	c.spawnLock.Lock()
	c.spawnClosed = true
	c.spawnLock.Unlock()
	c.group.Wait()
}

// considerGrowth adds one goroutine when the crew has just become fully committed, there is more work
// waiting, and the gate still has room. It is called by a worker that has just TAKEN a candidate, so the
// question it answers is: now that I am busy, is anyone left to take the next one?
//
// All three conditions are necessary and none alone is sufficient:
//
//   - NOBODY IDLE. An idle peer can already take the next candidate, so another goroutine would be pure
//     duplication. DO NOT tighten this to "every worker is simultaneously inside the handler": that
//     coincidence's probability decays exponentially in the crew size, which integrates to
//     N(t) ~ (task/db).ln(D.t) - logarithmic growth, so throughput converges to ln(D.t)/db and a long-task
//     workload caps at crew-size-over-task-duration however much capacity sits idle.
//   - THE GATE HAS ROOM. This is what makes so easy a trigger safe, and growth must never key on
//     saturation instead: when the bounded resource is already saturated another goroutine cannot dispatch
//     anything, it can only queue, so a trigger that fires on saturation gets truer the worse it gets
//     (measured: ~20% throughput lost to a crew ~3x the size that sufficed).
//   - WORK IS WAITING. Without it the last candidate of a batch spawns a worker that has nothing to do but
//     park, and it is also what BOUNDS growth: the crew can never outrun the work that justified it.
//
// DO NOT add a rate damper here. Growth is edge-triggered, so a suppressed spawn is not deferred, it is
// LOST - and in the very case this exists for (every worker blocked in a long call) there is no later pop
// to re-evaluate on. Measured: a 1ms damper grows the crew by exactly one past its resident set and then
// stops forever, serving 97 of 130 parked tasks. The three conditions are the fuse, not a clock.
func (c *Crew) considerGrowth() {
	// Ordered by cost and by how often each is the one that says no: an atomic load, then the cache's
	// mutex, then the gate's.
	if c.idle.Load() > 0 {
		return
	}
	shard, ok := c.cache.PeekShard()
	if !ok {
		return
	}
	if !c.gate.Available(shard) {
		return
	}
	c.spawn()
}

// growLoop re-evaluates the growth condition on a fixed cadence, and it is what makes growth RELIABLE
// rather than merely fast.
//
// The worker-loop trigger is edge-triggered on a worker TAKING work, and an edge alone cannot cover the
// state that matters most: once every worker is committed to a long call nobody pops, so work arriving
// after that moment fires no edge at all and the crew sits at its resident size with candidates queued in
// front of it. Measured on that shape - 130 parked tasks against a 96-worker resident set - an edge-only
// crew stops at 96, leaving 34 candidates unserved with permits and ceiling both to spare.
//
// So the cadence is the reliable path and the edge is the fast one, which is the same posture as the supply
// side's piston cycling UNCONDITIONALLY rather than on a trigger: a trigger must be fired from every site
// that can create the condition, and missing one wedges silently, whereas a cycle that always runs cannot
// be in that state.
//
// One spawn per tick is enough because the edge trigger cascades from it: the worker this spawns takes a
// candidate, finds nobody idle and work still waiting, and spawns the next - so a single tick unblocks a
// chain that grows as fast as workers can pop, and the cadence only has to cover the standing start.
func (c *Crew) growLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(growCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			c.considerGrowth()
		}
	}
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
	// Counted idle BEFORE the goroutine exists, so the trigger can never read a crew that has been spawned
	// but has not yet reached its park as "fully committed" - which is what made Start over-spawn.
	c.idle.Add(1)
	c.group.Go(func() {
		c.work(c.runCtx)
	})
}

// work is one goroutine's loop: park holding nothing, take a permit, take work, process, repeat.
//
// THE ORDER IS THE DESIGN, and both halves of it were arrived at by ruling out the alternative:
//
//   - PARK HOLDING NO PERMIT. A worker that waited for work while holding one would hoard admission
//     capacity it is not using, throttling the peers that do have work.
//   - TAKE THE PERMIT BEFORE THE POP. Popping first and then blocking would strand a candidate inside a
//     worker that cannot proceed with it, and the pop is what empties the very partition its peers are
//     choosing between. Acquire-then-pop keeps a candidate in the cache until someone can actually run it.
//
// The peeked shard can go stale while the acquire blocks, which is why the pop is non-blocking and its
// failure is ordinary rather than exceptional.
func (c *Crew) work(ctx context.Context) {
	// spawn already counted this goroutine as idle, so the loop's invariant on entry - and at the top of
	// every iteration - is "this worker is counted idle". Both returns below sit inside that region, so one
	// deferred decrement covers every exit without ever double-counting.
	defer c.idle.Add(-1)
	for {
		shard, ok := c.cache.WaitForWork()
		if !ok {
			return // the cache closed
		}
		release, ok := c.gate.Acquire(shard)
		if !ok {
			return // the gate closed: draining
		}
		j, ok, _ := c.cache.TryPopFrom(shard)
		if !ok {
			// Another worker took the peeked entry, or the partition drained while we waited. CONTINUE,
			// never return: resident never decrements, so a worker that returned here would erode the crew
			// under exactly the contention that caused the race, and silently. Still idle - nothing was
			// taken - so the counter is left alone.
			release()
			continue
		}
		c.idle.Add(-1)
		// The needRefill signal TryPopFrom also returns has no consumer here - the supply side runs on its
		// own cadence and is not nudged.
		c.considerGrowth()
		// The permit's lifetime crosses the callback boundary - the crew acquires, the handler releases -
		// so every early return inside the handler, and every caught panic, is a path that would otherwise
		// leak a permit permanently and decay admission until it stopped. OnceFunc plus an unconditional
		// call on the way out makes that leak unreachable rather than merely unlikely.
		once := sync.OnceFunc(release)
		err := errors.CatchPanic(func() error {
			return c.process(ctx, j.Shard, j.StepID, once)
		})
		once()
		if err != nil {
			c.logger.Load().ErrorContext(ctx, "Processing candidate",
				"stepID", j.StepID, "shard", j.Shard, "error", err)
		}
		c.idle.Add(1) // back to idle, restoring the top-of-loop invariant
	}
}
