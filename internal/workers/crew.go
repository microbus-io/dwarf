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
// It is GROW-ON-DEMAND, and the whole rule is one question asked at one place: a worker that has just taken
// a candidate adds a peer if NOBODY is left idle. So the crew holds a standing reserve of one, and that
// reserve is what makes a single edge trigger sufficient - see considerGrowth.
//
// It SHRINKS the same way - locally, with no coordinator: a worker that has held a candidate for too little
// of its own recent wall clock retires on a coin flip, never below Min. See considerRetirement.
//
// The gate belongs to the caller: the crew holds *a* gate without knowing what it gates, takes a permit
// before removing work from the cache, and hands it back when the handler says so. It is deliberately NOT
// consulted by the growth rule; blocking on it is what the rule reads instead, since a worker waiting for a
// permit is counted idle.
//
// The cache and the gate, not ctx, are the stop signals: close them, then Drain. Every Set* is safe to
// call from anywhere at any time; Start and Drain are the caller's lifecycle and must not overlap.
package workers

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/errors"
)

// There is deliberately no periodic growth check. A cadence was needed only while `idle == 0` could be a
// RESTING state: growth also required work to be waiting, so a crew that took the last candidate settled
// with nobody parked, and the next arrival had no one to wake and fired no edge (measured: 130 candidates
// against a 96-worker resident set stalled at 96). Spawning whenever idle reaches zero makes zero transient
// instead, so a parked worker is always there to be woken. Pinned by
// TestCrew_WorkArrivingWhileFullyCommittedStillGrows.

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
// Acquire blocks, and reports !ok only when the gate CLOSES - one of the two stop signals. The crew only
// ever ENTERS: whatever the gate does for work on its way out is the handler's business, not the crew's.
//
// It returns a context, which the crew passes to the handler in place of the one it was given. That is how
// a gate that carries per-unit-of-work state - an admission time, a band, whatever identifies this work to
// the resource - gets it to the handler without the crew knowing any of it exists. A gate with nothing to
// add returns the context unchanged.
//
// One method, and no capacity query, is the whole interface on purpose. The crew never asks whether the
// gate has room; it lets a worker BLOCK and reads that instead, because a worker waiting for a permit holds
// no candidate and so counts idle - which stops growth at its own next check. Saturation therefore needs no
// reporting, and a gate that meters more than one thing needs no say in how it is summarised.
type Gate interface {
	Acquire(ctx context.Context, shard int) (gated context.Context, release func(), ok bool)
}

// Crew is a grow-on-demand set of goroutines draining one candidate cache through one gate.
//
// Every Set* is safe to call from anywhere at any time. Start and Drain are the caller's lifecycle and
// must not overlap.
type Crew struct {
	// cache is the candidate source, and its close is what ends every goroutine - see Start.
	cache *candidates.Cache
	// gate bounds concurrent work against the resource the caller cares about.
	gate Gate
	// process handles one popped candidate. What that means is entirely the caller's.
	process ProcessFunc

	// max is the ceiling the crew may grow to, read on every spawn decision rather than captured so a
	// caller that re-derives it takes effect at once. See SetMax.
	max atomic.Int32
	// min is the floor retirement may not cross - the resident set Start was given. Not a setter: it is
	// what the caller already said it wants running unconditionally.
	min atomic.Int32
	// resident is how many goroutines exist. It is BOOKKEEPING plus the two bound checks, not a decision
	// input: what governs growth is idleness and the gate, and what governs retirement is a worker's own
	// busy fraction. EVERY exit decrements it - see work. It must, or a crew that retired most of its
	// goroutines would still believe it holds them and would never spawn again.
	resident atomic.Int32
	// idle is how many goroutines are NOT currently holding a candidate. Zero means the crew is fully
	// committed - nobody is left to take the next one - which is the growth signal.
	//
	// "Not holding a candidate" is deliberately wider than "parked waiting for work", and each of the three
	// states it covers is a worker that will take the next candidate without help, so none is a reason to
	// add another goroutine:
	//   - parked in WaitForWork,
	//   - blocked in the gate's Acquire (it proceeds the moment the gate lets it),
	//   - SPAWNED BUT NOT YET RUNNING. spawn increments this BEFORE the goroutine exists, and it must:
	//     counting only the park makes a freshly-started crew read as fully committed for the instant
	//     before its workers reach it, so Start itself trips the trigger and over-spawns.
	//
	// DO NOT move this to a counter the CALLER maintains around its own off-resource call. Correctness then
	// rests on where the caller puts a brace, and one line too many - wrapping the wait for a database
	// connection - makes saturation read as idleness: measured at ~20% throughput on a saturated 8-vCPU
	// shard (2,902 vs 3,523 steps/s), with ~1,300 goroutines where ~512 sufficed.
	idle atomic.Int32
	// gateWait is an optional observer of how long Acquire blocked. A pointer-to-func in an atomic so
	// it can be set before Start without a lock and read on the hot path without one; nil means unset.
	gateWait atomic.Pointer[func(shard int, waited time.Duration)]
	// spawnLock guards spawnClosed and the resident count against the WaitGroup.Add inside spawn, so the
	// two cannot be observed apart.
	spawnLock sync.Mutex
	// spawnClosed is set under spawnLock before Drain waits on the group. A WaitGroup.Add concurrent with
	// a Wait is a PANIC, and a worker can spawn a peer at any moment, so a flag checked under the same
	// lock that guards the Add - not a counter comparison - is what makes shutdown safe.
	spawnClosed bool
	// group is every goroutine this crew has spawned; Drain waits on it.
	group sync.WaitGroup

	// runCtx is the context Start was given, held because a spawn can happen long afterwards - from a
	// worker taking the last free candidate - and the new goroutine needs the same lifetime as its peers.
	runCtx context.Context

	// The retirement policy, set in New. Plain fields, not atomics, because they are written once before any
	// goroutine exists and only read afterwards - a same-package test rewrites them before Start, which
	// happens-before every worker that reads them. They are POLICY, not configuration: there is no setter,
	// because the rule self-scales and Min and Max already bound it from both ends.

	// retireWindow is how long a worker measures itself over before it may decide. Also the debounce that
	// makes a shrink partial: survivors re-measure a changed world before anyone decides again.
	retireWindow time.Duration
	// retireThreshold is the busy fraction at or above which a worker keeps its place. It sets the
	// equilibrium directly - crew settles at required_concurrency / threshold.
	retireThreshold float64
	// retireChance is the probability a worker under the threshold actually goes. Below 1 so one shared
	// verdict cannot take the whole surplus at once.
	retireChance float64

	// The crew's two ambient dependencies, injected for the same reason anything else here is: a seam inside
	// pure logic would mean a dependency should have been injected instead. There is no I/O to fault, so
	// these are the only things a test cannot otherwise control. Same lifetime rule as the policy above -
	// written in New, read by workers.

	// now is the ONLY source of time in this package, instrumentation included. Injected so a test can drive
	// the retirement window in discrete rounds rather than waiting out wall clock.
	now func() time.Time
	// roll draws the retirement coin, in [0,1). Injected so a test can make a partial shrink exact instead
	// of statistical. MUST be safe for concurrent use - every worker calls it, unsynchronized.
	roll func() float64

	// logger is swapped atomically because SetLogger may be called while goroutines are running.
	logger atomic.Pointer[slog.Logger]
}

// New returns a crew over a cache, the gate that bounds its concurrency, and the callback that handles what
// it pops. Nothing is spawned until Start.
func New(cache *candidates.Cache, gate Gate, process ProcessFunc) (*Crew, error) {
	if cache == nil {
		return nil, errors.New("cache is required")
	}
	if gate == nil {
		return nil, errors.New("gate is required")
	}
	if process == nil {
		return nil, errors.New("process is required")
	}
	c := &Crew{
		cache:           cache,
		gate:            gate,
		process:         process,
		retireWindow:    2 * time.Minute,
		retireThreshold: 0.5,
		retireChance:    0.25,
		now:             time.Now,
		roll:            rand.Float64, // the global source: safe for concurrent use, unlike a *rand.Rand
	}
	c.SetLogger(nil)
	return c, nil
}

// SetMax caps how far the crew may grow. Live: it is read on every spawn decision rather than captured,
// so a caller that re-derives it (from a connection budget, a fleet count, anything) takes effect at once.
//
// It retires nothing itself - lowering it stops growth rather than forcing a shrink. A crew above a lowered
// Max comes down only as its own workers measure themselves surplus, and only as far as Min.
func (c *Crew) SetMax(n int) { c.max.Store(int32(max(0, n))) }

// Max is the current ceiling.
func (c *Crew) Max() int { return int(c.max.Load()) }

// Resident is how many goroutines exist. It rises on demand and falls as workers retire, never below Min.
func (c *Crew) Resident() int { return int(c.resident.Load()) }

// Min is the floor retirement may not cross, set by Start.
func (c *Crew) Min() int { return int(c.min.Load()) }

// Idle is how many goroutines are not currently holding a candidate.
func (c *Crew) Idle() int { return int(c.idle.Load()) }

// SetLogger sets the logger. Nil restores the discarding default.
// SetGateWaitObserver registers a callback invoked with how long each successful Acquire blocked. It
// is optional and unset by default (the crew emits no metrics - instrument names are a public surface, so
// they belong to the owner), and it is called on EVERY acquire including the uncontended ones, so an
// observer that counts sees acquires and one that sums sees the mean.
//
// It exists because the gate's own free-permit count is instantaneous: a reservation saturated for a whole
// window without ever being sampled empty is indistinguishable from an idle one. The wait is the durable
// half. The callback runs on the worker's own goroutine, before it takes any work, so it must not block.
func (c *Crew) SetGateWaitObserver(f func(shard int, waited time.Duration)) {
	c.gateWait.Store(&f)
}

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
// It is ALSO Min, the floor retirement may not cross - which is what makes a caller passing resident == Max
// (a pinned crew) opt out of both growth and retirement without having to say so.
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
	c.min.Store(int32(max(0, resident)))
	for range resident {
		c.spawn()
	}
}

// Drain closes the crew to new goroutines and waits for every existing one to finish. The caller must
// close the cache AND the gate FIRST, or this blocks forever on goroutines parked in one of them.
//
// Closing before waiting is the load-bearing half: a worker can try to spawn a peer at any instant, and an
// Add racing a Wait panics. Only workers spawn, so the flag alone covers it - there is no separate grower
// goroutine to stop first.
func (c *Crew) Drain() {
	c.spawnLock.Lock()
	c.spawnClosed = true
	c.spawnLock.Unlock()
	c.group.Wait()
}

// considerGrowth keeps ONE goroutine in reserve: called by a worker that has just taken a candidate, it
// adds a peer if that take left nobody idle. The question it answers is "now that I am busy, is anyone left
// to take the next one?", and the answer restores the reserve rather than sizing the crew.
//
// The single condition carries three properties that used to need three:
//
//   - IT GROWS FOR LONG TASKS. A worker inside the handler still HOLDS its candidate, so it is not idle -
//     so a crew entirely inside the handler, with work waiting, grows on every take.
//     DO NOT tighten this to "every worker is simultaneously inside the handler": that coincidence's
//     probability decays exponentially in the crew size, which integrates to N(t) ~ (task/db).ln(D.t) -
//     logarithmic growth, so throughput converges to ln(D.t)/db and a long-task workload caps at
//     crew-size-over-task-duration however much capacity sits idle.
//   - IT STOPS ON SATURATION WITHOUT ASKING. Growth must never key on saturation - another goroutine
//     against a saturated resource cannot dispatch, only queue, so such a trigger gets truer the worse it
//     gets (measured: ~20% throughput lost to a crew ~3x the size that sufficed). No capacity query is
//     needed to avoid that: a worker blocked on the gate holds no candidate, so it counts IDLE and the next
//     check declines. The overshoot is exactly one goroutine per saturation episode, and that one is first
//     in line for the next permit. Pinned by TestCrew_SaturationDoesNotGrowThePool.
//   - IT NEEDS NO "WORK IS WAITING" TEST. This runs only after a successful pop, so work demonstrably
//     existed. Asking whether MORE remains is what used to let idle REST at zero when the last candidate
//     was taken - leaving nobody to wake when the next arrived, which is precisely why a periodic check
//     had to exist. Spawning anyway costs one parked worker and buys the reserve that replaced it.
//
// DO NOT add a rate damper here, and do not batch the spawn. Growth is edge-triggered, so a suppressed
// spawn is not deferred, it is LOST - measured, a 1ms damper grew the crew by one past its resident set and
// then stopped forever, serving 97 of 130 parked tasks. Batching is the mirror error: spawning n against
// one available candidate parks n-1, which drives idle to n-1 and suppresses the next n-1 checks. One per
// take is self-matching, since each spawn is paid for by a candidate actually taken.
func (c *Crew) considerGrowth() {
	if c.idle.Load() == 0 {
		c.spawn()
	}
}

// considerRetirement is the mirror of considerGrowth: a worker that has spent less than the threshold of its
// own recent wall clock HOLDING A CANDIDATE retires on a coin flip. It is called at the top of every
// iteration - see work for why that placement, and not the bottom.
//
// window and busy are the caller's own locals, so there is NO SHARED STATE here: no surplus counter, no
// coordinator, no resize to serialize against. A worker decides about itself and leaves by returning.
//
// DO NOT re-key this on items handled instead of time. Per-worker throughput is total/crew, so the
// actuation would move its own signal; and it is duration-confounded, since a worker on 8s tasks handles
// ~7.5 items/min BY DESIGN against thousands for a no-op one - so a fixed item rate retires precisely the
// crew that grow-on-demand deliberately created. A busy FRACTION is neither: that worker reads ~100% busy.
//
// DO NOT drop either control. The coin flip is what keeps one shared verdict from taking the whole surplus
// at once, and the window is what makes the survivors re-measure before anyone decides again.
func (c *Crew) considerRetirement(window *time.Time, busy *time.Duration) bool {
	elapsed := c.now().Sub(*window)
	if elapsed < c.retireWindow {
		return false
	}
	fraction := float64(*busy) / float64(elapsed)
	*window = c.now()
	*busy = 0
	if fraction >= c.retireThreshold || c.roll() >= c.retireChance {
		return false
	}
	return c.retire()
}

// retire gives up this goroutine's place if the crew is above Min, reporting whether it may go.
//
// The check and the decrement are ONE step under spawnLock, and they must be: every worker measures the same
// load and reaches the same verdict at the same instant, so an unreserved check lets an arbitrary number of
// them read "above Min" together and step straight through it. Sharing the lock with spawn is the point -
// the two are the same decision in opposite directions.
func (c *Crew) retire() bool {
	c.spawnLock.Lock()
	defer c.spawnLock.Unlock()
	if c.resident.Load() <= c.min.Load() {
		return false
	}
	c.resident.Add(-1)
	return true
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
//
// RETIREMENT IS EVALUATED AT THE TOP, so a worker decides on every pass - after a job, after losing a pop,
// and on waking from a park however long (the window is wall clock, so a long park reads as a near-zero
// fraction). DO NOT move it to the bottom: there it reaches only workers that got and FINISHED a candidate,
// missing every pass whose wake was consumed without work.
//
// A worker reaches this only by WAKING, and the cache wakes one waiter per candidate rather than
// broadcasting - so a batch smaller than the crew leaves most of it parked on any given refill. That is
// survivable only because its waiters are woken oldest-first, which rotates the surplus round to its
// verdict. That ordering is an implementation detail of sync.Cond rather than a promise of it, and this
// rule is why it matters; internal/candidates pins it with a test rather than assuming it.
func (c *Crew) work(ctx context.Context) {
	// spawn already counted this goroutine as idle, so the loop's invariant on entry - and at the top of
	// every iteration - is "this worker is counted idle". Every return below sits inside that region, so one
	// deferred decrement covers every exit without ever double-counting.
	//
	// resident is decremented on the way out too, and it MUST be: it gates spawning, so a crew that retired
	// most of its goroutines while still believing it held them would never grow again. Retirement is the one
	// exit that has already accounted for itself, under spawnLock - hence the flag rather than a second Add.
	counted := true
	defer func() {
		c.idle.Add(-1)
		if counted {
			c.resident.Add(-1)
		}
	}()
	// Per worker, starting now: a freshly spawned goroutine gets a full window of grace before it can judge
	// itself - it was spawned because somebody needed it.
	window, busy := c.now(), time.Duration(0)
	for {
		if c.considerRetirement(&window, &busy) {
			counted = false
			return
		}
		shard, ok := c.cache.WaitForWork()
		if !ok {
			return // the cache closed
		}
		gateStart := c.now()
		gated, release, ok := c.gate.Acquire(ctx, shard)
		if !ok {
			return // the gate closed: draining
		}
		if f := c.gateWait.Load(); f != nil {
			(*f)(shard, c.now().Sub(gateStart))
		}
		j, ok, _ := c.cache.TryPopFrom(shard)
		if !ok {
			// Another worker took the peeked entry, or the partition drained while we waited. CONTINUE,
			// never return: leaving here would shrink the crew under exactly the contention that caused the
			// race, on a signal that says nothing about whether this worker is surplus - which is what
			// considerRetirement is for, and it runs at the top of the next pass. Still idle - nothing was
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
		// The busy clock brackets exactly the region where this worker HOLDS A CANDIDATE - the same region
		// idle is decremented across. It excludes the park and the wait for a permit, which are what a
		// surplus worker is made of.
		held := c.now()
		err := errors.CatchPanic(func() error {
			return c.process(gated, j.Shard, j.StepID, once)
		})
		once()
		busy += c.now().Sub(held)
		if err != nil {
			c.logger.Load().ErrorContext(ctx, "Processing candidate",
				"stepID", j.StepID, "shard", j.Shard, "error", err)
		}
		c.idle.Add(1) // back to idle, restoring the top-of-loop invariant
	}
}
