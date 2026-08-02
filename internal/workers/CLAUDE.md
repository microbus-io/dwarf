# Dwarf `internal/workers` — the demand side of dispatch

> Load when: changing the growth trigger, the shrink rule, the gate, the drain protocol, or what
> `Start`/`Drain` own.
> Coupled with: `internal/candidates/CLAUDE.md` (the cache this drains, and why blocking on it is what
> shutdown turns on), `internal/turnstile/CLAUDE.md` (the gate the engine supplies; the crew only ever ENTERS it), and `engine/CLAUDE.md` §"Worker
> sizing" (where the resident count, the maximum and the gate's own bound come from, and what the callback does).

Workers are the **demand** side: a `Crew` of goroutines that pop candidates and hand each to one callback.
The **supply** side — deciding which candidates exist to be popped — is `piston`/`pipeline`/`planner`.

**It is a `Crew`, not a "pool", and the name is deliberate.** In this codebase *pool* already means the
database connection pool — `poolsize.go`, `recomputePools`, `shardPool`, `poolsLock` — which is precisely the
resource these goroutines contend for. A type named `Pool` sitting beside `e.db`'s pools, whose whole growth
rule turns on whether a worker is *holding a connection*, would be ambiguous at every call site.

The crew knows nothing about steps, flows, or databases. It owns three things and no more: **how many
goroutines exist, when one more is worth spawning or one is worth losing, and how they shut down without
racing each other.**

## The callback is `process`, deliberately not `ExecuteTask`

The natural-looking boundary — hand the crew the host's task executor — is wrong, and it is worth saying
why so it is not "simplified" back. The engine's worker calls `processStep`, and the host's `ExecuteTask`
happens deep inside it, after the claim CAS, the flow/graph load and state-ref resolution, and before the
persist/transition/fan-in half. A crew that took `ExecuteTask` would have to own all of that — which is not
extracting a worker crew, it is moving the engine into this package.

So the callback is `func(ctx, shard, stepID, release func()) error`: *the crew decides which goroutine runs
what and how many there are; the caller decides what processing means* — and, through `release`, **when its
work stops holding the bounded resource**. That last half is the caller's because only it knows where its own
off-resource call begins; the engine returns it as soon as its first database call is done.

Two consequences visible at the seam:

- **The claim-skip stays with the caller.** The engine's `process` closure does its `TryClaim` and emits its
  own preempted metric before calling `processStep`, so a skipped candidate costs nothing but the next park.
  It releases the permit explicitly on that path rather than leaving it to the backstop, since a preempted
  candidate does no database work at all. That keeps `claimstracker` and the meter out of this package
  entirely — the crew needs neither.
- **The panic catch is the crew's.** A panic escaping a goroutine kills the process, so the loop wraps every
  callback; an error is logged and the goroutine goes back for the next candidate. A worker that exited on an
  error would silently shrink the crew.

## The growth trigger: keep one worker in reserve

```
spawn when:  taking this candidate left NOBODY idle
```

One condition, one call site — a worker that has just popped. There is no capacity query, no work-waiting
test, and no periodic check, and each of those absences is load-bearing rather than incidental (below).

The worker loop is `park holding nothing → take a permit → take work → process`, and **that order is the
design**. A worker that waited for work while holding a permit would hoard admission capacity it is not
using; a worker that popped first and then blocked would strand a candidate inside a goroutine that cannot
proceed with it, having emptied the very partition its peers were choosing between.

**`idle` means "not currently holding a candidate", which is wider than "parked", and the width is
load-bearing.** It covers three states — parked in `WaitForWork`, blocked in `Acquire`, and *spawned but not
yet running*. Each is a worker that will take the next candidate unaided, so none justifies another
goroutine. The third is why `spawn` increments the counter **before** the goroutine exists: count only the
park and a freshly-started crew reads as fully committed for the instant before its workers reach it, so
`Start` itself trips the trigger and over-spawns.

**Do NOT tighten the trigger to "every worker is simultaneously inside the handler."** That coincidence's
probability decays exponentially in the crew size, and integrating `dN/dt = D·e^(−N·db/task)` gives
`N(t) ≈ (task/db)·ln(D·t)` — **logarithmic** growth. Throughput is `N/task`, so the `task` factor cancels
and throughput converges to `ln(D·t)/db`: independent of task duration, far under capacity, and approached
only logarithmically. Measured on a 16-vCPU shard at 600 flows/s with `-task-delay` the only variable:
**6,005 steps/s at 0s, 1,020 at 1s (1,157 goroutines), 637 at 8s (5,298 goroutines)** — against a derived
ceiling of 141,000 and a workload needing ~48,000 concurrent. Eight times the task duration bought 4.6x the
crew and *less* throughput.

**Saturation stops growth WITHOUT the trigger ever asking about it, and that is why `Gate` has one method.**
A worker blocked in `Acquire` holds no candidate, so it counts *idle*, so the next check declines. The
overshoot is exactly **one** goroutine per saturation episode — and that one is first in line for the next
permit, so it is not even waste. `TestCrew_SaturationDoesNotGrowThePool` pins the bound at 3 residents (two
holding both permits, plus the reserve) and fails at `Max` against a build that reads saturation as a reason
to keep spawning — measured against a gate that does not block: the crew ran to 398 where 64 sufficed.

**Do NOT add a capacity query to the gate.** It buys nothing the blocking already gives, while forcing the
gate to *summarise* itself for a consumer that cannot know what it meters — and that summary is rarely
obvious. A gate metering two populations has two candidate answers to "has room" and no correct single one;
worse, the tempting half can be **anti-correlated** with health, free precisely *because* nothing is getting
out. Reading the block instead of the count sidesteps the question rather than answering it wrongly.

**What the caller still owns is *when* the permit is released** — a genuine question only it can answer (the
engine releases as soon as its first database call is done) — and the crew backstops it with `sync.OnceFunc` plus an
unconditional call on the way out, so a leaked permit is **unreachable** rather than merely detected.

### The standing reserve is what makes ONE edge sufficient

A single edge — a worker that has just popped — cannot cover the state that matters most: **once every
worker is committed to a long call, nobody pops**, so work arriving *after* that moment fires no edge at
all. Measured on that shape, 130 candidates against a 96-worker resident set, the crew stalls at 96 with 34
unserved and permits and ceiling both to spare.

That stall needed a periodic check only while `idle == 0` could be a **resting** state, which it was when
growth also required work to be waiting: the worker taking the *last* candidate spawned nothing, so the crew
settled with zero parked workers and the next arrival had nobody to wake.

Spawning whenever a take leaves nobody idle makes `idle == 0` **transient**. There is always a parked worker
for `WaitForWork` to wake, the arrival is noticed, and the cascade takes over — the woken worker pops, finds
nobody idle, and spawns the next. The cost is one worker that parks when the cache happens to be empty,
which *is* the reserve. `TestCrew_WorkArrivingWhileFullyCommittedStillGrows` pins it and hangs against the
work-waiting rule (verified: it stalls at its 2 residents with 8 candidates unserved).

**Do NOT restore a "work is waiting" condition.** It looks free — the trigger runs after a *successful* pop,
so work demonstrably existed, and it only asks whether *more* remains — but that question is exactly what
lets the reserve reach zero, and it is what a cadence then has to paper over.

**Do NOT add a rate damper, and do NOT batch the spawn.** Growth is edge-triggered, so a suppressed spawn is
not deferred, it is **lost** (measured: a 1ms damper grew the crew by one past its resident set and then
stopped forever, serving 97 of 130 parked tasks). Batching is the mirror error: spawning *n* against one
available candidate parks *n−1*, driving `idle` to *n−1* and suppressing the next *n−1* checks. One per take
is self-matching, since every spawn is paid for by a candidate actually taken — and the ramp is bounded by
candidate supply anyway, not by spawn rate (nothing before `considerGrowth` touches the database, so a
spawn→pop→spawn link is tens of microseconds).

What bounds growth is idleness — the cache running dry parks a worker, the gate blocking parks a worker —
and `Max` absolutely. Not a clock.

## The shrink rule: busy FRACTION, with a coin flip

```
retire when:  I held a candidate for less than THRESHOLD of my own last WINDOW
              — and the coin says so — and the crew is above MIN
```

Each worker times itself, in its own two locals, over the region where it **holds a candidate** — the same
region `idle` is decremented across. There is **no shared state**: no surplus counter, no coordinator, no
resize to serialise against. A worker decides about itself and leaves by returning, which is what dissolves
the complexity objection that otherwise keeps retirement out of a worker lifecycle.

The three numbers (2m, 0.5, 0.25) are set in `New` and are **policy, not configuration** — no setter,
because the rule self-scales and `Min` and `Max` already bound it from both ends. A same-package test
rewrites the crew's copies before `Start`, along with the injected clock and coin below.

**TIME, NOT ITEMS — this is the whole design, and the alternative fails the axis check twice.** "Items
handled in the last minute below X" (a) contaminates its own measurement, since per-worker throughput is
`total/crew`, so the actuation moves the signal and the equilibrium `crew = throughput/X` is a disguised
crew-size target competing with whatever else sizes the crew; and (b) is duration-confounded — a worker on
8s tasks handles ~7.5 items/min *by design* against thousands for a no-op one, so a fixed X retires
precisely the crew that grow-on-demand deliberately created. A busy **fraction** is neither. The 8s worker
reads ~100% busy; a genuinely surplus worker reads near zero whatever the task length.

**The equilibrium is the right one, by Little's law:**

    crew = throughput × service_time / threshold = required_concurrency / threshold

`throughput × service_time` **is** the concurrency the offered load needs, so the rule **self-scales with
task duration without ever knowing it** — the same property `Max`'s derivation is built around, reached from
the other direction. That is what makes it pass the axis test, and why there is no knob.

**It is a fuse with hysteresis, not a controller with a setpoint**, and both controls are load-bearing. The
**coin flip** stops a synchronised cliff: every worker sees the same load and reaches the same verdict at the
same instant, so a deterministic rule retires the whole surplus at once and the next arrival finds nobody.
The **window** is a debounce as much as a measurement — it lets the survivors of one round re-measure a
genuinely changed world before anyone decides again.

**Waiting for a permit does NOT count as busy.** That looks aggressive under saturation and is correct: a
goroutine against a saturated resource cannot dispatch, only queue — the same fact that makes a worker
blocked in `Acquire` count *idle* for growth. Growth declining and retirement proceeding are one posture,
and `Min` is what bounds it.

### It is evaluated at the TOP of the loop

So a worker decides on **every pass**: after finishing a job, after losing a pop, and on waking from a park
however long — the window is wall clock, so a worker that parked ten minutes and woke reads a fraction near
zero.

At the bottom a worker would decide only after actually getting and finishing a candidate, so it would miss
every pass where the wake was consumed without work: a lost pop race, or a partition replaced between the
wake and the pop. Top-of-loop costs nothing and misses neither. **No test here discriminates the two
placements**, so do not read the suite's silence as licence to move it.

**What a worker can only reach by WAKING, so the shrink rule is coupled to `Cache.Refill`'s wake protocol.**
Refill wakes one waiter per candidate rather than broadcasting, which means a batch far smaller than the
crew leaves most of it parked on any given refill. That is survivable *only* because `sync.Cond` is FIFO:
signals rotate oldest-first, so every worker is woken periodically and the surplus still gets its verdict —
just once per `parked/batch` refills instead of every one, which is a few seconds at the derived cycle
against a two-minute window. A **LIFO** handoff would deliberately starve cold workers and take retirement
with it. See `internal/candidates/CLAUDE.md`; do not change that protocol without re-reading this.

The one state neither placement covers is a worker parked in a cache that never wakes it again. That is
accepted, not overlooked: such a crew costs nothing but its stacks, and it shrinks on the first work that
arrives. Do **not** add a timer to cover it — a cadence here is what the `control-loops-must-be-simple` bar
exists to keep out, and it would buy a shrink nobody is waiting on.

### `Min`, and why `resident` MUST decrement

`Start`'s resident set is `Min`, so a quiet engine never drops below its target and stalls on first arrival
— and a caller passing `resident == Max` (a pinned crew) opts out of both growth and retirement without
having to say so. `retire` checks `Min` and decrements **in one step under `spawnLock`**, because it must:
every worker measures the same load and reaches the same verdict at the same instant, so an unreserved check
lets an arbitrary number of them read "above `Min`" together and step straight through it. Sharing the lock
with `spawn` is the point — they are the same decision in opposite directions.

**`resident` gates spawning, so every exit decrements it.** A crew that retired most of its goroutines while
still believing it held them **would never grow again** — silently, and only once load returned.
`TestCrew_SurplusRetiresAndTheCrewGrowsAgain`'s third phase is built to catch that: `Max` is set to what
phase 1 grows to, and the assertion reads *goroutines inside the handler*, never `Resident()` — which is the
very counter the failure corrupts, so an assertion on it passes against the broken build. Verified: it does.

`SetMax` retires nothing directly; a crew above a lowered `Max` comes down only as its own workers measure
themselves surplus, and only as far as `Min`.

### The clock and the coin are INJECTED, and no seam belongs here

Everything else the crew touches already arrives as a parameter — the cache, the `Gate`, `process` — so a
test drives it by handing over one that misbehaves. The clock and the coin are the only two that could be
ambient, so they are fields set in `New`. **That is deliberately injection rather than a fault seam.** This
package has no I/O, so there is nothing at a boundary for a seam to perturb; a seam inside pure logic is a
sign a dependency should have been injected instead — which is the same rule `internal/piston` and
`internal/peers` state at their own `SetSeams`, both of which earn theirs at a database call.

Neither is exported. Callers have nothing to tune them against, and the tests are same-package, so they are
plain fields on the same lifetime rule as the policy above: written before any goroutine exists, read
afterwards. `now` is the **only** source of time here, instrumentation included — two notions of time in one
file would mean a reader had to know which was which. `roll` must be safe for concurrent use, which is why
the default is the global `rand.Float64` and never a `*rand.Rand`.

What they buy is that a shrink becomes **discrete and exact** instead of something a test waits out. With
the clock still, a worker's window can only elapse when the test moves it, and a verdict resets that
worker's window to the same still instant — so one advance is exactly **one verdict per worker**. With the
coin a fixed cycle, the proportion that dies is a count, not a tendency. Together they make
`TestCrew_TheCoinFlipShrinksOnlyPartOfTheSurplus` possible at all: it asserts 16 → 12 → 9 exactly, which is
the coin's whole purpose and is unobservable against a real RNG.

**Two traps, both found by mutation testing, both now guarded by helpers:**

- **A worker inside the busy bracket when the clock jumps banks the WHOLE jump as busy time**, reads a
  fraction of 1, and silently skips its verdict — surfacing much later as a count short by one. So `round`
  stops the supply, waits for the crew to quiesce, and only then moves time. (`TestCrew_LongTasksDoNotRetire`
  is the one place that advances without quiescing — there, banking the jump *is* the reading under test.)
- **A broken build descends PAST the right answer.** Drop the `Min` guard and the crew slides through it to
  zero; stop resetting a worker's window and every pass becomes a fresh verdict, so the crew slides to `Min`.
  Both *visit* the correct count in transit, so `eventually(Resident() == n)` passes against either — as it
  did, until mutation testing caught it. Assertions therefore use `settles`, which waits for the size to stop
  moving and returns where it came to **rest**. Do not weaken one back to a transient observation.

## Shutdown: the CACHE **and the GATE** are the stop signals, not `ctx`

```
caller: cache.Close()  →  gate.Close()  →  crew.Drain()
```

**Both closes are mandatory.** A goroutine with nothing to run is blocked in one of two places — parked in
`WaitForWork`, or waiting on a permit in `Acquire` — and each is released only by its own close. Closing the
cache alone leaves anyone blocked on the gate unreachable, and `Drain` then waits on them forever.
Cancelling `ctx` stops neither: `ctx` is what the callback receives, and the caller usually wants it live
until after every in-flight handler has committed its work, which is the other reason it cannot be the stop
signal.

That is also why the lifecycle is `Start`/`Drain` rather than a blocking `Run` (the shape `piston` uses).
`Drain` has to do **two** things in order — close the crew to new goroutines, then wait — and a single
`Run` that blocked in `Wait` could not: growth happens whenever a worker takes a candidate, so the `Add`
would race the `Wait` from the moment `Run` was entered. Workers are the *only* source of spawns, which is
what lets `Drain` be just the flag and the wait — with a separate grower goroutine it would also have to be
stopped and waited on first.

**`spawnClosed` under `spawnLock`, set before the `Wait`, is the load-bearing detail.** A
`WaitGroup.Add` concurrent with a `Wait` is a **panic**, not a race to be tolerated, and a worker can try to
spawn a peer at any instant — including while `Drain` is waiting. A counter comparison cannot close that
window; only a flag checked under the same lock that guards the `Add` can. This protocol is the strongest
argument for the package existing at all: it was previously untestable in isolation, and
`TestCrew_DrainRacesGrowthWithoutPanicking` now drives every worker at the spawn path while `Drain` runs.

## `runCtx` is held on the struct, which is usually a smell

A spawn can happen long after `Start` — from a worker taking a candidate — and the new goroutine needs the
same lifetime as its peers. There is nowhere else to get it from, so `Start` stores it. It is written once
before any goroutine exists and only read by `spawn` thereafter.

## What stays outside

**Sizing.** Both numbers are the caller's: the resident set (what it wants running unconditionally, which
is also `Min`) and `Max` (the ceiling growth may reach). In the engine those are the connection-derived
dispatch count and the lease-margin ceiling; `SetMax` is live because the engine re-derives its ceiling on
every fleet change, while `Min` is fixed at `Start` — the engine does not re-push a resident count, because
the shrink rule already brings the crew down to what a smaller budget can use.

**Metrics.** The crew emits none. `Resident`/`Idle`/`Min`/`Max` are exposed so an owner can build a gauge
from them, which keeps instrument names — a public surface dashboards bind to — with the owner. The engine
publishes `dwarf_workers_resident` from the first, and reads it against `dwarf_turnstile_available` (the
gate's own count) to tell the two long-task regimes apart: a crew far above the permit count *with permits
free* is serving long tasks correctly, while a crew pinned at `Max` is the one to alarm on. The count falls
as well as rises: climbing under load and settling back is the shrink rule working, wide oscillation at
steady load is not.

**The gate.** Sized, closed, and given meaning by the owner. The crew never asks what it bounds — in the
engine it is a multiple of that shard's connection pool, ordered by band and then by how long the asking job has been running,
and the release point is "as soon as the first database call is done". All of those are engine facts the crew would be
wrong to encode, which is why `Gate` is one blocking method and no accessor.
