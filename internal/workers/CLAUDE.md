# Dwarf `internal/workers` — the demand side of dispatch

> Load when: changing the growth trigger, the gate, the drain protocol, or what `Start`/`Drain` own.
> Coupled with: `internal/candidatecache/CLAUDE.md` (the cache this drains, and why blocking on it is what
> shutdown turns on), `internal/permits/CLAUDE.md` (the gate the engine supplies; the crew only ever ENTERS it), and `engine/CLAUDE.md` §"Worker
> sizing" (where the resident count, the maximum and the permit count come from, and what the callback does).

Workers are the **demand** side: a `Crew` of goroutines that pop candidates and hand each to one callback.
The **supply** side — deciding which candidates exist to be popped — is `piston`/`pipeline`/`planner`.

**It is a `Crew`, not a "pool", and the name is deliberate.** In this codebase *pool* already means the
database connection pool — `poolsize.go`, `recomputePools`, `shardPool`, `poolsLock` — which is precisely the
resource these goroutines contend for. A type named `Pool` sitting beside `e.db`'s pools, whose whole growth
rule turns on whether a worker is *holding a connection*, would be ambiguous at every call site.

The crew knows nothing about steps, flows, or databases. It owns three things and no more: **how many
goroutines exist, when one more is worth spawning, and how they shut down without racing each other.**

## The callback is `process`, deliberately not `ExecuteTask`

The natural-looking boundary — hand the crew the host's task executor — is wrong, and it is worth saying
why so it is not "simplified" back. The engine's worker calls `processStep`, and the host's `ExecuteTask`
happens deep inside it, after the claim CAS, the flow/graph load and state-ref resolution, and before the
persist/transition/fan-in half. A crew that took `ExecuteTask` would have to own all of that — which is not
extracting a worker crew, it is moving the engine into this package.

So the callback is `func(ctx, shard, stepID, release func()) error`: *the crew decides which goroutine runs
what and how many there are; the caller decides what processing means* — and, through `release`, **when its
work stops holding the bounded resource**. That last half is the caller's because only it knows where its own
off-resource call begins; the engine releases immediately before `ExecuteTask`.

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
load-bearing.** It covers three states — parked in `WaitForWork`, blocked in `AcquireEnter`, and *spawned but not
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
A worker blocked in `AcquireEnter` holds no candidate, so it counts *idle*, so the next check declines. The
overshoot is exactly **one** goroutine per saturation episode — and that one is first in line for the next
permit, so it is not even waste. `TestCrew_SaturationDoesNotGrowThePool` pins the bound at 3 residents (two
holding both permits, plus the reserve) and fails at `Max` against a build that reads saturation as a reason
to keep spawning.

**Do NOT reintroduce a capacity query on the gate.** It was there (`Gate.Available`, backed by
`permits.Available`) and it bought nothing the blocking already gives, while forcing the gate to *summarise*
itself for a consumer that cannot know what it meters. That summary is not obvious: with an enter/exit
split, "has room" has two candidate answers, and picking the enter side alone makes free permits
**anti-correlated** with health once the exit path is the constraint — free precisely *because* nothing is
getting out. Reading the block instead of the count sidesteps the question rather than answering it wrongly.

**What the caller still owns is *when* the permit is released** — a genuine question only it can answer (the
engine releases immediately before `ExecuteTask`) — and the crew backstops it with `sync.OnceFunc` plus an
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

## `resident` grows, never shrinks

Nothing retires a goroutine, and `SetMax` lowering below the current count stops growth without shrinking.
That is what makes `idle <= resident` an invariant rather than a hope, and it is a deliberate choice
elsewhere: retiring workers needs a surplus counter, a retirement check on the hot loop, resize
serialisation, and an interaction with the drain — a control protocol added to the lifecycle to reclaim
goroutine stacks. The surplus merely queues, so the whole prize is memory.

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

**Sizing.** Both numbers are the caller's: the resident set (what it wants running unconditionally) and
`Max` (the ceiling growth may reach). In the engine those are the connection-derived dispatch count and the
lease-margin ceiling, re-derived on every fleet change — `SetMax` is live for exactly that reason.

**Metrics.** The crew emits none. `Resident`/`Idle`/`Max` are exposed so an owner can build a gauge from
them, which keeps instrument names — a public surface dashboards bind to — with the owner. The engine
publishes `dwarf_workers_resident` from the first, and reads it against `dwarf_permits_available` (the
gate's own count) to tell the two long-task regimes apart: a crew far above the permit count *with permits
free* is serving long tasks correctly, while a crew pinned at `Max` is the one to alarm on.

**The gate.** Sized, closed, and given meaning by the owner. The crew never asks what it bounds — in the
engine it is a multiple of that shard's connection pool, split into separate enter and exit reservations,
and the release point is "immediately before the host call". All of those are engine facts the crew would be
wrong to encode, which is why `Gate` is one blocking method and no accessor.
