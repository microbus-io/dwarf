# Dwarf `internal/workers` — the demand side of dispatch

> Load when: changing the growth trigger, the gate, the drain protocol, or what `Start`/`Drain` own.
> Coupled with: `internal/candidatecache/CLAUDE.md` (the cache this drains, and why blocking on it is what
> shutdown turns on), `internal/permits/CLAUDE.md` (the gate the engine supplies), and `engine/CLAUDE.md` §"Worker
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

## The growth trigger: idleness, gated by a permit

```
spawn when:  nobody is IDLE  AND  work is waiting  AND  the gate has room
```

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

**The gate is what makes so easy a trigger safe, and the two are one design.** Growth stops exactly where
extra goroutines would become contention rather than capacity: with the bounded resource saturated there are
no permits, so nothing spawns. `TestCrew_SaturationDoesNotGrowThePool` pins it, expressing "saturated" as
*every permit held*; it is the test whose absence lets the runaway above ship.

**What the caller still owns is *when* the permit is released** — a genuine question only it can answer (the
engine releases immediately before `ExecuteTask`) — and the crew backstops it with `sync.OnceFunc` plus an
unconditional call on the way out, so a leaked permit is **unreachable** rather than merely detected.

### Growth needs BOTH an edge and a cadence

`considerGrowth` runs on two paths, and one is not enough:

- **The edge** — a worker that has just taken a candidate. Zero latency, and it *cascades*: the worker it
  spawns takes a candidate, finds nobody idle and work still waiting, and spawns the next.
- **The cadence** — `growLoop`, every `growCheckInterval` (5ms), unconditionally.

An edge cannot cover the state that matters most. **Once every worker is committed to a long call, nobody
pops**, so work arriving *after* that moment fires no edge at all: the crew sits at its resident size with
candidates queued in front of it, permits and ceiling both to spare. Measured on that shape — 130 parked
tasks against a 96-worker resident set — an edge-only crew stops at 96, leaving 34 candidates unserved. This
is the same posture as the supply side's piston **cycling unconditionally rather than on a trigger**, for
the same reason: a trigger must be fired from every site that can create the condition, and missing one
wedges silently.

One spawn per tick is sufficient precisely because the edge cascades from it — the cadence only has to cover
the standing start.

**Do NOT add a rate damper.** Growth is edge-triggered, so a suppressed spawn is not deferred, it is
**lost**, and in the very case this exists for there is no later pop to re-evaluate on. Measured: a 1ms
damper grows the crew by exactly one past its resident set and then stops forever, serving 97 of 130 parked
tasks. What bounds growth is the three conditions — work waiting caps it at the queue, the gate caps it at
the resource, `Max` caps it absolutely — not a clock.

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
`Run` that blocked in `Wait` could not: growth happens whenever a worker takes a candidate — and on the
grower's own cadence — so the `Add` would race the `Wait` from the moment `Run` was entered. (The grower is
stopped and waited on *first* inside `Drain`, since it is a second, non-worker source of spawns.)

**`spawnClosed` under `spawnLock`, set before the `Wait`, is the load-bearing detail.** A
`WaitGroup.Add` concurrent with a `Wait` is a **panic**, not a race to be tolerated, and a worker can try to
spawn a peer at any instant — including while `Drain` is waiting. A counter comparison cannot close that
window; only a flag checked under the same lock that guards the `Add` can. This protocol is the strongest
argument for the package existing at all: it was previously untestable in isolation, and
`TestCrew_DrainRacesGrowthWithoutPanicking` now drives every worker at the spawn path while `Drain` runs.

## `runCtx` is held on the struct, which is usually a smell

A spawn can happen long after `Start` — from a worker taking a candidate, or from the grower's cadence —
and the new goroutine needs the same lifetime as its peers. There is nowhere else to get it from, so `Start` stores it. It is written once before
any goroutine exists and only read by `spawn` thereafter.

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
engine it is `permitsPerConn x` that shard's connection pool, and the release point is "immediately before
the host call", both of which are engine facts the crew would be wrong to encode.
