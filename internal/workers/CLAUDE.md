# Dwarf `internal/workers` — the demand side of dispatch

> Load when: changing the growth trigger, `Offsite`, the drain protocol, or what `Start`/`Drain` own.
> Coupled with: `internal/candidatecache/CLAUDE.md` (the cache this drains, and why `Pop` blocking is what
> shutdown turns on), and `engine/CLAUDE.md` §"Worker sizing" (where the resident count and the maximum
> come from, and what the callback actually does).

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

So the callback is `func(ctx, shard, stepID) error`: *the crew decides which goroutine runs what and how
many there are; the caller decides what processing means.*

Two consequences visible at the seam:

- **The claim-skip stays with the caller.** The engine's `process` closure does its `TryClaim` and emits its
  own preempted metric before calling `processStep`, so a skipped candidate still costs nothing but the next
  `Pop`. That keeps `claimstracker` and the meter out of this package entirely — the crew needs neither.
- **The panic catch is the crew's.** A panic escaping a goroutine kills the process, so the loop wraps every
  callback; an error is logged and the goroutine goes back for the next candidate. A worker that exited on an
  error would silently shrink the crew.

## `Offsite` — the growth signal, and the one rule that matters

```go
onsite := crew.Offsite()
... the off-resource call ...
onsite()
```

The crew grows by one when **every** goroutine it has is away, because a replacement is then pure added
capacity rather than more contention for whatever resource actually binds.

**THE SCOPE MUST BE THE OFF-RESOURCE CALL AND NOTHING ELSE.** This is the entire contract, and getting it
wrong has a measured price. An earlier version of this counter lived in the engine and wrapped the whole
handler — which includes *waiting for a database connection*. That made "every worker away" mean "every
worker saturated": any backlog grew the crew, each new goroutine queued on the same connections, and the
signal got truer the worse it got. **~20% throughput on a saturated shard, and a crew bloated to ~1,300
goroutines where ~512 sufficed.** The question to ask of anything placed inside the scope: *does this
goroutine hold the scarce resource?* If it might, the scope is wrong. Both halves are pinned —
`TestCrew_SaturationDoesNotGrowThePool` is the one whose absence let that bug ship.

**The release is not deferred by the crew, on purpose.** A `defer` at the caller's function scope would hold
the state across everything *after* the off-resource call — which is precisely the bug above wearing a
different hat. The caller releases on the next straight line. (A `Offsite(fn func() error)` wrapper would
make the scope structural, and was considered; it costs an extra closure layer around one that already
exists at the single call site, and the caller's `CatchPanic` already guarantees control reaches the
release.)

**A leaked release is DETECTED, not prevented.** `offsite` can never legitimately exceed `resident` — a
goroutine is away at most once at a time, `resident` is how many exist, and it never shrinks — so exceeding
it means a release was skipped, or something that is not a crew goroutine called `Offsite`. That is a free
assertion: the spawn decision already compares those two counters.

It surfaces only once the crew is **at `Max`**, and that is inherent rather than a weakness: below the
ceiling each leaked increment spawns a replacement, so `resident` stays ahead and the leak is absorbed *as
growth*. Pinned at `Max` with nobody able to dispatch is the state a leak leaves behind, and the state worth
naming. Same footing as `dwarf_steps_unwedged`: nonzero means a latent bug, detection only, no repair.

## `resident` grows, never shrinks

Nothing retires a goroutine, and `SetMax` lowering below the current count stops growth without shrinking.
That is what makes `offsite <= resident` an invariant rather than a hope, and it is a deliberate choice
elsewhere: retiring workers needs a surplus counter, a retirement check on the hot loop, resize
serialisation, and an interaction with the drain — a control protocol added to the lifecycle to reclaim
goroutine stacks. The surplus merely queues, so the whole prize is memory.

## Shutdown: the CACHE is the stop signal, not `ctx`

```
caller: cache.Close()  →  crew.Drain()
```

A goroutine with nothing to run is blocked in `Pop`, which only a close releases — so cancelling `ctx` does
not stop the crew, and `Drain` before the close would block forever. `ctx` is what the callback receives,
and the caller usually wants it live until after every in-flight handler has committed its work, which is
the other reason it cannot be the stop signal.

That is also why the lifecycle is `Start`/`Drain` rather than a blocking `Run` (the shape `piston` uses).
`Drain` has to do **two** things in order — close the crew to new goroutines, then wait — and a single
`Run` that blocked in `Wait` could not: growth happens whenever a worker goes offsite, so the `Add` would
race the `Wait` from the moment `Run` was entered.

**`spawnClosed` under `spawnLock`, set before the `Wait`, is the load-bearing detail.** A
`WaitGroup.Add` concurrent with a `Wait` is a **panic**, not a race to be tolerated, and a worker can try to
spawn a peer at any instant — including while `Drain` is waiting. A counter comparison cannot close that
window; only a flag checked under the same lock that guards the `Add` can. This protocol is the strongest
argument for the package existing at all: it was previously untestable in isolation, and
`TestCrew_DrainRacesGrowthWithoutPanicking` now drives every worker at the spawn path while `Drain` runs.

## `runCtx` is held on the struct, which is usually a smell

A spawn can happen long after `Start` — from a worker going offsite — and the new goroutine needs the same
lifetime as its peers. There is nowhere else to get it from, so `Start` stores it. It is written once before
any goroutine exists and only read by `spawn` thereafter.

## What stays outside

**Sizing.** Both numbers are the caller's: the resident set (what it wants running unconditionally) and
`Max` (the ceiling growth may reach). In the engine those are the connection-derived dispatch count and the
lease-margin ceiling, re-derived on every fleet change — `SetMax` is live for exactly that reason.

**Metrics.** The crew emits none. `Resident`/`AwayCount`/`Max` are exposed so an owner can build a gauge
from them, which keeps instrument names — a public surface dashboards bind to — with the owner.
