# Dwarf `internal/permits` — a signed, per-shard, live-resizable semaphore

> Load when: changing the counter's shape, the waiting queues, `Resize`, or what `Close` promises.
> Coupled with: `engine/CLAUDE.md` §"Database-phase permits" (what this gates and why: the phase split,
> the peek-then-acquire ordering, `permitsPerConn`) and `internal/workers/CLAUDE.md` (the `Gate` this
> satisfies, and the release-across-a-callback-boundary contract).

This package is a counter, some condition variables, and no policy. What a permit *means* — which phases
hold one, how many exist, where the release goes — is the engine's, deliberately: the crew takes *a* gate
and the engine gives it meaning. What lives here is the four properties no off-the-shelf semaphore has
together, and each one rules out the obvious alternative.

## Why a mutex + cond, and not two buffered channels

Two channels would express the two reservations perfectly well — until they have to be **resized live**,
which is the one thing this must do: both ceilings follow the connection pool, which moves at runtime
(`Startup`, `recomputePools`, `SetMaxOpenConns`). A channel's capacity is fixed at `make`, so a resize
means swapping the channel out from under in-flight holders and reconciling what they will hand back.

`Resize` instead moves each available count by its **delta**, never to the new value, so permits held right
now stay held — assigning the ceiling outright would hand those out a second time. Shrinking below what is
held drives that count negative, which simply blocks that side until enough holders release. That is the
only way a count goes below zero: neither acquire path can. Pinned by `TestPermits_ResizeMovesByDelta`.

## One mutex, but a condition variable PER SHARD

This split is **correctness, not tuning**, and it is the one place a plausible simplification is silently
wrong. A waiter always waits on exactly one shard's count. With a single shared cond, a release on shard 2
can `Signal` a goroutine waiting on shard 5: it wakes, re-checks its own count, finds nothing, and sleeps
again — while shard 2's waiter is never woken and its free permit sits unused. That is a **lost wakeup**,
nothing detects it, and admission on the shard with capacity simply stops.

`Broadcast` on every release would be correct and wakes every waiter on the hottest path there is, to have
all but one re-sleep. Per-shard conds over the **same** mutex give precise wakeups with no herd: a cond is
only a waiting queue, so sharing the lock is what keeps the counts atomic across shards while separating
who gets woken.

`TestPermits_ReleaseWakesTheRIGHTShardsWaiter` drives exactly the bad interleaving — every shard exhausted,
one waiter parked on each, then a single release on shard 1 — and asserts both halves: shard 1's waiter
*must* proceed, and no other may.

**`Signal` on a release, `Broadcast` on a resize or a close.** One permit satisfies at most one `Acquire`,
so a release wakes one. A grow of `n` frees up to `n` waiters at once, so waking one would strand the rest
on permits that are already free; a shrink wakes them to no effect, which is cheaper than telling the two
cases apart.

## `Resize` moves `avail` by the DELTA, never assigns it

Assigning the new ceiling — the obvious spelling — hands out permits that in-flight holders are still
using, silently and permanently over-issuing the bound. Moving by the delta keeps held permits held.
Shrinking below what is currently held drives the count negative, which blocks admission until enough
holders release: the same self-correcting shape the debit relies on, so the shrink needs no special case.
Pinned by `TestPermits_ResizeMovesByDelta`.

## An unsized shard admits NOTHING, and that is the fail-closed direction

`avail` and `size` are maps, so a shard nobody sized reads zero and blocks. That is free rather than
checked, and it is the right way round for a configuration mistake: a stalled shard is visible in
`dwarf_permits_available` and in throughput, whereas an unbounded one is the collapse the gate exists to
prevent. The engine sizes every shard in `initRuntime` before any worker starts.
Pinned by `TestPermits_UnsizedShardAdmitsNothing`.

Conds are likewise created on demand and never removed — the shard set is fixed for a run, so there is
nothing to garbage-collect.

## The release is not idempotent, and the two ways to get it wrong are not symmetric

- A **double release** hands out a permit that was never taken: the bound inflates permanently, silently,
  and only upward.
- A **leaked release** is worse: admission decays with every leak until it stops entirely, and the shard
  wedges.

**Do not "fix" this by wrapping the closure in `sync.OnceFunc` here.** It would cover only the first
failure, on the hot path, for every acquire — while the second, the one that wedges a shard, still needs
the caller to call the release *unconditionally on the way out*. One mechanism at the acquiring site
(`OnceFunc` plus an unconditional call, which `internal/workers` does) covers both directions, and it is
the only place that knows where the release boundary belongs. `AcquireExit`'s release carries the same
exposure with no backstop at all, which is why its contract is "defer it on the next line".

Neither failure is detected in this package, by decision: the readout is the engine's signed
`dwarf_permits_available` gauge, where a bound drifting away from `permitsPerConn x pool` is visible.

## `Close` is a latch, not a state

It permanently unblocks every waiter and `Acquire` reports `!ok` **forever after** — there is no reopen,
mirroring the candidate cache's close. Both are mandatory at shutdown: a worker with nothing to run is
parked in one or waiting on the other, and closing only the cache leaves permit-waiters unreachable, so
`Crew.Drain` waits on them forever. `TestPermits_CloseReleasesEveryWaiter` pins that hang.

`Acquire` returns a **no-op release** alongside `ok=false`, so a caller that defers or backstops the
release unconditionally cannot nil-panic on the drain path.

**There is no `ctx` on `Acquire`.** The close is the stop signal, for the reason `internal/workers` records:
the context the caller holds usually has to stay live until in-flight work has committed, so it cannot also
mean "stop waiting".

## There is no capacity accessor, and no `TryAcquire`

**Nothing here reports whether a shard "has room", and do not add it back.** An `Available(shard)` existed
for the crew's growth decision and was deleted: the crew reads a worker BLOCKING instead, which is strictly
better information and needs no cooperation from this package. A blocked worker holds no candidate, so it
counts idle, so growth declines on its own.

The reason it cannot simply come back is that **this type has nothing to summarise itself as.** With two
reservations, "has room" has two candidate answers and no correct single one — and the tempting choice is
actively misleading: free ENTER permits become *anti-correlated* with health once the exit side is the
constraint, since they are free precisely because nothing is getting out. A consumer that cannot know what
is metered must not be handed a one-bit verdict about it.

A non-blocking `TryAcquire` is the missing piece of the cross-shard head-of-line mitigation in
`engine/CLAUDE.md` §"Database-phase permits". **Do not add it speculatively** — add it with the re-peek
that uses it, or it is an untested public surface on a type whose whole job is a bound.

What remains for observation is `Snapshot`, which reports both counts per shard for metrics and makes no
claim about what they mean.

A non-blocking `TryAcquire` is the missing piece of the cross-shard head-of-line mitigation in
`engine/CLAUDE.md` §"Database-phase permits". **Do not add it speculatively** — add it with the re-peek
that uses it, or it is an untested public surface on a type whose whole job is a bound.

## Two reservations, not one pool with a priority rule

**Entering and exiting work are counted separately, and that is the whole design.** A single pool forces a
choice about who wins a contested permit, and BOTH answers were measured failing:

- **Served evenly** (a plain semaphore), exits lose at random: `sync.Cond.Signal` wakes an arbitrary
  waiter, and an arriving entrant can take a permit before a woken exiter runs. Measured on a saturated
  local rig: **286 exits waited a full second** while entries kept winning releases.
- **Served with exits given precedence**, entries starve. When work is instant the exit queue never
  empties, so holding entrants off blocks entry continuously — and entry IS dispatch. Measured on a
  saturating cloud rig: **short-task throughput collapsed 3x (4,416 vs 7,964 steps/s)**, with creation
  itself throttled to 714 of 800 commanded.

Separate reservations remove the choice rather than answering it. Neither side can be blocked by the other
because they never queue on the same count, so **both sides can simply block** — no priority rule, no
timeout, no escape hatch past the bound. `TestPermits_ReservationsAreIndependent` pins both directions.

**An exit blocking cannot deadlock**, which is what makes the simplification safe: an exit waits only
behind other exits, and those are themselves finishing. Nothing an entering caller does can hold an exit
permit. What bounds the worst case — every in-flight caller finishing at once — is the owner's own
concurrency ceiling, not this type.

**Sizing is the caller's, per reservation** (`Resize(shard, enter, exit)`). Size them by where the work
actually is: the engine gives exits the larger share because most of a step's round trips are on that side,
and an even split starved exactly the half that holds the resource longest.

**A caller that gets `!ok` from `AcquireExit` must still do its work.** Close means the owner is draining,
not that the work may be dropped — its outcome exists nowhere else. The release is a no-op then, so calling
it unconditionally is always safe.
