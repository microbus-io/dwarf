# Dwarf `internal/permits` — a signed, per-shard, live-resizable semaphore

> Load when: changing the counter's shape, the waiting queues, `Resize`, or what `Close` promises.
> Coupled with: `engine/CLAUDE.md` §"Database-phase permits" (what this gates and why: the phase split,
> the peek-then-acquire ordering, `permitsPerConn = 8`) and `internal/workers/CLAUDE.md` (the `Gate` this
> satisfies, and the release-across-a-callback-boundary contract).

This package is a counter, some condition variables, and no policy. What a permit *means* — which phases
hold one, how many exist, where the release goes — is the engine's, deliberately: the crew takes *a* gate
and the engine gives it meaning. What lives here is the four properties no off-the-shelf semaphore has
together, and each one rules out the obvious alternative.

## Why a mutex + cond over an `int64`, and not a channel

**The count must be able to go BELOW ZERO.** The completion path takes a permit without waiting (`Debit`),
because a goroutine whose side effects have already fired must never queue — the reasoning is in
`engine/CLAUDE.md`, and the consequence is here: a semaphore that cannot represent a debt cannot express
it at all.

- **A buffered channel** encodes the count as buffer occupancy, which is `[0, cap]` by construction. There
  is no fill of −3, and no way to *make* one — you would have to model the debt in a side variable, at
  which point the channel is decoration over a mutex.
- **`x/sync/semaphore`** is worse than merely unable: releasing more than was acquired **panics**
  (`semaphore: released more than held`), so the debit is not a missing feature but a violation of its
  contract.
- Neither can be **resized live**, and both sizes here follow the connection pool, which moves at runtime
  (`Startup`, `recomputePools`, `SetMaxOpenConns`). A channel's capacity is fixed at `make`.

`TestPermits_DebitGoesNegativeAndSuppressesAdmission` pins the property end to end: three debits against a
one-permit shard drive it to −3, and admission stays shut until every `Restore` has paid the debt down
past zero.

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
the only place that knows where the release boundary belongs. The `Debit`/`Restore` pair has the same
exposure with no backstop at all, which is why its contract is "pair them with a `defer` on the next line".

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

## `Available` is a hint, and there is deliberately no `TryAcquire`

`Available` can be stale before the caller acts on it. Its only consumer is the crew's growth decision,
where being wrong costs one goroutine that parks harmlessly — never correctness — so it must not grow a
reservation or a lock hand-off to become accurate.

A non-blocking `TryAcquire` is the missing piece of the cross-shard head-of-line mitigation in
`engine/CLAUDE.md` §"Database-phase permits". **Do not add it speculatively** — add it with the re-peek
that uses it, or it is an untested public surface on a type whose whole job is a bound.

## No fairness, and nothing may start relying on one

`sync.Cond.Signal` wakes an arbitrary waiter, so waiters are not FIFO and a permit is not owed to whoever
waited longest. Permits are interchangeable, so nothing today can tell. If ordering ever becomes load
bearing, it needs an explicit queue — do not assume the cond supplies one.
