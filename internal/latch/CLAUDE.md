# Dwarf `internal/latch` — parking callers on a key until it settles

> Load when: changing how callers are woken (`Latch`/`Release`/`Close`), what a sweep asks for, or the
> `StatusResolver` contract.
> Coupled with: `internal/pipeline/CLAUDE.md` (the same "no goroutine, the owner drives it" shape and the
> same never-return-an-error argument), and `engine/CLAUDE.md` §"Await" (the SQL behind
> `StatusResolver`, where the cadence comes from, and what a status means).

A `Board` owns three things and no more: **who is waiting on what, one pass that asks which of those keys
are done, and how a parked caller is woken.** Everything else — the query, the cadence, the goroutine,
and what a status string *means* — belongs to the owner.

## Polling is what makes registration order irrelevant

This is the property the whole design rests on, and it is easy to lose by "improving" the wake path.

A wake delivered as an **event** must be armed before the event fires, so arming late loses it forever —
which is why an event-based waiter has to interleave its registration and its first read *just so*. A
**sweep** asks for the current state of every latched key instead, so a key that settled before anyone
latched onto it is reported by the very next sweep.

Registration order is therefore a **latency** question here and never a correctness one. **Do not add an
arm-then-read protocol, a "check once at registration" step, or a first-sweep trigger to close a race
that polling has already removed.** A caller that cannot afford one sweep of latency checks the key
itself before parking — which the engine does anyway, because it needs the full outcome and not just a
status.

## The board decides nothing about status

`StatusResolver` reports only the keys whose wait is over; an omitted key stays latched. So "which
statuses end a wait" lives with the caller that knows what a status *is*, and this package never grows
a predicate, a terminal-status list, or an opinion about what an absent row means.

The alternative — hand the board a `settled func(string) bool`, or a list of terminal statuses — was
rejected because it splits one decision across two packages: the owner would still choose what to query
and how to read a missing row, while the board held half the rule and would have to be kept in step with
`workflow`'s status vocabulary forever. A caller that wants a vanished key to wake its waiter simply
reports it with whatever status says so.

## Close travels the same channel as a status, and that is not a style choice

Each parked caller holds one buffered slot. `Release` sends a status into it; `Close` sends a closure
into it. Because the slot holds one thing, **a delivered status leaves no room for the closure** — a
caller whose key was reported just before shutdown is told the answer it asked for.

The first cut used a separate `done` channel that `Close` closed, with the parked caller selecting over
both. That is wrong in a way worth recording: **a `select` over two ready channels picks at random**, so
a caller released microseconds before shutdown got `ErrClosed` about half the time and its real status
the other half. Non-determinism, not a policy.

It was also **untestable**, which is how the flaw surfaced. Breaking the defensive branch (making the
close path always answer `ErrClosed`) did not fail the test written for it, because the parked goroutine
consumes its status the instant `Release` delivers it and the window never opens. If you find yourself
writing a test that repeats an operation N times hoping to catch an interleaving, the mechanism — not the
test — is what needs fixing.

**`TestClose_CannotDisplaceADeliveredRelease` places the waiter's channel on the board by hand rather
than parking a goroutine**, precisely because the window it pins is the one where the caller has not been
scheduled yet. Keep it that way; a goroutine-based version passes against a broken board.

## Registration and Close share one lock

`Latch` checks `closed` and appends its channel under the same lock `Close` holds while it broadcasts, so
a caller is **either** on the board in time to be woken **or** turned away with `ErrClosed` — never
neither. Splitting those (an atomic `closed` flag plus a separately-locked map, say) reopens the gap.

## `Sweep` returns a `Result`, never an `error`

Same hazard `pipeline` documents. The reflex a returned error invites is:

```go
for range ticker.C {
    if err := board.Sweep(ctx); err != nil {
        return          // <- detection ends for the life of the process
    }
}
```

Every parked caller then waits out its own context while the flows they are waiting for sit settled in
the database. There is no decision for the caller to make on a failed pass — the next sweep asks again —
so `Result.Err` routes the failure to the log line where it belongs.

**A partial answer is applied before the error is examined.** A `StatusResolver` that resolved three of
four keys must not hold the three back over the fourth; the unreported key is simply asked about again
next pass. This matters for the engine, where `StatusResolver` fans out over shards and one shard can
fail alone.

## Two small invariants that are load-bearing

**The key set is sorted.** Map iteration order is random, so an unsorted set hands `StatusResolver` a
fresh split of the same keys on every pass — which churns the prepared-statement cache of any implementation
that chunks or pads its `IN` list. Sorting costs nothing at these sizes and makes the batch boundaries
stable.

**A key with no waiters is deleted, not left empty.** An empty slice left behind is a key
`StatusResolver` is asked about forever on behalf of nobody, growing without bound over a long-lived
process.

## Concurrency

`Latch`, `Release`, `Latched` and `Close` are safe from anywhere. **`Sweep` is not** — one driver
goroutine is the intended shape, and it is what lets `StatusResolver` be written as if it were the only
caller of whatever it queries. `Release` sends outside the lock and never blocks: a caller that has
already been released, or gone home on its context, must not hold up the rest of the key's waiters.
