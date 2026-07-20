# Dwarf `internal/candidatecache` — bounded step-candidate hint cache

> Load when: changing the cache's `Offer`/`Refill`/`Pop`/`floor`/`lowWater` semantics.
> Coupled with: `engine/CLAUDE.md` §"Execution Model" — the refiller's two-level priority+fairness selection and
> the doorbell/single-slot-refiller protocol that *drive* this cache live there, not here.

This package is only the **mechanism**: a small, per-replica bounded set of step *candidates*. The invariant an
editor must not break is that entries are **hints, not ownership** — a worker pops a candidate and then
CAS-acquires the underlying step before executing, so a stale, duplicated, or already-claimed candidate is
harmless (the CAS loser just pops the next one). Nothing here may assume a popped candidate is still runnable.

**`Refill` is a wholesale replace, and that includes an EMPTY batch** - it empties the cache and resets the floor,
exactly as draining the last item through `Pop` does. An empty batch is the scan's statement that *nothing is due*, so
every still-cached candidate is a step that is no longer pending: a dead hint a worker would pop and burn a claim-CAS
round-trip on, under a floor advertising a band the cache no longer holds. (`Refill` used to early-return on
`len(batch)==0`, keeping the whole dead batch. Nothing broke - a CAS loser re-requests a refill, so the cache
self-corrects within a cycle - which is exactly why it went unnoticed; it was wasted work and a lying floor, not a
wedge.)

The corollary is a rule on the **caller**, and it is the sharp edge: a *failed* scan must **not** reach `Refill`. An
error means "unknown", not "nothing is due", and now that the empty batch is honored, routing one here would
wholesale-replace a healthy cache with nothing because the database blipped - idling every worker in `Pop` until the
1s re-poll. `runRefill` returns early on a scan error instead (`engine/scheduling.go`), keeping the existing hints;
they cost nothing, since a worker popping a stale one just loses its claim CAS. Pinned by
`TestCandidateCache_RefillEmptyIsWholesale` and `engine`'s `TestFault_RefillScanErrPreservesCache`.

**`Refill` returns the number of candidates it discarded un-popped**, which the engine feeds to
`dwarf_refill_candidates_discarded`. It is a *measurement* of the wholesale replace, not a new behavior:
the refiller is triggered after every `processStep` and, under a deep backlog, turns faster than the
workers drain, so a replace routinely drops a batch the previous pass paid a round-trip to fetch. Discarded
steps stay `pending` and are re-selected, so this is cost, never loss - but the ratio against the batch size
is the only visibility into how much of the refiller's work is thrown away, and nothing else in the engine
can see it. A **closed** cache reports `0`: the replace is a no-op there, and counting it would double-count
candidates the drain already abandoned.

The `floor`/`lowWater`/`Offer`-vs-`Refill` behavior is a protocol with the engine's refiller: any change to their
semantics must stay consistent with the selection algorithm described in the engine doc. The godoc on each method
states the local contract; the *why* (priority bands, urgent-arrival head-insert, liveness after `processStep`)
is the engine's.
