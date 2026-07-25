# Dwarf `internal/candidatecache` — bounded, shard-partitioned step-candidate hint cache

> Load when: changing the cache's `Offer`/`Refill`/`Pop`/floor/low-water/partition semantics.
> Coupled with: `engine/CLAUDE.md` §"Execution Model" — the per-shard refillers' global-plan selection and
> the doorbell/per-shard-trigger protocol that *drive* this cache live there, not here.

This package is only the **mechanism**: a small, per-replica bounded set of step *candidates*, partitioned
by shard. The invariant an editor must not break is that entries are **hints, not ownership** — a worker
pops a candidate and then CAS-acquires the underlying step before executing, so a stale, duplicated, or
already-claimed candidate is harmless (the CAS loser just pops the next one). Nothing here may assume a
popped candidate is still runnable.

**One cache, N partitions — not N caches.** Each shard's refiller wholesale-replaces its own partition
(`Refill(shard, batch, floor)`), and `Pop` selects across partitions: **lowest floor wins** (that is the
entire dispatch rule), ties break by depth (deepest first — round-robin would hand every shard an equal
share of workers regardless of backlog), then by shard index for determinism. One mutex and ONE condition
variable span all partitions: N cond vars would let a worker blocked on shard 3's emptiness sleep through
shard 1 filling.

**The floor is DERIVED (head item's priority), never stored — this is load-bearing.** The floor used to be
a stored field read only by `Offer`'s admission check, where a stale value was benign. Partition selection
promoted the floor to the dispatch key, and a stored floor lies after an `Offer` head-insert: one band-1
item over a band-5 body advertises floor=1; the band-1 head pops; the stored floor keeps saying 1, so
`Pop` prefers that partition and drains its entire band-5 body ahead of another partition's band-3 body —
up to a full partition of inversion for up to a full cycle. Deriving from `items[0]` is *exact*: `Offer`
inserts only strictly-better heads and `Refill` stamps its uniform band onto every batch job, so the head
is always the best item present. Jobs therefore carry `Priority`, assigned by the cache itself — callers
never set it.

**Low water is per partition** — half the partition's *last-refill depth* (`lastFill`), not a global
constant: partition sizes are dynamic (each refiller's slice of the global plan), so a global threshold
over-triggers small partitions and under-triggers large ones. `Pop`'s `needRefill` names the popped
partition (via the job's `Shard`), and the caller nudges that shard's refiller.

**`Refill` is a wholesale replace of ONE partition, and that includes an EMPTY batch** — it empties the
partition, exactly as draining the last item through `Pop` does. An empty batch is the scan's statement
that *nothing is due on that shard*, so every still-cached candidate there is a step that is no longer
pending: a dead hint a worker would pop and burn a claim-CAS round-trip on. The corollary is a rule on the
**caller**, and it is the sharp edge: a *failed* scan must **not** reach `Refill`. An error means
"unknown", not "nothing is due" — `runShardRefill` returns early on a scan error instead
(`engine/scheduling.go`), keeping the existing hints. Pinned by `TestCandidateCache_RefillEmptyIsWholesale`
and `engine`'s `TestFault_RefillScanErrPreservesCache`.

**The bound (`size`, `Capacity()`) is global — the sum over partitions — and it is a sizing target, not an
invariant.** `Offer` enforces it on a head-insert and `Resize` enforces it when the bound moves (trimming
**all** partitions proportionally to depth, largest-remainder, deterministic), but `Refill` deliberately
does not trim: each refiller slices its batch from its own independent roll of the global plan, so the sum
of slices can transiently overshoot and simply drains within a cycle. `Capacity()` is also what the
refillers draw their global plan at.

**`Offer` admits into an EMPTY partition, and that is load-bearing rather than incidental.** It once
declined, on the reasoning that an arbitrary-priority step must not jump an idle replica's queue — sound
only while a separate trigger let the refiller answer the decline within a fraction of a cycle. Under a
fixed cadence there is no trigger: a sequential chain holds exactly one pending step at a time, so its
partition is empty at *every* hop, and declining costs each hop a uniformly-random fraction of the cycle
interval (half of it on average, all of it at worst). Nothing special-cases the empty partition — its
derived floor is `math.MaxInt`, so any priority is strictly better and takes the ordinary head-insert
path.

There are consequently three outcomes, not two: strictly better than the floor **head-inserts**; no better
but under the bound **appends at the tail** (best-first at the head is what the derived floor rests on, so
a worse candidate may only ever go *behind* one); full **declines**. The tail entries are unplanned — they
sit outside the weighted fairness pick — but only until the next `Refill` wipes them wholesale, and under
real load the partitions sit at capacity so that path is never taken. Pinned by
`TestCandidateCache_OfferAdmissionCases` and `TestCandidateCache_OfferDeclinesWhenFull`.

**`Refill` returns the number of candidates it discarded un-popped**, which the engine feeds to
`dwarf_refill_candidates_discarded`. Discarded steps stay `pending` and are re-selected, so this is cost,
never loss — but the ratio against the batch size is the only visibility into how much of the refillers'
work is thrown away. A **closed** cache reports `0`: the replace is a no-op there, and counting it would
double-count candidates the drain already abandoned.

The floor/low-water/`Offer`-vs-`Refill` behavior is a protocol with the engine's refillers: any change to
their semantics must stay consistent with the selection algorithm described in the engine doc. The godoc on
each method states the local contract; the *why* (priority bands, urgent-arrival head-insert, liveness
after `processStep`, the census and slice rule) is the engine's.
