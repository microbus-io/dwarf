# Dwarf `internal/candidates` — bounded, shard-partitioned step-candidate hint cache

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

**Taking work is SPLIT in two — `WaitForWork` then `TryPopFrom` — and the split is the demand side's,
not the cache's.** `Pop` (park until something is available, then remove it) is still here and still the
single-call form, but the engine's workers use the pair, because they must take a permit *between* the two
halves: parking while holding a permit would hoard admission capacity, and popping before holding one would
strand a candidate inside a worker that cannot proceed with it. So `WaitForWork` blocks and reports only
**which shard** `Pop` would drain, and `TryPopFrom(shard)` removes from that partition without blocking.
`PeekShard` is the non-blocking twin of the first, for the growth decision.

Two consequences to keep straight:

- **The shard is a HINT.** Between the peek and the pop another worker may take the peeked entry and a
  better-banded arrival may land, so `TryPopFrom` failing is ORDINARY. Its caller must retry the park, never
  treat it as a close.
- **`WaitForWork` alone reports the close.** `TryPopFrom` deliberately does not distinguish "empty" from
  "closed" — both mean *go back and park* — and folding the close into it would let a caller mistake a lost
  race for a shutdown and exit. In the crew that would silently erode the worker set under exactly the
  contention that caused the race.

All four selection paths share `bestPartition`, so the lowest-floor rule exists in exactly one place; two
copies of it would drift, and the drift would be invisible (both would still dispatch *something*).

**The floor is the band a partition is serving for the current window, and it is FROZEN there** — set by
the `Refill` that planned it (or by an `Offer` into an empty partition), `math.MaxInt` when empty. It is
`Pop`'s key for choosing which partition to drain, and `Offer`'s bar for what it will admit.

Frozen rather than read off the head, and the difference is visible. `Offer` appends a better-banded step
*behind* the planned batch, so a head-derived floor would **dip** as that item reached the front and rise
again once it popped: a partition planned at band 6 holding `6 6 5 6` would advertise 6,6,5,6 as it
drained. That oscillates `Pop`'s partition choice — and worse, it swings `Offer`'s own admission bar with
it, so an offered band-6 step would be taken or refused purely on pop timing.

**Storing it is safe here in a way it was not before, and the direction is the whole argument.** `Offer`
declines anything *worse* than the band, so every item present is at least as good as it — a frozen floor
can only ever **understate** the partition's urgency, delaying a buried better-band step by at most a
cycle. The old stored floor failed the other way: back when `Offer` could head-insert, it kept advertising
band 1 *after* the band-1 head had popped, **overstating** urgency and draining a whole band-5 body ahead
of another partition's band-3 work. Understating costs latency; overstating costs ordering.

Jobs still carry `Priority`, assigned by the cache itself — callers never set it — but the floor no longer
derives from it; it rides along so a popped candidate is self-describing (the worker logs it).

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

**`Offer` admits into a VACATED SLOT: empty partition always, otherwise while the cache is under its
bound, and never reordering.** Its callers are step-origination sites - most often the successor of a step
that just completed - so the question is whether this replica can run the step now or must wait a cycle for
a plan to select it.

**AN ADMITTED SUCCESSOR IS NOT JUMPING THE LINE - it is spending a slot its key already won.** The plan
grants a fairness key a share of the batch for the CYCLE, not a single dispatch; a step whose predecessor
just completed is taking the slot that predecessor vacated, within the window it was granted. Read that way
the rule is not a fairness exception at all, and the bound is not arbitrary either: a successor may only be
admitted where a slot has actually been freed.

So the bound is exactly "a slot has been freed" - `totalLen < size` - and the EMPTY partition is the
special case that ignores even that, because the bound is a sum over ALL partitions and letting it gate an
idle shard's doorbell had one busy shard silence another (159 of 500 flows stranded in
`completionraceflow`). The overshoot is at most one candidate per shard.

It cannot amplify a tenant's share regardless: a successor exists only because its predecessor completed
and freed the worker that will run it. Fairness governs ADMISSION - which flows get started - and a
workload of long chains and one of many short flows still receive the same worker-seconds.

**IT EARNS ITS PLACE UNDER LOAD, WHICH IS THE OPPOSITE OF THE OBVIOUS PREDICTION.** The natural worry is
that this is a test-only trick: an idle engine has empty partitions at every hop, a loaded one does not, so
the mechanism should fade exactly where latency matters. Measured, it is the reverse - the two most loaded
fixtures gain the MOST from it:

| | `Offer` live | never admits | |
|---|---|---|---|
| whole fixtures suite | 11.1s | 15.7s | 1.41x |
| `completionraceflow` (500 flows, 2 shards, cache 8) | 2.9s | 6.0s | **2.05x** |
| `soakflow` (high volume) | 7.3s | 13.4s | **1.84x** |

The reason is in the engine's own supply numbers: a cycle runs only 1.04-1.47x ahead of consumption, so
under load the cache is SHALLOW and partitions drain to empty between cycles constantly. Load does not
suppress the empty-partition case; it manufactures it. Do not remove this on the theory that production
partitions are always full - they are not, and the measurement above is the arm to re-run before believing
otherwise.

**The one priority test is against a WORSE band, and it points the opposite way from the obvious guess.**
A *better* band appends: the step is at least as important as everything the partition was planned to run,
so it violates nothing and simply dispatches in arrival order. A *worse* band is declined — admitting it
would have a worker run band-900 work while band-100 work sat cached right there, which is a strict
priority inversion, not the soft cross-shard staleness the design already accepts. It waits for a cycle in
which its band is the global minimum, which is exactly when it is allowed to run. An **empty** partition
has nothing planned to be worse than, so it admits any band — which is what keeps a worse-band sequential
chain moving once the better band has drained.

Which callers can even bring a better band is worth knowing, because it is not the successor case:
priority is frozen at `Create` and inherited by every step of a flow, so a successor never arrives at a
better band than its own predecessor ran at. Only a **new flow** can (`Create`/`Continue`/`Fork` offering
its entry step), or the narrow straggler where a better band drained, the partition was refilled at a worse
one, and that band's last successor turns up afterwards.

**Admitted candidates are counted (`partition.offered`) and subtracted from `Refill`'s discard count.**
That return value is the REFILLER's oversupply signal, and charging it for candidates the doorbell admitted
would make the ratio read worst exactly when the doorbell is working best. The count is exact without a
per-job flag because of the layout: a `Refill` replaces wholesale and offers only ever append, so the
refill block is always the head and the offered ones the tail - which makes "is the item `Pop` is about to
remove an offered one?" the test `len(items) <= offered`. The engine emits the admissions as
`dwarf_steps_offered`; read against `dwarf_refill_candidates_selected` it is the share of dispatch that came
from step origination rather than from the weighted plan.

**The priority-preempting head-insert is GONE, and should not come back without evidence.** It let a
strictly-better band jump the queue so the first urgent step did not wait a cycle. Measured on
`fixtures/crossshardpriorityflow_test.go` — the fixture built to measure urgent-burst reaction — removing it
changed **nothing**: burst latency 134-146ms with, 134-152ms without, identical counts of lower-priority
work ahead of the burst. What it bought was one step, on one shard, arriving up to one interval sooner.

What it cost was subtlety spread across three docs: a fairness bypass that had to be argued as bounded to
one pioneer per band-opening; a bridge-window leakage story; an apparently arbitrary "only one" limit that
invited widening (built, measured, reverted — see the commit); and the reason the floor had to be derived.
It also only ever reordered **one replica's cache** — the planner learns of the new band from that shard's
next tally either way, so the fleet-level band change costs a cycle regardless.

It is also what `docs/scheduling-and-reliability.md` has always promised: *priority is never preemptive*,
and the delay before a new band is served is *bounded by the snapshot cycle*. The head-insert was code
quietly doing more than the public contract claimed.

**`Refill` returns the number of candidates it discarded un-popped**, which the engine feeds to
`dwarf_refill_candidates_discarded`. Discarded steps stay `pending` and are re-selected, so this is cost,
never loss — but the ratio against the batch size is the only visibility into how much of the refillers'
work is thrown away. A **closed** cache reports `0`: the replace is a no-op there, and counting it would
double-count candidates the drain already abandoned.

The floor/low-water/`Offer`-vs-`Refill` behavior is a protocol with the engine's refillers: any change to
their semantics must stay consistent with the selection algorithm described in the engine doc. The godoc on
each method states the local contract; the *why* (priority bands, urgent-arrival head-insert, liveness
after `processStep`, the census and slice rule) is the engine's.
