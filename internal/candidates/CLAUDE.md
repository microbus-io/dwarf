# Dwarf `internal/candidates` — bounded, shard-partitioned step-candidate hint cache

> Load when: changing the cache's `Offer`/`Refill`/`Pop`/floor/low-water/partition semantics.
> Coupled with: `internal/pipeline/CLAUDE.md` (the per-shard Supplier that wholesale-replaces a partition every
> cycle, and the scan-error policy), `internal/planner/CLAUDE.md` (the global plan a batch is sliced from), and
> `engine/CLAUDE.md` §"Execution Model" (the step-origination doorbell that calls `Offer`).

This package is only the **mechanism**: a small, per-replica bounded set of step *candidates*, partitioned
by shard. The invariant an editor must not break is that entries are **hints, not ownership** — a worker
pops a candidate and then CAS-acquires the underlying step before executing, so a stale, duplicated, or
already-claimed candidate is harmless (the CAS loser just pops the next one). Nothing here may assume a
popped candidate is still runnable.

**One cache, N partitions — not N caches.** Each shard's refiller wholesale-replaces its own partition
(`Refill(shard, batch, limit)`), and `Pop` selects across partitions: **the best band at the head wins**
(that is the entire dispatch rule), ties break by depth (deepest first — round-robin would hand every shard an equal
share of workers regardless of backlog), then by shard index for determinism. One mutex and ONE condition
variable span all partitions: N cond vars would let a worker blocked on shard 3's emptiness sleep through
shard 1 filling.

**Taking work is SPLIT in two — `WaitForWork` then `TryPopFrom` — and the split is the demand side's,
not the cache's.** `Pop` (park until something is available, then remove it) is the single-call form, but
the engine's workers use the pair, because they must take a permit *between* the two
halves: parking while holding a permit would hoard admission capacity, and popping before holding one would
strand a candidate inside a worker that cannot proceed with it. So `WaitForWork` blocks and reports only
**which shard** to drain, and `TryPopFrom(shard)` removes from that partition without blocking.
`PeekShard` is the non-blocking twin of the first.

Two consequences to keep straight:

- **The shard is a HINT.** Between the peek and the pop another worker may take the peeked entry and a
  better-banded arrival may land, so `TryPopFrom` failing is ORDINARY. Its caller must retry the park, never
  treat it as a close.
- **`WaitForWork` alone reports the close.** `TryPopFrom` deliberately does not distinguish "empty" from
  "closed" — both mean *go back and park* — and folding the close into it would let a caller mistake a lost
  race for a shutdown and exit. In the crew that would silently erode the worker set under exactly the
  contention that caused the race.

## Routing reserves a candidate, and the pop releases it

`WaitForWork` routes by **unreserved** depth - a partition's candidates minus the workers already routed
there that have not yet reached their `TryPopFrom` - and reserves one for its caller. `TryPopFrom` releases
it on both outcomes: a successful pop moves the candidate and the reservation together, so the depth others
route by does not change; a failed one must not leave the partition looking spoken for. In the engine a
reservation lives exactly as long as its worker waits at that shard's gate.

**Without it, every worker that peeks at once picks the same partition.** The peek is deterministic, so they
all queue at one shard's gate while the other shards' candidates wait for nobody - and a worker queued at a
gate counts idle, so the crew does not grow to cover the rest. It bites only where one replica dispatches
several shards; a replica dispatching one has nowhere else to send a worker.

- **A Refill does NOT reset the count.** The workers it counts are still queued and will pop from the new
  batch, so a reset would forget them and the next peeks would route up to that many more to the same
  shard - re-forming the herd every cycle under saturation, where the queues are deepest. A Refill that
  shrinks a partition below its reservations makes it read negative, which routes nobody new there; each
  failed pop then brings the count back down.
- **Routing ranks a partition by the band past its reservations** - the candidate the next routed worker would
  actually take, since those already routed take the ones ahead of it. Ranking by the head would send a second
  worker to `1 5 5` behind the first, who is on the way to the 1, and run a 5 while another partition's 3s wait.
  Pinned by `TestCandidateCache_RoutingReadsPastTheReservations`.
- **A fully reserved partition is passed over even when its band is better.** Its every candidate has a
  worker on the way, so an extra one could only queue behind them; the worse band it goes to is on another
  shard, where it competes with nothing the better band needs.
- **A waiter parks when nothing is UNRESERVED**, not only when the cache is empty, so a surplus worker parks
  rather than queueing at a gate for a candidate that is already spoken for. `Refill` and `Offer` (new
  candidates) are the only wake sites. A moved reservation (`Reroute`, `MoveReservation`) also makes a
  candidate routable again, on the shard it leaves, and wakes nobody - which is harmless: the mover leaves only
  a shard whose gate it just found full, so a woken waiter would queue at that same gate. The candidate goes to
  the next worker that comes round, or to a waiter the next `Refill`/`Offer` wakes.
- `Pop` ignores reservations; it is the single-call form and nothing in the engine mixes it with the pair.
- **`Reroute` moves a reservation** to the best other routable shard, for a worker that could not get started
  where it was routed (in the engine, a shard with no free turn). It routes nobody when nothing else is
  routable, leaving the reservation where it was.

All four selection paths share `bestPartition`, so the lowest-floor rule exists in exactly one place; two
copies of it would drift, and the drift would be invisible (both would still dispatch *something*).

## Waking: ONE SIGNAL PER CANDIDATE, never a broadcast

`Refill` wakes `min(unreserved, waiting)` waiters one at a time; `Offer` signals once for its single
candidate; **`Close` broadcasts, and must** — every waiter has to leave.

A refill admits at most the cache's capacity, so a broadcast wakes the whole parked crew to hand out that
many jobs, and useful work per wake is fixed while the herd grows with the crew. Go has no wake-N, and a
`Signal` loop *is* wake-N.

**Measured** (`BenchmarkCache_RefillWake`, medians of 5, 200 refills of 64 candidates, ns/op):

| crew | 64 | 256 | 808 | 2048 | 8192 |
|---|---|---|---|---|---|
| broadcast | 47.8k | 61.7k | 182k | 534k | 1,306k |
| signal-N | 48.5k | 60.2k | 59.5k | 61.8k | 60.6k |

Signal-N is **flat across a 128x crew range**; broadcast is linear in the crew above ~256. The two are a
wash at 64-256, so this is **not** a win at any crew size — it begins where the crew exceeds the batch,
which is exactly where a wake stops being able to do useful work.

**The cost is a convoy, not a spin, and the wrong model here leads to the wrong metric.** The losers of a
broadcast never get far enough to spin: they wake, queue on the single mutex, and by the time each acquires
it the batch is drained, so they re-park inside `WaitForWork`'s loop without ever returning. `takes per
candidate` therefore sits at 1.05-1.10 under *both* protocols — it is the control that makes the wall-clock
gap attributable to overhead, not the instrument that exposes the herd. Only the clock sees it.

**Under-waking cannot strand a candidate**, which is what makes the bound safe. A waiter parks only when it
finds nothing it could take (`Pop`: an empty cache; `WaitForWork`: no unreserved candidate), and both
re-check in a loop — so a woken worker that loses its race does not park on top of work, it goes round
again. The floor on progress is therefore ONE
worker, not one per candidate. The cap is read under the lock, where a waiter `Signal` has already picked
cannot yet leave `Wait`, so it cannot undercount the ones on their way out.

**The bound is a PERFORMANCE property and is not unit-testable — do not go looking for the test.** Two
measurements say so. A woken worker with a fast handler comes round, finds the cache still non-empty and
takes another without re-parking, so a batch of 64 is routinely drained by a handful of workers (measured:
7) however many were signalled — which is why waking once per refill instead of once per candidate passes
the entire suite. And the losers of a broadcast never return from `WaitForWork` either, so no count
distinguishes the two protocols from outside. `TestCandidateCache_OneRefillStrandsNoCandidate` pins the
safety (a refill that wakes *nobody* hangs it); `BenchmarkCache_RefillWake` is where the rest of the
evidence lives.

### The FIFO wake is load-bearing AND is not a guarantee — treat it as a pinned assumption

Signals rotate through the parked crew oldest-first, so every worker is woken periodically even when the
batch is far smaller than the crew. Two things depend on that: work spreads across the whole crew rather
than starving a tail, and `internal/workers` retires a surplus worker on a check it can **only reach by
waking**, so a protocol that kept re-waking the same few would stop the crew ever shrinking.

**`sync.Cond` does not promise it.** The documented contract is only that `Signal` "wakes one goroutine
waiting on c, if there is any" — nothing about *which* one, and it explicitly disclaims ordering against
goroutines contending for `c.L`. The behaviour comes from the runtime's **ticket-based `notifyList`**
(`runtime/sema.go`): a waiter takes a monotonically increasing ticket on arrival, and `notifyListNotifyOne`
wakes the holder of the next ticket in sequence. Strong, and stable for many releases — but not owed to us.

So it is **pinned rather than assumed**: `TestCandidateCache_WakesGoInArrivalOrder` spawns waiters one at a
time (each parked before the next starts, so arrival order is known) and asserts the wake sequence is
exactly that order. A Go release that relaxed it fails there, loudly, instead of silently starving the
crew's retirement check months later. Do not delete that test as testing-the-standard-library; the thing it
guards is ours.

**The scope is narrower than it first looks.** FIFO governs which waiter is *selected and readied*. It says
nothing about which then wins `c.L` — `Signal`'s own doc warns a goroutine merely locking `c.L` may barge
ahead of a woken one. Selection is all the no-starvation argument needs, so nothing here rests on the wider
claim.

**A LIFO handoff would take both properties with it.** Reusing hot workers and letting cold ones starve out
is the standard thread-pool trick, and it is what mirroring `internal/turnstile`'s per-waiter channels would
amount to. That is a real design with real merits, but it is not this one, and it must not arrive as a side
effect of a wake change.

**A partition is kept in BAND ORDER, and that is what makes every take a pop from the front.** A plan that
spilled hands `Refill` a batch spanning several bands; `Refill` sorts it stably by `Job.Priority` (the plan's
interleave survives within a band), and `Offer` inserts behind everything at its own band or better. So the
head is always the best candidate the partition holds, and the floor - the key every take ranks partitions by
- is simply the head's band, exact by construction. **Do not store a floor beside the items instead.** With
several bands in one partition a stored floor overstates urgency as soon as its best band drains: a partition
holding `1 5 5 5` would keep advertising 1 and drain its 5s ahead of another partition's 3s. **And do not append
offers at the tail.** A worker would then take a worse band while a better one waits behind it in the same
partition.

Jobs carry `Priority` - set by the plan for a batch, stamped by `Offer` for an admission - and it is what the
order and the floor read; a popped candidate is self-describing (the worker logs it).

**`limit` is the worst band `Offer` admits, and it is NOT the floor.** `Refill` sets it to the plan's reach -
the band where the fill ran out of capacity, `math.MaxInt` when everything due fit - and a partition no `Refill`
has planned starts at `math.MaxInt`. It is frozen until the next `Refill`, so the admission bar does not move as
the partition drains. **Do not let an arrival into an empty partition set it.** The partition draining says
nothing about what the plan left uncached, and the arrival's band moves the bar in either direction: a worse one
widens it, admitting worse successors ahead of the reach's uncached work every cycle, and a better one narrows
it, declining the spilled band's successors until the next `Refill`. Pinned by
`TestCandidateCache_OfferDeclinesAWorseBand`.

**`Refill` is a wholesale replace of ONE partition, and that includes an EMPTY batch** — it empties the
partition, exactly as draining the last item through `Pop` does. An empty batch is the scan's statement
that *nothing is due on that shard*, so every still-cached candidate there is a step that is no longer
pending: a dead hint a worker would pop and burn a claim-CAS round-trip on. The corollary is a rule on the
**caller**, and it is the sharp edge: a *failed* scan must **not** reach `Refill`. An error means
"unknown", not "nothing is due" — `internal/pipeline`'s Tallier answers a scan error with `planner.Clear`,
and its Supplier, seeing `!Plan.Tallied`, pushes nothing, keeping the existing hints. Pinned by
`TestCandidateCache_RefillEmptyIsWholesale`, `TestSupplier_UntalliedShardSparesTheCache`, and `engine`'s
`TestFault_RefillScanErrPreservesCache`.

**The bound (`size`, `Capacity()`) is global — the sum over partitions — and it is a sizing target, not an
invariant.** `Offer` enforces it on a non-empty partition and `Resize` enforces it when the bound moves (trimming
**all** partitions proportionally to depth, largest-remainder, deterministic), but `Refill` deliberately
does not trim: each refiller slices its batch from its own independent roll of the global plan, so the sum
of slices can transiently overshoot and simply drains within a cycle. `Capacity()` is also what the
refillers draw their global plan at.

**Within the limit, `Offer` admits into a VACATED SLOT: empty partition always, otherwise while the cache is
under its bound, and never reordering.** Its callers are step-origination sites - most often the successor of a step
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

The reason is that a partition holds one cycle's buffer, not a backlog, so under load it drains to empty
between cycles constantly. Load does not
suppress the empty-partition case; it manufactures it. Do not remove this on the theory that production
partitions are always full - they are not, and the measurement above is the arm to re-run before believing
otherwise.

**The one priority test is against a band WORSE THAN THE LIMIT, and the limit is the plan's reach, not its
best band.** Everything at the reach or better was planned with capacity to spare at the better bands, so an
admission there takes a slot the plan would have given that band anyway, and band order puts it behind what
the plan chose. A band worse than the reach is declined: the plan ran out of capacity at the reach, so work
there is still waiting uncached, and admitting a worse band would run it first — a strict priority inversion,
not the soft cross-shard staleness the design accepts. **Do not bar at the partition's best band.** It is the
starvation shape the spill exists to remove: with a sparse better band at the head, every successor of the
worse band the plan spilled into would be declined and wait a cycle per hop. An **empty** partition holds to the
same bar: a worse-band sequential chain keeps moving at every hop the plan's reach covers, and past it waits for
the next `Refill`, which raises the reach once the better band has drained.

Which callers can even bring a better band is worth knowing, because it is not the successor case:
priority is frozen at `Create` and inherited by every step of a flow, so a successor never arrives at a
better band than its own predecessor ran at. Only a **new flow** can (`Create`/`Continue`/`Fork` offering
its entry step), or the narrow straggler where a better band drained, the partition was refilled at a worse
one, and that band's last successor turns up afterwards.

**Admitted candidates are counted (`partition.offered`) and subtracted from `Refill`'s discard count.**
That return value is the REFILLER's oversupply signal, and charging it for candidates the doorbell admitted
would make the ratio read worst exactly when the doorbell is working best. Band order puts an offered job
anywhere in the partition, so each one carries an unexported flag that a take, a `Resize` trim and `Refill`
read to keep the count exact; the flag is cleared before a job leaves the cache, so a caller never sees it.
The engine emits the admissions as
`dwarf_steps_offered`; read against `dwarf_refill_candidates_selected` it is the share of dispatch that came
from step origination rather than from the weighted plan.

**Inserting a better band ahead of worse cached candidates is band order, not preemption.** Nothing running is
interrupted, and within its band the arrival goes last, so it bypasses no fairness decision - the plan's lottery
is per band. [Scheduling](https://docs.dwarf.run/building-workflows/scheduling/) states the public contract: *priority is never preemptive*,
and the delay before a new band is served is *bounded by the snapshot cycle*.

**`Refill` returns the number of candidates it discarded un-popped**, which the engine feeds to
`dwarf_refill_candidates_discarded`. Discarded steps stay `pending` and are re-selected, so this is cost,
never loss — but the ratio against the batch size is the only visibility into how much of the refillers'
work is thrown away. A **closed** cache reports `0`: the replace is a no-op there, and counting it would
double-count candidates the drain already abandoned.

The floor/`Offer`-vs-`Refill` behavior is a protocol with the per-shard Suppliers: any change to its
semantics must stay consistent with the selection algorithm in `internal/planner` and the error policy in
`internal/pipeline`. The godoc on each method states the local contract; the *why* of priority bands and
the slice rule is the planner's, and the doorbell's is the engine's.
