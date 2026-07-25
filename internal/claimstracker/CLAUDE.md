# Dwarf `internal/claimstracker` — the intra-replica in-flight claim set

> Load when: changing the window's length, how entries expire, or the key.
> Coupled with: `engine/CLAUDE.md` §"Candidate de-duplication" (the cross-peer half — the partition
> predicate — plus the three metrics that price candidate waste, and the measured numbers).

## Why it exists at all: no SQL filter can see this

The candidate selection predicate filters **committed** state. A claim CAS that has been *issued but not
committed* still reads `pending` with a free lease, so this replica's own next scan legitimately
re-selects a step this replica is mid-claim on. Only the process knows; there is no query that could
exclude it.

That is the whole justification for a Go-side structure here, and it is also the boundary: the
cross-*peer* duplicate has a completely different cause (every replica that picks a key fetches the same
oldest rows deterministically) and a completely different fix (the `step_id % R = ordinal` partition
predicate, in the SQL). Neither mechanism helps with the other's case. Do not try to merge them.

## Strictly advisory — the CAS is still the only grant

A missed entry costs one wasted round trip (the behavior that predates this package). A stale one costs a
skipped candidate that the next pass re-selects. Neither is a correctness question, which is what keeps
this from becoming a distributed lock. Same posture as the candidate cache holding *hints, not
ownership*.

## The window is 1–2s, and BOTH bounds were established by breaking them

- **Too short — releasing when the CAS returns.** Built, measured, and barely worked (claim miss 7.3% →
  5.7%). The gap to span runs from **selection to pop**, not the round trip between them: the refiller
  selects a step whose claim is uncommitted, the entry sits in the cache, and by pop time a CAS-scoped
  reservation is long gone. The window must outlast the maximum interval between supply cycles (~1s).
- **Too long — holding for the whole step.** Worse. A worker parked in a long `ExecuteTask` keeps its
  reservation for the entire task, so if that step's lease expires meanwhile (an overrun, a DB clock
  step) **no sibling worker can re-claim it and single-replica lease recovery stops working**. It only
  appeared to work because steps in that benchmark took ~15ms; it is workload-dependent and unbounded
  above. Caught by `TestLeaseFence_CompletionNoDuplicateSuccessor`, whose blocked first dispatch is
  exactly that shape.

So the window sits **above the cycle interval and far below `leaseMargin` (30s)**, and that upper bound
is the safety argument: a reservation can only ever DELAY this replica's dispatch of a step by a bounded
window, never prevent it, and it can never be why a lease-recovered step fails to re-dispatch.

## The two-generation roll, and the sweep it replaces

The naive shape is one map plus a per-entry timestamp and a sweep. That fails under a **pinned** worker
pool at high throughput (`SetWorkers(512)` at thousands of steps/s): the live population outgrows any
size gate, and the sweep walks the whole map on every claim, under one lock.

Two maps hold the current whole-second bucket and the previous one; a lookup checks both. Once a second
the maps **roll** — current becomes previous, previous is dropped, a fresh current is allocated — which
is three pointer assignments, not a walk. There is **no timestamp per entry and no scan**, and the only
cost paid at expiry is garbage-collecting a map reference.

The consequence to keep in mind: an entry lives **between one and two seconds** depending on where in the
second it landed. The window is a range, not a constant, and both ends of that range are inside the
bounds above.

**A backwards wall clock lands in the `default` arm and clears both generations.** That is the safe
advisory direction — it only forgets in-flight steps, costing at most a wasted round trip.

## Key on `(shard, stepID)`

`step_id` is a per-shard auto-increment, so every shard has a step 42. A step-id-only key would report
shard 2's step 42 as in-flight because shard 1's is, turning away live candidates on every shard but one.
A struct key is hashable and collision-free, unlike packing the two ints into one.

## The caller's obligation: relinquish before re-offering

The window is sized to outlive a *scan*, so it necessarily outlives a step's own dispatch — which means a
step re-armed and re-offered to this replica still carries the reservation its previous dispatch took.
Left in place, every worker that pops the step is turned away and the supply cycle keeps re-selecting it
for up to the full ~2s. **Every path that re-offers a step to this replica must `RelinquishClaim` first.**
The call sites and the measured cost of missing one are in `engine/CLAUDE.md`; a relinquish for a step id
this replica never reserved is a harmless no-op, which is why the guard belongs at the shared entry point
rather than at each caller.
