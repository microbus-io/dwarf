# Dwarf `internal/piston` — the engine's per-shard cylinder

> Load when: changing `Run`, `Liveness`, the two `Source` queries, the steal, the idle mode, or the
> instruments.
> Coupled with: `internal/pipeline/CLAUDE.md` (the cycle this drives and its error policy),
> `internal/planner/CLAUDE.md` (`Tally`/`Clear`), `internal/migrations/CLAUDE.md` (the `dwarf_steps`
> columns these queries touch), and `internal/peers/CLAUDE.md` (what consumes `Liveness`, and where the
> partition pair it is handed comes from).

One piston works one shard: it fires the same supply cycle over and over against its own database, on its
own clock, with no barrier against its peers. A fleet of shards is a fleet of pistons, and an engine with
N shards runs N of them.

**It is a CONSUMER of its database, never the owner.** The handle arrives already open and is closed by
whoever opened it, so there is no `Open`, no `Close`, and no pool control here. That is also why it is not
called a *shard*: the shard is the database partition; this is the thing that works it. A type named
`Shard` holding a `*sequel.DB` it does not own, next to a `ShardSet` that does, would be ambiguous at
every call site.

It **owns** the pipeline, the two queries behind it, and its instruments. It **borrows** the planner and
the candidate cache, both shared with every other piston on the replica. It publishes nothing about this
replica anywhere — `Liveness` is a pure read for whoever does.

## `Run` — one loop

```
cycle (paced by the pipeline) -> record -> repeat
```

`Run` blocks; the caller puts it in a goroutine and waits on its own WaitGroup. There is no `Stop` — a
cancelled context ends the loop, and that is safe with no second signal because **both queries are
read-only**: nothing to commit, nothing to strand, so abandoning mid-flight is free. Cadence lives entirely
in the pipeline, so the loop holds no timing policy of its own.

## `Liveness` — a counter, and why publishing is somebody else's job

**How often this replica's liveness is published must not depend on how long a cycle takes**, and that was
a real bug when the two shared a goroutine. Phase one's `rn <= capacity` cut early-stops only on **Postgres
15+**; on MySQL, SQL Server and SQLite a deep backlog is still O(backlog), measured in the **tens of
seconds** at a few million due rows. A signal gated on a cycle *returning* lets one such scan drop a
**healthy** replica out of its own fleet — shrinking every peer's pool divisor and reshuffling every
ordinal — exactly the outcome that should mean "the process is stuck, nothing less." Reporting rather than
publishing removes the coupling by construction: the reader samples on its own clock.

Three things the shape has to get right:

- **A counter, never a flag the reader clears.** A consuming getter would create a contract — call it once
  per publication, from one caller — and any second caller (a metric, a test) would silently swallow the
  evidence. Holding "since I last looked" belongs to the reader.
- **A turn in flight counts, but only once it has outrun the cycle period.** Same O(backlog) argument — a
  piston in the middle of a long scan is plainly still serving — but "in flight" alone is the wrong
  predicate and shipped as a bug. A cycle whose scan fails *instantly* is also briefly in its queries
  (building the error, recording the phase, logging it), measured at **~1.2% of samples**, and a reader
  sampling every 50ms catches that within seconds. It then keeps a piston that serves nothing looking alive
  indefinitely — the precise stranding the evidence exists to prevent. A scan shorter than the period will
  have completed and advanced the turn count before any reader looks, so nothing is lost by requiring the
  duration. Pinned by `TestPiston_FailingCyclesReportNoLiveness`, which samples ~1ms apart for half a second
  and requires *zero* busy readings; against the bool it fails with 16 of 438, and 333 of 440 at the zero
  period a bench sweep or a hand-driven caller can legitimately set — which is why the threshold is floored
  at `MinGap`, the package's existing fuse for that same degenerate regime.

  The threshold bounds the **false-alive** direction only. A query that hangs forever reads busy forever and
  keeps its residue class — indistinguishable from a legitimate long scan by construction, and the same
  trade the bool made. That is accepted: a piston stuck inside a query genuinely is a piston nobody should
  be redistributing work away from until its lease-recovery-shaped problems surface elsewhere.
- **A cycle that found nothing due still counts.** It proves the piston looked and could have served;
  gating on candidates instead would make a quiet fleet read as having no dispatchers at all, disable
  partitioning, and then thrash when work arrived.

## Idle

An idle piston runs no cycle and claims no work, and says so through `Liveness` — so its owner can keep
the replica counted for the connections it still holds while excluding it from anything that divides work.
That is the await-only replica.

**Going idle WITHDRAWS the shard, and skipping that wedges the replica.** The planner's contract is that
every shard either tallies or clears each cycle; an idle piston runs no cycle, so it does neither, and its
last tally would stand forever. The planner is **shared** across the replica's pistons, so that stale claim
on the best band is the documented wedge — every live piston finds none of *its* keys at that band and
dispatches nothing, indefinitely. It is benign only when every piston is idle, and `SetIdle` is per-piston,
so the API permits the bad case. `SetIdle(true)` therefore does the same two things an empty plan does:
`planner.Clear(shard)` and `cache.Refill(shard, nil, NoBand)` — the partition for the same reason, since a
dead hint costs a worker a claim round-trip. Pinned by `TestPiston_IdleWithdrawsTheShard`, which asserts the
release from a *peer's* point of view rather than just the local state.

The default is *not* idle, deliberately: a fresh piston dispatches, which is the common case, and a zero
value that silently did nothing would be the worse default.

Note the word is overloaded against the refill vocabulary, where *idle* means **nothing is due** — a
circumstance a dispatching piston meets constantly. Here it is a configured **mode**.

## The two queries

They implement `pipeline.Source`, which is the whole reason the piston owns the pipeline — nothing else
has the handle. Their per-query rationale lives in their doc comments; the cross-cutting rule is:

**The partition filters the ROWS this replica tallies but deliberately NOT the `MIN(priority)` subquery.**
The band is a cluster-wide fact, so mining it from one replica's slice would let replicas disagree about
which band is open. A replica holding nothing at the global band therefore tallies zero rows — correct,
since its own worse-band work must not be served until the better one drains. `TestPiston_PartitionDoesNotNarrowTheBand`
pins exactly this, and it is the thing a "simplify the scan" change would break silently.

The residue class is a **residency, not a lock**: the claim CAS remains the only thing that grants a step,
so a stale `(replicas, ordinal)` pair costs a lost claim, never correctness. `step_id % R` is not sargable,
so this reduces claim collisions rather than scan cost — the intended trade, since the scan was never the
contended resource.

**The pair is VALIDATED, not trusted** (`replicas > 1 && 0 <= ordinal < replicas`, else select everything).
Both bad shapes are strictly worse than not partitioning: `replicas == 0` emits `step_id % 0`, which errors
every query — so the scan fails, the pipeline clears this shard, and it stays out of planning for as long as
the func keeps saying so. An ordinal at or past `replicas` is quieter and worse: the predicate matches
nothing, so the piston reports `NoBand` while genuinely holding work, with no error anywhere. Today's
intended caller guards all of this itself, but fail-open is *this package's* advertised posture, so it is
enforced here rather than assumed of the caller. Pinned by `TestPiston_PartitionPairIsValidated`.

## Stealing — the answer to a peer that is SLOW rather than dead

A dead replica stops advancing `dispatched_at`, drops out of the dispatcher divisor within the dispatch
window, and its class is redistributed. A **slow** one keeps beating, keeps its class, and cannot serve it —
and nothing else in the fleet will look at those steps. `SetStealAfter` relaxes the predicate to close that.

**Measured, because the size of it is the whole justification.** Three replicas, one crippled, work created
by an await-only replica so the residue class is genuinely in the path:

| | steps/s (commanded 500) | claim lost |
|---|---|---|
| no steal, capacity-crippled peer (`1 worker`) | **137–153** | ~0 |
| no steal, latency-crippled peer (`+10ms` RTT) | **269** | ~0 |
| no steal, that peer REMOVED from the divisor | 494–505 | ~0 |
| steal, single tier | 501–558 | 8–27% |
| steal, two tiers | **458–501** | **0.4–5.4%** |

Two facts to take from the first three rows. Keeping a slow replica cost **more than deleting it** — the
fleet capped near `S·R` regardless of offered load, its class aging past 30s while healthy peers sat at a
third of a core. And two unrelated cripplings produced the same cap, so this is a property of the
PARTITION, not of how a replica goes slow.

### The gate is a SHORTFALL, not emptiness — and fixtures cannot show that

The gate arms when the last cycle's tally came in under `cache.Capacity()`: *can I fill the batch I am about
to plan?* An earlier cut armed on `Band == NoBand` — nothing due in this replica's own class at all — and it
**passed every fixture while doing nothing in production shape**. A burst workload drains a class to zero, so
the fixtures armed; a workload with CONTINUOUS arrivals leaves a keeping-up replica a step or two due on
almost every scan, so the gate never armed while its peer's class backed up unboundedly beside it. Measured
against a 50 flows/s open-loop bench with one crippled peer: **zero steals, throughput unchanged at 177**.
Do not weaken this back to emptiness; the fixtures will not catch it.

**While stealing, the scan the gate reads is the RELAXED one**, so a replica filling its batch by stealing
disarms, scans strictly next cycle, finds the shortfall again and re-arms. That alternation is deliberate:
it steals on every other cycle while re-checking its own class in between, so recovery is noticed within one
period and no separate re-probe is needed. Reading the strict tally instead would cost a second query per
cycle to learn a fact that is free to rediscover.

### Two tiers, and why the second one is not optional

```
own class            always
neighbour's class    after 1 grace   ((ordinal+1) mod replicas)
anyone's class       after 2 graces
```

Tier one gives each class exactly ONE designated stealer, so that window is contention-free — no two
replicas are ever eligible for the same step. Measured: it took the worst single-tier arm from **26.8% claim
loss to 0.4%** with throughput held.

Tier two exists because tier one alone can STRAND work. With two consecutive degraded replicas the far
one's class has no working stealer — its designated one is itself broken — so it gets *zero* service rather
than slow service, which is qualitatively worse than contention. Past two graces the class opens to
everyone. Pinned by `fixtures/TestStealTwoBadApplesflow`; measured on the bench at `1,1,64`, the fleet still
met the full commanded rate with `oldest_pending` at 0s.

The honest limit: tier one helps in proportion to the neighbour's SPARE capacity. In a saturated fleet most
work falls through to tier two and contention returns to the single-tier level. This is a win for one bad
apple in a fleet with headroom — the common deployment — not a general contention fix.

### The grace, and the three things it must not be

- **Measured from `not_before`, never `created_at`.** `not_before` is stamped `NOW_UTC()` at creation and
  pushed forward by `flow.Sleep` and every retry backoff, so the predicate reads "has been DUE for at least
  the grace". Against `created_at` an hour-long sleep would be stolen the instant it came due, on a fleet
  with nothing wrong with it.
- **Floored at the `pipeline.DefaultMinGap` CONSTANT, not the configured `MinGap`.** A caller may pin both
  interval and gap to zero (a bench sweep, a hand-driven cycle); deriving the floor from a configurable that
  can itself be zeroed yields a zero grace, which steals every foreign step the instant it comes due with no
  fuse at all.
- **NOT an absolute age threshold.** That shape was rejected for the fuse it replaced: normal queueing delay
  under load is *seconds*, so any constant either never fires or disables partitioning under exactly the load
  it exists for. The gate is what makes an age term usable, by restricting it to replicas already short of
  work.

`defaultStealAfter = 4` is not delicate: a healthy fleet's oldest due step sits at 0–1s while a stalled
owner's class ages to 23–41s, against a ~67ms derived period. Anything from ~2 to ~10 periods separates
those cleanly.

### What a healthy fleet does: nothing

At healthy RTT the bench fleet ran at ~30% of the database's capacity with spare everywhere and stole **0–23
steps** across five arms. The gate arms — every replica is under capacity — but nothing ages past the grace,
so nothing is taken. It is not that idle replicas steal harmlessly; they do not steal at all.

**A debounce on the gate was therefore considered and shelved.** The case for it was a uniformly slow fleet
stealing pointlessly, and it does happen (a `-race` fixture run: 6 of 40 steps; a bench cell at rtt 2.4ms:
79% of *claims*). But the second figure is a ratio over claims, and a failed claim is one round trip where a
completed step is ~9.6 — in real terms 7.6% of round trips, on a run already at **78% of the ceiling its RTT
allowed**. The missing throughput was the slow database, not the steal. Do not build the debounce without
evidence that names a cost the grace does not already bound.

**`FetchSteps` orders on `rn`, not on a recomputed age.** The window already ranks each key by
`(created_at, step_id)`, so `ORDER BY fairness_key, rn` is oldest-first *by construction*. An earlier cut
selected `DATE_DIFF_MILLIS(NOW_UTC(), created_at)` and re-sorted each key in Go by age descending — the same
ordering read backwards through millisecond-truncated arithmetic, agreeing only incidentally (anything
created inside one millisecond fell through to the `step_id` tiebreak, which is also what its test
exercised). The age was a leftover from the engine's version, where it fed a cross-shard merge that does not
exist here: the planner has already assigned the slots.

## Metrics take a `Meter`, not a `MeterProvider`

So the **owner** picks the instrumentation scope once for every module it assembles. Each package deriving
its own scope from a provider would split one engine's metrics across several scopes in the export the
moment a name drifted, and nothing here needs provider-level capability. Instrument names are a public
surface that dashboards bind to — `TestPiston_RecordsItsInstruments` asserts them by name so a rename
fails loudly.

**`refillBuckets`' LOW end is load-bearing and must not be raised.** A warm same-zone band scan measures
~0.29ms; the *same* query on the *same* data measures ~100ms once its Postgres statistics go stale and the
plan flips to a sequential scan — the flip the `phase` label exists to expose. Boundaries starting at
0.0005 file the healthy case in the first bucket and hide exactly that, which is what an earlier cut of
this package did. The values match the engine's, deliberately: these are the same four instruments,
and a histogram whose boundaries changed under a dashboard is a silent regression.

## Two seams, and why each is the exception

`SetSeams` takes the **owner's** `*seamster.Seamster` — one catalogue of fault names per application,
armed in one place, however many modules consult it — for the same reason `SetMeter` takes a `Meter`. Nil
restores an inert one, which is the default, so an unwired piston consults nothing and a disabled Seamster
makes every consult a bool read.

Exactly one fault is consulted: **`FaultScanErr`**, in `ScanBand`. It earns its place because it perturbs
the **database query**, which is the boundary a test cannot otherwise reach — the pipeline's scan-error
policy (clear this shard from planning, leave its cache partition *alone*) is only reachable by making a
real scan fail, and the two halves are asymmetric, so neither can be inferred from the other. The name is
exported so the owner's catalogue aliases it rather than re-spelling the string. Pinned by
`TestPiston_ScanErrSeamDrivesThePipelineErrorPolicy` and `TestPiston_SeamsDefaultInert`.

Two checkpoints are fired. **`CheckpointStole`** reports a fetch that took steps from outside this
replica's residue class, and it earns its place on the same boundary rule: a test proving a slow peer's work
is picked up cannot wait out a duration, because the steal fires on the first cycle after the gate arms and
the grace elapses — a function of the cadence, the peer's degradation and the backlog, none of which a test
controls. Without it the only available assertion is "the flows eventually finished", which passes equally
against a build where stealing does nothing and the dispatch-window eviction did the work seconds later.

The other is **`CheckpointCycleDone`**, in `Run`, once per cycle that PUSHED. It earns
its place on the same boundary rule read from the other side — it publishes the one fact about a cycle that
is invisible from outside the process and cannot be waited out on a clock. **Each piston turns on its own
cadence, so "every shard has reconciled its partition against the plan" is not a function of elapsed time**:
a shard whose goroutine is starved, or whose cycle is blocked on a slow round trip, can hold an unreconciled
partition arbitrarily long while its peers turn normally. A caller that needs that state — a test asserting
strict cross-shard priority must, because `Cache.Pop` ranks partitions by a FROZEN band and cannot tell a
doorbell-set one from a plan-set one — has no other way to know. It is gated on `r.Err == nil` because that
is exactly the push: both error paths return *before* pushing and deliberately leave the partition alone, so
a visit on error would mean "looked and gave up", which is the opposite of what a waiter needs.

The gate is what keeps this from being the forbidden kind of seam. A counting checkpoint over pure logic
would be a signal to inject a dependency instead; this one reports a **cycle's effect on shared state** the
package borrows (the cache partition), which no injection into `pipeline` would surface, since the effect is
the point rather than the call.

**`pipeline` gets none, and that is not an oversight.** Every fault a test could want of a cycle is
reachable through its `Source` — which is this type — or through `SetInterval`/`SetMinGap`, and
`pipeline.Result` gives a caller everything a counting checkpoint would. The rule that separates the two
cases: a seam inside **pure logic** is a signal a dependency should have been injected instead; a seam at
an **I/O boundary** is reaching the one thing that cannot be injected away. Do not add one to `planner`
either, for the same reason.

## Tests run against real SQL

Each test stands up its own isolated, migrated database through `internal/database.ShardSet` — a
**test-only** import, since the piston itself never opens a handle — and inserts real `dwarf_steps` rows.
That is what lets them cover things a fake could not: the parked-step predicate, the capacity cap actually
capping, the `ROW_NUMBER()` ordering, and the residue class splitting a real row set disjointly across
ordinals.
