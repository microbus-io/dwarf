# Dwarf `internal/piston` — the engine's per-shard cylinder

> Load when: changing `Run`, `Liveness`, the two `Source` queries, the idle mode, or the instruments.
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

Exactly one checkpoint is fired: **`CheckpointCycleDone`**, in `Run`, once per cycle that PUSHED. It earns
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
