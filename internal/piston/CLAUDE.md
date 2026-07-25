# Dwarf `internal/piston` — the engine's per-shard cylinder

> Load when: changing `Run`, the heartbeat, the two `Source` queries, the idle mode, or the instruments.
> Coupled with: `internal/pipeline/CLAUDE.md` (the cycle this drives and its error policy),
> `internal/planner/CLAUDE.md` (`Tally`/`Clear`), `internal/migrations/CLAUDE.md` (the `dwarf_peers` and
> `dwarf_steps` columns these queries touch), and `engine/CLAUDE.md` §"Peer discovery" (who reads the
> registry rows this writes, and where the partition pair comes from).

One piston works one shard: it fires the same supply cycle over and over against its own database, on its
own clock, with no barrier against its peers. A fleet of shards is a fleet of pistons, and an engine with
N shards runs N of them.

**It is a CONSUMER of its database, never the owner.** The handle arrives already open and is closed by
whoever opened it, so there is no `Open`, no `Close`, and no pool control here. That is also why it is not
called a *shard*: the shard is the database partition; this is the thing that works it. A type named
`Shard` holding a `*sequel.DB` it does not own, next to a `ShardSet` that does, would be ambiguous at
every call site.

It **owns** the pipeline, the two queries behind it, its instruments, and this replica's heartbeat. It
**borrows** the planner and the candidate cache, both shared with every other piston on the replica.

## `Run` — two loops, not one

```
cycle (paced by the pipeline) -> record -> repeat
beat -> sleep(heartbeatInterval) -> repeat
```

`Run` blocks; the caller puts it in a goroutine and waits on its own WaitGroup. There is no `Stop` — a
cancelled context ends both loops, and that is safe with no second signal because **both queries are
read-only**: nothing to commit, nothing to strand, so abandoning mid-flight is free. Cadence lives entirely
in the pipeline, so the cycle loop holds no timing policy of its own.

**THE BEAT MUST NOT SHARE THE CYCLE'S GOROUTINE, and this was a real bug.** Beating from the cycle loop
gates the beat not merely on the cycle *succeeding* — which it must not be, see below — but on the cycle
*returning*. Phase one's `rn <= capacity` cut early-stops only on **Postgres 15+**; on MySQL, SQL Server and
SQLite a deep backlog is still O(backlog), measured in the **tens of seconds** at a few million due rows.
Against a 40s peer-freshness window, one such scan drops a **healthy** replica out of `R` (every peer
regrows its pools) and out of the roster (every peer's ordinal reshuffles, handing residue classes around) —
exactly the outcome the registry is supposed to reserve for "the process is stuck, nothing less." A
separate loop makes the beat's cadence independent of how expensive the scan happens to be.

The two loops share exactly one field, `dispatchedSinceBeat`, which is atomic; nothing else needs
synchronizing. A consequence worth knowing when reading tests: the **first** beat fires before any cycle has
completed, so it correctly publishes *no* dispatch evidence, and the evidence appears on the next beat.

**Every `Set*` is safe from anywhere.** The live configuration is five atomics (`idle`, `partition`,
`logger`, `inst`, `seams`) rather than a mutex, and there is deliberately no grouped snapshot: nothing here
is coupled, so reading `idle` and the partition a microsecond apart cannot produce an inconsistent pair. The
four instruments are swapped as **one** atomic pointer, though, so a recording cycle never sees half of one
meter and half of another.

## Idle

An idle piston **still turns over** — it heartbeats, so the replica keeps its registry row and goes on
dividing the connection pools — but it runs no cycle and claims no work. That is the await-only replica.

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

## The heartbeat, and why it writes two timestamps

`seen_at` moves on every beat and means *this replica is alive and holding connections* — what the pool
divisor R counts. `dispatched_at` moves only when a cycle has actually completed since the last beat, and
means *this replica is genuinely serving* — what the candidate partition divides across.

**The distinction is evidence versus claim, and it exists because getting it wrong strands work.** A
replica handed a residue class of `step_id` it never selects leaves those steps unpicked by anyone. If the
divisor trusts a flag, a replica that publishes "I dispatch" and then wedges keeps its class forever. With
a timestamp, a wedged piston simply stops advancing `dispatched_at` and drops out of the divisor on its
own, while `seen_at` keeps it in R where it belongs. An idle piston runs no cycle, so it never advances
`dispatched_at` — the two populations separate with nothing believed. (`dispatches` still carries the same
fact as a flag, only until every reader has moved to the timestamp.)

**A cycle that found nothing due still counts.** It proves the piston looked and could have served.
Gating on candidates instead would make a quiet fleet read as having no dispatchers at all, disable
partitioning, and then thrash when work arrived.

**`dispatchedSinceBeat` is sticky** — any successful cycle sets it, the beat that publishes it clears it.
Beats are ~20x rarer than cycles, so sampling only the cycle that happens to precede one would let a
healthy piston look stalled whenever a transient error landed in that gap.

**The beat is NOT gated on the cycle succeeding**, and that is a correction to an easy assumption. There
is no MAX-union across shards protecting a silent piston: the reader picks **one** shard's roster whole
(`freshestRoster`), because R is its length and the ordinal is a position within it. So a piston that
stops beating can drop its whole replica from R and reshuffle ordinals. A shard whose scans are failing is
still a live replica holding connections; "this piston stopped beating" must mean the process is stuck,
nothing less.

**`lastBeat` advances even when the write fails.** Retrying a broken registry write every cycle would turn
a database blip into a write storm, and the next beat is a second away regardless. A beat on a cancelled
context is likewise dropped — best-effort by design, and the row ages out.

**`heartbeatInterval` (1s) is load-bearing, not merely cheaper than beating every cycle.** The
UPDATE-then-INSERT fallback reads `RowsAffected` to decide whether a row existed, and MySQL counts
CHANGED rows rather than matched ones — so two beats landing inside the same `NOW_UTC()` tick
(millisecond precision) would report zero and fire a spurious INSERT. At a second's spacing that cannot
happen; per-cycle, the margin is one millisecond. It is a `var` only so a test can shorten it, which is
why any test that does so must **not** be `t.Parallel()`.

## `engineID` is a constructor argument, not a setter

Its zero value is not a harmless default: `engine_id` is the registry's PRIMARY KEY, so every
unconfigured replica in a fleet would collide on id 0 and fight over one row. A setter makes that state
reachable; a constructor argument does not. It is immutable for the process, so it needs neither lock nor
atomic.

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

## One seam, and why it is the exception

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
capping, the `ROW_NUMBER()` ordering, the two-statement upsert, and the residue class splitting a real row
set disjointly across ordinals.
