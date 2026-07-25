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

## `Run`

```
cycle (paced by the pipeline) -> record -> heartbeat if due -> repeat
```

`Run` blocks; the caller puts it in a goroutine and waits on its own WaitGroup. There is no `Stop` — a
cancelled context ends it, and that is safe with no second signal because **both queries are read-only**:
nothing to commit, nothing to strand, so abandoning mid-flight is free. Cadence lives entirely in the
pipeline, so the loop body here holds no timing policy of its own.

**`Run` is single-goroutine; every `Set*` is safe from anywhere.** The live configuration is four atomics
(`idle`, `partition`, `logger`, `inst`) rather than a mutex, and there is deliberately no grouped
snapshot: nothing here is coupled, so reading `idle` and the partition a microsecond apart cannot produce
an inconsistent pair. The four instruments are swapped as **one** atomic pointer, though, so a recording
cycle never sees half of one meter and half of another.

## Idle

An idle piston **still turns over** — it heartbeats, so the replica keeps its registry row and goes on
dividing the connection pools — but it runs no cycle and claims no work. That is the await-only replica.

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

## Metrics take a `Meter`, not a `MeterProvider`

So the **owner** picks the instrumentation scope once for every module it assembles. Each package deriving
its own scope from a provider would split one engine's metrics across several scopes in the export the
moment a name drifted, and nothing here needs provider-level capability. Instrument names are a public
surface that dashboards bind to — `TestPiston_RecordsItsInstruments` asserts them by name so a rename
fails loudly.

## No test seams, and none are needed

Every fault an engine-level test could want is reachable through the engine's own `Source` implementation
— which is where the equivalent injected fault already lives — or through `SetInterval`/`SetMinGap`, and
`pipeline.Result` gives a caller everything a counting checkpoint would. A seam inside pure logic is a
signal a dependency should have been injected instead; here the dependency already is.

## Tests run against real SQL

Each test stands up its own isolated, migrated database through `internal/database.ShardSet` — a
**test-only** import, since the piston itself never opens a handle — and inserts real `dwarf_steps` rows.
That is what lets them cover things a fake could not: the parked-step predicate, the capacity cap actually
capping, the `ROW_NUMBER()` ordering, the two-statement upsert, and the residue class splitting a real row
set disjointly across ordinals.
