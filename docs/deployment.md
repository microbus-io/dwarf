# Deployment

> **For operators** standing dwarf up in production, and for developers doing the wiring. It covers choosing
> and tuning a database, declaring shards, connection pools, workers, and running multiple replicas. Once it
> is running, [Operating dwarf](operations.md) takes over.

This guide covers running dwarf in production: choosing and tuning a database, sharding, connection pools,
configuration, and running multiple replicas.

## Configuration

All configuration is set with `Set*` methods, each returning an `error`. They split by whether the knob can
change on a running engine: the **live** ones (`SetMaxOpenConns`, `SetTimeBudget`,
`SetDefaultPriority`) take effect immediately, even after `Startup`; the **construction-time-only** ones
(`SetShard`, `SetWorkers`, and the dependency-injection setters below) are rejected if called
after `Startup`. `SetMaxOpenConns`, `SetTimeBudget`, and `SetDefaultPriority` are live.

| Method | Default | Purpose |
|---|---|---|
| `SetShard(spec)` | one default shard | Registers one shard: `ShardSpec{Index, DSN, VirtualCPUs, Cordoned}`; call once per shard |
| `SetWorkers(n)` | derived | Expert override: pins the worker *maximum* (deterministic tests, benchmarks, memory-bounded hosts). Normally unset — derived from the crash-recovery lease margin and the round-trip time measured at startup, so it holds for any task duration; the pool grows into it only on demand |
| `SetTimeBudget(d)` | 2m | Per-step `ExecuteTask` deadline |
| `SetDefaultPriority(p)` | 100 | Priority for flows that don't set one |
| `SetMaxOpenConns(n)` | derived | Expert override: pins every shard's pool exactly (benchmarks, external poolers). Normally unset - each shard's pool derives from its `VirtualCPUs` |

Provide `ShardSpec.VirtualCPUs` (the database server's CPU count - a fact off its spec sheet) and the
engine derives the shard's connection budget (the measured knee, beyond which connections only queue, and
on smaller servers actively destabilize or collapse throughput - sized from the CPU count together with
the round-trip time measured at startup) and its new-flow placement weight
(capacity-proportional across heterogeneous shards). Leave `VirtualCPUs` unset and the engine assumes 2
— the floor of every current-generation RDS class, and small enough that the resulting pool is still
safe on the 1-vCPU machines Cloud SQL offers. Declare it: it is a fact off the machine's spec sheet, and
an 8-vCPU database sized as if it were a 2-vCPU one runs at a fraction of its capacity. `Cordoned: true` excludes a shard
from new-flow placement (resident flows and their subgraph children/continuations/forks proceed) - for
retiring or overloaded shards. The [cloud benchmarks](benchmark-cloud.md) document the measurements
behind the constants.

Dependency injection (set before `Startup`): `SetHost`, `SetLogger`, `SetMeterProvider`,
`SetTracerProvider`.

## Choosing a database

Dwarf speaks four SQL dialects through [`sequel`](https://github.com/microbus-io/sequel); the dialect is
auto-detected from the DSN. They behave very differently under concurrent INSERT/UPDATE load.

### PostgreSQL — recommended for production

MVCC means concurrent INSERTs don't lock each other on secondary indexes, and there are no gap locks at the
default `READ COMMITTED` isolation, so the fan-out/fan-in pattern runs deadlock-free at any worker
concurrency. Use Postgres 13+ for `JSONB` and partial indexes. For throughput, raise `max_connections` to
at least `NumShards × MaxOpenConns × replicas` and `shared_buffers` to ~25% of host RAM.

### SQL Server

Enable `READ_COMMITTED_SNAPSHOT ON` per shard database for Postgres-like non-blocking reads and near-zero
deadlock risk. No other tuning is mandatory.

### MySQL / MariaDB — supported, expect tuning

InnoDB at the default `REPEATABLE READ` takes next-key (row + gap) locks on every secondary-index touch, so
concurrent flow creations on a shard can deadlock. The engine retries lock-contention errors, but a
sustained deadlock rate degrades throughput. To minimize it:

- `transaction-isolation = READ-COMMITTED` (drops gap locks — the biggest single reduction)
- `innodb_autoinc_lock_mode = 2` with `binlog_format = ROW`
- `innodb_lock_wait_timeout` 5–10s, `innodb_deadlock_detect = ON`

MariaDB 10.5+ for `JSON`.

### SQLite — testing and single-instance dev only

Single-writer, so deadlocks are structurally impossible but throughput tops out at one transaction at a
time. Used automatically by `NewEngineUnderTest`. Do not run SQLite in production.

## Disk throughput

Write bandwidth is a throughput ceiling **separate from CPU and connections**, and it is the one most
often mis-sized, because on managed cloud databases disk performance is usually provisioned by disk
*size* rather than set directly. GCP Cloud SQL, for example, scales IOPS with the disk (~30 IOPS/GB) up
to a per-instance cap; a small disk therefore silently caps a write-heavy workload no matter how many
CPUs the instance has.

The symptom is distinctive and worth recognizing: a disk at its throughput limit does not just make the
engine slower, it makes throughput **swing run-to-run** — a fixed configuration stops giving a
repeatable number, because the disk alternates between keeping up and falling behind. If your throughput
is bistable rather than steady, suspect the disk before the engine.

Size the disk to the **workload's measured write rate, not to a round number.** The engine emits
`dwarf_state_write_bytes` (payload bytes written to step rows — see [observability](observability.md));
sum its delta over a representative window and divide by the window to get the sustained write MB/s, then
provision the disk's throughput comfortably above that. Leave headroom: checkpoints and the write-ahead
log write on top of the step payload, so the disk sees more than `dwarf_state_write_bytes` alone. This is
strongly workload-dependent — a carry-heavy or fan-out graph writes far more per step than a small-state
one — which is exactly why the right disk size is something you measure rather than guess. When
throughput is repeatable, the disk has enough headroom; provisioning past that point buys nothing.

## Network distance to the database

**Round-trip time to the database is a first-class sizing input, not a detail.** A saturated shard
serves roughly

```
throughput  =  connections / (k × RTT + s)
```

where `k` is the number of database round trips one step makes — measured at **~9–12** — and `s` is the
per-step time that is not a round trip. Because `k` multiplies RTT, **every 0.1 ms of extra distance
costs about a millisecond of connection time on every step in the system.**

This is not a rounding error. Two Cloud SQL instances of identical size, in the same region **and the
same zone**, measured **0.32 ms and 0.82 ms** — and the second served **29% less throughput** on an
identical build. Nothing about the engine or the database differed; only where the instance happened to
land.

**Three things are worth doing, in order of payoff:**

1. **Put the database in the same zone as the engine**, not merely the same region. Cross-zone is the
   single largest avoidable jump.
2. **Connect over a private IP, and avoid a connection proxy** on the hot path. A proxy adds a hop to
   every one of those `k` round trips.
3. **Measure it, and keep measuring it.** Take the *minimum* of a dozen `SELECT 1`s on **one long-lived
   connection** (a fresh connection per sample measures the handshake instead, and a proxy or container
   socket measures the wrong path entirely).

**On managed databases, the zone is the finest control you get.** Cloud SQL exposes no rack- or
host-level placement, so the variation above is a lottery you can only re-roll by recreating the
instance. If round-trip time turns out to bound your workload and you need better, the escape hatch is
running PostgreSQL yourself on compute instances, where cloud providers *do* offer co-location controls
(GCP's compact placement policies, for example) — at the cost of owning backups, failover and patching.
That is a real trade, worth making only once you have measured that distance is what binds.

**How to tell that it is.** Compare your ceiling against database CPU. If throughput flattens while the
connection pool sits fully in use and the database still has substantial idle CPU, the limit is
distance, not size — the connections are spending their time on the wire, and **adding connections to
chase a CPU target that path cannot reach is how you find the over-connection collapse.** A bigger
instance does not fix a long wire; a closer one does.

## Sharding

Registering multiple shards with `SetShard` partitions flows across databases (or schemas) to scale write
throughput and reduce index contention. Rough sizing by tolerated concurrent INSERT/sec per shard:

| Engine | INSERT/sec per shard | Suggested shards |
|---|---|---|
| PostgreSQL | 1000+ | 1–4 |
| SQL Server (RCSI) | 500–1000 | 2–4 |
| MySQL/MariaDB (READ COMMITTED) | 200–500 | 4–8 |
| MySQL/MariaDB (REPEATABLE READ) | 50–200 | 8–16 |

Rules:

- Shard indices start at 1 and must be unique, but need **not** be contiguous — `Index: 1` and
  `Index: 99` is valid. The shard appears as the leading number of a flow key
  (`{shard}-{flowID}-{token}`) and drives routing, so the index→DSN mapping must be **identical across
  all replicas** and stable across restarts.
- Every shard database must exist before startup — the engine migrates the schema but does not
  `CREATE DATABASE`. Each shard's DSN is used exactly as given — the engine never rewrites it, so a
  percent-encoded credential (a password `p@ss` written `p%40ss`) is safe.
- The shard set is fixed for the engine's life: shards are opened and migrated at `Startup`, and
  `SetShard` is rejected after. Each flow key encodes its shard, so changing the set requires a
  coordinated restart of every replica (a maintenance window), not a live/piecemeal change.
- New top-level flows are placed across shards in proportion to their declared `VirtualCPUs`, so a bigger
  database receives proportionally more work; cordoned shards are skipped. Subgraph flows, thread
  continuations and forks all stay on their originating shard.

```go
eng.SetShard(engine.ShardSpec{Index: 1, DSN: "postgres://user:pass@db-a.internal:5432/dwarf?sslmode=disable", VirtualCPUs: 8})
eng.SetShard(engine.ShardSpec{Index: 2, DSN: "postgres://user:pass@db-b.internal:5432/dwarf?sslmode=disable", VirtualCPUs: 8})
```

## Connection pool

Each shard's pool derives from two things: its `ShardSpec.VirtualCPUs`, and the round-trip time the engine
measures to that database at startup. The idle core is half the open ceiling.

The second axis is the less obvious one. **A connection held while a packet is in flight does the database
no good**, so a pool sized from CPU count alone leaves a distant server idle — a 16-vCPU database 4.8 ms
away ran at **33% CPU** with a CPU-derived pool, two thirds of it unreachable. Sizing for distance as well
held throughput at **93–100% of the same-zone peak out to 5 ms**. So connections per vCPU **rise with
distance**, roughly doubling from same-zone to 2 ms:

| database | 0.25 ms | 0.5 ms | 1 ms | 1.5 ms | 2 ms and beyond |
|---|---|---|---|---|---|
| 1 vCPU | 3.6× | 6.0× | 8.4× | 10.8× | 13.2× |
| 2 vCPU | 3.6× | 7.2× | 8.4× | 12.0× | 13.2× |
| 4 vCPU | 3.6× | 7.2× | 7.2× | 9.6× | 12.0× |
| 8 vCPU | 6.0× | 9.6× | 9.6× | 13.2× | 13.2× |
| 16 vCPU | 6.0× | 8.4× | 8.4× | 10.8× | 10.8× |
| 32 vCPU | 4.5× | 5.6× | 6.7× | 9.7× | 10.5× |
| 64 vCPU and beyond | 2.7× | 4.2× | 6.0× | 7.2× | 7.2× |

Round-trip times between two columns are interpolated, so a 0.9 ms path lands 80% of the way from the
0.5 ms ratio to the 1 ms one — a plain linear blend. **Sizes between two rows are blended too, but not the
same way**: the ratio is smooth in the *logarithm* of the CPU count, not in the raw count, so a 24-vCPU
database is blended between the 16- and 32-vCPU rows in log-log space (linear in log(ratio) against
log(vCPUs), exact at every row in the table above) rather than simply taking the larger server's ratio —
that blend lands at 24 vCPUs' own ~5.1× at 0.25 ms, between 16's 6.0× and 32's 4.5×, closer to 32's. Each
tabulated value is the smallest pool measured to sustain that distance's achievable throughput, plus 20%:
at the bare minimum the tail degrades sharply (a 16-vCPU database 1 ms away measured a p99 of 1,344 ms at
its minimum against 203 ms at 26% above it), while much beyond that the extra connections buy nothing and
start costing. **Past 64 vCPU there is no second row to blend against**, so a larger database is sized as
if it were 64 vCPU (the lowest ratio measured, and therefore the safe direction — under-sizing costs
throughput rather than risking collapse) until that tier is itself measured.

**Do not read the ratios as a trend across database sizes.** They do not fall monotonically — 8 and 16 vCPU
need proportionally *more* than 4 vCPU, and 64 vCPU needs the least of any size measured — and the arms
behind each row were not held at a matched fraction of their own tier's ceiling, so part of that spread
measures how hard each was pushed rather than the tier itself. What the measurements establish is the
level, not the shape. The rate of rise with distance is not uniform either: it roughly doubles across the
range on most sizes but rises 2.7× on 64 vCPU.

**Confidence is not uniform across that table.** Every row is measured (Cloud SQL for PostgreSQL, one arm
per cell) — there is no projected or modelled row. A database larger than 64 vCPU has not been measured at
all; the flat extrapolation above is a deliberate, safe placeholder until it is.

**On how the numbers were obtained**, because it bounds how sharp they are. Each is the smallest pool that
sustained the load across a 120-second window. This system exhibits a recurring stall of roughly 30
seconds that clears itself while load continues, so a window of that length sometimes catches one and
sometimes does not: the same pool at the same rate measured 46% and 98% of the offered rate in two
windows. The values are therefore placed from where *starvation* ends — which is monotonic and
reproducible — rather than from where a collapse begins. Treat them as well-sized, not as edges, and if
you run a database near its limit, measure it over minutes rather than seconds.

**Compensation stops at 2 ms**, and past roughly there the answer is more shards rather than a bigger one:
reaching a 32-vCPU server's same-zone peak across a 4 ms path would take ~1,163 connections. For context,
same-zone round trips run **0.05–0.96 ms** and cross-AZ same-region about **1.1 ms** — both inside the
compensated range — while cross-region (10–40 ms) is far outside it.

An undeclared count assumes 2 vCPUs (a pool of 7 same-zone). That is the floor of every
current-generation RDS class, so the guess cannot undershoot there, and it stays safe even on a genuinely
1-vCPU machine (that tier sustained its load on 3 connections and ran clean at 14). What it cannot do is
use a large database: **declare
`VirtualCPUs`.** It is a fact off the server's spec sheet, and it is the single most valuable thing you
can tell the engine. `SetMaxOpenConns` is an expert override that pins every shard's pool exactly —
for benchmarking sweeps, externally-constrained connection budgets, or a connection pooler in front of the
database — and bypasses both axes above. It is otherwise best left unset. The measurements behind these
constants are in the [cloud benchmarks](benchmark-cloud.md).

> **Declare it correctly.** `VirtualCPUs` is trusted, not verified — the engine has no way to check it
> and does not try. Declaring *more* CPUs than the database has sizes the pool past that machine's knee,
> and over-connection does not merely waste connections: it collapses throughput (a 1-vCPU instance fell
> from 856 to 385 steps/s as its pool grew). Under-declaring is the safe direction — it costs throughput
> while the system stays healthy.

> **Running more than one replica?** The derived budget is a property of the shard's *database*, not
> of one replica: R replicas each holding the full derived pool would overshoot the knee R
> times over, into the over-connection zone the cap exists to prevent. The engine handles this
> automatically: each replica records a periodic heartbeat in the shard databases it already shares
> with the others, reads the live replica count back from them **per shard**, and takes its 1/R share of
> that shard's derived pool — resizing live as the fleet scales in or out, with nothing to declare. Per
> shard because the budget is: a replica that loses touch with one shard mis-sizes only that shard's pool.
> The count lives in the shared databases, so nothing has to be delivered between replicas for it to
> converge. A joining replica also waits to be seen by the others before it opens its own connections, so
> the fleet shrinks to make room for it rather than briefly overshooting the budget together. (Lowering a
> pool's limit closes nothing, so a peer's surplus connections drain as they are returned rather than
> instantly.) (`SetMaxOpenConns`, when used, is an exact per-replica number and is never divided.)

> **Crashing replicas and `SetEngineID`.** A replica identifies itself in the registry by an id that is
> random by default. A replica that *crashes* (rather than shutting down cleanly) leaves its last entry
> behind until it ages out, so for a short window the fleet counts one replica too many and every live
> replica takes a slightly smaller pool share — a self-correcting, safe-direction dip. If a replica
> restarts under a *fresh* random id each time (a crashloop), those entries can pile up faster than they
> age out and shrink the shares more. To avoid this, call `SetEngineID(id)` before `Startup` with a value
> that is **stable across that replica's restarts** and **unique across your live replicas** — for example
> one derived from the deployment's own per-instance identity (a StatefulSet pod name/ordinal, or the
> hostname). A restarting replica then reuses its one entry instead of leaving a ghost. Leave it unset
> (random) if you don't have such a value — a stable id that collides between two live replicas counts
> them as one and *over*-sizes pools, which is the harmful direction; random is the safe default.
> Several engines in one process are fine on the default (each gets its own id and counts as a distinct
> replica).

## Workers

Workers are goroutines that dispatch steps: claim, call `ExecuteTask`, write the result. The count needs
no configuration, and — importantly — it is **not** derived from how long your tasks take, which the
engine cannot know.

What bounds it is the crash-recovery lease. If every in-flight task is blocked on the same downstream
(an LLM provider having an outage, say) and that downstream recovers, every task finishes at once and
their completion transactions all queue for the shard's connections. A completion that waits longer than
its remaining lease margin has its step re-claimed by a peer — correct, but the task runs a second time,
which for a two-minute LLM call is real money. So the engine derives the largest pool that keeps such a
storm inside the margin: `N_max = M × margin ÷ txTime × safety`, where `txTime ≈ 7·L + 3 ms` is the
post-task database phase and `L` is the round-trip time it measures with a few `SELECT 1`s at startup.
The worst shard's number wins.

The pool **grows into that ceiling on demand**: it starts at a resident set sized by the connection
budget (which is what dispatch is actually bound by) and adds a worker whenever taking a candidate left
none free. Growth stops itself under load without asking the database anything: before a worker picks up
a candidate it waits for a turn at its shard, and a worker waiting for one is holding no work, so it
counts as free and the next check declines to spawn. A worker takes a turn only for the calls that use a
connection, never across the task itself, so a workload of long tasks grows to fit its own concurrency
while a database-bound one stops growing at the point extra workers would only queue. Short-task
deployments stay small; no knob either way.

Reasons to call `SetWorkers(n)` anyway: **memory** (each in-flight step holds its state map, a size the
engine cannot see — the ceiling can be tens of thousands), a deliberately smaller global bound, or
deterministic tests. Setting it *above* the ceiling is allowed and logged as a warning: you are trading
the risk of duplicate task execution in a storm for long-task throughput.

Worker count is deliberately **not** a backpressure mechanism — a single global cap cannot express
"64 concurrent LLM calls, 1,000 concurrent database lookups, 200 Jira writes". Per-downstream limits
belong in your `ExecuteTask` (a semaphore keyed on the actual provider or account), with `flow.Retry`
when the downstream pushes back.

## Running multiple replicas

Dwarf scales horizontally: run many engine replicas against the same shards. Each replica selects and
dispatches work independently; the database (via an atomic claim) arbitrates, so two replicas never run the
same step.

**There is nothing to wire between them.** Replicas coordinate entirely through the databases they already
share — pending work, flow outcomes and fleet membership are all discovered by reading, on cadences the
engine sets. No message bus, no peer-to-peer transport, no host method to implement. Running a second
replica is: point it at the same shards and start it.

That has three consequences worth knowing:

- **A cross-replica `Await` needs no delivery.** A flow created on replica A and completed on replica B is
  found by A reading the shared database, so `Await` returns promptly with nothing sent.
- **Fleet size is observed, not declared.** Each replica registers itself in a small `dwarf_peers` table
  per shard and reads the others back, which is what splits each shard's connection budget (above). A
  joining replica waits to be seen before it opens its own connections, so the fleet makes room for it
  rather than briefly overshooting together.
- **A replica that dies needs no goodbye.** Its rows stop being refreshed and it drops out of both counts
  on its own; a clean shutdown deletes them outright and the fleet regrows immediately.

## Shutting down

`Shutdown` drains gracefully: it stops accepting new work, then waits for every worker to finish the step it is
already running. A step is not abandoned mid-flight.

**Allow a drain window longer than the largest `TimeBudget` any of your flows declares.** A worker finishing a
task cannot be hurried — the engine will not abort a running task to shut down faster — so the drain takes as long
as the longest task still in flight. If your platform kills the process before the drain completes (a container
runtime's termination grace period, for example), those steps are abandoned mid-task: their leases lapse, another
replica recovers them, and **the tasks run again**. That is safe (execution is at-least-once and tasks must be
idempotent), but it is wasted work, and it re-fires side effects that had already happened.

The engine imposes no ceiling on `TimeBudget`, so the number is yours: if you cap task budgets in your own layer,
size the drain window above that cap. If you do not cap them, the drain is bounded only by your slowest task.

A worker that is mid-way through *persisting* a step's outcome — retrying a write after a database blip — does not
delay the drain: it notices the shutdown, hands the step back for another replica to pick up immediately, and exits.

## Crash recovery

Recovery is built in and needs no operator action. Every in-flight step holds a time-based lease; if a
worker crashes, the lease expires and a background poll returns the step to `pending` for re-execution.
Multi-statement operations are transactional, and the design is self-healing across crash points — a flow
left mid-transition is picked up and completed by the next poll. Steps that aren't idempotent under
re-dispatch should be written defensively (the engine guarantees at-least-once dispatch, not exactly-once).

Next: [Testing](testing.md).
