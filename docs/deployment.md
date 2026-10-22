# Deployment

> **For operators** standing dwarf up in production, and for developers doing the wiring. It covers choosing
> and tuning a database, declaring shards, connection pools, workers, and running multiple replicas. Once it
> is running, [Operating dwarf](operations.md) takes over.

This guide covers running dwarf in production: choosing and tuning a database, sharding, connection pools,
configuration, and running multiple replicas.

## Configuration

All configuration is set with `Set*` methods, each returning an `error`. They split by whether the knob can
change on a running engine: the **live** one (`SetTimeBudget`) takes effect immediately, even after `Startup`; the
**construction-time-only** ones
(`SetShard`, `SetWorkers`, `SetStateCacheSize`, `SetMaxPriority`, and the dependency-injection setters below) are
rejected if called after `Startup`.

| Method | Default | Purpose |
|---|---|---|
| `SetShard(spec)` | one default shard | Registers one shard: `ShardSpec{Index, DSN, MaxConnections, Weight, Cordoned}`; call once per shard |
| `SetWorkers(n)` | derived | Expert override: pins the worker *maximum* (deterministic tests, benchmarks, memory-bounded hosts). Normally unset — derived from the crash-recovery lease margin and the round-trip time measured at startup, so it holds for any task duration; the pool grows into it only on demand |
| `SetStateCacheSize(bytes)` | 10% of memory | Memory the engine may use to keep large state fields for the steps that read them next, one cache for all its shards. The default is a tenth of `GOMEMLIMIT` when set; otherwise, on Linux, of the tightest of the process's cgroup memory limits and physical memory; otherwise 1 GiB |
| `SetTimeBudget(d)` | 2m | Per-step `ExecuteTask` deadline |
| `SetMaxPriority(n)` | 5 | How many priority levels flows may use, 1 (first) to `n`, at most 100; a flow that sets none gets the midpoint (3 by default). Use the same value on every replica sharing a database |

**Set `GOMEMLIMIT` on engine hosts**, a little below the memory the process may use (about 90% of its
container's limit). The engine sizes its state cache from it, and the Go runtime collects harder as the heap
nears it. Without it, a full cache can take
up to twice its size, since Go's garbage collector lets the heap reach about twice its live size.

A shard needs only its `Index` and `DSN`. The engine sizes its connection pool on its own (see
[Connection pool](#connection-pool)); `MaxConnections` overrides that size, and `Weight` sets the shard's
share of new flows relative to the other shards (default 1). `Cordoned: true` excludes a shard from
new-flow placement (resident flows and their subgraph children/continuations/forks proceed) - for retiring
or overloaded shards. The [cloud benchmarks](benchmark-cloud.md) document the measurements behind the
defaults.

Dependency injection (set before `Startup`): `SetHost`, `SetLogger`, `SetMeterProvider`,
`SetTracerProvider`.

## Choosing a database

Dwarf speaks four SQL dialects through [`sequel`](https://github.com/microbus-io/sequel); the dialect is
auto-detected from the DSN. They behave very differently under concurrent INSERT/UPDATE load.

### PostgreSQL — recommended for production

MVCC means concurrent INSERTs don't lock each other on secondary indexes, and there are no gap locks at the
default `READ COMMITTED` isolation, so the fan-out/fan-in pattern runs deadlock-free at any worker
concurrency. Use Postgres 13+ for `JSONB` and partial indexes. Set `max_connections` above each shard's
derived pool budget plus two connections for every replica beyond the few that dispatch it (see
[Running more than one replica](#connection-pool)), and `shared_buffers` to ~25% of host RAM.

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

Add `interpolateParams=true` to the DSN, e.g. `user:pass@tcp(host:3306)/dwarf?interpolateParams=true`.
Without it, the MySQL driver sends every statement that has arguments as a prepare and then an execute - two
round trips where one would do - which roughly doubles the database round trips each step makes. With it, the
driver substitutes the arguments client-side and escapes them itself. The driver rejects it together with the
`big5`, `gbk`, `sjis` and `cp932` collations, so use a `utf8mb4` connection collation.

Set `max_allowed_packet = 512M` on the server. It caps the size of any one statement and of any one row the
server returns. Dwarf stores values up to 256 MB (see [the size limit](data-handling.md#the-size-limit-256-mb-per-value)),
and a step's input state and its changes sit side by side in one row that is read back whole, so a row can
hold two such values. The defaults, 64M on MySQL 8 and 16M on MariaDB, refuse a large value only after the
task that produced it has run.

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

where `k` is the number of database round trips one step makes — about **5–6** on PostgreSQL, which sends
a step's statements in batches, and **8–10** on the other dialects — and `s` is the per-step time that is
not a round trip. Because `k` multiplies RTT, **every 0.1 ms of extra distance costs roughly half a
millisecond to a millisecond of connection time on every step in the system.**

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
distance, not size — the connections are spending their time on the wire. Raising `MaxConnections` buys
some of it back, but a bigger instance does not fix a long wire; a closer one does.

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
- New top-level flows go to non-cordoned shards. With every `Weight` at its default of 1, a replica places a
  new flow on a shard it dispatches, so flows land where they are created; any weight other than 1 switches
  every replica to placement over all non-cordoned shards in proportion to their weights. In a fleet of
  unequal databases, give a larger one a larger weight (see [Resharding](resharding.md)).
  Subgraph flows, thread continuations and forks all stay on their originating shard.

```go
eng.SetShard(engine.ShardSpec{Index: 1, DSN: "postgres://user:pass@db-a.internal:5432/dwarf?sslmode=disable"})
eng.SetShard(engine.ShardSpec{Index: 2, DSN: "postgres://user:pass@db-b.internal:5432/dwarf?sslmode=disable"})
```

## Connection pool

Each shard has a **connection budget**: the connections every replica dispatching that shard may hold
there, together. By default the engine sets it from two things it measures at startup:

- **The round-trip time to the database.** The budget is **250** connections up to 0.25 ms, rising evenly
  to **500** at 1 ms and staying there beyond. A connection held while a packet is in flight does the
  database no good, so a distant database needs more of them to stay as busy.
- **The server's own `max_connections`.** The budget never exceeds **half** of it, leaving the rest for your
  application, administration and the server's reserved slots. A server configured small takes a smaller
  budget (a stock PostgreSQL limit of 100 gives 50); raise the server's limit and the engine follows.

On SQLite the budget is a fixed 8: SQLite writes one transaction at a time, so more connections only queue.
Connections are opened only as work needs them, and close again after two minutes idle, so a lightly
loaded shard holds far fewer than its budget.

**Setting `ShardSpec.MaxConnections`** replaces the derived budget with exactly that number - no adjustment
for distance and no cap. Declare the same value for a shard on every replica.

Measured on Cloud SQL for PostgreSQL 16 (1 TB SSD, each database driven to saturation, one run per cell),
the default sits at or near the best of the pools tried on every size:

| database | default `max_connections` | default budget near | best measured near | recommendation |
|---|---|---|---|---|
| 4 vCPU | 500 | 250 | 125-250 (alike, ~2,050 steps/s) | default |
| 8 vCPU | 600 | 250 | 150-300 (~4,100-4,500 steps/s) | default |
| 16 vCPU | 800 | 250 | 200 (~8,500 steps/s) | default |
| 32 vCPU | 1,000 | 250 | 250 (~20,900 steps/s) | default |
| 64 vCPU | 1,000 | 250 | 500 (~25,400-29,400 steps/s, +14-22% over 250) | `MaxConnections: 500` |

At 1 ms the default rises to 500 (or half the server's limit, if lower), which recovered most of what the
distance cost where it was measured. Throughput keeps climbing slowly as connections are added - more
transactions share each write to the log - so a pool past the budget is not a cliff, only diminishing
returns. Under-sizing costs throughput; it does not destabilize the database.

**Sizing beyond these defaults**, use `MaxConnections` when:

- the database is larger than 32 vCPU (the default stops growing with the server there);
- the database is shared with other heavy users and you want dwarf to take less of it;
- a connection pooler sits in front of the database, so the server's reported limit is not the real one;
- several shards are databases on one server: each takes its budget from that server's limit without
  knowing about the others, so split the server between them explicitly.

**Compensation for distance stops at 1 ms by design.** Past roughly 1.5-2 ms a large database cannot be kept
busy at any reasonable pool, and the answer is to place the engine closer, or to add shards. For context,
same-zone round trips run **0.05-0.96 ms** and cross-AZ same-region about **1.1 ms**; cross-region
(10-40 ms) is far outside what any pool can compensate.

> **Running more than one replica?** The derived budget is a property of the shard's *database*, not
> of one replica: R replicas each holding the full derived pool would overshoot the knee R
> times over. The engine handles this
> automatically. Each replica records a periodic heartbeat in the shard databases it already shares with
> the others, and from those rows the fleet agrees **per shard** on a small number of **dispatchers** that
> split that shard's budget and run its steps: two per shard once there are two replicas, so one replica
> failing does not stop a shard. Every other replica
> holds just **two connections** to that shard, enough to create flows, answer reads and wait on outcomes,
> and runs none of its steps. A replica that stops dispatching a shard — another replica took its place, or
> it is shutting down — keeps a dispatcher-sized share until the steps it already started there finish, and
> the shard's dispatchers make room for it, so that work completes at full speed. So a shard's connections
> stay near its budget plus about one per extra
> replica, rather than growing with the fleet — a host running many replicas for its own traffic does not
> over-connect its workflow databases. Different shards pick different dispatchers, so the work spreads
> across the fleet. It all resizes live as the fleet scales in or out, with nothing to declare, and a
> joining replica waits to be seen by the others before it opens its own connections, so the fleet makes
> room for it rather than briefly overshooting together. (Lowering a pool's limit closes nothing, so a
> peer's surplus connections drain as they are returned rather than instantly.)
>
> Declare **the same `MaxConnections` for a shard on every replica**, so every dispatcher takes the same share
> of it. A new flow is created on a shard
> its replica dispatches when it has one, so its first step runs at once - unless any shard declares a
> `Weight` other than 1, in which case new flows are placed by weight across every shard. A flow created, resumed, forked or
> continued on a replica that does not dispatch its shard waits for the next scan (well under a second)
> before its first step runs, and then runs normally. The engine warns in the log when a fleet's
> connections to a shard approach the server's `max_connections`; past that, put a connection pooler in
> front of the database.

> **Crashing replicas and `SetEngineID`.** A replica identifies itself in the registry by an id that is
> random by default. A replica that *crashes* (rather than shutting down cleanly) leaves its last entry
> behind until it ages out, about 40 seconds. If it was one of a shard's dispatchers, the shard runs on the
> remaining dispatchers until then — slower, not stopped, unless every dispatcher of that shard crashed at
> once. A replica shutting down cleanly whose drain is still waiting on tasks a quarter-second in hands its
> place on then, not when the drain ends, so a long in-flight task does not hold a shard short of dispatchers. If a replica restarts under a
> *fresh* random id each time (a crashloop), those entries can pile up faster than they age out. To avoid this, call `SetEngineID(id)` before `Startup` with a value
> that is **stable across that replica's restarts** and **unique across your live replicas** — for example
> one derived from the deployment's own per-instance identity (a StatefulSet pod name/ordinal, or the
> hostname). A restarting replica then reuses its one entry instead of leaving a stale one behind. Leave it
> unset (random) if you don't have such a value — a stable id that collides between two live replicas counts
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
storm inside the margin: `N_max = M × margin ÷ txTime × safety`, where `txTime ≈ k·L + 3 ms` is the
post-task database phase, `k` its round trips (4 on PostgreSQL, 6 on SQLite, 7 on MySQL and SQL Server),
and `L` the round-trip time it measures with a few `SELECT 1`s at startup.
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
  per shard and reads the others back, which is how the fleet agrees on each shard's dispatchers and splits
  its connection budget (above). A joining replica waits to be seen before it opens its own connections, so
  the fleet makes room for it rather than briefly overshooting together. A replica configured with
  `SetWorkers(0)` runs no steps anywhere; it registers as not working, holds two connections to each shard,
  and is never chosen to dispatch one. Each replica also records the round-trip time it measured to each shard, so
  `SELECT engine_id, rtt_us, working FROM dwarf_peers` on a shard shows the fleet as that shard sees it
  (`rtt_us` in microseconds).
- **A replica that dies needs no goodbye.** Its rows stop being refreshed and it drops out of both counts
  on its own; a clean shutdown deletes them outright and the fleet regrows immediately.

## Sizing a deployment

The measurements behind this recipe, with the configuration of every run, are in the
[cloud benchmarks](benchmark-cloud.md). Same-size instances differ, so treat any one figure as a range, and
measure your own workload: task shape, payload size and round-trip time all move these numbers.

1. **Shards.** Plan each 32 vCPU database at **~19,000 steps/s** sustained, 95% of the slower full-speed
   instances measured, and size from your load's shape ([Planning for peaks](#planning-for-peaks)):
   `shards = average steps/s ÷ 19,000`, rounded up, then check that each peak's backlog drains in time and adds
   a wait you accept, or put the work that cannot wait in a higher priority band. Below one shard's worth,
   a 16 vCPU database (~6,700 sustained) or an 8 vCPU database (~4,000) costs less in total, at a higher
   price per step.
2. **Engine replicas.** Two per shard at half the database's vCPU: `2S` replicas of 8 vCPU for 32 vCPU
   shards. Each runs at about a third of its CPU and can carry its shard alone. A larger host can serve
   several shards instead: give its engine about one core per 3,300 steps/s its shards can deliver at
   most - four 32 vCPU shards, ~20–24 cores - and set `GOMAXPROCS` to that plus whatever your tasks use
   in the same process, rather than leaving it at the host's full count. Run at least two such hosts, so
   one can fail. Expect less from it: one 48 vCPU engine on four shards ran 81% of what eight 8 vCPU
   replicas ran on the same databases, so prefer `2S` small replicas where you can.
3. **Your tasks.** Add the CPU and memory your tasks use on the engine hosts. The engine itself holds ~15 KB
   per flow in flight, plus the state of each step it is running - large payloads need far more
   ([State size](benchmark-cloud.md#state-size)); with long tasks, flows in flight = flows/s × flow duration.
4. **Distance.** Put the engine in its database's zone. Up to 1 ms the budget compensates; past it, plan for
   less per shard.
5. **Disk.** Size each database's disk for IOPS, not only capacity (see [Disk throughput](#disk-throughput)).

**Worked example.** 50,000 steps/s on average (5,000 flows/s of 10-step flows), with a daily peak of 65,000
for 10 minutes: 3 × 32 vCPU shards sustain ~57,000, so the average fits. The peak leaves a backlog of
(65,000 − 57,000) × 600 s = 4.8 million steps, which clears in ~11 minutes once the load falls back to 50,000
and adds up to ~84 seconds to a flow meanwhile. If that wait is acceptable, the deployment is 3 shards and
6 × 8 vCPU engine replicas, about $6,900 a month at on-demand list price before your tasks' own cost. If it is
not, put the latency-sensitive flows in a higher priority band, or plan a fourth shard.

### Planning for peaks

**Plan from the shape of your load, not one percentage.** Call the rate a database sustains `C` (about 95% of
its saturated rate, [measured](benchmark-cloud.md#sustained-load)):

- **The average must stay at or below `C`.** Above it the backlog grows without end.
- **Peaks above `C` cost waiting, not work.** A peak at `P` steps/s lasting `T` seconds leaves a backlog of
  `(P − C) × T` steps. It drains at `C − A`, where `A` is the load after the peak, and while it lasts each new
  flow waits about `backlog ÷ C` longer.
- **Your tolerance for that wait decides the headroom.** If waiting during peaks is unacceptable, size so that
  peaks stay under `C`. If it is acceptable, size for the average, with quiet periods long enough to drain each
  peak before the next.

For example, a shard sustaining 19,000 steps/s that sees two minutes at 24,000 builds a backlog of 600,000 steps.
With the load back at 17,000 that clears in five minutes, and at its largest it adds about 30 seconds to a flow.

**Priority bands let urgent work skip that wait.** The engine always dispatches a higher band's steps ahead of
any lower band's, so during a peak the backlog forms in the lowest bands. Flows that cannot wait run in a higher
band and keep their latency; best-effort flows absorb the peak and catch up in the quiet that follows. What has
to stay under `C` is then the urgent work alone. Three things to know:

- A flow's priority is set when it is created and does not change.
- Dispatch is not preemptive, but the worker pool grows on demand, so a high-priority step normally waits only
  for a database connection to free up, typically well under a second; a step whose task is still running holds
  no connection. Only when every worker the engine may run is busy does it wait for a task to finish.
- Lower bands run on whatever capacity the higher ones leave unused; a higher band that keeps the engine at
  capacity keeps the lower bands from running at all.

### Reference configurations

Measured points to start a capacity plan from. All are Cloud SQL for PostgreSQL 16 on a 1 TB SSD, in the
same zone as the engine (round trip ~0.06 ms), running a 10-step chain of no-op tasks saturated against a
fresh database, averaged over a 16-minute window. The full conditions are in the
[cloud benchmarks](benchmark-cloud.md).

| shards × database vCPUs | engine replicas | steps/s |
|---|---|---|
| 1 × 8 | 2 × 2 vCPU | 4,186 |
| 1 × 16 | 2 × 4 vCPU | 7,015–7,122 |
| 1 × 32 | 1 × 16 vCPU | 21,878–21,952 |
| 1 × 64 (`MaxConnections: 500`) | 1 × 16 vCPU | 29,415 |
| 1 × 32 | 2 × 8 vCPU | 21,267 |
| 2 × 32 | 4 × 8 vCPU | 43,135 |
| 4 × 32 | 8 × 8 vCPU | 81,530 |
| 4 × 32 | 1 × 48 vCPU | 66,250 |
| 4 × 32 | 4 × 16 vCPU | 69,382 (earlier engine) |

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

For example, if your layer caps every task at 15 minutes, set the platform's termination grace period (on
Kubernetes, `terminationGracePeriodSeconds`) to 15 minutes plus a margin for the process's own shutdown, such
as 16 minutes. Container platforms commonly default this to well under a minute, which cuts short the drain
of any task longer than that.

A worker that is mid-way through *persisting* a step's outcome — retrying a write after a database blip — does not
delay the drain: it notices the shutdown, hands the step back for another replica to pick up immediately, and exits.

## Crash recovery

Recovery of a crashed replica's work is built in. Every in-flight step holds a lease of its `TimeBudget`
plus a 30-second margin; if the replica running it dies, the lease lapses and a background sweep, which
runs every minute, returns the step to `pending` for another replica. So a step claimed by a crashed
replica runs again within `TimeBudget` + 30 s + 1 minute. Steps that were pending but not yet claimed
are picked up by the surviving replicas within seconds. Steps that aren't idempotent under re-dispatch
should be written defensively: the engine guarantees at-least-once dispatch, not exactly-once.

One crash window is repaired differently. A replica that dies after recording a step as completed but
before inserting the step that follows it leaves the flow `running` with nothing left to dispatch. Once the
step's lease has lapsed, the next recovery sweep hands the step back - on the same `TimeBudget` + 30 s + 1 minute
bound - and dispatch creates its successor from the recorded output, without running the task again. These
recoveries count in `dwarf_steps_recovered`. A flow the engine cannot repair this way counts in
`dwarf_flows_orphaned` and needs manual recovery, described in the
[Runbook](runbook.md#dwarf_flows_orphaned-is-non-zero).

Next: [Testing](testing.md).
