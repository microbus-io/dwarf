# Deployment

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
engine derives the shard's connection budget (~6x CPUs, the measured knee beyond which connections only
queue - and on small servers actively collapse throughput) and its new-flow placement weight
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
- New top-level flows pick a random shard; subgraph flows stay on the parent's shard.

```go
eng.SetShard(engine.ShardSpec{Index: 1, DSN: "postgres://user:pass@db-a.internal:5432/dwarf?sslmode=disable", VirtualCPUs: 8})
eng.SetShard(engine.ShardSpec{Index: 2, DSN: "postgres://user:pass@db-b.internal:5432/dwarf?sslmode=disable", VirtualCPUs: 8})
```

## Connection pool

Each shard's pool derives from its `ShardSpec.VirtualCPUs`: open = ~6× the database's CPU count (the
measured knee — beyond it connections only queue inside the database, and on small servers actively
harm throughput), with a warm idle core of half that. An undeclared count assumes 2 vCPUs (a pool of
12), which stays under the knee of even a 1-vCPU machine — so a zero-config engine cannot reach the
collapse zone, but it also cannot use a large database: **declare `VirtualCPUs`.** `SetMaxOpenConns`
is an expert override that pins every shard's pool exactly — for benchmarking sweeps or
externally-constrained connection budgets — and is otherwise best left unset. The measurements behind
these constants are in the [cloud benchmarks](benchmark-cloud.md).

> **Declare it correctly.** `VirtualCPUs` is trusted, not verified — the engine has no way to check it
> and does not try. Declaring *more* CPUs than the database has sizes the pool past that machine's knee,
> and over-connection does not merely waste connections: it collapses throughput (a 1-vCPU instance fell
> from 856 to 385 steps/s as its pool grew). Under-declaring is the safe direction — it costs throughput
> while the system stays healthy.

> **Running more than one replica?** The derived budget is a property of the shard's *database*, not
> of one replica: R replicas each holding the full ~6 × `VirtualCPUs` pool would overshoot the knee R
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
budget (which is what dispatch is actually bound by) and adds a worker whenever every existing one is
parked in a task and more work is waiting. So a short-task deployment stays small, and a workload of
long tasks grows to fit its own concurrency — no knob either way.

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
