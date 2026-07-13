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
time. Used automatically by `RunInTest`. Do not run SQLite in production.

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

- Shard indices start at 1 and must be unique, but need **not** be contiguous — `SetShard(1, …)` and
  `SetShard(99, …)` is valid. The shard appears as the leading number of a flow key
  (`{shard}-{flowID}-{token}`) and drives routing, so the index→DSN mapping must be **identical across
  all replicas** and stable across restarts.
- Every shard database must exist before startup — the engine migrates the schema but does not
  `CREATE DATABASE`. A `%d` in a DSN is substituted with the shard index (a convenience for patterned
  hostnames or database names); arbitrary per-shard DSNs need no `%d`.
- The shard set is fixed for the engine's life: shards are opened and migrated at `Startup`, and
  `SetShard` is rejected after. Each flow key encodes its shard, so changing the set requires a
  coordinated restart of every replica (a maintenance window), not a live/piecemeal change.
- New top-level flows pick a random shard; subgraph flows stay on the parent's shard.

```go
eng.SetShard(1, "postgres://user:pass@db-a.internal:5432/dwarf?sslmode=disable")
eng.SetShard(2, "postgres://user:pass@db-b.internal:5432/dwarf?sslmode=disable")
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

> **Running more than one replica?** The derived budget is a property of the shard's *database*, not
> of one replica: R replicas each holding the full ~6 × `VirtualCPUs` pool would overshoot the knee R
> times over, into the over-connection zone the cap exists to prevent. The engine handles this
> automatically: replicas discover each other over the peer-signal channel (a hello on startup, a
> periodic ping, a goodbye on shutdown) and each takes its 1/R share of every derived pool, resizing
> live as the fleet scales in or out — nothing to declare. This makes wiring `SignalPeers` (below)
> load-bearing for pool sizing, not just for wake latency: a multi-replica deployment that leaves it
> a no-op has each replica believing it is alone, over-connecting the shard.
> (`SetMaxOpenConns`, when used, is an exact per-replica number and is never divided.)

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
same step. Most coordination is recovered automatically by each replica's background poll, but for low
latency replicas exchange **fire-and-forget peer signals**.

Implement your host's `SignalPeers` to publish those signals to your other replicas (over whatever
transport you have), and feed inbound signals back in with `DeliverSignal`. All signal kinds funnel
through this one method on the `Host` interface — the engine pre-serializes the body, so your host is a
pure pipe that never inspects `op` or `payload`:

```go
type Host interface {
    // ... required LoadGraph / ExecuteTask ...

    // op is a routing key (usable as a topic); payload is opaque bytes. Ship (op, payload) to OTHER
    // replicas; on receipt call eng.DeliverSignal(ctx, op, payload).
    SignalPeers(ctx context.Context, op string, payload []byte)
}
```

```
Outbound:  eng emits → host.SignalPeers(ctx, op, payload) → your transport → peers
Inbound:   peer transport → host → eng.DeliverSignal(ctx, op, payload)
```

Two delivery rules:

- **Deliver to other replicas only.** The engine applies each signal locally *before* publishing it, so
  the sender has nothing to learn from its own broadcast. If your transport delivers to the publisher,
  filter out self-delivery. As a backstop the engine also stamps each signal with its own instance id
  and silently discards an echo of its own broadcast — but that discard still costs your transport a
  round-trip per signal, so filtering at the transport remains the right design.
- **The flow-stop signal is what wakes a cross-replica `Await`.** A flow created on replica A but completed
  on replica B wakes A's `Await` only via this broadcast — without it, A blocks until its context deadline.

In a single-replica deployment, leave `SignalPeers` a no-op; none of this runs, and the
background poll is the only (and sufficient) recovery path.

## Crash recovery

Recovery is built in and needs no operator action. Every in-flight step holds a time-based lease; if a
worker crashes, the lease expires and a background poll returns the step to `pending` for re-execution.
Multi-statement operations are transactional, and the design is self-healing across crash points — a flow
left mid-transition is picked up and completed by the next poll. Steps that aren't idempotent under
re-dispatch should be written defensively (the engine guarantees at-least-once dispatch, not exactly-once).

Next: [Testing](testing.md).
