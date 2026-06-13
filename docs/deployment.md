# Deployment

This guide covers running dwarf in production: choosing and tuning a database, sharding, connection pools,
configuration, and running multiple replicas.

## Configuration

All configuration is set with `With*` builder methods. They are applied atomically, so they double as
**hot-reconfiguration** knobs — safe to call after `Startup`.

| Method | Default | Purpose |
|---|---|---|
| `WithDSN(dsn)` | `""` | Database connection string (dialect auto-detected) |
| `WithNumShards(n)` | 1 | Number of database shards |
| `WithWorkers(n)` | 64 | Per-replica worker concurrency cap |
| `WithTimeBudget(d)` | 2m | Per-step `TaskExecutor` deadline |
| `WithDefaultPriority(p)` | 100 | Priority for flows that don't set one |
| `WithMaxOpenConns(n)` | 8 | Max open DB connections per shard (idle == open) |

Dependency injection (set before `Startup`): `WithGraphLoader`, `WithTaskExecutor`,
`WithFlowStoppedCallback`, `WithPeerNotifier`, `WithLogger`, `WithMeterProvider`, `WithTracerProvider`.

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

`WithNumShards` partitions flows across databases (or schemas) to scale write throughput and reduce index
contention. Rough sizing by tolerated concurrent INSERT/sec per shard:

| Engine | INSERT/sec per shard | Suggested shards |
|---|---|---|
| PostgreSQL | 1000+ | 1–4 |
| SQL Server (RCSI) | 500–1000 | 2–4 |
| MySQL/MariaDB (READ COMMITTED) | 200–500 | 4–8 |
| MySQL/MariaDB (REPEATABLE READ) | 50–200 | 8–16 |

Rules:

- Shards are **1-indexed**. The shard appears as the leading number of a flow key (`{shard}-{flowID}-{token}`).
- When `NumShards > 1`, the DSN **must contain `%d`**, replaced with the shard index. Every shard database
  must exist before startup — the engine migrates the schema but does not `CREATE DATABASE`.
- `NumShards` can **grow** at runtime (new shards open, migrate, and become available immediately) but
  cannot shrink (old shards drain naturally — new flows land on new shards, existing flows stay put).
- New top-level flows pick a random shard; subgraph flows stay on the parent's shard.

```go
eng.WithDSN("postgres://user:pass@db:5432/dwarf_%d?sslmode=disable").WithNumShards(4)
```

## Connection pool

`WithMaxOpenConns` (default 8 per shard, with `MaxIdle == MaxOpen`) sizes each shard's pool. Workers spend
most of their time waiting on the `TaskExecutor` call, not holding a SQL connection, so a small absolute
number suffices. Keeping idle == open matters more than the absolute number: under bursty load, close/reopen
churn (TCP + TLS + auth per cycle) dominates query time. Pool 8 is a good default; much larger regresses
(pool-mutex contention with no usable extra concurrency). Tune explicitly only for a different workload mix.

## Running multiple replicas

Dwarf scales horizontally: run many engine replicas against the same shards. Each replica selects and
dispatches work independently; the database (via an atomic claim) arbitrates, so two replicas never run the
same step. Most coordination is recovered automatically by each replica's background poll, but for low
latency replicas exchange **fire-and-forget peer signals**.

Provide a `PeerNotifier` to publish those signals to your other replicas (over whatever transport you
have), and feed inbound signals back in with the matching `Handle*` method:

```go
type PeerNotifier interface {
    Enqueue(ctx context.Context, shard, stepID int)                          // a step is pending
    SyncValve(ctx context.Context, taskName string, wCong int, tCong time.Time) // rate-controller gossip
    TripBreaker(ctx context.Context, taskName string)                        // breaker gossip
    NotifyStatusChange(ctx context.Context, flowKey string, status string)   // wake cross-replica Await
}
```

| Outbound (you publish) | Inbound (you call on receipt) |
|---|---|
| `Enqueue` | `HandleEnqueue(ctx, shard, stepID)` |
| `SyncValve` | `HandleSyncValve(ctx, taskName, wCong, tCong)` |
| `TripBreaker` | `HandleTripBreaker(ctx, taskName)` |
| `NotifyStatusChange` | `HandleNotifyStatusChange(ctx, flowKey, status)` |

Two delivery rules:

- **Deliver to other replicas only.** The engine applies each signal locally *before* publishing it, so a
  signal echoed back to the sender is processed twice. If your transport delivers to the publisher, filter
  out self-delivery.
- **`NotifyStatusChange` is what wakes a cross-replica `Await`.** A flow created on replica A but completed
  on replica B wakes A's `Await` only via this broadcast — without it, A blocks until its context deadline.

In a single-replica deployment, leave `PeerNotifier` nil; none of this runs, and the background poll is the
only (and sufficient) recovery path.

## Crash recovery

Recovery is built in and needs no operator action. Every in-flight step holds a time-based lease; if a
worker crashes, the lease expires and a background poll returns the step to `pending` for re-execution.
Multi-statement operations are transactional, and the design is self-healing across crash points — a flow
left mid-transition is picked up and completed by the next poll. Steps that aren't idempotent under
re-dispatch should be written defensively (the engine guarantees at-least-once dispatch, not exactly-once).

Next: [Testing](testing.md).
