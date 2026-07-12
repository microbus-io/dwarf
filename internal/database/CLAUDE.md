# Dwarf `internal/database` — sharded SQL connections, dialects, and topology

> Load when: changing shard open/close/routing, the SQL-dialect guidance, the connection lifecycle, or the
> test-mode DSN resolution.
> Coupled with: `engine/CLAUDE.md` §"Connection pool sizing" (the engine computes the two pool sizes this package
> applies) and §"Database Sharding" (the engine-side *semantics* — flow-key shard encoding, subgraph shard
> affinity, `List` pagination); `internal/migrations/CLAUDE.md` (the schema this package migrates);
> `fixtures/CLAUDE.md` (the test-harness rationale behind the DSN resolution `Open` performs).

`ShardSet` owns the shard `map[int]*sequel.DB`: it opens+migrates every shard, routes by index (`Shard`), fans an
op out over all shards (`OnEach`), applies pool sizes, and closes. Each shard arrives as a `ShardConfig` (DSN +
resolved per-shard pool sizes) — the sizing *formula* is engine policy (see the engine doc §"Connection pool
sizing"), not this package's concern; the uniform `SetMaxIdleConns`/`SetMaxOpenConns` remain for the engine's
live override.

## Choosing a SQL engine

The engine speaks four SQL dialects via `sequel`: SQLite, MySQL/MariaDB, PostgreSQL, SQL Server. They behave very
differently under concurrent INSERT/UPDATE load. Pick by deployment shape.

**PostgreSQL - recommended for production.** MVCC means concurrent INSERTs do not lock each other on secondary
indexes; no gap locks at default `READ COMMITTED`; the fan-out/fan-in pattern runs deadlock-free at any worker
concurrency. Use Postgres 13+ for `JSONB` and partial indexes. For throughput, raise `max_connections` to at least
`(NumShards * MaxOpenConnsPerShard * replicas)` and `shared_buffers` to ~25% of host RAM.

**MySQL / MariaDB - supported, expect tuning.** InnoDB at default `REPEATABLE READ` takes next-key (row + gap) locks
on every secondary-index touch; two concurrent flow creations on a shard can deadlock on overlapping index ranges.
`createWithGraph` retries on `sequel.IsLockContentionError`, hiding most, but a sustained deadlock rate degrades
throughput. To minimize: `transaction-isolation = READ-COMMITTED` (drops gap locks; the largest single reduction);
`innodb_autoinc_lock_mode = 2` with `binlog_format = ROW`; `innodb_lock_wait_timeout` 5-10s; keep
`innodb_deadlock_detect = ON`. Per-shard databases: every shard's DB must exist before startup (the engine migrates
schema but does not `CREATE DATABASE`). MariaDB 10.5+ for `JSON`.

**SQL Server.** Enable `READ_COMMITTED_SNAPSHOT ON` per shard database for Postgres-like non-blocking reads and
near-zero deadlock risk. No other tuning mandatory.

**SQLite - testing and single-instance dev only.** Single-writer means deadlocks are structurally impossible (writes
serialize) but throughput tops out at one transaction at a time. Used automatically by `RunInTest` with an empty DSN.
The injected `busy_timeout` keeps workers from immediately failing on `SQLITE_BUSY` during fan-out; do not remove it.
Do not run SQLite in production.

## Sharding guidance & topology

Registering multiple shards with `SetShard(index, dsn)` partitions flows across databases (or schemas). Shard count
should equal or exceed steady-state concurrent flow-creating threads divided by the per-shard write contention the
engine tolerates. Rough sizing:

| Engine | Concurrent INSERT/sec per shard before contention | Suggested shards |
|---|---|---|
| PostgreSQL | 1000+ | 1-4 |
| SQL Server (RCSI) | 500-1000 | 2-4 |
| MariaDB/MySQL (RC) | 200-500 | 4-8 |
| MariaDB/MySQL (RR) | 50-200 | 8-16 |

The shard set is fixed for the engine's life (`SetShard` is construction-time only); changing it requires a
coordinated restart of every replica - see `engine/CLAUDE.md` §"Database Sharding" for why (flow keys encode the
shard).

**Shard-per-server is the recommended production topology.** Put each shard on its **own database server** - each
`SetShard` names a different server (arbitrary hostnames, e.g. cloud-managed instances, need no naming pattern):

```
# PROD (distributed): one server per shard
SetShard(1, "postgres://user:pass@db-a.internal:5432/dwarf?sslmode=disable")
SetShard(2, "postgres://user:pass@db-b.internal:5432/dwarf?sslmode=disable")

# test/dev (co-located): shards as databases on one server (%d substitutes the shard index)
SetShard(1, "postgres://user:pass@127.0.0.1:5432/dwarf_%d?sslmode=disable")
SetShard(2, "postgres://user:pass@127.0.0.1:5432/dwarf_%d?sslmode=disable")
```

The connection budget is a **per-server** property. With distributed shards each server hosts one shard per replica,
so it sees only `replicas × perShardPool` connections - **shard count never multiplies a server's load**, and the
per-shard ceiling (`SetMaxOpenConns`) *is* the per-server budget. With co-located shards (all shards as databases on
one server) the server sees `replicas × shards × perShardPool`, so the `× shards` multiplier can overrun the server's
`max_connections` under parallel load - which is why co-located is for test/dev/single-instance only. Both forms are
transparent to the engine and sequel: each shard gets its own independent pool, so the topology is purely a
deployment choice (zero code change). The engine migrates schema but does
**not** `CREATE DATABASE` or provision servers - those must exist.

## Connection lifecycle (idle drain & lifetime recycle)

`Open` also gives each server-driver pool a **2-minute `ConnMaxIdleTime`** (idle connections drain after a quiet
spell, releasing server connections post-burst; reuse resets each connection's idle clock, so a steadily-loaded shard
keeps its core warm) and a **1-hour `ConnMaxLifetime`** (every connection recycles at that age - sheds stale
connections, lets LB/DNS/failover rebalance; ~one reconnect/connection/hour). These are constants, not knobs, set via
the `*sql.DB` methods promoted through `sequel.DB`. They are **skipped for SQLite**, whose in-memory test databases are
dropped the instant their last connection closes. They are a production win (long-lived replicas); they never fire
during the fast test suite.

## Sharding mechanics

**Shard indices are sparse.** Valid indices are unique integers `>= 1`, not necessarily contiguous (`SetShard(1, …)`
+ `SetShard(99, …)` is fine - arbitrary DSNs need no naming pattern, and a drained shard's index could one day retire
without renumbering); `0` is a sentinel meaning "no shard / all shards" (used by `Query.Shard`). The index is the
leading number in flow keys (`{shard}-{flowID}-{token}`) and what `Query.Shard`/`ShardInfo` report. Internally `dbs`
is a `map[int]*sequel.DB` plus a sorted `indices` slice (deterministic `OnEach`/`Indices()` order); `Shard(n)` is a
map lookup whose miss returns a uniform not-found (an unregistered shard in a well-formed key must be
indistinguishable from a bad key). `Open` rejects two shards resolving to the same DSN - a silent collapse onto one
database would share flow_id sequences across "shards" and corrupt routing. The engine-side *encoding* of the shard
into flow keys is in `engine/CLAUDE.md`.

**Cross-shard fan-out is always parallel, never sequential.** `OnEach` builds a per-shard job set and runs them in
parallel. A sequential per-shard loop would grow total latency linearly with the shard count (at 8 shards a 10ms-per-shard
query becomes 80ms wall-clock); the parallel shape stays at single-shard latency regardless of shard count. (The
single-shard case skips the goroutines.)

**Not shard-fault-tolerant by design.** `OnEach` fails the whole call on any shard's error. A partial-tolerance
attempt was rejected: real outages mostly manifest as hangs, not errors; classifying "shard down" vs transient/data
errors is driver-specific and brittle; and a helper that *claims* partial tolerance only in a narrow subset of failure
modes lies to operators about resilience. `OnEach` is invoked once per shard with the resolved DB and the shard
index; any non-nil return fails the whole call. Each caller retries on its next natural cycle (`pollPendingSteps` next
tick, `scanPriorityBand` next refill), so a transient hiccup heals within one cycle and a persistent outage degrades
loudly.

**DSN format & test-mode resolution.** Each shard carries its own DSN (`Config.Shards`, index -> DSN); a `%d` in a
DSN is substituted with the shard index (a convenience for patterned database names and the test defaults, not a
requirement). `Open` resolves per shard: an explicit DSN wins; else, in test mode (`Config.TestID` set),
`SEQUEL_TESTING_DSN`, then the SQLite in-memory default `file:dwarf_%d?mode=memory&cache=shared`; after `%d`
substitution, test mode wraps the result via `sequel.CreateTestingDatabase` into an isolated, auto-dropped database
keyed on `(driver, baseDSN, TestID)`. Two shards resolving to the same DSN are rejected at `Open` (the collapse
guard). The test-harness rationale (per-test isolation, why the `%d` default, the multi-replica shared-key case)
lives in `fixtures/CLAUDE.md`.
