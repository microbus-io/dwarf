# Dwarf `internal/database` — sharded SQL connections, dialects, and topology

> Load when: changing shard open/close/routing, a shard's turns or residue class, the SQL-dialect guidance, the
> connection lifecycle, or the test-mode DSN resolution.
> Coupled with: `engine/CLAUDE.md` §"Connection pool sizing" (the engine computes the two pool sizes this package
> applies) and §"Database Sharding" (the engine-side *semantics* — flow-key shard encoding, subgraph shard
> affinity, the lists' cross-shard merge); `internal/migrations/CLAUDE.md` (the schema this package migrates);
> `fixtures/CLAUDE.md` (the test-harness rationale behind the DSN resolution `Open` performs);
> `internal/turnstile/CLAUDE.md` (the ordering a shard's turns follow) and `engine/CLAUDE.md` §"Turn-taking on the
> database" (the bands and where the engine takes turns).

`ShardSet` owns the shards, a `map[int]*Shard`: it opens+migrates every shard, routes by index (`Shard`), fans an
op out over all shards (`OnEach`), applies pool sizes, and closes. Each shard arrives as a `ShardConfig` (DSN +
resolved per-shard pool sizes) — the sizing *formula* is engine policy (see the engine doc §"Connection pool
sizing"), not this package's concern; the engine pushes each shard's size through `SetShardPool`.

## A `Shard` is the handle every actor on a shard is given

A `Shard` holds what everything working on one shard needs: its index, its `*sequel.DB`, its turns, and this
replica's residue class among the shard's dispatchers. Actors take the `Shard` rather than each piece wired
separately.

- **Every database call takes a turn, and the caller takes it** (`Shard.WaitTurn`, or `ContextWithPriority` once
  per job and the package's `WaitTurn(ctx)` at each call). Admission is this package's responsibility because all
  database access goes through it, but the turn is NOT bundled with the DB: a `WaitTurn` that returned the DB would
  make every helper that needs the DB take a turn, and a helper called under its caller's turn would then take a
  second one - hold-and-wait on the turnstile, which deadlocks once every turn is held by a caller waiting for
  another. So the DB and the turn are handed out separately, and the turn is taken at the outermost call
  (`engine/CLAUDE.md`, "Turn-taking on the database").
- **Two bands, and only two: `WaitTurn` (common) and `WaitPriorityTurn` (priority).** Bands are strict - a band
  is exhausted before the next is looked at - so the priority band holds only callers bounded by construction,
  which cannot starve the band below: the piston (a derived period, at most one turn per loop at a time) and the
  peer Sonar (a fixed cadence). Everything else shares the common band and is separated by age alone, which is
  starvation-free. **Do not add a band** for any caller that can arrive faster than it is served: it starves the
  band below it (a single-permit pool that gave completions precedence collapsed short-task throughput 3x). Nor
  one for EXITING (completing) work: exits arrive as fast as steps finish, and precedence for them serialises
  dispatch behind them - the host call's duration is taken off an exit's claim instead (`Away`).
  `ContextWithPriority` stamps for priority turns without taking one, for the piston's one stamp per cycle.
- **The turnstile is not exposed.** `Pass`, `Gate`, `WaitTurn`, `Away` and `HasClaim` wrap it, so callers depend
  on this package's turns, not on how they are implemented.
- **The turns follow the pool.** `SetShardPool` sizes a shard's turns at `Config.TurnsPerConn` per connection of
  the pool asked for, before the test-mode cap, and every pool change goes through it - so no path can change a
  pool and leave its turns behind. A shard's turnstile is made by its first `SetShardPool`; until then its turns
  let every caller through unordered. The turnstiles are a fresh set per `Open`.
- **`CloseTurns` is the first half of a drain**, apart from `Close`: it releases every caller waiting for a turn
  and makes the `Gate` report a stop, while the connections stay open so work under way finishes.
- **The residue class is one value with an expiry** (`SetResidency`): its writer vouches for its reading only for
  so long, and past `until` `Residency` reports none - take every row - since a stale class leaves rows nobody
  takes, while taking every row only overlaps the peers. **Every reading that restricts nothing is reported as the
  same `(0, 0, false)`** - none, expired, a single dispatcher, an ordinal out of range - so a reader that starts over
  when its class changes sees no change between them. Reported raw, a lone replica's dispatcher count, which moves
  from 0 to 1 once its piston has proven it serves, read as a new class and started the janitor's fast walks over,
  taking roots behind their positions that are a sweep's (`TestShard_ResidencyFailsOpen`).
- **The server is measured as each shard opens** (`RTT`, the least of a few `SELECT 1`s with the first discarded;
  `MaxConnections`, 0 on SQLite or a failed read), before anything takes a turn on it, and held for the
  run: a latency change or a resized server is measured again at the next `Open`. The sizing policy built on them
  - the budget, its clamp, the worker ceiling - is the engine's.
- `NewShard` makes a `Shard` outside a `ShardSet`, for another package's tests.

## Choosing a SQL engine

The engine speaks four SQL dialects via `sequel`: SQLite, MySQL/MariaDB, PostgreSQL, SQL Server. They behave very
differently under concurrent INSERT/UPDATE load. Pick by deployment shape.

**PostgreSQL - recommended for production.** MVCC means concurrent INSERTs do not lock each other on secondary
indexes; no gap locks at default `READ COMMITTED`; the fan-out/fan-in pattern runs deadlock-free at any worker
concurrency. Use Postgres 13+ for `JSONB` and partial indexes. For throughput, give each server a
`max_connections` of at least twice the connection budget of the shards it hosts (the engine clamps a derived
budget to half the limit) and `shared_buffers` to ~25% of host RAM.

*Aurora / managed variants:* Aurora PostgreSQL runs the **stock PG planner/executor** (Aurora replaces only the
storage layer), so it behaves as the matching PG major version - verify with `EXPLAIN`. Aurora **Limitless**
(distributed) is unverified and a *poor fit* anyway - dwarf already shards at the application layer (`ShardSet`),
so a dwarf shard on Limitless is double-sharded, and its distributed planner is unknown territory. **Do not
assume; `EXPLAIN` on the actual target.**

**MySQL / MariaDB - supported, expect tuning.** InnoDB at default `REPEATABLE READ` takes next-key (row + gap) locks
on every secondary-index touch; two concurrent flow creations on a shard can deadlock on overlapping index ranges.
`createWithGraph` retries on `sequel.IsLockContentionError`, hiding most, but a sustained deadlock rate degrades
throughput. To minimize: `transaction-isolation = READ-COMMITTED` (drops gap locks; the largest single reduction);
`innodb_autoinc_lock_mode = 2` with `binlog_format = ROW`; `innodb_lock_wait_timeout` 5-10s; keep
`innodb_deadlock_detect = ON`. Per-shard databases: every shard's DB must exist before startup (the engine migrates
schema but does not `CREATE DATABASE`). MariaDB 10.5+ for `JSON`.

**MariaDB has two driver names, and every MySQL branch must answer to both.** sequel reports `mariadb` for a database
opened with the `mariadb` driver name or a `mariadb://` URL, and `mysql` for a MariaDB opened from a DSN in the MySQL
driver's own format. So every dialect switch that names `"mysql"` names `"mariadb"` beside it, and every MySQL
migration section is `-- DRIVER: mysql mariadb`; a section marked `mysql` alone does not run on a `mariadb` database,
which then has no tables. A switch whose `default` arm is the MySQL form needs nothing. Test the `mariadb` name with a
`mariadb://` testing DSN: a MySQL-format DSN to the same server exercises only `mysql`.

**SQL Server.** Enable `READ_COMMITTED_SNAPSHOT ON` per shard database for Postgres-like non-blocking reads and
near-zero deadlock risk. No other tuning mandatory.

**SQLite - testing and single-instance dev only.** Single-writer means deadlocks are structurally impossible (writes
serialize) but throughput tops out at one transaction at a time. Used automatically by `NewEngineUnderTest` with an empty DSN.
The injected `busy_timeout` keeps workers from immediately failing on `SQLITE_BUSY` during fan-out; do not remove it.
Do not run SQLite in production.

## Sharding guidance & topology

Registering multiple shards with `SetShard(ShardSpec{Index, DSN, ...})` partitions flows across databases (or schemas). Shard count
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
SetShard(ShardSpec{Index: 1, DSN: "postgres://user:pass@db-a.internal:5432/dwarf?sslmode=disable"})
SetShard(ShardSpec{Index: 2, DSN: "postgres://user:pass@db-b.internal:5432/dwarf?sslmode=disable"})

# test/dev (co-located): shards as databases on one server - each named exactly
SetShard(ShardSpec{Index: 1, DSN: "postgres://user:pass@127.0.0.1:5432/dwarf_1?sslmode=disable"})
SetShard(ShardSpec{Index: 2, DSN: "postgres://user:pass@127.0.0.1:5432/dwarf_2?sslmode=disable"})
```

The connection budget is a **per-server** property. With distributed shards each server hosts one shard, so it
sees only that shard's budget, divided among the replicas that dispatch it - **shard count never multiplies a
server's load**. With co-located shards (all shards as databases on one server) the server sees every shard's
budget at once, and each shard clamps its derived budget to half of the SAME `max_connections` without knowing
about the others, so the `× shards` multiplier can overrun the server's limit - which is why co-located is for
test/dev/single-instance only, with `ShardSpec.MaxConnections` declared to split the server between them. Both forms are
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

## Every replica of a shard connects as the role that owns its tables (`checkOwner`)

On PostgreSQL a table belongs to the role that made it, so `openAndMigrate` checks, right after the migrations, that
the connection's role owns `dwarf_flows` and `dwarf_steps` exactly, and fails the shard's open otherwise, naming the
owner. It is a shard-level invariant, not an archive one: a later migration that alters a table needs the owner, and
the archive's day tables (`internal/archiver/CLAUDE.md`), made at runtime by whichever replica gets there, could not be
dropped by peers connecting as another role - nothing would show it until retention failed on them. The archive is its
main reason. It is not retried: nothing but a different role clears it. MySQL and SQL Server are not checked, but
there the one role matters too: their catalogs show a user only the tables it holds a privilege on, which is how the
archive lists its days. Pinned by `TestCheckOwner_ANonOwnerFails` (PostgreSQL).

## Time bounds are bound as text (`SQLTime`)

Every statement that compares a time column with a Go time binds `SQLTime`'s text, never a `time.Time`: rounded UP to
the millisecond the columns hold (`internal/migrations/CLAUDE.md`, "Timestamps come from the database clock"), in
UTC, cast to the dialect's column type, and clamped to the year 9999. The engine's lists and the janitor's scans both
use it; a second copy would split the rounding that keeps an inclusive and an exclusive bound exact. Pinned by
`sqltime_test.go`, on every dialect and in a PostgreSQL session whose zone is not UTC.

## Sharding mechanics

**Shard indices are sparse.** Valid indices are unique integers `>= 1`, not necessarily contiguous (`Index: 1`
+ `Index: 99` is fine - arbitrary DSNs need no naming pattern, and a drained shard's index could one day retire
without renumbering); `0` is a sentinel meaning "no shard / all shards" (used by `Query.Shard`). The index is the
leading number in flow keys (`{shard}-{hour}-{flowID}-{token}`) and what `Query.Shard`/`ShardInfo` report. Internally `shards`
is a `map[int]*Shard` plus a sorted `indices` slice (deterministic `OnEach`/`Indices()` order); `Shard(n)` is a
map lookup whose miss returns a uniform not-found (an unregistered shard in a well-formed key must be
indistinguishable from a bad key). `Open` rejects two shards resolving to the same DSN - a silent collapse onto one
database would share flow_id sequences across "shards" and corrupt routing. The engine-side *encoding* of the shard
into flow keys is in `engine/CLAUDE.md`.

**Cross-shard fan-out is always parallel, never sequential.** `OnEach` builds a per-shard job set and runs them in
parallel. A sequential per-shard loop would grow total latency linearly with the shard count (at 8 shards a 10ms-per-shard
query becomes 80ms wall-clock); the parallel shape stays at single-shard latency regardless of shard count. (The
single-shard case skips the goroutines.)

**The error names its shard.** `OnEach` annotates whatever it returns with a `"shard"` property
(`errors.Trace(err, "shard", idx)`), because a bare driver error tells an operator that a background sweep or the refiller
failed but not WHICH database is unhealthy - and this package's contract is that a persistent outage degrades
*loudly*. It must stay a **named** pair: an unnamed `int` in this `errors` package is interpreted as an HTTP status
code, so passing the index bare would silently stamp the error's status instead. When several shards fail
concurrently, the **lowest failing index** is the one reported - deterministic, unlike "whichever failed first,"
which is a race between the parallel ops. Pinned by `TestOnEach_ErrorNamesTheFailingShard`.

**Not shard-fault-tolerant by design.** `OnEach` fails the whole call on any shard's error. A partial-tolerance
attempt was rejected: real outages mostly manifest as hangs, not errors; classifying "shard down" vs transient/data
errors is driver-specific and brittle; and a helper that *claims* partial tolerance only in a narrow subset of failure
modes lies to operators about resilience. `OnEach` is invoked once per shard with the resolved DB and the shard
index; any non-nil return fails the whole call. Each caller retries on its next natural cycle (`recoverExpiredLeases`
and the archive's catalog refresh on their next tick, the latch detector on its next sweep), so a transient hiccup heals within one cycle
and a persistent outage degrades loudly. (The pistons do not use `OnEach`: each runs against its own shard.)

**DSN format & test-mode resolution.** Each shard carries its own DSN (`Config.Shards`, index -> `ShardConfig`).

**In production the DSN is used EXACTLY as given - never formatted, never rewritten.** Do not run it through
`fmt.Sprintf` to substitute a `%d`: that interprets *every* `%` as a verb, so a percent-encoded credential (a password
`p@ss` written `p%40ss` - entirely routine) is silently corrupted (`%40s` parses as "width 40, verb s", swallowing
the shard index, leaving a real `%d` as `%!d(MISSING)`), and the driver then fails with an opaque parse error naming
neither the shard nor the cause. Shards are declared one at a time with their own DSN, so no template is needed.

**In test mode the DSN IS a template**, and that is where `%d` lives: one base is shared across shards and the index
substitution is what makes each shard's database distinct (`strings.ReplaceAll`, so no other `%` sequence is touched).
`Open` resolves per shard: an explicit DSN wins; else `SEQUEL_TESTING_DSN`; else the SQLite in-memory default
`file:dwarf_%d?mode=memory&cache=shared`. It then wraps the result via `sequel.CreateTestingDatabase` into an
isolated, auto-dropped database keyed on `(driver, baseDSN, TestID)`. Two shards resolving to the same DSN are
rejected at `Open` (the collapse guard). The test-harness rationale (per-test isolation, why the `%d` default, the
multi-replica shared-key case) lives in `fixtures/CLAUDE.md`. Pinned by `internal/database/database_test.go`.

### The test-database key is salted per PROCESS (`testid.go`)

`Config.TestID` is what sequel keys an isolated test database on, and the name it derives carries the hour
and that key and nothing else that varies. Two `go test` runs of the same package in the same hour
therefore land on the same database names, and the second one's `CreateTestingDatabase` drops and recreates
a database the first is still using. Reproduced against Postgres: `failed to create database
testing_01_dwarftest_1_...: duplicate key value violates unique constraint "pg_database_datname_index"`,
and a soak fixture failing on rows deleted underneath it. Neither failure names the real cause, which is
what makes it worth preventing rather than diagnosing.

`TestID` salts the hash with a per-process nonce, so concurrent runs cannot collide. Two properties of that
scope are load-bearing:

- **Per process, not per call.** Several engines in ONE test must resolve to the SAME databases - that is
  how a multi-replica fixture gives its peer engines shared state - and a test wanting separate ones says so
  by passing a distinct name to `NewEngineUnderTest`.
- **It does NOT distinguish `-count=N` iterations, and nothing needs it to.** They run in one process, so
  the nonce is the same for all of them; they are also sequential (a count round's parent blocks on its
  parallel children before the next starts), and sequel reference-counts the handles on a testing database
  and evicts its cached DSN when the last one closes, so each iteration re-mints the database it dropped.
  Verified: `-count=2` over `engine` + `fixtures` against a real Postgres is clean.

Every caller goes through this one function, so the hashing and the salt cannot drift between them.
