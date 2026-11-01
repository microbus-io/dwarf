# Production checklist

> **For developers and operators** about to run dwarf for real. Every item links to the guide that explains
> it. Most defaults are safe; what follows is the set that is **not** defaulted, or where the default is
> deliberately conservative and wrong for production.

## Database

- [ ] **PostgreSQL**, unless you have a reason. It needs no isolation-level tuning and runs fan-out
  deadlock-free at any concurrency. → [Deployment](deployment.md#choosing-a-database)
- [ ] **Not SQLite.** It is single-writer and used automatically by the test harness; it is not a production
  database.
- [ ] **MySQL/MariaDB only:** `transaction-isolation = READ-COMMITTED`, `innodb_autoinc_lock_mode = 2` with
  `binlog_format = ROW`. Without the first, concurrent flow creation deadlocks on gap locks.
  → [Runbook](runbook.md#deadlock-errors-on-mysql-or-sql-server)
- [ ] **MySQL/MariaDB only:** `interpolateParams=true` in the DSN. Without it every statement with arguments
  costs two round trips, roughly doubling each step's. → [Deployment](deployment.md#mysql--mariadb--supported-expect-tuning)
- [ ] **SQL Server only:** `READ_COMMITTED_SNAPSHOT ON`.
- [ ] **Every replica connects to a shard as one database user**, allowed to create and drop tables in the
  engine's schema. The engine keeps the archive in a table per day, creating and dropping them as it runs.
  → [Deployment](deployment.md#sharding)
- [ ] **Disk IOPS sized to the write rate**, not just to capacity. Cloud providers often provision IOPS by
  disk size, and a throttled disk shows up as throughput collapse with everything apparently idle.
  → [Deployment](deployment.md#disk-throughput)
- [ ] **PostgreSQL 16 and older: `autovacuum_work_mem` set to 1 GB.** At a 64 MB default, vacuum on the step table
  rescans every index each time its memory fills, and falls behind. → [Deployment](deployment.md#postgresql--recommended-for-production)
- [ ] **`ANALYZE`/autovacuum tuned for `dwarf_steps`.** It is a high-churn queue table whose row counts swing
  by orders of magnitude, and stale statistics can flip the selection query from an index scan to a
  sequential one. → [Runbook](runbook.md#backlog-is-growing)
- [ ] **No statement, transaction or idle-in-transaction timeout** on the engine's database, its role or its
  connection string. That means PostgreSQL's `statement_timeout`, `transaction_timeout` and
  `idle_in_transaction_session_timeout`, and MySQL's `max_execution_time`. General database guidance often
  recommends them; leave them unset here.
  - Some of the engine's background queries grow with the backlog, so no value is safe for every backlog. A
    timeout that cuts them off can stop a database's work from being dispatched at all.
  - An idle-in-transaction timeout ends transactions while the engine is busiest, and the work is retried.
- [ ] **Long-running queries stay off the shard databases:** analytics, reporting, ad hoc sessions. While a query
  runs for minutes, PostgreSQL cannot clean up the engine's queue table behind it, and dispatch slows until it
  ends. A read replica with `hot_standby_feedback` on holds cleanup back the same way, and so does a lagging
  replication slot. → [Runbook](runbook.md#backlog-is-growing)

## Engine configuration

- [ ] **Connection budgets suit each database.** The default (250, rising to 500 for a distant database,
  never more than half its `max_connections`) suits databases of 4-32 vCPU. Set `MaxConnections` for a larger
  one or one shared with other heavy users, and set `Weight` on shards of unequal size so new flows follow
  capacity. → [Deployment](deployment.md#connection-pool)
- [ ] **The shard index-to-DSN map is identical on every replica** and stable across restarts. A flow key
  encodes its shard, so a flow created on a shard a peer does not know about is unroutable there.
  → [Resharding](resharding.md)
- [ ] **`SetEngineID` pinned** if your platform restarts replicas in place. Otherwise a crash-looping replica
  leaves a stale registry row per restart, and one that ranked as a shard's dispatcher holds that slot until it
  ages out, leaving the shard fewer replicas running it. Must be unique among
  concurrently-live replicas. → [Upgrading](upgrading.md#deploying-your-own-application)
- [ ] **`GOMEMLIMIT` set on engine hosts**, a little below the memory the process may use. The engine keeps a
  tenth of it for large state fields, and without it Go's garbage collector can let the heap reach about twice
  its live size. → [Deployment](deployment.md)
- [ ] **Logger, meter provider and tracer provider injected.** All three default to no-ops, so an engine with
  nothing injected is silent — including its error logs. → [Observability](observability.md)
- [ ] **Two engine replicas per shard, each at half the database's vCPU** (two 8-vCPU replicas per 32-vCPU
  database), so either can carry the shard alone. → [Deployment](deployment.md#sizing-a-deployment)

## Lifecycle

- [ ] **`Shutdown` is called** on termination, and the platform's **termination grace period exceeds your
  largest `TimeBudget`**. Graceful shutdown waits for in-flight tasks; if the platform kills the process
  first, those steps are abandoned and **their tasks re-run elsewhere**.
  → [Deployment](deployment.md#shutting-down)
- [ ] **Deploys replace replicas one at a time**, letting the fleet settle between steps, so pools re-divide
  cleanly rather than transiently over-connecting. → [Upgrading](upgrading.md)
- [ ] **Dwarf version bumps are planned as a maintenance window**, not a rolling deploy — v0.x makes no
  compatibility promise. → [Upgrading](upgrading.md#dwarf-is-v0x-and-that-is-the-headline)

## Observability

- [ ] **These three page someone.** Each is documented as zero-forever in a healthy engine:
  `dwarf_steps_unwedged_total`, `dwarf_flows_orphaned_total`, `dwarf_steps_write_failed_total`.
  → [Runbook](runbook.md)
- [ ] **`dwarf_steps_dispatch_lag_seconds` is dashboarded** — it is the honest backlog signal, better
  than a queue count.
- [ ] **Per-shard gauges aggregate with `max` per shard, not `sum`.** `dwarf_steps_dispatch_lag_seconds`,
  `dwarf_steps_pending` and `dwarf_steps_oldest_pending_age_seconds` describe a shard, and more than one
  replica can report the same one; summing across replicas overstates the backlog and makes the age readings
  meaningless. → [Observability](observability.md)
- [ ] **Someone knows where the runbook is** before the first alarm, not after.

## Data and retention

- [ ] **The retention is set to what you must keep.** Ended flows are archived and kept for `SetRetention` days
  (30 by default), then dropped a day at a time; flows that have not ended are never removed by the engine.
  → [Data handling](data-handling.md#retention-and-deletion)
- [ ] **The archive's disk is sized for the retention**: a day's ended flows times the retention, per shard.
- [ ] **Backups cover every shard on the same schedule**, and a restore has been rehearsed against a staging
  fleet — the part worth testing is what your workflows do when they re-run.
  → [Backup and restore](backup-and-restore.md)
- [ ] **`List` (and its `Query.Search`) is gated by credential** in your host. The engine has no notion of a caller, and
  `List` is the one operation that returns flow keys wholesale — which are capabilities.
  → [Data handling](data-handling.md#access-control-is-the-hosts-job)
- [ ] **Quotas enforced before `Create`.** Beyond refusing any single value over 256 MB, the engine bounds
  nothing: not initial state size, not baggage, not fan-out width, not subgraph depth.
  → [Data handling](data-handling.md#what-the-engine-does-not-bound)
- [ ] **A flow's final state fits in 256 MB.** State can grow past it while the flow runs, but the final
  state holds every field at once. → [Data handling](data-handling.md#the-size-limit-256-mb-per-value)
- [ ] **MySQL/MariaDB: `max_allowed_packet` is at least 512M.**
  → [Deployment](deployment.md#mysql--mariadb--supported-expect-tuning)

## Workflows and tasks

- [ ] **Tasks are idempotent**, keyed on `f.StepKey()` where the downstream supports it. Execution is
  at-least-once and two attempts can overlap. → [Writing tasks](tasks.md#idempotency)
- [ ] **Tasks respect their context deadline.** A task that overruns its `TimeBudget` loses its lease to a
  peer and runs twice concurrently.
- [ ] **No PII in error text or terminate/cancel reasons.** Error text is returned by both `List` and
  `History` — the readers that otherwise expose no state payloads — and the terminate or cancel reason by
  `List`. Both are substring-searchable. → [Writing tasks](tasks.md#keep-payload-data-out-of-error-text)
- [ ] **Large integers read with typed accessors, or carried as strings.** Beyond ±2^53 storage is exact and
  `GetInt` and `when` expressions are exact, but an untyped read rounds. And **binary data base64-encoded** —
  a `NUL` byte is rejected by PostgreSQL and accepted by everything else.
  → [Writing tasks](tasks.md#large-integers-exact-in-storage-rounded-by-untyped-reads)
- [ ] **Every field whose writes should combine has an explicit reducer.** There is no inference from a field's name —
  an unwired `sumTotal` silently takes `replace`, not `add`. → [Building graphs](graphs.md)
- [ ] **Flow lifetimes are bounded in author space** if you need them bounded. The engine imposes no flow
  deadline, and `interrupted` flows wait indefinitely. → [Writing tasks](tasks.md#timestamps)

## First week

- [ ] Watch `dwarf_peer_replicas` match your actual replica count.
- [ ] Watch `dwarf_steps_recovered_total` — a steady trickle usually means tasks are overrunning their
  budget, or that your termination grace period is cutting drains short. Neither is broken, but both are
  worth knowing. → [Runbook](runbook.md#tasks-are-running-more-than-once)
- [ ] Confirm the live tables stay level under load, by watching table size rather than disk usage —
  PostgreSQL holds its high-water mark. → [Runbook](runbook.md#disk-did-not-shrink-after-a-purge)
- [ ] Re-read the sizing rules against what you actually observe. The published numbers assume cheap
  in-process tasks; a host dispatching over a network transport pays more engine CPU per step.
