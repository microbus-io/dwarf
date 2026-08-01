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
- [ ] **SQL Server only:** `READ_COMMITTED_SNAPSHOT ON`.
- [ ] **Disk IOPS sized to the write rate**, not just to capacity. Cloud providers often provision IOPS by
  disk size, and a throttled disk shows up as throughput collapse with everything apparently idle.
  → [Deployment](deployment.md#disk-throughput)
- [ ] **`ANALYZE`/autovacuum tuned for `dwarf_steps`.** It is a high-churn queue table whose row counts swing
  by orders of magnitude, and stale statistics can flip the selection query from an index scan to a
  sequential one. → [Runbook](runbook.md#backlog-is-growing)

## Engine configuration

- [ ] **`VirtualCPUs` declared on every shard.** Undeclared shards are assumed to be 2-vCPU machines, so a
  large database will be sized for a trickle and run at a fraction of its capacity. Nothing detects an
  over-declared value either. → [Deployment](deployment.md#sharding)
- [ ] **The shard index-to-DSN map is identical on every replica** and stable across restarts. A flow key
  encodes its shard, so a flow created on a shard a peer does not know about is unroutable there.
  → [Resharding](resharding.md)
- [ ] **`SetEngineID` pinned** if your platform restarts replicas in place. Otherwise a crash-looping replica
  leaves a registry corpse per restart, shrinking every survivor's connection pool. Must be unique among
  concurrently-live replicas. → [Upgrading](upgrading.md#deploying-your-own-application)
- [ ] **Logger, meter provider and tracer provider injected.** All three default to no-ops, so an engine with
  nothing injected is silent — including its error logs. → [Observability](observability.md)
- [ ] **At least 3 replicas**, and **1 engine vCPU per 6 database vCPUs** at a 70% database utilisation
  target. Two separate constraints, not one maximum. → [Cloud benchmarks](benchmark-cloud.md#the-sizing-formula)

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
- [ ] **`dwarf_steps_oldest_pending_age_seconds` is dashboarded** — it is the honest backlog signal, better
  than a queue count.
- [ ] **Cluster-wide gauges aggregate with `max`, not `sum`.** `dwarf_steps_pending` and
  `dwarf_steps_oldest_pending_age_seconds` are computed by querying the shared database, so every replica
  reports the same number; summing across three replicas triples your backlog and makes the age reading
  meaningless. → [Observability](observability.md)
- [ ] **Someone knows where the runbook is** before the first alarm, not after.

## Data and retention

- [ ] **A retention policy exists and something enforces it.** The engine **never** auto-purges — every flow
  stays until you remove it, because every flow is potentially resumable, continuable or forkable.
  → [Data handling](data-handling.md#retention-and-deletion)
- [ ] **The retention job loops.** `Purge` marks at most 4,096 roots per call.
- [ ] **Backups cover every shard on the same schedule**, and a restore has been rehearsed against a staging
  fleet — the part worth testing is what your workflows do when they re-run.
  → [Backup and restore](backup-and-restore.md)
- [ ] **`List` and `Search` are gated by credential** in your host. The engine has no notion of a caller, and
  `List` is the one operation that returns flow keys wholesale — which are capabilities.
  → [Data handling](data-handling.md#access-control-is-the-hosts-job)
- [ ] **Quotas enforced before `Create`.** The engine bounds nothing: not initial state size, not baggage,
  not fan-out width, not subgraph depth. → [Data handling](data-handling.md#what-the-engine-does-not-bound)

## Workflows and tasks

- [ ] **Tasks are idempotent**, keyed on `f.StepKey()` where the downstream supports it. Execution is
  at-least-once and two attempts can overlap. → [Writing tasks](tasks.md#idempotency)
- [ ] **Tasks respect their context deadline.** A task that overruns its `TimeBudget` loses its lease to a
  peer and runs twice concurrently.
- [ ] **No PII in error text or cancel reasons.** Error text is returned by both `List` and `History` — the
  readers that otherwise expose no state payloads — and the cancel reason by `List`. Both are
  substring-searchable. → [Writing tasks](tasks.md#keep-payload-data-out-of-error-text)
- [ ] **Large integers read with typed accessors, or carried as strings.** Beyond ±2^53 storage is exact and
  `GetInt` is exact, but an untyped read or a `when` expression rounds. And **binary data base64-encoded** —
  a `NUL` byte is rejected by PostgreSQL and accepted by everything else.
  → [Writing tasks](tasks.md#large-integers-exact-in-storage-rounded-by-untyped-reads)
- [ ] **Every non-default fan-in field has an explicit reducer.** There is no inference from a field's name —
  an unwired `sumTotal` silently takes `replace`, not `add`. → [Building graphs](graphs.md)
- [ ] **Flow lifetimes are bounded in author space** if you need them bounded. The engine imposes no flow
  deadline, and `interrupted` flows wait indefinitely. → [Writing tasks](tasks.md#timestamps)

## First week

- [ ] Watch `dwarf_peer_replicas` match your actual replica count.
- [ ] Watch `dwarf_steps_recovered_total` — a steady trickle usually means tasks are overrunning their
  budget, or that your termination grace period is cutting drains short. Neither is broken, but both are
  worth knowing. → [Runbook](runbook.md#tasks-are-running-more-than-once)
- [ ] Confirm the retention job actually removes rows, by watching table size rather than disk usage —
  PostgreSQL holds its high-water mark. → [Runbook](runbook.md#disk-did-not-shrink-after-a-purge)
- [ ] Re-read the sizing rules against what you actually observe. The published numbers assume cheap
  in-process tasks; a host dispatching over a network transport pays more engine CPU per step.
