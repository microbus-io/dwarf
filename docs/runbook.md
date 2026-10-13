# Runbook

> **For operators** responding to a live incident. Each entry is a symptom you can observe from a dashboard
> or an alert, followed by how to confirm it and what to do. For the orientation — what the engine repairs on
> its own, and on what schedule — see [Operating dwarf](operations.md). Timings quoted here are the engine's
> defaults.

**Before anything else: check whether it is already being handled.** Most faults are absorbed. A database
outage shorter than a step's lease produces latency and no errors at all; a dead replica's work is
re-dispatched within the flow's time budget plus about a minute and a half, including a step it had finished
but not yet advanced. If the symptom is "things are slow" rather than "a counter
fired," start at [Backlog is growing](#backlog-is-growing) and resist restarting
replicas — a restart discards in-flight work that was about to complete and adds a phantom replica to the
registry.

> **On metric names:** headings below use the instrument name as the engine emits it. A Prometheus exporter
> appends `_total` to counters at the scrape boundary, so query `dwarf_flows_orphaned_total` for the
> instrument named `dwarf_flows_orphaned`. See [Observability](observability.md).

## Symptom index

| Symptom | Impact |
|---|---|
| [`dwarf_flows_orphaned` is non-zero](#dwarf_flows_orphaned-is-non-zero) | Flows are stuck — needs manual recovery |
| [`dwarf_steps_unwedged` is non-zero](#dwarf_steps_unwedged-is-non-zero) | Repaired automatically; a latent bug remains |
| [`dwarf_steps_write_failed` is non-zero](#dwarf_steps_write_failed-is-non-zero) | Flows failed for a storage reason |
| [Backlog is growing](#backlog-is-growing) | Throughput |
| [Throughput collapsed after a restart or crash](#throughput-collapsed-after-a-restart-or-crash) | Throughput |
| [One shard is slow or unreachable](#one-shard-is-slow-or-unreachable) | Partial outage |
| [The database was down](#the-database-was-down) | Usually no action |
| [Tasks are running more than once](#tasks-are-running-more-than-once) | Correctness of side effects |
| [Deadlock errors on MySQL or SQL Server](#deadlock-errors-on-mysql-or-sql-server) | Throughput |
| [Disk did not shrink after a purge](#disk-did-not-shrink-after-a-purge) | Usually no action |
| [A flow is stuck `interrupted`](#a-flow-is-stuck-interrupted) | Usually not a fault |

---

## `dwarf_flows_orphaned` is non-zero

**What it means.** A flow is `running`, every one of its steps has reached a terminal status, nothing has
touched it for five minutes, and none of its steps is one the engine knows it can safely resume. It has no
successor step and nothing will create one. The engine repairs the common version of this shape — a replica
killed between completing a step and creating its successor — by resuming that step from its recorded output,
without running its task again, and counts it in `dwarf_steps_recovered`. A flow counted here is one it could
not repair: re-driving it would mean re-deciding which transition should have been taken, and a false positive
would double-advance it. The engine detects and counts; a human recovers.

**Cause.** Not crash residue, which is repaired automatically. Expect zero; a non-zero count points at a latent
bug, or at rows edited by hand.

**Confirm.** The counter carries a `workflow` label, which narrows the search. Find the candidates:

```go
flows, _, err := eng.List(ctx, workflow.Query{
    Status:    workflow.StatusRunning,
    OlderThan: 10 * time.Minute,
})
```

For each, `eng.History(ctx, flowKey)` — an orphan shows every step terminal with no `pending` or `running`
step anywhere, including inside subgraphs.

**Recover.** A terminal flow is immutable and a running flow cannot be forked, so recovery is two steps:

```go
// 1. Terminalize the orphan. It stops being running and becomes a valid fork source.
eng.Terminate(ctx, flowKey, "orphaned by replica termination")

// 2. Re-run from the last step that completed successfully. Its key comes from History.
newFlowKey, err := eng.Fork(ctx, lastGoodStepKey, nil)
```

The fork is a new flow that re-runs from that step and inherits the original's priority, fairness and
baggage. The original is preserved for audit. Pass state overrides as the third argument if the flow needs a
correction to get past whatever it stalled on.

**Escalate** every occurrence. The `workflow` label plus the affected flows' histories are what an engine
maintainer needs.

---

## `dwarf_steps_unwedged` is non-zero

**What it means.** A step was parked waiting on a subgraph child, the release never arrived, and a sweep
released it. **The flow itself is fine** — the sweep re-drives the normal release path, so no work was lost
and no manual recovery is needed. What the counter tells you is that a step reached a state it should not
have been able to reach.

**Respond.** Nothing urgent. The `park_type` label distinguishes the two cases: `orphaned_child` is a live
subgraph child whose parent already ended, and anything else is a caller whose child vanished. Neither is
expected in normal operation, including under heavy use of `Terminate`, so either is worth reporting.

**Escalate** with the `park_type` label and the timing if the rate is sustained rather than occasional.

---

## `dwarf_steps_write_failed` is non-zero

**What it means.** A task ran, and its outcome could not be written **while the database was reachable**. The
engine proved reachability by successfully writing a plain failure record immediately afterward — so the
database is not the problem, the payload is. The affected steps are `failed` with the driver's error text,
and their flows failed with them. The task ran exactly once.

**Cause.** Almost always a value the database will not store. Two known cases:

- **A `NUL` byte (`U+0000`) in a string.** PostgreSQL rejects it in JSON outright; other dialects accept it.
  Base64-encode binary data instead of putting raw bytes in state.
- **A payload exceeding a column or packet limit.** Check your driver's max packet size against the size of
  the state being written; `dwarf_state_write_bytes_total` shows what workflows are writing.

**Respond.** Read the failed flows' error text — it names the driver's actual complaint. Fix the task that
produces the value, then `Fork` the failed flows from the offending step with a state override that repairs
it. This is a workflow bug, not an infrastructure one; restarting replicas will not help.

---

## Backlog is growing

**Confirm it is real.** `dwarf_steps_dispatch_lag_seconds` is the honest signal — aggregate it with `max`
across replicas, never `sum`. A rising *count* with a flat lag is just more work arriving; a rising lag means
dispatch is genuinely falling behind. The lag covers only the priority band being served: a worse band aging
in `dwarf_steps_oldest_pending_age_seconds` while the lag stays flat is strict priority serving the better
band first, as designed.

**See where it is.** Break the lag down by shard,
`max by (shard) (dwarf_steps_dispatch_lag_seconds)`, and the shape points at the cause:

- **Every shard rising together:** capacity. The database or the engine is saturated — 1 or 2 below.
- **One shard rising:** that shard's database (its `dwarf_turnstile_waiting` deep, its CPU high — see
  [One shard is slow or unreachable](#one-shard-is-slow-or-unreachable)), or the replicas dispatching it
  (`dwarf_shard_dispatching` names them; see 4 below and the slow-dispatchers note under the same heading).
- **A shard with no lag series at all:** nobody dispatches it, so nothing on it runs.
  `sum by (shard) (dwarf_shard_dispatching)` reads zero for it. Check that replicas with workers are running
  and can reach that database (`dwarf_peer_blind_seconds`).
- **The lag flat, a worse band aging** in `max by (priority) (dwarf_steps_oldest_pending_age_seconds)`:
  higher-priority work is using the capacity, as designed. If that band has a deadline, the fix is capacity or
  fewer flows at the better bands, not the engine.

**Then find which of four things is binding.**

**1. The database is saturated.** `dwarf_turnstile_available` sits at zero with `dwarf_turnstile_waiting`
deep, and database CPU is high. This is the expected shape at capacity — the engine queues rather than
overwhelming the server. Add database capacity, or add a shard ([Resharding](resharding.md)). Adding engine
replicas will **not** help: they share the same per-database connection budget.

**2. The engine is saturated.** Engine CPU is high and the database is not. Plan for roughly 2,600 steps/s
per engine core. Add replicas.

**3. Neither is busy — the query planner has gone stale.** This is the one that looks like nothing is wrong:
engine CPU low, database backends idle, backlog climbing anyway. Check
`dwarf_refill_query_duration_seconds` split by the `phase` label. If `band_keys` shows a tail in the tens or
hundreds of milliseconds on a backlog that has not changed, the planner has stopped using the selection index.

On PostgreSQL, measured on one rig: the same query on the same data, 0.3ms with fresh statistics and 100ms
with stale ones. A queue table's row counts swing by orders of magnitude faster than autoanalyze samples, so
this is a structural hazard rather than a misconfiguration. The fix:

```sql
ANALYZE dwarf_steps;
```

To prevent recurrence, make autovacuum more aggressive for that table specifically:

```sql
ALTER TABLE dwarf_steps SET (autovacuum_analyze_scale_factor = 0.01,
                             autovacuum_vacuum_scale_factor  = 0.02);
```

**4. Work is stranded on a peer that is alive but not dispatching.** `dwarf_steps_stolen_total` is non-zero.
See [One shard is slow or unreachable](#one-shard-is-slow-or-unreachable) — the same diagnosis applies to a
replica that has stopped serving its share.

---

## Throughput collapsed after a restart or crash

**Suspect phantom replicas first.** Each replica registers a row per shard and heartbeats it, and the live
rows decide which replicas dispatch each shard and how its connection budget divides — so rows belonging to
replicas that no longer exist can hold a dispatcher's place, leaving that shard with fewer replicas running
its steps, and can shrink the surviving dispatchers' pools. A replica that crash-loops is the worst case:
unless its identity is pinned, every restart mints a fresh one and leaves another stale row.

**Confirm.** Compare `dwarf_peer_replicas` against your actual replica count. If the gauge is higher, that is
the fault. Directly:

```sql
SELECT engine_id, seen_at FROM dwarf_peers ORDER BY seen_at;
```

**Respond.** A stale row stops counting toward the replica count after **40 seconds** — so the damage
resolves itself that fast. Deletion of the row is slower and needs two conditions: the row must be older
than **80 seconds**, *and* the registry must have been continuously readable for **5 minutes**. **Wait
first** — the cleanup is deliberately patient because deleting a row is the only irreversible act in the
registry. If rows persist well beyond that, delete the
ones whose `seen_at` is clearly stale, by explicit id, never by a timestamp range:

```sql
DELETE FROM dwarf_peers WHERE engine_id IN (...);   -- list the dead ids explicitly
```

**Prevent recurrence** for a crash-looping deployment by pinning each replica's identity to something stable
across its restarts — a pod ordinal or hostname — so a restart re-uses its one row instead of adding another.
Pass it via `SetEngineID` before startup. The contract is that the id must be unique among concurrently-live
replicas sharing the databases: two live replicas sharing an id count as one, which over-sizes pools, so a
wrong stable id is worse than the random default.

---

## One shard is slow or unreachable

**Confirm.** `eng.ShardInfo(ctx)` is the right first call and **keeps working when a shard is down** — it
returns a row per shard with that shard's error recorded on it, rather than failing as a whole, so it tells
you *which* shard is unhappy. Then `dwarf_refill_query_duration_seconds` split by `shard` — one shard's tail
dragging while its peers stay flat is the shard, not the engine. `dwarf_peer_blind_seconds` non-zero for a
shard means replicas cannot read its registry at all.

**Understand the blast radius.** Dwarf is **not** shard-fault-tolerant: `List` and
`Purge` fail as a whole if any shard errors. Flows resident on a healthy shard keep executing
normally; flows on the failed shard are unreachable until it returns. Each shard's selection runs on its own
cycle, so a slow shard slows its own dispatch, not its peers'.

**Respond.** Recover the database. There is no drain-and-migrate path that is faster than fixing the server,
and flows cannot be moved between shards. If the shard is healthy but should stop taking *new* work — you are
retiring it, or it is overloaded — cordon it instead: `ShardSpec.Cordoned` excludes a shard from new-flow
placement while everything already resident keeps running. That is a configuration change requiring a
restart; see [Resharding](resharding.md).

**If a peer is alive but not serving its share** (`dwarf_steps_stolen_total` climbing), other replicas have
already started covering for it — the steal exists exactly for this. Find the replica whose dispatch has
stalled and restart it; the fleet is carrying the load meanwhile, at the cost of some wasted claim attempts.

**If both replicas dispatching a shard are slow, nothing covers for them.** At most two replicas dispatch
each shard, and the others do not run its steps, so with both of them slow the shard runs at their pace and
`dwarf_steps_stolen_total` stays near zero, because no healthy replica is looking at it. The sign is
`dwarf_steps_dispatch_lag_seconds` climbing for that shard while the database server itself shows headroom
(its own CPU and active sessions): the database could take more, and the work is not being picked up.
`dwarf_shard_dispatching` names the replicas dispatching each shard; check those hosts for CPU saturation,
memory pressure or a slow network path to the database. Fixing or replacing the host is the reliable remedy.
A graceful restart (one that runs `Shutdown`) hands the replica's place to another working replica only when
there is one beyond the two; with exactly two working replicas the restarted one takes its place back.

---

## The database was down

**Usually there is nothing to do**, and the counters tell you which of two things happened.

**A freeze** (the server stopped responding without dropping connections — a failover pause, a container
suspend). In-flight writes simply waited it out. Expect `dwarf_steps_recovered_total` and
`dwarf_steps_write_retried_total` both at zero if the outage was shorter than a step's lease
(`TimeBudget` + 30s). The only casualties are callers whose own request deadline expired; their flows
completed anyway.

**A severed connection** (a network partition, a hard restart — connections error instantly rather than
hanging). Expect `dwarf_steps_write_retried_total` to rise as outcome writes land after reconnect — **with no
task re-execution** — and `dwarf_steps_recovered_total` to rise a few minutes later for the leases that
lapsed past the retry budget. Those steps *do* re-run. Both are the designed recovery path.

**What must stay at zero through both:** `dwarf_steps_write_failed_total`. The classifier is specifically
built to avoid mis-terminalizing a flow because the database was away, so a non-zero reading here during an
outage is a genuine finding — see that entry above.

**Do not restart replicas during a database outage.** They will recover on their own; a restart abandons
in-flight work and re-runs those tasks.

---

## Tasks are running more than once

**This is the contract, not a bug** — dwarf guarantees at-least-once execution. What the engine guarantees is
that persisted *state* reflects exactly one execution; side effects are the task's responsibility. If
duplicate side effects are causing harm, the task needs an idempotency key.

**But a steady `dwarf_steps_recovered_total` is worth chasing**, because it means leases are lapsing rather
than crashing. Two causes:

- **Tasks overrun their time budget.** A step's lease is its `TimeBudget` plus 30 seconds. A task that ignores
  its context deadline and runs longer loses its lease to a peer. Raise `FlowOptions.TimeBudget` for those
  flows, or make the task respect its deadline.
- **The drain window is too short.** If your platform kills the process before a graceful shutdown finishes,
  steps in flight are abandoned and re-run elsewhere. The drain waits for the longest task still running, so
  the termination grace period must exceed the largest `TimeBudget` any of your flows declares.

---

## Deadlock errors on MySQL or SQL Server

**Expected at low rates; the engine retries them.** A sustained rate degrades throughput and is a
configuration problem.

**MySQL / MariaDB.** InnoDB's default `REPEATABLE READ` takes gap locks on every secondary-index touch, so
concurrent flow creation on one shard can deadlock. Set:

```
transaction-isolation     = READ-COMMITTED    # drops gap locks — the biggest single reduction
innodb_autoinc_lock_mode  = 2                 # with binlog_format = ROW
innodb_lock_wait_timeout  = 5                 # 5–10s
innodb_deadlock_detect    = ON
```

**SQL Server.** Enable read-committed snapshot isolation:

```sql
ALTER DATABASE [yourdb] SET READ_COMMITTED_SNAPSHOT ON;
```

Both changes remove the lock cycle outright rather than reducing it. PostgreSQL needs neither and is the
recommended production dialect; see [Deployment](deployment.md).

---

## Disk did not shrink after a purge

**Expected on PostgreSQL.** `Delete` and `Purge` mark flows; a reaper removes the rows within about a minute.
But PostgreSQL holds its high-water mark — freed space is reused for new rows rather than returned to the
filesystem. Measured on one rig: 198 MB still allocated for 37 remaining rows.

**Respond.** Nothing, normally — the space is genuinely reusable. What this *does* mean is that a "disk usage
= live data" dashboard will mislead you; track row counts instead. If you must reclaim the filesystem space,
`VACUUM FULL` takes an exclusive lock on the table and should be treated as a maintenance window.

**One thing to check:** confirm the reaper is actually running. Marked-but-unreaped flows disappear from
`List` and `History` immediately, so a growing gap between "rows in `dwarf_flows`" and "flows visible to
`List`" means marking is working and reaping is not.

---

## A flow is stuck `interrupted`

**This is almost always correct behaviour.** `interrupted` is a parked state, not a fault — a task called for
external input and the flow is waiting for someone to provide it. It is not terminal, consumes no worker, and
will wait indefinitely.

**Confirm what it is waiting for** with `eng.Snapshot(ctx, flowKey)` — `InterruptPayload` carries whatever the
task published when it parked.

**Respond** by resuming it with the data it asked for, or by terminating it:

```go
eng.Resume(ctx, flowKey, map[string]any{"approved": true})
eng.Terminate(ctx, flowKey, "abandoned")
```

Both must be addressed by the **root** flow key. A subgraph child's key is read-only for lifecycle changes
and is rejected — address the root and the engine threads the resume down to the right leaf.

**If a flow has been interrupted longer than your business process allows**, that is a policy question rather
than an engine fault: the engine imposes no flow lifetime. Find them with `List` filtered on
`Status: interrupted` and `OlderThan`, and terminate or escalate per your own rules.

---

## Getting help

For anything you escalate, capture: the alarm counter and its labels, `eng.History(ctx, flowKey)` for one
affected flow, the dialect and version, replica count, and whether a deploy or a termination preceded it.
`workflow.NewFlowRenderer(steps).Render()` turns that history into a Mermaid diagram of the execution DAG, which
is usually the fastest way to show what a stuck flow actually did.
