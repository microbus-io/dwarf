# Operating dwarf

> **For operators** running dwarf in production. This is the orientation page: what the engine repairs on its
> own and on what schedule, what genuinely needs a human, and where the rest of the operational documentation
> lives. If you are writing workflow code rather than running it, start at [Driving flows](flows.md).

Dwarf is a library, not a service. It runs inside your application's process, and everything durable about it
lives in the SQL databases you point it at — there is no broker, no coordinator, and no state on disk beside
the database. That shapes what operating it means: **you operate a fleet of application replicas and their
databases**, and the engine's own moving parts are background loops inside those replicas.

## What the engine repairs without you

Most of what looks alarming in a distributed system is already handled here, on a fixed schedule. Knowing the
schedule is what tells you whether to intervene or wait — and the answer is usually wait.

| Situation | What happens | How long |
|---|---|---|
| A replica dies holding in-flight steps | Each step's lease lapses and the step returns to `pending` for another replica | Up to the flow's `TimeBudget` + 30s, then within the next 1-minute recovery sweep |
| A replica dies after completing a step but before creating its successor | A sweep hands the step back and dispatch creates the successor from its recorded output — the task does **not** re-run | Up to the flow's `TimeBudget` + 30s, then within the next 1-minute recovery sweep |
| A database blip mid-write | The step's outcome write retries in place, holding its lease — the task does **not** re-run | 1s doubling to 32s, about a minute in all; longer outages fall through to lease recovery |
| A replica joins or leaves | Every peer re-reads the shared registry and re-divides each database's connection budget | Registry read every 250ms; a dead replica stops counting after 40s |
| Work is created on a replica that does not dispatch its shard | A replica that does dispatch it finds the work on its next selection cycle | Under a second at the derived cycle rate |
| A flow marked for deletion | A background reaper removes the flow and its whole subgraph tree | Within ~1 minute |
| A subgraph park that never released | A sweep re-drives the release, or terminates an orphaned child | Detected after 5 minutes, swept every 5 minutes |

Two consequences worth internalising:

- **Execution is at-least-once.** A recovered step re-runs its task. That is the contract, not a failure —
  tasks must be idempotent. What the engine guarantees is that the flow's *persisted state* reflects exactly
  one execution, even when two workers overlap.
- **A slow database is absorbed, not escalated.** An outage shorter than a step's lease is invisible: workers
  wait it out and continue. You will see latency, not errors.

## What actually needs you

Four things, and everything else in this section of the docs is one of them.

1. **An alarm fired.** Three counters are documented as "zero forever in a healthy engine," and a non-zero
   reading means either a latent bug or a flow that needs manual recovery. → **[Runbook](runbook.md)**
2. **You are deploying.** Redeploying your own application is an ordinary rolling restart; moving to a new
   version of **dwarf** is not — while dwarf is v0.x it makes no compatibility promise, and schema migrations
   run at startup. → **[Upgrading](upgrading.md)**
3. **You are changing the shard set.** Shard membership is baked into every flow key, so this is a planned
   maintenance operation, not a live resize. → **[Resharding](resharding.md)**
4. **You need to answer for the data.** What the engine stores, what free-text search reaches, and how
   retention is driven. → **[Data handling](data-handling.md)**

## What to watch

The full instrument catalogue is in [Observability](observability.md). For a dashboard, the split that
matters is between counters that should **never** move and counters that are merely informative. Counters
are named here as you query them in Prometheus, with the `_total` suffix its exporter appends; the
instrument itself carries no suffix (`dwarf_steps_unwedged`).

**Alarms — page on any non-zero rate:**

| Instrument | Means |
|---|---|
| `dwarf_steps_unwedged_total` | A step wedged and a sweep papered over it. The effect is repaired; the cause is not. |
| `dwarf_flows_orphaned_total` | A running flow is stranded with every step terminal, and the engine could not repair it. These need manual recovery. |
| `dwarf_steps_write_failed_total` | A step's outcome could not be stored while the database was reachable, so the payload is at fault. |

**Health — alert on sustained levels, not on any movement:**

| Instrument | Read it as |
|---|---|
| `dwarf_steps_dispatch_lag_seconds` | The honest backlog signal: how long the most overdue step at the priority band being served has been due, per shard. Rising means dispatch is not keeping up. Aggregate with `max`, never `sum`. |
| `dwarf_steps_oldest_pending_age_seconds` | The same question per priority band, every 30 seconds. A worse band rising while the lag stays flat is strict priority doing its job, not a fault. |
| `dwarf_steps_recovered_total` | Steps recovered because their worker was lost to a crash or an overrun. A steady trickle means tasks are overrunning their budget. |
| `dwarf_peer_replicas` | The fleet size each replica believes in. Should equal your actual replica count. |
| `dwarf_turnstile_available` / `_waiting` | Read as a pair. Sustained zero available with a deep wait queue means database connections are the binding constraint. |
| `dwarf_steps_stolen_total` | Non-zero names a peer that is alive but not dispatching its share. |

**One dashboard trap worth stating up front:** the backlog gauges describe a shard, and more than one replica
can report the same shard, so summing them across replicas overstates the backlog and makes an age reading
meaningless. Take the `max` per shard first. The per-replica/per-shard split is tabulated in
[Observability](observability.md).

### Alerting on lag

Three rules cover dispatch falling behind. The thresholds are starting points; set them from what your
workflows can tolerate.

```promql
# Dispatch is falling behind on a shard. A shard that keeps up reads under a second.
max by (shard) (dwarf_steps_dispatch_lag_seconds) > 30            # for: 5m

# A shard has nobody dispatching it, so nothing on it runs and it reports no lag at all.
sum by (shard) (dwarf_shard_dispatching) == 0                       # for: 2m

# A priority band that must not starve has waited too long. Add one per band you care about.
max by (priority) (dwarf_steps_oldest_pending_age_seconds{priority="100"}) > 600   # for: 15m
```

- **The lag counts from when a step became runnable**, not from when it was created. A step that sleeps, backs
  off, waits on a subgraph or waits for a `Resume` does not raise it, so a long-running workflow does not need
  a looser threshold.
- **It covers only the band being served**, and reads zero on a shard whose best band is waiting behind a
  better one elsewhere. Strict priority holding back worse bands is by design. Alert on those bands through
  `dwarf_steps_oldest_pending_age_seconds`, and only for bands that carry a deadline.
- **Do not alert on the per-band series being absent.** `dwarf_steps_pending` and
  `dwarf_steps_oldest_pending_age_seconds` are refreshed every 30 seconds by one replica per shard. They are
  missing for up to 30 seconds after startup or while that duty passes between replicas, and they go absent if
  the refresh keeps failing for 90 seconds. The lag and `dwarf_shard_dispatching` are the liveness signals.
- **For trend, not paging:**
  `sum by (priority) (increase(dwarf_flows_started_total[15m])) - sum by (priority) (increase(dwarf_flows_ended_total[15m]))`
  staying positive means flows are arriving faster than they finish, per band, before any lag shows.

When a rule fires, [Backlog is growing](runbook.md#backlog-is-growing) walks through finding the cause.

## Sizing and capacity

Sizing is measured rather than guessed, and the numbers live with the measurements:

- **[Deployment](deployment.md)** — choosing a database, declaring shards, connection pools, workers,
  drain windows, the disk-throughput requirement, and
  [reference configurations](deployment.md#reference-configurations) with their measured throughput.
- **[Cloud benchmarks](benchmark-cloud.md)** — how to size a deployment, and the measurements behind it,
  against managed PostgreSQL across a real network hop.

The rules to carry into a capacity plan: plan a 32-vCPU database at **~19,000 steps/s** of average load, check
what your peaks cost in waiting ([planning capacity](benchmark-cloud.md#planning-capacity)), and give each shard
**two engine replicas at half its vCPU**, either of which can carry the shard alone. Both assume your tasks
are cheap; a host dispatching tasks over a network transport pays more engine CPU per step and should
measure its own.

## Where to look next

| Document | For |
|---|---|
| [Production checklist](production-checklist.md) | Before you go live — the pre-flight list |
| [Runbook](runbook.md) | An alarm is firing right now |
| [Upgrading](upgrading.md) | Redeploying your app, upgrading dwarf itself, schema migrations |
| [Resharding](resharding.md) | Changing the shard set |
| [Backup and restore](backup-and-restore.md) | What to back up; what a restore does to flows |
| [Data handling](data-handling.md) | What is stored, what is searchable, retention |
| [Observability](observability.md) | Logs, metrics, traces |
| [Deployment](deployment.md) | Database choice, pools, replicas, shutdown |
| [Cloud benchmarks](benchmark-cloud.md) | Sizing a deployment and the measurements behind it |
