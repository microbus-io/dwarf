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
| A replica dies holding in-flight steps | Each step's lease lapses and the step returns to `pending` for another replica | Up to the flow's `TimeBudget` + 30s, then within the next 5-minute recovery sweep |
| A database blip mid-write | The step's outcome write retries in place, holding its lease — the task does **not** re-run | 1s, 2s, 4s; longer outages fall through to lease recovery |
| A replica joins or leaves | Every peer re-reads the shared registry and re-divides each database's connection budget | Registry read every 250ms; a dead replica stops counting after 40s |
| A doorbell is missed between replicas | The owning replica's next selection cycle finds the work by scanning | Under a second at the derived cycle rate |
| A flow marked for deletion | A background reaper removes the flow and its whole subgraph tree | Within ~1 minute |
| A subgraph park that never released | A sweep re-drives the release, or cancels an orphaned child | Detected after 5 minutes, swept every 5 minutes |

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
matters is between counters that should **never** move and counters that are merely informative.

**Alarms — page on any non-zero rate:**

| Instrument | Means |
|---|---|
| `dwarf_steps_unwedged_total` | A step wedged and a sweep papered over it. The effect is repaired; the cause is not. |
| `dwarf_flows_orphaned_total` | A running flow is stranded with every step terminal. **Detection only** — these need manual recovery. |
| `dwarf_steps_write_failed_total` | A step's outcome could not be stored while the database was reachable, so the payload is at fault. |

**Health — alert on sustained levels, not on any movement:**

| Instrument | Read it as |
|---|---|
| `dwarf_steps_oldest_pending_age_seconds` | The honest backlog signal. Rising means dispatch is not keeping up. Aggregate with `max`, never `sum`. |
| `dwarf_steps_recovered_total` | Leases lost to crashes or overruns. A steady trickle means tasks are overrunning their budget. |
| `dwarf_peer_replicas` | The fleet size each replica believes in. Should equal your actual replica count. |
| `dwarf_turnstile_available` / `_waiting` | Read as a pair. Sustained zero available with a deep wait queue means database connections are the binding constraint. |
| `dwarf_steps_stolen_total` | Non-zero names a peer that is alive but not dispatching its share. |

**One dashboard trap worth stating up front:** two of the gauges are computed by querying the shared
database, so every replica reports the *same* number. Summing `dwarf_steps_pending` across three replicas
shows a 1,000-step backlog as 3,000, and a summed `oldest_pending_age` is meaningless outright. The
per-replica/cluster-wide split is tabulated in [Observability](observability.md).

## Sizing and capacity

Sizing is measured rather than guessed, and the numbers live with the measurements:

- **[Deployment](deployment.md)** — choosing a database, declaring shard facts, connection pools, workers,
  drain windows, and the disk-throughput requirement.
- **[Cloud benchmarks](benchmark-cloud.md)** — the sizing formula and every constant behind it, measured
  against managed PostgreSQL across a real network hop.

The two rules to carry into a capacity plan: **1 engine vCPU per 6 database vCPUs** at a 70% database
utilisation target, and **at least 3 replicas** for resiliency. They are separate constraints on different
dimensions, not a single maximum. Both assume your tasks are cheap; a host dispatching tasks over a network
transport pays more engine CPU per step and should measure its own.

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
| [Cloud benchmarks](benchmark-cloud.md) | Sizing formula and the measurements behind it |
