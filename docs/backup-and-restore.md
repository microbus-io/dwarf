# Backup and restore

> **For operators** planning backups, and for whoever is holding the pager when a restore actually happens.
> A restore is a workflow-level event, not just a database one — flows re-run, completed work rewinds, and
> side effects fire again. Know that before you need to.

## What to back up

**The shard databases, and nothing else.** Dwarf keeps no state outside SQL: no local disk, no broker, no
external service, no in-memory state that matters across a restart. A replica is disposable — everything
durable is in the tables. So your backup strategy is entirely your database's, and the engine adds no step
to it.

Two things worth confirming rather than assuming:

- **Every shard is backed up**, on the same schedule. A shard is an independent database and nothing
  reconciles them.
- **The migration ledger travels with the data.** Dwarf records which migrations have run in a table in each
  shard database; a restore that produces data without it will attempt to re-create existing tables.

## Restore all shards to the same point, or none

A flow lives entirely on one shard, and subgraph trees, threads and forks are all shard-local. So a partial
restore does not corrupt individual flows — nothing is split across a boundary.

What it does produce is worse in a quieter way: **a fleet where some tenants are days behind others, with no
engine-level signal that this is the case.** Every flow looks internally consistent. Nothing alarms. You
find out from customers.

If you must restore one shard, treat the divergence as the incident: know which tenants sat on that shard
(`Query.FairnessKey` is normally the tenant, and `Query.Shard` scopes a listing to one shard), and decide
deliberately what happens to their work.

## What a restore does to flows

Three consequences, all of which follow from flows being durable and leases being wall-clock timestamps.

**Every step that was running at backup time re-dispatches immediately.** Leases in the restored data are
all in the past, so they read as expired. Expect a burst of `dwarf_steps_recovered_total` and a
corresponding burst of duplicate side effects the moment the fleet comes up.

**Work that completed after the backup point is rewound.** Those flows return to whatever state they were in
and run forward again from there. A flow that finished, and whose completion your application already acted
on, will finish a second time.

**Flows deleted after the backup point come back.** `Delete`, `Purge` and `DeleteOnCompletion` removals are
ordinary row deletions, so a restore resurrects them. If you deleted data in response to an erasure request,
**a restore reinstates it** — that is a compliance concern, not just an operational one, and it is worth
knowing which of your backups still contain a record you were asked to remove.

## Before you restore

1. **Stop the fleet.** Restoring underneath running replicas mixes restored rows with live ones and produces
   states nothing in the engine expects.
2. **Know your window.** Everything between the backup point and now is lost or will re-run. Which of your
   tasks are safe to run twice? That question is answered by whether they're idempotent
   ([Writing tasks → Idempotency](tasks.md#idempotency)), and the answer is the same one that governs
   ordinary crash recovery — a restore is just a much larger dose of it.
3. **Expect side effects to re-fire.** Tasks with an idempotency key keyed on `StepKey()` survive this
   correctly, because a restored step keeps its key. Tasks without one will repeat their effect.
4. **Consider whether to restore at all.** Because the engine never auto-purges, a flow that failed or
   stalled is still present and still forkable. Recovering specific flows with `Fork` is often narrower and
   safer than rewinding every tenant to a point in the past.

## After a restore

Expect, and do not be alarmed by:

- **A burst of `dwarf_steps_recovered_total`** as expired leases re-dispatch. This is the designed path.
- **A backlog spike**, since everything that was in flight becomes runnable at once.
  `dwarf_steps_oldest_pending_age_seconds` will climb and then drain.
- **Phantom peers** if the `dwarf_peers` rows were captured in the backup. Replicas that no longer exist
  will be counted until their rows age out (they stop counting after 40 seconds; the rows are deleted once
  over 80 seconds old with the registry continuously readable for 5 minutes), shrinking every live replica's connection pool meanwhile. See the
  [Runbook](runbook.md#throughput-collapsed-after-a-restart-or-crash) if it does not resolve on its own.

What should **not** happen: `dwarf_steps_write_failed_total`, `dwarf_steps_unwedged_total` or
`dwarf_flows_orphaned_total` moving. Those are alarms in a restored fleet exactly as in a healthy one — see
the [Runbook](runbook.md).

## Test the restore, not just the backup

The engine-specific thing worth rehearsing is not whether the bytes come back — your database vendor has
that covered — but **what your workflows do when they re-run.** Restore into a staging environment, bring a
fleet up against it, and watch which side effects fire a second time. That is the part no database-level
test exercises, and it is the part that will surprise you.

If a rehearsal shows duplicate side effects you cannot tolerate, the fix is idempotency keys in the affected
tasks, not a change to how you back up.
