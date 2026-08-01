# Upgrading

> **For operators** deploying an application that embeds dwarf. It covers two different operations that get
> confused with each other — redeploying your own code, and moving to a new version of dwarf — plus the
> compatibility obligation that outlives both: a long-running flow dispatches tasks from a graph frozen
> weeks ago.

## Dwarf is v0.x, and that is the headline

**There is no backward-compatibility promise yet.** The API and the database schema are both still evolving,
and a v0.x release may change either. Version skew between replicas is not tested and not supported.

So the practical rule while dwarf is pre-1.0:

> **Upgrading dwarf itself is a maintenance window, not a rolling deploy.** Stop the fleet, upgrade every
> replica, start the fleet. Read the release notes first.

This is a deliberate stance, not an oversight. Dwarf has no production community yet, so the cost of holding
a compatibility line now is paid entirely in constrained design, with nobody on the other side benefiting
from it. Once there is real usage, v1.0 will need to commit to the discipline sketched at the bottom of this
page. Until then, the freedom to change the schema is worth more than the promise not to.

## Two operations, only one of which is risky

| You are changing | Dwarf version | Procedure |
|---|---|---|
| Your own application code, workflows, or configuration | unchanged | **Ordinary rolling deploy.** See below. |
| The dwarf dependency | changed | **Maintenance window.** Stop everything, upgrade, restart. |

The everyday case is the first, and it is unremarkable — replicas of your app come and go all the time, and
the engine is built for it. What needs care is the second.

## What happens at startup

Every replica runs schema migrations against **every shard it opens**, as part of `Startup`, before it
dispatches any work. Migrations are numbered, applied in order, recorded per database, and **forward-only** —
there are no down migrations and no rollback path in the engine.

The consequence that drives the maintenance-window rule: **the first replica of a new version to start
migrates the database that every old replica is still using.** You do not get to sequence the schema change
separately from the code change. Starting the new code *is* the schema change, and if that migration is not
one an older replica can tolerate, the older replicas are now running against a schema they do not
understand — with nothing detecting it and nothing warning you.

**Downgrade is not supported.** Migrations are forward-only, so rolling back to a version that predates a
migration leaves that version running against a schema it did not create. **Roll forward.** If you must go
back across a migration, treat it as a database restore, not a deploy — and read the restore section below,
because a restore has workflow-level consequences.

**One thing that is permanent even in v0.x:** the migration sequence identity. The engine namespaces its
migrations under a fixed name in the database's migration ledger, and it must never change once deployed — a
changed name makes every migration look unapplied, and the engine will try to re-create tables that already
exist.

## Upgrading dwarf (v0.x)

1. **Read the release notes** for schema or API changes. Assume both are possible.
2. **Take a database backup** of every shard, and know the restore consequences below before you need them.
3. **Drain the fleet.** Stop every replica gracefully — see the drain window below. In-flight steps that are
   killed rather than drained will re-run after the upgrade, which is safe but wasteful.
4. **Confirm nothing is executing.** No replica running means no step is being dispatched; flows simply wait.
   Durability is not at risk here — flows resume where they left off.
5. **Start one replica.** It applies the migrations. Confirm it comes up clean before starting the rest.
6. **Start the remainder**, one at a time, letting the fleet settle between each.

Flows survive all of this untouched. A stopped fleet means work stops moving, not that anything is lost:
pending steps stay pending, `interrupted` flows stay parked, and leases on steps that were in flight lapse
and re-dispatch once workers are back.

## Deploying your own application

With the dwarf version unchanged, this is an ordinary rolling restart. Three things make it go smoothly.

**Give each replica a drain window longer than your largest `TimeBudget`.** Graceful shutdown stops accepting
work and then waits for every in-flight task to finish; it will not abort a running task to shut down faster.
If your platform's termination grace period expires first, those steps are abandoned, their leases lapse, and
**the tasks run again** on another replica. Safe — execution is at-least-once — but wasteful, and it re-fires
side effects that already happened.

**Replace replicas one at a time and let the fleet settle between steps.** Each replica registers itself per
shard, and the live count divides each database's connection budget. A joining replica waits to be seen
before opening its own connections, so the fleet makes room rather than briefly overshooting — but that only
holds if you give it the moment it needs. Replacing the whole fleet at once is the case that transiently
over-connects the databases.

**Pin replica identity if your platform restarts replicas in place.** By default each process mints a random
identity, so a restart leaves the old registry row behind until it ages out (it stops counting toward the replica
count after 40 seconds; the row itself is deleted once it is over 80 seconds old and the registry has been
continuously readable for 5 minutes). That is fine for a deploy. It is not fine for a
crash-looping replica, which accumulates a corpse per restart and shrinks every survivor's connection pool.
Pass a value stable across that replica's restarts — a pod ordinal or hostname — to `SetEngineID` before
startup. It must be unique among concurrently-live replicas: two live replicas sharing an id count as one,
which over-sizes pools, so a wrong stable id is worse than the random default.

**Verify after.** `dwarf_peer_replicas` should settle at your intended replica count, and
`dwarf_peer_changes_total` should stop moving once the deploy finishes — it counts observed fleet changes, so
in a settled fleet it is flat by construction. Continued movement means replicas are still churning.

## The obligation that outlives every deploy: graph evolution

**A flow freezes its workflow graph at creation.** A 30-day approval flow created today will still be
dispatching the graph it was created with a month from now, long after your codebase has moved on. Nothing
re-validates or re-loads it. This is true regardless of dwarf's own versioning, and it is entirely about
*your* code.

That makes task names a **long-lived compatibility surface**, longer-lived than any deploy:

- **A task name that any live flow's frozen graph can still reach must remain dispatchable.** If your host
  can no longer route a task name, every flow whose frozen graph reaches it fails at that step — and it fails
  when the flow gets there, which may be weeks after you removed the task.
- **Renaming a task is a breaking change** for every in-flight flow. Add the new name and keep the old one
  routable until no live flow can reach it.
- **Changing what a task *does* is generally safe**, because the frozen graph pins the name and the routing,
  not the behaviour. Changing what a task *expects in state* is not: a flow that started under the old shape
  will arrive at the new code carrying the old state.

**Retiring a task safely:**

1. Stop creating flows whose graphs reference it.
2. Wait out the longest lifetime any existing flow can have. If you do not know it, find out: `List` on
   `Status: running` and on `Status: interrupted` shows what is still alive.
3. Confirm nothing is left — including `interrupted` flows, which wait indefinitely and are the ones most
   likely to outlive your assumption.
4. Remove the task.

**There is no way to move an existing flow onto a new graph.** This is the part to plan around, because the
obvious escape hatches are not ones:

- **`Fork` carries the frozen graph forward.** It clones the origin flow's stored graph verbatim into the
  new flow — it does not re-fetch the definition. A fork of a flow frozen against the old graph is still
  running the old graph.
- **`Continue` does the same.** A new turn in a thread runs the same graph the thread was created with.

**Only `Create` loads a graph**, so the only way to get a flow onto a new definition is to start a new flow.
If you cannot wait out the existing ones, that means cancelling them and creating replacements — carrying
whatever state matters across yourself, by reading the old flow's outcome with `Snapshot` and passing it as
the new flow's initial state. You lose the original's progress and history continuity; it is a migration,
not a resume.

Which is why the compatibility obligation above is a hard requirement rather than a preference: **keeping
the old task name routable is the only cheap option.** Plan the retirement, or plan the migration.

## Rolling back across a migration

Migrations are forward-only, so there is no downgrade path in the engine. If you must go back across one,
that is a database restore rather than a deploy — and a restore has workflow-level consequences (flows
re-run, completed work rewinds, deleted flows come back). See
**[Backup and restore](backup-and-restore.md)** before doing it.

## What v1.0 will need to promise

Recorded here so the eventual commitment is a decision rather than a discovery. None of this is in force
today.

- **Additive schema changes only** across supported versions — no dropped or renamed columns, no `NOT NULL`
  additions without a default — so an older replica can run against a newly migrated database.
- **A defined skew window**, most likely one minor version, making a rolling upgrade of dwarf itself
  supported rather than a maintenance window.
- **A stated downgrade position**, since forward-only migrations mean rollback needs either a documented
  guarantee or an explicit "restore instead."
- **A CHANGELOG and a semver policy**, so an operator can tell from the version number whether a release
  needs a window.

Until those exist, treat every dwarf version bump as potentially breaking.
