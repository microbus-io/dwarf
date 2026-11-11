# Dwarf `internal/janitor` — one shard's background jobs, a goroutine each

> Load when: changing how a janitor's jobs are scheduled, started, stopped or drained, how a batch takes its turn
> and its connection, or the janitors' metrics.
> Coupled with: `engine/CLAUDE.md` §"The janitors" (the supervisor, the drain order, the test seams) and
> `internal/turnstile/CLAUDE.md` (what a claim's age means).

## What a Janitor is

One `Janitor` per shard, on each replica that dispatches the shard; a replica that does not dispatch a shard runs
none there. It owns its jobs' goroutines and everything they learn - positions, failures, observations - and the
gauges read it through `Observe`. A new janitor starts with nothing learned: everything it keeps is a hint, so a
handoff or a restart loses no tree, only where a job had got to.

**One goroutine per job, never one goroutine for all of them.** Each job is a loop of rounds on its own goroutine,
so a job that is slow, blocked on a held root or working through a large tree does not slow the others. A single
goroutine taking turns between jobs in time slices fell behind the rate flows closed under a saturated bench, its
backlog growing all run. Do not reintroduce slices, a pass budget or a shared tick.

## The round and the idle wait

A round reports whether it made progress: work done that moved the job forward. A round with progress is followed
at once by the next; a round without - nothing found, or every candidate held or failing - by `Idle` (1 s). **The
wait follows a round without progress, not only an empty scan:** a job whose candidates all fail would otherwise
spin on them. No failure counters, backoff or retry classes beyond that.

The janitor records the time of each round with progress, and `Observe` reports how long since, per job
(`dwarf_janitor_progress_age_seconds`). A blocked job shows there: SQL Server waits on a held lock without bound, and
nothing else bounds a job's statement.

## The batch: a fresh turn, its timings

A round does its work in batches through the `Batch` it is handed.

- **Each batch takes a turn of its own, from a context with no claim.** The turnstile keeps the age of a context
  stamped before, so a job that reused a batch's context would rank as old as its first batch and outrank every
  dispatch that arrived since. A batch whose context carries a claim fails rather than run - the same check catches
  a batch inside a batch, a turn taken while one is held. Pinned by `TestJanitor_EachBatchTakesAFreshClaim`.
- **The turn is at dispatch's band** (the engine's `priorityCommon`). A lower band could starve the jobs at peak
  load and let the live tables grow; equal standing does not guarantee them any share, which the batch timings are
  there to show.
- **The batch runs on the shard's pool**, so each statement takes a connection of its own and queues for it behind
  dispatch; on a full pool those waits, one per statement, can be most of a batch's time. `Handle` is the seam where
  a batch's one connection goes: anything with sequel's query methods and `Transact`.
- **Timings** (`dwarf_janitor_batch_seconds`, by shard, job and part): `turn` is the wait for the turn, `work` the
  batch's statements, the pool's waits included. They attribute a job's shortfall to its own waits, which the
  pool-wide counters cannot.

## Stopping

`Stop` cancels the jobs' context and joins their goroutines. A batch in flight is abandoned - its statement
cancelled, its transaction rolled back - which strands nothing: every write is one statement or one transaction.
Jobs have no deadline, so cancellation is the only thing that ends a blocked one; the engine stops the janitors
before it drains its workers for that reason (`engine/CLAUDE.md`). Pinned by `TestJanitor_StopCancelsABlockedJob`
and the engine's `TestJanitor_AJobBlockedInAStatementIsCancelled`.

## The deterministic test mode

`Manual` starts no goroutine, and `Drain` runs the jobs: each job's rounds until one makes no progress, over and over
until a pass over every job makes none. It returns where a running job would wait, so a test whose roots are held or
failing cannot hang on it, and jobs that feed each other are run until both are done. A round holds its job's mutex,
so a `Drain` beside the goroutines never overlaps a round (`TestJanitor_ADrainBesideTheGoroutineNeverOverlapsARound`).
Pinned by `TestJanitor_DrainReturnsWhereAJobWouldWait`.
