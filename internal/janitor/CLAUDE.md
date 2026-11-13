# Dwarf `internal/janitor` — one shard's background jobs, a goroutine each

> Load when: changing how a janitor's jobs are scheduled, started, stopped or drained, how a batch takes its turn
> and its connection, fast archiving or fast deletion, or the janitors' metrics.
> Coupled with: `engine/CLAUDE.md` §"The janitors" (their activity, the residue class, the drain order, the test seams),
> `internal/turnstile/CLAUDE.md` (what a claim's age means), `internal/reaper/CLAUDE.md` (the work the janitor has not
> taken over: the sweeps, staging, deletion, retention) and `internal/archiver/CLAUDE.md` (the day tables).

## What a Janitor is, and what a job is

One `Janitor` per shard on every replica, for the replica's run. It is **active only while the replica dispatches
the shard** (`Resume`, `Pause`): paused, it holds no goroutine and no connection. It is a runner of jobs: their
goroutines, their idle waits, their time since progress, and the turn and connection each batch takes. It works on
the `database.Shard` it is made with, and the engine sets its activity from the dispatch role.

**A job owns everything particular to it**: its settings, its dependencies (fast archiving's archive and retention),
its position and what it observed, its instruments and gauges. The janitor holds no setting of any job. A job's
settings are fixed when it is added (`AddJob(name, job)`), and **changing them means adding a new job under the same
name**, which replaces the old one and starts over - which is what a test changing the engine's fields mid-run wants,
and production never does. The name is the job's handle - its metric label, what `RemoveJob` takes - so two jobs of
one kind can run side by side (`fast1`, `fast2`). `Job` is an interface whose methods are unexported: only this
package's jobs, and its tests', satisfy it.

A resume starts every job over, its position and observations forgotten: a replica that dispatches the shard again
saw nothing of what happened meanwhile, so everything it kept is a stale hint, and losing it loses no tree. Each job
reads its residue class from the shard at every round (`Shard.Residency`).

**One goroutine per job, never one goroutine for all of them.** Each job is a loop of rounds on its own goroutine,
so a job that is slow, blocked on a held root or working through a large tree does not slow the others. A single
goroutine taking turns between jobs in time slices fell behind the rate flows closed under a saturated bench, its
backlog growing all run. Do not reintroduce slices, a pass budget or a shared tick.

## The round and the idle wait

A round reports whether it made progress: work done that moved the job forward. A round with progress is followed
at once by the next; a round without - nothing found, or every candidate held or failing - by the job's own idle wait
(1 s for the jobs on the live tables; `SetIdle` replaces every job's for a test). **The wait follows a round without
progress, not only an empty scan:** a job whose candidates all fail would otherwise spin on them. No failure
counters, backoff or retry classes beyond that.

The janitor records the time of each round with progress, and `JobAges` reports how long since, per job
(`dwarf_janitor_progress_age_seconds`). A blocked job shows there: SQL Server waits on a held lock without bound, and
nothing else bounds a job's statement.

## The batch: a fresh turn, its timings

A round does its work in batches through the `batchFunc` it is handed.

- **Each batch takes a turn of its own, from a context with no claim.** The turnstile keeps the age of a context
  stamped before, so a job that reused a batch's context would rank as old as its first batch and outrank every
  dispatch that arrived since. A batch whose context carries a claim fails rather than run - the same check catches
  a batch inside a batch, a turn taken while one is held. Pinned by `TestJanitor_EachBatchTakesAFreshClaim`.
- **The turn is a common turn on the janitor's shard** (`Shard.WaitTurn`), dispatch's band. A lower band could starve
  the jobs at peak load and let the live tables grow; equal standing does not guarantee them any share, which the
  batch timings are there to show.
- **Each batch takes one connection from the pool, once, and runs every statement on it** (a `sequel.Conn`, closed
  when the batch ends). Under a full pool every acquisition queues behind dispatch, and a batch that took one per
  statement - its scan, its attempt read, its sizing, its transaction - spent most of its time in those queues rather
  than in SQL. Pinned by
  `TestJanitor_ABatchRunsOnOneConnection`. Its reads run on the connection before its transaction begins: SQLite
  needs a transaction to open with a write, and on MySQL a read inside it would pin its snapshot for the batch.
- **A batch never takes a second connection while it holds its own** - no statement on the shard's `DB`, nor on
  anything holding it, the archive included (`archiver.TodayLeftOn` reads the day on the batch's connection). A
  batch holding one connection and waiting for another is hold-and-wait: enough of them at once take every
  connection of the pool and wait forever. Read each query's rows to the end before the next statement, too: the
  connection runs one statement at a time. Pinned by `TestJanitor_NoBatchTakesASecondConnection`, which runs every job
  on a pool of one connection.
- **A batch that cannot take its connection fails** without running its work, and its job reports it: an archive
  failure for fast archiving, a logged error otherwise (`TestJanitor_ABatchThatCannotTakeItsConnectionFails`,
  `TestFast_ABatchThatCannotTakeItsConnectionIsAFailure`).
- **Timings** (`dwarf_janitor_batch_seconds`, by shard, job and part): `turn` is the wait for the turn, `acquire`
  for the connection, `work` the batch's statements - none for a batch that could not take its connection. They attribute a job's shortfall to its own waits, which the
  pool-wide counters cannot.

## Pausing and stopping

`Pause` cancels the active period's context and returns; `Resume` waits for that period's goroutines to be gone
before the jobs start over, so two never run one job. `Stop` ends the run: it cancels and joins, and a stopped
janitor never resumes. A batch in flight is abandoned - its statement cancelled, its transaction rolled back - which
strands nothing: every write is one statement or one transaction. Jobs have no deadline, so cancellation is the only
thing that ends a blocked one; the engine stops the janitors before it drains its workers for that reason
(`engine/CLAUDE.md`). Pinned by `TestJanitor_PauseAndResume`, `TestJanitor_ABlockedStatementIsCancelled` (a pause and
a stop, inside a statement that never ends by itself), `TestJanitor_StopCancelsABlockedJob` and the engine's
`TestJanitor_APauseOrShutdownCancelsABatchInFlight`.

## The deterministic test mode

`SetManual` starts no goroutine, and `Drain` runs the jobs of an active janitor: each job's rounds until one makes no progress, over and over
until a pass over every job makes none. It returns where a running job would wait, so a test whose roots are held or
failing cannot hang on it, and jobs that feed each other are run until both are done. A round holds its job's mutex,
so a `Drain` beside the goroutines never overlaps a round (`TestJanitor_ADrainBesideTheGoroutineNeverOverlapsARound`).
Pinned by `TestJanitor_DrainReturnsWhereAJobWouldWait`.

## Fast archiving (`fastarchiving.go`)

The janitor's first job (`JobFastArchiving`): the closed roots as they come due, by close time, one batch a round,
moved into today's archive in one transaction - or deleted, at a retention of 0. The move, its root locks and the
rechecks under them, the directory rows and the archive time are the reaper's, carried over (`move.go`;
`internal/reaper/CLAUDE.md` holds their rationale while the reaper stands).

- **A position, and nothing behind it.** The job scans from its position, the last root a round was done with, and
  moves the position past every root the round considered, whatever became of it: moved, held, failed, or not its own.
  A root behind the position - one it failed on, one held, one that committed late with an older close - is left to a
  sweep (the reaper's repair walk). A job that held its position on a failure would retry it every round and reach
  nothing behind it (`TestWalk_FastPassesTheBatchItAttempted`). A round that removed nothing - held or failed - makes no
  progress and the job waits; the batches after it are reached on the next round.
- **Not its own, and passed:** a root with an archive attempt (the staged path's, whoever copies it); with staging on,
  a tree over the staging threshold, alone in its batch, which the reaper's repair walk stages; at a retention of 0, a
  tree too large for a batch, which the job stamps for deletion - due at once - and the deletion of stamped trees
  removes in chunks. Passing them counts as progress. The stamp locks the root as a move does, so a root a Fork holds
  is skipped on PostgreSQL and MySQL rather than waited for (`TestFast_AHeldRootIsNotWaitedForWhenStamped`).
- **The residue class is read every round, and a change starts the walk over.** When a peer leaves, the roots of its
  class are already behind the survivors' positions, which were not theirs then; started over, the job takes them at
  once rather than leaving them to a sweep (`TestFast_AResidencyChangeStartsOver`). The scan also takes another class's
  roots overdue by the steal grace, a fixed 30 s past the archive grace - but only those ahead of the position, which
  happens when this job is behind. By the time a slow peer's roots are overdue, a job keeping up has passed them: they
  are a sweep's (the reaper's repair walk).
- **An atomic batch holds at most `MaxBatchFlows` (4,096) flows**, whatever its rows and bytes allow: every flow of a
  batch gets one archive time, and `ListClosed` cannot page across a tie larger than its `Limit`, so the lists'
  maximum must stay above it. A tree of more goes alone (`TestMove_ABatchHoldsAtMostMaxBatchFlows`).
- **The job reports its own gauges** - `dwarf_archive_lag_seconds` and `dwarf_archive_failing_seconds` - registered
  on the meter when it is added, observed only while its janitor is active, and unregistered when it is removed or
  the janitor stops. The engine reads nothing of it.
- **A probe reads the shard's oldest due closed root** - one `LIMIT 1` per terminal status on
  `idx_dwarf_flows_status`, unrestricted: every replica's class, behind the position or ahead of it. It runs at most
  once a second while the job is busy, and in every round whose scan finds nothing, which the idle wait paces. A
  probe's scan consults no fault, or it would consume one a test armed for a round's scan.
- **The lag is the probe's**: how long past the grace the oldest due root had waited, a root a failure left behind
  the position included, and one being staged; 0 when none is. Pinned by
  `TestFast_TheLagIsTheShardsOldestDueRootPastTheGrace`.
- **The failure episode** (`dwarf_archive_failing_seconds`) begins at an archive failure - a day not readable or not
  whole, a batch that cannot take its connection, a failed scan, attempt read, sizing or move - and ends at a later
  move that archives something, or at a probe that finds the oldest due root without an archive attempt came due
  after the last failure: everything due then has left the live tables. A staged tree in progress is not a failure,
  so it holds no episode open. The probe's ages are the database's and the failure's time is this process's, compared
  as durations, so neither clock's offset matters. **Do not end it at an empty round:** the scan is after the
  position, and a root a failure left behind is not in it, so a failing root on a quiet shard would read as
  recovered. Ending it only at a move fails the other way: one transient error on a quiet shard reads as failing for
  as long as nothing closes. **And end it only in a round that could archive** - its day read and open: a closed day
  fails every round, and an episode ended in between would read as starting anew each time
  (`TestArchiveMetrics_ASweepWhileTomorrowIsMissingEndsNoFailure`). Pinned by
  `TestFast_TheFailureEndsAtAMoveOrOnceItsRootsHaveLeft` and the engine's
  `TestArchiveMetrics_ATransientFailureEndsWhenNothingDueAtItRemains` and
  `TestArchiveMetrics_TheReaperCountsWhatItMoves`. A stop records nothing. Only the episode's first failure is
  logged (`TestMove_ALastingFaultPreservesAndLogs`). At a retention of 0 nothing is archived, so nothing starts an
  episode (`TestMove_AScanFailureAtRetentionZeroIsNoArchiveFailure`).
- **The counters it shares with the reaper** - `dwarf_flows_archived`, `dwarf_archive_move_failures`,
  `dwarf_flows_reaped` - are created on the same meter by both packages, each with its description written word for
  word the same in both: two instruments of one name with different descriptions are exported as two streams of that
  name. The reaper does not import the janitor; the text is duplicated until the reaper is deleted.
- **The recheck under the root lock is the authority, not the scan:** a root selected and sized is moved only if it is
  still a closed root due, unstamped, and with no archive attempt when locked (`TestMove_TheLockRechecksEveryRoot`); a
  stamped one only if still stamped and due (`TestDelete_TheLockRechecksTheStamp`).
- **The position's close time is normalized** (`database.StoredTime`) before it is bound back: a MySQL driver with a
  DSN `loc` other than UTC places the stored wall clock in that location. The fault and checkpoint names are the reaper's strings, named in
  `internal/diagnostics`, so a test arming one arms both.

## Fast deletion (`fastdeletion.go`)

The second job (`JobFastDeletion`): the trees stamped for deletion - a Delete's, a DeleteOnCompletion's once its
grace has run, a published tree's live copy - deleted whole as their stamps come due, one batch a round, by the same
transaction as an archive move with no day (`move`, the stamp rechecked under the root lock). It walks the stamped
roots by `flow_id` through `idx_dwarf_flows_delete_after`, from a position it moves past every root a round
considered, and starts over when its residue class changes, as fast archiving does.

- **A stamped tree too large for a batch is passed**, stamped and whole, to the deletion in chunks (the reaper's repair
  walk of stamped trees), which yields between its chunks; one transaction deleting a tree grows with the tree until it
  cannot finish (`TestDelete_ALargeStampedTreeIsPassed`).
- **By `flow_id`, any stamp that comes due behind the position waits for a sweep**, and more do than the old-flow case
  suggests: a Delete of an old flow; a DeleteOnCompletion flow still in its grace when a newer flow's stamp comes due
  (an operator Delete of a fresh flow passes every such flow below it); with mixed durations, every
  DeleteOnCompletion flow outlived by one created after it. Every read hides a stamped tree the moment it is stamped,
  so the wait costs storage, not correctness. Ordering by due time instead would need an index on it.
- **`dwarf_flows_reaped{reason="deleted"}` counts only trees never archived**: a published tree's live copy, stamped
  with its attempt or carrying the archive time its publication copied, is counted at the move's root locks and left
  out (`publishedSQL`, `TestDelete_APublishedLiveCopyIsNotCountedReaped`).

Until large-tree archiving moves (step 5 of the plan), a staged archive is copied only by the reaper's repair walk,
which never ends or begins an archive failure episode: a stuck staged copy shows in
`dwarf_archive_oldest_attempt_seconds`, not in `dwarf_archive_failing_seconds`. Staging is off in production.

Its tests run on a rig (`rig_test.go`): an isolated shard database, an archive with today's tables made, and a Manual
janitor whose settings a test changes between passes.
