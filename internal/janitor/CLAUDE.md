# Dwarf `internal/janitor` — one shard's background jobs, a goroutine each

> Load when: changing how a janitor's jobs are scheduled, started, stopped or drained, how a batch takes its turn,
> any of its jobs - fast, sweep and large-tree archiving, fast and sweep deletion and their shares, live retention,
> the archive deletion sweep, archive retention, expiration, the recovery jobs - chunked deletion, the shared sweep
> machinery, or the janitors' metrics.
> Coupled with: `engine/CLAUDE.md` §"The janitors" (their activity, the residue class, the drain order, the test seams),
> `internal/turnstile/CLAUDE.md` (what a claim's age means), `internal/archiver/CLAUDE.md` (the day tables, the
> directory, why a row goes into a day only once the next is whole) and `internal/migrations/CLAUDE.md` (the column
> catalog and the indexes the scans walk).

## What a Janitor is, and what a job is

One `Janitor` per shard on every replica, for the replica's run. It is **active only while the replica dispatches
the shard** (`Resume`, `Pause`): paused, it holds no goroutine and no connection. It is a runner of jobs: their
goroutines, their idle waits, their time since progress, and the turn each batch takes. It works on
the `database.Shard` it is made with, and the engine sets its activity from the dispatch role.

**A job owns everything particular to it**: its settings, its dependencies (fast archiving's archive and retention), its
position and what it observed, its instruments and gauges. The janitor holds no setting of any job. A job's settings are
fixed when it is added (`AddJob(name, job)`), and **changing them means adding a new job under the same name**, which
replaces the old one and starts over - which is what a test changing the engine's fields mid-run wants, and production
never does. The name is the job's handle - its metric label, what `RemoveJob` takes - so two jobs of one kind can run
side by side (`fast_archiving_1`, `fast_archiving_2`: "Shares" below). `Job` is an interface whose methods are
unexported: only this package's jobs, and its tests', satisfy it.

**A replacement is one step**: `AddJob`, `RemoveJob` and `Stop` are serialized (`edit`), so two `AddJob`s of one name
never both remove the old job before either adds theirs, leaving two running under it
(`TestJanitor_AReplacementByNameIsOneStep`). `edit` is held across a job's attach and detach, which wait for the meter's
lock, so it is never taken under `life` nor by a gauge's callback. **A removal reaches every round of the job**, a
`Drain`'s included, which runs on the active period's context and its own copy of the jobs: each job has a removal
context that cancels its round in flight and stops any later one, and removal waits out that round before detaching
(`TestJanitor_RemovingAJobEndsItsDrainedRound`).

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
(`SetIdle` replaces every job's for a test). **The wait follows a round without progress, not only an empty scan:** a
job whose candidates all fail would otherwise spin on them. No failure counters, backoff or retry classes beyond that.

**The fast jobs wait 1 s, the repair jobs 30 s** (`fastIdle`, `repairIdle`; `TestJanitor_TheIdleWaits`), archive
retention its pass interval, expiration its own pace and the recovery jobs their interval (below). A fast job polls for work that keeps arriving. A repair job - the sweeps, live
retention, large-tree archiving - normally has nothing to do, and its wait is what it costs then: polled every second it would take a turn and its reads every second
on every shard, and retry a root or tree that keeps failing every second, a large tree's failing chunk copied again
each time. So in a healthy fleet the janitor's steady cost is its fast jobs. The wait slows nothing that works: a
round with progress is followed at once by the next. What it delays: a large tree's first chunk after fast archiving
starts its attempt, a root fast archiving passed (up to the sweep's window plus the wait, which is how long a single
failure shows in `dwarf_archive_lag_seconds`), and the repair jobs' gauges, which are as old as their last round - an
attempt published, or a held root taken, reads until the next.

The janitor records the time of each round with progress, and `JobAges` reports how long since, per job
(`dwarf_janitor_progress_age_seconds`). A blocked job shows there: SQL Server waits on a held lock without bound, and
nothing else bounds a job's statement.

## The batch: a fresh turn, its timings

A round does its work in batches through the `batchFunc` it is handed - every job's, archive retention's, expiration's
and the recovery jobs' included.

- **Each batch takes a turn of its own, from a context with no claim.** The turnstile keeps the age of a context
  stamped before, so a job that reused a batch's context would rank as old as its first batch and outrank every
  dispatch that arrived since. A batch whose context carries a claim fails rather than run, logged by the janitor -
  the same check catches a batch inside a batch, a turn taken while one is held. Pinned by
  `TestJanitor_EachBatchTakesAFreshClaim`.
- **The turn is a common turn on the janitor's shard** (`Shard.WaitTurn`), dispatch's band. A lower band could starve
  the jobs at peak load and let the live tables grow; equal standing does not guarantee them any share, which the
  batch timings are there to show.
- **Its statements run on the shard's DB**, each taking a connection from the pool and returning it, as dispatch's
  do; a transaction holds one until it ends. **Do not take a connection for the batch** (a `sequel.Conn` for all its
  statements): it exempts the job's later statements from the pool's queue, a priority no other caller has (user).
  Its reads run before its transaction begins: SQLite needs a transaction to open with a write, and on MySQL a read
  inside it would pin its snapshot for the batch. **On PostgreSQL the sizing reads run in a short transaction of their
  own** (`cutTrees`, `sizingReads`), committed before the batch's, for the bounded reads below - three round trips a
  batch.
- **Inside a transaction every statement runs on its `Tx`, and a query's rows are read to the end before the next
  statement.** A statement on the DB meanwhile takes a second connection while one is held - hold-and-wait: enough of
  them at once take every connection of the pool and wait forever. The archive's statements run on the shard's DB,
  so a call of it that reads or writes - its clock, `Maintain` - is made outside any transaction. Pinned by
  `TestJanitor_EveryJobRunsOnAPoolOfOne`, every job on a pool of one connection.
- **Timings** (`dwarf_janitor_batch_seconds`, by shard, job and part): `turn` is the wait for the turn, `work` the
  batch's statements, their waits for the pool included. They attribute a job's shortfall to its own waits, which the
  pool-wide counters cannot.
- **A batch is at most 500 roots, about 4,000 rows or 16 MiB of payload**, whichever it reaches first (`SetBatch`;
  the package's defaults and the engine's `reapBatch`, `reapBatchRows`, `reapBatchBytes`), sized to stay under about
  0.25 s a transaction on a 32 vCPU PostgreSQL under load (user: the aim). Its roots go in one bound list of a length
  `database.IDList` pads to, and a large tree's chunk in two lists of step ids (user).

## Pausing and stopping

`Pause` cancels the active period's context and returns; `Resume` waits for that period's goroutines to be gone
before the jobs start over, so two never run one job. `Stop` ends the run: it cancels and joins, and a stopped
janitor never resumes. A batch in flight is abandoned - its statement cancelled, its transaction rolled back - which
strands nothing: every write is one statement or one transaction. Jobs have no deadline, so cancellation is the only
thing that ends a blocked one; the engine stops the janitors before it drains its workers for that reason
(`engine/CLAUDE.md`). **The one exception is archive retention making a day**: sequel's `Migrate` takes no context, so
a `Stop`, or a `Resume` after a `Pause`, waits for a migration in flight - up to sequel's 15-second lock lease when a
peer died holding the day's lock - and the engine resumes under its pools lock. Accepted: a day is made once a day.
Pinned by `TestJanitor_PauseAndResume`, `TestJanitor_ABlockedStatementIsCancelled` (a pause and
a stop, inside a statement that never ends by itself), `TestJanitor_StopCancelsABlockedJob` and the engine's
`TestJanitor_APauseOrShutdownCancelsABatchInFlight`.

## The deterministic test mode

`SetManual` starts no goroutine, and `Drain` runs the jobs of an active janitor: each job's rounds until one makes no progress, over and over
until a pass over every job makes none. It returns where a running job would wait, so a test whose roots are held or
failing cannot hang on it, and jobs that feed each other are run until both are done. A round holds its job's mutex,
so a `Drain` beside the goroutines never overlaps a round (`TestJanitor_ADrainBesideTheGoroutineNeverOverlapsARound`).
Pinned by `TestJanitor_DrainReturnsWhereAJobWouldWait`.

## Shares: jobs of one kind side by side (`share.go`)

A shard's janitor runs several fast archiving jobs, several fast deletion jobs and several large-tree archiving jobs,
each told its share when it is made (`SetShare(k, of)`: the k-th of `of`, from 1) and added under
`ShareName(kind, k)` - `fast_archiving_1`, `fast_archiving_2`, `fast_deletion_1`, `large_archiving_1` - a fast job with
a sweep of its own behind it under the same share's name (`sweep_archiving_1`). How many is the engine's: two of each,
one on SQLite (`engine/CLAUDE.md`). Large-tree archiving's share is by `flow_id` alone, below.

- **A root's share is `(flow_id / R) % of = k-1`**, R the residue class's count of dispatchers, or `flow_id % of = k-1`
  when the class restricts nothing. It is a conjunct beside the class (`closedResidency`, `stampedResidency`) and
  restricts even when the class fails open. Within a class the shares divide its roots, and a root has one share
  whichever class it is in, so a fast job steals another class's overdue roots of its own share only: each stolen root
  is still one job's on each replica (`TestShare_TheSharesSplitTheClosedRoots`,
  `TestShare_TheSharesSplitTheStampedRoots`, `TestShare_EachFastJobTakesItsShare`).
- **The share is one comparison, `(flow_id + R*of - R*(k-1)) % (R*of) < R`**: the range of `flow_id % (R*of)` from
  `R*(k-1)` to `R*k-1`, rotated. One modulus, since `/` is not integer division on MySQL. **Do not write it as a range**
  (`BETWEEN`, or two comparisons): PostgreSQL estimates a range on an expression at 0.5% of the rows and a comparison at
  a third, and at 0.5% a scan's `LIMIT` reads as a walk of every row. Measured on PostgreSQL 18 with 300k closed and 30k
  stamped roots, the range sent the stamped scan to a bitmap read and sort of every stamped root (10 ms against 0.3 ms)
  and the closed scan to parallel workers (25 ms against 1.2 ms). Pinned by `TestShare_TheScansWalkTheirIndexes`.
- **Each sweep sweeps its own fast job's share**, behind that job's position: the boundary carries the share with the
  class, and the sweep restricts its scan to both. A root of another share behind the position is that share's
  sweep's, and a sweep whose fast job has not run takes nothing (`TestShare_EachSweepTakesWhatItsFastJobPassed`). A
  share is fixed for its job, so the boundary's class comparison leaves it out.
- **The gauges are per job, each labelled with its name** (`job`), so two series of one name never collide: each fast
  archiving job's lag and failure episode are its share's - its probe reads the oldest due root of its share, of every
  class - and the shard's lag is the max of its jobs' (`TestShare_EachFastArchivingJobReportsItsLag`); each sweep's held
  count and failing series are its own.

## Fast archiving (`fastarchiving.go`)

The fast archiving jobs (`JobFastArchiving`, one a share): the closed roots of the job's share as they come due, by
close time, one batch a round, moved into today's archive in one transaction - or deleted, at a retention of 0. The
move - its root locks and the rechecks under them, the directory rows and the archive time - is under "The move" below.

**Do not shorten the archive grace (the engine's `archiveGrace`, a minute) to cut the archive's WAL.** A shorter grace
does log less - a row that leaves the live tables sooner outlives fewer checkpoints, so fewer of its pages are logged
whole - but the job then deletes from pages the hot path is still inserting into and updating, and the shard pays for
it in CPU. Measured on one 32 vCPU PG16 shard, 8 KB JSON per step, saturated, `max_wal_size` 20 GB: 1 min / 30 s / 15 s
ran 10,335 / 10,214 / 9,680 steps/s, with whole pages logged per step 1.83 / 1.26 / 1.03 and database CPU per step
1.52 / 1.78 / 1.88 ms; at 15 s `BufferContent` waits rose and the refill fetch averaged 197 ms against 17. Throughput
was set by the WAL flush cycle in every arm, so neither trade moved it far, and 1 min is best on both CPU and
throughput.

- **A position, and nothing behind it.** The job scans from its position, the last root a round was done with, and
  moves the position past every root the round considered, whatever became of it: moved, held, failed, or not its own.
  A root behind the position - one it failed on, one held, one that committed late with an older close - is left to a
  sweep (sweep archiving, below). A job that held its position on a failure would retry it every round and reach
  nothing behind it (`TestWalk_FastPassesTheBatchItAttempted`). A round that removed nothing - held or failed - makes no
  progress and the job waits; the batches after it are reached on the next round.
- **Not its own, and passed:** a root with an archive attempt (large-tree archiving's); with staging on, a tree over
  the staging threshold, alone in its batch, whose attempt the job starts - one small transaction under the root's lock, `claimAttempt`
  - for large-tree archiving to copy (`TestFast_ATreeOverTheStagingThresholdHasItsAttemptStarted`); at a retention of 0, a
  tree too large for a batch, which the job stamps for deletion - due at once - and the deletion of stamped trees
  removes in chunks. Passing them counts as progress. The stamp locks the root as a move does, so a root a Fork holds
  is skipped on PostgreSQL and MySQL rather than waited for (`TestFast_AHeldRootIsNotWaitedForWhenStamped`).
- **The closed scan carries each root's attempt flag** (`dueRoot.attempted`), so `cutClosed` passes attempted roots
  without another query. It is only a hint: an attempt started since the scan is rechecked under the move's lock,
  and a retention of zero ignores it (`TestClosed_AScannedAttemptIsAHint`). Fast and sweep archiving execute their
  routes through `removeClosed`, keeping their own rules for whether passing a cut made progress.
- **The residue class is read every round, and a change starts the walk over.** The shard reports every reading that
  restricts nothing alike (`internal/database/CLAUDE.md`), so a lone replica's dispatcher count arriving, or a class
  lapsing at its expiry, is no change here (`TestFast_AReadingThatRestrictsNothingKeepsThePosition`). When a peer leaves, the roots of its
  class are already behind the survivors' positions, which were not theirs then; started over, the job takes them at
  once rather than leaving them to a sweep (`TestFast_AResidencyChangeStartsOver`). The scan also takes another class's
  roots overdue by the steal grace, a fixed 30 s past the archive grace - but only those ahead of the position, which
  happens when this job is behind. By the time a slow peer's roots are overdue, a job keeping up has passed them: they
  are a sweep's.
- **An atomic batch holds at most `MaxBatchFlows` (4,096) flows**, whatever its rows and bytes allow: every flow of a
  batch gets one archive time, and `ListClosed` cannot page across a tie larger than its `Limit`, so the lists'
  maximum must stay above it. A tree of more goes alone (`TestMove_ABatchHoldsAtMostMaxBatchFlows`).
- **The job reports its own gauges** - its series of `dwarf_archive_lag_seconds` and of `dwarf_archive_failing_seconds`
  (labelled with the job's name, `job="fast_archiving_1"`) - registered on the meter when it is added, observed only
  while its janitor is active, and unregistered when it is removed or the janitor stops. The engine reads nothing of
  it.
- **A probe reads the oldest due closed root of the job's share** - one `LIMIT 1` per terminal status on
  `idx_dwarf_flows_closed_roots` on PostgreSQL (`idx_dwarf_flows_status` elsewhere), restricted to the share alone:
  every replica's class, behind the position or ahead of it. It
  runs at most once a second while the job is busy, and in every round whose scan finds nothing, which the idle wait
  paces. A probe's scan consults no fault, or it would consume one a test armed for a round's scan.
- **The lag is the probe's**: how long archiving is behind - how long past the grace the oldest due root has waited, a
  root a failure left behind the position included, and one being staged; 0 when none is. **It goes on aging after its
  probe** (the probe records when the root passed the grace), so between probes it reads how long that root has waited,
  not how long it had: a reading frozen at the probe would understate the lag by up to a probe's interval, and by the
  whole stall when the job is blocked. Pinned by
  `TestFast_TheLagIsTheShardsOldestDueRootPastTheGrace`.
- **The failure episode** (`dwarf_archive_failing_seconds`) begins at an archive failure - a day not readable or not
  whole, a failed scan, sizing or move - and ends at a later move that archives something, or at a probe
  that finds no due root of its share without an archive attempt, or finds the oldest came due more than `endMargin` (1
  s) after the last failure: everything due then has left the live tables. A staged tree in progress is not a failure,
  so it holds no episode open. The probe's ages are the database's and the failure's time is this process's, compared as
  durations, so neither clock's offset matters - **but they are read at different instants and resolutions, so do not
  compare them without the margin.** A root that failed just after it came due then reads as due after its own failure,
  and the episode ends with the root still live: with no margin, `TestArchiveMetrics_TheReaperCountsWhatItMoves` failed
  49 of 300 runs on SQLite. **Do not end it at an empty round:** the scan is after the position, and a root a failure
  left behind is not in it, so a failing root on a quiet shard would read as recovered. Ending it only at a move fails
  the other way: one transient error on a quiet shard reads as failing for as long as nothing closes. **And end it only
  in a round that could archive** - its day read and open: a closed day fails every round, and an episode ended in
  between would read as starting anew each time (`TestArchiveMetrics_ASweepWhileTomorrowIsMissingEndsNoFailure`). Pinned
  by `TestFast_TheFailureEndsAtAMoveOrOnceItsRootsHaveLeft` and the engine's
  `TestArchiveMetrics_ATransientFailureEndsWhenNothingDueAtItRemains` and
  `TestArchiveMetrics_TheReaperCountsWhatItMoves`. A stop records nothing. Only the episode's first failure is logged
  (`TestMove_ALastingFaultPreservesAndLogs`). At a retention of 0 nothing is archived, so nothing starts an episode
  (`TestMove_AScanFailureAtRetentionZeroIsNoArchiveFailure`).
- **The counters more than one job creates** - `dwarf_flows_archived`, `dwarf_flows_reaped` - take their
  descriptions from one constant each (`job.go`): two instruments of one name with
  different descriptions are exported as two streams of that name.
- **The recheck under the root lock is the authority, not the scan:** a root selected and sized is moved only if it is
  still a closed root due, unstamped, and with no archive attempt when locked (`TestMove_TheLockRechecksEveryRoot`); a
  stamped one only if still stamped and due (`TestDelete_TheLockRechecksTheStamp`).
- **The position's close time is normalized** (`database.StoredTime`) before it is bound back: a MySQL driver with a
  DSN `loc` other than UTC places the stored wall clock in that location. The fault and checkpoint names are
  `internal/diagnostics`' (`FaultReap*`, `CheckpointReap*`).

## The move (`move.go`)

A batch of trees leaves the live tables in one transaction: moved into a day's archive tables, or deleted.

- **On SQL Server, MySQL and MariaDB a tree larger than a batch is staged, never moved whole**: the engine sets
  their staging threshold to the batch (`addJanitorJobs`). Moved in one transaction, such a tree locks the live
  tables against every dispatch until the move commits. On SQL Server a statement that takes ~5,000 row locks on a
  table escalates to an `X` lock on the whole table, and the move deletes a tree's steps in one statement and its
  flows in another: a tree of 9,216 steps escalated on `dwarf_steps`, one of 8,193 flows on `dwarf_flows` as well. On
  MySQL and MariaDB, with the tree a fifth of `dwarf_flows`, the planner scans the whole table for the copy and the
  delete, and at REPEATABLE READ locks every row and gap of it. Measured with a tree of 4,097 flows and 9,216 steps
  among 16,384 open flows (`TestRemoval_ALargeTreeLeavesTheTablesUnlocked`: the first transaction deleting the tree's
  live rows is held open after its deletes, at `CheckpointReapDeleted`, while a flow runs beside it; it fails on all
  three with the threshold left at 262,144). **Do not fix SQL Server by disabling escalation** (`LOCK_ESCALATION = DISABLE`): the move's
  delete then takes page locks, and a neighbour's insert waits on one; forcing row locks as well (`WITH (ROWLOCK)`)
  passes, but a tree at the 262,144-row threshold then holds a lock per row and index entry, on the order of a
  million. **Keep a statement under about 5,000 rows a table there**: the batch (4,000 rows), a chunk and Purge's
  marks (2,000 roots) do; do not raise the batch past it without rerunning the test on those dialects. A batch of
  small trees is still one move, and on MySQL a batch that is a large share of a small live table can be scanned the
  same way, for that short transaction (not measured). PostgreSQL locks only the rows it deletes and moves a tree up
  to the staging threshold whole.
- **The root lock is two-sided.** Every transaction that removes a tree, live or archived, locks its root first with
  the strength a `DELETE` takes (`lockRoots`: `FOR UPDATE`, or the `touch` flip), skipping a held root on PostgreSQL
  and MySQL. The engine's Fork locks its source root `FOR NO KEY UPDATE`, which conflicts with it, so a Fork and a
  removal of one tree serialize. Either side weakening its lock breaks the other; the engine's `lockRows` names this
  one.
- **On SQL Server every janitor statement that locks, updates or deletes rows by their ids reaches them by a forced
  seek** (`lockRoots`, `deleteSQL`, `updateSQL`), so a transaction locks only its own trees' rows. On a small table
  SQL Server plans a clustered-index scan for `flow_id IN (...)` and `root_flow_id IN (...)`. That scan takes an update
  lock on every row it passes, rows it does not change included, even under `READ_COMMITTED_SNAPSHOT`, which versions
  reads but not a modification's read of its own target. Two janitor transactions, each holding its roots and
  scanning past the other's, deadlock. Every retry loses too, because the move dies in its first statement and SQL
  Server kills the transaction cheapest to roll back. Without the hints, `TestDispatcherDrop_TheJanitorsKeepEveryFlow`'s
  shutdown arm failed on these deadlocks.
  `FORCESEEK` is refused on an `UPDATE` or `DELETE` target (error 10724), so the target is aliased in a `FROM` clause.
  The hint names its index because a bare `FORCESEEK` may pick another one whose range covers other trees' rows:
  - `lockRoots`' due conditions match the status index, whose range spans every closed root.
  - Large archiving's `day<>N` matches the directory's day index.

  A day table's indexes are named for the table (`idx_<table>_root`, `idx_<table>_flow_id`).
  `TestMove_PassesARootHeldOutsideTheBatch` pins it.
- **The janitor makes no table.** It archives into today only while the archive's record holds tomorrow whole; a
  missing tomorrow holds closed trees live while the deletions go on (`TestMove_NoClosedTreeMovesWhileTomorrowIsNotWhole`).
- **`archived_at` is the move's time, clamped to its filing day's last millisecond** (`archivedAtSQL`), so a day holds
  only rows archived on it. A quiet margin before the day's end (`quietMargin`: a minute, or an eighth of a short day)
  stops archive moves only; a test turns it off.
- **Both tables move by name.** `archivedFlowColumns` names every flow column the day tables have, `archived_at`
  apart, so not the live-only `archive_attempt_at`; `archivedStepColumns` names every step column, in the move and
  in the staged copy alike. **Do not copy a row by position** (`RETURNING *` into `SELECT *`): PostgreSQL, SQL Server
  and SQLite add a column to an existing table at its end, so an upgraded live table and a day made with the column in
  place disagree on order - the copy fails on PostgreSQL and SQL Server, and on SQLite writes values into the wrong
  columns. Pinned by `TestMove_NamesEveryColumn` (each list against its tables) and `TestMove_StepsAreCopiedByName`
  (a day's column moved to the end, a tree moved whole and one staged). On PostgreSQL each table moves in one `DELETE ... RETURNING` into an `INSERT`, so
  what is archived is exactly what is deleted; elsewhere it is copied, then deleted, and a straggler is archived as it
  was at the copy.
- **The directory rows for a late or early filing are written in the move's transaction**, from the same rows;
  `DirectoryHours` decides which (`TestMove_DirectoryNamesOnlyWhatTheLocateCannotFind`). Directory, flows and steps go
  together: a lost response after a commit may undercount a metric, but a retry finds the root gone, and no partial
  tree is ever published.
- **An archive failure never becomes permission to delete.** At a positive retention a tree archiving fails on stays
  live until live retention gives it up, past its retention (above); a late successful move still takes its retention
  from its filing day.
- **`MoveTrees`** is the move into a day of the caller's choosing, for an engine test that files a tree there.

## Fast deletion (`fastdeletion.go`)

The fast deletion jobs (`JobFastDeletion`, one a share): the trees stamped for deletion - a Delete's, a
DeleteOnCompletion's once its grace has run, a published tree's live copy - deleted whole as their stamps come due, one
batch a round, by the same transaction as an archive move with no day (`move`, the stamp rechecked under the root lock).
Each walks the stamped roots of its share by `flow_id` through `idx_dwarf_flows_delete_after`, from a position it moves
past every root a round considered, and starts over when its residue class changes, as fast archiving does.

- **A stamped tree is deleted whole, whatever its descendants' status - deliberately; do not reguard on it.** Only a
  closed root is ever stamped (the engine refuses to stamp an open one), so the only open descendant a stamped tree can
  hold is an orphaned child - an open child whose parent already terminalized, which only a bug leaves, and which the
  engine's wedge sweep would terminate anyway (`recoverOrphanedSubgraphChildren`). Deleting it is safe: a worker
  mid-dispatch on it no-ops through the lease fence (its claim and writes match zero rows once the tree is gone), so
  nothing strands and nothing is corrupted; whichever of the deletion and the wedge sweep reaches the orphan first
  wins. The same holds for chunked deletion.
- **A stamped tree too large for a batch is passed**, stamped and whole, to sweep deletion, which deletes it in chunks;
  one transaction deleting a tree grows with the tree until it cannot finish (`TestDelete_ALargeStampedTreeIsPassed`).
  It is behind the position once passed, so it waits for the sweep's window like any root left there.
- **By `flow_id`, any stamp that comes due behind the position waits for sweep deletion's window**, and more do than the
  old-flow case
  suggests: a Delete of an old flow; a DeleteOnCompletion flow still in its grace when a newer flow's stamp comes due
  (an operator Delete of a fresh flow passes every such flow below it); with mixed durations, every
  DeleteOnCompletion flow outlived by one created after it. Every read hides a stamped tree the moment it is stamped,
  so the wait costs storage, not correctness. Ordering by due time instead would need an index on it.
- **`dwarf_flows_reaped{reason="deleted"}` counts only trees never archived**: a published tree's live copy, stamped
  with its attempt or carrying the archive time its publication copied, is counted at the move's root locks and left
  out (`publishedSQL`, `TestDelete_APublishedLiveCopyIsNotCountedReaped`).

## The sweeps (`sweep.go`)

The sweep jobs: sweep archiving or sweep deletion behind each fast job, and live retention, stamping the trees past
their retention. They share one account of their sweeps (`sweep`: whether one is under way, the size of its next batch, the
held count), one gauge under the job's `kind` (`closed`, `deleted`, `retention`; `sweepGauge`) and name, and one round
(`sweepRound`); each job owns its population, its position and its routing.

- **A failed batch is taken again a root at a time**, from the same position; a root that fails alone is held -
  counted and passed - and the next sweep comes back to it. Only the first `heldLogLimit` (8) a sweep holds are logged,
  by correlation id: through an outage a sweep holds every root it takes, up to 8 a second. A batch that fails nothing
  doubles the next, up to the batch. A round takes batches until one makes progress or `sweepFailureLimit` (8) have
  failed, then gives way to the idle wait.
- **Passing a root is no progress** - one another transaction holds, one not the job's - though the round goes on past
  it: a root that stays behind a fast job's position is passed by every sweep, and counting it would keep the job from
  ever idling and a test's drain from ever returning (`TestSweep_PassingAStuckAttemptIsNoProgress`).
- **The held count is the larger of the current sweep's and the last complete one's**, published as each root is held
  (`TestSweep_HeldRootsAreVisibleBeforeTheSweepCompletes`); a sweep dropped before it completes - its fast job's
  residue class changed - keeps its count.
- **Sweep deletion deletes a large tree one at a time**, a chunk a batch, until it is gone, no longer due, held by another
  transaction, or a chunk fails - held, as a root that fails alone is - and only then the next root (user): with the
  trees' chunks interleaved, each tree's deletion would take longer the more were in flight, all of them holding their
  rows meanwhile (`TestSweepDeletion_ALargeTreeIsFinishedBeforeTheNext`). A chunk that does not finish its tree is
  progress, even one that deleted only part of a flow's steps.

## Sweep archiving (`sweeparchiving.go`)

The job behind each fast archiving job (`JobSweepArchiving`): the closed roots of its share fast archiving left - one it
failed on, found held or could not start an attempt for, one whose close committed late with a time behind its position,
one of a slow peer's class it passed. Routed as fast archiving routes them, through the same functions (`closed.go`):
moved, a tree over the staging threshold given its attempt, and at a retention of 0 deleted or stamped. **When
archiving, a root with an attempt is not in its scan**: it is large-tree archiving's, its copy can run for a day before
it publishes, and every sweep would pass it again, a batch at a time.

- **The boundary is fast archiving's position**, published at the end of every round with the residue class and the
  share its scan was restricted to (`boundary`, read lock-free). A sweep captures it as it begins and takes the roots at
  or behind it in close order: the root fast archiving was last done with is the sweep's, failed or not
  (`TestSweep_TheLastRootFastFailedOnIsTheSweeps`). **The end is fixed for the sweep**, so roots that close while it
  runs are ahead of it and a sweep completes however fast they arrive (`TestSweep_CompletesUnderContinuousArrivals`).
  Before fast archiving's first round there is no boundary and no sweep begins; a position at the start has nothing
  behind it, and a sweep completes at once, empty.
- **A root waits out the window** (`SetOverdue`, `DefaultSweepOverdue`, 30 s) past the archive grace before a sweep
  takes it. Fast archiving never comes back for a root behind its position - a starting-over after a class change or a
  restart leaves the sweep nothing behind it - so the window only gives what failed a root (a Fork holding it,
  contention, a connection lost) time to clear before it is tried again. It is the steal grace, so a slow peer's roots
  behind the position are taken about a minute past due. Overdue is not "since fast archiving tried it": an old root
  fast archiving just failed on can be retried at once (`TestSweep_ARootWaitsOutTheWindow`). The failed root is the
  shard's oldest due root until the sweep takes it, so a single failure shows in `dwarf_archive_lag_seconds` for the
  window and the sweep's idle wait.
- **The residue class is the boundary's, not read from the shard.** A sweep sweeps the share and the class fast
  archiving's position was taken in, plus the share's roots overdue by the steal grace past its window, and is dropped
  when fast archiving's class changes: fast archiving then starts over, and the next sweep has the new class's position
  to sweep behind (`TestSweep_FollowsFastArchivingsResidueClass`). Read from the shard, a sweep could pair one class
  with a position taken in another. A dropped sweep's held roots still count, and, not having completed, it ends no
  failure episode.
- **A failed batch is retried a root at a time, as every sweep does** (`TestSweep_APoisonTreeDoesNotLoseItsBatchMates`,
  `TestSweep_PassesFailedTrees`).
- **Its gauges are its own**, observed only while its janitor is active: the held count under `kind="closed"` and its
  name, and its series of `dwarf_archive_failing_seconds`.
- **Its failure episode** begins at a failure - the day, a scan, a sizing, a move - and ends at a sweep that completes
  having failed nothing, in a batch whose day was read and open: a sweep starting after the failure and finding every
  root behind fast archiving's position taken (`TestSweep_HeldRootsAndTheFailureEpisode`). At a retention of 0 nothing
  is archived and nothing starts one.
- **It moves nothing while the day is closed** - tomorrow not whole, or the quiet margin - as fast archiving: the roots
  stay live and the sweep is failing (`TestSweep_NothingMovesWhileTomorrowIsNotWhole`). Ending its episode there is
  barred by the open-day condition above, or every round would end it and the next begin it again.

## Sweep deletion (`sweepdeletion.go`)

The job behind each fast deletion job (`JobSweepDeletion`): the stamped roots of its share fast deletion left - a tree
too large for a batch, one it failed on or found held, one whose stamp came due behind its position (a
DeleteOnCompletion tree still in its grace when passed), one of a slow peer's class.

- **The boundary is fast deletion's position** (a `flow_id`) with its residue class and share, published at the end of
  every round as fast archiving's is. A sweep captures it as it begins and takes the stamped roots at or behind it in
  `flow_id` order, overdue by its window past their stamp's due time, in the class it captured plus every root overdue
  by the steal grace past the window; a class change drops the sweep (`TestSweepDeletion_TheEndIsFixed`,
  `TestSweepDeletion_TakesWhatFastDeletionPassed`, `TestSweepDeletion_FollowsFastDeletionsResidueClass`).
- **A tree a batch holds is deleted with its batch** (`move`, the stamp rechecked under the root lock); a larger one in
  chunks, one tree at a time (above). A failed chunk is held. No failure episode: deleting is not archiving.
- **`dwarf_flows_reaped{reason="deleted"}` counts each chunk as it commits**, so a later chunk's failure loses none of
  it, and only trees never archived (`TestChunk_CommittedChunksAreCountedWhenALaterOneFails`). Its held count is
  under `kind="deleted"` (`TestSweepDeletion_TheGauges`).

## Live retention (`liveretention.go`)

The job that gives up on the closed trees archiving never moved once their retention has passed (`JobLiveRetention`):
archiving failing for the whole retention keeps them live, and nothing else would remove them. Its name sets it apart
from the archive's day tables' retention, a job of its own.

- **It only stamps** (user): each tree past its retention is stamped for deletion, due at once, as a Delete does, and
  fast or sweep deletion deletes it - an old root, behind fast deletion's position, is sweep deletion's once its window
  passes, chunked if large. One deletion path, and a stamp is one small statement a batch of roots, so the job holds
  no tree in progress and needs no sizing.
- **A tree is past its retention once its filing day ended `retentionDays` before today** - its close plus the archive
  grace, on the database clock and the archive's day length - computed as day arithmetic, never a duration, so a huge
  retention cannot overflow (`TestLiveRetention_TheCutoffAndAHugeRetention`). **The cutoff is rechecked under the root
  lock** (`dueSQL`): an archive failure never becomes permission to delete sooner
  (`TestLiveRetention_CountsFromTheCloseAndTheGrace`, `TestLiveRetention_LeavesTreesWithinTheirRetention`). The archive
  is used for its days only; no table is needed.
- **Each sweep walks the closed roots before the cutoff in close order, from the start**, with no window and no
  boundary: no fast job precedes it. **No residue class**: every root past its retention is overdue by the steal
  grace, so a class would restrict nothing; replicas meeting on a root skip it on PostgreSQL and MySQL, and a stamp is
  short (user: a rare event, not worth a class).
- **The stamp sets `updated_at` to now**, so the tree's wait for deletion - sweep deletion's window - counts from when
  it was given up, not from its close days ago, which would make a tree given up just now overdue at once.
- **`dwarf_flows_reaped{reason="archive_failed"}` counts at the stamp** - the flows of each stamped tree, counted by
  root before the transaction so no root is held through a read of whole trees (a closed tree gains no flow; one that
  leaves the live tables in between is not locked, so not counted): the records given up, which must stay zero. Their deletion counts them again under `deleted`, as a
  retention-0 tree's is, so the two reasons overlap for these trees (user;
  `TestLiveRetention_StampsAndTheDeletionJobsDelete`). At a retention of 0 the job does nothing: closed trees are
  deleted as they come due, not retained.

## The archive deletion sweep (`deletemarked.go`)

The job that deletes the archived trees marked for deletion (`JobArchiveDeletionSweep`, type `DeleteMarked`): **every** archived tree a Delete or a Purge removes - the calls only mark it, a statement each
(user), and the published copy a Delete takes back from a tree whose live copy is being deleted.

- **Each sweep goes through the retained days, oldest first, a probe a day** (`markedIn`, on the day's deletion index:
  partial on PostgreSQL, SQLite and SQL Server, named on MySQL and MariaDB, with a fallback read for a day made before
  the index), and stops at the first day holding marked roots. Not a `UNION` over the days: a probe a day touches one
  day's table at a time, and a sweep with work reads few days (user). A day whose tables are gone is passed, its
  trees gone with it; a day whose probe fails is counted unknown. The sweep starts from the oldest day each time:
  with no budget, nothing needs to resume past a slow day.
- **The trees a batch holds are deleted together** (`deleteMarkedTrees`: the roots locked, a held one skipped on
  PostgreSQL and MySQL, then the directory's rows for their flows, their steps and their flows, in one transaction); a
  larger one in chunks, one tree at a time (`deleteChunk` on the day's tables). A failed batch is taken again a root at
  a time; a root that fails alone is held and passed (`TestDeleteMarked_SmallTreesGoTogether`,
  `TestDeleteMarked_ALargeTreeInChunksOneAtATime`, `TestDeleteMarked_AFailingTreeIsPendingAndTheRestGo`).
- **Its gauge:** `dwarf_archive_deletions_pending`, the marked roots a sweep found and could not delete - failing, or
  held by another transaction (`stillMarked` tells a held root from one another replica deleted). A day whose probe
  fails is logged, its trees not counted. It reads the larger of the sweep under way's count and the last complete
  one's, published as a tree fails rather than when the sweep completes: with 8 failures a
  round and a 30 s wait between rounds, a sweep through a failing backlog runs for hours
  (`TestDeleteMarked_FailuresShowBeforeTheSweepCompletes`, `TestDeleteMarked_AHeldTreeIsPending`,
  `TestDeleteMarked_ADroppedDayIsNoErrorAndAnUnreadableOneLogged`). A held tree's log names it by its correlation id,
  which needs the root's `created_hour`: the probe reads it. Nothing reads until a sweep has completed - zero would
  claim every day swept - but a failure as soon as it is met. A root another replica's sweep holds mid-deletion
  counts as pending until this replica's next sweep finds it gone. Since every
  archived deletion goes through the sweep, a pending count is normal for a moment after a Purge; one that stays is
  the signal.
- **A day whose deletions all fail holds the days after it back**: each failing root costs a failed batch, and a round
  gives way after eight, so such a day passes about seven roots a round. Accepted, as every sweep through a failing
  backlog is slow: the gauges show it, and a day failing whole is an operator's problem first.
- **On MySQL and MariaDB the probe seeks on `delete_after_ms=1`**: their deletion index is the full
  `(delete_after_ms)`, so an equality - every mark is 1 - with the primary key InnoDB appends is a seek in `flow_id`
  order, where a range read and sorted every mark of the day for each batch.
- **No residue class**, as live retention: replicas meeting on a root skip it on PostgreSQL and MySQL. It waits 30 s
  after a round without progress, as every repair job.

## Archive retention (`archiveretention.go`)

The job that keeps a shard's archive days (`JobArchiveRetention`): each round is one pass of the archiver's `Maintain` -
the days ahead made, then the days past the retention dropped with their directory rows - paced by its idle wait, the
pass interval (`PassInterval`): hourly for a day of a day or longer, a twenty-fourth of a shorter one, at least a second
(user: an hour). Its name sets it apart from live retention.

- **A pass that saw a new day's tables whole, or dropped a day, made progress** (`Maintain`'s `newDay` and its count);
  the round after, at once, finds neither and the wait follows. So its progress age reads how long since its days last
  moved - up to a day and a pass interval while it works - rather than growing from the resume, which a job that never
  reported progress would show, healthy or blocked alike.
- **The engine adds it first**, so a drain makes the days before fast archiving looks for tomorrow
  (`engine/CLAUDE.md`). A test's `SetIdle` replaces its pass interval too: with the engine's 10 ms it runs a hundred
  passes a second, each a clock read, a catalog read and a directory select under a turn - test-only, and accepted.
- **It keeps making the days ahead, not only dropping old ones**: archiving into today waits for tomorrow's tables to
  be whole, so a job that only dropped would stop archiving within two days.
- **A pass is one batch**: the archiver's migrations and drops run on the shard's DB under the batch's turn
  (`TestArchiveRetention_WaitsForATurn`).
- **The first pass runs as the janitor resumes**, then every pass interval: Startup makes days but drops none, so a
  fleet whose replicas live shorter than an interval would otherwise never drop one.
- **A pass holds its turn across sequel's polls for a peer's migration lock** and, off PostgreSQL, across a drop's
  unbounded lock wait: one turn an hour.
- **Its counter:** `dwarf_archive_maintain_failed`, a failed pass, not a stopped one
  (`TestArchiveRetention_KeepsTheDays` on days of 48 hours, which the rig never made, so a job that only dropped
  fails it; `TestArchiveRetention_AFailedPassIsCountedAndAStopIsNot`). The engine's
  `CheckpointArchivePassDone` is its catalog refresh's; a test waiting for a maintenance pass waits for
  `CheckpointArchiveMaintained`.

## Expiration (`expiration.go`)

The job that ends the flows still open past the flow expiration (`JobExpiration`), added only while the expiration is
on. **The pass is the engine's** (`ExpirePass`, the engine's `expirePass`): ending a tree is the engine's termination,
the one Terminate uses, and the job holds no copy of it. What the job owns is the position - the `flow_id` the next
pass goes on after, forgotten at a resume - and the pace; the counters (`dwarf_flows_expired`,
`dwarf_flows_expire_failed`) stay the pass's. The walk, the budget and the mark are in `engine/CLAUDE.md`.

- **Every hour, or every half of the expiration when that is shorter, and at least a minute apart** (user): a flow
  expires up to that long after it is due. Expiring a flow within seconds makes little sense, and the setter accepts a
  millisecond, which would otherwise poll a shard every half of one (`TestExpiration_ThePace`).
- **Its progress is the pass's**: ended a root, or stopped at its budget with more to read. A pass stopped at its
  budget is followed at once by the next, on a turn of its own, so a backlog drains a budget at a time without holding
  one turn. **But a sweep from the oldest starts only an idle wait past the last one's end** (`swept`, forgotten at a
  resume), whatever that one ended: a sweep that ended a root would otherwise start the next at once, so a steady
  trickle of expiring flows - one per sweep, a sweep lengthened by an archive outage's closed backlog - would keep
  every dispatcher of the shard sweeping back to back, and a drain would never return
  (`TestExpiration_TheRoundReportsThePassProgress`).
- **A pass is one batch**, as archive retention's: under its turn, each tree ends in a transaction of its own on the
  shard's DB (`TestExpiration_ThePassRunsUnderATurn`).
- **No residue class**: every dispatcher of a shard walks the same rows, and the later of two on one root finds it
  ended and does nothing.
- **What it ends is marked for deletion**, and the deletion jobs delete it: mostly sweep deletion, within its window,
  since an expired root is old and so behind fast deletion's position once a newer root has been stamped.

## Recovery (`recovery.go`)

The jobs that run the engine's recovery repairs (`NewRecovery`): `lease_recovery`, `orphan_repair` and `wedge_sweep`,
each running the engine's pass for one repair (`RecoveryPass`) as expiration runs its own. What each repair recovers,
and why each is a job apart, is the engine's (`engine/CLAUDE.md`, "Background Recovery"); what the job owns is the
pace.

- **A pass every interval, counted from the last one's end.** A round runs a pass unless the last ended less than an
  interval ago, and the idle wait is the interval, so the next pass comes an interval after a completed one and after a
  failed one alike. A resume runs one at once - what a replica restarting after a crash needs, since its own expired
  leases are lease recovery's (`TestRecovery_ThePace`).
- **A completed pass is progress, and nothing else is** (user): a healthy shard has nothing to repair, so progress as
  "repaired something" would leave the progress age growing from the resume, healthy or blocked alike. With a completed
  pass the age reads up to an interval and a pass while the job is well, and grows past that when it is blocked or
  failing (`TestRecovery_AFailedPassIsNoProgress`). The pace makes the round after a completed pass, which progress
  runs at once, a no-op, so progress never runs passes back to back.
- **A drain runs a pass once** (`TestRecovery_ADrainRunsOnePass`) - which, being progress, takes the drain round every
  job once more.
- **A pass is one batch**, under one common turn (`TestRecovery_ThePassRunsUnderATurn`), its statements on the shard's
  DB. The pass logs its own failures, so the job adds none.
- **No residue class**: every dispatcher of a shard runs every repair, each idempotent.

## Chunked deletion (`chunk.go`)

A tree larger than a batch is deleted over several transactions (`deleteChunk`), each about a batch, under its root's
lock, each rechecking that the root is marked - stamped and due in the live tables, marked in a day's (`markedSQL`),
the directory's rows for each flow deleted with it there. **Its SQL, and the sizing's and the root lock's, takes a
table suffix** (`""` for the live tables, `daySuffix(day)` for a day's), never a table name (user). The mark is the
authorization, durable, so a chunked deletion
needs nothing in memory to finish (`TestChunk_ARestartResumesTheDeletion`) and two replicas on one tree serialize per
chunk (`TestChunk_TwoJanitorsShareATree`). A deletion that grows with its tree eventually cannot finish in one
transaction, and then never finishes: the delete wall.

- **Descendants first, newest first, each flow's steps then the flow; the root's steps; then the root, last.**
  Children are read 256 at a time (`treeChunk`), from the chunk's position (`chunkPos`), so a read seeks past the
  entries earlier chunks deleted, which stay in the indexes until vacuum. The position also holds the last step deleted
  of a flow part deleted, so the next chunk seeks past that flow's deleted steps: read from the start, a flow of a
  million steps - one `forEach` flow is one - would cost each chunk every step deleted before it (measured on
  PostgreSQL before vacuum, 112 ms against 42 ms for the last chunk of a 100,000-step child). The position is kept by
  the job deleting the tree and lost with it; a lost position costs a reread from the newest descendant, not a row.
- **Every read in a chunk is bounded by its budget, never by the tree.** Which children fit is decided by sizing at
  most the budget's worth of their steps (`stepsSizeUpTo`, a `LIMIT` or `TOP` under a `COUNT` and a sum of their byte
  counts) with the children's own bytes, halving the run of children until it fits; a child larger than what is left
  has a budget's worth of its steps deleted. **Do not size the children with a full `COUNT ... GROUP BY`**: it recounts
  a large child's remaining steps on every chunk, quadratic over the tree (measured 279 ms a chunk on PostgreSQL at
  4,000 steps a child). Step ids are read in `step_id` order from `idx_dwarf_steps_flow_id` and deleted by bound
  lists (`database.IDList`, two at the default budget of 4,000): a range or an ordered `DELETE` with a limit is not
  portable, a range is not contiguous in a flow's steps anyway, and the read stays bounded by the flow's own steps
  only with that index keyed on `(flow_id, step_id)`. **On PostgreSQL every chunk transaction turns bitmap scans off
  first** (`boundReads`, deleting and copying alike): with no statistics on a large flow - written since the last
  ANALYZE - the planner reads its steps by a bitmap scan of the flow index and sorts them, every remaining step of the
  flow on every chunk; without the bitmap scan it walks the index in order and stops at the limit. The 1% ANALYZE
  trigger on `dwarf_steps` narrows that window and cannot close it, so do not rely on statistics for the bound
  (`TestChunk_APopulatedStepReadStopsAtItsBudget`, a populated table with none; `TestChunk_TheStepBoundaryWalksTheFlowsIndex`,
  the plan at the production budget on every dialect but SQL Server). **The same statement forces a custom plan**
  (`plan_cache_mode`): every chunk statement is bound and prepared once per connection, and PostgreSQL may move a
  prepared statement to a generic plan after five runs, planned without the values and not replanned when the
  transaction turns bitmap scans off.
- **The descendant walks need the tree's flow-id order**: PostgreSQL's live and archive root indexes are keyed on
  `(root_flow_id, flow_id)`. A root-only key makes a 256-row page read and sort the whole tree when statistics are
  missing, even with bitmap scans disabled. The other dialects' root indexes already carry the primary key in order.
  `TestChunk_DescendantReadsWalkTheTreeIndex` covers both directions, live and archived, with and without statistics.
- **The sizing reads are bounded the same way** (`cutTrees` runs every caller's sizing through `sizingReads`, a
  transaction of its own on PostgreSQL with `boundReads`' settings). Left to the planner, a flow's step read in the
  sizing statement is a bitmap scan of the flow's whole index whatever its limit - without statistics, and with
  statistics once a flow is estimated at a few hundred rows or more, as a custom plan or a cached generic one (measured
  on a flow of 1M steps sized up to 8,193 rows: 47 ms against 2 ms). **Do not order the step read instead**
  (`ORDER BY step_id` in the LATERAL): with no statistics the planner sorts the whole flow (measured up to 1 s). The
  other dialects' step read stops at its limit by its plan. Pinned by `TestSize_TheSizingReadStopsAtItsLimit`, with
  and without statistics, custom and cached generic.
- **A budget is rows and bytes, and a chunk stops at whichever it reaches first** (`limit`, `size.go`), the byte counts
  read from the `*_bytes` columns, so no payload is read to size it. **A chunk takes at least one step only while it
  has taken nothing** (`stepsWithin`'s `fresh`): a step over the whole budget then goes alone, and a chunk that already
  took rows ends rather than overshoot its bytes. **A flow row is charged its own bytes before it is taken**
  (`rowFits`, `flowBytes`): it goes with its last steps only when both fit, and otherwise first in the next chunk
  (`TestChunk_AFlowRowIsChargedItsBytes`, `TestChunk_AChunkKeepsToItsBytes`).
- **The root goes only after a recheck that the tree holds nothing else:** two probes, the first remaining descendant
  and the first root step, not an `EXISTS` over the tree, which MariaDB materializes over the whole steps index
  (275 ms on a 100,000-child tree). On MySQL and MariaDB the probes are locking reads, since a plain read there sees the
  snapshot the chunk's first plain read took. A closed tree gains no flow, so this guards against a bug, not a path
  (`TestChunk_ALateDescendantKeepsTheRoot`).
- **Readers never see part of a tree**: the engine's reads reject a tree whose root is marked
  (`engine/CLAUDE.md`, "Reading an archived flow").

## Large-tree archiving (`largearchiving.go`)

The third job (`JobLargeArchiving`): every tree whose staged archive attempt has started - one too large to move in
one transaction, its attempt started by fast archiving or by sweep archiving - copied into its attempt's day
over as many transactions as it takes, invisible there, then published in one small one: the root last, the live root
stamped for deletion with its attempt kept. The protocol:

- **The attempt.** `claimAttempt`, under the live root's lock while the root is still due, joins the attempt T in
  `archive_attempt_at`, or starts one at `NOW_UTC()` - only while the day is startable (before the quiet margin,
  tomorrow whole) and only when T falls in the filing day, so the stamp groups have room. **T is the attempt's
  identity and its day** (`DayOf(T)`), so two copiers on one T write the same rows, and a copier holding a replaced T
  fails its next lock. A root with an attempt goes the staged way whatever its size, unsized
  (`TestStage_AnAttemptedRootIsStagedWhateverItsSize`), and the atomic move locks only roots with none, so a selection
  made before a claim cannot move the tree into a day that holds staged rows. When staging, a batch holds no more than
  the staging threshold, rows and bytes, so a tree over it is alone and staged (`TestStage_EveryTreeOverTheThresholdIsStaged`).
- **Every chunk re-locks the root and rechecks it: closed, unstamped, its attempt still T** (`stageChunk`). It copies
  descendants in `flow_id` order, each flow's steps and then its flow row and directory row, bounded by the budget as a
  chunked deletion is. Each write inserts only what is not there (`NOT EXISTS` by the stable id), so a rerun writes the
  same rows (`TestStage_RestartInsideLargeFlow`). A tree holding a flow that is not closed is refused: only a bug leaves
  one, and its copy would not be settled.
- **Archive times are stamp groups, never the commit's time:** descendant i (in `flow_id` order) is stamped
  `T + i/4096` ms (`stageGroup`), the root `T + G` with `G = ceil(N/4096)`. A flow's time is a function of the tree
  alone, and no staged tree puts more than 4,096 flows on one time. A stamp past the day's end is an error that holds
  the tree: a clamp would put its flows on one time. The lists show a staged tree under its attempt's time.
- **Publication is the last chunk:** the root's last steps, its flow row and directory row, and the live root stamped
  `delete_after_ms=1` with T kept and its `archived_at` copied from the archive root, in one transaction. Stamped with T
  means published, so a lost response is retried as nothing to stage (`TestStage_APublishedTreeIsNotCopiedAgain`).
  **It waits for the day after the attempt's to be whole**, since a locate finds a flow only where the day after its
  own is. **A waiting copy first runs a statement on its own day**: a copy whose every row is in writes nothing else
  there, so without it a dropped day would never fail a statement and the copy would wait forever
  (`TestStage_ADroppedDayIsReplacedWhileWaitingToPublish`).
- **A dropped day replaces the attempt.** A chunk's statement on a dropped `DayOf(T)` ends its transaction, so a fresh
  one, still under the root lock and only while T is unchanged, starts a new attempt in today; the old rows went with
  their day (`TestStage_ADroppedDayIsReplaced`, `TestStage_AHalfDroppedDayIsReplaced`, the latter the reason the
  archiver drops a day's flows table first).
- **A Delete during staging** stamps the root and clears T, so the copier's next lock fails and the rows it copied stay
  in the day, invisible, until the day drops (`TestStage_ADeleteAbandonsTheCopy`; the engine's Delete).

- **It finds its trees by their attempt, through `idx_dwarf_flows_attempt`**, never by walking the due roots: one
  `LIMIT 1` after its position in `flow_id` order, wrapping at the end, so an attempt behind any number of roots without
  one is reached, and a closed day - which stops new attempts - stops no copy already started
  (`TestStage_AStartedAttemptBehindABatchIsReached`, `TestStage_AnAttemptGoesOnWhenNoneMayStart`). **Its queries inline
  the attempt predicate exactly as the index is partial on** (`attemptedSQL`); bound, PostgreSQL cannot use the index.
- **One tree at a time**, by `flow_id`, a chunk a batch, until it publishes, fails, is found held or waits for its
  day's next day; then the next, and the position wraps at the end (`TestStage_ATreeIsFinishedBeforeTheNext`). **Do
  not interleave the trees' chunks**: each tree's time to publication then grows with the trees in flight, every one
  holding its live rows and its invisible copy until it publishes, and while large trees keep arriving none finishes.
  The cost, accepted: one huge tree holds the large trees behind it - fast archiving is not held. A tree that fails
  gives way at once and is taken again when the position comes round to it (`TestStage_AFailedChunkIsRetried`).
  **A round goes on to the next tree until a chunk makes progress or a lap makes none**, each tree once a round, and
  only then does the job wait: idling after any tree without progress would let one stuck tree hold every other to a
  chunk a second (`TestStage_AStuckTreeDoesNotHoldTheOthers`). A failed chunk is logged.
- **At a retention of 0 it copies nothing**, since nothing is archived there: a tree whose attempt an earlier
  run started is fast archiving's to delete, which it does attempt or not (its move fences on the attempt only when
  archiving, and its stamp clears it). Copying it as well would race the deletion and could publish a tree into an
  archive that keeps nothing (`TestStage_RetentionZeroArchivesNothing`).
- **No residue class.** A chunk locks its root, skipping a held one on PostgreSQL and MySQL, and writes only what is
  not there, so two replicas on one tree serialize per chunk and write the same rows (`TestStage_ConcurrentCopiers`).
  Dividing the attempts between replicas would only leave a tree waiting for its replica.
- **Shares, by `flow_id` alone** (`SetShare`): the job copies the attempts of its share, `flow_id % of = k-1`, in its
  scan and its probe. Every replica runs every share, so no tree waits for a particular replica, and each job still
  copies one tree at a time: two jobs copy two trees at once on one replica - a shard has at most two dispatchers, so
  the jobs, not the replicas, are what let one replica alone keep up. With both replicas, PostgreSQL and MySQL copy up
  to four (a held tree is skipped), SQL Server and SQLite two (it is waited for). Pinned by
  `TestShare_EachLargeArchivingJobTakesItsShare`.
- **Each copy's position is in memory**, at most `maxStages` (64) of them. A position advances only when a chunk
  commits, so it is kept on every path where one did - a copy waiting to publish, a lost response - and dropped only
  once the tree is published or gone. A position forgotten - at the bound, a restart - is read back from what the
  attempt's day already holds, a count over the copied descendants. **A remembered position whose chunk copied nothing
  is stale** - another replica copied past it - and the copy is read back from the day and goes on in the same batch,
  rather than re-read a chunk's worth of rows under the root's lock each turn (`TestStage_TheCopiedRowsAreCounted`). **Do not bound
  the positions by recency**: the trees are visited in turn, which is the access pattern an LRU serves worst - past the
  bound it evicts the next tree to visit, every time. The job drops an arbitrary one.
- **`dwarf_archive_oldest_attempt_seconds` is the job's own**, labelled by job, a probe of the oldest unpublished
  attempt of its share at most once a second while busy and in every round that finds nothing, observed only while the
  janitor is active; a shard's reading is the max of its jobs'. It goes on
  through a closed day (`TestStage_OldestAttemptDuringDayOutage`). **The reading goes on aging after its probe** (the
  probe records when the attempt began, not its age): a chunk that blocks stops the probes too, and a reading frozen at
  the last one would hide exactly the stuck copy the gauge is for (`TestStage_TheOldestUnpublishedAttemptIsObserved`). `dwarf_archive_staged_rows` counts each chunk's
  inserted rows, so a rewrite after a forgotten position adds nothing (`TestStage_TheCopiedRowsAreCounted`).
- **Its failures are an episode of its own** - `dwarf_archive_failing_seconds{job="large_archiving_1"}`, beside fast
  archiving's series; each job owns its series, and a reader aggregates with max. It begins at a failed chunk or a
  failed replacement of a dropped day's attempt, and ends once a lap over every started archive of its share fails
  none: the position wrapping is the lap's end, every tree having been taken again and copied, published, found held
  or gone.
  **Only a scan that reads from the start is a wrap** - never one continuing the tree in progress, inferred from its
  `flow_id`: with that tree at `flow_id` 1 every chunk read as a wrap, and the episode ended while another tree was
  still failing (`TestStage_ContinuingATreeIsNoLap`).
  **Count a lap only when it runs**: a round that meets a tree it has already taken stops without taking it, and
  counting that wrap would credit the next round with a clean lap it never ran - ending the episode between two
  failing passes. Fast archiving's end rule (a probe of the unattempted roots) cannot serve here, since a staged
  tree is excluded from it by design (`TestStage_AFailingCopyIsAFailureUntilALapFailsNone`, and with
  `DWARF_TESTING_STAGE_ROWS=1` the engine's `TestArchiveMetrics_TheReaperCountsWhatItMoves`).
- **A published tree's live copy is fast deletion's** when a batch holds it, and otherwise sweep deletion's, in chunks:
  fast deletion passes a stamped tree larger than a batch, and its position is then past it.

Staging is on past 256k rows or 128 MiB of payload by the engine's defaults, and past a batch on SQL Server, MySQL and
MariaDB ("The move", above; `engine/CLAUDE.md`). The engine and
fixtures suites exercise it on every tree with `DWARF_TESTING_STAGE_ROWS=1`.

## The archive attempt (`attempt.go`) and publication accounting

A live root's `archive_attempt_at` holds when its staged copy began: the copy's day and its identity.

- **Every stamp of a live root but a publication clears the attempt in the same UPDATE** (`NoAttemptSQL`): live
  retention's, a retention of 0's, Delete's. So a root stamped with an attempt is one published to the archive whose
  live copy is being deleted (`AttemptedSQL`), and the engine routes reads, Fork and Continue of it to the archive's
  copy. An expiration's mark writes none: an open root never has an attempt.
- **`dwarf_flows_archived` counts publication; `dwarf_flows_reaped` counts live removals without it**, so their sum
  does not count a staged tree twice. The deletions tell a published live copy from an unarchived tree under the root
  lock (`publishedSQL`): stamped with its attempt, or carrying the `archived_at` its publication copied - which survives
  a Delete clearing the attempt (`TestStage_PublishedCleanupNotUnarchived`, `TestDelete_APublishedLiveCopyIsNotCountedReaped`).
  This provenance is not a visibility or authorization guard.

## Tests

Its tests run on a rig (`rig_test.go`): an isolated shard database, an archive with today's tables made, and a Manual
janitor whose settings a test changes between passes. Trees are seeded by direct `INSERT` (`seedTrees`), as valid
closed trees. The engine keeps the behaviour oracle - archived reads, Purge, Delete, DeleteOnCompletion, expiration,
the archive metrics - and its own copy of the seeder.

**Every failure a job reports fires `CheckpointJanitorFailed`** - a statement it logs, a batch refused for carrying a
claim, a failed pass, a root a sweep holds, an archive failure past its episode's first log - never at a stop, so a test
asserting that no job failed counts its visits rather than log lines. One failure can fire it twice (a sweep's held
root is an archive failure and a hold), so a count is a floor, not a tally. **A batch a sweep splits is no report**:
taken again a root at a time, only a root that then fails alone is held and fires it, as only it is logged. The
expiration job's pass is the engine's and fires it beside its own logs (`expiration.go`). Pinned a site at a time, one
job's round so no other reports: `TestStage_AFailedChunkIsRetried` (a logged statement),
`TestSweep_HeldRootsAreVisibleBeforeTheSweepCompletes` (fast archiving's episode, sweep archiving's, the hold),
`TestArchiveRetention_AFailedPassIsCountedAndAStopIsNot`, `TestJanitor_EachBatchTakesAFreshClaim`,
`TestRecovery_AFailedPassIsNoProgress` and the engine's `TestExpiration_AFailingRootIsPassedOver`.
