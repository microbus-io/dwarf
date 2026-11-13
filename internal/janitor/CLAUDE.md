# Dwarf `internal/janitor` — one shard's background jobs, a goroutine each

> Load when: changing how a janitor's jobs are scheduled, started, stopped or drained, how a batch takes its turn
> and its connection, fast archiving, sweep archiving, large-tree archiving or fast deletion, or the janitors' metrics.
> Coupled with: `engine/CLAUDE.md` §"The janitors" (their activity, the residue class, the drain order, the test seams),
> `internal/turnstile/CLAUDE.md` (what a claim's age means), `internal/reaper/CLAUDE.md` (the work the janitor has not
> taken over - sweep deletion, chunked deletion, retention - and the staged protocol's rationale) and
> `internal/archiver/CLAUDE.md` (the day tables).

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
(`SetIdle` replaces every job's for a test). **The wait follows a round without progress, not only an empty scan:** a
job whose candidates all fail would otherwise spin on them. No failure counters, backoff or retry classes beyond that.

**The fast jobs wait 1 s, the repair jobs 30 s** (`fastIdle`, `repairIdle`; `TestJanitor_TheIdleWaits`). A fast job
polls for work that keeps arriving. A repair job - sweep archiving, large-tree archiving - normally has nothing to do,
and its wait is what it costs then: polled every second it would take a turn, a connection and its reads every second
on every shard, and retry a root or tree that keeps failing every second, a large tree's failing chunk copied again
each time. So in a healthy fleet the janitor's steady cost is the two fast jobs. The wait slows nothing that works: a
round with progress is followed at once by the next. What it delays: a large tree's first chunk after fast archiving
starts its attempt, a root fast archiving passed (up to the sweep's window plus the wait, which is how long a single
failure shows in `dwarf_archive_lag_seconds`), and the repair jobs' gauges, which are as old as their last round - an
attempt published, or a held root taken, reads until the next.

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
  sweep (sweep archiving, below). A job that held its position on a failure would retry it every round and reach
  nothing behind it (`TestWalk_FastPassesTheBatchItAttempted`). A round that removed nothing - held or failed - makes no
  progress and the job waits; the batches after it are reached on the next round.
- **Not its own, and passed:** a root with an archive attempt (large-tree archiving's); with staging on, a tree over
  the staging threshold, alone in its batch, whose attempt the job starts - one small transaction under the root's lock, `claimAttempt`
  - for large-tree archiving to copy (`TestFast_ATreeOverTheStagingThresholdHasItsAttemptStarted`); at a retention of 0, a
  tree too large for a batch, which the job stamps for deletion - due at once - and the deletion of stamped trees
  removes in chunks. Passing them counts as progress. The stamp locks the root as a move does, so a root a Fork holds
  is skipped on PostgreSQL and MySQL rather than waited for (`TestFast_AHeldRootIsNotWaitedForWhenStamped`).
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
- **The job reports its own gauges** - `dwarf_archive_lag_seconds`, and its series of `dwarf_archive_failing_seconds`
  (labelled with the job's name, `job="fast_archiving"`) - registered on the meter when it is added, observed only
  while its janitor is active, and unregistered when it is removed or the janitor stops. The engine reads nothing of
  it.
- **A probe reads the shard's oldest due closed root** - one `LIMIT 1` per terminal status on
  `idx_dwarf_flows_status`, unrestricted: every replica's class, behind the position or ahead of it. It runs at most
  once a second while the job is busy, and in every round whose scan finds nothing, which the idle wait paces. A
  probe's scan consults no fault, or it would consume one a test armed for a round's scan.
- **The lag is the probe's**: how long archiving is behind - how long past the grace the oldest due root has waited, a
  root a failure left behind the position included, and one being staged; 0 when none is. **It goes on aging after its
  probe** (the probe records when the root passed the grace), so between probes it reads how long that root has waited,
  not how long it had: a reading frozen at the probe would understate the lag by up to a probe's interval, and by the
  whole stall when the job is blocked. Pinned by
  `TestFast_TheLagIsTheShardsOldestDueRootPastTheGrace`.
- **The failure episode** (`dwarf_archive_failing_seconds`) begins at an archive failure - a day not readable or not
  whole, a batch that cannot take its connection, a failed scan, attempt read, sizing or move - and ends at a later
  move that archives something, or at a probe that finds no due root without an archive attempt, or finds the oldest
  came due more than `endMargin` (1 s) after the last failure: everything due then has left the live tables. A staged
  tree in progress is not a failure, so it holds no episode open. The probe's ages are the database's and the failure's
  time is this process's, compared as durations, so neither clock's offset matters - **but they are read at different
  instants and resolutions, so do not compare them without the margin.** A root that failed just after it came due
  then reads as due after its own failure, and the episode ends with the root still live: with no margin,
  `TestArchiveMetrics_TheReaperCountsWhatItMoves` failed 49 of 300 runs on SQLite. **Do not end it at an empty round:** the scan is after the
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

## Sweep archiving (`sweeparchiving.go`)

The job behind fast archiving (`JobSweepArchiving`): the closed roots fast archiving left - one it failed on, found held
or could not start an attempt for, one whose close committed late with a time behind its position, one of a slow
peer's class it passed. Routed as fast archiving routes them, through the same functions (`closed.go`): moved, a tree
over the staging threshold given its attempt, and at a retention of 0 deleted or stamped. **When archiving, a root with
an attempt is not in its scan**: it is large-tree archiving's, its copy can run for a day before it publishes, and
every sweep would pass it again, a batch at a time.

- **The boundary is fast archiving's position**, published at the end of every round with the residue class its scan
  was restricted to (`boundary`, read lock-free). A sweep captures it as it begins and takes the roots at or behind it
  in close order: the root fast archiving was last done with is the sweep's, failed or not
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
- **The residue class is the boundary's, not read from the shard.** A sweep sweeps the class fast archiving's position
  was taken in, plus the roots overdue by the steal grace past its window, and is dropped when fast archiving's class
  changes: fast archiving then starts over, and the next sweep has the new class's position to sweep behind
  (`TestSweep_FollowsFastArchivingsResidueClass`). Read from the shard, a sweep could pair one class with a position
  taken in another. A dropped sweep's held roots still count, and, not having completed, it ends no failure episode.
- **A failed batch is taken again a root at a time**, from the same position; a root that fails alone is held -
  counted and passed - and the next sweep comes back to it. Only the first `heldLogLimit` (8) a sweep holds are logged,
  by correlation id: through an archive outage the sweep holds every root it takes, up to 8 a second. A batch that
  fails nothing doubles the next, up to the batch. A round takes batches until one makes progress or
  `sweepFailureLimit` (8) have failed, then gives way to the idle wait (`TestSweep_APoisonTreeDoesNotLoseItsBatchMates`,
  `TestSweep_PassesFailedTrees`). **Passing a root is no progress** - one another transaction holds, or whose attempt
  started under it - though the round goes on past it: a root that stays behind the position is passed by every
  sweep, and counting it would keep the job from ever idling and a test's drain from ever returning
  (`TestSweep_PassingAStuckAttemptIsNoProgress`).
- **Its gauges are its own**, observed only while its janitor is active: `dwarf_archive_held_roots`,
  `dwarf_archive_oldest_held_seconds` and `dwarf_archive_sweep_seconds` under `kind="closed"` - the engine reports the
  other kinds from the reaper's walks, under the same descriptions (`HeldRootsDescription` and its siblings, which the
  engine imports) - and its series of `dwarf_archive_failing_seconds`. The held count is the larger of the current
  sweep's and the last complete one's, published as each root is held
  (`TestSweep_HeldRootsAreVisibleBeforeTheSweepCompletes`); the front's age is the oldest due root at a sweep's first
  scan, aging until the next sweep's.
- **Its failure episode** begins at a failure - the day, a scan, a sizing, a move, a connection - and ends at a sweep
  that completes having failed nothing, in a batch whose day was read and open: a sweep starting after the failure and
  finding every root behind fast archiving's position taken (`TestSweep_HeldRootsAndTheFailureEpisode`). At a retention
  of 0 nothing is archived and nothing starts one: a connection it could not take is logged
  (`TestSweep_AtRetentionZeroAConnectionErrorIsNoArchiveFailure`).
- **It moves nothing while the day is closed** - tomorrow not whole, or the quiet margin - as fast archiving: the roots
  stay live and the sweep is failing (`TestSweep_NothingMovesWhileTomorrowIsNotWhole`). Ending its episode there is
  barred by the open-day condition above, or every round would end it and the next begin it again.

## Large-tree archiving (`largearchiving.go`)

The third job (`JobLargeArchiving`): every tree whose staged archive attempt has started - one too large to move in
one transaction, its attempt started by fast archiving or by sweep archiving - copied into its attempt's day
over as many transactions as it takes, invisible there, then published in one small one: the root last, the live root
stamped for deletion with its attempt kept. The protocol - the attempt as the copy's identity and day, the stamp groups,
the publication's wait for the next day, a dropped day replacing the attempt, `NOT EXISTS` rewrites, the open-flow
guard, Delete abandoning the copy - is the reaper's, carried over (`internal/reaper/CLAUDE.md`, "Staged archival of a
large tree", holds its rationale while the reaper stands).

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
  chunk a second (`TestStage_AStuckTreeDoesNotHoldTheOthers`). A failed chunk counts in `dwarf_archive_move_failures`
  and is logged.
- **At a retention of 0 it copies nothing**, as the reaper's archive path never ran there: a tree whose attempt an earlier
  run started is fast archiving's to delete, which it does attempt or not (its move fences on the attempt only when
  archiving, and its stamp clears it). Copying it as well would race the deletion and could publish a tree into an
  archive that keeps nothing (`TestStage_RetentionZeroArchivesNothing`).
- **No residue class.** A chunk locks its root, skipping a held one on PostgreSQL and MySQL, and writes only what is
  not there, so two replicas on one tree serialize per chunk and write the same rows (`TestStage_ConcurrentCopiers`).
  Dividing the attempts would only leave a tree waiting for its replica.
- **Each copy's position is in memory**, at most `maxStages` (64) of them. A position advances only when a chunk
  commits, so it is kept on every path where one did - a copy waiting to publish, a lost response - and dropped only
  once the tree is published or gone. A position forgotten - at the bound, a restart - is read back from what the
  attempt's day already holds, a count over the copied descendants. **A remembered position whose chunk copied nothing
  is stale** - another replica copied past it - and the copy is read back from the day and goes on in the same batch,
  rather than re-read a chunk's worth of rows under the root's lock each turn (`TestStage_TheCopiedRowsAreCounted`). **Do not bound
  the positions by recency**: the trees are visited in turn, which is the access pattern an LRU serves worst - past the
  bound it evicts the next tree to visit, every time. The job drops an arbitrary one.
- **`dwarf_archive_oldest_attempt_seconds` is the job's own**, a probe of the oldest unpublished attempt at most once
  a second while busy and in every round that finds nothing, observed only while the janitor is active. It goes on
  through a closed day (`TestStage_OldestAttemptDuringDayOutage`). **The reading goes on aging after its probe** (the
  probe records when the attempt began, not its age): a chunk that blocks stops the probes too, and a reading frozen at
  the last one would hide exactly the stuck copy the gauge is for (`TestStage_TheOldestUnpublishedAttemptIsObserved`). `dwarf_archive_staged_rows` counts each chunk's
  inserted rows, so a rewrite after a forgotten position adds nothing (`TestStage_TheCopiedRowsAreCounted`).
- **Its failures are an episode of its own** - `dwarf_archive_failing_seconds{job="large_archiving"}`, beside fast
  archiving's series; each job owns its series, and a reader aggregates with max. It begins at a failed chunk or a
  failed replacement of a dropped day's attempt, and ends once a lap over every started archive fails none: the
  position wrapping is the lap's end, every tree having been taken again and copied, published, found held or gone.
  **Only a scan that reads from the start is a wrap** - never one continuing the tree in progress, inferred from its
  `flow_id`: with that tree at `flow_id` 1 every chunk read as a wrap, and the episode ended while another tree was
  still failing (`TestStage_ContinuingATreeIsNoLap`).
  **Count a lap only when it runs**: a round that meets a tree it has already taken stops without taking it, and
  counting that wrap would credit the next round with a clean lap it never ran - ending the episode between two
  failing passes. Fast archiving's end rule (a probe of the unattempted roots) cannot serve here, since a staged
  tree is excluded from it by design (`TestStage_AFailingCopyIsAFailureUntilALapFailsNone`, and with
  `DWARF_STAGE_ROWS=1` the engine's `TestArchiveMetrics_TheReaperCountsWhatItMoves`).
- **A published tree's live copy is fast deletion's** when a batch holds it, and otherwise the reaper's chunked deletion
  until sweep deletion moves (step 7): fast deletion passes a stamped tree larger than a batch, and its position is then
  past it.

Staging is off in production (`reapStageRows` 0): fast archiving starts no attempt, and the job copies only attempts
already there. The engine and fixtures suites exercise it with `DWARF_STAGE_ROWS=1`.

Its tests run on a rig (`rig_test.go`): an isolated shard database, an archive with today's tables made, and a Manual
janitor whose settings a test changes between passes.
