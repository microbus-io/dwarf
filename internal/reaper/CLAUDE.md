# Dwarf `internal/reaper` — removing closed trees from one shard's live tables

> Load when: changing how closed trees are archived or deleted, the walks over due roots, the batch move, the
> archive-side deletions behind Delete and Purge, or the reaper's metrics and observations.
> Coupled with: `engine/CLAUDE.md` §"Data Retention" (the engine's wiring, Delete and Purge, the deletion race gate),
> `internal/archiver/CLAUDE.md` (the day tables, the directory, why a row goes into a day only once the next is whole)
> and `internal/migrations/CLAUDE.md` (the column catalog and the indexes the scans walk).

## What the package owns, and where its boundary is

A `Reaper` owns what its passes learned, per shard: each walker's position (`walks`), since when archiving has been
failing (`failures`) and the archive lag (`lag`). That state is the reason it is a package, beside the SQL over the
live tables it runs (as `internal/piston` does, borrowing its handle and partition from the engine).

- **One Reaper per engine run, built at Startup before the gauge callback registers.** A fresh one is the single reset
  point, so nothing a previous run learned is ever reported, a failed Startup included. Do not add a `Reset`: two
  reset points let a gauge read between them report the last run's walks. That window is NOT pinned:
  `TestReaperContract_ARestartForgetsTheWalksAndTheLag` and `TestReaperContract_AFailedStartupLeavesNoReadings` read the
  gauges only once Startup has returned, so they pin that no reading outlives a run, and pass with a `Reset` after the
  callback registers too. The engine holds the Reaper in an atomic pointer, since an operation still running from the
  previous run may read it while a new Startup stores the next.
- **`Config` is built by the engine for every pass** from its fields, because tests change them on a running engine.
  Nothing in it is kept.
- **`Residency` is the only callback into the engine**, re-read on every walk. Everything else comes in by value, and
  the engine reads the state only through `Observe`. The package imports neither `engine` nor `engineseams`;
  `engineseams` aliases the fault names, so an engine test arms the same strings.
- **The engine keeps** the ticker, the per-shard turn, the `dispatchesOn` gate (and `Forget` when it fails),
  expiration after the pass, the gauges, `dwarf_flows_removed`, and Delete's key and status validation. The gauges
  stay there because they are gated on dispatching when read, while `Forget` runs only at the next pass.
- **Operation-level exports only.** `MoveTrees`, `DeleteLive`, `DeleteArchivedTree`, `DeleteArchivedRoots` and
  `DeleteMarked` are operations;
  nothing exports a transaction or a lock. A later protocol step that needs the root lock is added here as an
  operation too, so the lock order stays in one package.

## A pass

On one shard the stages share the caller's turn and run serially: retention cleanup, then stamped and closed trees
alternately until neither has more, the budget is spent, or the pass is stopped. Each walker yields after `Slice`,
checked between attempts; healthy backlog uses up to `Budget`. Every stage gets its first slice even if an earlier
attempt overshot the budget, and a failed stage does not suppress the next. `dwarf_reap_pass_seconds` times the pass
alone; the engine's expiration stage runs after it (`TestReaperContract_ThePassTimeExcludesExpiration`).

**A tree is always one atomic move, descendants included, and a deletion of a tree that fits a batch is one too.** A
batch considers at most `Batch` roots and trims a leading run to about `BatchRows` joined step rows, always admitting
the first tree (a flow with no steps counts one row). A tree to DELETE that is larger than a batch alone is deleted in
chunks instead (below). The sizing is a hint outside the transaction, bounded by the attempt deadline; it is not a guard and is not
cached. The transaction locks a sorted copy of the roots, rechecks eligibility under the lock, and processes their
whole `root_flow_id` trees, steps before flows. PostgreSQL and MySQL skip held roots; SQLite and SQL Server wait
under the deadline. `Transact` retries contention, including a straggler's reverse lock order. A stop records no
failure; an uncertain commit is reconciled by scanning the live roots again, never by assuming an error proves a
rollback.

**Two walkers share one traversal.** The fast walker keeps a position and advances past each attempted range,
including a failed sizing or move. The repair walker independently covers **all due roots**, behind or ahead of the
fast position: each sweep freezes an inclusive end (the database clock and the largest flow id), keeps its position
across slices and wraps when exhausted, so a root committed late with an old timestamp, or due behind the position,
is found on the next sweep. The fast position is a hint only; neither deletion authority nor repair coverage depends
on it.

Repair starts at one root, doubles after an actual removal up to `Batch`, and drops to one after any failure. A
failed batch of several stays at its position; a failed single advances and is logged once in that sweep. A
zero-lock attempt advances without counting a failure or growing the batch. Eight failed attempts in a row yield
until a later slice. A scan failure has no known range to skip and ends its slice. No transient-error classes, retry
counters at one position, deletion handover stamps or chunk boundaries are needed, and none should be added.

**Residency.** Both walkers take this dispatcher's residue class plus any root overdue by `StealGrace` (two engine
ticks). Traversal stays ordered, without the own class first. Closed and retention roots walk `(updated_at,
flow_id)`, each closed status a branch that seeks by timestamp, merged in Go, with exact stored timestamps bound
through `database.SQLTime`; stamped roots walk `flow_id`. Roots a fleet change or an idle class misses are covered by
repair. Residency is a work preference; the transactional locks are the ownership.

## Chunked deletion of a large live tree

A deletion that grows with its tree eventually cannot finish within one attempt, and then never finishes: the delete
wall. So a tree to delete - stamped, closed at a retention of 0, or past its retention - that is larger than
`BatchRows` alone is deleted over several transactions (`deleteLarge`, `chunk.go`), each about `BatchRows` rows.

- **Every chunk is authorized by the root's stamp.** The deletion kind's roots are stamped already. A retention of 0
  and the retention cleanup first stamp the root (`delete_after_ms=1`, due at once) in a transaction of their own,
  with their own due predicate as the guard. Each chunk then locks the root and rechecks "stamped and due" (the
  deletion kind's predicate), so a chunk never deletes from an unstamped tree, and a pass that stops part-way leaves
  a stamped tree the deletion kind finishes, on this replica or another, with nothing in memory.
- **Descendants first, newest first, each flow's steps then the flow; the root's steps; then the root, last.**
  Children are read 256 at a time (`treeChunk`), from the chunk's position (`chunkPos`), so a read seeks past the
  entries earlier chunks deleted, which stay in the indexes until vacuum. The position is in memory, for one
  `deleteLarge`; a new call starts again from the newest descendant. It also holds the last step deleted of a flow
  part deleted, a child or the root, so the next chunk seeks past that flow's deleted steps: read from the start, a
  flow of a million steps - one `forEach` flow is one - would cost each chunk every step deleted before it (measured
  on PostgreSQL before vacuum, 112 ms against 42 ms for the last chunk of a 100,000-step child).
- **Every read in a chunk is bounded by its budget, never by the tree.** Which children fit is decided by counting
  at most the budget's worth of their steps (`stepsUpTo`, a `LIMIT` or `TOP` under a `COUNT`), halving the run of
  children until it fits (at most eight probes); a child larger than what is left has a budget's worth of its steps
  deleted. Do not size the children with a full `COUNT ... GROUP BY`: it recounts a large child's remaining steps on
  every chunk, quadratic over the tree (measured 279 ms a chunk on PostgreSQL at 4,000 steps a child, and past the
  attempt at about a million). Step ids are read in `step_id` order from `idx_dwarf_steps_flow_id` and deleted by a
  literal list: a range or an ordered `DELETE` with a limit is not portable, and the read stays bounded by the flow's
  own steps only with that index keyed on `(flow_id, step_id)` (`internal/migrations/CLAUDE.md`). The walk's first
  sizing of its candidates (`rootsWithinRows`) still counts each tree whole.
- **The root goes only after a recheck that the tree holds nothing else:** two probes, the first remaining
  descendant and the first root step, not an `EXISTS` over the tree, which MariaDB materializes over the whole steps
  index (275 ms on a 100,000-child tree). The descendant probe seeks above the position's top: the walk has just found
  none below its position and deleted those between, so only a newer descendant can be left. On MySQL and MariaDB the
  probes are locking reads, since a plain read there sees the snapshot the chunk's first plain read took. A closed tree gains no flow, so this guards against a bug, not a path
  (`TestChunk_ALateDescendantKeepsTheRoot`).
- **Readers never see part of a tree.** The engine's live closing read rejects a tree whose root is stamped and due,
  and the lists and Summary hide a stamped tree's children (`engine/CLAUDE.md`, "Reading an archived flow").
- **The walk keeps its position on a tree it did not finish without a failure**, so the next slice resumes it. A
  chunk that fails is passed like any failed batch: the fast walker goes on, repair counts it held and passes it, and
  the trees behind it are not held back (`TestChunk_AFailingLargeTreeDoesNotHoldBackTheRest`).
- **`dwarf_flows_reaped` counts each chunk as it commits**, so a later chunk's failure or a stop loses none of it
  (`TestChunk_CommittedChunksAreCountedWhenALaterOneFails`), under the reason of the walk that deleted it: a tree
  retention or a retention of 0 stamped and a later pass finished counts the rest as `deleted`.
- **Archiving stays atomic.** A tree to archive that is larger than a batch moves in one transaction.

Pinned by `chunk_test.go`: a large tree deleted over several passes for each deleting kind, the root last and no step
outliving its flow; a faulted chunk rolled back whole, the stamp kept; a restart (a fresh Reaper) resuming; two
Reapers sharing a tree; a chunk with nothing to do; the late descendant; a failing tree passed; committed chunks
counted; the step read's plan; and an envelope, a half-drained wide tree and a child of 100,000 steps, each beside
50,000 unrelated steps.

## Staged archival of a large tree (`stage.go`)

A tree too large to move in one transaction is archived in stages: copied into the archive over several
transactions, invisible there, then published in one small one. **It is off in production**: `Config.StageRows` is 0
(the engine's `reapStageRows`), and tests set it. Off, a tree is still staged when its root already has an attempt.

- **The attempt.** `claimAttempt`, under the live root's lock while the root is still due, joins the attempt T in
  `archive_attempt_at`, or starts one at `NOW_UTC()`. It starts one only before the quiet margin and only when T falls
  in the filing day, so the stamp groups have room. Every visit joins: another walker, another replica, a restart, a
  visit after midnight. **T is the attempt's identity and its day** (`DayOf(T)`), so two copiers on one T write the
  same rows, and a copier holding a replaced T fails its next lock.
- **Every chunk re-locks the root and rechecks it: closed, unstamped, its attempt still T** (`stageChunk`). It copies
  descendants in `flow_id` order, each flow's steps and then its flow row and directory row, sized and bounded by the
  budget as a chunked deletion is (a flow larger than the budget is copied a budget at a time). Each write inserts
  only what is not there (`NOT EXISTS` by the stable id), so a rerun - a restart, a second copier - writes the same rows.
  The budget is charged the rows a chunk reads, whether there already or not.
- **Archive times are stamp groups, never the commit's time:** descendant i (in `flow_id` order) is stamped
  `T + i/4096` ms, the root `T + G` with `G = ceil(N/4096)`. The group size is a constant, so a flow's time is a
  function of the tree alone, and no staged tree puts more than 4,096 flows on one time. Nothing is clamped: a stamp
  past the day's end is an error that holds the tree, since a clamp would put its flows on one time. Lists therefore
  show a staged tree under its attempt's time, not its publication's: a window that moves while archival runs is not
  a lossless feed of what was archived.
- **Publication is the last chunk:** the root's last steps, its flow row, its directory row, and the live root
  stamped `delete_after_ms=1` with T kept, in one transaction, no whole-tree statement. Stamped with T means
  published, so a lost response is retried as nothing to stage (`TestStage_APublishedTreeIsNotCopiedAgain`). The
  deletion kind then removes the live tree, **in the same pass**: a walk that published reports more work, so the pass
  runs its walks again rather than leave the live copy to the next tick (`TestStage_ALargeTreeIsPublishedWhole`).
- **A dropped day replaces the attempt.** A chunk's statement on a dropped `DayOf(T)` ends its transaction, so a fresh
  one, still under the root lock and only while T is unchanged, starts a new attempt in today; the old rows went with
  their day (`TestStage_ADroppedDayIsReplaced`, `TestStage_AHalfDroppedDayIsReplaced`, the latter the reason the
  archiver drops a day's flows table first).
- **The position is in memory** (`stagePos`, in `stages`, per shard and root, so the fast and repair walkers share
  it). **A fresh one - a restart, another replica - resumes from the day itself**: descendants are copied in
  `flow_id` order and each flow row after its steps, so every descendant up to the largest the day holds is whole, and
  their count numbers the next one's stamp group. Do not start a fresh position at the first descendant: a restart
  then re-reads the whole prefix at a chunk's budget, and a copier restarted every few chunks never gets past the first
  descendants (`TestStage_ARestartAtEveryStep`). It
  is still kept whenever the copy may go on, since the resume costs a count of what is there: **a root another transaction holds - another copier, a Fork - locks nothing, as
  a stamped one does, and only a read of the root tells them apart** (`treeGone`). Do not forget the position when a
  lock takes nothing: a large copy would restart at every collision, and two replicas would keep throwing away each
  other's progress (`TestStage_AHeldRootKeepsItsCopysPosition`). A position is forgotten when the copy publishes or its tree
  is stamped or gone; past `maxStages` per shard the least recently used goes, never the rest
  (`TestStage_APublishedCopyIsForgotten`).
- **The atomic path is fenced**: `move` locks only roots with no attempt, so a selection made before a staging claim
  cannot move the tree atomically into a day that holds staged rows, and the walk sends a root with an attempt to the
  staged path whatever its size, once its sizing succeeds (`rootsWithinRows` reads the attempt with the sizing;
  `TestStage_AnAttemptedRootIsStagedWhateverItsSize`).
- **The day's gate stops new archives only** (`filingDay.startable`: before the quiet margin, tomorrow whole). A walk
  that may not start one spends its slice on the trees whose attempt has started (`resumeStarted`), each going on into
  its own day: an attempt into yesterday is not held back by today's tomorrow
  (`TestStage_AnAttemptGoesOnWhenNoneMayStart`). That scan has a position of its own (`startedAfter`), wrapping at the
  end, and reads only which roots have an attempt, so an attempt behind any number of roots without one is reached
  (`TestStage_AStartedAttemptBehindABatchIsReached`). The walk's own position stays on those roots: moved past them, the
  fast walker would leave a whole outage's backlog to the repair walker's slower batches once the day opens.
- **Publication waits for the day after the attempt's to be whole**, since a locate finds a flow only where the day
  after its own is: the copy finishes, and the root goes in once it is. **A waiting copy first runs a statement on its
  own day**: a copy whose every row is in writes nothing else there, so without it a dropped day would never fail a
  statement, and the copy would wait forever for a next day that is never made
  (`TestStage_ADroppedDayIsReplacedWhileWaitingToPublish`). The archive's record of the days may be stale, so only the
  statement is proof.
- **Which path a tree takes is decided by capped counts, never a whole-tree count** (`rootsWithinRows`,
  `cutWithinRows`). The attempts are read first, by primary key: a root with one goes to the staged path unsized. The
  rest are counted one by one with `treeRowsUpTo`, the first up to one past what decides its path (the batch, and
  `StageRows` when staging), each later one up to one past what is left of the batch. Do not size a batch with one
  `GROUP BY` over its trees: a huge tree anywhere in it makes that count miss its deadline, the batch gets no sizes,
  and the tree never gets the attempt that would stop it being counted. The cost is a round trip per tree in the
  batch; Purge's sizing uses the same rule.
- **A tree whose atomic move runs into its deadline takes the staged path from its next visit**, whatever its size,
  when staging is on: the walk remembers it (`preferences`, in memory, per shard and root), and its next visit treats
  it as one with an attempt - unsized, a batch of its own, the staged path - whose locked claim starts the attempt.
  **The claim waits for the next visit**: a claim right after the timeout finds the root still held on MySQL, whose
  driver abandons a timed-out statement by closing its connection while the server runs it on, holding the root until
  it ends. A visit that cannot claim the root, or may not start an attempt, yields and keeps the preference, and
  **never tries another atomic move** (`TestStage_ATreeThatMissesItsDeadlineIsStaged`,
  `TestStage_APreferenceWaitsForItsRoot`). A preference is dropped once its tree has an attempt, is stamped or gone,
  or a batch removed it. A full set makes room only by dropping preferences whose trees are no longer live, never a
  pending one; one that finds no room is not kept (`TestStage_APreferenceIsHarmlessOnceItsTreeIsGone`). A restart, a
  handoff or no room forgets a preference, and the tree times out once more before it is remembered: it is not a
  durable guarantee, the attempt is (`TestStage_ARestartForgetsOnlyThePreference`). A lock wait that ran out stages a
  tree that did not need it, which costs only transactions; do not add failure counters or timeout classes to avoid it.
- **The oldest unpublished attempt the repair sweep sees is a gauge** (`dwarf_archive_oldest_attempt_seconds`), read
  from the attempts the sizing reads anyway, the larger of the current sweep's and the last complete one's, aging
  since: an incomplete sweep never reads as nothing outstanding. A replaced attempt starts its age again, so it is
  read beside the archive lag (`TestStage_TheOldestUnpublishedAttemptIsObserved`).
- **Copy progress is a counter apart from failures** (`dwarf_archive_staged_rows`): the rows each staging chunk
  inserted, counted as it commits, so a rerun over rows already there adds nothing. A growing attempt age with this
  moving is a large tree copying; with it flat, a stuck copy (`TestStage_TheCopiedRowsAreCounted`).
- **A tree holding a flow that is not closed is refused**: only a bug leaves one, and its copy would not be settled.
- **Delete during staging** stamps the root and clears T (`DeleteLive`). The copier's next lock fails, and the rows
  it copied stay in the day, invisible, until the day drops (`TestStage_ADeleteAbandonsTheCopy`).

`FaultArchiveErr` fails a staging chunk as it fails a move, so a test that makes archiving fail makes it fail on
either path. **The engine and fixtures suites run with staging on** under `DWARF_TEST_STAGE_ROWS=1` (every tree of
more than one row staged); run them so after a change to this protocol.

Pinned by `stage_test.go` and the engine's `TestStaged_ATreeArchivedInStagesReadsAsOne` (reads mid-copy, after
publication, and after the live copy goes).

## Archive failures never become permission to delete

At a positive retention an archive failure keeps the tree live. The retention walker deletes an unstamped closed tree
only when `today - retention > day(updated_at + Grace)`, on the database clock and the archive's day length, and the
strict cutoff is rechecked under the root lock. Day arithmetic avoids overflowing a duration for a large accepted
retention. The retention walk needs no archive tables and no whole tomorrow. Stamped deletion and a retention of 0 keep
their own authority. A late successful move still takes its retention from its filing day.

**Every scan, sizing, day lookup and tree transaction has the same `Attempt` limit.** The slice and budget yield
between attempts and never interrupt a transaction. Atomicity does not make an arbitrarily large tree serviceable: one
that keeps missing its attempt limit stays whole and is retried indefinitely, DeleteOnCompletion and retention
included. Do not add a destructive fallback to get past it; the operator repairs the bottleneck. Driver cancellation
must leave pooled connections usable: `TestWalk_ABlockedAttemptIsAFailureAndAStopIsNot` reads through the pool right
after a cancelled attempt.

## The move

- **The reaper makes no table.** It archives into today only while the archive's record holds tomorrow whole; a
  missing tomorrow holds closed trees live while stamped and retention deletion go on. Pinned by
  `TestMove_NoClosedTreeMovesWhileTomorrowIsNotWhole`.
- **`archived_at` is the move's time, clamped to its filing day's last millisecond** (`archivedAtSQL`), so a day holds
  only rows archived on it. A quiet margin before the day's end (a minute, or an eighth of a short day) stops archive
  moves only; `NoQuietMargin` turns it off for tests.
- **The flows move by name, the steps by column order.** `archivedFlowColumns` names every flow column the day tables
  have, `archived_at` apart, so not the live-only `archive_attempt_at`; the steps move with `RETURNING *` /
  `SELECT *`, so the day's steps table must keep the live column order (a rule in `internal/migrations/CLAUDE.md`).
  Both pinned by `TestMove_NamesEveryFlowsColumn`. On PostgreSQL each table moves in one `DELETE ... RETURNING` into an `INSERT`, so what is archived is exactly what is deleted;
  elsewhere it is copied, then deleted, and a straggler is archived as it was at the copy.
- **The directory rows for a late or early filing are written in the move's transaction**, from the same rows;
  `DirectoryHours` decides which. Pinned by `TestMove_DirectoryNamesOnlyWhatTheLocateCannotFind` and
  `TestMove_DirectoryTheReapersBounds`.
- **Directory, flows and steps go together.** A lost response after a commit may undercount a metric, but a retry finds
  the root gone; no partial tree is ever published.

## The root lock is two-sided

Every transaction that removes a tree, live or archived, locks its root first with the strength a `DELETE` takes
(`lockRoots`: `FOR UPDATE`, or the `touch` flip). The engine's Fork locks a source root with `FOR NO KEY UPDATE`,
which conflicts with it, so a Fork and a removal of one tree serialize. Either side weakening its lock breaks the
other; the engine's `lockRows` names this one, and Fork's pins stay in the engine.

## Archive-side deletion

`DeleteArchivedTree` (Delete) and `DeleteArchivedRoots` (Purge) run on the caller's context: no attempt deadline and
none of the pass's faults, since an operator's call has its own deadline
(`TestReaperContract_RemovalsRunOnTheCallersContext`). Purge's row budget is the engine's `reapBatchRows`, the same
value as a pass's `BatchRows`, passed in explicitly.

- **A tree that fits a batch is deleted in one transaction**, the directory's rows with it. A day whose
  tables are gone deletes nothing and is no error. `FaultArchiveDelete`, targeted at a root, fails the transaction that
  would delete it.
- **A larger tree is marked, then deleted in chunks.** The mark is `delete_after_ms=1` on the archive root, written by
  the one UPDATE that also takes the root's lock; it hides the whole tree from every read at once (the visibility
  rule), so the call counts the tree removed. Whether a tree fits is counted only as far as the batch
  (`treeRowsUpTo`), so sizing a tree of millions of rows does not spend the call before its allowance begins. The
  call then deletes chunks (`chunk` on `archiveTarget`, which also
  deletes the directory's rows) until its deadline - the engine's `archiveDeleteInCall`, 5 s, since an API call must
  not run for tens of seconds - and the rest is left. **The chunks run under that deadline**, not only between them:
  checked between chunks alone, a chunk waiting on a lock held the call for as long as the lock was held
  (`TestDelete_TheInCallChunksKeepToTheirDeadline`). A chunk that fails after the mark is no failure of the call.
- **A marked tree is never counted twice:** a mark already there matches nothing, Purge's list skips marked roots
  (`TestDelete_APurgeOfALargeTreeCountsItOnce`), and the atomic deletion's lock skips them too: a selection made
  before another call marked a tree, the tree shrunk below a batch since, would otherwise delete it and count it
  again (`TestDelete_AMarkedRootIsNotDeletedAtomically`).
- **`DeleteMarked` finishes them**, on the reaper's five-minute stage, day by day: a probe of the day's deletion index
  (`markedIn`), then that day's trees in chunks, each under the attempt deadline, until the pass's budget is spent.
  - **It starts where the last pass stopped, in (day, root) order, and wraps around** (`archiveCleanup.from`). Do
    not probe every day before deleting, nor start from the first tree each time: one slow day's probe, or one tree
    whose chunk waits out the budget, then takes every pass (`TestDelete_AStuckMarkedTreeDoesNotStarveTheOthers`,
    `TestDelete_EachCallGetsPastWhereTheLastStopped`).
  - **The first tree a pass finds gets at least one chunk** however the budget went, so a pass always makes progress.
  - **On MySQL and MariaDB the probe names the index, and falls back to a probe without it**: a day made before the
    index has none, and a hint naming a missing index is an error there
    (`TestDelete_ADayWithoutTheDeletionIndexIsSwept`).
  - **The position and observations are one per-shard record** (`archiveCleanup`), published together after the
    pass. `pending` counts discovered roots left; `unknown` counts retained days not examined, separately. Observe
    reads both without SQL; a zero pending count means completion only when unknown is also zero. A held root still
    counts as pending: `chunk` also reports done when another transaction holds the root, so `stillThere` checks it
    (`TestDelete_AHeldMarkedTreeIsPending`). Cancellation keeps the previous observations and advances only the
    position. `Forget` removes the whole record.
- `DeleteArchivedRoots` returns the trees its committed transactions deleted or marked even when a later one fails, and
  the engine records `dwarf_flows_removed` from that count, once per shard per call. Pinned by
  `TestDelete_ADroppedDayIsNoError`, `TestDelete_APartialRemovalCountsWhatCommitted` and
  `TestDelete_ALargeArchivedTreeIsMarkedThenDeleted`.

## The archive attempt (`archive_attempt_at`)

A live root's `archive_attempt_at` holds when its staged copy began: the copy's day (`DayOf`) and its identity. The rule
that gives it meaning:

- **Every stamp of a live root but a publication clears the attempt in the same UPDATE** (`NoAttemptSQL`): the
  reaper's stamp before a chunked deletion (`stamp`), and Delete's (`DeleteLive`). So a root stamped with an attempt
  is one published to the archive whose live copy is being deleted (`AttemptedSQL`), and the engine routes reads and
  Fork and Continue of it to the archive's copy.
- **`DeleteLive` chooses the mutation under the live root lock.** The engine authenticates the key and rejects
  children and open flows before calling it; a closed root cannot reopen. The lock waits rather than skipping a
  held root. An unstamped root is stamped with its attempt cleared; a stamped root without an attempt is already
  deleted; a published root has its archive copy marked and its attempt cleared in the same transaction. A missing
  live root sends the engine to archive lookup. The lock order is live root, then archive root. `Deletion` reports
  which side was newly marked so repeats count nothing. A rolled-back transaction returns no removal count.
  Publication and live cleanup can run between the engine's validation and this lock, which is why its decision
  must use the locked row (`TestPublished_ADeleteRacingAPublication`, `TestPublished_ADeleteRacingTheLiveCleanup`).
  The attempt's day comes from `database.StoredTime`: the MySQL driver's location must not shift the stored wall time.
- **The archive's copy already gone is the deletion done.** No root row (Purge removed it): the attempt is cleared in
  the same transaction. The day dropped: the statement on it ends the transaction, so a fresh one clears the attempt
  it read, rechecked under the stamp; the first error is the proof, since a dropped day is never made again. Pinned by
  the engine's `TestPublished_*`.

## Tests

Package tests run on a rig (`rig_test.go`): an isolated shard database through `internal/database`, an archive with
today's tables made, enabled seams and a log counter. Trees are seeded by direct `INSERT` (`seedTrees`), as valid
closed trees. The engine keeps the behaviour oracle - archived reads, Purge, Delete, DeleteOnCompletion, expiration,
the archive metrics - and its own copy of the seeder, since this package's tests cannot import `internal/enginetest`
(it reaches this package through `engineseams`).
