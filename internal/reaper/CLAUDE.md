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
  expiration after the pass, the gauges, `dwarf_flows_removed`, and Delete's stamp. The gauges stay there because
  they are gated on dispatching when read, while `Forget` runs only at the next pass.
- **Operation-level exports only.** `MoveTrees`, `DeleteArchivedTree` and `DeleteArchivedRoots` are operations;
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
- **The flows move by name, the steps by column order.** `archivedFlowColumns` names every live flow column but
  `archived_at`; the steps move with `RETURNING *` / `SELECT *`, so the day's steps table must keep the live column
  order (a rule in `internal/migrations/CLAUDE.md`). Both pinned by `TestMove_NamesEveryFlowsColumn`. On PostgreSQL
  each table moves in one `DELETE ... RETURNING` into an `INSERT`, so what is archived is exactly what is deleted;
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

`DeleteArchivedTree` (Delete, one tree, no sizing) and `DeleteArchivedRoots` (Purge) run on the caller's context:
no attempt deadline and none of the pass's faults, since an operator's call has its own deadline
(`TestReaperContract_RemovalsRunOnTheCallersContext`). Purge's row budget is the engine's `reapBatchRows`, the same
value as a pass's `BatchRows`, passed in explicitly. `DeleteArchivedRoots` returns the trees its committed transactions deleted even when a later one fails,
and the engine records `dwarf_flows_removed` from that count, once per shard per call. A day whose tables are gone
deletes nothing and is no error. `FaultArchiveDelete`, targeted at a root, fails the transaction that would delete it.
Pinned by `TestDelete_ADroppedDayIsNoError` and `TestDelete_APartialRemovalCountsWhatCommitted`.

## Tests

Package tests run on a rig (`rig_test.go`): an isolated shard database through `internal/database`, an archive with
today's tables made, enabled seams and a log counter. Trees are seeded by direct `INSERT` (`seedTrees`), as valid
closed trees. The engine keeps the behaviour oracle - archived reads, Purge, Delete, DeleteOnCompletion, expiration,
the archive metrics - and its own copy of the seeder, since this package's tests cannot import `internal/enginetest`
(it reaches this package through `engineseams`).
