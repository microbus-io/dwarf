# Dwarf `migrations` — schema, indexes, and SQL-authoring rules

> Load when: editing schema/migrations or indexes, or writing any SQL query.
> Coupled with: root `CLAUDE.md` — the engine issues these queries; see its landmine list.

## The migration sequence: what must never change once deployed

The live tables' migrations are `live/<n>.sql`, and each day's archive tables' are `archive/<n>.sql` (below).
`database.Open` runs `db.Migrate(sequenceName, migrations.Live())` (`internal/database/database.go`). `sequel`
records progress per **sequence name** in `sequel_migrations(seq_name, seq_num, completed)` and, on every open,
runs only the files numbered **above the highest completed `seq_num`** for that name. Every rule below follows
from that watermark.

**Before v1.0.0 only the first rule below is upheld.** Through v0.x shipped files are edited in place and any
release may change the schema, because there are no production databases to protect: an upgrade is a maintenance
window, and a database made by an earlier release does not pick up an edit. The other rules are written down now so
that a change which breaks one is recognized as breaking every existing database. When a change does, tell the user,
and give the statements an existing database needs by hand in the release notes, as
[Upgrading](https://docs.dwarf.run/operating/upgrading/)'s "Upgrading From v0.14.0" does.

- **`sequenceName` (`"github.com/microbus-io/dwarf"`) never changes, and is never derived from the module
  path.** A new name reads a watermark of 0 and replays `1.sql` against a database whose tables already exist -
  at best a failed open, at worst a half-applied re-run. That includes a future `/v2` module path: the literal
  stays as it is.
- **A file that has shipped in a tagged release is immutable.** An edit never runs on a database already past
  that number, so editing it forks the schema - new installs get the edit, upgraded ones do not, and nothing
  reports the difference. A change is a new `N+1.sql`. A file has shipped when the latest tag carries it:
  `git ls-tree --name-only $(git describe --tags --abbrev=0) internal/migrations/live/ internal/migrations/archive/`.
  No checksum test guards it: whoever edits a shipped file can edit its checksum, so the rule is held here and by
  the release step that diffs `internal/migrations` against the previous tag.
- **Numbers only append.** The watermark is a `MAX`, not a set, so a file numbered at or below an applied number
  (a skipped number filled in later, a `2a.sql` slotted between) is silently never run on an existing database.
- **A new `N.sql` means a MINOR release at least, never a PATCH** (the published versioning policy in
  [Upgrading](https://docs.dwarf.run/operating/upgrading/)). A PATCH must run side by side with the earlier patches of its minor version during a
  rolling deploy, so it carries no migration - and no change to what replicas write to or expect from the
  shared tables either: a new meaning for an existing column or status, or a change to how replicas coordinate
  through them (leases and `lease_seq`, the claim, the peer registry and the residue classes it deals out, the
  archive day catalog). A MINOR may change the database and may need a maintenance window; the release notes say
  whether it does. Replicas of two versions share a database only where the policy says so - patches of one
  minor, and a MINOR whose release notes say it can roll; no other mix needs to work. In such a release, a change
  that could touch a stored meaning or that coordination - a fix in lease handling, say - is checked by running
  the previous release and the new one side by side on one database, through that path; a change that touches
  neither needs no mixed-version check. Before a PATCH is tagged,
  `git diff <its minor's .0 tag> -- internal/migrations` shows nothing.
- **One `N.sql` per change, covering every dialect.** The file is split on `;\n`; a statement carrying a
  `-- DRIVER: <names>` line runs only on those drivers, an untagged one runs on all. A dialect's statement
  missing from a shipped file cannot be added later by editing that file; it needs the next number.
- **A new file runs on populated tables that a released version made.** A column it adds carries a default every
  existing row can take, and that the engine reads correctly on a row written before the upgrade. Check it by
  opening, under the new code, a database the previous tag made and filled - running, interrupted and archived
  flows - and driving those flows on, on every supported dialect, since each runs its own statements: every
  fixture makes a fresh database, so the suites cannot see this.
- **A new file applies whole, or can run again from its first statement - which one depends on the dialect and on
  how many of its statements that dialect runs** (`sequel` counts them after `-- DRIVER:` filtering):
  - **Two or more, on PostgreSQL, SQL Server and SQLite:** one transaction, with the completion mark inside it. A
    failure leaves nothing applied and nothing marked, and the next open runs the file whole, so its statements need
    no guard. Such a file cannot hold a statement that refuses a transaction (`CREATE INDEX CONCURRENTLY`, `VACUUM`,
    `ALTER DATABASE`, `PRAGMA journal_mode`), nor begin or commit one itself. It holds every lock its statements take
    until it commits - on SQLite the whole database's write lock - so a long backfill or index build on a populated
    table goes in a file of its own.
  - **One, or any number on MySQL and MariaDB:** each statement runs on its own, outside any transaction, and the
    file is marked complete after the last: a statement that fails leaves the ones before it applied, and the next
    open runs the file again from the top. Guard each statement (`IF NOT EXISTS`; a `sys.indexes` or `COL_LENGTH`
    check on SQL Server), and where a dialect has no guard (MySQL 8 has no `IF NOT EXISTS` for `ADD COLUMN` or
    `CREATE INDEX`), make it that dialect's only statement in the file. A runner that dies between a statement and
    the mark leaves it applied and the file unmarked, and every later open fails on it (`already exists`). The
    repair is by hand: confirm every statement of the file applied, then mark the number complete with
    `UPDATE sequel_migrations SET completed=TRUE WHERE seq_name='github.com/microbus-io/dwarf' AND seq_num=<N>`
    (`completed=1` on SQL Server and SQLite). A MySQL or MariaDB file that failed partway is repaired the same way,
    its remaining statements run by hand first, or by undoing what applied and letting the next open run it whole.
  - **A `CREATE INDEX CONCURRENTLY`, alone in its file, is written unguarded.** A build that fails or is cancelled
    leaves an invalid index of its name, which `IF NOT EXISTS` would then skip for good: never used by the planner,
    and maintained on every write. Unguarded, the next open fails on `already exists` until the index is dropped by
    hand (`DROP INDEX CONCURRENTLY`). The re-run-safe pair, a drop then a create, is two statements, which this
    dialect runs in a transaction that both refuse.

  The shipped `live/` files are not guarded: they have only run on fresh databases, where a failed first open is
  dropped and made again. The `archive/` files are (below), so a day's sequence
  (`github.com/microbus-io/dwarf/archive/<day>`) needs no repair: the next run completes it.

## Schema Column Catalog

The `migrations/*.sql` migration files carry **no prose comments by design** - only the functional
`-- DRIVER: <dialect>` directives the `sequel` runner parses. All schema rationale lives here.

#### `dwarf_flows`

| Column | Meaning |
|---|---|
| `flow_id` | Per-shard auto-increment primary key. The external flowKey is `{shard}-{created_hour}-{flow_id}-{flow_token}` |
| `flow_token` | Random token component of the flowKey, guards against id guessing |
| `workflow_url` | URL of the workflow graph this flow runs (the resolve key passed to `Create` and the host's `LoadGraph`) |
| `workflow_name` | The graph's human-friendly display name (`NewGraph(name)`), denormalized from `graph.Name()` at every insert (`insertFlowTx`) and copied by `Fork`'s clone. Read by the lists (`FlowSummary.WorkflowName`, the `Query.WorkflowName` equality filter and the free-text search) and by `History`'s subgraph-child map. Deliberately unindexed, like every list filter (see "The lists get no live indexes of their own") |
| `graph` | JSON of the workflow graph, frozen at `Create` time |
| `graph_hash` | Hex SHA-256 (64 chars) of the `graph` bytes as `Create` stored them (`graphcache.HashOf`), carried verbatim by `Continue` and `Fork` - never recomputed from a copied column, whose bytes Postgres and MySQL re-spell. Keys the replica's shared graph-parse layer with `workflow_url`, so a dispatch that misses the per-flow cache finds the parse without reading `graph`; returned by the claim's flow read (the joined row on Postgres). `NOT NULL` with no default: every insert binds it. Never a fast or truncated hash - a collision runs a flow on another flow's graph. See `internal/graphcache/CLAUDE.md` |
| `baggage` | JSON of the opaque `baggage` map captured at `Create` and passed to every `LoadGraph`/`ExecuteTask` call. Flow-scoped and frozen at `Create`; the engine does not interpret it |
| `status` | Flow lifecycle: `created`/`running`/`interrupted`/`completed`/`failed`/`terminated`/`cancelled`. `created` is internal/transient only (Fork's clone holds its new root `created` until the clone is mapped) - a flow is never externally observed in `created`; Create inserts it `running` |
| `stopped_step_id` | The step the flow STOPPED at: the step that completed or lost it, or its interrupted step; `0` while it runs, and for a stop with no single step (Terminate of a running flow, a failed cohort resolved by a completing arrival). Written only by writes the row takes anyway - never per step (`engine/CLAUDE.md`). Read by the lists' join for `StoppedTaskName` |
| `surgraph_flow_id` | Parent (surgraph) flow id if this is a subgraph flow; `0` otherwise |
| `surgraph_step_id` | PK of the parent's parked surgraph step - the **sole** link from a subgraph child to the step that launched it, so completion/interrupt/resume bind to that exact step even with parallel parked surgraph steps at one depth. Do not link by the caller's step depth: it is ambiguous across parallel callers at one depth |
| `root_flow_id` | Denormalized **tree-membership** index: the `flow_id` of the root of this flow's subgraph tree, so the whole tree (root + all subgraph descendants) is reachable in one query (`WHERE root_flow_id=?`) instead of a recursive `surgraph_flow_id` walk. Write-once at creation, immutable: a top-level flow (`Create`/`Run`/`Continue`/`Fork`) is its **own** root (`root_flow_id = its own flow_id`, so the root's own row matches the scan); a subgraph child **inherits** the parent's. Single-shard by construction (subgraph parent-shard affinity). It augments, does **not** replace, `surgraph_flow_id`/`surgraph_step_id`: it answers *which flows are in the tree* (membership/set), not *who is whose parent* (structure) - ordered/structural walks (`surgraphChain`, `interruptedSubgraphChain`) still use the surgraph links. See "Denormalized root pointer" |
| `trace_parent` | W3C `traceparent` of the flow's root "workflow" span, minted at `Create` (or, for a subgraph, parented to the caller step's span). Reconstructed as the parent of every per-step span. Not inherited by `Continue`, which mints a new root span for its new flow; a subgraph inherits the caller step's context, not this column. See "Tracing" |
| `delete_on_completion` | Set from `FlowOptions.DeleteOnCompletion` at `Create`; `1` makes the flow *schedule* its own deletion (stamp `delete_after_ms`) when it reaches `completed`. Root-only (not inherited by children); `failed`/`cancelled`/`interrupted` flows are never auto-deleted. See "Data Retention" |
| `delete_after_ms` | Milliseconds added to `updated_at` to compute the reap time; `0` = keep (default), `>0` = the janitor's deletion jobs delete the flow's whole subtree (keyed on `root_flow_id`) once `updated_at + delete_after_ms <= NOW`. Stamped on the **root** only - by DeleteOnCompletion (= the 1-min grace, outcome observable meanwhile), by `Delete` of a closed flow not archived yet (= 1ms, due immediately), and by expiration in the UPDATE that terminates the root (= 1ms). `delete_after_ms > 0` always implies a terminal status; such a flow, and every flow of its tree, is excluded from the lists/`History` but still serves its outcome to `Snapshot`/`Await`, and `Step` serves its steps until the stamp is due. A tree larger than a batch is deleted in chunks, the root last. Indexed `WHERE delete_after_ms > 0` (partial on pgx/mssql/sqlite, full on mysql). See "Data Retention" |
| `final_state` | JSON state computed at termination - the full merged state of the terminal step(s), unfiltered. Narrowing happens in the workflow's terminal task via `flow.Del`. **Typed `JSON` on MySQL, not `TEXT`** - it carries arbitrary workflow output (same as `state`/`changes`, both `JSON` on MySQL), so a `TEXT` column would silently cap terminal state at 64 KB **on MySQL only** (pgx `TEXT` = 1 GB, mssql `VARBINARY(MAX)`, sqlite `TEXT` have no such limit), failing/truncating a large-output flow on one dialect. Never regress it to `TEXT`; it is never compared in a `WHERE`/`CASE`, so it dodges the JSON-`=` landmine below |
| `graph_bytes` / `baggage_bytes` / `final_state_bytes` | The length of the column before it as the engine wrote it, set by the same statement that writes the value; `0` for a value never written (`final_state` before the close). On PostgreSQL and MySQL the stored JSON may be re-spelled, so the count is the written size, not the stored one. They give a row's payload size without reading the payload. Unindexed: an index would make the close's write non-HOT. Every write of one of these payloads must set its count - pinned by `TestPayloadBytes_EveryWriterCountsWhatItWrites`, which drives every writer and, on SQLite and SQL Server, compares each count with the stored length. Each widens the row by 4 bytes, so the HOT check in the fillfactor section below applies |
| `forked_from_step` | `Fork` provenance: the *original* fork-point `step_id` this flow was cloned from; `0` for a non-fork flow. Subsumes the origin flow id (derivable via the step's `flow_id`) and pins the exact divergence node. See "Fork" |
| `created_at` | UTC creation time. Append-only and PK-correlated. Surfaced to tasks via `Flow.CreatedAt()`. A `Fork` clone is a new flow with its own `created_at` = fork time. **Deliberately unindexed on `dwarf_flows`**: no query on the flows table filters or orders on it (`ListOpen`'s window anchors on `updated_at`, and `ListClosed`'s and `Purge`'s on `archived_at`, the correct "time since finished" retention signal, and only closed flows are purged). The fairness scheduler's `created_at` ordering runs on the **steps** copy, which *is* indexed (see `dwarf_steps.created_at`); the flows copy is never a hot-path order/filter key. A `dwarf_flows.created_at` index would be pure write amplification; if a "created in window X" analytics filter is ever added, add the index with the `Query` field then |
| `created_hour` | The flow's creation hour: whole hours since 2000-01-01 00:00 UTC. **Written by the column's DEFAULT, on the database clock**, never bound from Go, so the key cannot disagree with the database at an hour boundary. The expression is per dialect because rounding is: `FLOOR` on PostgreSQL and MySQL (a `CAST` there rounds), `DATEDIFF(HOUR, ...)` on SQL Server (which counts hour boundaries, i.e. truncates), and integer division of `STRFTIME('%s')` on SQLite (not every SQLite build has `FLOOR`). 262968 is 2000-01-01 in hours since the Unix epoch. Read back by the INSERT that creates the flow (`RETURNING`/`OUTPUT`, or a `SELECT` by `LAST_INSERT_ID()` on MySQL - `queueInsertFlow`) and by Fork's clone, since the key carries it. Write-once; every step of the flow carries it as `flow_created_hour`. A Fork clone or a `Continue` turn is a new flow with its own hour. Every key-holder gate checks it with `flow_token` (`internal/keys/CLAUDE.md`). Unindexed: every read by it also names the primary key |
| `started_at` | UTC time this attempt began dispatching. Stamped when the flow goes `running` at `Create` (and by `Fork` when the clone goes live); there is no separate `Start`. Distinct from `created_at`, which is the row's INSERT moment. Drives `FlowSummary.Duration()` (`updated_at - started_at`) |
| `updated_at` | UTC time of the last **status transition** (`created`→`running`→terminal, `running`↔`interrupted`). Surfaced to tasks via `Flow.UpdatedAt()`. It is **not** bumped on intra-flow step progress: a running flow advancing through N steps leaves `updated_at` fixed at its go-`running` time - no step writes the flow row, and the per-step lock grabs (the transition, `completeFlow`, the fan-in) are locking reads on PostgreSQL and MySQL and flip the non-indexed `touch` column on SQLite and SQL Server, so the running band of `idx_dwarf_flows_status` does not churn once per step (it moves only when the flow's own status changes). Consequence: for a **running** flow `updated_at ≈ started_at` (so `FlowSummary.Duration()` reads ~0 until it stops); for a **closed** flow it is the finish time (Duration = total runtime, and `Purge`'s `ToTime` = "finished before" - both correct, since only closed flows are retention targets) |
| `touch` | Churn-avoidance toggle (`SMALLINT`, `0`/`1`). Carries **no meaning** and is never SELECTed - it exists only so a flow-row write can (a) acquire the row's write lock without moving the `(status, updated_at)` index entry, and (b) guarantee a value change so `RowsAffected()` reflects the `WHERE` match on every driver (MySQL's default "changed rows" count included). A `dwarf_flows` UPDATE whose matched count is read must change a value on every row it matches; the status-transition writes flip it (`touch=1-touch`) *alongside* `updated_at=NOW_UTC()`, and a write that always moves another column (the `delete_after_ms` mark, the creation fix-up of a new root's own-id `root_flow_id`) does not flip it. A self-assign (`SET col=col`) was rejected for (b): MySQL reports 0 rows changed when no value differs, which would silently break the terminal-status `RowsAffected==0` guards. **A lock that writes nothing else is not an UPDATE on PostgreSQL or MySQL** (`lockFlowRowsSQL`): there it is a locking read (`FOR NO KEY UPDATE` / `FOR UPDATE`), because on PostgreSQL every UPDATE writes a new row version even when it changes nothing, so a lock-only UPDATE on the row every step locks would be a row version per step on a row that otherwise changes a few times per flow (see the fillfactor section). SQLite and SQL Server still flip `touch` for it |
| `archived_at` | Archive filing time: an atomic move's clamped transaction time, or a staged copy's attempt time plus its stamp group. Defaults to `9999-12-31 23:59:59` on live rows. Publication records the archive root's time on the live root too, so cleanup accounting recognizes an archived tree even after Delete clears its attempt; live descendants retain the default. This is accounting provenance, not the publication/visibility guard. Fork clones take the default. Indexed only on archive days. SQLite spells the sentinel with `.000` for consistent text comparison. |
| `archive_attempt_at` | When a live root's archive attempt began (`NOW_UTC()`): its staged copy's day and identity. `2000-01-01 00:00:00` (`.000` on SQLite), "no attempt", by default; MySQL allows no function default. **Every stamp of `delete_after_ms` but a publication writes "no attempt" in the same UPDATE** (Delete, live retention's, a retention of 0's stamp before a chunked deletion; an expiration's mark need not, on a root its UPDATE terminates, which as an open root never had an attempt), so a root stamped with an attempt is one published to the archive whose live copy is being deleted (`janitor.AttemptedSQL`). **Live only:** the day tables do not have it, so the move names its columns without it and the column-parity tests skip it |
| `priority` | Scheduling priority, `1..max` (the engine's `SetMaxPriority`, 5 by default), lower runs first. Resolved at `Create` from `FlowOptions` else the midpoint of the range; inherited unchanged by `Continue`/subgraph. Immutable. The refiller counts bands `1..max` one by one and every priority beyond `max` together as an overflow served last, so a row outside the range (written before the range existed, or by a replica with a larger one) still runs |
| `fairness_key` | Fairness bucket. From `FlowOptions`, else `''`: at most 256 ASCII letters, digits, `-`, `_` and `.` (`validFairnessKey`), so its byte, character and UTF-16 lengths agree with every dialect's column width. Immutable. **Deliberately unindexed on `dwarf_flows`** (it *is* indexed on `dwarf_steps` as the hot selection key). The only reader on the flows table is `ListOpen`'s `Query.FairnessKey` filter (`ListClosed` and `Purge` read the day tables) (the documented "list tenant X" path), a cold per-shard scan on a warm operator path - not worth a secondary-index entry per insert against the write-amplification budget. If tenant-scoped listing ever becomes a routine operational path, add `(fairness_key, flow_id)` with the filter then. `ListOpen`'s window needs no index of its own: its branches range over `idx_dwarf_flows_status`, and a standalone `updated_at` index would amplify writes on a churny column - exactly the cost the dropped `created_at` indexes carried. `workflow_name`/`priority` filters are likewise deliberately unindexed |
| `fairness_weight` | Relative dispatch share of the `fairness_key`: a whole number, `1..1,000,000` (`maxFairnessWeight`). From `FlowOptions`, else `1`. An integer, not a float (4 bytes on every dialect but SQLite, whose `INTEGER` is variable-width): the planner takes only ratios, and an integer is exact in every dialect, driver and JSON reader. With the default at 1 no key can be given less than the default share; raising the other keys says that |
| `error` | Task error string for `failed` flows. Written by `failStep` to the **failing flow only** (`WHERE flow_id=? AND status NOT IN (terminal)`, so first-failure-wins on that flow). Cross-subgraph failure does **not** write the whole chain in one UPDATE - it bubbles up level-by-level: when a subgraph child fails (via cohort accounting, never eagerly - see "Failure back to the parent"), `deliverFlowFailureToParent` surfaces the error to the parked caller step (`subgraph_error`), whose task re-fails on re-dispatch, writing `error` on the parent flow, and so on to the root. (`deliverSubgraphError` does the same re-dispatch from the wedge sweep only.) Surfaced as `FlowOutcome.Error` |
| `cancel_reason` | Reason passed to `Cancel` or `Terminate` - one column shared by both. `Cancel` writes it to every open flow in the tree when the request is recorded, before any step is marked, so a step claimed in between reads it. **May be empty** (a reason is optional), so it is never the test for "was this flow cancelled" - `cancel_watermark` is. Surfaced as `FlowOutcome.CancelReason` |
| `cancel_watermark` | `0` until `Cancel` runs, then the flow's highest `step_id` at that moment (every flow has an entry step, so it is then at least `1`). Two jobs: a successor inherits the cancellation when its source step is at or below it and was not itself covered - which closes the gap between a step's completion write and its transition - and Cancel's own step mark reaches only steps at or below it, so a step inserted afterwards keeps the mark its insert decided. **Nonzero is also the "already cancelled" test**: a flow is cancelled once, a repeat `Cancel` records nothing new and only re-applies the first call's watermark-bounded mark, and the write is guarded on `cancel_watermark=0` so concurrent calls raise it once |
| `time_budget_ms` | Per-flow task time budget, resolved from `FlowOptions.TimeBudget` (else the `SetTimeBudget` default) and frozen at `Create`; the engine imposes no ceiling (a host bounds it before `Create`). Seeds every step's `time_budget_ms`. Inherited by subgraph children **and** by `Continue`/`Fork` (which carry the source's policy). Always stored concrete at `Create`; a `0` is unexpected and falls back to the live engine default at step insert (pure defense) |
| `payload_ver` | The format of every payload column of every step row of this flow (`state`, `changes`, `state_refs`, `state_reduced`). Stamped at creation with the creating build's format and never raised; Fork's clone copies its origin's. A build reads formats up to its own `payloadVer`: it defers a step of a newer flow at dispatch and refuses the flow's step rows everywhere else, and it writes into a flow in the flow's format (see "Payload format versions" in `engine/CLAUDE.md`, which holds the checklist for introducing a new version). Read at the claim and joined onto the final-state tails read, never in a predicate. It widens the row by 4 bytes, so the HOT check in the fillfactor section below applies |
| `engine_id` | Forensic provenance stamp: the random per-process id (fresh on every restart) of the engine replica that INSERTed the row - `Create`/`Continue`'s `insertFlowTx` and `Fork`'s clone stamp the creator. Never read by the engine; **deliberately unindexed** (an index on hot-insert tables would be pure write amplification for a rare one-off forensic query). `0` (the column default) = not stamped. The same random per-process id is what a replica writes into `dwarf_peers` to be counted for the connection-pool split (see that table below); on the flows/steps rows it is provenance only |

#### `dwarf_steps`

| Column | Meaning |
|---|---|
| `step_id` | Per-shard auto-increment primary key. External stepKey is `{shard}-{flow_created_hour}-{flow_id}-{step_id}-{step_token}` |
| `flow_id` | Owning flow |
| `step_depth` | Sequential transition depth; fan-out siblings share it. **Purely informational** (History ordering + the surfaced `FlowStep.StepDepth`, useful to see how deep a flow goes) - it is *not* used for the execution DAG (that is `predecessor_id`/`successor_id`), fan-in firing (`lineage_id`/cohort counters), final state (tail steps), or selection. The entry step is `callerStepDepth+1` (1 for a top-level flow; a subgraph continues from its caller's depth); a fan-in step is `max(cohort step_depth)+1` |
| `step_token` | Random token component of the stepKey |
| `task_name` | Graph task name of the task this step executes |
| `task_url` | The task's dispatch URL (`graph.SetURL`), resolved from the flow's graph at insert and copied by `Fork`. **Read by nothing in the engine** - dispatch resolves the URL from the frozen graph (`dispatchURLOf`) - so it is a forensic record of where the step was sent. Unindexed |
| `state` | JSON input snapshot. Immutable except on retry/resume. A field carried by REFERENCE is **absent** here - see `state_refs` |
| `changes` | JSON output delta the task produced: `workflow.Changes`, one object holding a contribution under its field's name, an overwrite under `name!` (a delete is `"name!": null`), and an addition under `name+` (the elements a list with no reducer gained, recorded by the engine in place of a rewrite that only extended it). Decode it as `Changes` (`changesOf`), never as a plain `State`, which reads `name!` as a field of its own. Always literal: a ref never appears in `changes`, which is what lets one refs column cover the whole scheme |
| `state_refs` | `{"<field>": <anchor step_id>}` (a forEach element: `"<field>": "<anchor> <array field>[<index>]"`, an item ref into the array; a field under `append`, or extended by an addition: a list `"<anchor><s\|c>[a:b];..."` of the rows holding its elements - the grammar is `internal/staterefs/CLAUDE.md`'s "Lists") - fields whose bytes live in another step's row rather than in this step's `state`, so a large carried field is stored once instead of once per step (measured ~29x fewer state bytes on a fan-out doc-extraction shape). The anchor's bytes may be in **either** its `changes` (a task produced them) or its `state` (the flow's initial input at the entry step; a fan-in's reducer output), so resolution reads both, `changes` shadowing `state`. Own column, not an inline `$ref` key, so `Fork`'s DB-side `INSERT...SELECT` clone can remap anchors with one tiny UPDATE instead of pulling every large state blob through the engine. Empty (`'{}'`) for the overwhelming majority of rows. The full design is in `engine/CLAUDE.md` |
| `state_reduced` | On a fan-in step inside an outer cohort only: per field, which part of the folded value in its `state` its cohort added (`{"log": {"from": 3}, "total": {"value": 5}, "tags": {"keys": ["a"]}}`), so the next cohort out takes that fan-in's contribution from its one row (`cohortParts`). `'{}'` on every other row. Written only by the fan-in INSERT, read only by `cohortParts`, copied by Fork's clone. Its own column so the record never travels with the state: a successor's row that inherited it would carry a boundary that does not describe its own input. See "A member's contribution is its `changes`, EXCEPT an inner fan-in's" in `engine/CLAUDE.md` |
| `payload_deferrals` | How many times dispatch has deferred this step because its flow's `payload_ver` was newer than the dispatching build reads, over the step's whole life; the bound that fails it reads this. Kept apart from `attempt`, which a task sees through `flow.Attempt()`. Not copied by Fork |
| `interrupt_payload` | JSON outbound payload from `flow.Interrupt()` - what the awaiting caller sees |
| `interrupt_done` | `1` once the interrupt park has been resumed; drives `flow.Interrupt`'s return-vs-arm decision |
| `resume_data` | JSON inbound payload recorded by `Resume`; returned by `flow.Interrupt` on re-dispatch. `'{}'` until resumed |
| `subgraph_done` | `1` once a `flow.Subgraph` park resolved; drives `flow.Subgraph`'s return-vs-arm decision. A retry clears it to re-run the child |
| `subgraph_result` | JSON child `final_state` returned by `flow.Subgraph`. `'{}'` until resolved |
| `state_bytes` / `changes_bytes` / `interrupt_payload_bytes` / `resume_data_bytes` / `subgraph_result_bytes` | The length of each payload column as the engine wrote it, set by the same statement that writes it; `0` for a value never written (the column's `'{}'` default). Each column sits right after its payload, in the live and the archive day tables alike; an existing table added them at its end, which the archive's copies, by name, do not mind. As `dwarf_flows`' `*_bytes` above: the written size, unindexed, and every writer sets its count (`TestPayloadBytes_EveryWriterCountsWhatItWrites`) |
| `subgraph_error` | JSON: the child's error as `TracedError` JSON (status code and properties kept, a cancellation marker among them) for a failed `flow.Subgraph` park, returned as the `err`. `'{}'` when none, which the claim read maps to no error (`subgraphErrorOf`) |
| `status` | Step lifecycle: `created`/`pending`/`running`/`interrupted`/`completed`/`failed`/`terminated`/`cancelled` |
| `error` | Error text when `failed`; `''` otherwise |
| `time_budget_ms` | Execution budget; the deadline on the `ExecuteTask` call context. Denormalized from the flow's `time_budget_ms` at step insert (frozen, not the live config), and also self-referenced in the claim CAS to size the crash-recovery lease (`time_budget_ms + leaseMargin`) |
| `attempt` | `flow.Retry` attempt counter, drives the backoff |
| `not_before` | Earliest UTC time the step may execute (`flow.Sleep` / retry backoff). Once past, it reads as **"due since"**: the steal grace, the dispatch-lag gauge and the per-band backlog age all measure from it. So **every write that returns a step to `pending` stamps it** - `NOW_UTC()` for a step due at once (a subgraph revive or error re-arm, a resume, a lease reset, a lease release, an orphan rewind, Fork's re-arm), a future time for a sleep or a backoff. A reset that leaves it alone makes a step that waited three days on an interrupt read as three days overdue the moment it is resumed, and stealable by every peer at once |
| `lease_expires` | Crash-recovery lease; `recoverExpiredLeases` reclaims `running` steps past this |
| `lease_seq` | Lease **generation** (write fence). Bumped `lease_seq=lease_seq+1` in the claim CAS and returned with the claimed row; every post-execution write to the dispatched step carries `AND lease_seq=?`, so a worker whose lease was lost and re-granted to a peer (slow task overran `budget+leaseMargin`, or the DB wall clock stepped forward past `lease_expires`) writes zero rows and abandons instead of corrupting/terminalizing the peer's re-execution. A genuine `WHERE` predicate, so no `touch` column is needed - **but MySQL's `RowsAffected` counts CHANGED rows, so every fenced write must change a value on every row it matches** (in practice the status); one that can leave its row unchanged reads as fenced on MySQL alone and silently drops a legitimate write. Bumped by the claim (a lease *grant*) and by the orphan sweep's rewind of a `completed` step (which must fence a worker already past its completion gate); the `recoverExpiredLeases` expired-lease reset leaves it unchanged, so a step reset-but-not-reclaimed keeps its generation. See "Lease fencing" in `engine/CLAUDE.md` |
| `created_at` | UTC creation time. Read by the refiller two ways: projected as the fairness `ageMs` (`NOW - created_at`), and as the **oldest-first ordering key within a `fairness_key`** in the fetch (`ORDER BY created_at, step_id` in its lateral, or the same `ROW_NUMBER()` ordering where there is none). For that ordering it is the **trailing pair** of `idx_dwarf_steps_selection` (`..., created_at, step_id`), so a key's due steps come off the index already age-ordered and the fetch needs no sort. Unlike `dwarf_flows.created_at` (unindexed - nothing on the flows table orders on it), the steps copy earns its place in the *existing* hot selection index; it adds no new index and no churn (`created_at` is write-once, and the index's leading `(status, parked)` already repositions every entry on each transition regardless) |
| `flow_created_hour` | Its flow's `dwarf_flows.created_hour`, copied at insert - NOT the step's own creation hour, which is why it is named for the flow: a step created days into a long flow carries an hour days older than its `created_at`, and a Fork clone's prefix steps keep the origin's `created_at` while carrying the clone's hour. The entry step reads it from the flow row inserted just before it, a successor or fan-in step binds the claimed step's, and a Fork clone binds the new flow's (never the origin's). **No default, on purpose**: a step insert that forgets it fails, where a default would put the step under the wrong hour silently. The step key carries it, and the step-key gates check it with `step_token` |
| `started_at` | UTC time the *current attempt* first dispatched. The lease UPDATE stamps it via CASE only on a fresh attempt's first dispatch (`attempt=0 AND subgraph_done=0 AND interrupt_done=0`) and **preserves** it on a continuation (subgraph re-dispatch, interrupt re-dispatch, retry re-dispatch). A retried step's duration includes every attempt. Drives per-step body duration and inter-step wait labels in `FlowRenderer` |
| `updated_at` | UTC time of the last status transition |
| `lineage_id` | Cohort frame: the spawn step's `step_id` on a push, else inherited. Drives explicit `SetFanIn` arrival counting and merge, and the fan-in hold (the refiller's fetch projects it, so `idx_dwarf_steps_selection` carries it, and on mysql/sqlite `idx_dwarf_steps_due` too). A cohort-counting device, **not** a DAG. `0` = no `SetFanIn` |
| `cohort_size` | On a fan-out spawn step: number of branches spawned |
| `cohort_arrivals` | On a fan-out spawn step: branches that reached the fan-in; fan-in fires when `arrivals >= size` |
| `cohort_failures` | On a fan-out spawn step: branches that failed via `failStep` with no `onError` (bumped by `propagateCohortFailure`). When the cohort fully arrives with `cohort_failures > 0` the flow is failed instead of creating a fan-in step. `0` = none |
| `cohort_cancellations` | On a fan-out spawn step: how many of the `cohort_failures` were cancellations (bumped beside it by `propagateCohortFailure`, by the loss's `cancelled` flag). A resolving cohort passes a cancellation up only when `cohort_cancellations == cohort_failures`, so one real failure anywhere below makes every enclosing resolution a failure. `Fork` recomputes it per branch (a branch holding cancelled steps and no failed one) and zeroes it on the leaf. `0` = none |
| `fan_out_ordinal` | This branch's index in its fan-out; fan-in merges in this order so the order-sensitive reducers (`append`/`union`/`concat`/`merge`) fold in input order, not completion order. Preserved across an in-place `flow.Retry` rewind and copied verbatim by `Fork`. `0` = not part of a fan-out |
| `predecessor_id` | Step that ran immediately before this one in the execution DAG. `0` = none |
| `successor_id` | Step that runs immediately after this one. `0` = none (exit) |
| `priority` | Denormalized copy of the flow's `priority` for the hot selection path |
| `fairness_key` | Denormalized copy of the flow's `fairness_key` |
| `fairness_weight` | Denormalized copy of the flow's `fairness_weight` |
| `advancing` | `1` = the step's completion write landed and its transition transaction has not committed. Set by the completion UPDATE, cleared (fenced on `lease_seq`) by whichever statement of the transition transaction already writes the dispatched row; left set when the transaction terminalizes the flow, where it is never read. On a `completed` step of a `running` or `interrupted` flow it marks a lost transition the orphan sweep may rewind; on a claimed step it means **replay** - the task is not run again, and the transition is re-derived from `changes` and the `advance_*` columns. **Unindexed, deliberately** - the sweep reaches it by `flow_id` from the open flows it lists, and an index on it would churn twice per step. See "Background Recovery" #3 in `engine/CLAUDE.md` |
| `advance_goto` / `advance_sleep_until` / `advance_on_error` | The control facts a step's transition needs beyond its `state` and `changes`, recorded by the completion write beside `advancing`: the task's `Goto` target (`''` = none), its `Sleep` as a deadline (`DATE_ADD_MILLIS(NOW_UTC(), ?)` - the claim returns the time left, so a replay keeps the original wake time), and `1` if the completion was error-routed to an `onError` handler (set also by the Cancel-covered reroute). Read only by a replay; meaningless while `advancing=0`. Unindexed |
| `cancelling` | `1` = a graceful `Cancel` covers this step. Set by `cancel()` on every `pending`/`running`/`interrupted` step of the tree, and decided by the INSERT that writes each successor, from the flow's `cancel_watermark` (a scalar subquery: per row in `successorRowSQL`, once per statement in PostgreSQL's array insert, `successorArraySQL`). Consumed at two checkpoints, each reading it off a write it already makes: the claim CAS returns it (a marked step is preempted unrun), and the completion write returns it (`RETURNING`/`OUTPUT`, a follow-up `SELECT` on MySQL). Never cleared - the mark stays on the row, so a rewound retry or a resumed interrupt is preempted at its next claim. `SMALLINT` on Postgres: scan it into an `int`, never a `bool`. Unindexed. See "Cancel" in `engine/CLAUDE.md` |
| `parked` | Selection discriminator. `0` = active; `1` = surgraph park. The selection index leads with `(status, parked)`, the due index with `parked` on pgx/sqlite and `(status, parked)` on mssql/mysql, and the claim CAS requires `parked=parkedNone`, so non-zero rows are excluded from the hot path. See "Step Parking" |
| `parked_child_flow_id` | The subgraph child that owns this step's park, `0` when none. Written by the spawn's insert, cleared by the delivery that consumes the park and by the Retry rewind; every delivery is fenced on it, so a stale child cannot settle a park a Retry made for a newer one. It must live on this row: a delivery's PostgreSQL snapshot predates its row lock, so the child flows read in its predicate can be stale. Fork's clone does not copy it and re-points a re-parked ancestor at its cloned child |
| `engine_id` | Forensic provenance stamp, dual-role: the INSERT stamps the **creator** (entry step, transition successors, fan-in step, Fork clone - the replica whose worker created the row) and the claim CAS **overwrites** it with the **claimer** (the replica that leased and ran this attempt; rides the existing UPDATE, zero extra round trips). So a pending row shows who created it, a running/terminal row who executed it. Never read by the engine; deliberately unindexed; `0` (the column default) = not stamped |

#### `dwarf_flow_days`

The archive's directory: the day each flow was archived on, for the flows a locate would not find by its creation
hour alone - those archived two or more days after their creation day, or on a day before it (`live/4.sql`). Written
by the janitor in the move's transaction, read by the archiver's locate, deleted by retention
(`internal/archiver/CLAUDE.md`, "Locating an archived row"). Sparse by design - a flow archived on its creation day or
the day after has no row - so it holds only flows that ran about a day or longer. Its name carries no `_archive`, which
`live/` may not name.

| Column | Meaning |
|---|---|
| `flow_id` | **Primary key** - the archived flow (a root or a subgraph child; a step is found by its flow) |
| `day` | The archive day its tree is in. Indexed (`idx_dwarf_flow_days_day`) for retention's delete of the oldest days, whose rows are at the low end of an index whose inserts arrive at the high end |

#### `dwarf_peers`

The replica registry behind both fleet decisions - which replicas dispatch a shard (and so split its
connection budget) and how many demonstrably serve it (which divides its work). One row per **live**
engine replica, written to **every** shard, and read **per shard**: every timestamp here is stamped by the shard
holding it, so ages are comparable within a shard and timestamps are comparable nowhere. The mechanism is
`internal/peers/CLAUDE.md`; the engine's use of it is `engine/CLAUDE.md` §"Peer discovery".

| Column | Meaning |
|---|---|
| `engine_id` | **Primary key** - the replica's per-process id (the same value stamped as provenance on `dwarf_flows`/`dwarf_steps`). PK, not a mere index: it is *identity*, so a heartbeat is a clean update-by-id and the astronomically-unlikely id collision surfaces as a loud constraint violation rather than a silent double-count. Random and fresh on every restart *by default*, so a restarted replica inserts a new row and its old one ages out (never updated again) - unless the host pins a stable id via `SetEngineID` (see `engine/CLAUDE.md` §"SetEngineID - a stable replica identity"), in which case a restart re-upserts its one row and leaves no stale row behind |
| `seen_at` | UTC heartbeat timestamp, `NOW_UTC()` at every write (never a bound Go time - one clock per shard). A replica is **counted** toward the pool divisor while this is inside the fresh window (40s, some 40 beats - generous because under-counting over-sizes every pool derived from it, and the beat is itself a pooled write that can starve for a connection under load). A crashed peer, whose owner sends no goodbye, drops out on its own; its dead row becomes eligible for the hygiene **DELETE** past the straggler age (80s), and even then only once the registry has been continuously readable for five minutes. The delete names ids explicitly rather than filtering on a timestamp range, so the server never re-evaluates a clock the deleting process did not observe - which is what keeps a stall that ages every row at once from emptying the registry fleet-wide - and it never includes the deleter's own row. Shutdown deletes the row outright |
| `joined_at` | When the row was INSERTed: the replica joined this shard's registry. **Read by nothing in the engine** - it is there for an operator. Do not order the dispatcher cut by it: each shard stamps its own, so replicas that start together order differently per shard (see `ordering_seq`) |
| `ordering_seq` | The replica's place in the order the dispatcher cut tiles, lowest first, ties to the lower `engine_id` (`internal/peers/CLAUDE.md`, "Who dispatches"). Written by the replica, the same value on every shard, by registration (INSERT and UPDATE alike) and never by the beat: the engine's start time in Unix milliseconds, or for a pinned-id restart the oldest value its surviving rows hold on any shard, so a pinned-id restart and a registration repair keep their place and every shard's row agrees. The one value here that compares across shards, because no shard stamps it. `NOT NULL` with no default, so a writer that forgets it fails rather than ranking first |
| `dispatched_at` | UTC time this replica last *demonstrably* served the shard - written `NOW_UTC()` only by a heartbeat that has a **completed refill cycle** behind it. It is evidence rather than a claim, and that is the whole reason it exists: **do not add a flag saying "I dispatch"** - a replica that set one and then wedged would keep its residue class of `step_id` forever and nothing would ever select those steps, whereas a wedged replica simply stops advancing `dispatched_at` and drops out of the partition divisor on its own while `seen_at` keeps it in R where it belongs. A cycle that found *nothing due* still counts - it proves the replica looked and could have served; gating on candidates instead would make a quiet fleet read as having no dispatchers at all. An **idle** replica runs no cycle, so it never advances this and needs no flag to say so. Defaults to `'2000-01-01 00:00:00'` - a **literal** constant, not a computed `NOW_UTC()`-relative default, because MySQL admits no function-based expression default other than `CURRENT_TIMESTAMP`; the literal is decades stale against any clock, so a freshly-inserted row correctly reads "never dispatched" until it earns otherwise. The default is the pessimistic posture on purpose - absence of evidence must not read as evidence, and the cost is one heartbeat of under-counting rather than an indefinitely stranded residue class. The registry answers two different questions and conflating them strands work: **R** counts *every* fresh row, since an await-only replica still holds connections, while the **candidate partition** divides across dispatchers only |
| `working` | `1` when the replica has workers and is taking on work; `0` when it is configured to run no steps (`SetWorkers(0)`) or has started draining at shutdown. Written at registration and by `Withdraw` (the drain), never by the beat. Read **only by the dispatcher ranking**, which skips a `0` row so it cannot win a slot it will not serve, while it still counts in R. **Not evidence of serving** (see `dispatched_at` for why the registry must never carry a claim to be serving): it states intent, and the partition never reads it - a replica that says `1` and then wedges holds a dispatcher slot until its row goes stale, but strands no residue class. Defaults to `1`, so a row written by anything that does not set it ranks normally |
| `draining` | `1` while the replica holds a dispatcher-sized share of the shard's connection budget from **outside** the dispatcher cut - demoted with claimed steps still finishing, or withdrawn at shutdown while its drain runs; `0` otherwise. Written by the beat (it moves during the life of the process), by `Withdraw`, and at registration. Peers add the count of draining rows to the dispatcher count when they divide the budget, so the drained share is divided around rather than grown into. **Feeds the pool divisor only, never the partition** - so unlike a claim to be serving (see `dispatched_at`), a row stuck at `1` costs headroom, not stranded work. Independent of `working`: a demoted replica is `working=1, draining=1`, a withdrawn one `working=0, draining=1`. Defaults to `0` |
| `rtt_us` | The round-trip time the replica probed to this shard at Startup, in whole microseconds, written at registration. **Read by nothing in the engine** - it exists so an operator can see the fleet's distances from one `SELECT`. An integer rather than a float so the column is one type on every dialect; the probe's jitter (~0.1 ms) is far coarser than a microsecond. `0` = not recorded. Do not rank or size from it: a replica far from every shard would never be chosen, and each replica already sizes from its own probe |

**No secondary index, by design.** The table holds one row per live replica - a handful, effectively always in the
buffer pool - so every statement against it runs as a trivial full scan. A `seen_at` index would be pure write
amplification on a hot-ish row for zero read benefit. The PK on `engine_id` is *identity* (the upsert target), not a
scan-speed index. The read is deliberately **unfiltered**, for the same reason: it is a whole-table scan either way,
and returning every row is what lets the freshness windows live in Go, gives the hygiene delete its candidates from
the same reading everything else is derived from, and keeps a row that is ABSENT distinguishable from one that is
merely stale.

## Database Indexing Strategy

The `dwarf_flows` and `dwarf_steps` tables grow indefinitely. The indexing strategy keeps hot-path queries fast
without fragmentation or excessive write amplification.

### Design Principles

1. **Append-only terminal sections.** Indexes leading with `status` partition the B-tree by status. Terminal
   statuses are append-only (entries arrive with monotonically increasing `updated_at`), so terminal sections stay
   well-ordered - no mid-tree page splits.
2. **Small transient sections.** The `pending`/`running` sections churn but stay small (proportional to active work,
   not history); page reuse is efficient.
3. **Partial indexes where the dialect has them.** Where only non-terminal statuses are queried, the index is
   filtered to `status IN ('pending','running')` on **pgx, sqlite and mssql** - SQL Server calls them *filtered*
   indexes and the schema does use them, see the `-- DRIVER: mssql` blocks and the filtered-index hazard in
   "Status predicates are inlined literals" below. **MySQL alone has no partial index support** and takes the
   full composite.
4. **A column used ONLY in a partial predicate still belongs in the KEY.** On a dialect that filters, that column
   is constant inside the index, so it reads as dead weight in the key and the temptation is to drop it. Do not.
   **MySQL's copy of the same index is full**, and that column is the only thing that lets a scan *seek past* the
   rows the predicate would have excluded - drop it and MySQL filters them one row at a time, silently, and on
   that dialect only. The asymmetry is what makes this a rule rather than a judgement call: keeping it costs a
   few bytes per entry on the dialects that filter (a constant column compresses well), dropping it costs a full
   walk of the excluded population on the one that cannot express the filter at all. **Keep the KEY identical
   across all four dialects and let only the PREDICATE diverge** - the same split already used for `INCLUDE`
   versus trailing key columns.
   Corollary, and it is the sharp edge: **adding a column to a filtered predicate obliges every query that wants
   the index to compare that column against an INLINED literal**, for exactly the reason status does (below).
   `parked` is compared as a bound `parked=?` in every `WHERE` that tests it today, so a future `parked=0`
   predicate would silently defeat the filtered index on SQL Server and SQLite until every such site inlines it.

### Index Catalog

#### `dwarf_flows`

| Index | Columns | Purpose |
|---|---|---|
| PK | `(flow_id)` | Row lookups by flow ID |
| `idx_dwarf_flows_status` | `(status, updated_at)` | `ListOpen`'s branches (one per open status, each a range seek on this exact prefix in its order), the janitor's closed scans outside PostgreSQL (one branch per terminal status), and the orphan sweep's `status IN ('running', 'interrupted')` listing (`recoverOrphanedFlows`). Its running band does not churn per step: no step writes the flow row, and a lock grab never moves `updated_at`, so a running flow's entry moves only on a genuine status transition (see the `updated_at`/`touch` catalog rows) |
| `idx_dwarf_flows_closed_roots` | `(status, updated_at, flow_id) WHERE surgraph_flow_id=0 AND delete_after_ms=0`, PostgreSQL only | The janitor's closed-root scans and oldest-root probes, excluding the live descendants of staged trees. A 250,000-flow tree otherwise makes a one-root probe read the whole table. The general status index remains for open-flow and orphan scans |
| `idx_dwarf_flows_workflow_url` | `(workflow_url)` | The lists by workflow URL |
| `idx_dwarf_flows_surgraph` | `(surgraph_flow_id)`, partial `WHERE surgraph_flow_id > 0` on pgx/sqlite/mssql | Walking the subgraph chain |
| `idx_dwarf_flows_surgraph_step` | `(surgraph_step_id)`, partial `WHERE surgraph_step_id > 0` on pgx/sqlite/mssql | Point lookups on the caller-step link (`WHERE surgraph_step_id=?`): the `flow.Retry` reap (`fork.go`), the parked-step wedge sweep, and `Step`-navigation. Cheap to carry: `surgraph_step_id` is write-once at insert (no update churn/fragmentation) and `0` for every root flow, so the partial index covers only subgraph children. Without it, the retry-reap's unindexed predicate is the SQL-Server clustered-index-scan U-lock **deadlock** class (the same one the fan-in `successor_id` write avoids by targeting primary keys; see that write in `dispatch.go`). Same profile as `idx_dwarf_flows_surgraph` above. **Deliberately single-column**, not `(surgraph_step_id, flow_id)`: the `ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)` probes on this column - the wedge sweep's latest-child lookup (`wedge.go`) and `Step`-navigation's descent into a child subgraph (`history.go`: the own-child probe and `skipSurgraphForward`/`skipSurgraphBackward`) - each match only the handful of retry-attempt children of one caller step, and History's subgraph walk is one batched `root_flow_id` scan (`loadSubgraphChildren`), not a per-step probe. A second key column would only widen every subgraph-child insert against the write-amplification budget for no measurable gain |
| `idx_dwarf_flows_root` | `(root_flow_id, flow_id)` on PostgreSQL; `(root_flow_id)` with an implicit primary-key suffix elsewhere | Ordered descendant pages and one-query whole-tree scans (`WHERE root_flow_id=?`) for the membership walks. A plain (non-partial) index: `root_flow_id` is set for every flow (a root points at itself), so a `> 0` partial would index everything anyway |
| `idx_dwarf_flows_delete_after` | `(flow_id)`, partial `WHERE delete_after_ms > 0` on pgx/mssql/sqlite; `(delete_after_ms)`, full, on MySQL/MariaDB | The janitor's scans of stamped roots (`delete_after_ms > 0 AND surgraph_flow_id = 0 AND DATE_ADD_MILLIS(updated_at, delete_after_ms) <= NOW ... ORDER BY flow_id`). `delete_after_ms` is `0` for all but the flows scheduled for deletion, so the partial index holds only those, **keyed on `flow_id` because the scan orders by it**. Keyed on `delete_after_ms` it serves no order the scan wants, and every dialect walked the primary key instead, past every unstamped root ahead of the first stamped one - an archive outage's whole held backlog, on every repair sweep (1.42M rows: PostgreSQL 1.29 s, SQL Server 1.13 s, SQLite 126 ms; keyed on `flow_id`: 0.35 ms, 333 ms, 1.7 ms). MySQL and MariaDB have no partial index: the scan names this index (`FORCE INDEX`), reads every stamped root and sorts them, bounded by what is stamped (~250 ms at 220k) rather than by what is held (MariaDB left to itself walked the primary key, 2.05 s). Its entries are written only when a root is stamped, and the stamp's `UPDATE` sets the predicate's column, so neither key changes HOT eligibility |
| `idx_dwarf_flows_attempt` | `(flow_id)`, partial `WHERE archive_attempt_at > '2000-01-01 00:00:00'` on pgx/mssql (`'... 00:00:00.000'` on sqlite); `(archive_attempt_at)`, full, on MySQL/MariaDB | The janitor's large-tree archiving, finding the live roots whose staged archive attempt has started, in `flow_id` order, and the oldest one's age. Every live flow holds the no-attempt default (`2000-01-01 00:00:00`), so the partial index holds only the roots with an attempt - a few large trees - and costs a flow's insert nothing where it is partial. **The queries inline the predicate exactly as the index spells it** (`attemptedSQL`): a planner uses a partial index only for a query whose predicate implies the index's, which a bound value cannot show, and on SQLite the comparison is of text, so `.000` is part of the spelling. MySQL and MariaDB have no partial index: the scans name this one (`FORCE INDEX`), seek past the no-attempt default and sort what has an attempt; there every flow insert and delete writes an entry, as for `idx_dwarf_flows_delete_after`. An attempt is set and cleared by small `UPDATE`s of one root, which then cannot be HOT |

#### `dwarf_steps`

| Index | Columns | Purpose |
|---|---|---|
| PK | `(step_id)` | Row lookups, lease acquisition in `processStep` |
| `idx_dwarf_steps_flow_id` | `(flow_id, step_id)` on pgx, MySQL and SQLite; `(flow_id)` on mssql, whose nonclustered index carries the clustering key | Per-flow step queries. A chunked deletion reads a flow's step ids in `step_id` order from it, bounded by the flow's own steps; on PostgreSQL `(flow_id)` alone walked the primary key past every other flow's steps instead (3.6k-6.9k buffers a chunk at 4% density, against about 20). Pinned by `internal/janitor`'s `TestChunk_TheStepBoundaryWalksTheFlowsIndex` |
| `idx_dwarf_steps_selection` | `(status, parked, priority, fairness_key, created_at, step_id)` + covering `not_before, lease_expires, fairness_weight, lineage_id` (an `INCLUDE` on pgx/mssql; trailing key columns on mysql/sqlite, which have no `INCLUDE`) - partial on pgx/mssql/sqlite, full on mysql | Two-level priority+fairness candidate selection (the three-phase refill: the piston's `ScanBand` -> the planner's `Plan` -> the piston's `FetchSteps`). The `(status, parked)` prefix excludes terminal and parked rows without an in-memory filter; `priority`/`fairness_key` seek the band and its keys. The trailing `(created_at, step_id)` serves the oldest-N fetch (phase 3): a key's due steps arrive already age-ordered, so it needs no sort. Phase 1 is a plain `GROUP BY` and needs no order from any index. `step_id` is listed explicitly because pgx does not implicitly append the PK to a secondary index (mysql/mssql/sqlite do); it also resolves same-`created_at` ties (fan-out siblings can share a millisecond). Cheap to carry: `created_at`/`step_id` are write-once, and the leading `(status, parked)` already repositions the entry on every transition, so the wider key adds size but no extra churn. The `not_before<=NOW AND lease_expires<=NOW` due-predicates and the projected `fairness_weight` are **carried in the index** so phase 1 can evaluate due-ness without deforming the row. All three or none: carrying only the two timestamps leaves `fairness_weight` to drag the row in anyway. `lineage_id` is carried for phase 3, which projects it (the engine's fan-in hold keys the candidate cache by it): uncarried, the lateral fetch reads the row of every entry it walks, rejected ones included, and the ranking fetch the row of every due row it ranks. They are appended, never part of the ordering prefix, so index order and the no-sort property are unchanged. **Measured under load (single shard, 8 vCPU, PG16, 36k-50k due rows): x1.43 throughput on a fan-out workload, and no measurable change on a linear one** - the latter being the point, since the wider index must not cost anything at a backlog too shallow to need it. **The win is per-tuple scan work, NOT fewer page reads, which inverts two natural assumptions.** (1) Do not justify this index by buffer counts: the plain `Index Scan` already reads ~0.13 buffers/row in the engine's real insert pattern, because the trailing `(created_at, step_id)` sort keys mean index order tracks physical heap order and consecutive entries land on the same page; the covering plan reads no fewer buffers, it is simply faster per tuple. (A fixture that scatters rows measures ~1 buffer/row, which does not describe this table.) (2) The benefit does **not** depend on a warm visibility map: measured with autovacuum having run 0-1 times and ~75% of rows still taking a `Heap Fetch`, the x1.43 held anyway. An index-only scan does want all-visible pages (PostgreSQL's visibility map; InnoDB checks a page-level trx id and needs no VACUUM), so a well-vacuumed steady state should only do better - but the gain is not contingent on getting there, and it degrades to the plain index-scan plan (never worse) under uniform churn. Cost is storage only: +100% on PostgreSQL, +32% on InnoDB (whose secondary index already carries the PK). `EXPLAIN (ANALYZE, BUFFERS)` should show phase 1 planning as an index-only scan; judge it by scan time rather than by heap-fetch count, which stays high under normal write rates without costing the benefit |
| `idx_dwarf_steps_due` | `(parked, priority, not_before)` on pgx/sqlite, `(status, parked, priority, not_before)` on mssql/mysql, + covering `fairness_key, created_at, step_id, lease_expires, fairness_weight` (an `INCLUDE` on pgx/mssql; trailing key columns on mysql/sqlite), and `lineage_id` on mysql/sqlite, whose ranking fetch reads this index and projects it (write-once, so it adds size and no churn; pgx/mssql fetch by a lateral that cannot use this index) - partial `WHERE status = 'pending' AND step_id <> 0` on pgx/mssql/sqlite, full on mysql | **The sleeper index.** A step that is `pending` but not yet DUE - `flow.Sleep`, a retry backoff, a long-poll that retries for days - sits at its band in `idx_dwarf_steps_selection` and is walked by every refill cycle, on every shard, on every replica, forever, because `not_before` is only *carried* there (appended after the ordering prefix) and can therefore be filtered but never seeked. This index makes due-ness seekable, and a workload with a large sleeping population is a supported one, not a corner: whether pending work is mostly due or mostly asleep is a property of what the host's workflows do, so no single index shape can be right for everyone and both must be available for the planner to choose between. **Column order is the whole design and neither half is optional.** `priority` sits *before* `not_before` so the band scan gets `priority = <band>` as an **equality seek** followed by a `not_before` range on the very next column - it reads exactly the due rows at that band and nothing else. The same order serves the band scan's per-band counts, each an equality on `priority` with a `LIMIT` on the `not_before` range, so a band of sleepers is an empty range and costs one probe. **Measured (PG 18.1, 400k pending at one band, 32 keys), phase 1 end to end: dense 47.4 -> 45.1ms (no regression), 396k old sleepers + 4k fresh due steps 16.7 -> 3.7ms, whole keys asleep 49.7 -> 6.8ms.** Three shapes that were tried and lost, each for a reason that recurs: (1) folding `not_before` into `idx_dwarf_steps_selection`'s key **costs 9x on the dense band scan and 98x on the fetch** - a range predicate terminates a B-tree's usable ordering, so the trailing `(created_at, step_id)` stops being an ordering and `EXPLAIN` grows a `Sort`, killing both the ranking fetch's no-sort property and the per-key `LIMIT`'s early stop; (2) leading with `not_before` before `priority` can only target "due rows at *any* band" and then filters, measuring 40.7ms against this index's 6.8 when whole keys sleep; (3) `(…, priority, step_id)` for a band probe alone cures clustered sleepers (29.9 -> 0.008ms) but **walks all 396k of them (22ms) when the sleepers are simply older than the due work**, which is the ordinary shape of a long-lived polling population - `step_id` is insertion order and carries no information about due-ness. Adding a second `not_before`-leading index *on top* of this one buys nothing (identical within noise on all three shapes), so this is one index, not two. Cost is write amplification on `not_before`, which - unlike the write-once `created_at`/`step_id` that made the selection index cheap to widen - is rewritten by every sleep and every retry backoff, i.e. by exactly the workload it helps. That trade is accepted deliberately. **Whether `status` leads the key is per dialect, and both directions have bitten.** **On PostgreSQL (and SQLite) it must not:** the predicate fixes it, and PostgreSQL removes from the index conditions a clause the predicate implies, so a leading `status` column is left with no condition, and every version before 18 (no skip scan) walks the whole index for every band. `EXPLAIN` prints the same `Index Cond` either way, so judge it by buffers: on PostgreSQL 16 an empty band over 65k due rows read 685 buffers with `status` leading and 6 without, and on a rig whose due index had grown to 3.5 GB one empty-band probe read ~266k. 18's skip scan reads 11 and hides it. Pinned by `TestScanBand_AnEmptyBandIsAProbe`, which fails on versions before 18 (measured on 16). SQLite keeps the clause as a seek column either way, so it takes the narrower key. **On SQL Server it must:** without the filter column in the index, the optimizer costs the filtered index as needing a lookup and prefers `idx_dwarf_steps_selection`, which walks a band's sleepers. Measured at 131k rows with both indexes present: a sleeper-only band's counts read 3 logical reads with `status` leading and 471 without, the dense tally 389 against 1,401, the backlog aggregate 774 against 1,868. Plan tests that drop the selection index cannot see this; `TestScanBand_SQLServerChoosesTheDueIndex` keeps both. mysql's index is full, so `status` is a real seek column there and leads. **The `step_id <> 0` conjunct is always true and is a usage gate:** a query uses this index only if it states `step_id<>0` itself, so the lateral fetch, which must not, cannot be planned onto it under any statistics (`internal/piston/CLAUDE.md`, "The lateral fetch cannot use the due index"); every query that wants the index states it. It is an inequality, never the range `step_id > 0`, which makes the PostgreSQL planner read the primary key's dead low end on every plan (same section). The status part is `status = 'pending'` alone, **narrower than the selection index's `IN ('pending','running')`**: the refiller never wants a due `running` row, and the narrower filter makes a claim a pure delete from this index rather than a delete-plus-reinsert. Do **not** narrow the selection index to match - `wedge.go`'s expired-lease sweep (`status='running' AND parked=0 AND lease_expires<=NOW`) reads it as an index-only scan, and dropping `running` would leave that hot recovery path with no index that carries `parked` and `lease_expires`. It does **not** help `FetchSteps` (0.077 -> 0.084ms dense, ~3.4ms under sleepers, i.e. unchanged): sleepers interleaved inside one key's run are still walked by the per-key `LIMIT`, and no index fixes that without losing the dense case |

**Do not add a `(status, updated_at)` step index.** No query needs one: the only query shaped for it, the
5-minute parked-subgraph sweep (`recoverWedgedSubgraphParks`), ranges over in-flight `running` steps and is served
by `_selection`'s `(status, parked)` prefix. Every claim and completion would pay to maintain it, and as the
smallest partial index on `pending` rows it is the one a Postgres generic plan picked for a claim predicate
written as plain conjuncts, walking the backlog (`claimEligible` in `engine/claim.go`).

### Ordered janitor flow indexes (PostgreSQL)

PostgreSQL's live and archive root indexes are keyed `(root_flow_id, flow_id)`, and its live table carries
`idx_dwarf_flows_closed_roots` beside `idx_dwarf_flows_status`, both in the original migration files (live `1.sql`,
archive `1.sql`). No other dialect needs either: its primary key is already the ordered suffix of the secondary index.
The root-only closed index adds writes only for unstamped roots, not their subgraph children
(`TestClosed_TheProbeSkipsStagedDescendants`). An existing PostgreSQL deployment gets them by the DDL in the release
notes - dropping and rebuilding the live root index and every retained day's, and creating the closed-roots index -
which reads the tables and takes index-build locks; a new archive day is made with them.

### The archive day migrations (`archive/`)

Each day's archive tables (`dwarf_flows_archive_<n>`, `dwarf_steps_archive_<n>`) are made and altered by the files in
`archive/`, run as that day's own migration sequence (`SequenceName(day)`) with the day's names substituted by
`ArchiveDay` (`internal/archiver/CLAUDE.md`). The files write the tables as `dwarf_flows_archive` and
`dwarf_steps_archive`, stand-ins no table carries: `live/` makes no empty template tables, so nothing but a day
ever exists under those names. So:
- **The columns are the live tables'** - but `archive_attempt_at`, a live root's archive attempt, which means nothing
  archived and is added to the live table alone - with the queue-only ones kept at their defaults; an archived row is
  copied whole, **by name** (`internal/janitor`'s `archivedFlowColumns` and `archivedStepColumns`), so the order is not
  load-bearing: an existing table gets an added column at its end on PostgreSQL, SQL Server and SQLite. A column added
  to a live table is added to its list there too. Pinned by `internal/janitor`'s `TestMove_NamesEveryColumn`. **No auto-increment**: `flow_id` and `step_id` keep their live values, which keys
  and state refs carry. A column added to a live table in `live/` is added in `archive/` in the same release.
- **Every index is named for its stand-in** (`idx_dwarf_flows_archive_root`, ...), so the substitution renames it
  too. An index with any other name collides across days on PostgreSQL and SQLite, where index names are per
  schema.
- **Every statement is idempotent** (`IF NOT EXISTS`; `IF OBJECT_ID(...) IS NULL` / `sys.indexes` guards on SQL
  Server), because a day is made at runtime by any replica, and a runner that dies mid-migration leaves it to a peer
  that runs it again - over whatever it applied, on MySQL and MariaDB and for a one-statement file anywhere.
- **`archive/` is its own sequence, numbered from 1, under the same rules as `live/`**: numbers only append, a
  shipped file is immutable from v1.0 on, a change is the next number. A file there names only the stand-ins, never
  a live table: the whole file runs once per day, so a live table in it would be changed once per day. The engine's
  Startup applies a new one to every existing day within the retention right after the shards open. A live-table
  change and its archive counterpart are therefore two files, one in each folder.
- **The flows table carries every live flows index**, so a flow lookup runs with the same
  plan on either side - `workflow_url`, the two surgraph links (partial where the dialect has partial indexes), and
  `root_flow_id` - except that the status index is on `(status, archived_at)` rather than `(status, updated_at)`, plus
  an `(archived_at)` range: a day is read in archive order.
  - **`delete_after_ms` on an archive root is the archive's deletion mark** (a stamped live root is deleted, never
    archived): a Delete or Purge marks the root, the tree is hidden at once, and the janitor's archive deletion sweep
    deletes it. Its deletion index, `(flow_id) WHERE delete_after_ms > 0` (partial; filtered on SQL Server; the full
    `(delete_after_ms)` on MySQL and MariaDB, named with `FORCE INDEX`), is what the sweep's probe of each retained day
    reads: empty but for a mark on the partial dialects, about 5% on the archive insert on MySQL.
    Without it the probe reads each day's flows table in full (24-82 ms per 415,000-flow day).
  - **The roots index, `(archived_at, flow_id) WHERE surgraph_flow_id = 0`** (partial; filtered on SQL Server;
    `(surgraph_flow_id, archived_at, flow_id)`, full, on MySQL and MariaDB), serves the roots-only lists and Purge in
    their order: without it they walk `(archived_at)` past every child archived in the window, 79-837 ms for a page
    beside one tree of 300,000 children against 0.24-3.1 ms with it; 0-7% on the archive insert. SQL Server also
    carries a roots-only `(status, archived_at, flow_id)`, without which a rare status regresses (2.1 to 15.6 ms).
    SQLite's planner takes the status index whenever the status is compared plainly, so its roots-only archive
    listings write `+f.status` (`listArchived`). Pinned by `TestArchiveVisibility_RootsListingsWalkTheRootsIndex`.
  - **Both are in `archive/1.sql`**, so a day made before them has neither, and keeps the plans above until it drops.
  They cost two to four index
  inserts per archived FLOW beyond the root and archive-time ones (the surgraph ones only for a subgraph child),
  about a tenth of the rows, mostly at the right end of their index. **The steps table
  carries `flow_id` only - `(flow_id, step_id)` on PostgreSQL, MySQL and SQLite, so a chunked deletion reads a flow's
  steps in order and bounded, as on the live table - never the dispatch indexes** (`selection`, `due`): an archived step is terminal, so on
  the dialects with partial indexes they would stay empty, and on MySQL, which has none, they would be full indexes
  on the largest table, on a path bound by WAL bytes.
- **PostgreSQL freezes a day early** (`autovacuum_freeze_min_age = 0`, an insert-triggered vacuum at 1%, on the table
  and its TOAST): every row must be frozen once before the transaction-id counter wraps, and a page frozen soon after
  it fills writes a small freeze record rather than a full-page image.

Pinned by `migrations_test.go`: a day's files name only that day and every index for its table, and `live/` names
no archive table.

### The live tables vacuum in small unthrottled passes (PostgreSQL)

`live/1.sql` and `live/2.sql` set per-table storage parameters on `dwarf_flows` and `dwarf_steps` and their TOAST
tables: vacuum and insert scale factors 0.01, cost delay 0, `vacuum_truncate = false`. Every closed flow leaves the live tables
within minutes, so they churn at the full write rate while staying small, and the defaults (a 20% threshold, a throttled
worker) let dead rows - TOAST above all - pile up faster than vacuum reclaims them. **Do not let vacuum truncate
them**: truncating takes an access exclusive lock, stalling every statement on the table while vacuum waits for it,
and the freed tail is refilled within seconds; on a loaded shard the truncate wait shows beside dispatch stalls.

**`dwarf_steps` is also analyzed at 1%** (`autovacuum_analyze_scale_factor = 0.01`; the default is 10%). A chunk of a
large tree's deletion or staged copy reads a flow's steps after a position, bounded by its budget only while the
planner's statistics know the flow: with none for a large new flow, PostgreSQL 16 took a bitmap scan of the flow index
plus a sort, reading every remaining step of the flow on every chunk (measured: 40,000 steps scattered at 4% density,
~1 s a chunk cold, before any ANALYZE); with them, an ordered scan the budget bounds. ANALYZE samples a fixed number of
rows, so the more frequent pass costs little. It narrows the window and cannot close it: the janitor's chunks turn
bitmap scans off for their transactions as well (`boundReads`, `internal/janitor/CLAUDE.md`).

### A claim RELOCATES its entry in `idx_dwarf_steps_selection` - that is where a load stall lives

Two `dwarf_steps` indexes are partial on `status` (`selection`, `due`), so every status change touches
both. Only the one whose predicate **spans** the transition pays a structural cost.
`idx_dwarf_steps_selection` is partial on `IN ('pending','running')` and leads with `status`, so a
`pending -> running` claim keeps the row in the index and moves it: a delete from the `pending` region and an
INSERT into the packed `running` region, which splits leaves. `idx_dwarf_steps_due` is partial on
`status = 'pending'`, so the same claim is a pure delete, and deletes never split. This qualifies design
principle 2 above: the transient sections are small, but under a building backlog their leaves are packed
and splitting them is not free.

**Measured** (PostgreSQL 16, 16-vCPU managed instance, one shard, 96 connections, linear workload, open loop,
`pgstatindex` sampled every 10s through a stall):

| pending | claim CAS mean | `selection` density / frag | `due` density / frag |
|---|---|---|---|
| 4 | 0.45ms | 64.4 / 0.8 | 89.7 / 0.1 |
| 8,545 | 12.45ms | 49.5 / 19.2 | 86.1 / 0.6 |
| 15,112 | 31.51ms | 52.1 / 17.9 | 84.9 / 0.5 |
| 25,497 | 0.25ms | 57.1 / 9.0 | 88.6 / 0.2 |

The claim CAS is a primary-key update, yet it inflates ~70x while `selection` loses leaf density and `due`
barely moves. It tracks neither backlog depth (0.24ms at 21k pending, 27.6ms at 12k) nor the table-wide dead
tuple count (0.42ms at 730k dead, 206ms at 177k).

**The fragmented state is the CHEAP state.** Once the stall passes, fragmentation stays at ~17 for the rest of
the run while the CAS sits at 0.24ms: leaves with free space take inserts cheaply. The expense is the one-off
reorganisation of packed leaves, which is why the stall is transient (~30s) and clears under the same
sustained offered rate. Do not read a fragmented `selection` index as a problem to fix. **Not separated:** the
split window overlapped an autovacuum completion by ~20s, and both relieve the index. The evidence for splits
over dead tuples is that fragmentation persists while latency recovers.

**Levers that were tried and do not work. Do not re-raise them without new evidence:**

- **Autovacuum off** (`autovacuum_enabled=false` on `dwarf_steps`): 1,708 against 6,837 steps/s. The claim CAS
  climbed steadily to 395ms and never recovered. Vacuum is the cure, not the contention.
- **Aggressive autovacuum as the cure for this stall** (`scale_factor=0.02`, `cost_delay=0`, 120s windows on a
  `dwarf_steps` that kept every closed flow): 5,702 steps/s with a 115-second p99; it does not relieve the
  split stall. It is no evidence against the live tables' settings above, which exist to reclaim dead rows in
  tables that stay small while they churn at the full write rate.
- **`fillfactor` below 100 on `dwarf_steps`, to get HOT updates:** made no difference to throughput at 70 or 90,
  and the HOT ratio moved only 25.7% -> 29.8%. A local sweep (linear workload, ~1,000 steps/s, 60s per setting) read
  30.7 / 30.8 / 31.0 / 31.0% HOT on `dwarf_steps` at 100 / 90 / 80 / 70. It cannot work by construction: HOT
  requires that no indexed column changes, and about 70% of the engine's roughly 5.6 updates per step write
  `status`, which leads the step indexes. On a linear step (claim, completion, successor link) only the link is
  HOT-eligible, which caps the ratio near 1/3, and HOT pruning already leaves the link room on the page without
  a lower fillfactor. A higher ratio needs the claim and completion to stop writing an indexed column - a schema
  change, not a storage setting. `dwarf_flows` is the opposite case - its rows are few and wide, so whether an update's new version
  stays on its page, HOT or not, is decided by page room, which fillfactor does control (next section). The one place
  it shows on `dwarf_steps` is a row kept wide and inline by its payload (~1-2 KB): its claim and completion leave
  their page and log the whole row, and a lower fillfactor keeps them on it. **The default stays anyway, because what
  that buys at 1 KB it costs at large payloads.** Saturated on a 32 vCPU PostgreSQL 16 at the recommended checkpoint
  spacing, fillfactor 50 against the default: at 1 KB inline, +5.6% steps/s and -16% WAL per step (updates leaving
  their page 52% -> 15%) for a ~30% larger table; at 0 B, neutral; at 64 KB, -5.9% steps/s, +20% WAL and +44%
  full-page images per step - the payload is out of line, so there is little rewriting to save, and twice the heap
  pages are imaged again after every checkpoint. Do not lower it for the 1 KB gain without measuring large payloads
  at the same setting.
- **`SET STORAGE MAIN` on `state`/`changes`, to keep a value inline instead of TOASTing it:** raises WAL per
  step sharply. A row held inline at 2-8 KB leaves no room on its page, so HOT all but stops and nearly every
  claim and completion re-logs the whole wide tuple onto a new page. fillfactor 80 does not
  rescue it. Past ~2 KB, an out-of-line TOAST value is what keeps the updated row narrow.
- **It is not client-side.** Active backends held at 72-99 before, during and after the stall (the pool caps
  them), and row-lock waits (`Lock:transactionid`, `Lock:tuple`) read zero in every sample.

Narrowing `selection`'s predicate to `status = 'pending'` would attack the mechanism directly, because it
makes a claim a pure delete. It is rejected for the reason given in the `idx_dwarf_steps_due` row above: the
expired-lease sweep reads `running` rows from this index as an index-only scan.

### `dwarf_flows` updates stay on their page only while it has room, and the row's width decides that (PostgreSQL)

A flow's row is written a few times per flow, never per step: the interrupt and Resume writes and its terminal
write. Creation adds none: the INSERT writes a new root's own-id `root_flow_id` itself
(`engine/CLAUDE.md`, "Creating a flow"). The lock grabs are locking reads and `stopped_step_id` records only the
step a flow stopped at. **Keep it that way:** a per-step write to this row is a row version per step and puts it
on the cliff below, and an UPDATE of `root_flow_id` after the INSERT adds an entry to each of the
row's indexes for every flow.

**Almost no flow-row write can be HOT.** The status transitions (interrupt, Resume, the terminal write, the Fork
root's `created`->`running` flip) move `idx_dwarf_flows_status`, and the `delete_after_ms` mark moves its partial
index. What is left HOT-eligible is Cancel's `cancel_reason`/`cancel_watermark` mark, Resume re-pointing
`stopped_step_id` on a flow a sibling interrupt keeps interrupted, and the `stopped_step_id` write to a Fork's
cloned subgraph flow - its `updated_at=NOW_UTC()` leaves the index key where the INSERT put it, because on
PostgreSQL `NOW_UTC()` is the transaction's start time. So `n_tup_hot_upd` on `dwarf_flows` reads near zero in a
healthy deployment, and is not a health signal.

**Page room still decides every update's cost.** An update's new version goes on the row's own page whenever it
fits there, HOT or not; HOT additionally spares the index entries. Whether it fits depends on one thing: room on
the row's page for the new version once the page's dead versions are pruned. The flow row carries the graph JSON inline, so the row is large (~1-2
KB for an ordinary graph), only a handful fit on a page, and the room left after packing - and how fast pruning
can reclaim it while flows rewrite their rows - is what decides.

**At the default fillfactor this is a cliff in row width, and not a monotonic one.** A width change of a few
dozen bytes - one added column, or a slightly larger graph - can flip the flow-row writes from fitting on their
page to not, and a wider row can land back on the good side. The page arithmetic does not predict which side a width falls
on; it has to be measured. When it flips, every write takes a new heap page, and a HOT-eligible one stops being HOT and adds index entries too: the
heap grows many-fold for the same live rows, every `dwarf_flows` write slows, and the occasional slow
completion holds a connection long enough to exhaust the pool. **The signature is a tail, not a mean:** the
same statements per step and the same throughput, but more pool waits and a heavier `Create` p99, with the
table far larger than `n_live_tup` explains.

**What bounds it:** PostgreSQL compresses or moves a row's large values out of line once the row passes ~2 KB
(`TOAST_TUPLE_THRESHOLD`, a compile-time constant), and an update that does not change a TOASTed value keeps its
pointer. So no `dwarf_flows` row is ever wider than ~2 KB inline, whatever the graph's size, and a large graph
costs a write nothing. The exposed range is the inline one, up to ~2 KB. `toast_tuple_target` does not
move the threshold down: lowering it leaves a sub-2 KB row's graph inline.

**`dwarf_flows` takes the default fillfactor: a lower one keeps more writes on their page and buys nothing for
it.** With the flow row rewritten on every step, `fillfactor=80` kept its writes on their page across the whole
inline range (88-90% HOT at widths from ~1.1 KB to the ~2 KB TOAST threshold, against 73-75% at the default for
widths on the cliff), and `fillfactor=90` did not. The row is now written two to four times per flow - on a linear
workload one of those writes is an update, never HOT - and measured that way (32 vCPU PostgreSQL 16, a ~1.9 KB row,
a fixed 906 flows/s with the archive on), 80 halved the updates leaving their page (65% -> 33%) while WAL per flow
stayed at ~110 KB and the table grew 21%, the same live rows with more dead and free space. Do not reintroduce it
without a workload that writes the flow row more often than that. Any column added to `dwarf_flows` moves the
row's width; check on PostgreSQL that the table does not grow far past `n_live_tup` rather than assume it.

### The lists get no live indexes of their own, and their `ORDER BY` is not a bug

**Indexes here are earned by load-bearing hot-path queries** - candidate selection, the claim CAS, fan-in, the
tree walks, the janitor - and each is paid for on every INSERT and every status transition of the busiest tables
in the system. The lists and `Purge` are the opposite: an **arbitrary query surface** (status, workflow URL/name,
task name, fairness key, priority, age range, free-text search, in any combination). There is no index
set that covers a cross-product, and adding one per filter would tax the write path to speed up an operator
screen. So most list filters are deliberately unindexed (see the `fairness_key` catalog row), and that is a
decision, not an omission.

**The live reads order by `updated_at`, one branch per status**: `ListOpen` descending within its window, the
janitor's closed scans ascending. `(status, updated_at)` serves each branch as one range, already in order, and the
branch stops at its limit; a single scan over several statuses has no index order and would sort every matching live
row for the top `Limit`. Their plans at scale are not measured. Do not add an index for them before a measurement says
one pays for its write cost.
An archive day's read orders by `archived_at`, which the day tables index for it (`(status, archived_at)`,
`(archived_at)`): a day is written once and read in that order, so the index costs no status churn there.

**And do NOT "fix" it by dropping the `ORDER BY`.** It is what makes each table's read its first `Limit` rows in
the list's order, which the engine's cross-shard merge assumes (`engine/CLAUDE.md`, "`ListOpen` and `ListClosed`
are cross-shard top-Ns"). Unordered, a read returns an arbitrary `Limit` matches, and flows that belong in the
result but were not returned are silently left out of the merge.

### Status predicates are inlined literals, not bound parameters

Every `status` comparison in a **`WHERE`** clause inlines the `workflow.Status*` constant into the SQL string
(`"...WHERE status='"+workflow.StatusRunning+"'..."`, `"... status IN ('"+workflow.StatusCreated+"', ...)"`)
rather than binding it (`status=?`). This is load-bearing for index usability, not a style choice:

- **SQL Server refuses a filtered index for a parameterized predicate.** The step indexes are filtered
  `WHERE status IN ('pending','running')` (mssql) / partial (pgx/sqlite). SQL Server compiles one cached plan
  that must be valid for *every* parameter value - and since a filtered index omits rows (`status='completed'`
  is not in it), a plan that used it could return wrong results for some `@status`, so the optimizer **rejects
  the filtered index and scans** (a full clustered-index scan on the hot refiller band scan and lease
  recovery). A **literal** `status='pending'` is provably a subset of the filter, so the index is
  used. (`OPTION (RECOMPILE)` also works but recompiles every execution - wrong for hot queries.) SQLite's
  partial-index prover has the same gap; Postgres is safe via custom plans but a literal is deterministic and
  never worse. MySQL runs these indexes unfiltered (no partial-index support), so a literal is neutral there.
- **Safe to concatenate:** the values are `workflow.Status*` **constants** the engine controls. The one
  caller-supplied filter (the lists' and `Purge`'s `Query.Status`, `query.go`) is validated with
  `workflow.IsValidStatus` **before** it is inlined and rejected `400` otherwise, so only a known constant -
  never arbitrary input - reaches the SQL string; there is no injection surface. Inlining is the fix chosen over
  unfiltering the mssql indexes, which would bloat them with the unbounded terminal-step history the partial
  index exists to exclude. Note the lists' win is *not* the filtered-index case (`idx_dwarf_flows_status` is
  unfiltered): there a parameter defeats the seek because an `ORDER BY ... LIMIT` under a density-average
  estimate makes the optimizer scan for a *rare* status; the literal lets it read the histogram and seek. Same fix,
  different reason.

**Two deliberate exceptions stay bound (`?`):** (1) `SET status=?` **assignments** (an assignment is not a
predicate and never selects an index); (2) a `WHERE` predicate whose status is a runtime *variable* on a
PK-keyed statement (the lock-contention recovery reset in `dispatch.go`, `WHERE step_id=? AND status=?` with a
computed `fromStatus`) - PK seek, no index to help, and the value is not a literal. A new query comparing
`status` in a `WHERE` must inline the constant (validating first if the value is caller-supplied) unless it
falls in one of these two.

### `LIMIT_OFFSET` requires an `ORDER BY` on SQL Server

`sequel`'s `LIMIT_OFFSET(limit, offset)` macro compiles to `LIMIT … OFFSET …` on mysql/pgx/sqlite but to
`OFFSET … ROWS FETCH NEXT … ROWS ONLY` on **mssql**, and SQL Server's `OFFSET/FETCH` is a **syntax error
without a preceding `ORDER BY`**. So every `LIMIT_OFFSET` statement must carry an `ORDER BY` - **even a pure
existence probe** (`SELECT 1 … LIMIT_OFFSET(1, 0)`) where the ordering is semantically irrelevant (any match
suffices). An `ORDER BY` on such a probe looks removable and is not: stripping it passes on four dialects
and fails only on mssql. Do not strip an `ORDER BY` from a `LIMIT_OFFSET` query. (If a cheap order is wanted, `step_id` - the PK - is the natural choice.)

### Ids are bound, never written into a statement; a list is bound through `database.IDList`

**Every distinct statement text is a new statement to the database**, and an id written into the text makes a new
text every time. The PostgreSQL driver keeps every statement prepared on each connection (up to 512 per connection by
default), and each holds backend memory for its plan - measured at about 3 MB for a statement carrying 8,192 literal
ids, where one bound statement run any number of times holds one. A statement that runs on whichever pooled
connection it takes (a janitor batch) fills every connection's cache, until the database runs out of memory. SQL
Server compiles and caches a plan per text, and MySQL prepares and closes one.
So bind every id, and a list through **`database.IDList`**: its placeholders and arguments, padded by repeating the
last id up to the first of **4, 20, 100, 500 and 2,000** (`database.MaxListIDs`) that holds it, so a statement takes
one of five texts. Repeating an id leaves `IN` and `NOT IN` matching the same rows - padding with `0` would not do on
a column where `0` is a value (`surgraph_flow_id`, `lineage_id`). A list longer than `MaxListIDs` goes out in chunks
(`slices.Chunk`); `IDList` panics past it.

**What stays in the text:** a status (inlined for the filtered indexes, "Status literals, not binds" in the root
doc); a day's table name and its day number and hours (one text per day, which the cache rotates out); the piston's
priority bands (bounded by `SetMaxPriority`) and its capacity (changing only with the pools); the shard index in the
lists' flow-key expression (one per shard); and the positional keys of a statement whose branch count is fixed (the
janitor's sizing statement, padded to 1, 4, 16 or 64 trees).

**The chunk is sized to every bind of the statement.** SQL Server rejects a statement with more than **2,100
parameters**, the tightest ceiling of the dialects (SQLite's is 32,766, PostgreSQL's and MySQL's 65,535), and the
failure is a hard error that fails the operation every time. `MaxListIDs` leaves 100 for a statement's other binds.
Any list the engine sizes by a cohort, a subgraph tree, a subgraph chain or a fan-out goes out with
`slices.Chunk(ids, inListChunk)` and `inList` (`engine/inlist.go`): 500 ids, a rung of the same lengths, in ascending
order when the statements lock (`lockOrder`), because some of those statements bind more per id: a
`CASE WHEN id=? THEN ?` binds two per id, so a chunk carrying one (the interrupt's `stopped_step_id`) binds ~1,500.
**A surgraph chain is not bounded**: it is as deep as the subgraph nesting, which nothing limits, so the interrupt's
and Resume's chain writes are chunked too; unchunked, an interrupt 700 flows deep rolled back on SQL Server every time.
Only SQL Server shows the overrun, so `fixtures/wideflow_test.go` (wide trees) and `engine/deepchain_test.go` (a
2,101-flow chain) are the pins there. The janitor's lists are capped by its settings (roots per batch at most
`MaxListIDs`, a tree read 256 descendants at a time) and a chunk's step ids are chunked at `MaxListIDs`.

### MySQL Column Defaults

In `-- DRIVER: mysql mariadb` schema sections, `TEXT`/`BLOB`/`JSON` columns cannot take a bare literal `DEFAULT` (MySQL error
1101); the value must be a parenthesized expression default, `DEFAULT ('{}')` (MySQL 8.0.13+). The same applies to
function defaults other than `CURRENT_TIMESTAMP`, which is why `NOW_UTC()` expands parenthesized. `VARCHAR`/`CHAR`
keep bare literal defaults. Postgres, SQL Server, and SQLite permit bare literal defaults on text/JSON types, so this
is MySQL-only. Mirror the parenthesized form on every MySQL `TEXT`/`JSON` column or fresh MySQL deployments fail to
migrate.

**Comparing a payload column to a string literal is a THREE-WAY dialect split.** `WHERE json_col = '{}'` returns zero
rows on MySQL - the JSON-typed column is not implicitly compared against the bare SQL string `'{}'` (you'd need
`CAST(json_col AS CHAR) = '{}'` or `json_col = CAST('{}' AS JSON)`). On **SQL Server the payload columns are
`VARBINARY(MAX)`** (see "Payload columns are binary on SQL Server" below), so the literal must be the **byte** form
`0x7B7D`; a varchar literal happens to be accepted there through an implicit conversion, but the same conversion is
*rejected outright* in a `DEFAULT`, so do not lean on that asymmetry. Only SQLite (`TEXT`) and Postgres (`JSONB`,
which casts the unknown literal) match `'{}'` directly. A single shared query string is therefore wrong on two of
four dialects. The `interrupt_payload='{}'` first-writer-wins guard in `handleInterrupt` (`execution.go`) is the
case in point: written as one shared string, its payload write matches nothing on MySQL and `flow.Interrupt` payloads
come back empty. It branches on `db.DriverName()` - `CAST(interrupt_payload AS CHAR)='{}'` for MySQL,
`interrupt_payload=0x7B7D` for SQL Server.
**Assignments** (`SET col=?`) and the column `DEFAULT` are unaffected by the *comparison* rule - only `=`/`<>`
comparisons in a `WHERE`/`CASE` need the per-driver treatment. Any new query comparing a payload column to a literal
must apply the same treatment.

### Payload columns are binary on SQL Server, and every payload bind is a Go `[]byte`

On the `-- DRIVER: mssql` blocks the eleven JSON payload columns - `dwarf_flows.graph`/`baggage`/`final_state` and
`dwarf_steps.state`/`changes`/`interrupt_payload`/`state_refs`/`state_reduced`/`resume_data`/`subgraph_result`/
`subgraph_error` - are **`VARBINARY(MAX) NOT NULL DEFAULT 0x7B7D`** (`0x7B7D` is `{}`), not `NVARCHAR(MAX)`. The
genuinely-textual columns (`error`, `cancel_reason`, `workflow_url`, `workflow_name`, `task_name`, `task_url`,
`fairness_key`, `status`, the tokens, `trace_parent`) stay `NVARCHAR`/`NCHAR` - they are compared, searched, and read
by humans.

**Why.** go-mssqldb sends a Go `[]byte` bind as **VARBINARY**. Aimed at an `NVARCHAR` column, SQL Server performs an
implicit VARBINARY→NVARCHAR conversion that reinterprets successive **byte pairs as UTF-16 code units** - silent
mojibake, with **no error at write time**, surfacing much later as a JSON decode failure on the read. Measured:

```
[]byte -> NVARCHAR : "≻慮敭㨢䜢慲桰Ⱒ砢㨢紱"        (corrupt, silent)
[]byte -> VARBINARY: "{\"name\":\"Graph\",\"x\":1}"  (correct)
```

SQLite/Postgres/MySQL accept a `[]byte` **or** a `string` for their text/JSON columns, so nothing outside SQL Server
can catch the difference. A `string(...)` conversion at a payload bind site looks like redundant ceremony and, on
an `NVARCHAR` column, is a load-bearing encoding conversion: deleting it corrupts every flow on SQL Server.

**The invariant, and why the column type enforces it better than a convention could.** Every bind to a payload
column is a Go `[]byte`; every bind to a text column is a Go `string`. Get it backwards and SQL Server now raises
`Implicit conversion from data type nvarchar to varbinary(max) is not allowed` - a **hard error at the first write**,
not silent corruption. That is the whole point of the binary column: the failure mode moves from undetectable to
unmissable, and the database enforces what Go's type system cannot (both `string` and `[]byte` satisfy `any`).
Bonus: `NVARCHAR` stores UTF-16, so ASCII JSON cost 2 bytes/char on the wire and on disk; `VARBINARY` **halves**
SQL Server payload storage and transfer, which matters on a byte-throughput-bound engine.

**Cost, accepted knowingly:** `SELECT state FROM dwarf_steps` in SSMS returns hex rather than readable JSON, and
server-side `JSON_VALUE` on these columns is foreclosed (already deferred - see the `JSON_FIELD` note in
`internal/staterefs/CLAUDE.md`).

### Text columns compare in binary: `utf8mb4_bin` on MySQL and MariaDB, `Latin1_General_100_BIN2` on SQL Server, `"C"` on PostgreSQL

**Every text column of `dwarf_flows` and `dwarf_steps`, live and archive, is binary-collated**: each MySQL-family
table is converted to `utf8mb4_bin` right after its `CREATE TABLE` (which also makes it the table's default, so a
column added later inherits it), each SQL Server `NVARCHAR` column carries `COLLATE Latin1_General_100_BIN2`, and
each PostgreSQL `VARCHAR` column carries `COLLATE "C"`. SQLite's default is `BINARY`.

**On PostgreSQL the collation sets an index's order, not equality.** Equality is byte-wise under any deterministic
collation, so two values never merged there. But a column left at the database's collation - a C library locale such
as `en_US.UTF-8` on most servers, the managed ones included, and on the local test servers - orders its indexes by
that library's rules, and a glibc update can change them (2.28 reordered punctuation, the `-`, `_`, `.` and `/` that
keys and URLs hold). An index built before such an update is out of order after it, and a scan can miss rows until a
`REINDEX`: on `fairness_key` that is the selection index, the dispatch fetch's. `"C"` orders by bytes, never changes,
and is the order Go and the other dialects use.
- **The free-text and payload columns keep the database's collation**: `error`, `cancel_reason` (Search lowercases
  both, and under `"C"` `lower()` changes only ASCII letters) and `final_state`. Nothing indexes or orders them.
- **A new `VARCHAR` column needs `COLLATE "C"`**, in the live migration and its archive twin.
  `TestCollation_PostgresIdentityColumnsAreC` fails without it. A `"C"` column compared with a default-collation one
  is not an error (a non-default collation wins over the default); two different non-default ones are.

**Why.** The engine treats a fairness key, a workflow URL, a task name and the rest as exact identities in Go,
and the database must agree, because the same values are grouped and matched in SQL as well: the planner's tally
groups by `fairness_key` and its fetch matches `(priority, fairness_key)`, and every list filter and `Purge`
compares with `=`. Each server's default collation is case-insensitive (`utf8mb4_general_ci` on MariaDB 10.11,
`utf8mb4_0900_ai_ci` on MySQL 8, `SQL_Latin1_General_CP1_CI_AS` on SQL Server), and MariaDB's ignores accents too.
Under them `Acme` and `acme` are one value in SQL and two in Go, so two tenants shared a fairness bucket, a list
for one tenant returned the other's flows, and a `Purge` of one tenant deleted the other's. Measured with the
defaults, all three servers: each list filter returned both flows and the `Purge` removed both. Pinned by
`engine/collation_test.go`, which fails on all three with the collations removed.

- **A new text column of these tables needs its collation on SQL Server** (`COLLATE Latin1_General_100_BIN2` on
  the column, there is no table default), in the live migration and its archive twin alike, which must agree: a
  comparison or `UNION` between two columns of different collations is an error there.
- **Do not pick a MySQL-only or MariaDB-only collation** (`utf8mb4_0900_bin`, `utf8mb4_nopad_bin`, which would
  also stop ignoring trailing spaces). sequel names the driver `mariadb` only for a `mariadb://` data source name;
  a MariaDB server reached with a MySQL-style one runs as `mysql`, so a `-- DRIVER: mysql` block that names a
  MySQL-only collation fails the migration there (`Unknown collation: 'utf8mb4_0900_bin'`).
- **Trailing spaces still compare equal**, under `utf8mb4_bin` (a `PAD SPACE` collation) and on SQL Server under
  every collation. That is why the identities the engine groups or matches on refuse whitespace at the door: a
  graph name and a task name may not begin or end with it (`Graph.Validate`), nor a workflow URL (`create`,
  `flow.Subgraph`, and the spawn in `parkSubgraph` for a carrier that bypassed it), and a fairness key is letters,
  digits, `-`, `_` and `.` only (`validFairnessKey`).
- **Case-insensitive matching is spelled out where it is wanted**: `Query.Search` lowercases both sides
  (`LOWER(...) LIKE ?`), so it stays case-insensitive under a binary collation.

### Timestamps come from the database clock, never from Go

**Every timestamp column is written with a SQL expression (`NOW_UTC()`, `DATE_ADD_MILLIS(NOW_UTC(), ?)`), never a
bound Go `time.Time`.** Two reasons, both load-bearing:

1. **Clock source / skew.** `created_at` ordering, lease expiry, `not_before`, and the fairness `ageMs` all compare a
   stored timestamp against the database's own `NOW_UTC()`. If some rows were stamped by
   the *application* clock and others by the *database* clock, every such comparison would carry the app↔DB skew (and,
   across shards, the inter-node skew the scheduling design is careful to cancel - see "Selection", where both terms
   of `ageMs` come from one shard clock so per-shard offset cancels exactly). Writing only via `NOW_UTC()` keeps a
   single clock per shard authoritative.

2. **Native string format.** Each driver's `NOW_UTC()` emits that engine's *native* datetime text, and the same
   engine's date functions consume it without conversion. SQLite is the sharp edge: its native form is
   **space-separated** (`2026-06-16 01:18:14.596`, from `STRFTIME`/`datetime()`), and that is what `NOW_UTC()`
   produces. A bound Go `time.Time`, by contrast, is serialized by the modernc-sqlite driver as **RFC3339**
   (`2000-01-01T00:00:00Z`) - which `JULIANDAY`/`DATE_DIFF_MILLIS` then fails to parse (returning NULL → a silent
   `0`), so an age guard like `DATE_DIFF_MILLIS(NOW_UTC(), updated_at) > ?` quietly never matches. (The reverse is a
   *read*-only artifact and harmless: modernc reformats a `DATETIME` *column* back to RFC3339 when marshaling to Go,
   but the value stored on disk and compared in SQL is still the native space form, so engine-internal `WHERE`
   comparisons are unaffected.) Tests are not exempt: a test that backdates `updated_at` with `time.Date(...)` writes
   RFC3339 and its age guard silently never matches; backdate with `DATE_ADD_MILLIS(NOW_UTC(), -ms)` - DB clock,
   native format. Never round-trip a timestamp out to Go and back into a `WHERE`/`SET`; recompute it in SQL.

**Time columns hold UTC wall time without a zone, to the millisecond: `TIMESTAMP(3)` on PostgreSQL, never
`TIMESTAMPTZ`.** It matches `DATETIME(3)` on MySQL, `DATETIME2(3)` on SQL Server and sequel's own `sequel_migrations`
table.

**A column must be exactly as fine as the clock that writes it.** `NOW_UTC()` is milliseconds, truncated, on every
driver. A finer value stored into a millisecond column is ROUNDED, upward about half the time, so a row written as
due and checked at once with `col <= NOW_UTC()` reads not yet due for up to half a millisecond. A finer column than
the clock gains nothing and makes the time bounds dialect-dependent (`database.SQLTime` would need two units). Declare the
precision on every new time column; a bare `TIMESTAMP` on PostgreSQL is microseconds. `NOW_UTC()` on
PostgreSQL is a `timestamp` without a zone. Assigned to a `TIMESTAMPTZ` column, or
compared with one, it is converted through the SESSION's `TimeZone`:
- On a UTC session nothing shows.
- On any other session every stored instant is off by the zone's offset. Comparisons inside SQL stay
  self-consistent; values read into Go do not.
- Measured on a server in `America/Los_Angeles`: `CreatedAt` read 7 h in the future.

Pinned by `TestTimestamps_SurviveANonUTCSession`, which fails on a `TIMESTAMPTZ` column. The Docker images run UTC
and hide it, so a native or managed server in another zone is where it shows.

