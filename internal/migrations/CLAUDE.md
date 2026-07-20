# Dwarf `migrations` — schema, indexes, and SQL-authoring rules

> Load when: editing schema/migrations or indexes, or writing any SQL query.
> Coupled with: root `CLAUDE.md` — the engine issues these queries; see its landmine list.

## Schema Column Catalog

The `migrations/*.sql` migration files carry **no prose comments by design** - only the functional
`-- DRIVER: <dialect>` directives the `sequel` runner parses. All schema rationale lives here.

#### `dwarf_flows`

| Column | Meaning |
|---|---|
| `flow_id` | Per-shard auto-increment primary key. The external flowKey is `{shard}-{flow_id}-{flow_token}` |
| `flow_token` | Random token component of the flowKey, guards against id guessing |
| `workflow_url` | URL of the workflow graph this flow runs (the resolve key passed to `Create` and the host's `LoadGraph`) |
| `graph` | JSON of the workflow graph, frozen at `Create` time |
| `baggage` | JSON of the opaque `baggage` map captured at `Create` and passed to every `LoadGraph`/`ExecuteTask` call. Flow-scoped and frozen at `Create`; the engine does not interpret it |
| `status` | Flow lifecycle: `created`/`running`/`interrupted`/`completed`/`failed`/`cancelled`. `created` is internal/transient only (Create's own transaction, Fork's leaf-gate) - a flow is never externally observed in `created`; Create returns it `running` |
| `step_id` | The flow's current step; `0` during fan-out (multiple steps active at one depth) |
| `surgraph_flow_id` | Parent (surgraph) flow id if this is a subgraph flow; `0` otherwise |
| `surgraph_step_id` | PK of the parent's parked surgraph step - the **sole** link from a subgraph child to the step that launched it, so completion/interrupt/resume bind to that exact step even with parallel parked surgraph steps at one depth. (The old depth-based `surgraph_step_depth` link was dropped.) |
| `root_flow_id` | Denormalized **tree-membership** index: the `flow_id` of the root of this flow's subgraph tree, so the whole tree (root + all subgraph descendants) is reachable in one query (`WHERE root_flow_id=?`) instead of a recursive `surgraph_flow_id` walk. Write-once at creation, immutable: a top-level flow (`Create`/`Run`/`Continue`/`Fork`) is its **own** root (`root_flow_id = its own flow_id`, so the root's own row matches the scan); a subgraph child **inherits** the parent's. Single-shard by construction (subgraph parent-shard affinity). It augments, does **not** replace, `surgraph_flow_id`/`surgraph_step_id`: it answers *which flows are in the tree* (membership/set), not *who is whose parent* (structure) - ordered/structural walks (`surgraphChain`, `interruptedSubgraphChain`) still use the surgraph links. See "Denormalized root pointer" |
| `thread_id` | Groups flows in a thread (`Continue`, or `Create` with `FlowOptions.ThreadKey`); defaults to `flow_id` (each flow its own thread) |
| `thread_token` | Token component of the thread's flowKey |
| `trace_parent` | W3C `traceparent` of the flow's root "workflow" span, minted at `Create` (or, for a subgraph, parented to the caller step's span). Reconstructed as the parent of every per-step span. Inherited by `Continue` only as a fresh trace (a new root span is minted per turn); a subgraph inherits the caller step's context, not this column. See "Tracing" |
| `delete_on_completion` | Set from `FlowOptions.DeleteOnCompletion` at `Create`; `1` makes the flow *schedule* its own deletion (stamp `delete_after_ms`) when it reaches `completed`. Root-only (not inherited by children); `failed`/`cancelled`/`interrupted` flows are never auto-deleted. See "Data Retention" |
| `delete_after_ms` | Milliseconds added to `updated_at` to compute the reap time; `0` = keep (default), `>0` = the background reaper deletes the flow's whole subtree (keyed on `root_flow_id`) once `updated_at + delete_after_ms <= NOW`. Stamped on the **root** only - by DeleteOnCompletion (= the 1-min grace, outcome observable meanwhile) and by `Delete`/`Purge` (= 1ms, due immediately). `delete_after_ms > 0` always implies a terminal status; such a flow is excluded from `List`/`History` but still serves its outcome to `Snapshot`/`Await`. Indexed `WHERE delete_after_ms > 0` (partial on pgx/mssql/sqlite, full on mysql). See "Data Retention" |
| `final_state` | JSON state computed at termination - the full merged state of the terminal step(s), unfiltered. Narrowing happens in the workflow's terminal task via `flow.Del`. **Typed `JSON` on MySQL, not `TEXT`** - it carries arbitrary workflow output (same as `state`/`changes`, both `JSON` on MySQL), so a `TEXT` column would silently cap terminal state at 64 KB **on MySQL only** (pgx `TEXT` = 1 GB, mssql `VARBINARY(MAX)`, sqlite `TEXT` have no such limit), failing/truncating a large-output flow on one dialect. Never regress it to `TEXT`; it is never compared in a `WHERE`/`CASE`, so it dodges the JSON-`=` landmine below |
| `forked_from_step` | `Fork` provenance: the *original* fork-point `step_id` this flow was cloned from; `0` for a non-fork flow. Subsumes the origin flow id (derivable via the step's `flow_id`) and pins the exact divergence node. `Continue` excludes forks via `forked_from_step=0`. See "Fork" |
| `created_at` | UTC creation time. Append-only and PK-correlated. Surfaced to tasks via `Flow.CreatedAt()`. A `Fork` clone is a new flow with its own `created_at` = fork time. **Deliberately unindexed on `dwarf_flows`**: no query on the flows table filters or orders on it (`List`/`Purge` age filters anchor on `updated_at` - which for a terminal flow is its finish time, the correct "time since finished" retention signal, and only terminal flows are purged). The fairness scheduler's `created_at` ordering runs on the **steps** copy, which *is* indexed (see `dwarf_steps.created_at`); the flows copy is never a hot-path order/filter key. A `dwarf_flows.created_at` index would be pure write amplification; if a "created in window X" analytics filter is ever added, add the index with the `Query` field then |
| `started_at` | UTC time this attempt began dispatching. Stamped when the flow goes `running` at `Create` (and by `Fork` when the clone goes live); there is no separate `Start`. Distinct from `created_at`, which is the row's INSERT moment. Drives `FlowSummary.Duration()` (`updated_at - started_at`) |
| `updated_at` | UTC time of the last **status transition** (`created`→`running`→terminal, `running`↔`interrupted`). Surfaced to tasks via `Flow.UpdatedAt()`. It is **not** bumped on intra-flow step progress: a running flow advancing through N steps leaves `updated_at` fixed at its go-`running` time - the per-step flow-row writes (transition-tx open+advance, `completeFlow`/fan-in lock-grab) flip the non-indexed `touch` column instead, so the running band of `idx_dwarf_flows_status` does not churn once per step (it moves only when the flow's own status changes). Consequence: for a **running** flow `updated_at ≈ started_at` (so `FlowSummary.Duration()` reads ~0 until it stops); for a **terminal** flow it is the finish time (Duration = total runtime, and `Purge`/`List` `OlderThan` = "time since finished" - both correct, since only terminal flows are retention targets) |
| `touch` | Churn-avoidance toggle (`SMALLINT`, `0`/`1`). Carries **no meaning** and is never SELECTed - it exists only so a flow-row write can (a) acquire the row's write lock without moving the `(status, updated_at)` index entry, and (b) guarantee a value change so `RowsAffected()` reflects the `WHERE` match on every driver (MySQL's default "changed rows" count included). **Every** `UPDATE dwarf_flows` flips it (`touch=1-touch`); the intra-flow-progress writes flip *only* it (no `updated_at`), the status-transition writes flip it *alongside* `updated_at=NOW_UTC()`. A self-assign (`SET col=col`) was rejected for (b): MySQL reports 0 rows changed when no value differs, which would silently break the terminal-status `RowsAffected==0` guards |
| `priority` | Scheduling priority, integer >= 1, lower runs first. Resolved at `Create` from `FlowOptions` else `SetDefaultPriority`; inherited unchanged by `Continue`/subgraph. Immutable |
| `fairness_key` | Fairness bucket. From `FlowOptions`, else the host-supplied key, else `''`. Immutable. **Deliberately unindexed on `dwarf_flows`** (it *is* indexed on `dwarf_steps` as the hot selection key). The only reader on the flows table is the `List`/`Purge` `Query.FairnessKey` filter (the documented "list tenant X" path), a cold per-shard scan on a warm operator path - not worth a secondary-index entry per insert against the write-amplification budget. If tenant-scoped listing ever becomes a routine operational path, add `(fairness_key, flow_id)` with the filter then. A status-less `OlderThan` purge scans for the same reason (`idx_dwarf_flows_status` is unusable without the `status` prefix), and is intentionally left as-is: a standalone `updated_at` index would amplify writes on a churny column - exactly the cost the dropped `created_at` indexes carried. `workflow_name`/`priority` filters are likewise deliberately unindexed |
| `fairness_weight` | Relative dispatch share of the `fairness_key`. From `FlowOptions`, else `1` |
| `error` | Task error string for `failed` flows. Written by `failStep` to the **failing flow only** (`WHERE flow_id=? AND status NOT IN (terminal)`, so first-failure-wins on that flow). Cross-subgraph failure does **not** write the whole chain in one UPDATE - it bubbles up level-by-level: when a subgraph child fails (via cohort accounting, never eagerly - see "Failure back to the parent"), `deliverFlowFailureToParent` surfaces the error to the parked caller step (`subgraph_error`), whose task re-fails on re-dispatch, writing `error` on the parent flow, and so on to the root. (`deliverSubgraphError` does the same re-dispatch from the wedge sweep only.) Surfaced as `FlowOutcome.Error` |
| `cancel_reason` | Reason passed to `Cancel(flowKey, reason)`. Written to every flow in the cancellation chain in the same UPDATE that sets `status='cancelled'`, first-cancel-wins. Surfaced as `FlowOutcome.CancelReason` |
| `time_budget_ms` | Per-flow task time budget, resolved from `FlowOptions.TimeBudget` (else the `SetTimeBudget` default) and frozen at `Create`; the engine imposes no ceiling (a host bounds it before `Create`). Seeds every step's `time_budget_ms`. Inherited by subgraph children **and** by `Continue`/`Fork` (which carry the source's policy). Always stored concrete at `Create`; a `0` is unexpected and falls back to the live engine default at step insert (pure defense) |
| `awaited` | Peer-broadcast gate for flow stops. `0` default; stamped `1` (write-once, `WHERE awaited=0`) at the top of every `Await`/`Poll` (`Run` awaits, so it stamps too). A flow-stop site broadcasts the `statusChange` peer signal only when set - that broadcast's sole purpose is to wake remote awaiters, so a never-awaited (fire-and-forget) flow stops without a `SignalPeers` fan-out; local waiters are woken in-memory regardless. Advisory, never load-bearing: an `Await` whose stamp races the stop's read is caught by its own `awaitPollInterval` re-snapshot, the same backstop that covers any lost wake, and an unreadable flag is treated as `1` (broadcasting is always safe). Never reset; not inherited (`Fork`/`Continue`/subgraph children start at `0` - their awaiters are their own). Unindexed: read by PK (the stop sites' existing flow-row reads, or one batched `IN` scan on the multi-flow Cancel/interrupt/orphan paths) |
| `engine_id` | Forensic provenance stamp: the random per-process id (fresh on every restart) of the engine replica that INSERTed the row - `Create`/`Continue`'s `insertFlowTx` and `Fork`'s clone stamp the creator. Never read by the engine; **deliberately unindexed** (an index on hot-insert tables would be pure write amplification for a rare one-off forensic query). `0` = pre-column row. The same random per-process id is what a replica writes into `dwarf_peers` to be counted for the connection-pool split (see that table below); on the flows/steps rows it is provenance only |

#### `dwarf_steps`

| Column | Meaning |
|---|---|
| `step_id` | Per-shard auto-increment primary key. External stepKey is `{shard}-{step_id}-{step_token}` |
| `flow_id` | Owning flow |
| `step_depth` | Sequential transition depth; fan-out siblings share it. **Purely informational** (History ordering + the surfaced `FlowStep.StepDepth`, useful to see how deep a flow goes) - it is *not* used for the execution DAG (that is `predecessor_id`/`successor_id`), fan-in firing (`lineage_id`/cohort counters), final state (tail steps), or selection. The entry step is `callerStepDepth+1` (1 for a top-level flow; a subgraph continues from its caller's depth); a fan-in step is `max(cohort step_depth)+1` |
| `step_token` | Random token component of the stepKey |
| `task_name` | Graph node name of the task this step executes |
| `state` | JSON input snapshot. Immutable except on retry/resume. A field carried by REFERENCE is **absent** here - see `state_refs` |
| `changes` | JSON output delta the task produced. Always literal: a ref never appears in `changes`, which is what lets one refs column cover the whole scheme |
| `state_refs` | `{"<field>": <anchor step_id>}` - fields whose bytes live in another step's row rather than in this step's `state`, so a large carried field is stored once instead of once per step (measured ~29x fewer state bytes on a fan-out doc-extraction shape). The anchor's bytes may be in **either** its `changes` (a task produced them) or its `state` (the flow's initial input at the entry step; a fan-in's reducer output), so resolution reads both, `changes` shadowing `state`. Own column, not an inline `$ref` key, so `Fork`'s DB-side `INSERT...SELECT` clone can remap anchors with one tiny UPDATE instead of pulling every large state blob through the engine. Empty (`'{}'`) for the overwhelming majority of rows. The full design is in `engine/CLAUDE.md` |
| `interrupt_payload` | JSON outbound payload from `flow.Interrupt()` - what the awaiting caller sees |
| `interrupt_done` | `1` once the interrupt park has been resumed; drives `flow.Interrupt`'s return-vs-arm decision |
| `resume_data` | JSON inbound payload recorded by `Resume`; returned by `flow.Interrupt` on re-dispatch. `'{}'` until resumed |
| `subgraph_done` | `1` once a `flow.Subgraph` park resolved; drives `flow.Subgraph`'s return-vs-arm decision. A retry clears it to re-run the child |
| `subgraph_result` | JSON child `final_state` returned by `flow.Subgraph`. `'{}'` until resolved |
| `subgraph_error` | child error text for a failed `flow.Subgraph` park, returned as the `err`. `''` when none |
| `status` | Step lifecycle: `created`/`pending`/`running`/`interrupted`/`completed`/`failed`/`cancelled` |
| `error` | Error text when `failed`; `''` otherwise |
| `time_budget_ms` | Execution budget; the deadline on the `ExecuteTask` call context. Denormalized from the flow's `time_budget_ms` at step insert (frozen, not the live config), and also self-referenced in the claim CAS to size the crash-recovery lease (`time_budget_ms + leaseMargin`) |
| `attempt` | `flow.Retry` attempt counter, drives the backoff |
| `not_before` | Earliest UTC time the step may execute (`flow.Sleep` / retry backoff) |
| `lease_expires` | Crash-recovery lease; `pollPendingSteps` reclaims `running` steps past this |
| `lease_seq` | Lease **generation** (write fence). Bumped `lease_seq=lease_seq+1` in the claim CAS and returned with the claimed row; every post-execution write to the dispatched step carries `AND lease_seq=?`, so a worker whose lease was lost and re-granted to a peer (slow task overran `budget+leaseMargin`, or the DB wall clock stepped forward past `lease_expires`) writes zero rows and abandons instead of corrupting/terminalizing the peer's re-execution. Genuine `WHERE` predicate, so `RowsAffected` reflects the match on every driver (MySQL included) - no `touch` trick. Bumped **only** by the claim (a lease *grant*); the `pollPendingSteps` expired-lease reset leaves it unchanged, so a step reset-but-not-reclaimed keeps its generation. See "Lease fencing" in `engine/CLAUDE.md` |
| `created_at` | UTC creation time. Read by the refiller two ways: projected as the fairness `ageMs` (`NOW - created_at`), and as the **oldest-first ordering key within a `fairness_key`** (`ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id)` in both refill phases). For that ordering it is the **trailing pair** of `idx_dwarf_steps_selection` (`..., created_at, step_id`), so a key's due steps come off the index already age-ordered and the window needs no sort. Unlike `dwarf_flows.created_at` (unindexed - nothing on the flows table orders on it), the steps copy earns its place in the *existing* hot selection index; it adds no new index and no churn (`created_at` is write-once, and the index's leading `(status, parked)` already repositions every entry on each transition regardless) |
| `started_at` | UTC time the *current attempt* first dispatched. The lease UPDATE stamps it via CASE only on a fresh attempt's first dispatch (`attempt=0 AND subgraph_done=0 AND interrupt_done=0`) and **preserves** it on a continuation (subgraph re-dispatch, interrupt re-dispatch, retry re-dispatch). A retried step's duration includes every attempt. Drives per-step body duration and inter-step wait labels in `FlowRenderer` |
| `updated_at` | UTC time of the last status transition |
| `lineage_id` | Cohort frame: the spawn step's `step_id` on a push, else inherited. Drives explicit `SetFanIn` arrival counting and merge. A cohort-counting device, **not** a DAG. `0` = no `SetFanIn` |
| `cohort_size` | On a fan-out spawn step: number of branches spawned |
| `cohort_arrivals` | On a fan-out spawn step: branches that reached the fan-in; fan-in fires when `arrivals >= size` |
| `cohort_failures` | On a fan-out spawn step: branches that failed via `failStep` with no `onError` (bumped by `propagateCohortFailure`). When the cohort fully arrives with `cohort_failures > 0` the flow is failed instead of creating a fan-in step. `0` = none |
| `fan_out_ordinal` | This branch's index in its fan-out; fan-in merges in this order so list/sum reducers are deterministic. Preserved across an in-place `flow.Retry` rewind and copied verbatim by `Fork`. `0` = not part of a fan-out |
| `predecessor_id` | Step that ran immediately before this one in the execution DAG. `0` = none |
| `successor_id` | Step that runs immediately after this one. `0` = none (exit) |
| `priority` | Denormalized copy of the flow's `priority` for the hot selection path |
| `fairness_key` | Denormalized copy of the flow's `fairness_key` |
| `fairness_weight` | Denormalized copy of the flow's `fairness_weight` |
| `parked` | Selection discriminator. `0` = active; `1` = surgraph park. The selection and saturation indexes lead with `(status, parked)` and the claim CAS requires `parked=parkedNone`, so non-zero rows are excluded from the hot path. See "Step Parking" |
| `engine_id` | Forensic provenance stamp, dual-role: the INSERT stamps the **creator** (entry step, transition successors, fan-in step, Fork clone - the replica whose worker created the row) and the claim CAS **overwrites** it with the **claimer** (the replica that leased and ran this attempt; rides the existing UPDATE, zero extra round trips). So a pending row shows who created it, a running/terminal row who executed it. Never read by the engine; deliberately unindexed; `0` = pre-column row |

#### `dwarf_peers`

The replica registry that backs the observed replica count R (which divides each shard's connection pool - see
`engine/CLAUDE.md` §"Peer discovery"). One row per **live** engine replica, written to **every** shard.

| Column | Meaning |
|---|---|
| `engine_id` | **Primary key** - the replica's random per-process id (the same value stamped as provenance on `dwarf_flows`/`dwarf_steps`). PK, not a mere index: it is *identity*, so a heartbeat is a clean update-by-id and the astronomically-unlikely id collision surfaces as a loud constraint violation rather than a silent double-count. Fresh on every restart, so a restarted replica inserts a new row and its old one ages out (never updated again) |
| `seen_at` | UTC heartbeat timestamp, `NOW_UTC()` at every write (never a bound Go time - one clock per shard). A replica is **counted** while `seen_at > NOW - 4x pingInterval` (so a crashed peer, whose owner sends no goodbye, drops out of the count on its own) and its dead row becomes eligible for the prune **DELETE** once `seen_at < NOW - 8x pingInterval` (hygiene only; the read filter already stopped counting it). The prune is **conditional + statistical** - it runs only when a heartbeat's scan finds `stale > 0` and the replica wins a `1/R` dice roll - so steady state issues no range-DELETEs and the only writes are conflict-free per-PK upserts (see `engine/CLAUDE.md` §"Peer discovery"). Shutdown deletes the row outright |

**No secondary index, by design.** The table holds one row per live replica - a handful, effectively always in the
buffer pool - so the `COUNT`/`DELETE`/`UPDATE` all run as trivial full scans. A `seen_at` index would be pure write
amplification on a hot-ish row for zero read benefit. The PK on `engine_id` is *identity* (the upsert target), not a
scan-speed index. Written and read on **all** shards via `OnEach` (parallel), taking `MAX(count)` across them, so a
future shard-fault-tolerant `OnEach` reads the count from the survivors with no schema change.

## Database Indexing Strategy

The `dwarf_flows` and `dwarf_steps` tables grow indefinitely. The indexing strategy keeps hot-path queries fast
without fragmentation or excessive write amplification.

### Design Principles

1. **Append-only terminal sections.** Indexes leading with `status` partition the B-tree by status. Terminal
   statuses are append-only (entries arrive with monotonically increasing `updated_at`), so terminal sections stay
   well-ordered - no mid-tree page splits.
2. **Small transient sections.** The `pending`/`running` sections churn but stay small (proportional to active work,
   not history); page reuse is efficient.
3. **Partial indexes for PostgreSQL.** Where only non-terminal statuses are queried, Postgres uses a partial index
   filtered to `status IN ('pending','running')`. MySQL and SQL Server use the full composite (no partial support).

### Index Catalog

#### `dwarf_flows`

| Index | Columns | Purpose |
|---|---|---|
| PK | `(flow_id)` | Row lookups by flow ID |
| `idx_dwarf_flows_status` | `(status, updated_at)` | **The orphan sweep** (`detectOrphanedFlows`: `status='running' AND updated_at < NOW-5m`) - a textbook range seek on this exact prefix, and the query the index is *for*. Also the `OlderThan`/`NewerThan` age range. It is **not** an index for `List` (see below), despite an earlier version of this row saying so. Its running band no longer churns per step: intra-flow flow-row writes flip the non-indexed `touch` column, not `updated_at`, so a running flow's entry moves only on a genuine status transition (see the `updated_at`/`touch` catalog rows) |
| `idx_dwarf_flows_workflow_url` | `(workflow_url)` | `List` by workflow URL |
| `idx_dwarf_flows_thread` | `(thread_id, flow_id)` | `Continue` (latest in thread) and `List` by thread |
| `idx_dwarf_flows_surgraph` | `(surgraph_flow_id)`, partial `WHERE surgraph_flow_id > 0` on pgx/sqlite/mssql | Walking the subgraph chain |
| `idx_dwarf_flows_surgraph_step` | `(surgraph_step_id)`, partial `WHERE surgraph_step_id > 0` on pgx/sqlite/mssql | Point lookups on the caller-step link (`WHERE surgraph_step_id=?`): the `flow.Retry` reap (`fork.go`), the parked-step wedge sweep, and `Step`-navigation. Cheap to carry: `surgraph_step_id` is write-once at insert (no update churn/fragmentation) and `0` for every root flow, so the partial index covers only subgraph children. Without it, the retry-reap's unindexed predicate is the SQL-Server clustered-index-scan U-lock **deadlock** class the fan-in `successor_id` fix documented. Same profile as `idx_dwarf_flows_surgraph` above. **Deliberately single-column** (a review suggested `(surgraph_step_id, flow_id)`): the only `ORDER BY flow_id DESC LIMIT 1` on this column was a per-step N+1 in `subgraphHistory`, since replaced by one batched `root_flow_id` scan (`loadSubgraphChildren`); the sole remaining ordered probe (the wedge sweep's latest-child lookup) matches only the handful of retry-attempt children of one caller step and runs on the latency-tolerant recovery loop, so a second key column would only widen every subgraph-child insert against the write-amplification budget for no measurable gain |
| `idx_dwarf_flows_root` | `(root_flow_id)` | One-query whole-tree scans (`WHERE root_flow_id=?`) for the membership walks. A plain (non-partial) index: `root_flow_id` is set for every flow (a root points at itself), so a `> 0` partial would index everything anyway |
| `idx_dwarf_flows_delete_after` | `(delete_after_ms)`, partial `WHERE delete_after_ms > 0` on pgx/mssql/sqlite; full on MySQL | The **reaper's only index**. Its due-scan (`delete_after_ms > 0 AND surgraph_flow_id = 0 AND DATE_ADD_MILLIS(updated_at, delete_after_ms) <= NOW`) runs every ~1min per shard, and `delete_after_ms` is `0` for all but the small set of flows inside their deletion window - exactly the shape a partial index exists for. Without it the reaper full-scans `dwarf_flows` on every tick |

#### `dwarf_steps`

| Index | Columns | Purpose |
|---|---|---|
| PK | `(step_id)` | Row lookups, lease acquisition in `processStep` |
| `idx_dwarf_steps_flow_id` | `(flow_id, step_id)` on MySQL **and SQLite**; `(flow_id)` on pgx/mssql | Per-flow step queries |
| `idx_dwarf_steps_status` | `(status, updated_at)` - partial `WHERE status IN ('pending','running')` on pgx | `pollPendingSteps` recovery and pending discovery |
| `idx_dwarf_steps_selection` | `(status, parked, priority, fairness_key, created_at, step_id)` + covering `not_before, lease_expires, fairness_weight` (an `INCLUDE` on pgx/mssql; trailing key columns on mysql/sqlite, which have no `INCLUDE`) - partial on pgx/mssql/sqlite, full on mysql | Two-level priority+fairness candidate selection (the three-phase refill: `scanBandKeys` -> `planBatch` -> `fetchBandSteps`). The `(status, parked)` prefix excludes terminal and parked rows without an in-memory filter; `priority`/`fairness_key` seek the band and its keys. The trailing `(created_at, step_id)` serves the per-key `ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id)` in **both** refill phases: a key's due steps arrive already age-ordered, so the oldest-per-key aggregate (phase 1) and the oldest-N fetch (phase 3) need no sort. `step_id` is listed explicitly because pgx does not implicitly append the PK to a secondary index (mysql/mssql/sqlite do); it also resolves same-`created_at` ties (fan-out siblings can share a millisecond). Cheap to carry: `created_at`/`step_id` are write-once, and the leading `(status, parked)` already repositions the entry on every transition, so the wider key adds size but no extra churn. The `not_before<=NOW AND lease_expires<=NOW` due-predicates and the projected `fairness_weight` are **carried in the index** so phase 1 never touches the row: without them every candidate entry fetched its heap/clustered row purely to evaluate due-ness, measured on PostgreSQL 18 at 200k due rows as an `Index Scan` with 203,280 buffer hits for 200,000 rows - about one row touch apiece, and the dominant term in the refiller's cost. All three or none: carrying only the two timestamps leaves `fairness_weight` to drag the row in anyway. They are appended, never part of the ordering prefix, so index order and the no-sort property are unchanged. Counter-intuitively this pays on a churned queue table: an index-only scan needs the row's page all-visible (PostgreSQL's visibility map; InnoDB checks a page-level trx id and needs no VACUUM), and a queue churns at its HEAD while a deep backlog sits `pending` and untouched behind it - so the covering scan works precisely when the band scan is most expensive, and degrades to the old plan (never worse) under uniform churn. Cost is storage only: +100% on PostgreSQL, +32% on InnoDB (whose secondary index already carries the PK). Verify with `EXPLAIN (ANALYZE, BUFFERS)` that phase 1 plans as an index-only scan with few heap fetches before assuming the covering columns are doing their job |
| `idx_dwarf_steps_saturation` | `(status, parked, task_url)` - partial as above | Per-task in-flight count for the `dwarf_task_concurrency_running` gauge. Parked rows excluded so a surgraph parent doesn't inflate the executing-slot count |

### `List` gets no indexes of its own, and its `ORDER BY` is not a bug

**Indexes here are earned by load-bearing hot-path queries** - candidate selection, the claim CAS, fan-in, the
tree walks, the reaper - and each is paid for on every INSERT and every status transition of the busiest tables
in the system. `List`/`Purge` are the opposite: an **arbitrary query surface** (status, workflow URL/name,
thread, task name, fairness key, priority, age range, free-text search, in any combination). There is no index
set that covers a cross-product, and adding one per filter would tax the write path to speed up an operator
screen. So most `List` filters are deliberately unindexed (see the `fairness_key` catalog row), and that is a
decision, not an omission.

**In particular, `idx_dwarf_flows_status` cannot serve `List`'s `ORDER BY f.flow_id DESC` + `flow_id` cursor,
and does not need to.** A review filed this as a bug and proposed adding `(status, flow_id)`. Measured on
Postgres (500k flows: 499,600 `completed`, 400 `failed`), the two plans the planner actually picks are
self-balancing:

| `List` call | Plan | Cost |
|---|---|---|
| no status filter | backward PK scan | 6 buffers |
| `Status: completed` (common) | backward PK scan, `Rows Removed by Filter: 1` | 6 buffers |
| `Status: failed` (rare) | index seek -> 400-row band -> top-N heapsort | 404 buffers |

A **rare** status has a small band, so sorting it is cheap. A **common** status makes the backward PK scan
optimal - it finds the first 100 matches in ~101 rows, and is **~67x cheaper** than the index path would be. The
planner *chooses* the PK scan there; it is not "falling back," and the cursor is not "the only thing saving it"
(the review's claim). Cost is bounded by `min(band, ~limit/selectivity)`; the only genuinely expensive case is a
status band that is large in absolute terms (e.g. 100k `interrupted`), on a cold operator path.

**And do NOT "fix" it by dropping the `ORDER BY`.** It is what makes the cursor correct: pagination is keyset
(`flow_id < cursor`, cursor = the smallest `flow_id` returned). Unordered, page 1 returns an arbitrary subset
and page 2 asks for `flow_id < min(page 1)` - so every match above that id which page 1 did not happen to return
is **silently never shown**. `flow_id` is the cursor precisely because it is immutable; `updated_at` moves on
every status transition, so keyset-paginating in the index's own order would shuffle rows between pages.

### Status predicates are inlined literals, not bound parameters

Every `status` comparison in a **`WHERE`** clause inlines the `workflow.Status*` constant into the SQL string
(`"...WHERE status='"+workflow.StatusRunning+"'..."`, `"... status IN ('"+workflow.StatusCreated+"', ...)"`)
rather than binding it (`status=?`). This is load-bearing for index usability, not a style choice:

- **SQL Server refuses a filtered index for a parameterized predicate.** The step indexes are filtered
  `WHERE status IN ('pending','running')` (mssql) / partial (pgx/sqlite). SQL Server compiles one cached plan
  that must be valid for *every* parameter value - and since a filtered index omits rows (`status='completed'`
  is not in it), a plan that used it could return wrong results for some `@status`, so the optimizer **rejects
  the filtered index and scans** (a full clustered-index scan on the hot refiller band scan, lease recovery, and
  the saturation gauge). A **literal** `status='pending'` is provably a subset of the filter, so the index is
  used. (`OPTION (RECOMPILE)` also works but recompiles every execution - wrong for hot queries.) SQLite's
  partial-index prover has the same gap; Postgres is safe via custom plans but a literal is deterministic and
  never worse. MySQL runs these indexes unfiltered (no partial-index support), so a literal is neutral there.
- **Safe to concatenate:** the values are `workflow.Status*` **constants** the engine controls. The one
  caller-supplied filter (`List`/`Purge`'s `Query.Status`, `history.go`) is validated with
  `workflow.IsValidStatus` **before** it is inlined and rejected `400` otherwise, so only a known constant -
  never arbitrary input - reaches the SQL string; there is no injection surface. Inlining is the fix chosen over
  unfiltering the mssql indexes, which would bloat them with the unbounded terminal-step history the partial
  index exists to exclude. Note the `List` win is *not* the filtered-index case (`idx_dwarf_flows_status` is
  unfiltered): there a parameter defeats the seek because the `ORDER BY flow_id DESC` + `LIMIT` under a
  density-average estimate makes the optimizer clustered-scan for a *rare* status; the literal lets it read the
  histogram and seek. Same fix, different reason.

**Two deliberate exceptions stay bound (`?`):** (1) `SET status=?` **assignments** (an assignment is not a
predicate and never selects an index); (2) a `WHERE` predicate whose status is a runtime *variable* on a
PK-keyed statement (the lock-contention recovery reset in `execution.go`, `WHERE step_id=? AND status=?` with a
computed `fromStatus`) - PK seek, no index to help, and the value is not a literal. A new query comparing
`status` in a `WHERE` must inline the constant (validating first if the value is caller-supplied) unless it
falls in one of these two.

### `LIMIT_OFFSET` requires an `ORDER BY` on SQL Server

`sequel`'s `LIMIT_OFFSET(limit, offset)` macro compiles to `LIMIT … OFFSET …` on mysql/pgx/sqlite but to
`OFFSET … ROWS FETCH NEXT … ROWS ONLY` on **mssql**, and SQL Server's `OFFSET/FETCH` is a **syntax error
without a preceding `ORDER BY`**. So every `LIMIT_OFFSET` statement must carry an `ORDER BY` - **even a pure
existence probe** (`SELECT 1 … LIMIT_OFFSET(1, 0)`) where the ordering is semantically irrelevant (any match
suffices). The `ORDER BY step_id` on the refiller's due-exists probe (`scheduling.go`) looks removable and is
not; a review that flagged it as "needless" missed the mssql requirement. Do not strip an `ORDER BY` from a
`LIMIT_OFFSET` query. (If a cheap order is wanted, `step_id` - the PK - is the natural choice.)

### MySQL Column Defaults

In `-- DRIVER: mysql` schema sections, `TEXT`/`BLOB`/`JSON` columns cannot take a bare literal `DEFAULT` (MySQL error
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
four dialects. The `interrupt_payload='{}'` first-writer-wins guard in `handleInterrupt` (`execution.go`) hit exactly
this: on MySQL the payload write matched nothing and `flow.Interrupt` payloads came back empty. It now branches on
`db.DriverName()` - `CAST(interrupt_payload AS CHAR)='{}'` for MySQL, `interrupt_payload=0x7B7D` for SQL Server.
**Assignments** (`SET col=?`) and the column `DEFAULT` are unaffected by the *comparison* rule - only `=`/`<>`
comparisons in a `WHERE`/`CASE` need the per-driver treatment. Any new query comparing a payload column to a literal
must apply the same treatment.

### Payload columns are binary on SQL Server, and every payload bind is a Go `[]byte`

On the `-- DRIVER: mssql` blocks the nine JSON payload columns - `dwarf_flows.graph`/`baggage`/`final_state` and
`dwarf_steps.state`/`changes`/`interrupt_payload`/`state_refs`/`resume_data`/`subgraph_result` - are
**`VARBINARY(MAX) NOT NULL DEFAULT 0x7B7D`** (`0x7B7D` is `{}`), not `NVARCHAR(MAX)`. The genuinely-textual columns
(`error`, `subgraph_error`, `cancel_reason`, `workflow_url`, `workflow_name`, `task_name`, `task_url`,
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
can catch the difference. This shipped once already: the "State object cleanup" commit changed the payload fields
from `string` to `[]byte` and deleted the `string(...)` conversions at the bind sites, which looked like redundant
ceremony and were in fact load-bearing encoding conversions - it corrupted every flow on SQL Server.

**The invariant, and why the column type enforces it better than a convention could.** Every bind to a payload
column is a Go `[]byte`; every bind to a text column is a Go `string`. Get it backwards and SQL Server now raises
`Implicit conversion from data type nvarchar to varbinary(max) is not allowed` - a **hard error at the first write**,
not silent corruption. That is the whole point of the binary column: the failure mode moves from undetectable to
unmissable, and the database enforces what Go's type system cannot (both `string` and `[]byte` satisfy `any`).
Bonus: `NVARCHAR` stores UTF-16, so ASCII JSON cost 2 bytes/char on the wire and on disk; `VARBINARY` **halves**
SQL Server payload storage and transfer, which matters on a byte-throughput-bound engine.

**Cost, accepted knowingly:** `SELECT state FROM dwarf_steps` in SSMS returns hex rather than readable JSON, and
server-side `JSON_VALUE` on these columns is foreclosed (already deferred - see the `JSON_FIELD` note in
`engine/CLAUDE.md`).

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
   comparisons are unaffected.) The lesson surfaced in a test that backdated `updated_at` with `time.Date(...)`; the
   fix was to backdate with `DATE_ADD_MILLIS(NOW_UTC(), -ms)` - DB clock, native format. Never round-trip a timestamp
   out to Go and back into a `WHERE`/`SET`; recompute it in SQL.

