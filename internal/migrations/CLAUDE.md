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
| `notify_on_stop` | Set from `FlowOptions.NotifyOnStop` at `Create`; `1` fires the `FlowStopped` callback (with the flow's baggage on ctx) when the flow stops, `0` = no notification. The host resolves the delivery target from baggage - the engine stores no address. `Continue` **inherits** the thread's flag; `Fork` forces it **off** (a debug clone never notifies) |
| `delete_on_completion` | Set from `FlowOptions.DeleteOnCompletion` at `Create`; `1` makes the flow delete itself (and cascade to subgraph descendants) the instant it reaches `completed`. Root-only (not inherited by children); `failed`/`cancelled`/`interrupted` flows are never auto-deleted. See "Data Retention" |
| `final_state` | JSON state computed at termination - the full merged state of the terminal step(s), unfiltered. Narrowing happens in the workflow's terminal task via `flow.Delete`/`Transform` |
| `forked_from_step` | `Fork` provenance: the *original* fork-point `step_id` this flow was cloned from; `0` for a non-fork flow. Subsumes the origin flow id (derivable via the step's `flow_id`) and pins the exact divergence node. `Continue` excludes forks via `forked_from_step=0`. See "Fork" |
| `created_at` | UTC creation time. Append-only and PK-correlated. Surfaced to tasks via `Flow.CreatedAt()`. A `Fork` clone is a new flow with its own `created_at` = fork time. **Deliberately unindexed**: no query filters or orders on it (`List`/`Purge` age filters anchor on `updated_at` - which for a terminal flow is its finish time, the correct "time since finished" retention signal, and only terminal flows are purged - and the fairness `ageMs` only *projects* `NOW - created_at` while ordering by `step_id`). A `created_at` index would be pure write amplification; if a "created in window X" analytics filter is ever added, add the index with the `Query` field then |
| `started_at` | UTC time this attempt began dispatching. Stamped when the flow goes `running` at `Create` (and by `Fork` when the clone goes live); there is no separate `Start`. Distinct from `created_at`, which is the row's INSERT moment. Drives `FlowSummary.Duration()` (`updated_at - started_at`) |
| `updated_at` | UTC time of the last **status transition** (`created`→`running`→terminal, `running`↔`interrupted`). Surfaced to tasks via `Flow.UpdatedAt()`. It is **not** bumped on intra-flow step progress: a running flow advancing through N steps leaves `updated_at` fixed at its go-`running` time - the per-step flow-row writes (transition-tx open+advance, `completeFlow`/fan-in lock-grab) flip the non-indexed `touch` column instead, so the running band of `idx_dwarf_flows_status` does not churn once per step (it moves only when the flow's own status changes). Consequence: for a **running** flow `updated_at ≈ started_at` (so `FlowSummary.Duration()` reads ~0 until it stops); for a **terminal** flow it is the finish time (Duration = total runtime, and `Purge`/`List` `OlderThan` = "time since finished" - both correct, since only terminal flows are retention targets) |
| `touch` | Churn-avoidance toggle (`SMALLINT`, `0`/`1`). Carries **no meaning** and is never SELECTed - it exists only so a flow-row write can (a) acquire the row's write lock without moving the `(status, updated_at)` index entry, and (b) guarantee a value change so `RowsAffected()` reflects the `WHERE` match on every driver (MySQL's default "changed rows" count included). **Every** `UPDATE dwarf_flows` flips it (`touch=1-touch`); the intra-flow-progress writes flip *only* it (no `updated_at`), the status-transition writes flip it *alongside* `updated_at=NOW_UTC()`. A self-assign (`SET col=col`) was rejected for (b): MySQL reports 0 rows changed when no value differs, which would silently break the terminal-status `RowsAffected==0` guards |
| `priority` | Scheduling priority, integer >= 1, lower runs first. Resolved at `Create` from `FlowOptions` else `SetDefaultPriority`; inherited unchanged by `Continue`/subgraph. Immutable |
| `fairness_key` | Fairness bucket. From `FlowOptions`, else the host-supplied key, else `''`. Immutable. **Deliberately unindexed on `dwarf_flows`** (it *is* indexed on `dwarf_steps` as the hot selection key). The only reader on the flows table is the `List`/`Purge` `Query.FairnessKey` filter (the documented "list tenant X" path), a cold per-shard scan on a warm operator path - not worth a secondary-index entry per insert against the write-amplification budget. If tenant-scoped listing ever becomes a routine operational path, add `(fairness_key, flow_id)` with the filter then. A status-less `OlderThan` purge scans for the same reason (`idx_dwarf_flows_status` is unusable without the `status` prefix), and is intentionally left as-is: a standalone `updated_at` index would amplify writes on a churny column - exactly the cost the dropped `created_at` indexes carried. `workflow_name`/`priority` filters are likewise deliberately unindexed |
| `fairness_weight` | Relative dispatch share of the `fairness_key`. From `FlowOptions`, else `1` |
| `error` | Task error string for `failed` flows. Written by `failStep` to the **failing flow only** (`WHERE flow_id=? AND status NOT IN (terminal)`, so first-failure-wins on that flow). Cross-subgraph failure does **not** write the whole chain in one UPDATE - it bubbles up level-by-level: when a subgraph child fails (via cohort accounting, never eagerly - see "Failure back to the parent"), `deliverFlowFailureToParent` surfaces the error to the parked caller step (`subgraph_error`), whose task re-fails on re-dispatch, writing `error` on the parent flow, and so on to the root. (`deliverSubgraphError` does the same re-dispatch from the wedge sweep only.) Surfaced as `FlowOutcome.Error` |
| `cancel_reason` | Reason passed to `Cancel(flowKey, reason)`. Written to every flow in the cancellation chain in the same UPDATE that sets `status='cancelled'`, first-cancel-wins. Surfaced as `FlowOutcome.CancelReason` |
| `time_budget_ms` | Per-flow task time budget, resolved from `FlowOptions.TimeBudget` (else the `SetTimeBudget` default) and frozen at `Create`; the engine imposes no ceiling (a host bounds it before `Create`). Seeds every step's `time_budget_ms`. Inherited by subgraph children **and** by `Continue`/`Fork` (which carry the source's policy). Always stored concrete at `Create`; a `0` is unexpected and falls back to the live engine default at step insert (pure defense) |

#### `dwarf_steps`

| Column | Meaning |
|---|---|
| `step_id` | Per-shard auto-increment primary key. External stepKey is `{shard}-{step_id}-{step_token}` |
| `flow_id` | Owning flow |
| `step_depth` | Sequential transition depth; fan-out siblings share it. **Purely informational** (History ordering + the surfaced `FlowStep.StepDepth`, useful to see how deep a flow goes) - it is *not* used for the execution DAG (that is `predecessor_id`/`successor_id`), fan-in firing (`lineage_id`/cohort counters), final state (tail steps), or selection. The entry step is `callerStepDepth+1` (1 for a top-level flow; a subgraph continues from its caller's depth); a fan-in step is `max(cohort step_depth)+1` |
| `step_token` | Random token component of the stepKey |
| `task_name` | Graph node name of the task this step executes |
| `state` | JSON input snapshot. Immutable except on retry/resume |
| `changes` | JSON output delta the task produced |
| `interrupt_payload` | JSON outbound payload from `flow.Interrupt()` - what the awaiting caller sees |
| `interrupt_done` | `1` once the interrupt park has been resumed; drives `flow.Interrupt`'s return-vs-arm decision |
| `resume_data` | JSON inbound payload recorded by `Resume`; returned by `flow.Interrupt` on re-dispatch. `'{}'` until resumed |
| `subgraph_done` | `1` once a `flow.Subgraph` park resolved; drives `flow.Subgraph`'s return-vs-arm decision. A retry clears it to re-run the child |
| `subgraph_result` | JSON child `final_state` returned by `flow.Subgraph`. `'{}'` until resolved |
| `subgraph_error` | child error text for a failed `flow.Subgraph` park, returned as the `err`. `''` when none |
| `status` | Step lifecycle: `created`/`pending`/`running`/`interrupted`/`completed`/`failed`/`cancelled` |
| `goto_next` | Task-requested `flow.Goto` target; `''` = none |
| `error` | Error text when `failed`; `''` otherwise |
| `time_budget_ms` | Execution budget; the deadline on the `ExecuteTask` call context. Denormalized from the flow's `time_budget_ms` at step insert (frozen, not the live config), and also self-referenced in the claim CAS to size the crash-recovery lease (`time_budget_ms + leaseMargin`) |
| `attempt` | `flow.Retry` attempt counter, drives the backoff |
| `not_before` | Earliest UTC time the step may execute (`flow.Sleep` / retry backoff) |
| `lease_expires` | Crash-recovery lease; `pollPendingSteps` reclaims `running` steps past this |
| `lease_seq` | Lease **generation** (write fence). Bumped `lease_seq=lease_seq+1` in the claim CAS and returned with the claimed row; every post-execution write to the dispatched step carries `AND lease_seq=?`, so a worker whose lease was lost and re-granted to a peer (slow task overran `budget+leaseMargin`, or the DB wall clock stepped forward past `lease_expires`) writes zero rows and abandons instead of corrupting/terminalizing the peer's re-execution. Genuine `WHERE` predicate, so `RowsAffected` reflects the match on every driver (MySQL included) - no `touch` trick. Bumped **only** by the claim (a lease *grant*); the `pollPendingSteps` expired-lease reset leaves it unchanged, so a step reset-but-not-reclaimed keeps its generation. See "Lease fencing" in `engine/CLAUDE.md` |
| `created_at` | UTC creation time. Deliberately unindexed (see the `dwarf_flows.created_at` note); the refiller reads it only as the projected fairness `ageMs` (`NOW - created_at`), ordering by `step_id` |
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
| `idx_dwarf_flows_status` | `(status, updated_at)` | `List` by status; the `OlderThan`/`NewerThan` age range; the orphan/wedge sweep age guards. Its running band no longer churns per step: intra-flow flow-row writes flip the non-indexed `touch` column, not `updated_at`, so a running flow's entry moves only on a genuine status transition (see the `updated_at`/`touch` catalog rows) |
| `idx_dwarf_flows_workflow_url` | `(workflow_url)` | `List` by workflow URL |
| `idx_dwarf_flows_thread` | `(thread_id, flow_id)` | `Continue` (latest in thread) and `List` by thread |
| `idx_dwarf_flows_surgraph` | `(surgraph_flow_id)`, partial `WHERE surgraph_flow_id > 0` on pgx/sqlite/mssql | Walking the subgraph chain |
| `idx_dwarf_flows_surgraph_step` | `(surgraph_step_id)`, partial `WHERE surgraph_step_id > 0` on pgx/sqlite/mssql | Point lookups on the caller-step link (`WHERE surgraph_step_id=?`): the `flow.Retry` reap (`fork.go`), the parked-step wedge sweep, and `Step`-navigation. Cheap to carry: `surgraph_step_id` is write-once at insert (no update churn/fragmentation) and `0` for every root flow, so the partial index covers only subgraph children. Without it, the retry-reap's unindexed predicate is the SQL-Server clustered-index-scan U-lock **deadlock** class the fan-in `successor_id` fix documented. Same profile as `idx_dwarf_flows_surgraph` above. **Deliberately single-column** (a review suggested `(surgraph_step_id, flow_id)`): the only `ORDER BY flow_id DESC LIMIT 1` on this column was a per-step N+1 in `subgraphHistory`, since replaced by one batched `root_flow_id` scan (`loadSubgraphChildren`); the sole remaining ordered probe (the wedge sweep's latest-child lookup) matches only the handful of retry-attempt children of one caller step and runs on the latency-tolerant recovery loop, so a second key column would only widen every subgraph-child insert against the write-amplification budget for no measurable gain |
| `idx_dwarf_flows_root` | `(root_flow_id)` | One-query whole-tree scans (`WHERE root_flow_id=?`) for the membership walks. A plain (non-partial) index: `root_flow_id` is set for every flow (a root points at itself), so a `> 0` partial would index everything anyway |

#### `dwarf_steps`

| Index | Columns | Purpose |
|---|---|---|
| PK | `(step_id)` | Row lookups, lease acquisition in `processStep` |
| `idx_dwarf_steps_flow_id` | `(flow_id, step_id)` on MySQL; `(flow_id)` on pgx/mssql | Per-flow step queries |
| `idx_dwarf_steps_status` | `(status, updated_at)` - partial `WHERE status IN ('pending','running')` on pgx | `pollPendingSteps` recovery and pending discovery |
| `idx_dwarf_steps_selection` | `(status, parked, priority, fairness_key)` - partial on pgx/mssql/sqlite, full on mysql | Two-level priority+fairness candidate selection. The `parked` second column excludes parked rows without an in-memory filter |
| `idx_dwarf_steps_saturation` | `(status, parked, task_url)` - partial as above | Per-task in-flight count for the `dwarf_task_concurrency_running` gauge. Parked rows excluded so a surgraph parent doesn't inflate the executing-slot count |

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

**Comparing a MySQL `JSON` column to a string literal does not match.** `WHERE json_col = '{}'` returns zero rows on
MySQL - the JSON-typed column is not implicitly compared against the bare SQL string `'{}'` (you'd need
`CAST(json_col AS CHAR) = '{}'` or `json_col = CAST('{}' AS JSON)`). The same `= '{}'` predicate *does* match on
SQLite (`TEXT`), Postgres (`JSONB` casts the unknown literal), and SQL Server (`NVARCHAR`), so a single shared query
string silently no-ops only on MySQL. The `interrupt_payload='{}'` first-writer-wins guard in `handleInterrupt`
(`execution.go`) hit exactly this: on MySQL the payload write matched nothing and `flow.Interrupt` payloads came back
empty. It now branches on `db.DriverName()` to use `CAST(interrupt_payload AS CHAR)='{}'` for MySQL. **Assignments**
(`SET col='{}'`) and the parenthesized column `DEFAULT ('{}')` are unaffected - only `=`/`<>` *comparisons* against a
JSON column in a `WHERE`/`CASE` need the cast. Any new query comparing a JSON/JSONB column to a literal must apply the
same per-driver treatment.

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

