# Dwarf `engine` package — orchestrator internals

> Load when: editing anything under `engine/` - operations, execution/scheduling, subgraphs, fan-in,
> crash recovery, metrics/tracing, sharding, or the Host seam.
> Coupled with: root `CLAUDE.md` (conventions, Core Concepts vocabulary, radiating landmines) and the
> `workflow/`, `migrations/`, `fixtures/` package docs it cross-references.

### The Host interface (how the engine reaches the outside world)

The graph/task seam is a single **`Host`** interface, registered once via `SetHost`; the observability
providers below are injected separately. It has exactly two methods, both required.

**THE ENGINE SENDS NOTHING TO ITS PEERS, and nothing may be added that does.** Replicas coordinate purely
by reading the database they share - work discovery, `Await` wakes and fleet membership are all polled, on
cadences the engine derives - so there is no inter-replica transport in the contract and no host obligation
to provide one. Three signal kinds have existed here and all three were deleted after measurement, in this
order: a per-step work doorbell (volume O(steps), and it bought no latency because every piston was already
scanning at its cycle interval), a per-flow stop broadcast (the await latch's detector reads the shared
rows on a tighter cadence than a broadcast could beat), and a fleet-membership nudge (peers re-read the
registry every 250ms, which is faster than a broadcast converges). The rule that killed each: a signal may
only ACCELERATE a convergence the database already guarantees, and once the poll is faster than the
message, it accelerates nothing. Do not reintroduce one without showing it beats the poll it would replace.

The interface methods:

- **`LoadGraph(ctx, workflowURL string) (*workflow.Graph, error)`** - fetches a workflow graph by name.
  Called at `Create` (and on subgraph spawn); the graph JSON is then frozen on the flow row. The flow's opaque
  baggage rides on ctx (`workflow.BaggageFrom(ctx)`) for identity-dependent loading (authz, per-actor graphs).
- **`ExecuteTask(ctx, taskName string, flow *workflow.Flow) error`** - executes one task. Receives the flow
  carrier with state pre-populated; writes its changes back onto the flow. The engine never knows *how* the task is
  reached (local call, RPC, message bus). Any error the task returns is terminal for that attempt: routed via the
  graph's `onError` transition if one exists, else the step fails. A **panic** in the in-process handler is caught at
  the call boundary and treated as such an error (see "Host-call panic isolation"). The engine never sniffs status
  codes or error text; a task backing off on a transient failure detects that itself and arms `flow.Retry`.
- **`*slog.Logger`** - structured logging sink (`SetLogger`); defaults to a **discard** logger (the engine and
  its sequel DB layer stay silent until a logger is injected, rather than writing to the application-owned
  `slog.Default()` - the library convention). A nil logger resets to that silent default. The engine logs through
  the `…Context` variants so a context-aware handler (e.g. the `otelslog` bridge) can correlate records with the
  active step span. The injected logger is also handed to sequel (only when explicitly set), so the SQL layer's
  migration logs flow through the same sink.
- **`metric.MeterProvider`** - OTEL meter provider (`SetMeterProvider`); defaults to the global
  `otel.GetMeterProvider()` (no-op unless the host configures the SDK). The engine builds its `dwarf_*`
  instruments under the `github.com/microbus-io/dwarf` scope (see "Metrics" below).
- **`trace.TracerProvider`** - OTEL tracer provider (`SetTracerProvider`); defaults to the global
  `otel.GetTracerProvider()` (no-op unless the host configures the SDK). The engine mints the root
  "workflow" span at `Create` and a per-step span in `processStep`, both under the
  `github.com/microbus-io/dwarf` scope (see "Tracing" below). The host injects only the provider - no
  span code.

The **baggage** is opaque to the engine: set once at `Create` via `FlowOptions.Baggage` (an `any`, like
`initialState`), stored on the flow row (the `baggage` column), and delivered on the dispatch **context** to every
`LoadGraph`/`ExecuteTask` call for the flow's lifetime - the host reads it with `workflow.BaggageFrom(ctx)` (which
lives in the `workflow` package so task code needn't import `engine`). Authored in one typed place, observed
ambiently; most callbacks ignore it. The engine never interprets it; a host carries actor claims / tenant identity
there, receiving it back as a `workflow.State` (the JSON-decoded form, numbers as float64), like flow state.
`BaggageFrom` returns a `State` directly (a nil State when none was set); the set value (`FlowOptions.Baggage`)
stays an `any` a host fills with a struct or map. (Unlike W3C/OTEL request baggage this is *flow*-scoped and frozen
at `Create`, not per-request mutable.) See "Identity / baggage propagation".

### Backpressure is the task's or host's job, never the engine's

The engine deliberately holds **no** backpressure machinery - no rate valve, no circuit breaker, no failure
dispositions. An earlier design had all three; they were removed and must not return. The reason is structural:
the engine's only vantage is the **task URL**, and that is the wrong axis for every scarcity that actually causes
backpressure.

- A **rate limit** is keyed by the *account*, which is invisible to the engine, shared across many task URLs, and a
  single task URL may even span several accounts. A controller keyed on the task URL throttles a dimension that was
  never the constraint - and for a single never-satisfiable request (one larger than the whole per-window budget) it
  ratchets throughput toward zero and never recovers, because the request keeps failing identically.
- **Availability** is keyed by the *downstream service the task calls*, also invisible: the engine sees the task's
  call site, never what that task reaches. A present task whose own downstream is down is not the engine's to detect.

So resource-accurate control must live in the layer that holds the resource identity. That is the **task itself**
(it sees its downstream's 429/503, knows the account, and arms `flow.Retry` with an appropriate horizon), or the
**host adapter** for the one case only it can see - *no responder for the dispatch* (the task's hosting microservice
is absent), which the host detects and rides out by arming `flow.Retry` on the carrier. The engine just executes,
routes `onError`, and honors the backoff primitives.

A consequence of having no breaker is no probe *election*: each parked step is its own probe and discovers recovery
independently on its own backoff. The trade is the breaker's coordinated backlog release (instant unblock the moment
one probe succeeds) for zero engine-side policy and no shared-state machinery to coordinate across replicas.

### Host-supplied payloads are NOT precision-checked (a KNOWN, backlogged punt)

State is float64-domain, so an integer-shaped number beyond ±2^53 does not survive the JSON round trip and comes
back silently rounded; and a NUL (`U+0000`) in a string is rejected by Postgres `JSONB`. Neither is guarded at
ingress anymore - the former write-side storability guard (and its whole `internal/jsonx` package) was removed by
deliberate decision (the two edge cases are rare, and the full rationale + workarounds are in `workflow/CLAUDE.md`).
There is **no** 400 for either at any ingress point (`Create`/`Run`, `resume`, `Fork`'s overrides, `Continue`'s
`additionalState`); a host handing an oversized integer or a NUL now gets a rounded value / a Postgres write
failure rather than a clean 400. The host inputs are still normalized to a `workflow.State` at each door (via
`workflow.NewState`, which JSON-round-trips a map/struct - so `Continue`'s `additionalState` is canonicalized for
reducer comparison, see "Continue" below), but that normalization only decodes; it does not range-check. If a guard is reintroduced, it must run on the **raw**
caller bytes *before* the decode (the decode rounds >2^53 to `float64`) and must **not** re-check the engine's own
derived merges - a legitimate `ReducerAdd` sum past 2^53 stores and round-trips fine but marshals integer-shaped,
so a naive re-check would falsely reject it.

### Size and count limits are the host's job, not the engine's

The engine enforces **no** size or count bound anywhere - not on initial state, `baggage`, the per-flow-frozen
graph JSON, interrupt/resume payloads, `forEach` fan-out width (one step row + full state snapshot per array
element), or subgraph nesting depth. This is the **same structural reason** as backpressure and the absent
time-budget ceiling: the sizing that matters is workload-defined and keyed on identity the engine cannot see.
A cap that fits a small control flow would reject a document-processing workflow that legitimately channels
tens of megabytes of state per flow, and picking one number for both is impossible - so the engine refuses to
own the number. The host holds the caller/tenant identity a quota turns on, so it enforces limits where that
context lives: reject an over-large `initialState`/`Baggage` before `Create`, cap a `forEach` source array in
an author-space entry task, bound retention (recall `Purge` deletes ≤4096 roots/call, so a retention job
loops). For a **pass-through host** that adds no policy of its own, the obligation flows through to the
application using that host - it is not silently absorbed anywhere.

Subgraph nesting depth is the one axis that was *also* a latent crash vector, now closed: `Fork` clones the
tree with an **explicit LIFO worklist** (`cloneTree` drives `cloneOneFlow` per flow), not recursion, so
arbitrarily deep nesting costs O(1) goroutine stack and only one flow's clone state is live at a time. The
former recursive `cloneSubtree` held every ancestor's clone state (including each flow's `graph`/`baggage`
JSON) on the stack at once and, at pathological depth, could overflow the goroutine stack - fatal, since a Go
stack overflow is *not* recoverable by `errors.CatchPanic`, unlike a host-call panic. Deep nesting now costs
only bounded storage (one flow row + step rows per level), which falls back under the host-quota rule above.

### Configuration (`Set*` methods)

Configuration is applied through `Set*` methods, each returning an `error` rather than chaining a `*Engine` (so
every setter can surface an error). They split into two groups by whether the knob can change safely on a running
engine:

- **Live** (take effect immediately, callable any time): `SetMaxOpenConns` (an **expert override** that pins
  every shard's pool exactly - the benchmarking / external-pooler path; normal deployments never call it),
  `SetTimeBudget`, `SetDefaultPriority`. `SetTimeBudget`/`SetDefaultPriority` are read fresh at each `Create` (an
  existing flow keeps the budget/priority frozen at its own `Create`). The replica count that divides the derived
  pools is NOT a setter - it is **read** live from the shared `dwarf_peers` registry (see "Peer discovery" below), and
  `recomputePools` pushes each shard's recomputed size through the per-shard `sequel.DB` pool setters (hot/atomic);
  the uniform override rides `ShardSet.SetMaxIdleConns`/`SetMaxOpenConns`. **The derived *worker* ceiling follows the
  pools, and EVERY path that changes a pool must re-derive it** (`recomputeWorkerCeiling`): the ceiling encodes how
  fast a completion storm drains through `M` connections, so a shrunken pool must never leave a stale, too-high bound
  on exactly the storm the ceiling exists to contain. There are two such paths and both call it - `recomputePools`
  (a fleet change) and `SetMaxOpenConns` (the live override). The override path is not optional: once an override is
  set, `recomputePools` early-returns (the override pins the pools), so `SetMaxOpenConns` is the *only* path left that
  can re-derive the ceiling. Pinned by `TestPoolSizing_CeilingFollowsLivePoolChange`.
  `shardRTTMs` (the Startup RTT probes the ceiling is derived from) is published and read **under `shardsLock`** -
  its readers are the peer-signal goroutines and this live setter, while `Startup` reassigns it on a restart, and an
  unsynchronized map read/write is a *fatal throw*, not a recoverable panic.
- **Construction-time only** (return an error if called after `Startup`): `SetShard(ShardSpec)`, `SetWorkers`,
  `SetHost`, `SetLogger`, `SetMeterProvider`, `SetTracerProvider` (plus the `SetDebugLogger` convenience). Applying
  these on a running engine would mean opening live connections and mutating a shard set that flow keys already
  encode (`SetShard` - see "Database Sharding"), resizing the worker pool + candidate
  cache (`SetWorkers`), or re-resolving a frozen provider - so the setter **rejects** it with an explicit error rather
  than silently no-op'ing. The error wording is `"<what> cannot be changed after Startup"`.

For the observability providers specifically (`SetLogger`/`SetMeterProvider`/`SetTracerProvider`): the engine
resolves the logger/tracer/meter once at startup (the logger feeds the worker hot path and is read lock-free; the
meter registers an async gauge callback) and passes all three into `ShardSet.Open` (via `database.Config`), which
wires them into every shard's sequel DB. Hot-swapping a provider on a live engine is deliberately unsupported: a half-hot version
that only re-pointed the DBs (sequel's setters are atomic/hot) but left the engine's own logger/tracer/metrics
frozen would be inconsistent, and a full-hot version (atomic logger + tracer re-resolve + meter rebuild/Unregister)
is real complexity for a need that does not arise in practice.

### Engine Operations

These are methods on `*engine.Engine`.

**Every public operation is gated on a live engine (`ensureStarted` -> 503).** Not a nicety: without it a
stopped engine *lies*. The key-addressed operations all route through `ShardSet.Shard`, which returns
**"flow not found" (404)** when no shard is open - so a stopped engine tells the caller its flow does not
exist, and the caller may act on that (stop retrying, recreate the work). The cross-shard operations are
worse: `List`/`Purge`/`ShardInfo` fan out over an **empty index set** and return **success with an empty
result** - "you have no flows." Both are indistinguishable from the truth; 503 says the one true thing. The
state is reachable in production, not just by API misuse: `Shutdown` -> `ShardSet.Close` nils the index set,
so a host still serving while it tears the engine down (or a request in flight when `Shutdown` lands) hits
it. `pickShard` keeps its **own** no-open-shards guard, because `started` can flip between the gate and the
pick - and there, indexing the empty index slice (`rand.IntN(0)`) **panicked the host's process**. Inbound
Pinned by `TestPoolSizing_NoOpenShardsIs503`.

**Policy is set once at genesis and inherited by derivation; the operation is the inherit-vs-default selector.**
A flow's policy (priority, fairness, time budget, baggage, thread membership) is authored
explicitly via `FlowOptions` only at **genesis** - `Create`/`Run`. **Derived** operations carry policy from
their source rather than taking `FlowOptions`: `Continue` inherits the thread's policy, `Fork` inherits the
origin flow's, and subgraph children inherit the parent's. So there is no `opts` on `Continue`/`Fork`; choosing
one of those operations *is* choosing "inherit." The explicit-policy escape hatch for a continuation is
`Create` with `FlowOptions.ThreadKey` set (join an existing thread but specify policy yourself).

**Create** - Creates a flow **and runs it**. Calls `LoadGraph` to fetch the graph, **validates it**
(`graph.Validate()` after a nil check), then inserts the flow row (already `running`) and its entry-point
step (`pending`) in one transaction and rings the doorbell - returning a running flow's key. The graph JSON is
frozen at creation. There is no separate start call; `created` is never an externally-visible resting state.

*Create-time validation is load-bearing, not just doc-hygiene.* A `LoadGraph` returning `(nil, nil)` is a
clean 404 here (it would otherwise nil-deref in `EntryPoint()`); a structurally invalid graph is a 400. In
particular `Validate()` rejects a graph whose fan-out branches don't converge on a single fan-in - the shape
the empty-`forEach` fan-in shortcut depends on - so a malformed fan-in can't reach dispatch, where it would
silently *complete the flow* instead of firing the fan-in (skipping every downstream task: silent data loss).
`Validate()` is **pure**: it computes the fan-out-to-fan-in map into a local to run that check and stores
nothing on the graph, so only the author's definition is frozen into the flow's graph JSON. The routing map
itself is an engine-side optimization derived per flow at dispatch from the frozen definition
(`internal/faninmap.New`, cached beside the parsed graph in `graphCache`), not persisted - it is a pure
`O(V+E)` function of the structure, computed once per flow rather than per step. The **subgraph-spawn** path
validates identically (a nil/invalid child graph fails the caller step like any `LoadGraph` error). One
consequence for graph authors: a graph with no explicit transition to `END` (relying on "no matching
transition completes the flow") is rejected at `Create` - `Validate` requires an explicit `END` edge. `FlowOptions.ThreadKey` (optional) joins the new flow into an existing thread
(any **root** flowKey in that thread; a bad/stale key 404s, and a **subgraph-child key 400s** - see "Subgraph
keys are read-only"). The engine has **no creation-time delay** (no `StartAt`):
every flow runs as soon as it is created. A flow that should wait runs author-side - an entry **gate** task that
calls `flow.Sleep(until)` for a one-shot durable delay, or an interrupt-first entry task + `Resume` (staged
start) - and recurring schedules are an external concern (a host/cron service that calls `Create`/`Run` on a
schedule), never the engine's.

**Snapshot** - Returns a `*workflow.FlowOutcome` for a flow at the current moment. For terminal statuses
(`completed`/`failed`/`cancelled`) it returns the flow's `final_state` (plus `Error`/`CancelReason`); for
`interrupted` it returns the interrupted step's merged `state+changes` and its `interrupt_payload`. When the
flow has several interrupted steps, Snapshot picks the **same one the next `Resume` will resolve** - the
earliest-`updated_at` (`step_id` tiebreak), *not* by `step_depth` - so a Snapshot reports exactly the
interrupt the next Resume acts on. For a `running` flow it returns the status with an **empty `State`** -
dwarf does not reconstruct live in-flight merged state (including the fan-out `step_id=0` case). Exposing a
live fan-out snapshot is a deliberately-deferred decision; confirm against product intent before implementing.

**Resume** - Continues a flow paused by `flow.Interrupt`. Walks up the surgraph chain (`surgraphChain`) and down the
interrupted subgraph chain (`interruptedSubgraphChain`) to the leaf interrupted step. Records resume data on the
leaf's `resume_data` column (the leaf already has `interrupt_done=1`, set when the task armed `flow.Interrupt`); on
re-dispatch the task's `flow.Interrupt` call returns that data with `yield=false`. Resume data is **not** merged into
`state`/`changes` - the task receives it as the return value. Re-parks intermediate surgraph steps, resets the leaf
to `pending`, transitions all flows in the chain to `running`. Propagation goes both directions. Must be addressed
by the **root** flow key (a subgraph-child key is rejected with 400 - see "Subgraph keys are read-only"). If
multiple fan-out siblings interrupt, each `Resume` handles one; the flow returns to `running` only when no
interrupted steps remain.

**Cancel** - Aborts a created, running, or interrupted flow. Walks **down** (`allSubgraphFlows`) the hierarchy,
atomically cancels all steps across all flows, computes `final_state` per flow, and cancels all flows with per-flow
`final_state` via CASE - all in one transaction (`cancelSubtree`). Must be addressed by the **root** flow key (a
subgraph-child key is rejected with 400 - see "Subgraph keys are read-only"). Takes a reason string surfaced as
`FlowOutcome.CancelReason`.

*Down only - there is no up-walk, and there was never a live one.* Cancel used to call `surgraphChain` too, but
being root-only it can have no ancestors: the chain returns no ancestor steps, so the block that cancelled them was
unreachable, and the call itself was a full `root_flow_id` tree scan run to learn the flow's own id and token - both
of which the caller already holds. Every cancellation the engine performs, present or planned, is down-only or
one-level: a root-addressed Cancel has no ancestors; the orphan sweep starts at a child whose ancestor chain is
already terminal; and a child terminalizing settles its *one* caller step by `surgraph_step_id` (a PK), not by
walking to the root. The up-walk would only come alive if Cancel accepted a mid-tree key - which it deliberately
does not. (`surgraphChain` itself stays: `Resume`, `handleInterrupt`, and `Fork` are its real users.)

**`cancelSubtree` is shared with the orphan sweep** (`cancelOrphanedSubtree`), which had been a near-duplicate of
this transaction. The two differ in exactly one behavior, and it is a real one: a zero-row flow UPDATE (a racing
terminalization) is a **409** for an operator Cancel - the caller asked to stop something that had already stopped -
and a **benign no-op** for the sweep, which asked for an outcome that already happened.

**Fork** - The sole recovery/exploration operation, given terminal-flow immutability. `Fork(stepKey,
stateOverrides)` clones a terminal flow's execution tree up to a chosen step into a brand-new,
self-contained **root** flow, then re-runs from that step (optionally with `stateOverrides` merged onto it). The
original is never mutated. It takes no `FlowOptions` - as a derived operation it inherits the origin flow's
scheduling and baggage. The fork point may be **any recorded step** - in the
root flow or deep inside a subgraph (so an execution-DAG UI can fork from any clickable node). Returns the new
flow's key.

*Copy-only-keep, iterative tree walk.* `surgraphChain(forkStepFlow)` yields `rewindByFlow`: each flow on the path
root→fork-flow maps to its rewind step (the leaf fork step in its own flow; the caller step in each ancestor). Per
cloned flow, only the steps that are **not** strict DAG-descendants of that flow's rewind step are copied
(everything, for an off-path completed-prefix subgraph); a per-step `INSERT…SELECT` copies all columns DB-side
(native timestamps) under a fresh `flow_id`/`step_token`, building an old→new id map; predecessor/successor/lineage
references are remapped (a ref to a pruned step → 0); cohort `arrivals`/`failures` are recomputed on cloned spawns
by **counting BRANCHES**, **excluding the whole rewound branch** (so the existing fan-in path converges/
fails the fork with no special escalation). *Branches, not lineage members* - the trap is that `lineage_id` is a
cohort-**counting** device, not a DAG: every step of a per-element sub-pipeline inherits the spawn's lineage, so a
3-branch cohort of 2-step branches has 6 members, and counting members wrote `cohort_arrivals=5` against
`cohort_size=3` (overshooting the pinned invariant, and pre-arriving a fan-in whose rewound branch has not re-run).
The walk therefore starts at each direct child of the spawn that shares its lineage and descends the branch's
sub-DAG; one arrival per surviving branch, one failure if any step in it failed. Excluding the whole *branch*
rather than the rewind *step* is the same fix's other half (a mid-branch rewind leaves its own already-completed
earlier steps kept, and they are not arrivals).

*Membership is the lineage CHAIN, not the lineage id* - and this is the trap one level deeper. A **nested**
fan-out re-lineages its own children (`childLineageID = stepID`), so in
`Seed -forEach-> Cell -forEach-> Chunk -> JoinChunk -> JoinCell` the `Chunk` steps carry `lineage_id = Cell`, not
`Seed` - while `JoinChunk`, inserted with the inner spawn's *own* lineage, carries `Seed` and is reachable only
**through** those `Chunk`s (its `predecessor_id` is the inner cohort's last completer). Filtering the descent on
`lineage_id == spawn` therefore **dead-ends the outer walk at the inner spawn**, making everything past the inner
cohort invisible, with two consequences: a rewind at or past the inner frame was never seen, so the branch counted
as *arrived* and its re-run pushed `cohort_arrivals` past `cohort_size`; and a failure inside a **kept** branch's
inner cohort was never seen, so the clone lost `cohort_failures` and **a fork that had to re-fail COMPLETED
instead** - silently absorbing an unrecovered branch failure, inverting the whole point of the partial-recovery
fork. So a step is in spawn `S`'s cohort - at *any* nesting depth - iff walking its lineage chain upward reaches
`S`. That descends through nested cohorts and still stops cleanly at the outer fan-in (whose lineage is the
spawn's own, and so never reaches `S`). It costs **no extra query at any depth**: Fork already holds every step of
the flow in memory, and a kept step's lineage ancestors are always kept (a lineage ancestor is a DAG ancestor, and
pruning removes only descendants of the rewind step). A materialized `lineage_path` column was considered and
rejected - unbounded length, and a reindexing obligation on every insert, to replace a free in-memory walk.

Pinned by `engine/forkcohort_test.go` (flat multi-step branches; nested rewind; nested failure in a kept branch).
The older fork fixtures all use single-step, non-nested branches - the degenerate case where members == branches
and neither bug is visible.
`cloneTree` drives this per flow (`cloneOneFlow`) over an explicit
LIFO worklist of kept subgraph-caller children, skipping the leaf fork step (which re-spawns a fresh child) -
iterative rather than recursive so nesting depth costs O(1) goroutine stack (see "Size and count limits").

*Interrupted-kept-step guard.* `cloneOneFlow` copies a kept step's status verbatim, so a kept `interrupted`
step would clone into the running fork as an orphan - unresumable (`Resume` needs the flow `interrupted`, but the
fork is `running`) and, as a cohort member, uncountable at fan-in (the cohort recompute scores `interrupted` as
neither arrival nor failure), wedging the fork permanently. This *cannot* arise from a valid origin: an interrupt
forces the whole surgraph chain non-terminal (up to the root), and Fork rejects a non-terminal root - so a
terminal fork origin never holds an interrupted step (a failed branch of a cohort that also has an interrupted
branch rests the flow `interrupted`, not `failed`, because the cohort can never fully arrive). The guard is
therefore defense-in-depth against a broken invariant: cloneOneFlow rejects the fork with 409 if any kept
(non-rewind) step is `interrupted`, turning a would-be silent permanent wedge into a loud, clean, transactional
failure that never mutates the origin. The rewind steps themselves (leaf fork step, re-parked ancestor callers)
are exempt - they are reset/re-parked, so forking *at* an interrupted step is fine.

*Chain rewind.* The leaf fork step is reset (merged overrides, cleared output/park/cohort) and **held `created`
until the full id mapping is in place, then flipped to `pending`** - so a crash mid-clone leaves an inert flow that
never dispatches (the clone is also one transaction, so a crash rolls back). Each **ancestor caller** up the
surgraph chain is **re-parked** (`running`/`parkedSubgraph`, `subgraph_done=0`); when the re-run child re-completes,
`completeSurgraphFlow` revives the caller and execution bubbles back to the root. Chain flows are set `running`;
off-path prefix subgraphs keep their `completed` status.

*Scheduling, identity, threading.* Scheduling (priority/fairness/time-budget) is inherited from the **root's
frozen values**, resolved once and applied **uniformly to every cloned flow and step**, because the original
tree is uniform (subgraph children inherit the parent's scheduling) and a deep-subgraph fork's re-running leaf
lives in a descendant, so it must carry the same values. The new root mints a fresh detached trace (like
`Continue`), sets `forked_from_step` to the *original* fork step's id (provenance + Continue exclusion), copies
the origin's `thread_id` (so it groups in `List`) but does **not** notify or auto-delete. A failed-fan-out
fork-of-fork is the partial-recovery path: fork one failed branch at a time; the first fork re-fails cleanly via
cohort accounting (no limbo) until every failed branch is fixed.

*Caveat:* the cloned-prefix steps keep the **origin's** timestamps (the `INSERT…SELECT` copies `created_at`/
`started_at`/`updated_at` verbatim); only the **rewound** rows (the leaf fork step and re-parked ancestor
callers) are stamped fork-time (`NOW_UTC()`). So a cloned prefix's timings match the original, and only the
re-run boundary reads as new.

**History / Step** - `History` returns the step-by-step execution as `[]workflow.FlowStep`; each includes key, depth,
task name, status, error, timestamps, and DAG links - but **not** `state`/`changes` (its two-round-trip tree scan
deliberately omits the payload columns, so `FlowStep.State`/`.Changes` are nil on a History result). Subgraph-executing
steps have `Subgraph=true` with nested `SubHistory`. `Step` returns one step by key **with** its `state`/`changes`
(refs resolved) - it is the payload-bearing reader; use it when you need a step's actual data.

**List** - Queries flows by status, workflow URL, or thread key, with cursor pagination (newest first **per shard**
- see "`List` uses per-shard pagination" under Database Sharding; single-shard, the default, is globally
newest-first - default 100).
Returns `ThreadKey` and a `Subgraph` bool in each `workflow.FlowSummary`. By default it returns **root flows only**;
`Query.IncludeSubgraphs` adds subgraph children to the results. Combined with `WorkflowURL` (a graph that runs only
as a subgraph has no root flows under that URL) this locates every run of a graph that executed as a subgraph.
`Purge` **rejects** the flag with a 400 (`purge cannot include subgraphs`) rather than silently ignoring it, and
always targets roots only (deleting a subgraph child directly would strand its parent's surgraph step).

**Continue** - `Continue(threadKey, additionalState)` creates and runs a new flow from the latest completed flow
in a thread, merged with `additionalState` using the graph's reducers - sugar over `Create` for the multi-turn
case. The `threadKey` accepts any **root** flowKey in the thread (a subgraph-child key is rejected with 400 - see
"Subgraph keys are read-only"); `Continue` resolves the thread via `thread_id`, finds the
latest **non-fork** flow (`ORDER BY flow_id DESC` with `forked_from_step=0`), validates it is completed, and
creates the new flow in the same thread with the same graph, returned **`running`** (like `Create`). The fork
exclusion keeps a debug `Fork` (which shares the thread's `thread_id` for `List` grouping) from ever becoming a
production continuation base. The prior turn's `final_state` passes through unfiltered as the new flow's initial
state; a workflow author wanting narrower carryover scrubs with an entry adapter task using
`flow.Del`. As a **derived** operation `Continue` takes no `FlowOptions`: it **inherits the
thread's policy** (priority/fairness/budget/baggage) from the latest turn; a caller wanting different policy
uses `Create` with `FlowOptions.ThreadKey` (explicit policy, same thread).

*`additionalState` is canonicalized at the door, because reducers compare MARSHALLED bytes.* A reducer
dedupes (`union`) and overwrites (`merge`) on the marshalled form of its operands, and those bytes are
canonical only for a **decoded** value: Go sorts a map's keys but marshals a **struct's** fields in
*declaration* order, so a struct a caller re-contributes compares byte-unequal to its own decoded twin (the
sorted-key map a prior turn stored). Every other reducer input arrives decoded from the
database (the fan-in merge, `computeFinalState`) - which is what makes their byte comparison sound - but
`Continue`'s `additionalState` is the caller's **raw Go value** and skips the database entirely. Unfixed, a
caller re-contributing an element already in the thread's state *as a struct* produced a second, byte-different
spelling of it and `union` kept **both** (pinned by `fixtures/continuecanonicalflow_test.go`). So
`continueFlow` passes it through `workflow.NewState`, which JSON-round-trips a map or struct (it has **no**
short-circuit for a raw `map[string]any` - that fast path was removed precisely so this canonicalizes), decoding
it exactly like a value read from a column. Done outside the transaction so a lock-contention retry re-runs the
closure and this stays invariant - keeping *reducers only ever see decoded values* an invariant rather than a
coincidence. This is why the fix belongs here and not in `reduceUnion`: it covers **every** reducer, not just the
one whose symptom was noticed.

*Concurrent Continue is serialized by a thread-anchor lock, so exactly one wins.* The latest-turn read
(`find latest non-fork flow`), the completed-check, and the new-turn insert run in **one transaction**
(`continueFlow`), opened **write-first on the thread anchor row** (`UPDATE dwarf_flows SET touch=1-touch WHERE
flow_id=threadID` - the same lock-grab idiom as the flow-advancing transactions). Without the lock, two
concurrent `Continue`s on one thread can both read turn N as the latest completed turn and both insert a
successor, **silently branching a thread that is meant to be linear** - a benign-but-wrong TOCTOU hole between
check and insert (no corruption; the invariant sweep stays clean, but the thread's continuation base becomes
ambiguous for the *next* Continue). The anchor lock closes it deterministically: the winner inserts its new
`running` turn; every other concurrent Continue then reads **that** running turn as the latest, fails the
completed-check, and returns **409**. So the outcome is "exactly one succeeds per race," not timing-dependent.
`touch` is the non-indexed lock-grab column, so the anchor's `updated_at` stays frozen (it is a terminal flow),
and because interrupt/cancel/resume never lock a thread *sibling*'s rows, this anchor lock cannot cycle with
them. Determinism caveat: it also requires the winner's turn to still be `running` when the losers re-check - it
is, because the new turn is inserted `running` and only completes after its entry task runs; a test proving the
"exactly one" contract must keep that entry task from completing during the race (see
`fixtures/concurrentcontinueflow_test.go`). The insert half is shared with `Create`/subgraph spawn via
`insertFlowTx` (one copy of the flow+entry-step INSERTs); `Continue` differs only by wrapping it in the
lock-and-recheck transaction. Edge case: if the thread anchor row (`flow_id == thread_id`) was `Delete`d while
later turns remain, the write-first UPDATE matches no row and the serialization degrades to the old racy
behavior for that thread - still safe, just non-deterministic; continuing via a deleted anchor *key* already
404s, so this only affects continuing via a surviving later-turn key on a thread whose original root was removed.

**Run** - Create + Await in one call, returning `(flowKey string, *workflow.FlowOutcome, error)` -
the new flow's key alongside its outcome (the key is the flow's identity, not part of the outcome; callers
need it for later `History`/`Resume`/`Fork`). Error semantics are phase-split: a **create** failure
returns `flowKey == ""` with a nil outcome (no flow exists); an **await** failure (usually the caller's
ctx expiring first) **leaves the flow running** and returns its **`flowKey`** with a nil outcome and the
error, so the caller retains a handle. `Run` never cancels the flow on the caller's behalf - tearing down
a healthy durable flow just because the caller stopped waiting is an availability footgun; a caller that
wants teardown-on-timeout calls `Cancel` itself. (This is the corrected behavior of the former bug where
`Run` cancelled the just-started flow with the already-expired await ctx - so the cancel silently never
ran *and* the intent to cancel a healthy durable flow was itself wrong.)

**Await** - Blocks until the flow stops (see "Await" below).

**Delete / Purge** - Operator-driven retention (see "Data Retention").

**ShardInfo** - Per-shard health/size summary.

**HistoryMermaid** - Writes the execution DAG as a Mermaid diagram to an `io.StringWriter`.

**There is no inbound peer entry point.** `peers.go` records the two signal shapes that must never come
back - a per-step work doorbell and a per-flow stop broadcast - and the standing rule above covers the
third.

### Subgraph keys are read-only

A subgraph child flow has a real flowKey (a task inside it reads its own via `flow.FlowKey()`, and `List` with
`IncludeSubgraphs` surfaces it), but that key is a **read** handle, not a write unit: a child cannot be mutated
independently because its parent is parked waiting on it, and the unit for any lifecycle change is the whole tree.
So the **lifecycle mutations reject a subgraph-child key with 400** (`surgraph_flow_id != 0`): `Resume`, `Cancel`,
`Delete`, `Continue`, and `Create` with `FlowOptions.ThreadKey` (in `resolveThread`). The rejection is folded into each operation's existing flow-row SELECT (no extra round-trip;
the 404-not-found check still takes precedence). The caller addresses the tree by the **root** key instead - which
it always holds (it came from `Create`/`Run`/`Continue`, or from `List` of roots). The rationale per op: `Resume`/
`Cancel` are inherently tree-wide (they walk up to the root and down), so a child key is just a confusing alias for
the root; `Delete` cascades *down* only, so deleting a child directly would strand the parent's surgraph step; and
`Continue` on a child's own (private) thread would spin up a detached top-level flow from the subgraph's final state,
not a thread turn. **`Create(ThreadKey: childKey)` is the subtlest of the five** - it does not mutate the child at
all, it *joins its thread*, and a child gets its own thread precisely so it cannot contaminate the parent's
continuation chain: the new flow would be a top-level root grouped under a subgraph's thread (polluting `List` by
thread), and a later `Continue` of it would build on the subgraph's turns. **Reject, not silently widen** - widening `Delete(childKey)` into a whole-tree delete is a
surprising blast radius, so the engine makes the caller name the root.

What a subgraph-child key *is* good for: **introspection** - `Snapshot`, `Fingerprint`, `History`, `HistoryMermaid`,
`Step` all operate on that child's own subtree - and **`Fork`**, which intentionally accepts any recorded step,
including one deep inside a subgraph. `fixtures/subflowguardflow_test.go` pins both halves (the four 400 rejections,
introspection + `List`-by-subgraph-URL still working).

### FlowOutcome and side-channel signals

`Snapshot`, `Await`, and `Run` return a `*workflow.FlowOutcome` (`Run` also returns the `flowKey` separately;
`Snapshot`/`Await` callers already hold it). The shape:

```go
type FlowOutcome struct {
    Status           string
    State            map[string]any
    Error            string         // populated when Status == "failed"
    InterruptPayload map[string]any // populated when Status == "interrupted"
    CancelReason     string         // populated when Status == "cancelled"
}
```

The flow key is delivered separately, not on the outcome: the caller passed it to `Snapshot`/`Await`, or `Run`
returns it.

Side-channel fields are populated only for the matching status. `Run`'s Go-level `error` return is reserved for
infrastructure failures (DB, timeout); a *workflow failure* surfaces as `Status == "failed"` with `Error` set, so
callers don't disambiguate "the workflow rejected my input" from "the engine is down."

The interrupt path is split from `State`: `Snapshot` of an `interrupted` flow returns `State` as the merged step
snapshot *at the time of the interrupt* and `InterruptPayload` as the raw `flow.Interrupt(payload)` argument. Folding
the payload into `State` was lossy (the caller could not tell workflow state from the resume request). A caller that
wants the two combined applies the payload onto the state itself, field by field - a resume request is not fan-in
data, so it does not go through the graph's reducers.

### Flow-stop notification is not an engine concern

The engine has **no** stop-notification mechanism: there is no `FlowStopped` callback, no `NotifyOnStop`
option, and no `notify_on_stop` column (all removed). A caller that wants to learn a flow's outcome either
**`Await`s** it (blocking, bridges the workflow clock to a synchronous caller) or **composes** the
notification into the workflow itself - an orchestrating graph whose final task reports the outcome to the
upstream (with `flow.Retry` for durable delivery). This keeps notification policy and transport entirely in
the host/author, matching the engine's "carry facts, not policy" posture (baggage, signals).

### Execution Model

The engine uses a **queue-as-cache execution model** with a configurable worker pool (`SetWorkers`) and **one
refiller goroutine per shard** (decoupled 2026-07-19; the former single merged refiller fanned out through
`OnEach`, a barrier whose max-over-shards wait was measured at 2.02x by 6 shards on the path that is the
engine's throughput ceiling). The in-memory `candidates.Cache` is bounded, **partitioned by shard** (each
refiller wholesale-replaces its own partition; workers pop from the lowest-floor partition - see
`internal/candidates/CLAUDE.md`), and holds *hints*, not ownership. Each worker pops a candidate and calls
`processStep`:

1. Reserve the step (atomic CAS `UPDATE ... WHERE step_id=? AND status='pending' AND parked=parkedNone AND
   not_before<=NOW AND lease_expires<=NOW`).
2. Check for terminal flow status (abort if cancelled/failed/completed).
3. Load the flow's graph, config, and baggage.
4. Execute the task via the host's `ExecuteTask` with a time budget on the call context.
5. Persist changes, evaluate transitions, create next steps (in a transaction), ring the doorbell.

Acquisition is the atomic CAS, so a stale or duplicated candidate is harmless: the CAS loser gets zero rows and pops
the next. The cache holds hints, never ownership; only the CAS grants a step. **The CAS predicate includes
`parked=parkedNone`**: a step that was offered to the cache and then parked (waiting on a subgraph) is rejected at
claim time rather than dispatched, so a parked step in a stale cache entry never runs.

**Selection (two-level priority + fairness), in three phases, one PISTON per shard.** The pistons, not
the workers, decide *what* runs. Each shard's piston cycles against its own database on its own clock,
with **no barrier anywhere** between shards (a merged pass returned when the slowest shard did - an
order-statistics tax measured at 2.02x by 6 shards, on the path that is the engine's throughput ceiling).
One cycle is `sleep -> tallying -> planning -> fetching -> pushing` (`internal/pipeline`). (1) **Aggregate
scan** (`piston.ScanBand`): the shard returns *one row per fairness key* at its strict-minimum `priority`
band - the key's due **count CAPPED at cache capacity** (`MAX(rn)` under a `rn <= capacity` cut, not
`COUNT(*) OVER`; see "The phase-1 scan count is CAPPED, not exact" below) plus the age and
`fairness_weight` of its *oldest* due step (`ROW_NUMBER()=1`). The band is a
`priority=(SELECT MIN(priority) ... due)` subquery, so band and aggregates are self-consistent within the
statement. (2) **Global merge + weighted pick** (`internal/planner`): the shard's tally is recorded in the
planner - a shared map holding each shard's LAST report, which is what replaced the barrier - and the plan
is computed over all of them: global minimum band (strict priority is cluster-wide; worse-band shards
contribute nothing), counts summed per key, the globally-oldest step's weight winning, then repeatedly
weighted-random pick a key (Efraimidis-Spirakis over the *keys*, not the rows) until the plan reaches
`capacity`. **The plan is global; only the fetch is per-shard**, so fairness semantics are exactly the
barrier's - never re-derive this as per-shard lotteries, presence-scaled (`1/d`) weights, or capacity
allocated by shard counts (count-proportional capacity makes backlog drive share, repealing the "count
does not drive share" property that makes the design fair). Each piston rolls its own plan from its own
snapshot - independent rolls change nothing in expectation and need no coordination; the staleness is that
a piston sees the *other* shards up to one of their cycles old, which fairness (slowly-varying) tolerates
and dispatch (own-shard scan, always fresh) never sees. (3) **Sliced fetch** (`planner`'s slice rule +
`piston.FetchSteps`): the plan's per-key demand is split across the shards holding the key - **first slot
to the shard with the key's oldest step** (preserves globally-oldest-first dispatch and prevents a purely
proportional split from rounding a quiet shard's lone old step to zero forever), remainder proportional to
per-shard counts, largest-remainder rounding, deterministic - and the piston fetches only its own slice
(at most its slice's max per-key demand, oldest first per key), replaying the plan's interleave for its
occurrences. Intra-key ordering across shards is approximate below the head.

**A cycle is UNCONDITIONAL, and that is what removed an entire class of liveness machinery.** There is no
trigger, no nudge, and no wake channel: a piston scans on its period whether or not anything happened.
Four mechanisms existed only to decide *when* to scan, and all four are gone with the trigger - the
per-shard single-slot `refillTriggers`, the `refillAboveBand` census watch (`awaitBandRelease`), the
`refillStarved` self-re-arm, and the `refillIdleInterval` cap on parking. Each existed because a parked
refiller could only be woken by shard-LOCAL activity, so a shard holding work nothing local had announced
would sleep; a cycle that always runs cannot be in that state. **Do not reintroduce a trigger to shave
latency** - what it would shave is bounded by one interval, and the doorbell already covers the case that
matters (see below).

**Strict priority across shards: no spill, no back-off needed.** A shard whose own minimum band sits above
the global minimum plans an **empty slice** and fetches nothing; its partition is cleared, because hints at
a band it no longer holds due work at are dead. Under the old trigger design this needed its own outcome
and a census watch to avoid spinning; now it is simply a cycle that fetched nothing, and the next one
re-evaluates on its own. The same is true of a shard AT the global band that wins zero slots because a
capacity-bound plan gave them all to shards holding more (or older) of the planned keys. `planner.Plan`
deliberately does not say WHICH of those happened - every case takes the same action, hold no candidates.

**A shard that could not LOOK must clear itself, and this is the one thing a cycle must never skip.**
A failed scan leaves the planner holding that shard's last tally, which may claim the best band; every
peer then computes the same global minimum, finds none of its own keys there, and dispatches nothing -
forever, waiting on a shard that will never report again. So the pipeline calls `planner.Clear` on a scan
error. Participation is **declared, never inferred**: there is no TTL and no timeout, because the reports
come from this replica's own per-shard workers over a fixed shard set - the caller *knows* its scan
failed, and a clock cannot tell a dead shard from a slow one anyway. (The old census carried a
`censusTTL()` scaled to the slowest observed pass precisely to guess at that; declaring it retires the
guess.) A merely SLOW shard says nothing, keeps its tally, and stays counted - correct, since it is alive
and its last report is still the best information anyone has.

`created_at` (read as an age, comparable across shards) does two things per
key: fixes the key's `fairness_weight` from the key's oldest step (so a tenant cannot self-promote with newer
high-weight tasks) and orders dispatch oldest-first within the key. It is the only ordering signal comparable across
shards: `step_id` is a per-shard auto-increment, so a `(shard, step_id)` order would let a brand-new task on a low
shard jump an old task on a high shard for the *same* tenant (unbounded intra-tenant starvation). The age is
`DATE_DIFF_MILLIS(NOW_UTC(), created_at)` per shard, and `created_at` defaults to that shard's `NOW_UTC()` at insert
- both terms on one shard clock, so per-shard clock offset *cancels exactly*; no inter-shard clock-skew term in
`ageMs`. The only residual is the dispersion in *when* each shard runs its age query (the per-shard refillers scan
on independent cadences, so one shard's tally ages can lag another's by up to one cycle), a soft, self-correcting
reordering of one tenant's own queue - not a fairness violation (the weighted *key* pick governs cross-tenant
fairness) and not a correctness issue (the CAS arbitrates).
Same-age ties break by `(shard, step_id)` for determinism. The pick is re-rolled per step so expected dispatch share
is proportional to weight and independent of backlog depth or shard layout. Strict priority means no aging: a fed
higher-priority band starves lower bands by design.

**The wire/heap cost scales with fairness-key CARDINALITY, not with the backlog, and this is load-bearing.** The
three-phase split exists to keep that promise. Its history is three stages:

- **Unbounded scan** (the original): the band query returned every due row of every key, each allocated as a
  a Go struct, only to be discarded down to `capacity` by the pick. Cost grew with the **backlog**, so under a
  deep one - the case the refiller exists for - it re-read hundreds of thousands of rows on every pass.
- **Per-key cut** (the intermediate fix): a `ROW_NUMBER() OVER (PARTITION BY fairness_key ...)` cut at `capacity`
  bounded it to `capacity` rows *per key*. That killed the backlog-depth dependence but not the **cardinality** one:
  with thousands of tenants at the band it still returned up to `capacity * keys` rows (e.g. 768 x thousands) to pick
  only `capacity`. A plain global `LIMIT n` was never an option - it would let one tenant's old backlog fill the
  window and starve every other key (a fairness bug traded for the waste).
- **Three-phase** (current): phase 1 collapses each key to *one* aggregate row (count + oldest age/weight) server-
  side, so the scan returns O(distinct keys) rows; phase 2 picks the per-key demand from those aggregates; phase 3
  fetches only the selected steps. Total rows crossing the wire are bounded by `capacity^2` (at most `capacity`
  distinct keys chosen, each fetched at the uniform per-key cap `<= capacity`) - **independent of key cardinality**.
  At high cardinality the per-key cap is ~1, so the fetch is ~`capacity`.

The uniform per-key fetch cap (phase 3's `maxNeeded`, not each key's exact demand) is a deliberate simplicity choice:
an exact per-key cap would need a per-key `VALUES`/`LATERAL` join, non-trivial across the four SQL dialects, to shave
an over-fetch that only appears under extreme weight skew among many keys - the low-cardinality regime that never had
a scaling problem. The uniform cap stays complete for every key (a key's globally-oldest N steps sit at most N on any
one shard, so the per-shard `rn<=maxNeeded` cut captures them all; the cross-shard merge then sorts by age). The
resulting batch is *identical* to what the earlier fully-materialized pick produced. Pinned by
`TestRefillScan_BoundedPerFairnessKey` (one aggregate row per key, the per-key fetch cut, oldest-first).

**The phase-1 scan count is CAPPED, not exact - the three-phase split fixed WIRE cost, this fixes SERVER
SCAN cost.** The three-phase split bounds the *rows crossing the wire* to `capacity^2`, but phase 1's
server-side scan was still **O(backlog)**: `COUNT(*) OVER (PARTITION BY fairness_key)` must read every due
row of a key to count it. That is invisible on a fragmented backlog (many tiny keys) but catastrophic on a
**single-key flood** - a `forEach` fan-out's N branches all inherit the flow's `fairness_key`, so a 3M-way
fan-out is *one* key with 3M due steps, counted in full **every refiller pass**. Measured (`engine/refillfetchscaling_test.go`):
~15-23s per pass at 3M on Postgres (→ the ~99s seen on a loaded rig), which stalls dispatch fleet-wide.

The fix: the tally `Count` is now `min(count, capacity)` - computed as `MAX(rn)` under a `rn <= capacity`
cut (`piston.ScanBand`), never `COUNT(*) OVER`. **The cap is LOSSLESS, and this is the whole point:**
the planner builds a plan of at most `capacity` slots, so a single key can be picked at most `capacity`
times, and the per-key fetch cap is `<= capacity`
too. So a count above `capacity` is indistinguishable from `capacity` for every downstream consumer. **This is
NOT the forbidden "approximate count"** (below): an approximation is wrong in *either* direction and distorts
fairness; a cap is *exact* where it matters (`count < capacity`) and *saturated-correct* above it. The oldest
step's age/weight still come from the `rn=1` row (`MAX(CASE WHEN rn=1 ...)`); `MAX(priority)` returns the band
(all inner rows are at the min band).

**The cross-dialect behavior is load-bearing - `rn <= cap` early-stops ONLY on Postgres 15+.** The `rn <= N`
cut becomes a `WindowAgg` **run condition** (an executor early-stop) *only* on PostgreSQL 15+ (measured:
`Run Condition: (row_number() OVER w1 <= '4608')`). MySQL 8, SQL Server, and SQLite have **no** equivalent -
they compute the window over all rows and filter after. So off-Postgres the scan is still O(backlog); the win
there is *only* from dropping the second window-aggregation pass (`COUNT(*) OVER`). It is still a win
everywhere (measured: PG flood 15.3s→2.2s @3M; PG fragmented 1.47s→0.76s; SQLite flood 4.6s→3.2s), just for
different reasons. **Even on Postgres it is not sub-linear:** `PARTITION BY` resets `row_number`, so PG cannot
terminate the scan of a *single* flooded partition (it must keep scanning in case another partition begins) -
the run condition skips per-partition *output* past cap, not the scan. Truly sub-linear phase 1 needs distinct-
key enumeration (skip-scan: PG18+, or a portable recursive-CTE loose index scan) - deliberately **deferred**
(not portable, and O(backlog)-with-a-small-constant is acceptable). The **correctness** of the cap depends on
none of this; only the flood's *speed* does.

*Aurora / managed variants:* Aurora PostgreSQL runs the **stock PG planner/executor** (Aurora replaces only
the storage layer), so the run condition is present **iff the Aurora PG major version is >= 15** - verify with
`EXPLAIN` (look for `Run Condition`). Aurora **Limitless** (distributed) is unverified and a *poor fit* anyway
- dwarf already shards at the application layer (`ShardSet`), so a dwarf shard on Limitless is double-sharded,
and whether the run condition survives its distributed planner is unknown. **Do not assume; `EXPLAIN` on the
actual target.** Where the run condition is absent, a single-key flood is O(backlog) per pass (the constant is
one scan, not the old two), which is why the cap is still the right change even there.

**Why the sibling phase-3 window (`piston.FetchSteps`) was deliberately LEFT ALONE.** Phase 3's
`ROW_NUMBER() OVER (PARTITION BY key) WHERE rn<=perKey` has the *same* run-condition dependency. On Postgres 15+
it already early-stops (measured 2ms on a 3M flood), so a per-key `ORDER BY ... LIMIT` rewrite is **no faster**
- and it would replace one query with a **UNION-ALL branch per chosen key**, which hits SQLite's
`SQLITE_MAX_COMPOUND_SELECT` (500-term) wall and was measured at **121s** on a 500k-key fragmented backlog
(vs the window's 276ms). On MySQL/SQL Server the phase-3 window *is* O(backlog) for a flood and a per-key
`LIMIT`/`TOP` (which the executor honors as a hard early-stop on every engine, unlike `rn<=N`) **would** help -
a gated (few chosen keys) + chunked (`<=500` UNION branches / `<=900` IN-keys) hybrid was designed for that,
then **deferred** as unneeded for the recommended Postgres deployment. **Do not revive the phase-3 rewrite
without a MySQL/SQL Server flood workload showing it binds** (and if you do, it must be gated + chunked - an
unconditional UNION is a 121s footgun on the fragmented regime and on SQLite tests).

Reproduce/measure any of the above with `engine/refillfetchscaling_test.go` (opt-in: `DWARF_BENCH_ROWS`;
targets a real DB via `SEQUEL_TESTING_DSN`; `DWARF_BENCH_EXPLAIN=1` prints the plans).

**The work doorbell is PURELY LOCAL - it reaches this replica's candidate cache and nothing else.** It used to
also broadcast to peers (op `enqueue`, one message per step per peer, plus a `{0,0}`-sentinel one on every
flow completion). That broadcast was **removed**, and the reasoning is worth keeping because the idea
re-suggests itself:

- **It bought no latency where it cost the most.** Under load every peer's refiller is already scanning at
  its derived cycle interval (~67ms), so the doorbell only ever beat a scan that was about to happen anyway.
- **It cost a round-trip on every receiver.** The inbound path had to resolve the announced step's `priority`
  and `not_before` with a PK lookup - the exact round-trip `enqueueStepDue` exists to avoid locally - R-1
  times per step.
- **It head-inserted UNPARTITIONED**, so a peer could offer a step outside its residue class and race the
  owner to the claim CAS (measurable as `dwarf_steps_claim_lost` scaling with R).

What replaced it is that every piston cycles **unconditionally** on its own period: a peer discovers work
by scanning, bounded, with no message at all. The consequence to hold in mind when
reading the origination sites: a step created here is offered to **this replica's cache only**, so if this
replica cannot serve it (its partition is non-empty, and the step falls in a peer's residue class) the step
waits for that peer's next scan. If a per-step peer signal is ever revived it must be coalesced and
payload-free ("shard S has work", rate-limited), never per-step - and it would first have to beat the
cycle interval it is trying to shorten, which is the bar every removed signal failed.

Pinned by `TestSignals_VolumeDoesNotScaleWithSteps`, which asserts on the emitted **op names** rather than a
count: a per-step broadcast reintroduced under any new op name fails it.

**Queue-as-cache, per-shard single-slot triggers.** The doorbell carries no step to a queue; it is an
`Offer` into `candidates.Cache`. The generic path (`enqueueStep`) resolves the step's priority *and*
`not_before` in one PK lookup (off the selection path) - but the **hot-path origination sites skip the lookup**
(`enqueueStepDue`): the completing worker/creator just bound the step's priority into its INSERT/reset and its own
sleep branch already diverged, so it offers the step directly with the values in hand - one round-trip per completed
step that re-read a row this replica just wrote. The lookup path remains for the cold origination sites
(surgraph revive, resume, `Fork`'s leaf, `Continue`, the wedge sweep) where the priority is not in hand. If
`not_before` is in the future the doorbell short-circuits and does NOTHING - the work is not due, nothing to
preempt, the cache stays untouched, and nothing is scheduled: the step's own `not_before` keeps it invisible
to selection until it comes due, and visible to the next cycle after that.
Otherwise the step is offered to its own shard's PARTITION (an `Offer` routes by `Job.Shard`), which admits
it into a **vacated slot**: an empty partition always, otherwise while the cache is under its bound, and at
the TAIL so nothing the plan chose is reordered. The one priority test is against a **worse** band, which
is declined - running it while better-band work sits cached is a strict inversion; a *better* band is
harmless and simply appends, waiting its turn. The partition's floor stays FROZEN at the band it was
planned to serve rather than tracking the head, so neither `Pop`'s partition choice nor `Offer`'s own
admission bar oscillates as a better-banded arrival passes through.
Admissions are counted as `dwarf_steps_offered` and subtracted from the refiller's discard signal, so waste
stays attributed to whoever caused it.

**An EMPTY partition ADMITS the arrival, and reversing that is what makes a fixed-cadence refiller viable.**
It used to decline (request a scan, cache nothing), reasoning that an arbitrary-priority step must not jump
an idle replica's queue - an inversion that was genuinely observed. That held only while the single-slot
trigger let the refiller answer the decline within a fraction of a cycle. It does not survive the trigger's
removal: a sequential chain holds exactly one pending step at a time, so its partition is empty at *every*
hop, and declining costs each hop a uniformly-random fraction of the cycle interval - half on average, all
of it at worst (~330ms over a 10-step flow at the derived ~67ms).

*This is not a fairness exception, and the framing matters:* the plan grants a fairness key a share of the
batch **for the cycle**, not a single dispatch, so a successor taking the slot its predecessor just vacated
is that key spending what it already won. It cannot amplify a share either - the successor exists only
because its predecessor freed the worker that will run it. Fairness governs **admission**, which flows get
started.

*Measured, and the obvious prediction is WRONG:* one expects this to be a test-only trick, since an idle
engine has empty partitions at every hop and a loaded one should not. The reverse holds - the two most
loaded fixtures gain most from it (`completionraceflow` 2.05x, `soakflow` 1.84x, whole suite 1.41x),
because a cycle supplies only 1.04-1.47x ahead of consumption, so under load the cache is shallow and
partitions drain to empty constantly. See `internal/candidates/CLAUDE.md` for the table.

**The priority-preempting head-insert that used to sit beside it is GONE.** It let a strictly-better band
jump the queue so the first urgent step did not wait a cycle, at the price of a bounded fairness bypass. It
was removed after measuring `fixtures/crossshardpriorityflow_test.go` - the fixture built for exactly this -
with and without: burst latency 134-146ms vs 134-152ms, identical ordering. It only ever reordered one
replica's cache anyway, since the planner learns of the new band from that shard's next tally either way,
so the fleet-level change costs a cycle regardless. See `internal/candidates/CLAUDE.md` for the full
accounting, and `docs/scheduling-and-reliability.md`, which has always promised the weaker (and now
accurate) contract: priority is never preemptive, and a new band is served within a snapshot cycle or two.

**The cycle period is the supply control, DERIVED per shard (`deriveRefillInterval` /
`recomputeRefillIntervals`, ~67ms at the reference config) - NOT a fixed constant.** The pipeline paces each
cycle from the *start* of the previous scan, so a slow cycle pays for itself rather than stacking. It exists
because an unpaced piston runs at a **100% duty cycle** -
measured, in *both* the merged and decoupled builds: every refiller scanning back to back for a whole 60s
window. The merged pass was accidentally self-limiting (its straggler wait made it slow); deleting the
barrier made each pass fast and the loop hot, raising phase-1 scan load **3.4x**. **Phase 1 costs per DUE
ROW regardless of how many rows the pass then fetches**, which is why sizing the batch can never substitute
for scanning less often.

*The number is DERIVED at Startup and re-derived in `recomputePools`*, per shard, from static configuration
only - capacity, that shard's declared vCPUs, and the observed replica count. It reads **no observed rate**,
and that is the whole distinction: an interval set from measured *consumption* oscillates (consumption is
`min(demand, supply)`, so the actuation contaminates its own measurement) and was measured doing exactly
that. Static arithmetic cannot. It lives on the same "every path that changes a pool must re-derive what
depends on it" rule as the dispatch count and the cache, because it is measured against the cache's
capacity. The arithmetic: workers drain a partition at `drain = sustainedDrainPerVCPU·vCPUs/R` candidates/s
and a pass hands it at most `capacity/N = 96·vCPUs/R`, so `T = bufferShare/(headroom·drain)`. Substituting
the engine's own constants (`6·vCPUs/R` conns, `8x` dispatch workers, `2x` cache; `sustainedDrainPerVCPU` =
**720**, the MEASURED sustained drain of ~120 steps/s/conn × the 6 conns/vCPU, not `capacityWeight`'s 450
placement peak) gives `96/(2·720)` ≈ **67ms with vCPUs and R cancelling** - capacity and drain both scale in
`vCPUs/R`, so their ratio is configuration-independent. That is *why* one constant works. `headroom` =
**2.0**, chosen for MAXIMUM THROUGHPUT (see the measured paragraph): a ~2x supply buffer absorbs the
drain-rate jitter that briefly stalls workers at a tighter margin. Two caveats before treating the value as
universal: `workersDispatch`'s `max(64, …)` floor breaks the cancellation on small deployments, and 720
steps/s/vCPU is workload-shaped (a byte-heavy or fan-out shape drains slower, which makes the optimum
interval **longer**).

**Do NOT re-derive `headroom` from "waste" (discarded/selected).** Waste looked like the tuning lever on an
IOPS-throttled disk, where slow dispatch piled up a deep pending backlog and the refiller over-supplied it
(waste 25-50%, apparently trading against throughput). That was a **disk artifact**: on a healthy disk steps
dispatch fast, the pending backlog stays shallow, the refiller supplies close to consumption, and waste is
~2% across the whole good interval range - flat, so it does not distinguish the optimum. Throughput does.
Confirmed after the IOPS diagnostic (below) made throughput resolvable.

**THE FLOOR DEPENDS ON `workersPerConnBudget` (8) THROUGH THE CACHE - which is why it is derived rather
than hardcoded.** `capacity = 2 x workersDispatch = 2 x 8 x conns`, so anything changing the 8 rescales the
buffer this floor is measured against, proportionally. That nearly happened: the 8 assumes `T/db≈8` while
5ms tasks measure ~1.5-2, so the resident worker count overshoots ~4x. The overshoot was
deliberately KEPT (it is throughput-neutral - surplus workers queue for a connection and connections are
never idle), but had it been "corrected" with a hardcoded floor, capacity would have fallen ~4x, the
per-partition share 768 -> 192 and the supply ceiling proportionally, leaving the floor **longer than the
buffer can cover** - the measured starvation mode (-10% throughput). Deriving it closes that by
construction. **A change to worker or cache sizing now rescales the floor automatically; do not reintroduce
a constant here.**

**Why scanning less often helps more than the scan count alone suggests:** the band scan's apparent "fixed
~46ms" is largely **connection-pool wait**, so refiller scans queue against worker traffic on
the same pool. Scanning 3.4x more often therefore also made each scan slower, and the effect compounds.
Measured across the floor sweep: phase 1 fell from ~36ms at the unlimited and 150ms arms to **15.6ms at the
300ms arm**. It is also what makes per-shard pass durations diverge (28ms vs 125ms) on hardware whose RTT
differs by 0.036ms - that spread is pool wait, not a slow shard, so do not read it as one.

*Measured.* On 500GB/6-shard (linear, R=1): unlimited scanning cost 8% candidate churn and a **77% worse
p99** while buying no throughput; a fixed 150ms gave **+19% throughput and a tail at or better than the
barriered baseline**. But throughput there was **bimodal** (40-86% run-to-run spread) - the disk, not the
engine (see the IOPS finding below), so the exact optimum was unresolvable. On a **1TB / 16-vCPU single
shard** (IOPS non-binding, spread collapsed to ~4%): throughput peaked at **110-150ms** and fell off ~15% by
~260ms. A later M-sweep measured the drain per CONNECTION directly - sustained ~120 steps/s/conn, roughly
flat across connection counts, instance sizes, and backlog volumes - and recalibrated `sustainedDrainPerVCPU`
340→720, moving the reference floor 141ms→**67ms**, inside a flat-good 10-80ms band at high connection
counts. (The two peaks were taken on different rigs/loads; the connection-rate measurement is what the
constant now encodes, since it held across the widest range - and a validation sweep at 67ms beat the old
141ms by ~50% at M=8 and M=64.) headroom stays **2.0**: the decline past the peak is drain-rate jitter
stalling workers at a tight buffer, which a ~2x buffer absorbs.

**Three adaptive alternatives were built and all lost.** This is the only record of them - `scheduling.go`
carries the formula and its local traps, not the campaign. Do not re-propose any without new evidence: an
adaptive fetch DEPTH was *inert* (batch
moved 179→173→190 across a 60% margin change, because the batch is set by the backlog and the plan slice,
never by a target); a DERIVED interval (set from observed consumption) was *actively harmful* (~1,000x
the discard, 2.4x the p99) because consumption is `min(demand, supply)`, so the actuation contaminates its
own measurement; and an AIMD loop that crawled each shard's floor toward its own optimum won at low
connection counts but was *unprovable* - its over-supply signal (`discarded`) measures only worker
starvation, blind to refiller scan cost, so it could not find the high-connection optimum and over-crawled
to the clamp. It was rig-validated, then SHELVED for the recalibrated static floor it could not be proven to
beat (only bounded) - the same `control-loops-must-be-simple` bar the removed rate valve failed. The
FIXED-headroom static formula beats all three.

**The throughput bimodality was IOPS contention, not engine noise.** Cloud SQL provisions IOPS by disk size
(~30 IOPS/GB); the 500GB/8-vCPU shards throttled under the write-heavy load, producing the 40-86% run-to-run
swing that made every 500GB throughput number suspect (this is why the campaign leaned on *waste*, a
DB-independent phase metric, over throughput). A 16-vCPU/1TB shard (30k IOPS) collapsed the spread to ~4%,
confirming it. Operator guidance (size disk throughput to the workload's `dwarf_state_write_bytes` rate) is
in `docs/deployment.md`.

**CROSS-SHARD PRIORITY IS STRICT ONLY WITHIN ONE OR TWO CYCLE INTERVALS**, and this must be stated rather
than assumed. There used to be a floor-cutting nudge channel (`requestRefillDemand` / `routeRefill`, fed by
an `urgent` flag out of `Offer`) whose whole job was to close the publish gap below; it is gone, and so is
the ordinary trigger that replaced it.

The gap: a shard learns of an arriving better band from its doorbell, but peers learn of it only from that
shard's next **tally**, which it publishes by *scanning*. So the band becomes globally visible one interval
later (this shard cycles and tallies) plus up to one more (a peer plans on its own next cycle). Inside that
window a peer computes a stale global minimum, finds itself holding it, and legitimately dispatches
worse-band work. That is an inversion of the observable dispatch ORDER, not merely of latency - do not
repeat the old claim that "the rate limit can only delay dispatch, never invert order." It was true of a
cycle planning from a *current* picture and false across the publish gap, which is exactly the case that
matters.

Pinned by `fixtures/shardedflow_test.go` and, on the latency axis,
`fixtures/crossshardpriorityflow_test.go`. **Both are interval-relative, so both are sensitive to the period
being derived sanely** - which is how the `bufferShare` zero-division bug was found. A cache smaller than
the shard count integer-divided to zero and took the derivation's degenerate branch, answering with the 1s
CAP: the slowest period there is, for the case that wants the fastest. Every `SetWorkers(1)` multi-shard
fixture silently ran at second-long scan intervals, which is why the fix took ~13s off the whole fixtures
suite. `recomputeRefillIntervals` clamps `share` at 1. If either fixture starts failing on timing again,
suspect the derived period before suspecting the test.

**THE PUBLISH GAP IS NOT THE ONLY WAY ORDER INVERTS. The second way needs no arriving band at all, and it
is the local cache: `Pop` ranks partitions by a FROZEN band that the DOORBELL can set.** `Offer` admits into
an EMPTY partition unconditionally and stamps `p.band` from the arrival itself; `Pop` then picks the lowest
`floor()` - which *is* `p.band` - and never consults `items[0].Priority` nor the planner's current global
minimum. So a doorbell-admitted hint is indistinguishable, at pop time, from one the plan chose. Each
partition is reconciled only by its OWN shard's next pushing cycle (an empty plan clears it), and the pistons
turn independently - so with several shards, the window between "N steps were offered locally" and "every
shard has reconciled" is one in which dispatch order is decided by *which pistons have got round to it*,
not by priority.

**Consequence: cross-shard dispatch order is only as strict as the SLOWEST piston's reconciliation, and the
failure is ASYMMETRIC - a uniformly slow fleet is fine.** Every piston cycling slowly still reconciles before
the work drains; one piston starved or blocked on a round trip while its peers turn is what strands a stale
hint into the drain. This is why a loaded server reproduces it and a uniformly slowed local run never does.

Measured, by A/B on `fixtures/shardedflow_test.go` (8 shards, one worker, nine flows at distinct bands, all
created before any can dispatch): with the pistons pinned to a 1s interval and no wait for reconciliation,
**5 of 5 runs mis-ordered** - always adjacent swaps (`p6 p9 p7 p8`, `p7 p6`, `p6 p5`), the same signature seen
once on a loaded SQL Server suite. Waiting for two pushing cycles per shard before releasing the work, with
the pacing otherwise identical, **8 of 8 passed**. That wait is what `piston.CheckpointCycleDone` exists for
(`CheckpointRefillCycleDone` in the engine catalogue): a pushing cycle is exactly the moment a shard's
partition reflects the plan, and no elapsed time substitutes for it.

None of this is a correctness bug - the cache holds hints, the claim CAS grants the step - and none of it is
reachable by a single-shard deployment. It is an ordering caveat, and the honest statement of the contract is
that priority governs *admission* across shards within a cycle or two, not the exact interleaving of a batch
already sitting in one replica's cache.

What is NOT weakened: ordering among work already tallied **and reconciled** is strict, and nothing is
preemptive - which is now true of the local cache too, since `Offer` no longer head-inserts a better band. The
public statement of all this is in `docs/scheduling-and-reliability.md` - keep the two in step.

`fixtures/crossshardpriorityflow_test.go` still asserts urgent-burst LATENCY as its sensitive axis (an
ordering-only test passes a starved build silently - verified: a 5s period produced correct ordering and
took 22x as long).

**The period is the only standing rate limit, and its fuse is the pipeline's `MinGap` (20ms) - NOT a
minimum on the period itself.** The derived formula can bottom out: `share` is `capacity/N`, so a small
cache over many shards produces a sub-millisecond period - `SetWorkers(1)` gives capacity 2, `share` 2, and
~2ms. At those values the period limits nothing and an unpaced piston restores the 100%-duty-cycle scan
loop, whose cost is measured and severe: at high backlog ~half of all pops and their claim round-trips were
stale, with the entire due backlog streamed every few ms.

The fuse used to be `refillScanFloorMin`, a 20ms clamp inside the derivation. It moved to `MinGap`, which
bounds the quiet time between the END of one cycle and the START of the next, and that is the STRONGER
form: a start-to-start minimum cannot bound a cycle that outruns it, which is exactly the deep-backlog case
the fuse exists for. Inert in the derived path (~67ms), so a healthy configuration pays nothing, and
`SetRefillInterval` lowers the gap to match a pinned interval when that is tighter, so a bench sweep can
still measure the unlimited arm; it never raises it, so a 500ms pin keeps the ordinary 20ms gap.

Liveness is unaffected: a cycle always runs, so a drained-early partition waits at most the remainder of the
period. Pinned by `TestRefillInterval_DeepBacklogLiveness` (a deep backlog still drains under a period
pinned into the over-limiting regime, with a single worker).

**Liveness guarantee.** It is now structural rather than protocol: every piston cycles unconditionally, so
the scan after a completion always sees the freed slot without anything having to ask for it. This retires a
rule that was genuinely subtle - a worker had to request its refill *after* `processStep` returned, never at
pop time, because requesting before the CAS let the refiller re-select the in-flight step and, under
single-slot coalescing, never scan post-completion state, wedging a single-worker replica with a backlog.
The other half of liveness is the doorbell: `Offer` admits into an EMPTY partition, so a sequential chain's
next hop dispatches immediately rather than waiting out a cycle. The cache holds 2x the worker count.

**There is no timer goroutine and no dispatch-facing poll.** Background repair is one function -
`recoverExpiredLeases` - running on `recoveryLoop` beside the other repairs (see "Background Recovery"),
which is **THE** cadence for every background repair the engine performs.

**Do not reintroduce any of the three mechanisms that used to sit here.** Each is individually plausible
and each is strictly dominated:

- **A due-backlog existence probe** (to cap the sweep when an idle replica missed a doorbell). A piston
  cycles unconditionally, so it covers that per shard, far sooner, with one band scan.
- **An early-wake channel** (`shortenNextPoll` / `nudgeTimer` / `wakeTimer`). Every site that armed a wake
  did it to make the poll ring the doorbell at the right instant, and nothing rings a doorbell from here:
  every piston's scan predicate is already `not_before<=NOW_UTC()`, so a sleeping or backed-off step becomes
  visible to the next cycle on its own. Verified by no-oping `shortenNextPoll` and running the full suite
  green, sleep and retry fixtures included. It also carried a past-deadline-replace predicate defending
  against a ~1-in-40 `sleepretrycomposeflow` wedge, which cannot arise with no deadline to replace.
- **A self-sizing deadline** (`MIN(lease_expires)`, scheduling the next sweep at the soonest future expiry).
  This is the one that reads as load-bearing and is not. Its period works out to
  `lease - age of the oldest in-flight step`, so with ordinary millisecond steps it sits at ~2.5 minutes
  (the default `budget + leaseMargin`) and 5 at idle - not a hot loop, and all it buys is recovery landing
  *at* the instant a lease lapses instead of within a sweep of it, for a per-shard query every sweep plus a
  deadline field, a mutex, an error clamp and a fault seam.

**Lease recovery costs latency, and the number is the constraint on shortening anything else.** Worst-case
recovery is `budget + leaseMargin + wedgeSweepInterval` - at the defaults 2m + 30s + 5m. That is acceptable
because the path runs only when a worker has already died. It stops being acceptable at a SHORT configured
budget: a 5s budget makes the lease 35s, which a five-minute sweep dominates. The fix there is to **derive
`recoveryLoop`'s ticker from the configured budget**, never a per-sweep query asking the database for it.

**Lease recovery is the ONLY thing that recovers a crashed worker**, because a piston never touches a
`running` row - and it is *not* the backstop for anything else. A missed doorbell leaves a `pending` row,
which the next cycle selects like any other; a step that is `pending` with a future `lease_expires` is
invisible to both (see the lease-extension guard in `persist.go`).

**The pistons need no error clamp either.** A scan or fetch can fail on the same transient DB error that the
poll's clamp used to cover, and swallowing it would be the mirror wedge: the shard's partition refills
**empty** and its workers block in `Pop`. Under the trigger design that needed an explicit re-poll; now the
next cycle is at most one interval away unconditionally, so the retry *is* the cadence. What a cycle does do
on a scan error is `planner.Clear` (this shard stops claiming a band it cannot serve) while leaving the
cache partition intact - an error means "unknown", not "nothing is due". A fetch error clears neither: the
tally already succeeded and is still true.

*The trade from removing the early wake, restated:* `flow.Sleep(until)` and retry backoffs land within a
cycle interval of their deadline rather than on a precise wake. At the derived ~67ms that is as good as the
floor-gated path it replaced; at the `refillIntervalCap` (1s), or under a pinned bench interval, it can
overshoot by that much. Acceptable for a durable sleep - do not reintroduce a timer to shave it.

### Round-trip minimization in `processStep`

`processStep` is the hot path, and it is strictly **sequential** end to end - the engine spawns no goroutines outside
its lifecycle loops and imports no errgroup. What optimization there is collapses round-trips to a remote database,
not parallelism:

- **Claim UPDATE + step SELECT** - on pgx/sqlite/mssql the claim and read are **one** round-trip via
  `RETURNING`/`OUTPUT` (reads the row *as updated* - a consistent snapshot). MySQL lacks RETURNING, so they are two
  statements run **serially** (claim first, read only on success): running them on separate connections is unsafe -
  an independent read snapshot can observe the pre-transition row and deliver an empty resume payload (the reason is
  spelled out at the claim site in `execution.go`). The lease size comes from the step row's own `time_budget_ms`
  (referenced self-referentially in the claim UPDATE), not a pre-SELECT.
- **Flow data** - runs after the claim+read, since it needs the `flow_id`.

Fan-in accounting no longer issues sibling/subgraph COUNT queries at all - it reads the
`cohort_arrivals`/`cohort_failures`/`cohort_size` counter columns.

**Transaction constraint (do not reintroduce parallelism here):** a function receiving a `sequel.Executor` - which may
be a `*sequel.Tx` - must not run concurrent statements on it, because a SQL transaction is not safe for concurrent
use. This is exactly what forbids wrapping an errgroup around, say, the flow read and `resolveStateRefs` (which takes
a `sequel.Executor`). It applies to `computeFinalState` and code inside `failStep`/`Cancel` transactions.

### Fan-Out and Fan-In

**Static fan-out** occurs when multiple transitions match from one task. All targets run in parallel, sharing a
`step_depth`. The flow's `step_id` is `0` during fan-out.

**Dynamic fan-out** uses `forEach` on a transition to iterate a state array and spawn one task instance per element,
each receiving the element under the `as` key. An **empty array** spawns no branches but does **not** stop the flow:
the transition path routes straight to the fan-in node (`fireFanInDirect`), which runs on the source step's own
`state + changes` with the graph's reducers applied - so an empty cohort and a non-empty one agree on what the fan-in
sees for a reducer-managed field the branches never touched (`fixtures/emptyforeachreducerflow_test.go`). The
fan-in target comes from the routing map the engine derives per flow at dispatch (`internal/faninmap`), not from
`Validate`. The `fanInTarget == ""` arm - complete the flow at the source - is therefore effectively unreachable for
any graph the engine accepted: `Validate` (run at `Create`) requires a fan-out source to converge on a `SetFanIn`
node. It is a defensive fallback, not the documented behavior of an empty array.

**A branch sees its flow's state, plus its element.** Each `forEach` branch's local `state` is the flow state with
three injected fields: `<as>` (the element), `<as>Index` (its position), `<as>Count` (the cohort size). Nothing is
removed. **There was once a "branch state strip"** - the engine deleted the source array from each branch's local
state (an N-element forEach feeding `forEach -> A -> B -> C -> J` otherwise writes N copies of the array into every
step row of every branch). **It was removed, and must not come back as a special case.** It was a byte optimization
for the one carried field the engine happens to know the name of, while every *other* large carried field paid the
same N x chain-length cost unaddressed; it made a branch's state a lie (the branch could not see the array its own
element came from); and it is what made a failed fan-out's `final_state` come back *missing* the array - a failed
cohort never reaches its fan-in, so the terminal-state merge bases on a completed sibling's branch-local snapshot,
which is precisely the stripped one. De-duplicating large carried state is a general mechanism - **state refs**,
below - not a per-field deletion. The source array is now *ref'd* (one stored copy, every branch pointing at it),
which is the same byte saving without deleting anything or lying to the branch.

**Downstream suppression via explicit clear** (below) remains the author-space way to drop a large source array past
the fan-in.

**Downstream suppression via explicit clear.** A branch that wants to suppress the source array past the fan-in calls
`flow.Set(<forEach>, nil)` in its body. That writes the new value into the branch's `changes`, the replace reducer
at fan-in folds it over the spawn-step base, and the field is absent (or whatever the branch wrote) past the fan-in -
useful for a forEach over a very large array where downstream tasks only care about the per-element transformation.

**Fan-in strip on dynamic fan-out.** The injected per-branch bookkeeping (`<as>`, `<as>Index`, `<as>Count`) has no
meaning once the cohort is behind the flow: with the Replace reducer, one branch's element value and index would
otherwise win arbitrarily and ride forward - the flow's state would say which element it saw by *completion order*.
So `stripForEachBookkeeping` (`completion.go`) deletes those three names. A workflow wanting the element value past
the fan-in must forward it under a different key. **Two paths call it and both must**, or they disagree on what a
fan-out's state means:

- `insertFanInStep` - the cohort **converged**; the merged fan-in snapshot is stripped, scoped to the **spawn's own
  task** (`tr.From == spawnTaskName`).
- `computeFinalState` - the cohort **failed** and so never reached its fan-in. The terminal state is the merge of
  the tail steps, and a completed sibling of a failed cohort *is* a tail (its `successor_id` was never pointed at a
  fan-in that never fired), so the merge base is a branch-local snapshot carrying that branch's bookkeeping. Without
  the strip, a failed fan-out's `final_state` reports an arbitrary branch's element as the flow's own. The cohorts
  to strip are read from **the merge base tail's own lineage chain** (`cohortSpawnTasks` walks `lineage_id` upward,
  one PK lookup per nesting level), never from the graph at large.

**The strip is scoped to the cohort being closed, and BOTH halves of that scoping are load-bearing.** Stripping
"every `forEach` in the graph" - which it briefly did - broke two things at once:

- **Name collision (silent data loss).** It made the three injected names of *every* `as` globally reserved. A graph
  with `forEach … as "page"` reserved `pageCount` for the whole workflow, so a task writing its own `pageCount` -
  even one downstream of the fan-in, outside the cohort entirely - had it deleted from `final_state` while `History`
  still showed the step had written it. The author was never told the name was reserved. Scoping by the merge base's
  *lineage* fixes it: a tail outside any cohort (the ordinary completed flow, whose terminal step is downstream of
  every fan-in) is inside no cohort and strips **nothing**.
- **Nesting.** At an *inner* fan-in it also deleted the *outer* cohort's bookkeeping, so a step converging out of the
  inner cohort - still inside the outer branch - could no longer see which outer element it was working on. Scoping
  by `tr.From == spawnTaskName` fixes it: each cohort's names die at its **own** fan-in and no earlier.

The names are reserved only *within* their own cohort. Pinned by `engine/foreachstrip_test.go` (collision, nesting,
and the failed-fan-out case the strip exists for), each verified to fail against the unscoped version.

**Fan-in** is implicit. When the last sibling at a cohort completes, the engine merges all siblings' changes using
reducers and creates the next step(s) in a transaction that prevents duplicate next steps when multiple workers
finish siblings simultaneously.

**Fan-in does not escalate on cancelled or failed siblings.** If a sibling is `failed` or `cancelled` when fan-in
evaluates, the flow is already being driven by another path - a sibling's `failStep` cascaded the flow to failed, or an
external `Cancel` cancelled it. The fan-in worker returns `nil` instead of calling `failStep` on its own step: doing
so would race an in-flight `OnError` handler (an errored branch routes to its handler, whose next step runs at depth
N+1 while the fan-in worker is still finishing depth N) and could incorrectly fail an otherwise-recoverable flow.

**Fan-in merge order and contribution (lineage `SetFanIn` path).** `insertFanInStep` reads cohort members
(`lineage_id = cohortSpawnID`) `ORDER BY fan_out_ordinal, step_id`. `fan_out_ordinal` is stamped at fan-out from the
branch's position in the spawn loop (the `forEach` array index or static declaration order), so `list`/`append`/
`sum`/`set` reducers fold in input-array order rather than completion order; `step_id` breaks ties. The firing gate is
`cohort_arrivals >= cohort_size`, a counter on the spawn step independent of the merge query, so the merge's status
filter cannot deadlock fan-in. Only `completed` members contribute `changes`; `failed`/`cancelled`/`pending`/
`running` contribute nothing.

**Escalation is counter-based (`cohort_failures`), not status-based, and happens *before* `insertFanInStep`.**
`insertFanInStep` itself never marks the flow terminal - but it is only reached when the cohort resolves with
`cohort_failures == 0`. The escalation decision lives one level up, in the cohort-arrival path of `processStep`:
when a branch's own `failStep` runs with no `onError` handler, `propagateCohortFailure` bumps the spawn step's
`cohort_arrivals` **and** `cohort_failures` (walking up nested cohorts); and when a cohort fully arrives with
`cohort_failures > 0`, the flow is **failed** instead of creating a fan-in step (`fullyResolved && failures > 0`).
The signal is a *counter* incremented only by a genuine `failStep`, so it is distinct from the removed status-based
"poison" (which scanned member *statuses* in the merge and failed on any `failed`/`cancelled` member - that raced
with OnError recovery and made the fanouterrorflow fixture flaky). A branch that errored but has an `onError` handler
does **not** bump `cohort_failures` - its step is marked `completed` and routed to the handler, so the cohort still
converges and the flow recovers via the handler -> fan-in path; likewise a `cancelled` member (from an external
`Cancel`) does not escalate. So the invariant is precise: escalate only on a genuinely-unhandled branch failure
(`failStep` with no `onError`), never on an onError-handled error or an externally-cancelled sibling.

**Retry rejoins its cohort naturally.** `flow.Retry` rewinds the failed step in place - same `step_id`, `lineage_id`,
`fan_out_ordinal`, just `status='pending'` and the prior error/park slot cleared. The merge query sees one row per
branch regardless of attempts, so retry can't double-count.

**Do NOT replace the shared `cohort_arrivals` counter with per-member arrival state. It was built, measured
and reverted (2026-07-19).** The idea is sound and keeps re-suggesting itself, so here is the record.

*The motivation.* Every sibling of a cohort bumps `cohort_arrivals` on the SAME spawn row, and that row's write
lock is held until COMMIT, so siblings serialize. That is real and measurable: at fan-out width 64 the wait was
~84% of an arrival transaction locally. `docs/benchmark-cloud.md` also (wrongly - see below) blamed it for the
~9,400 steps/s fan-out ceiling.

*What was built.* `cohort_arrived` on each member's own row plus `cohort_resolved` on the spawn as an
exactly-one-resolver claim, so a branch marks its own row, COMMITS, and only then counts the cohort and
resolves it in a second transaction. Committing before counting is required, not stylistic: counting inside the
arrival's own transaction lets the last two members each miss the other's uncommitted mark under READ
COMMITTED, so neither resolves and the cohort strands silently forever.

*It worked, and it did not pay.* Contention fell 12-40x locally and 4.4x on the cloud rig. Throughput went the
wrong way: six A/B points on the ceiling rig (3x 8-vCPU shards, 4-vCPU engine, width 16, interleaved, n=3, fresh
databases) gave +7.5%, -12.1%, -11.0%, -0.5%, -9.7%, -29.1%. Only the least production-like point was positive.
**It degrades as load gets realistic** - it trades a lock wait for an extra transaction. The binding resource at
the ceiling is connection occupancy and round trips - and a blocked lock IS connection occupancy: a sibling
queued on the spawn row holds its connection for the whole wait. (An earlier phrasing here - "the scarce resource
is round trips and connection occupancy, *not lock time*" - drew a false line: lock wait is a *driver* of
occupancy, not a thing apart from it.) So relieving the lock pays only if the relief does not itself occupy
connections *more*. A second transaction does. A per-peer mutex does not - which is the next section.

*Three collateral findings worth keeping:*

- **The convoy is intra-cohort.** Spreading a cohort's branches over ~100ms of task latency HALVES the spawn-row
  contention. So production, where branches finish at different times, has less of this than any zero-delay
  benchmark - while the extra transaction costs the same. The two effects compound against the redesign.
- **It does not reduce write amplification.** Marking each member's own row REDISTRIBUTES row versions rather
  than removing them (5.59 -> 6.18 updates per step, HOT ratio flat). The hoped-for fix for the volume finding
  is not there.
- **The ceiling diagnosis in `docs/benchmark-cloud.md` was wrong** and has been corrected. Row-lock waits are
  ~15% of active backends, not "the great majority"; the "W serialized fsyncs" mechanism is refuted (width held
  fixed while varying workers moved lock wait 31x - the queue is `min(width, workers)` and the serialized
  quantity is round trips inside the hold); and eliminating the lock entirely did not raise the ceiling. **What
  binds at ~9,400 steps/s is an open question.**

*If you are tempted again,* first show that the cohort row lock is the BINDING constraint on the target
workload - it was not, on any of six points. The measurement recipe is a single long-lived `psql` session
sampling `pg_stat_activity` (`wait_event='transactionid'` is a row-lock wait, `'tuple'` is the queue behind it,
exclude `ClientRead` from the denominator); it needs no engine rebuild and instruments both arms identically.

**The lock's connection occupancy IS relievable per-peer - with a Go mutex, not a schema redesign (2026-07-21).**
The reverted redesign attacked the lock the expensive way (per-member rows + a second transaction). The cheap way
keeps the shared `cohort_arrivals` counter and its single-transaction atomicity untouched and instead serializes a
cohort's arrivals THROUGH THIS PEER before they reach the database. Each worker takes a fixed striped Go mutex
(`e.cohortLocks`, keyed on the cohort's spawn via `cohortLockStripe`) BEFORE opening the transition (`processStep`)
or failure (`failStep`) transaction - the point a losing sibling would otherwise take its connection and then queue
on the spawn row's write lock. The loser now parks on the mutex holding NO connection; the row stays the cross-peer
source of truth, so the count and the fan-in trigger are byte-for-byte unchanged. Spawn-row connection occupancy
drops from cohort-width to R (one in-flight arrival per peer).

This is the counter-proof to the "not lock time" correction above: it relieves the *same* lock, adds NO round trip,
and throughput RISES where the per-member redesign fell. Measured locally (single peer, width 64, conc 64, 16-vCPU
pool): the spawn-row `tuple` waiters - the queue the reverted section names - went 146 -> 0, width-64 throughput
rose (~+30-40%, noisy locally) and p50 fell. It is **not** a ceiling fix: at width 256 it is throughput-NEUTRAL,
because the binding lock there is no longer the spawn row (`tuple` -> 0) but relation-extension (`extend`, from the
successor-INSERT firehose) plus closed-loop queue depth - consistent with the reverted section's finding that the
cohort lock was never the ~9,400 ceiling. Freeing its connection occupancy helps the mid-width regime, not the
ultimate ceiling, which stays open. A single-workload bench also UNDERSTATES it: the freed connections serve OTHER
flows in a real multi-tenant engine, which the bench cannot see.

*Implementation load-bearers (`processStep`, `failStep`, `cohortLockStripe`):*
- **Fixed striped array, never resized.** A stripe array's mutual exclusion rests on a key always mapping to the
  same mutex instance; a resize (e.g. to track a changing worker ceiling) would hand two goroutines different
  mutexes for one key and silently lose exclusion. Sized generously (8192), not tuned: a collision is benign - two
  live cohorts sharing a stripe cost one arrival a brief connection-free wait, never correctness.
- **Acquired before any DB work, at most one per worker, leaf-level only.** The stripe is the direct (bottom)
  cohort level; `propagateCohortFailure`'s ancestor bumps walk UP and stay DB-serialized. Because every worker
  takes its one stripe BEFORE holding any database lock and never takes a second, no Go/DB lock cycle can form.
- **Released before `processStep`'s post-commit switch.** A non-contention persist error routes to
  `failOnPersistError` -> `failStep`, which re-locks the SAME stripe, and `sync.Mutex` is not reentrant - so the
  normal release is explicit there; the deferred unlock is only the panic backstop.
- **Coalescing was considered and rejected.** Folding a peer's arrivals into one `+k` write would split a member's
  result-commit from its arrival-count, and either order leaves a crash window that strands (result committed,
  in-memory arrival lost on a peer crash -> the cohort never reaches `cohort_size`) or double-handles a member. The
  single-transaction coupling of (own result + arrival) is the same invariant the reverted redesign's
  commit-before-count wrestled with. Serialize-only keeps it intact, and connection occupancy was the whole cost.

### Candidate de-duplication: the partition (cross-peer) and the claim map (intra-peer)

A candidate the refiller selects can be one another worker is already claiming, and every such duplicate
costs a **full dispatch round trip to be told it lost the CAS**. There are two distinct sources, they need
two distinct mechanisms, and each is useless against the other's case:

- **Cross-peer.** The planner's key pick is independently random per replica, but *within* a key the fetch
  is deterministic (`ORDER BY created_at, step_id`), so every replica that picks a key fetches the SAME
  oldest rows. Handled by `partitionPredicate` - `AND step_id % R = ordinal` in **both** phase 1 (so the
  tally reflects the owned slice) and phase 3 (so the fetched ids actually are owned). Phase 1 alone is
  not enough: it only shapes per-key counts, while phase 3 returns the ids workers claim.
- **Intra-peer.** The selection predicate filters **committed** state, and a claim that has been issued
  but not committed still reads `pending` with a free lease. So *this* replica's own next pass legitimately
  re-selects a step *this* replica is mid-claim on. No SQL filter can see an uncommitted write - only the
  process can. Handled by the `claims` tracker (`internal/claimstracker`): `TryClaim(shard, stepID)` in the
  worker loop, before `processStep`, skips a step a sibling worker in this replica already has a claim CAS
  in flight on. Keyed on `(shard, stepID)` - `step_id` is a per-shard auto-increment, so a step-id-only key
  would report shard 2's step 42 as in-flight because shard 1's is.

Both are **advisory**: the claim CAS remains the only thing that grants a step, so a stale or missing entry
costs a wasted round trip or a deferred dispatch, never correctness. That is the same posture as the
candidate cache holding "hints, not ownership", and it is what keeps either mechanism from becoming a
distributed lock.

**Three metrics price the three stages of candidate waste, and they are not interchangeable:**
`dwarf_refill_candidates_discarded` (selected, never popped - refiller oversupply),
`dwarf_steps_claim_preempted` (popped, skipped before any claim - round trips SAVED by the map), and
`dwarf_steps_claim_lost` (popped, claimed, lost the CAS - round trips WASTED). A healthy engine converts
what would have been *lost* into *preempted*; comparing the two is how you tell the map is working rather
than over-suppressing.

**Measured (local Postgres, `linear`, 256 keys).** Cross-peer: claim miss at R=2 fell **41.3% -> 11.6%**,
and with re-delivery suppressed (a 250ms cycle interval, where the R=1 baseline is 0%) R=2 measures **0.12%** -
peer contention eliminated, and R=2 throughput goes **flat** against R=1 (×1.04), which is the correct
outcome for replicas sharing one database's fixed capacity. Intra-peer: at a 0ms floor (scanning flat out)
claim miss fell **7.3% -> 0.1%** at unchanged throughput.

**The reservation is a bounded WINDOW (1-2s), not a lifetime tied to the step. Both bounds were
established by breaking them:**

- **Too short - releasing when the CAS returns** was built and measured and barely worked (7.3% -> 5.7%).
  The gap to span runs from SELECTION to POP, not the round trip between them: the refiller selects a step
  whose claim is uncommitted, the entry sits in the cache, and by pop time a CAS-scoped reservation is long
  gone. The window must outlast the max interval between cycles (~1s) so a step is never
  re-selected while its own claim is still in flight.
- **Too long - holding for the whole STEP** was tried next and is worse. A worker parked in a long
  `ExecuteTask` keeps its reservation for the entire task, so if that step's lease expires meanwhile (an
  overrun, a DB clock step) **no sibling worker can re-claim it and single-replica lease recovery stops
  working**. Caught by `TestLeaseFence_CompletionNoDuplicateSuccessor`, whose blocked first dispatch is
  exactly that shape. Step-scoped only appeared to work because steps in that benchmark take ~15ms; it is
  workload-dependent and unbounded above.

The window is a fixed 1-2s (`internal/claimstracker`), well over the cycle interval and **far below
`leaseMargin` (30s)** so it can never be the reason a lease-recovered step fails to re-dispatch - the
integration guard is `TestLeaseFence_CompletionNoDuplicateSuccessor`. The tracker gets both bounds and
expiry from a **two-generation rolling window** rather than a per-entry TTL + sweep: two maps hold the
current whole-second bucket and the previous one, a lookup checks both, and once a second the maps ROLL
(current -> previous, previous dropped, fresh current) - O(1), three pointer assignments, no per-entry
walk. This is what closes the sweep's failure mode: a per-entry-timestamped map under a *pinned* worker
pool at high throughput (`SetWorkers(512)` at thousands of steps/s) outgrows any size gate and gets walked
on every claim under one lock; the rolling window has no scan at all. An entry lives 1-2s (from its point
in the second to the drop two rolls later), which is the whole safety argument - a reservation can only
ever DELAY this replica's dispatch of a step by a bounded window, never prevent it. See the package doc.

**Three implementation load-bearers:**

- **Key on `(shard, stepID)`.** `step_id` is a per-shard auto-increment, so every shard has a step 42; a
  step-id-only key would turn away live candidates on every shard but the one holding the reservation.
- **Check in the worker loop, not inside `processStep`.** A skip then costs nothing but the next `Pop`
  (which blocks when the cache is empty, so there is no busy-spin), and skips are self-limiting: in-flight
  claims are bounded by the workers currently in the claim window, against a cache of 2x the worker count.
  Do **not** ask for an extra scan on the skip path - it was tried (as a `requestRefill`, back when a
  trigger existed), and at a 0ms interval it fed an already-spinning refiller (throughput 1,632 -> 941).
- **EVERY path that re-offers a step to THIS replica must `RelinquishClaim` first.** The window is sized to
  outlive a *scan*, so it necessarily outlives a step's own dispatch - which means a step re-armed and
  re-offered here still carries the reservation its previous dispatch took. Left in place it makes the
  replica skip its own re-dispatch: every worker that pops the step is turned away by `TryClaim` and the
  refiller keeps re-selecting it, for up to the full ~2s. Three paths do this and all three relinquish -
  the recovery-defer reset and the `flow.Retry` rewind (both `execution.go`), and **`enqueueStep`**
  (`operations.go`), which covers the cold re-offer sites: the surgraph revive (three call sites), the
  resume leaf, and the wedge sweep. The `enqueueStep` one was **missing**, and it is a *latent* bug that
  the empty-partition `Offer` merely exposed: the reservation held either way, but declining the offer hid
  it behind the refiller's cadence. With the offer admitted, the revived caller is popped at once, skipped,
  and re-skipped until the window ages out - measured at `fixtures/completionraceflow_test.go` as 189 of
  500 flows failing to drain in 30s. Adding it took that fixture to **2.2s, below its 5.9s pre-change
  baseline**, because a revive no longer waits out a stale reservation at all. A relinquish for a step id
  this replica never reserved (Fork's leaf, `Continue`) is a harmless no-op, which is why the guard belongs
  at the shared entry point rather than at each caller.

**The partition divides DISPATCHERS, not the pool divisor** - counted from `dwarf_peers.dispatched_at`
freshness, per shard (`internal/peers`). The column is EVIDENCE that a piston turned rather
than a claim about intent: a replica that publishes "I dispatch" and then wedges keeps its residue class
forever, and a class nobody selects is work nobody runs, whereas a stale timestamp drops it from the
divisor on its own while `seen_at` keeps it counted for the connections it holds. The two windows are
asymmetric in OPPOSITE directions and deliberately so - over-counting the pool divisor over-sizes pools and
can collapse a database, while over-counting DISPATCHERS strands work, so the first errs toward keeping a
replica and this errs toward dropping one.
`dispatched_at` also advances while a cycle has been working LONGER THAN ITS OWN PERIOD, without which a
deep-backlog scan (still O(backlog) on any dialect lacking the run-condition early-stop) would drop every
healthy replica in a loaded fleet out of the divisor at once. The duration qualifier is load-bearing rather
than incidental: "a cycle is in flight" reads true ~1.2% of the time even when every scan fails instantly,
which a reader sampling on a cadence catches within seconds - so a piston serving NOTHING would hold its
residue class forever. See `internal/piston/CLAUDE.md`. Registration does NOT stamp it - intent is not evidence - so a
replica earns it on its first cycle, and the beat rides the read cadence when that evidence flips so the
window is a read interval rather than a beat interval. An await-only replica (`SetWorkers(0)`) holds
connections, so it counts toward the pool divisor, but it claims nothing: giving it a residue class means
*nothing ever selects those steps*. This shipped broken in the first cut and hung
`fixtures/crossreplicaawait_test.go`, which uses exactly that configuration.

**Everything fails open.** Solo dispatcher, unknown ordinal (self absent from the roster), an ordinal out of
range for the divisor, a Sonar gone blind, or no Sonar at all - every one disables partitioning on that
shard. Partitioning EXCLUDES rows, so a wrong pair strands a residue class while declining only restores
overlapping selection - slower but complete. The pair is published as ONE value, so a reader can never
catch half a fleet change: an ordinal only means anything against the count it was derived from.

*The count and the ordinal come from ONE shard's rows*, which is what per-shard accounting makes automatic -
there is no cross-shard roster to assemble, and no timestamps from different clocks to rank.

**The doorbell is deliberately NOT partitioned, and the case for that got simpler twice.** `Offer` now
admits only into an EMPTY partition, so a busy replica never offers at all, and a uniform-priority workload
offers only at the head of a drained chain. Partitioning that check would delay exactly the sequential-hop
case it exists for, and the claim CAS still arbitrates, so the worst case is one lost claim. Note this
concerns **only this replica's own** origination sites: with the peer `enqueue` broadcast removed, no
unpartitioned offer can arrive from a peer at all, which closed the one case where it genuinely raced the
residue class's owner.

**Known residual: replica death strands a slice for up to the dispatch window (5s) plus a read cadence.** A
dead replica's residue class is owned by nobody until its row ages out of the dispatcher count and ordinals
re-seat. Self-healing,
accepted deliberately rather than papered over with an age-based fuse (a fuse threshold above normal
queueing delay - which is *seconds* under load - would not fire when needed, and below it would disable
partitioning under exactly the load it is for).

**A replica that is SLOW rather than dead is a different failure, and the partition alone has no answer for
it - `internal/piston`'s STEAL is that answer.** Eviction handles death: the dispatch evidence stops, the
divisor shrinks, the class is redistributed. A slow replica keeps beating, keeps its class, and cannot serve
it - so nothing else will look at those steps. Measured on a three-replica fleet with one replica crippled,
work created by an await-only replica so the residue class is genuinely in the path: throughput fell to
**137-153 steps/s against a commanded 500**, its class aging past 30s while its healthy peers sat at a third
of a core - *worse than removing that replica from the divisor entirely* (494-505). A capacity cripple (one
worker) and a latency cripple (+10ms RTT) produced the same cap, so the coupling belongs to the partition,
not to how a replica goes slow. With the steal: **458-501**, at 0.4-5.4% claim loss.

The mechanism, its gate, its two tiers and the measurements behind each are in `internal/piston/CLAUDE.md`.
Three things stay the engine's:

- **The pair the piston partitions on is still `partitionOn(shard)`** - the steal relaxes that pair's
  predicate, it does not change how the pair is derived. An await-only replica is still excluded from the
  divisor, and every fail-open case still disables partitioning outright, which leaves nothing to relax.
- **`dwarf_steps_stolen{shard}` is the operator's signal that a peer is alive but not serving its share.**
  Zero in a healthy fleet by construction (measured 0-23 steps across five arms, because the grace blocks a
  fleet that is keeping up). A sustained nonzero rate names the condition nothing else reports - and it is
  the quantity to read `dwarf_steps_claim_lost` against, since stealing trades exclusivity for coverage.
- **The doorbell bypasses all of it, so a fixture must create from an await-only replica to test any of
  this.** `Offer` admits a step into the local cache regardless of residue class, so a chain created on a
  dispatcher walks hop by hop on that dispatcher and never consults a class. That is why the steal fixtures
  create through a `SetWorkers(0)` replica: its offers land in a cache with no workers to pop them, so its
  work is reachable only by a peer scanning. The bench disables the doorbell outright to isolate the path;
  **the fixtures do not** - they run at the production default and still exercise the steal, which is the
  stronger statement of the two.

### State refs — a large carried field is stored once (`internal/staterefs`)

A step's `state` is a full input snapshot, so a field that is *carried* but not *changed* is re-serialized into
every row it survives into: D copies down a chain of depth D, and **N·D across a fan-out of N branches**. A field
above a size bar is therefore not written into the successor's `state` at all: it is omitted, and the
`dwarf_steps.state_refs` column records `{"<field>": <anchor step_id>}` — the step that physically holds the bytes.
The doc-extraction shape (a document fanned out over pages, then chunks) measures **~29x** fewer stored state bytes.

**The mechanism, the size policy, and the invariants that cost bugs to find live in `internal/staterefs/CLAUDE.md`
— read it before changing any of them.** Three things stay the engine's, because each is engine state rather than
ref policy, and they are what `engine/staterefs.go` holds:

- **Which Linker.** One per shard, built in `initRuntime` from the shard's own handle. A `step_id` is only unique
  within a shard, so the resolve cache's key is too; the driver (the policy's one dialect-dependent term) is a
  property of that handle as well.
- **How an anchor is read.** `anchorLoader` turns a shard handle into the `staterefs.Loader` the Linker calls once
  per resolve. It **may be a transaction**, and at the two fan-in sites it is: anchors never cross a flow and a
  flow never crosses a shard, so resolution is always a same-connection read and those paths resolve
  inside the transactions they already run in. That is exactly why the Loader is a per-call argument rather than a
  connection bound at construction.
- **The byte metric.** `anchorLoader` counts the RAW payload columns it scanned and the wrapper attributes the
  total to a workflow. Count there, not from the values the Linker returns: those are the decoded per-field
  bytes, which drop every key, brace and separator and read zero for a column that failed to decode - all wrong
  for `dwarf_state_read_bytes`, whose job is tracking the database's byte-throughput ceiling.

**The shape: resolve once at dispatch, mint once at the successor write.** Everything in between works on
literals — `when` evaluation, `forEach` expansion, the task carrier, the transport to a remote task — so the ref
encoding never leaks into transition evaluation or task-facing code. Minting needs **no database read**, which is
what makes the write-side win free of round-trips. Intra-step merge is replace-only (`State.Merge`, then
`DelNils`), so three cases per key: a literal drops the ref (and may re-anchor here), a tombstone drops field and
ref, absence carries the ref forward.

**Engine-side invariants** (the storage-side ones are in the package doc):

- **Flatten at every flow boundary** — `final_state` (which outlives its steps; `DeleteOnCompletion` may reap
  them), `Continue`, subgraph `in`/`out`, and Fork's leaf.
- **`Snapshot`/`Step` resolve** — the encoding is internal storage, never API-visible. `History` is exempt: it is
  metadata-only and selects no payload columns, so no ref reaches it — use `Step` for a step's data.
- **Anchors never cross a shard**, which is what lets the fan-in and final-state paths resolve inside their own
  transactions.
- **Refs are minted only from settled predecessors** — a future "re-run a completed step in place" feature would
  invalidate every ref into it.

**Fan-in resolves only what it reduces** (`ResolveReduced`). A combining reducer needs its accumulated base, so a
ref'd field it folds must be materialized or the fold would apply the delta to an *absent* base and silently lose
everything accumulated. A field is materialized iff the graph registers a reducer for it — a safe superset. A
merely *carried* ref is passed through untouched, still pointing at its original anchor: materializing it at every
fan-in would re-anchor the payload and hand back the win in exactly the fan-out graphs the design exists for. The
fan-in mints against the **spawn** step, while member-contributed and reduced fields are inlined into the fan-in's
own `state`.

**A carried ref's anchor is NOT necessarily the spawn.** The spawn's row holds a carried field's bytes only when
the spawn is itself that field's anchor (e.g. a fan-out source that is also the entry step holding the flow's
initial input). When an anchoring step *precedes* the fan-out source, the spawn merely carries the field as a ref
pointing at that earlier step. Because such a ref is deliberately not materialized, the carried key is absent from
the fan-in's `merged` state — which is why `Mint` re-emits an inherited ref whose key is absent from `merged`, and
why `inlineExcessAnchors` pins such an anchor past the cap. Omitting either silently dropped the field from the
fan-in step onward and from `final_state`. Pinned by `fixtures/staterefscarryreview_test.go` (populated / empty /
nested cohorts, plus `Continue`/`Fork`).

**`Fork` remaps ref targets** through its clone id map, and this is *why refs live in their own column*: Fork
clones step rows with a DB-side `INSERT...SELECT`, so the payload bytes never pass through the engine — remapping
a ref buried inside the state JSON would have meant reading every large blob into Go to rewrite it, hauling
precisely the payloads this feature exists to stop hauling. An anchor is always an ancestor of the step that refs
it, and pruning removes only descendants of the rewind step, so a kept step's anchors are always kept; a zero
mapping is caught, not written as a dangling ref. The fork *leaf* gets materialized state and cleared refs.

**The forEach element is never ref'd** (`<as>`/`<as>Index`/`<as>Count`) — the engine synthesizes it per branch, so
its bytes are in no step row and a ref to it would dangle. The engine passes those names as the mint's
`inlineOnly` set, which is a distinct signal from a field appearing in `changes` (see the package doc — a nil
tombstone cannot substitute for it).

Pinned by `internal/staterefs` (the tiers, both-column resolve, one-hop assert, the batched load, the
bytes-not-values cache), `engine/staterefs_test.go` (the fan-out and fan-in storage assertions against a real
database) and `fixtures/staterefsflow_test.go` (end-to-end: a carried document across fan-out/fan-in, `Continue`,
`Fork`, overwrite and tombstone).

**One open follow-up is the engine's rather than the package's:** a **large-state warning**.
`metricStateWriteBytes` is already emitted per step; a warning when a step's state crosses a threshold would turn
"you were supposed to clear the PDF" from folklore into something the engine says out loud. Nearly free, and its
firing rate is itself evidence for how much refs are needed.

### Execution-DAG edges (`predecessor_id` / `successor_id`)

`lineage_id` is a cohort-counting device, not a DAG: a `forEach` source applies one `childLineageID` to every branch,
so an entire per-element sub-pipeline collapses into a single lineage and cannot reconstruct true parent/child
structure.

`dwarf_steps.predecessor_id` and `successor_id` record the actual execution edges, so the DAG
is *recorded*, not *reconstructed*. Every edge lands on at least one endpoint:

- **Linear** `X->Y`: `Y.predecessor_id=X` (at insert) and `X.successor_id=Y` (post-loop UPDATE in `processStep`).
- **Fan-out** `X->{Yi}`: every `Yi.predecessor_id=X`; `X.successor_id` = the first child only (the full set recovered
  from the children's `predecessor_id`).
- **Fan-in** `{Yi}->Z`: `Z.predecessor_id` = the last cohort member to finish; every cohort *exit* step gets
  `successor_id=Z`. The exit set is `lineage_id == cohortSpawnID AND task_name IN` the graph-predecessor tasks of
  the fan-in **AND `successor_id == 0`** - **not** the whole lineage, so `A`/`B` in `forEach->{A->B->C}->J` are
  excluded and only the `C`s point at `J`. The `successor_id == 0` conjunct is load-bearing, not incidental: a
  branch that LOOPS through its exit task via `flow.Goto` (`forEach->{Work -Goto-> Work -> J}`) leaves several
  `Work` steps in the cohort, but only the last transitioned to the fan-in - the earlier iterations already wrote
  a forward edge to their next loop step. A step that transitions *into* a fan-in inserts no successor of its own
  (the fan-in step is created here; an earlier arriver's transition was only a counter bump), so its `successor_id`
  is still 0, while a loop iteration or a multi-step branch's interior step set `successor_id` when it created that
  successor. Without the conjunct, the task-name match alone overwrote those interior edges with the fan-in id,
  corrupting the recorded DAG (spurious `step -> fan-in` edges in `History`/`HistoryMermaid`). Pinned by
  `TestFanIn_GotoLoopBranchKeepsInteriorDAGEdges`. The `successor_id` write targets the exit steps **by primary
  key** (ids collected during `insertFanInStep`'s cohort-member merge scan), not by the
  `(flow_id, lineage_id, task_name)` predicate - which is unindexed and deadlocked concurrent fan-ins on SQL Server
  (the deadlock story is at that write in `execution.go`).
- **flow.Retry**: rewinds the step in place (same row), so `predecessor_id` is preserved. (A `Fork` copies the
  step into a new row and remaps `predecessor_id` to the cloned predecessor.)
- **Entry / subgraph-entry steps**: `predecessor_id` defaults to 0.

The Mermaid renderer ignores `step_depth` and `lineage_id`: it draws the deduped union of `{predecessor_id -> step}`
and `{step -> successor_id}`, exact for arbitrary nesting. Heads are nodes with no incoming edge, tails with no
outgoing.

**A failed fan-out's terminal state is rebuilt the way the convergence would have built it, not from the tails.**
When the merge base tail is a cohort member (`lineage_id != 0`), the cohort resolved with failures and never reached
its fan-in, and `computeFinalState` switches to `mergeCohortState` - the *same* merge `insertFanInStep` runs: the
**spawn** step's `state + changes` as the base, then every **completed member's** `changes` folded through the
reducers in `fan_out_ordinal, step_id` order. Merging the tails instead loses data, because **a branch's
intermediate output lives in the NEXT step's `state`, not in any tail's `changes`**: in
`forEach -> Cell -> Enrich -> Join`, only the *first* tail's `state` is consulted, so branch 0's `Cell` output rides
in via the base and branches 1..N-1's is silently dropped (measured: 1 of 3 branches surviving, and *which* one is
arbitrary). The spawn's row is the only base common to the whole cohort, and every member's `changes` is exactly the
set of per-branch outputs - every step of a per-element sub-pipeline inherits the spawn's lineage, so one indexed
scan covers `Cell` *and* `Enrich`, for every branch. Reducer order matters (`append`/`union`/`concat` are not
commutative), which is why the scan is ordered by the branch's position in the spawn loop rather than by completion
order. Pinned by `TestFailedFanOut_KeepsEveryBranchesIntermediateOutput`.

`computeFinalState` also reads the DAG, not `step_depth`. The terminal state is the merge of the tail steps -
completed steps with `successor_id = 0` (`mergeTerminalSteps`) - for a flow that is *not* a failed fan-out. The earlier `MAX(step_depth)` heuristic was wrong for
any graph where an intra-thread `flow.Goto` self-loop sits inside a fan-out: each loop iteration pushes
`step_depth + 1`, so the looping branch can outrun the fan-in/terminal step in depth, and `MAX(step_depth)` selected
the dangling loop step (empty state). The tail-step merge is depth-agnostic: loop iterations carry
`successor_id = <fan-in step>` (set by the cohort-exit UPDATE), so only the real terminal step qualifies. Two-tier
and depth-free: the completed tail (`successor_id = 0 AND status = completed`) for a normal finish; if none, the
non-completed tail (`successor_id = 0`, any status) for a flow force-terminated by `Cancel`/`failStep` before any
step completed. An empty map is returned for a flow with no steps.

### Time Budgets

Each step has a `time_budget_ms` that bounds the `ExecuteTask` call's context deadline. It defaults to the engine's
`SetTimeBudget` config (default 2m) but is **per-flow overridable** via `FlowOptions.TimeBudget`: the value is
resolved at `Create`, frozen onto the `dwarf_flows.time_budget_ms` column, and denormalized onto every step's
`time_budget_ms` (the entry step at `Create`, fan-out/fan-in steps from the flow-row value read in `processStep`).
The graph still carries no per-task timing - the budget is a per-*flow* default every step inherits. A host that
wants a tighter per-task bound enforces it inside its `ExecuteTask` (or the task itself), shortening the call context;
narrowing the deadline at dispatch is always allowed. The engine's budget is the outer ceiling.

`FlowOptions.TimeBudget` is **frozen at `Create`** (immutable for the flow's life, like `priority`), **inherited by
subgraph children** (`createSubgraphFlow` reads the parent's `time_budget_ms` alongside priority/fairness) and by
`Continue` (the thread's latest turn, since it takes no `FlowOptions`). A later `SetTimeBudget` change does not
retro-edit existing flows; it only seeds flows created afterward. On a subgraph spawn the child's `LoadGraph` is
bounded by the caller flow's budget; the create-time `LoadGraph` runs on the caller's own request context instead.

**No engine-imposed ceiling.** The engine enforces **no** upper bound on `FlowOptions.TimeBudget`, mirroring its
refusal to own a flow-level deadline. A host wanting an SLA cap validates `FlowOptions.TimeBudget` against its own
limit before `Create` and rejects an over-limit request there (a request to *exceed* the ceiling, distinct from
narrowing the deadline at dispatch, which the host may always do). A standalone caller setting a very large budget
owns the consequences below.

The worker lease is sized from the **step's own `time_budget_ms`** + `leaseMargin` (30s), written self-referentially
in the claim CAS (`lease_expires = DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?)`, only the margin a bind param), so
the lease always outlasts the budget that bounds the `ExecuteTask` call - even for a flow that overrode its budget
above the engine default. Sizing from the row (not in-memory config) is what keeps lease and budget from diverging;
it needs no upfront SELECT because `time_budget_ms` is already on the step row at claim time, the same read-locality
reason `priority`/`fairness` are denormalized there. Consequence: a *crashed* worker is recovered no sooner than its
step's `budget + leaseMargin`, so a flow's budget directly bounds its worst-case crash-recovery latency - which is the
practical reason a host caps the budget. (The earlier config-sized lease and its "decrease `TimeBudget` mid-flight"
re-dispatch trade-off are retired: each step's lease now follows its own frozen budget.)

### Persisting a step's outcome: retry the WRITE, never the task (`persist.go`)

The task has **already run** when its outcome is persisted - its side effects have fired - so this write is the
only record that it ran. Before `persist` existed, a database error here left the step `running` with `error=''`
and `attempt=0` (reading as perfectly healthy) and lease recovery re-dispatched it every `budget + leaseMargin`,
**re-executing the task**, forever. Silent and eternal: `detectOrphanedFlows` could not see it, because a
non-terminal step *did* exist. Reproduced against a live Postgres with a `\u0000` in state (no longer guarded on
write - see the storability punt in `workflow/CLAUDE.md` - but `persist`'s classifier now turns such a
rejected payload write into a clean step failure rather than the old eternal loop; the structural hazard was the
point).

The rule is: **retry the WRITE, never the task.** Re-dispatching is the one recovery that re-fires side effects,
and it was being used for failures the task had nothing to do with.

- **In-place retry, holding the lease.** A short exponential (1s/2s/4s). The errors this exists for - a failover, a
  dropped connection, a momentary connection-limit rejection - clear in **seconds**, so a blip is absorbed with
  **zero re-execution**. A minutes-long backoff would be slow in both directions: slow to recover from something
  that already cleared, and slow to report a permanent failure.
- **The lease is extended first** (`persistLeaseExtensionMs`, 30s). Without it, a task that consumed most of its
  budget has only `leaseMargin` left, so sleeping through the backoff would put the worker *past* its lease: a peer
  re-claims, **re-executes the task**, and the late write is fenced away - exactly the re-execution this prevents.
  The extension is itself a fenced, payload-free write, so a **zero-row** match means a peer already re-claimed us
  and we abandon silently.
- **Drain aborts it.** A worker asleep in the backoff selects on `drainStop` (closed at the top of `drainRuntime` -
  the lifetime ctx is deliberately not cancelled until *after* the workers drain, so there is nothing else to
  watch), releases its lease, and exits. That *does* re-execute the task on the peer that picks it up - it is the
  at-least-once contract, and it is what lease expiry would have done anyway, only sooner.

**The classifier: ask the database, do not read the error code.** When the retries are exhausted, `failOnPersistError`
attempts the **clean** terminal write (`failStep` - a status and an error message, none of the payload):

- it **lands** ⇒ the database is reachable, so the database is not the problem: the **payload** is. The failure is
  permanent, re-running the task would only reproduce it, and the step **fails** naming the driver's actual error.
  **The task ran exactly once.**
- it **also fails** ⇒ the database is unreachable, so nothing was recorded and nothing *can* be. Leave the step for
  lease recovery (`dwarf_steps_recovered` counts it). Re-execution here is unavoidable **and correct** - from the
  database's point of view the step never ran.

The two attempts are milliseconds apart and differ only in what they carry, which is what makes the classification
sound. It needs **no per-driver error taxonomy** (the part that would rot). Guessing the other way - terminalizing
on any unknown error - would kill live flows on every routine failover, and a terminal flow is **immutable**
(recovery is a human running `Fork`).

**Lock contention is excluded, and that exclusion is load-bearing.** `Transact` already retries it to exhaustion;
past that it reaches the recovery **defer** (rewind + re-poll), never the classifier. Terminalizing a flow because
the database was busy would be exactly backwards, and it is why `processStep` tests `IsLockContentionError` *before*
calling `failOnPersistError`. Consequence: the defer's `completed→pending` arm is now reached only by contention (or
by a classifier that could not write at all), which is what `TestLeaseFence_RecoveryResetFenced` drives it with.

**A fuse that can itself be poisoned is not a fuse.** `failStep` is the only way out, and it reads other steps'
payloads via `computeFinalState`. If an unreadable value had already landed in a row, that merge would die the same
way, `failStep` would error, and the classifier would misread it as "the database is down" - restoring the very
loop this closes. So `computeFinalState` failing on this path falls back to `final_state='{}'`, and the error
message is stripped of control bytes before it goes into a text column (Postgres rejects a NUL in `text` exactly as
in `jsonb`, so a driver error quoting the offending value back could kill the clean write).

Two counters: `dwarf_steps_write_retried` (a blip absorbed - the database is flaky, the workflow is fine) and
`dwarf_steps_write_failed` (an **alarm**, like `dwarf_steps_unwedged`: the database was reachable and the outcome
still could not be stored, so the payload is at fault and a latent bug exists). Pinned by `engine/persist_test.go` -
a transient error absorbed with the task running **once**, a permanent one terminalized with the task running
**once**, and a drain during the backoff releasing the lease instead of sleeping it out. Before the fix, both of the
first two tests **hang forever**.

### Lease fencing (`lease_seq`) — at-least-once, never state corruption

**Execution is at-least-once and may be concurrent; the fence guarantees only that the flow's persisted
state reflects exactly one execution.** Lease recovery re-dispatches a step whose `lease_expires` passed,
but nothing can distinguish a *crashed* worker from a merely *slow* one (failure detection over an async
boundary is unreliable), so the engine must let a second worker start in both cases — the task body can run
twice, and under lease loss the two runs can overlap. That is the standard durable-workflow contract:
**tasks must be idempotent.** Exactly-once *side effects* are impossible for the engine to provide (it does
not own the task's downstream), the same structural reason it disclaims backpressure and resource control.

What the engine *does* guarantee is that a late/slow worker (a "zombie") can never **corrupt or terminalize**
a flow the current owner is healthily re-executing. Two triggers open the zombie window, and only the first
is a task contract violation:

- The task ignores its `ExecuteTask` ctx deadline and runs past `budget + leaseMargin`.
- The **DB wall clock steps forward** past `lease_expires` (NTP correction, VM migration) while the worker's
  own **monotonic** ctx timer — unaffected by the DB clock — has not fired. No ctx discipline can close this.

The lease is `budget + leaseMargin` (30s) while the ctx deadline is `budget`, so a *cooperative* task always
returns and writes inside the margin, before its lease can expire — the common case never enters the window.

**The fence.** `dwarf_steps.lease_seq` is the lease **generation**, `lease_seq = lease_seq + 1` in the claim
CAS (and returned via the same `RETURNING`/`OUTPUT`/serial read that loads the step). Every post-execution
write to the *dispatched step* carries `AND lease_seq=?` with the generation the claim returned. A zombie
holds a stale generation (the current owner's claim bumped it), so its write matches **zero rows** and it
**bails with `nil`** — the same benign lost-race as losing the claim CAS itself (which also returns `nil`).
Returning an *error* instead would be actively wrong: it would spin `processStep`'s recovery defer (whose own
reset must also be fenced) and log an ERROR for a normal, expected lease-protocol outcome. The predicate is a
genuine `WHERE` filter (not a value-changing `SET`), so `RowsAffected` reflects the real match on **every**
driver, MySQL included — no `touch`-column trick needed on steps.

**`lease_seq` is bumped only where a lease is *granted* — the claim CAS.** `recoverExpiredLeases`' expired-lease
reset (`running`→`pending`) leaves it untouched: the reset does not grant a lease, it only makes the step
claimable, and the next claim bumps the generation. Consequence: a step reset-but-not-yet-reclaimed still
carries the prior worker's generation, so that worker's write is *not* fenced — but this is the benign
"before a peer re-claims" case (the peer's claim CAS still arbitrates: it cannot claim until `pending`, and
once it does it bumps the generation and redoes the work correctly). The dangerous case — a peer already
re-claimed and is running concurrently — is exactly the one the bumped generation fences.

**One gate per path; downstream is protected by the gate, not separately fenced.** Each post-execution path
has exactly one fenced write to the dispatched step, and everything after it is safe:

- **complete / goto / fan-out / fan-in-direct / flow-complete** — gated by the completion UPDATE
  (`WHERE step_id=? AND status!='cancelled' AND lease_seq=?`). Past it the step is `completed`, so no peer can
  re-claim (claim needs `pending`); the entire transition transaction — successor inserts, `cohort_arrivals`
  bumps, `successor_id` writes, `fireFanInDirect`, `completeFlowSequential`, `insertFanInStep` — needs no fence.
  `completeFlowSequential` in particular makes **no step write at all**: the gate already completed the step, so the
  trailing `UPDATE dwarf_steps SET status='completed'` it used to run was a re-write of the same value — a wasted
  transaction on every flow completion, and the one post-execution step write with neither a status guard nor a
  fence. It was removed; do not reintroduce it. (Its only other effect was to bump the step's `updated_at` a second
  time, to *after* `completeFlow`'s transaction — inflating the step's recorded task duration, which `History` and
  the `FlowRenderer` compute as `updated_at - started_at`, by the cost of completing the flow.)
- **fail** (`failStep`) — gated by the step-fail UPDATE, the transaction's first write, so a zero-row match
  wrote nothing: it commits the empty tx and returns `fenced=true`, and `failAndReturn` surfaces `nil` so the
  flow the peer is re-running is never failed. This closes the "late error → healthy-flow kill" case.
- **retry** and **subgraph park** — gated by their `status='running' AND lease_seq=?` UPDATE (both already
  bailed on zero rows); their in-tx follow-ups (`deleteSubgraphFlowsRootedAt`, child spawn) are protected by
  the gate's row lock.
- **interrupt** (`handleInterrupt`) — the leaf fence needs care: the combined step UPDATE must run **first**
  and lock the whole surgraph chain in PK order (matching `resume`'s lock order — the D2-deadlock guard), so
  the fence cannot be the leaf's first write. Instead the combined UPDATE runs unchanged, then an **in-tx read
  of the leaf's `lease_seq`** decides: on mismatch the whole transaction **rolls back** (undoing the ancestor
  re-park, which would otherwise flip callers out of `parkedSubgraph` and strand the parent revive) via the
  `errLeaseFenced` sentinel, and the caller returns `nil`. The check reads the leaf directly rather than a
  same-table subquery, which MySQL rejects on an `UPDATE` target.
- **recovery defer reset** — fenced with `AND lease_seq=?` so a zombie's `err`-path reset cannot rewind the
  peer's freshly-claimed step. Skipped when the captured generation is `0` (a claim/read error *before* any
  execution, where no peer can have stolen the lease yet — keeps prompt pre-execution recovery unfenced).

Writes to *other* rows (a shared cohort spawn step, the flow row, child flows) are not the zombie's ownership
to prove and are already arbitrated by the transition-tx flow-row lock-grab; the terminal-flow reap at claim
time (`status=flowStatus WHERE step_id=?`) converges to the frozen outcome idempotently and is left unfenced.

### Flow lifetime is the workflow author's responsibility

The engine imposes no flow-level deadline. Picking a max-lifetime that fits both a 1-hour batch and a 30-day approval
workflow is impossible, and a knob defaulting to "no deadline" is surface area without a customer. Workflows needing a
bound implement it in author space: a guard task reading `flow.CreatedAt()` that returns a 408 when too much time has
elapsed; a `flow.Retry` loop that exhausts after a chosen bound; an `OnError`/timeout transition; or an external
caller scheduling a `Cancel`. `Flow.CreatedAt()` and `Flow.UpdatedAt()` are populated on every dispatch, so the
elapsed-time guard is one call away inside any task.

### Transition Evaluation

Transitions are evaluated after a task completes successfully:

1. If the task called `flow.Goto(target)`, only `withGoto` transitions matching that target are taken.
2. Otherwise, all non-goto, non-error transitions are evaluated: those without `when` always taken; those with `when`
   taken if the expression matches the merged state.
3. `forEach` transitions iterate a state array and spawn one task per element. `forEach` cannot combine with
   `withGoto`.
4. Multiple matches -> all taken in parallel (fan-out).
5. No matches -> the flow completes.

**Error transitions.** When a task returns an error, the failed task's `onError` transition fires and preempts any
other transition (`onError` is exclusive with `when`/`forEach`/`withGoto`, so it is unconditional). The error is
serialized as a `TracedError` into state field `onErr` **with its stack frames stripped** (`Stack=nil` on a
shallow copy, so the shared error object keeps its stack for logs) - `onErr` rides into `changes`->`final_state`
and is readable by a state-bearing flow reader (`Snapshot`'s `final_state`, `Step`'s `state`/`changes` - not
`History` or `List`, which carry no state), and internal stack traces are code-structure
disclosure; the handler still gets the message, status code, trace id and properties for routing. The handler task
becomes the next step, and the failed step is marked `completed` - **with the task's own changes DROPPED**. (The
`error` *column* already carries only `err.Error()` - the message, no frames.)

**An error voids the task's changes**, and this is the contract on **both** error paths: the `onError` route builds a
**fresh** `RawFlow` seeded with the input snapshot plus `onErr` (`execution.go`), so anything the task wrote with
`flow.Set` before returning its error never reaches the handler, the step's `changes` column, or `final_state`; and
`failStep` writes only `status`/`parked`/`error`, never the changes. The two paths agree, so the contract does not
depend on whether the author happened to declare a handler. (This doc previously claimed the opposite - "changes
preserved" - which was never true of either path.)

The rationale is the same one that governs everything else here: **execution is at-least-once**. If a worker loses
its lease mid-task, a peer re-runs the task from the same input snapshot and *recomputes* its changes - so "what the
failing attempt wrote before it died" is not a stable fact about the flow; it depends on which attempt you observed.
Preserving it would hand authors a record that looks dependable and is not, and compensation logic built on it would
pass its tests and be wrong under lease recovery. It matches Go's own convention (an error voids the other returns).
The deliberate channels for a task that has something to say to its handler: put it **in the error** (`onErr` carries
message, status code, trace id, and properties), or give an external side effect **its own task**, so its success is
durably recorded before anything downstream can fail. Pinned by `fixtures/errorchangesflow_test.go` (both error
paths, plus a success control proving the write is only lost *because* of the error). Fan-out siblings are **not** cancelled - the errored branch
continues down its handler path and rejoins the cohort as a normal arrival (convergence is by cohort arrivals, not by
cancellation). If there is no `onError` transition, the step fails via `failStep`.

**Fan-out sibling constraint:** `Graph.Validate()` (via `validateLineage`) enforces that all branches of one
fan-out **converge on the same fan-in node**. The cohort shares a spawn step, and the cohort-resolution path in
`processStep` picks the fan-in target from whichever sibling completes *last*, so branches routing to different
fan-in nodes would make the convergence node depend on completion order (nondeterministic). The check walks a
local fan-out-to-fan-in mapping: `validateLineage` records each fan-out source's fan-in as branches pop the
lineage frame, and a second branch popping the same source to a *different* fan-in is the violation. (That map
is validation-only and discarded; the engine recomputes the same mapping for routing via `internal/faninmap`.)
(Divergent
non-fan-in *immediate* targets that still converge on one fan-in are fine - each sibling spawns its own next step;
only the shared fan-in target must be single-valued.)

**A fan-in arrival must have a cohort to arrive at, and this is enforced at BOTH layers.** `cohortSpawnID` is the
dispatched step's `lineage_id`, which is `0` for any step *outside* a cohort - so a trunk step routed into a fan-in
node bumps `cohort_arrivals` on `step_id=0` (zero rows, no error) and then SELECTs that step: `sql.ErrNoRows`, which
aborts the transition transaction. The `processStep` recovery defer then rewinds the just-`completed` step to
`pending`, it re-dispatches, and **the task re-runs - side effects and all - in an unbounded hot loop** that hammers
the database and never advances the flow (measured: 2,735 failed transitions in 2 seconds).

- **`validateLineage` rejects the edges that produce it.** Its `switch` tests `g.fanInNodes[tr.To]` **before**
  `tr.WithGoto, tr.OnError, tr.Switch` - **the order is load-bearing**. The engine treats *any* transition into a
  fan-in node as a cohort arrival and does not care which kind of edge carried it, so testing the goto/onError/switch
  arm first (as it once did) skipped the frame-pop check for precisely the edge kinds that can reach a fan-in from
  outside a cohort. A goto into the fan-in from *inside* a cohort stays legal - that branch has a frame to pop.
- **`processStep` guards it at runtime** (`fanInArrivals > 0 && cohortSpawnID == 0` -> `failAndReturn`). Not
  redundant with the validator: a graph frozen onto a flow *before* the validator fix is replayed from the flow row
  on every dispatch and **never re-validated**, so the guard is all that stands between such a flow and the hot loop.
  Failing the step is the honest outcome - the flow terminates with a clear error instead of looping forever.

Pinned by `TestGraph_ValidateFanInFromOutsideCohort` and `TestFanInNoCohort_FailsInsteadOfHotLooping`.

### State Across Subgraphs

**Subgraph is a function call.** The signature is `flow.Subgraph(url string, in any, out any) (yield bool, err
error)`. Only the explicit `in` passed in crosses into the child as its initial state; only the explicit `out`
target (the child's `final_state`) crosses back. The parent's state and accumulated changes do NOT auto-cross either
direction. `in` is any JSON-marshalable value (a struct or a `map[string]any`), normalized to a `State` via
`workflow.NewState` (nil → empty state); `out` is a pointer (a `*struct` or `*map[string]any`) the child's
`final_state` is unmarshaled into by JSON tag (`State.Parse`), or nil to ignore the result. A typed struct on either
side gives field-level type safety without manual `map[string]any` casts.

The engine has no single-task front door: a bare task is only ever a node in a graph, never an independently
invocable child. To run one unit of work in isolation, declare a one-node workflow and `Subgraph` it. ("Subflow" is
the umbrella term for any child flow and is the name of the typed host client, not an engine primitive.)

**Into the child:** `SubgraphRequested` passes `subgraphInput` (the `toStateMap`-normalized `in`) directly to
`createSubgraphFlow` as the child's initial state (nil normalized to `{}`). No merge with parent state. A caller
wanting the parent's full state passes `flow.Snapshot()` as `in` - explicit opt-in.

**Back to the parent:** `completeSurgraphFlow` writes the child's `final_state` to the surgraph step's
`subgraph_result` column, sets `subgraph_done=1`, and re-dispatches the parent task. On re-entry `flow.Subgraph`
unmarshals that `final_state` into the caller's `out` (yield=false), and the task reads the fields it wants. The
child output is **not** merged into the parent's `changes`.

**Failure back to the parent (a subgraph child fails exactly like a top-level flow, never eagerly).** When a
task inside a subgraph child errors with no `onError`, the child's failure is surfaced to the parent's
`flow.Subgraph` call as the `err` return (carried in `subgraph_error`, re-dispatching the parked caller step -
`deliverFlowFailureToParent`). The child still `signalStop`s its own failure
(a subgraph-child key is a legal read-only `Await`/`Snapshot` target), so a blocked `Await(childKey)` wakes on
the signal rather than waiting for the detector to read the row - **both** child-settle paths signal: `failStep`'s
subgraph-child branch (a failing last-arriver) and the transition path's cohort-fail branch in `processStep` (a
completing last-arriver resolving a cohort with failures), each exactly as the top-level path does. The load-bearing
rule is *when*: the child flow fails only **when it actually fails as a flow** - which, for a child with an
internal fan-out, means **after its cohort fully resolves**, running the *same* `cohort_failures` accounting a
top-level flow runs (`failStep` for a failing last-arriver, the `processStep` cohort-arrival path for a
completing last-arriver; both call `deliverFlowFailureToParent` when `failFlow` becomes true). It is **not** an
eager terminalization on the first branch error. Failing the child eagerly (an earlier `failStep` short-circuit
straight into `deliverSubgraphError`, bypassing cohort accounting) stranded the child's *other* live branches and
any subgraph descendants they had parked on: every tree walk skips a terminal flow (`Resume`'s down-walk descends
only `interrupted` children; `Cancel`/`allSubgraphFlows` stop at terminal nodes; the parked-caller wedge sweep
sees a *terminal* caller step, not `running`+`parkedSubgraph`), so the stranded sub-tree had no path out but
`Delete`. Deferring the child's failure to cohort resolution means every branch has settled (completed or failed)
before the child terminalizes, so there is no live sibling to strand - and a sibling parked on a grandchild that
*interrupts* propagates up normally, so the whole tree parks `interrupted` and a root `Resume` still threads down
to it (rather than the grandchild's approval being silently cancelled). `deliverSubgraphError` remains, now used
**only** by the wedge sweep (a wedged caller whose child already went terminal); the live failStep path no longer
calls it. Defense in depth for any residual orphan (e.g. the Cancel-vs-spawn race) is
`recoverOrphanedSubgraphChildren` (see "Background Recovery"). `fixtures`/`engine`
`TestSubgraphCohortFail_NoStrandOnBranchFailure` pins the child staying `running` after one branch failed while a
sibling is parked on a live grandchild, then converging to a clean terminal tree with the branch error surfaced
to the root. `fixtures/subgrapherrorwaitflow_test.go` pins the `Await(childKey)` wake: an `Await` registered on a
still-running child returns promptly (well under the poll interval) when the child fails, proving the signal, not
the backstop, woke it. `fixtures/subgraphcohortfailwaitflow_test.go` pins the same wake for the other settle
path (a completing last-arriver resolving a failed cohort).

### Surgraph Step Identification

Each subgraph flow's row stores `surgraph_flow_id` *and* `surgraph_step_id` - the PK of the parked surgraph step
it belongs to. `completeSurgraphFlow` (and the interrupt/resume chain walks) look the surgraph step up by primary
key, so they can never match a sibling at the same `(flow_id, step_depth)`. This matters for: (1) a fan-in race
where a non-subgraph sibling at the same depth is momentarily `running`; (2) parallel subgraphs at one depth, each
parked at `parked=1`. The PK lookup keeps each child flow bound to the step that launched it. (An earlier design
also stored the caller's `surgraph_step_depth` and matched on depth; that was ambiguous across parallel callers at
one depth and was removed - `surgraph_step_id` is the sole, precise link.)

### Denormalized root pointer (`root_flow_id`)

Every flow carries `root_flow_id`, the `flow_id` of the root of its subgraph tree, so a whole tree (a root plus
all its subgraph descendants, recursively) is reachable in **one query** - `WHERE root_flow_id=?` - instead of a
recursive `surgraph_flow_id` walk that issues one round-trip per tree level. It is **write-once at creation,
immutable** thereafter:

- A top-level flow (`Create`/`Run`/`Continue`) is its **own** root: `root_flow_id = its own flow_id` (set in the
  same post-insert UPDATE that fixes `thread_id`/`step_id`). The own-id convention - rather than `0`-means-self -
  keeps the root's *own* row matching the membership scan with no `COALESCE`/`OR` predicate.
- A subgraph child **inherits** the parent's `root_flow_id` (`createSubgraphFlow` reads it alongside the inherited
  scheduling/budget and passes it into `createWithGraph`).
- A `Fork` clone is a fresh self-contained tree: the new root is its **own** root, and the fork's cloned subgraph
  descendants inherit the **new** root's id (`cloneOneFlow` stamps `cc.newRootFlowID`, set right after the root
  insert and before any descendant recurses). The fork does **not** inherit the origin's `root_flow_id`.

Single-shard by construction: subgraph flows have parent-shard affinity, so a whole tree lives on one shard and
`root_flow_id` never crosses shards. (If that affinity ever changed, tree scans would need cross-shard fan-out.)

**Membership, not structure.** `root_flow_id` answers *which flows are in this tree* (the set); it does **not**
encode *who is whose parent* (the structure). So it **augments**, never replaces, the `surgraph_flow_id`/
`surgraph_step_id` links: `root_flow_id` is the **membership index** that loads the tree's rows in one scan, and
the surgraph links - now read from those *in-memory* rows - are still what supplies the parent/caller/child
*structure* every walk follows.

**Where it is used.** All four tree walks fetch the whole tree in one `root_flow_id` scan
(`WHERE root_flow_id = (SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)`) and derive their result **in
memory** by following the loaded `surgraph_flow_id`/`surgraph_step_id` pointers - identical results to the former
level-by-level recursion, but a fixed number of round-trips regardless of nesting depth. The subquery resolves the
tree first, so each walk works whether its starting flow is the root or a mid-tree node:

- `allDescendantSubgraphFlows` (Delete cascade, `Fingerprint`) - BFS *down* from the given flow over the loaded
  rows (any status).
- `allSubgraphFlows` (Cancel's descendant set) - BFS *down* through **non-terminal** nodes only, matching the old
  walk, which also stopped descending at a terminal node. Mid-tree Cancel/Delete therefore keep their exact prior
  "descendants of *this* node" semantics, not "the whole tree."
- `surgraphChain` (the ordered *up*-walk: Cancel, Resume, Fork, interrupt propagation) - follows
  `surgraph_flow_id`/`surgraph_step_id` pointers from the flow up to the root, collecting each ancestor's caller
  step + token. One scan vs the former *two* queries per level.
- `interruptedSubgraphChain` (Resume's *down*-walk) - one tree scan plus **one** batched query for every flow's
  interrupted leaf (`status=interrupted ... ORDER BY flow_id, updated_at, step_id`, first row per flow taken in
  memory), then descends by `surgraph_step_id`. SQL still does the earliest-`updated_at` ordering, so there is no
  fragile Go-side timestamp comparison (the leaf is still the one Snapshot reports).

`Fork`'s own tree-discovery still uses `surgraphChain` + per-flow child queries while it recurses (it needs the
structure to clone, not just membership), and `deleteSubgraphFlowsRootedAt` stays step-scoped (`surgraph_step_id`).

`History` assembly rides the same principle with its own two inline `root_flow_id` queries rather than a named
walk (it needs a *caller-step*-keyed child map with each child's `workflow_url`/`workflow_name`, a shape the
`[]int` membership helpers don't supply): one query builds `surgraph_step_id -> latest child flow`
(`ORDER BY flow_id`, last wins = the child the caller used), a second loads every tree flow's steps
(`WHERE flow_id IN (SELECT ... root_flow_id ...)`), and the nested history is stitched **in memory**
(`assembleHistory` recurses caller-step -> child). This replaced a per-step `WHERE surgraph_step_id=?` lookup -
an N+1 over every step in the tree - so `History`/`HistoryMermaid` are now a fixed two round-trips regardless of
nesting depth. (`idx_dwarf_flows_surgraph_step` still backs the remaining single-row `surgraph_step_id=?` lookups
elsewhere - the retry-reap, the wedge sweep, and `Step`-navigation.)

**Consistency.** Denormalized + write-once-at-create is low-risk, but a creation path that forgot to set it (or set
it wrong on a fork descendant) would silently drop rows from tree scans - and, now that the structural walks ride
the same scan, mis-route a Cancel/Resume/Fork. `TestRootFlowID_*` (engine package, white-box) pins the three
population paths: top-level self-root, subgraph inheritance, and Fork self-root + non-inheritance; `Continue`
starting a fresh root. `fixtures/deepsubgraphflow_test.go` pins the walks themselves at depth 5: a leaf interrupt
propagating up to the root, Resume descending back to the leaf with state threaded through every level, and a
root Cancel tearing down the whole interrupted tree.

### Interrupt/Resume Propagation Across Subgraphs

**Interrupt propagation (up):** when a step inside a subgraph flow is interrupted, the engine uses `surgraphChain` to
walk to the root surgraph, collecting flow IDs and parked surgraph step IDs, then interrupts all flows and steps in
the chain with bulk `UPDATE ... WHERE flow_id IN (...)` / `WHERE step_id IN (...)`. This ensures the caller awaiting
the top-level flow sees `interrupted`.

**Resume propagation (both directions):** `Resume` walks up (`surgraphChain`) and down (`interruptedSubgraphChain`),
re-parks intermediate surgraph steps, records resume data on the leaf's `resume_data`, resets the leaf to `pending`,
transitions all flows to `running`, and rings the doorbell - all in one transaction. The down-walk picks each
flow's interrupted step by **earliest `updated_at`** (`step_id` tiebreak) - the same selection `Snapshot` uses, so
a Snapshot reports exactly the interrupt the next Resume resolves - and descends into the child subgraph spawned by
that step via **`surgraph_step_id`** (the caller step's PK), never by depth (which is ambiguous when parallel
subgraph callers share a depth).

**Fan-out interaction:** one sibling may interrupt while others continue. The flow is marked `interrupted` by the
first; others run to completion. `Resume` handles one interrupted sibling at a time; the flow returns to `running`
only when no interrupted steps remain at any level.

### Identity / baggage propagation

The opaque baggage set in `FlowOptions.Baggage` at `Create` (stored in the `baggage` column) is delivered on the
dispatch **context** to every `LoadGraph` and `ExecuteTask` call for the flow's lifetime, including dispatches
long after creation - the host reads it with `workflow.BaggageFrom(ctx)`. The engine never interprets it; a host
uses it to carry the original caller's identity (e.g. mint a fresh token inside its `ExecuteTask`). It is
**inherited** by subgraph flows (`createSubgraphFlow` copies the parent's `baggage`) and by `Continue` (the next
turn reads the prior flow's `baggage` column and carries it forward - `Continue` takes no options, so the thread's
baggage always flows on), so a multi-turn conversation keeps the caller's identity across turns. A turn wanting
narrower context scrubs it in an entry adapter task, or starts a fresh flow with `Create` (with
`FlowOptions.ThreadKey` to stay in the thread) and its own `FlowOptions.Baggage`.

**Delivery is context, authoring is `FlowOptions`.** Baggage is *set* explicitly and visibly (a typed
`FlowOptions` field on `Create`/`Run`) but *read* ambiently (off ctx), so the value the engine never
interprets is not a parameter on every callback and task handler. The engine injects it into the ctx it hands the
callbacks (in `processStep` for the per-step executor, and at the create-time `LoadGraph` call); the
`ContextWithBaggage`/`BaggageFrom` helpers live in the `workflow` package so task-defining code reads baggage
without importing `engine`; both are typed on `workflow.State` (delivery and read), while the *set* value
(`FlowOptions.Baggage`) stays an `any` the host fills with a struct or map. The create-time injection round-trips
the value through JSON (`json.Marshal` then `workflow.NewState`) so the loader sees the same decoded shape every
dispatch will (a `map[string]any`/struct decodes to a `State` with numbers as float64); the dispatch and inherit
paths read the `baggage` column the same way, via `workflow.NewState`.

### Keys are capabilities, not authorization

A flow/step key (`{shard}-{id}-{token}`) is an **unguessable bearer capability**, not an authorization
decision - the host-facing contract is in `doc.go`; the key *format* and the token-entropy decision are in
`internal/keys/CLAUDE.md`; this section is the engine-side *enforcement* and posture. The engine
holds no authN/authZ, no rate limiting, and no notion of caller identity: its only vantage is the flow
reference and the task URL. Ownership and tenancy - the axes any real access-control check turns on - are
structurally invisible to it, the same reason it owns no backpressure (see "Backpressure is the task's or
host's job"). So the token cannot *be* the access control; it can only make the reference **unforgeable**,
and authorization must live in the host, which has the principal (typically via the baggage it set at
`Create`).

**What the token buys, precisely.** `flow_id` is a per-shard sequential integer - an enumeration oracle on
its own. The random `flow_token` turns "an integer anyone can type" into "a handle you must have been
given," so a host authz bug alone is not sufficient to walk the table (you would also need each flow's
token): two independent failures, not one. It also preserves the no-existence-oracle property (every
operation on an unknown-or-mismatched key returns a uniform not-found - verified) and enables opaque
capability-URL patterns (a "resume your approval" link). What it is **not** is access control: a leaked,
logged, or shared key is a full write capability for that one flow. The amplifier is therefore `List` - the only
operation that returns keys wholesale (a separate concern) - not the token itself. (There is no `Search` operation;
free-text search is a field on `workflow.Query` that `List` consumes.)

**Token entropy (64-bit) and the every-gate-is-`flow_id`+`flow_token` invariant that makes it adequate** live in
`internal/keys/CLAUDE.md`. Load-bearing for this section: the token is never a standalone lookup, so a leaked key
is a full write capability for exactly one flow and nothing more.

**Related engine-side hardening (all verified, none a substitute for host authz):** uniform not-found on
key mismatch (no existence oracle); telemetry carries only the token-free correlation id and there is no
correlation-id→key lookup (see "Tracing"); subgraph-child keys are read-only for lifecycle mutations (see
"Subgraph keys are read-only"). The engine cannot go further - it never sees the principal.

### Await

`Await` blocks until a flow stops (no longer `created`/`pending`/`running`); it returns on `completed`/`failed`/
`cancelled`/`interrupted`. Its shape is **read, park, read - at most twice, and never in a loop**: snapshot and
return if the flow has already stopped; otherwise park on the **latch board** (`internal/latch`, held as
`e.latches`) until it settles, then snapshot once more to build the outcome.

**Each of those two reads earns its place, and the second is not a re-check.**

- **The first** is what lets an already-settled flow answer AT ONCE. Parking instead would be *correct* - the
  detector reports the current state of every parked key, so a stop that landed before the caller arrived is
  found anyway - but it would be found on the sweep's cadence, turning every `Await` on a finished flow into a
  wait of up to one full `latchSweepInterval`. Measured: removing this read **doubled** the `engine` package's
  test time (≈8-11s → ≈17-20s), while `Run` was unaffected (a `Run` caller parks long before its flow stops).
  Keep it.
- **The second** runs only after a release, and a release is a promise that the row has **settled** - reached a
  stop, or stopped resolving. That invariant is what removed the loop this once needed, and `signalStop`
  **enforces** it by dropping any non-stopped status rather than delivering it. Do not wake a caller for a
  status a flow is merely passing through: with no loop, the caller would hand a running flow back as an
  outcome. (Nothing needs such a wake anyway - nobody is parked on an `interrupted` flow, since that is a stop,
  and nobody can be parked on a key `Fork` has not returned yet.)

**Registering AFTER the first read is safe only because the board is polled.** An event-based wake must be
armed before the event fires, so an event-based `await` would have to register *first* and read second. The
detector asks for the current state of every parked key instead, so a stop committing between the read and the
park is reported by the next sweep. Registration order is a latency question, not a correctness one - do not
"restore" an arm-then-read ordering.

**Two things can end a park, and they are not interchangeable:**

- **`signalStop` → `Board.Release`** - instant, in-memory, for a stop THIS replica just committed.
- **The detector** (`latchLoop` → `Board.Sweep` → `resolveStoppedFlows`, `latchSweepInterval` **50ms**) - one
  indexed `IN` lookup per shard holding a parked key, and *nothing at all* when nobody is awaiting. This is the
  only path that can see a stop made by a **peer**, so it is the primary wake for the cross-replica case, not a
  backstop. Its `IN` lookup scales with concurrent **awaiters**, which is what lets the cadence stay tight
  without watching how fast the engine is running - but the recent-stop pre-scan beside it scales with the
  **stop rate** instead, so the pass is no longer throughput-independent. See the pre-scan bullet below for
  the axis swap and where it inverts.

**A parked caller runs no query of its own, and that is a property to protect.** `await` parks for the whole
remaining budget in ONE `Latch` call; the detector's per-shard lookup is the only read on its behalf. Do not
re-introduce a per-caller re-read on a timer "as a backstop": it would make the engine's query rate grow with
the number of blocked callers, and it can only find what the sweep - reading the same rows on a far tighter
cadence - has already found. The thing such a timer would guard is a bug in `internal/latch`'s in-memory
bookkeeping, which is unit-tested directly and cheaper to keep correct than to poll around.

**A ctx with no deadline gets `awaitDefaultBudget` (15m), and nothing narrower.** The engine has to bound an
unbounded wait somehow - a flow that never stops otherwise blocks its caller for the life of the process - but
the budget applies *only* where the caller expressed nothing; an explicit deadline is honored as given, however
long. It is set high on purpose: a parked caller is free (previous paragraph), so a shorter budget would buy
nothing and would cut short waits callers legitimately asked for. Note the consequence at the `Await` boundary:
when the budget (rather than the caller's ctx) expires, **`ctx.Err()` is nil**, so `Await` must name the timeout
itself instead of tracing that nil - tracing it returns a nil error alongside a non-stopped outcome.

**Shutdown closes the board**, which wakes every parked caller with `latch.ErrClosed` and `await` turns into a
503. `drainRuntime` stops the detector *before* closing the board (a `Sweep` in flight is not interrupted by
`Close`) and closes it before `e.db.Close()`, so no wake path touches a closing database. A caller already
holding a released status keeps it - the closure travels the same one-slot channel as a status and cannot
displace one - so a flow that stopped microseconds before shutdown still returns its outcome. Pinned by
`fixtures/awaitshutdownflow_test.go`.

**`resolveStoppedFlows` is the board's status resolver, and five of its shapes are load-bearing** (`latch.go`):

- **One query per SHARD, not per key.** Keys carry their shard, so they are grouped and each shard is asked once;
  a pass costs O(shards), not O(awaiters).
- **A recent-stop PRE-SCAN settles keys without naming them, and it is an optimization ONLY.** It reads the
  flows that stopped within **`2 x latchSweepInterval`** (100ms); every key it matches - by token, the same
  rule as the lookup - is settled and DROPPED from the `IN` list. It never resolves a key as absent (a token
  mismatch goes to the lookup, which owns that verdict), so no width of the window can cost a wake, and a
  scan error is recorded but not fatal. Pinned by
  `TestLatchResolve_RecentStopScanIsOnlyAnOptimization`, which resolves a fresh stop and one backdated past
  the window in the same pass - **with both `updated_at`s stated outright**, because a 100ms window is
  shorter than the test's own setup and a "fresh" row left to real time drifts out of the scan, leaving the
  test passing on the lookup alone and covering nothing.

  **TWO SWEEPS IS THE REQUIREMENT, and widening it is pure cost.** A key stays on the board until some pass
  settles it, so the window only has to span the gap between consecutive scans of one shard - one interval of
  coverage, one of slack for a pass that ran late. Rows scanned grow linearly with it, so a wider window
  (1s was the first cut) buys nothing the next pass would not have caught. No clock term needs padding
  against: `updated_at` is written with `NOW_UTC()` and compared against `NOW_UTC()` on the same shard, so
  the two cancel exactly - the same argument that removes clock skew from the refiller's `ageMs`.

  **It buys on the OPPOSITE axis from the lookup it shrinks.** Its rows are `stop rate x window`; the lookup's
  binds are the awaiter count. So it wins where awaiters are many and flows are long (a `Run`-heavy host) and
  **loses** on a high-throughput engine with few awaiters, where it scans 20x a second per shard to settle
  almost nothing. Only the no-parked-key early-out bounds that. If it is ever measured biting, gate the
  pre-scan on the shard's key count or shorten the window toward the sweep interval - do not widen it.
- **The token is compared against the row.** A flow key is a capability, so resolving on `flow_id` alone would
  answer a caller holding a forged or stale token. Pinned by `TestLatchResolve_ReportsStoppedFlowsOnly`.
- **A key that names NO row settles too**, as `flowUnresolved` - the woken caller's own read turns that into
  the not-found. A row that does not exist can never change status, so without this a parked caller would wait
  out its entire budget for an answer that was already decided. Absence may only be inferred from a chunk whose
  query **fully succeeded**: on a failed chunk every row is missing, and calling those keys unresolved would
  404 live flows on a transient blip.
- **A shard error is not a failed pass.** Whatever the other shards resolved is returned *alongside* the error
  (`OnEach` is all-or-nothing, so its callback never propagates one), and the unresolved keys are simply asked
  about again next tick.

Two smaller rules at the same site: the `IN` list is **chunked at 512** (SQL Server's 2,100-parameter ceiling is
the tightest bound) and **padded up to one of ten bucket arities** by repeating an id, because arity is part of
the statement text and an unbucketed list mints a distinct prepared statement per awaiter count. The status
filter is applied **in Go**, not in the `WHERE` clause - the rows are already in hand, and binding a status is
the filtered-index landmine.

**Cross-replica `Await`.** A flow created on one replica but completed on another is found by the
**detector**, and only by it: the stop is committed in the shared database, nothing is sent, and the sweep is
what reads it. `signalStop` reaches this replica's own board and stops there. Non-terminal (`running`)
transitions take the same local-only path.

**Nothing marks a flow as awaited, and nothing may start.** `Await` is a pure reader: it writes no row, sets
no flag, and is invisible to every stop site - so `Await`/`Poll`/`Run` put no write on their synchronous
request path at all. A flag telling the stop sites whether anyone is waiting only makes sense as a gate on
sending something, and nothing is sent; adding one back would buy a stop site the right to skip a wake that
is already local and free, at the price of a write per await.

### Write-ordering & lock contention

- **Write-first transactions** - `advanceFlow` does an `UPDATE` as the first operation to immediately acquire a write
  lock. On MySQL/Postgres this serializes concurrent workers (like `SELECT ... FOR UPDATE`). On SQLite with
  `cache=shared`, starting with a write avoids the deadlock where two read-first deferred transactions both hold
  SHARED locks and neither can upgrade. **The write-first `UPDATE dwarf_flows` flips the non-indexed `touch` column
  (`touch=1-touch`), not `updated_at`.** The lock acquisition is identical, but `updated_at` sits in
  `idx_dwarf_flows_status (status, updated_at)`, so bumping it on every step transition moved a running flow's index
  entry once per step (the flow-row is written twice per transition tx - the open lock-grab and the closing `step_id`
  advance - so it was two moves). Flipping `touch` (indexed by nothing) takes the lock, keeps `updated_at` = the
  flow's last genuine *status* transition, and confines `idx_dwarf_flows_status` churn to actual status changes; on
  Postgres the grab becomes HOT-eligible. `touch=1-touch` (not a `SET col=col` self-assign) is deliberate: it always
  changes the value, so the terminal-status `RowsAffected()==0` guard on the open UPDATE
  (`WHERE ... status NOT IN (terminal)`) still fires correctly on MySQL, which counts *changed* rows. Every
  `UPDATE dwarf_flows` flips `touch`; the status-transition writes flip it alongside `updated_at=NOW_UTC()`, the
  intra-flow-progress writes flip only it. (This applies to `dwarf_flows` only - `dwarf_steps` has no such per-step
  churn: a step row is short-lived and its `updated_at` moves track its own `pending→running→terminal` transitions,
  which are genuine and unavoidable.) **Every flow-terminating transaction must be write-first for the same
  reason**, and the failure mode is worse than a transient error: the terminal step is marked `completed` by
  `processStep` *before* the disposition runs, so if the disposition's `Transact` exhausts its retries and errors,
  lease recovery (which only resets `running` rows) can't re-dispatch the now-`completed` step - the flow strands
  `running` with every step terminal (a permanent orphan). `failStep`, the fan-in transaction, and `completeFlow` all
  write first. A high-volume soak (`fixtures/soakflow_test.go`) and `fixtures/completionraceflow_test.go` reproduce
  the wedge without the fix. This write-first rule governs the flow-*advancing*/*terminating* transactions only; the
  lifecycle mutations (`Resume`/`Cancel`/`failStep`/`Delete`) run the **opposite** (steps-first) order on purpose, so
  the two disciplines cross on row-locking engines - see "Transactions" for why that crossing is tolerated (retry-
  recovered) rather than reconciled, and do not "fix" one side into matching the other.
- **Busy timeout** - `sequel` applies `_pragma=busy_timeout(1000)` to SQLite DSNs without one, so concurrent workers
  hitting a write lock wait up to 1s instead of failing immediately with `SQLITE_BUSY`. Essential during fan-out.
- **Lock contention recovery** - `processStep` defers a check: on a lock-contention error
  (`sequel.IsLockContentionError`), it resets the step it had leased (`running` -> `pending`, `lease_expires=NOW`),
  so the step is claimable again. The reset is load-bearing on its own: `recoverExpiredLeases` only recovers running
  steps whose lease has *already* expired, and a freshly leased step holds a minutes-long lease, so without the
  rewind the step (and its fan-in) would stall until the lease lapsed. It no longer needs a paired re-poll - the
  step goes back to `pending` and the next cycle selects it. Guarded by `WHERE status='running'`, so only the
  leased-and-uncommitted case is rewound.

### Database Choice and Configuration

Choosing a SQL dialect (Postgres/MySQL/SQL Server/SQLite tradeoffs under concurrent write load), the shard-count
guidance table, the shard-per-server production topology, and the per-server connection-budget reasoning all live in
`internal/database/CLAUDE.md`. The engine-side concern is the connection **pool sizing** — the *formula*, which is
engine policy — below.

**Connection pool sizing - fact-derived per shard (`poolsize.go`).** The operator provides facts on
`ShardSpec` and the engine owns the measured constants (cloud benchmark campaign, `docs/benchmark-cloud.md`):

- **`ShardSpec.VirtualCPUs` drives each shard's pool** at the measured knee, `idle = open/2` (warm core).
  The ratio is **12x at 32 vCPUs or more, 6x below** (`connsPerVCPUFor`), and STABILITY places that
  threshold, not throughput. 12x is the throughput knee from 16 vCPUs up (+11.7%/+5.0%/+14.0% at 16/32/64),
  but counted over an open-loop campaign it also introduces a collapse mode. Arms that entered it, per
  cell: 8 vCPU **1/13 vs 2/11**, 16 vCPU **0/13 vs 2/9**, 32 vCPU **0/14 vs 0/9**, 64 vCPU **0/7 vs 1/14**
  (6x vs 12x), totalling **1/47 (2%) against 5/43 (12%)**. The collapse is not a slowdown - active backends
  spike (177 vs a normal 140), the WAL share of their waits falls ~69% -> ~14%, CPU:running rises
  ~6% -> ~80%, and throughput drops ~15x (8,629 -> 486 steps/s) until it recovers on its own. Roughly 10%
  of peak throughput does not buy that risk. Below 32 the older cliff argument also still holds: a 1-vCPU
  instance loses 55% past its knee and a 4-vCPU one 35%.

  ⚠️ **The threshold is NOT "12x is free above 32", and the 64-vCPU cell is why.** 12x collapses there at
  1/14 (7%) - about the rate that placed the threshold at 32 to begin with - so the "free" claim rests on
  32's 0/9 alone while the size ABOVE it shows a collapse, and the totals say 12x is ~6x more likely to
  collapse at every size measured. It stands because the gain is 5-14% of peak against a rare,
  self-recovering mode, not because large instances were shown immune. **Do not place a stability
  threshold from any count under ~15 arms per cell** - every cell above is under it, this threshold moved
  8 -> 32 across one campaign as more arms landed, and the 64-vCPU collapse arrived last, in a six-arm
  confirmation run made after 32 had already shipped. Moving it again needs new ARMS, not a re-reading of
  these. `docs/benchmark-cloud.md` publishes the same table; keep the two in step.
- **The fleet is known BEFORE any worker dispatches, so pools are sized right the first time - there is no
  async grace window.** Reading the registry needs open connections, so `Startup` opens the shards at a small
  **bootstrap** pool (`startupBootstrapConns`, 4 - enough to register the row, probe the RTT and read the fleet,
  which is all any pre-dispatch work needs), then builds the Sonars, joins every shard in parallel, and only
  then resizes each pool to its derived share and computes `workersDispatch` - all before `initRuntime` spawns
  a worker. So the replica takes on work with pools already correct for the fleet it is in, with no
  partial-count over-connect to defend against. The ordering rationale - announce, let peers shrink, then grow -
  is in §"Peer discovery"; what matters here is that the sizing happens after it and before dispatch.

  **The bootstrap pool is deliberately tiny** because a cold-starting fleet's only connections during that window
  are these (no worker dispatches yet, and lazy fill means the ceiling barely materializes - roughly one
  connection per shard, since everything in the window runs sequentially on one goroutine per shard), so even N
  replicas starting together stay far under any server's `max_connections`. It is not an over-connect guard - that
  is "the fleet is known before dispatch" - just enough to bootstrap the read.

  **`lastAppliedR` starts EMPTY (`"nothing derived yet"` per shard), and Startup records each shard's count**
  after sizing its pool directly, so the first reconcile `recomputePools` dedupes against the values the pools
  actually hold. `shardPool` clamps `replicas = max(1, replicas)`, so an absent count never reaches the
  arithmetic.

  None of this depends on any transport: the registry is the only thing anyone reads (see "Peer discovery"
  below).

- **The derived budget is per DATABASE, so the observed replica count R splits it**: each replica
  takes `max(2, open/R)`. The knee belongs to the shard's server, not to one replica - R replicas
  each holding the full budget overshoot it R times over. R is never declared: the engine reads it
  from the shared `dwarf_peers` registry (see "Peer discovery"), and every fleet change pushes
  recomputed pools to the open shards immediately. The `SetMaxOpenConns` override is a per-replica
  exact number and is never divided.

**Peer discovery (`peers.go` + `internal/peers`).** Everything the engine knows about its fleet comes from
the shared **`dwarf_peers`** registry, NOT from the host transport, and it comes **per shard**: one
`peers.Sonar` per open shard owns this replica's row in that shard's registry, reads the whole registry on
its own cadence, and publishes what the reading implies. The mechanism - the two timestamps, the two windows
and their opposite postures, the blindness rules, the id-list prune - lives in `internal/peers/CLAUDE.md`.
What belongs here is the engine's side of it.

**Two numbers, from one reading, with opposite risk profiles.** `replicasOn(shard)` divides that shard's
connection **pool**, because the budget belongs to the shard's database and N replicas each holding the whole
budget would overshoot it N times over. `partitionOn(shard)` divides that shard's **work** - the residue
class of `step_id` each replica selects - across the replicas that demonstrably serve it (see §"Candidate
de-duplication"). Over-counting the first over-sizes pools and can collapse a database; over-counting the
second hands a residue class to a replica that never selects it and strands the work in it. They are
therefore never the same number, and the second excludes an await-only replica (`SetWorkers(0)`) that the
first counts.

**PER SHARD is the point, and a fleet-global count cannot express it.** A peer whose piston wedges on shard 3
drops out of shard 3's work divisor and stays in every other shard's; a peer whose beats to shard 3 fail
mis-sizes only shard 3's pool. It also retires cross-shard comparison entirely - every timestamp in the
registry is stamped by the shard holding it, so ages are comparable within a shard and timestamps are
comparable nowhere. Consequently **every derivation that consumes a replica count does so per shard**:
`recomputePools`, `recomputeWorkerCeiling` (worst shard wins, since a storm drains through the tightest
pool) and `recomputeRefillIntervals` (the period is measured against the pool that shard drains through).
`lastAppliedR` is a per-shard map under `poolsLock` for the same reason: one shard's change must not re-push
another's unchanged sizes, and one shard's stillness must not mask a change elsewhere.

**The two halves are wired to each other through the engine**, so neither package imports the other:
`piston.SetPartitionFunc(func() { return e.partitionOn(idx) })` and `sonar.SetEvidence(p.Liveness)`. Both are
pure reads of live state, so neither captures anything that can go stale. The piston publishes nothing about
this replica anywhere - it only reports whether it is turning - which is what keeps how often that fact
reaches the registry independent of how long a cycle takes.

**The engine PULLS; nothing calls back.** `runReconcileLoop` ticks at an eighth of the Sonars' read cadence
and calls `recomputePools`, which early-returns when no shard's count moved. It is a reconcile loop rather than a
change notification, and the distinction is load-bearing: its invariant is "the applied pool sizes match the
currently observed fleet", a job that exists whether or not anything announces a change, and it absorbs
drift no notification could express - an override landing, a push that errored, a count moving while a
previous push was in flight. An edge would have to be fired from every path that can move a published value
(a confirmed fall, a recovery from blindness, a registration repair, a prune); missing one would leave the
derived sizes stale forever with no backstop, and it would run pool policy on a Sonar's goroutine, coupling
one shard's beat to another shard's slow push. **The tick must stay a small fraction of the read cadence,
and half is not small enough** - the apply spends part of the same budget `Join`'s two-cadence wait is
already spending (detection alone costs a cadence plus a pass), so at half a cadence a loaded rollout
priced a four-replica fleet at 56 connections against a 52 bound, two survivors still holding the pre-join
share while the joiner had already grown. Slower than the read cadence discards detection the reads have
already paid for; anything faster than an eighth is pure loss on a loop that early-returns.

**Startup announces before it consumes, and that ordering is the guarantee.** `buildSonars` then `joinFleet`
(parallel across shards), and only then are the pools sized. A joining replica sizes its own pool for the
fleet it is joining while its peers still hold pools sized for the fleet *without* it, so consuming
immediately would put the shard's server over budget by roughly one replica's share until they caught up.
`Join` blocks two read cadences - a peer's read may have begun just before this replica's row was committed,
so only the reading after it must see the row - which also gives a simultaneously-starting fleet's rows time
to land, so what follows is a settled roster rather than a partial one. A partial one *under*-counts, which
over-sizes pools. **`Join`'s own wait covers peers DETECTING the row, and `joinFleet` adds a grace for them
to APPLY what they read** - a peer shrinks its pool on its reconcile tick, up to a tick after the reading
that told it to. Skipping that grace put a rollout over budget on every step (measured: a
budget+bootstrap bound failed 10 of 10 rollouts under `-race`).

**The grace SHRINKS the window and cannot close it.** A peer's apply is local to its process, so nothing
here can prove it happened, which leaves one reachable worst case at any grace: every survivor still on the
pre-join split - summing to the whole budget - while the joiner has grown into its post-join share. So the
fleet's guarantee is **budget + one post-join share**, not budget + bootstrap; measured peaks of 52 and
60 against a 48 budget, and pinned at that bound by
`TestPeerRollingRestart_FleetNeverExceedsTheShardBudget`, which still catches an ordering regression
because that prices the fleet at the R-1 split (64) instead.

What the wait buys is peers having STOPPED ACQUIRING beyond their new cap, not connections
already closed: lowering a pool's limit closes nothing, so any surplus drains as connections are returned.

**Shutdown drains the Sonars LAST**, after the pistons, because this replica must stay registered while its
workers are still executing steps - every one of them drains against pools each peer sized for a fleet that
still includes us. Only once their loops have returned is the last possible beat behind us, which is what
makes `leaveFleet`'s delete final: a beat only ever UPDATEs, so nothing can resurrect a row deleted after its
loop stopped.

**Everything fails open.** A shard with no Sonar (one that failed to build, or any lookup before Startup)
sizes for a solo replica - the pool-safe direction - and declines to partition. The Sonar's own uncertain
cases (solo dispatcher, self absent from the roster, an ordinal outside the divisor, a Sonar gone blind) do
the same, and `internal/piston` validates the pair a third time as its own advertised posture. Three guards,
deliberately: each package answers for what it knows.

**Under test the cadence is shortened** (`testPeerCadence`, `buildSonars`), because every engine pays
`Join`'s two cadences on the way up and a suite standing one up per test would spend most of its time there.
It must not be shortened too far: the blindness grace is TWO cadences, and a Sonar that reads as blind
declines to partition, so a grace inside ordinary goroutine-scheduling jitter disables partitioning at random
under a parallel suite - the same cliff a slow pass produces in production, reached through the test knob.

**`SetEngineID(id int64)` pins the identity, defaulting to random.** `engine_id` (a `BIGINT`, minted
`rand.Uint64()>>1` per instance in `NewEngine`) is the peer-registry PK and the flow/step provenance stamp. Random is
correct for the common case *including several engines in one process* (each `NewEngine` mints its own, so bundled
replicas - e.g. an integration test exercising cross-replica behavior - count as distinct peers automatically), and
its only cost is ghosts: a crash-restart mints a *new* id, orphaning the old row for the counting window, and a fast
crashloop *accumulates* orphans (each restart a fresh id) faster than 4x ages them out, ballooning R. `SetEngineID`
lets a host pin a value **stable across a replica's restarts** (derived from the deployment's own per-instance
identity - e.g. a StatefulSet pod name/ordinal, or `os.Hostname()`, which survives container restarts as a pod-level
property with no manifest change), so a restart re-upserts its one row and R never inflates. It is construction-time
only (the id is registered at Startup and baked into signal-echo suppression), rejects `id <= 0` (0 is the
pre-column/no-engine sentinel), and the host owns the contract that the id is **unique among concurrently-live
replicas sharing the databases** - a collision registers two live replicas as one (under-count -> over-size -> the
dangerous direction), so it is opt-in and a wrong stable id is worse than the random default. The multiple-engines-
per-process case (bundled host) deliberately does *not* opt in: it keeps the random default (correct distinct
count), and its test-teardown ghosts are handled by clean `Shutdown` (deletes the row) + fresh-DB-per-run, not by an
identity.

**Nothing announces a fleet change, and nothing needs to.** The Sonars have no nudge entry point,
deliberately: reading every `peers.Cadence` converges faster than a broadcast would, so a nudge could only
ever arrive after the reading that made it redundant.

Design posture: a **lookup, not a control loop** (the counts are exact and discrete and
independent of the actuation - a shrunk pool still beats), and they are **tuning** numbers (a wrong count
mis-sizes pools, corrupts nothing).

**Every APPLICATION of a pool size is serialized under `poolsLock`** - both writers: the derived
recompute (`recomputePools`, driven by the reconcile loop) and the live
override (`SetMaxOpenConns`). The `lastAppliedR` dedupe skips a no-op recompute but does **not order two live ones**:
two recounts microseconds apart during a rolling deploy each read a different R, and with nothing serializing
read-of-R through push, the **R=2 sizes can land AFTER the R=3 sizes** - every replica then holds a half-budget
pool against a fleet of three, over-connecting the shard's server, and it is *sticky* (the next
recompute sees R unchanged and skips). The override races the same way and worse: `recomputePools`
reads `maxOpenConns` and only *then* pushes, so a `SetMaxOpenConns` landing in that window has its
**pinned pools silently overwritten by derived ones** - the operator's explicit pin evaporates. With
the lock spanning read through push, whichever writer goes second sees a settled world (the override
applies last, or the recompute early-returns because the override is now set). Lock order:
`poolsLock` -> `shardsLock` (the counts are lock-free reads of the Sonars' published state, so they drop out of the order). Pinned by
`TestPoolSizing_ConcurrentRecomputeAppliesLatestR` and
`TestPoolSizing_ConcurrentRecomputeDoesNotClobberOverride` (both drive the interleaving with the
`slowPoolPush` seam rather than racing for it - staging the two counts through the registry itself and waiting for
each to be OBSERVED before the recompute that must read it; without the lock they measure 24 instead of 16, and a
pinned 7 turning into 24).
`engine_id` (random per process, fresh on restart) is the id a replica writes into `dwarf_peers`; it is
also **stamped on every flow/step INSERT** (creator) **and overwritten by the claim CAS** (claimer) - forensic
provenance there ("which replica created/ran this row"), deliberately unindexed.

**A note on tests sharing the registry (`peers.go` / `poolsizing_test.go`).** Because the count is now DB-backed, two
engines that share a test database (`NewEngineUnderTest` keyed by the same `t.Name()`, or `SetTestName` with a shared name) share one `dwarf_peers` and count
each other - which is exactly right for a genuine multi-replica test (`fixtures/crossreplicaawait_test.go`), and
exactly wrong for a single test that spins up several *independent* engines to assert their solo pool sizes. The
latter (`TestPoolSizing_DerivedWorkers`) uses `startSolo`, a unique per-engine test-DB key, so each reads R=1. The
sizing tests drive fleet size by writing fake peer rows (`addPeerRow`/`delPeerRow`, which wait for the Sonars to
observe the change) rather than through signals.
- **`VirtualCPUs = 0` (undeclared) assumes `defaultVirtualCPUs = 2`** (pool 12). The vCPU count is a fact
  off the spec sheet - something an operator KNOWS - so this covers the zero-config case, not a guess
  anyone should rely on. It is bounded, not reckless: 2 is the FLOOR of every current-gen AWS RDS class,
  and on the smaller machines that do exist (Cloud SQL's 1-vCPU `db-custom-1-*`) a pool of 12 still sits
  under the measured knee - that tier peaked at M=16 and only collapsed from M=32 up. **Do NOT raise the
  assumption to 4**: that yields a pool of 24, which lands in the unmeasured gap between the 1-vCPU
  tier's peak (M=16) and its collapse (M=32). The asymmetry is the whole argument - under-connecting
  costs throughput but stays healthy (excess load queues client-side, measured benign), over-connecting
  collapses the database. Consequence to state plainly: an 8-vCPU database left undeclared runs at a
  fraction of its capacity, and **nothing detects an over-declared `VirtualCPUs`** - the declared facts
  are trusted, exactly like the shard set.
- **`SetMaxOpenConns` is the expert override**: pins every shard's pool to exactly n (idle = open = n),
  replacing derivation. Exists for pool-size benchmarking sweeps and externally-constrained budgets.
- Heterogeneous fleets get heterogeneous pools: sizing is per shard (`database.ShardConfig`), resolved at
  `Startup` from each spec.
**Worker sizing: the lease-margin ceiling, with a grow-on-demand pool (`poolsize.go`, `engine.go`).**
The worker count is split into two numbers, because they answer different questions:

- **`workersDispatch` (resident, eagerly spawned; also the candidate cache's size)** =
  `max(64, 8 x sum(per-shard open))` (`workersPerConnBudget`). Dispatch is DATABASE-bound, so the
  connection budget is what sizes it; the 8 is a generous `T/db` allowance (measured ~3 for no-op tasks).
  **The cache - and therefore the refiller's selection, which fills at most the cache capacity of candidates
  and fetches per fairness key capped by it (see "Selection") - must never be sized from the maximum below**:
  a worker parked in a long `ExecuteTask` holds no connection and dispatches nothing, so a ceiling-sized cache
  would select far more of the backlog than the replica can ever claim.
  **The CACHE follows the pool split; the RESIDENT WORKER COUNT does not, and that asymmetry is deliberate.**
  `workersDispatch` is computed once in `Startup`, from the pools already sized for the R **discovered there** (for
  a solo start that is R=1, so `sum(per-shard open)` is the full per-database budget). Every later fleet change
  re-divides the pools by R (`recomputePools`), which **also re-derives the dispatch count from the post-split
  budget and `cache.Resize`s to it** - the same "every path that changes a pool must re-derive what depends on it"
  rule the worker ceiling obeys - but does **not** re-spawn the resident worker set.
  - The **cache** had to follow, because it was the half that costs throughput. The refiller selects up to the
    cache's capacity of candidates and wholesale-replaces it, so a replica in a fleet of 8 holding a cache
    sized for the whole budget is handed ~6x more candidates than it can ever claim: stale hints whose claim CAS
    loses to a peer, and wasted round-trips, exactly when the fleet is busiest. It is the same "never size the
    cache from more than the replica can claim" rule that keeps it away from the worker *ceiling*, reached
    through a different door. Pinned by `TestPoolSizing_CacheFollowsTheReplicaSplit` (768 -> 128 on an 8-vCPU
    shard at R=8). `Cache.Resize` trims the tail; a trimmed candidate stays `pending` and is re-selected, exactly
    as one pushed past the bound by an `Offer` head-insert already is.
  - The **resident worker set** is deliberately left over-provisioned. The surplus workers merely queue on the
    pool - bounded, and *non-compounding* now that the growth trigger counts workers OFFSITE rather than
    saturation - so the entire prize for shrinking it is goroutine stacks (~3MB at 384 workers). Buying that
    needs a worker-retirement protocol (a surplus counter, a retirement check on the hot loop, resize
    serialization, and an interaction with the pool's drain ordering). That is a control
    protocol added to the worker lifecycle for a memory saving, which is the trade the removed rate valve lost.
    If a large-R deployment ever shows the goroutine count actually hurting, the design to build is a **surplus
    counter** (retire the first N workers to reach a safe point), never per-worker indices - indices are what
    make a shrink-then-grow race produce duplicate ids.
- **`workers` (the MAXIMUM the pool may grow to)** = `workerCeiling` = `M x margin / txTime x safety`,
  the largest pool that keeps a **synchronized completion storm** inside the crash-recovery lease margin.
  The storm: every in-flight task blocks on one downstream (an LLM provider outage) and is released at
  once, so N completions contend for M connections, draining at `M/txTime`; a completion that out-waits
  its remaining margin is fenced after a peer re-claims - correct, but the task **re-executes**,
  duplicating the most expensive work at the worst moment. `txTime = 7 x RTT + ~3ms` (the post-task phase
  is 7 round trips: the standalone completed-UPDATE plus the transition tx's lock-grab, successor INSERT,
  successor_id UPDATE, step_id UPDATE, COMMIT), `safety = 1/4` (claims compete with the drain, tx time
  varies, a mature DB is ~20% slower, shards are unevenly loaded). The fleet takes the **worst shard's**
  number. **RTT is measured at Startup** (`probeRTT`: a few `SELECT 1`s per shard, MINIMUM of the samples,
  first discarded - it pays connection setup); a failed probe falls back to the measured same-zone
  constant. Every input is engine-visible - **no task duration T anywhere**, which is exactly what makes
  this derivable where `N = M x T/db` is not.
- **`turnstiles` (how many database calls may be admitted on a shard at once)** = `turnstilePassesPerConn x
  open`, eight turns per connection - deliberately MORE than the pool, so a queue forms in front of it (see
  "Turn-taking on the database"; sizing it to the pool measured a 6x collapse). This is what lets the crew grow freely for long tasks without the
  growth turning into pool contention, and the turn taken BEFORE a candidate is picked up is what regulates
  crew growth - by blocking, not by being consulted. See "Turn-taking on the database" below.

- **The pool grows on demand** (`internal/workers`): a worker that takes a candidate and finds **nobody
  idle** adds one, keeping a standing reserve of one. The gate's turn is what makes so easy a trigger safe —
  but by BLOCKING, not by being queried: a worker waiting for a turn has not popped, so it holds no
  candidate, so it counts idle and the next check declines. The two are a single design and neither is
  separately tunable.

  **DO NOT gate growth on every worker being simultaneously inside `ExecuteTask`.** That is a *sufficient*
  condition for "a replacement adds capacity" and nowhere near a necessary one, and the gap is quantifiable:
  with a worker off-resource a fraction `task/(task+db)` of the time, `P(all N away) ~ e^(-N.db/task)`, so
  integrating `dN/dt = D.e^(-N.db/task)` gives **`N(t) ~ (task/db).ln(D.t)`** - logarithmic. Throughput is
  `N/task`, so the `task` factor CANCELS and throughput converges to `ln(D.t)/db`: **independent of task
  duration, and approached only logarithmically.** Measured on a 16-vCPU shard at 600 flows/s with
  `-task-delay` the only variable: 6,005 steps/s at 0s, 1,020 at 1s (1,157 goroutines), 637 at 8s (5,298
  goroutines) - the last against a derived `workerCeiling` of 141,000 and a workload needing ~48,000
  concurrent. Eight times the task duration buys 4.6x the crew and *less* throughput, with both long-task
  arms in the same band. No threshold tuning fixes it: the predicate is on the wrong quantity, an
  instantaneous COUNT where the answer is a ratio of DURATIONS.

  **DO NOT reintroduce a caller-maintained counter for this either.** Its correctness rests on the caller
  wrapping exactly the host call and nothing else, and one line too many - wrapping the wait for a
  connection - makes saturation read as idleness, so any DB-bound backlog grows the pool toward the ceiling
  with each new worker queueing on the same connections: **~20% throughput on a saturated 8-vCPU shard
  (2,902 vs 3,523 steps/s), ~1,300 workers where ~512 sufficed.** The crew maintains its own idleness
  counter; what the caller owns is *when the turn is returned*, backstopped by `sync.OnceFunc` plus an
  unconditional call, so a leak is unreachable rather than merely detected.

  Both directions stay pinned - `TestPoolSizing_PoolGrowsForLongTasks` and
  `TestPoolSizing_SaturationDoesNotGrowThePool`, the second being the one whose absence let the runaway ship.
  Shutdown is the crew's two-phase `Drain`, and the engine's half is closing the CACHE **and the PERMITS**
  first: a worker with nothing to run is parked on one of them, and only a close releases it. Closing the
  cache alone leaves any worker waiting for a turn unreachable and `Drain` waits forever. See
  `internal/workers/CLAUDE.md`.
- **`SetWorkers` pins the maximum** (deterministic tests use `SetWorkers(1)`, which also disables growth;
  benchmark sweeps; hosts wanting a smaller global bound, e.g. for memory - the in-flight state maps are
  a cost the engine cannot see). Setting it ABOVE the ceiling is allowed and warned about at Startup: an
  operator may consciously trade storm-re-execution risk for long-task throughput. `SetWorkers(0)` is the
  await-only replica.

The worker count is deliberately **not** a backpressure knob: a global cap cannot express "64 concurrent
LLM calls, 1000 concurrent DB lookups, 200 Jira writes", and a per-task-URL cap is the removed valve's
wrong axis all over again (see "Backpressure is the task's or host's job"). Per-downstream concurrency
belongs in the host's `ExecuteTask` (a semaphore keyed on the real provider/account) with `flow.Retry`
when the downstream says no. The engine sizes workers for throughput, bounded only by what it can see:
the lease margin.

### Turn-taking on the database (`internal/turnstile`) — order the wait, do not remove it

**Every database call a step makes takes a turn at its shard's turnstile, and turns are served by band and
then by how long the JOB asking has been running.** The claim is stamped once, when the candidate is picked
up, and rides the context (`turnstile.Set.ContextWithPriority`) so every later call of that step presents
the same age. A step already under way is therefore older than anything admitted while its task ran, and is
served ahead of it — work in progress finishes rather than queueing behind work just starting.

```
park for work                       <- holding NOTHING
  -> peek: which shard has work?
  -> Gate.Acquire: stamp the claim, take the FIRST turn (blocking)
  -> TryPopFrom that partition       (empty? return the turn, retry)
  -> claim CAS                       <- the turn covers exactly this
  -> return the turn
  -> flow/graph load                 <- its own turn, on the same claim
  -> ExecuteTask                     <- unbounded, holds nothing, takes no turn
  -> persist / transitions           <- its own turn, still on the pickup-time claim
```

**Sizing is `turnstilePassesPerConn = 8`, and ONE TURN PER CONNECTION IS THE TRAP.** The turnstile does not
remove the wait for a connection, and it does not empty the pool's queue — **a caller holding a turn still
competes for a connection, and still waits when they are all busy.** What it changes is *who gets to
compete, and in what order*: the population at the pool is bounded by the turn count, and admission into
that competition is by band and then by age. So pool wait is expected at saturation, and is not evidence
of anything being wrong.

**Sizing turns to the connection count was measured at a 6x collapse** — 281 steps/s against 1,687 for the
gate it replaced, at 600 flows/s on a local Postgres. With exactly as many turns as connections there are
exactly as many candidates for one, so every gap between a turn-holder finishing and the next waiter being
woken, scheduled and asking the pool is idle connection time, on the critical path of all ~9.6 round trips a
step makes. **The pool queue is not waste; it is what keeps connections busy** — the same margin
`workersPerConnBudget` deliberately keeps, reached through a different door.

Above 8 the returns are small and unsettled: 10x and 12x measured 1,688 and 1,734 against 8x's 1,559, with
per-connection service time identical across all three, so what the extra multiple buys is queue depth
rather than throughput — and those arms differed by less than the rig's own RTT drift.

**A turn is held for ONE CALL, and that is what the count MEANS - not what makes it small.** A turn admits
one round trip, so the multiple is how many round trips may be queued for the pool at once, not how many
connections exist. Do not read the measured warning that
1x-per-connection *entry* "was the only setting that fell behind the offered flow rate" (crew at 60,765
against 70,783) as applying here: that was one permit held across a whole entry **phase** — claim, step
read, flow and graph load, plus the Go work between them — which is why it needed an 8x multiple. What a
per-call turn bounds is concurrent round trips, which is exactly what a connection is.

**A pass must ENCLOSE the connection, AND NOTHING ELSE.** A query hands back a cursor that keeps holding its
connection until it is closed, so the turn must be held until the reading is done, and a transaction takes
one turn for the whole `Transact` rather than one per statement inside it. Equally, it must be handed BACK
across any wait that is not the database — `yieldTurn` does that around the subgraph `LoadGraph`, the
persist retry backoff, and the cohort stripe mutex. The stripe is the sharpest of the three: its whole
purpose is that a losing sibling waits holding no connection, and a turn is that occupancy one level up, so
holding one there would put a whole cohort's losers into the very queue the stripe exists to keep them out
of. Callers holding connections without turns and
callers holding turns without connections deadlock each other, and neither side can break the cycle. This is
why nothing wraps `sequel.DB` — the call site is what knows when the connection is released. See
`internal/turnstile/CLAUDE.md`.

**Two bands, and the second one is the piston's alone.** `priorityRefill` (0) is above `priorityCommon` (1),
and bands are strict — a band is exhausted before the next is looked at. The piston has it because it is the
only caller bounded by construction (a derived period, two turns per cycle per shard), so it cannot starve
what sits below it; and it needs it because candidate supply runs only 1.04–1.47x ahead of consumption, so a
cycle queueing behind the dispatch it feeds starves the workers it is filling for.

**Everything else shares one band and is separated by age alone. That is a decision, not an unfinished
state.** Age ordering cannot starve — every claim eventually becomes the oldest — while a second band given
to any caller that can arrive faster than it is served starves the band below it. That is the shape of the
measured collapse that killed the single-permit-pool design (below), reached through a different door.

**DO NOT split entering and completing work into two dedicated reservations — the age ordering is what makes
one queue safe.** Counting them separately is the obvious defence, and it is only needed if the single queue
has to choose who wins. **Both ways of choosing were measured failing**:

- **Served evenly**, completions lost at random and queued behind admission: **286 of them waited a full
  second**, with transactions inflating 3ms → 54ms.
- **Served with completions given precedence**, entry starved — and entry IS dispatch — so short-task
  throughput collapsed **3x (4,416 vs 7,964 steps/s)**, with creation itself throttled to 714 of 800
  commanded flows/s.

Ordering by job age removes the choice rather than answering it: a completing step holds an older claim than
anything admitted while its task ran, so it wins, and no population can starve because every claim
eventually becomes the oldest. So if completions are measurably waiting, the reading is that the shard's
connections are saturated by work at least as old — which no split can fix, because the two populations are
not competing for different things.

**THE GATE'S TURN IS THE CREW'S GROWTH BRAKE, and that is its real job.** `Crew.idle` counts goroutines not
holding a candidate, and `considerGrowth` spawns only when a take left nobody idle. A worker blocked in
`Gate.Acquire` has not popped yet, so it counts **idle**, so growth declines. The crew overshoots a
saturated shard by exactly one worker, and that worker is first in line for the next turn.

**Do NOT move the gate's turn after the pop.** Two things break at once, and the first is severe:

- **The crew runs away.** A worker that pops first and then blocks *holds* a candidate, so it counts busy,
  so every take spawns a peer, bounded only by `workerCeiling` — which is derived deliberately huge for long
  tasks. Measured by removing the wait from `Gate.Acquire`: the crew went **64 → 398** under saturation.
  Pinned by `TestPoolSizing_SaturationDoesNotGrowThePool` and `TestCrew_SaturationDoesNotGrowThePool`, both
  of which fail against a gate that does not wait.
- **A candidate is stranded.** Work not yet taken is still visible to every other worker; taking it first
  and blocking afterwards parks it inside a worker that cannot proceed with it, while the pop is what empties
  the partition its peers are choosing between.

**Per shard, resolved by peek-then-acquire.** Connections are per shard, so turnstiles must be — but the pop
is what chooses the partition, so the shard is not known until after a pop, which is the ordering above
rules out. Hence: peek the shard, acquire, then pop **from that partition only**, returning the turn and
retrying on a lost race. **Do NOT make the turnstiles GLOBAL** — that is the removed rate valve's error, a
control keyed on the wrong resource: one shard's work would consume every turn and deepen exactly the pool
queue this exists to keep empty.

**Everything that takes a connection should take a turn.** A bypasser competes for connections without
having queued for the right to, so it is served ahead of turn-holders that are older than it - which is
precisely the ordering this exists to impose. It also puts the contending population back above the turn
count. Anything still unstamped (peer beats, the reaper, recovery sweeps, the operation API) runs unordered
by fail-open, which is the safe direction but is not the finished state.

**Fail open, and note the asymmetry with what it replaced.** An unstamped context, an unsized shard, an
expired ctx and a closed set all let the caller through with a zero pass. **Do not "harden" this into
failing closed.** A gate whose bound *is* the point must fail closed; this one only orders access to
something the pool already bounds, so refusing to run would wedge a shard over a sizing mistake while
letting it through merely costs ordering. The consequence to hold in mind is that a sizing mistake is
therefore **silent**, visible only as ordering that is not happening.

**Every path that changes a pool must re-size the turnstiles**, the same standing rule the worker ceiling,
the cache and the refill interval obey: `Startup`, `recomputePools`, and `SetMaxOpenConns` (the last is not
optional — once an override is set `recomputePools` early-returns, so it is the only path left). `Resize`
moves the available count by the **delta**, never to the new ceiling, or an in-flight holder's turn would be
handed out twice.

**Known watch-item: head-of-line blocking across shards.** If shard S's turns are exhausted, a worker blocks
on S while shard T sits with work and free turns — the one place this design can idle capacity it already
has. It is self-balancing with many workers, so this is a watch-item rather than a blocker, and it is
roughly no worse than an ungated crew (those workers would instead be queued on S's connections). It has a
second facet that is easy to miss: a worker blocked in `Acquire` counts *idle* to the crew, which is right
same-shard and wrong across shards — so while S is exhausted, `considerGrowth` declines to spawn for T even
when T has both work and free turns. The cheap mitigation, if it ever binds, is a non-blocking acquire plus
a re-peek for a shard with capacity; that variant does not exist today, and **do not add it speculatively —
add it with the re-peek that uses it.**

**New-flow placement is capacity-weighted (`pickShard`).** Placement is the engine's only load-balancing
moment - flows are shard-pinned for life (subgraph affinity, thread continuations, forks) - so heterogeneous
fleets must be loaded in capacity proportion or the smallest shard saturates first while larger ones idle.
The weight is `capacityWeight(VirtualCPUs)`: proportional to the measured steps/s ceiling (~flat <= 2 vCPUs -
the 1- and 2-vCPU tiers ceiling near the same throughput - then ~450/vCPU). Unknown-CPU shards get the
smallest known weight (conservative); all-unknown degrades to uniform. `Cordoned` shards are excluded from
placement entirely (everything resident proceeds - execution, subgraph children, `Continue`, `Fork`); all
shards cordoned is a loud 503 at `Create`. Pinned by `engine/poolsizing_test.go`.

**Adaptive/AIMD budgeting was considered and rejected** (2026-07-13): discovering each shard's knee online
(TCP-style probe-up/back-off) would eliminate the last declared fact, `VirtualCPUs` - but that fact is
trivial for an operator to supply, and the engine has already shipped-and-removed one control loop (the
per-task rate valve + breaker, 25959d0). The remaining exposure is honest and documented: a *wrong* declared
`VirtualCPUs`, or a deployment that isolates each replica's databases from the others (each then reads a
registry containing only itself, sees a fleet of one, and over-connects). Both are declared-fact failures,
the same contract the shard set already carries. Do not reintroduce a controller without a much stronger
reason than tidiness.

The idle-drain / lifetime-recycle connection timers (`ConnMaxIdleTime` / `ConnMaxLifetime`, server-drivers only)
are a database-layer mechanism — see `internal/database/CLAUDE.md`.

**Live re-sizing.** Derived sizing **is** live. `recomputePools` re-derives each shard's pool from the observed
replica count and pushes `SetMaxOpenConns`/`SetMaxIdleConns` to every open shard, `Resize`s the candidate cache, and
re-derives the worker ceiling - on every reconcile tick that finds a shard's count moved. The
`SetMaxOpenConns` override is the opposite of "the live one": it *pins* every shard's pool and thereby **suppresses**
the derived path (`recomputePools` early-returns once an override is set). See the live-vs-derived split at lines
122-133 and 1506-1511, and the load-bearing rule there - "EVERY path that changes a pool must re-derive it," and "the
cache follows the pool split": a new pool-derived quantity must be wired into `recomputePools`, not merely computed
once at `Startup`.

### Flow Scheduling (priority / fairness)

The schema carries `priority`, `fairness_key`, `fairness_weight` on **both** `dwarf_flows` (authoritative) and
`dwarf_steps` (denormalized), so the two-level selection never joins `dwarf_flows` on the hot path - the same
split used for `time_budget_ms`/`baggage`.

`resolveFlowOptions` resolves a caller's `*workflow.FlowOptions` against the engine defaults: priority falls back to
`SetDefaultPriority`, the fairness key to the host-supplied key (or `""`), the weight to `1`, and the time budget to
`SetTimeBudget` (no ceiling; see "Time Budgets"). These values are immutable for the flow's life (switching a flow's
fairness key mid-run would be a self-promotion abuse vector). This is the **genesis** path (`Create`/`Run`); the
**derived** operations inherit instead - `createSubgraphFlow` from the parent (so a high-priority parent never
silently spawns default-priority descendants), `Continue` from the thread's latest turn, `Fork` from the origin. See
"Policy is set once at genesis" under Engine Operations.

Propagation onto step rows: where the resolved values are in hand (the entry step), they are literal bind parameters;
in the deep `processStep` paths (fan-out and the two fan-in inserts), the values - including the flow's frozen
`time_budget_ms` - are read once per step execution in the parallel flow-row SELECT and threaded through the call
chain into the INSERTs as bind parameters (vs. the previous scalar subqueries, which meant 3N PK lookups per N-way
fan-out). `Fork` resolves scheduling once for the whole cloned tree and binds it on every cloned flow and step
(see "Fork").

**Why the scheduling design is shaped this way:**

- **Priority is a property of the flow, not the task or workflow type.** Step order *within* a flow is dictated by the
  graph, not urgency; priority only arbitrates *between* flows competing for workers, so it is resolved once at
  `Create` and immutable (`workflow.FlowOptions` is flow-level for the same reason).
- **Workers default to the lease-margin ceiling with a grow-on-demand pool** (see "Worker sizing"); an
  explicit `SetWorkers` pins the maximum. A worker blocked on an `ExecuteTask` call is just a goroutine
  stack plus a socket, so the derivation errs generous - and the pool only grows into it when work is
  actually parked.
- **Fairness weight is denormalized at `Create`, never resolved on the selection path** (a resolver hook would put
  synchronous I/O on the hot critical section). When a key's steps carry inconsistent weights, the oldest candidate
  step's weight is used; keeping weights consistent for a key is the caller's responsibility.
- **Completion writes are deliberately not gated by the refiller slot.** That slot bounds selection only; finishing
  in-flight work must outrank starting new work, so the post-execution advance is never serialized behind selection.

> Observability note: per-priority backlog/age and distinct-fairness-key counts are aggregate-only metrics by design
> (per-key labels would be unbounded cardinality). Metric emission is deferred in the engine and is a host concern;
> the engine exposes the underlying data through logging and return values.

### Step Parking (`parked` column)

`dwarf_steps.parked SMALLINT NOT NULL DEFAULT 0` takes a step out of the selection band without changing its
`status`. The selection index `(status, parked, priority, fairness_key, created_at, step_id)` and saturation index
`(status, parked, task_url)` lead with the partitioning columns, so parked rows are physically excluded from every
hot-path scan - no in-memory filter at refill time. (The selection index's trailing `(created_at, step_id)` serves
the refiller's per-key oldest-first ordering; see `internal/migrations/CLAUDE.md`.) The `parked` value labels *why* the step is held:

- `parked=0` (`parkedNone`, default) - active. Selection sees it; `recoverExpiredLeases` recovers it if its lease
  expires; saturation counts it as one in-flight slot. (Also the precondition the claim CAS requires.)
- `parked=1` (`parkedSubgraph`) - the step called `flow.Subgraph` and is waiting for the child. `status='running'`
  (logically running, blocked on its child) but excluded from selection, saturation, AND lease-expiry recovery. No
  lease deadline - the row sits until `completeSurgraphFlow` flips it back to `(pending, parked=0)`. This replaced an
  earlier `lease_expires = NOW + 7 days` "park" indicator that broke for subgraphs running longer than 7 days
  (the lease lapsed, the parent recovered, the task re-ran, launching a duplicate child).

**Terminal status implies `parked=parkedNone`.** The park value is meaningful only while a step is actively waiting.
Once terminal (`completed`/`failed`/`cancelled`), the park slot is gone, and the column must read `parkedNone`. Every
terminal-transition code path resets `parked` in the same UPDATE (the `failStep` write, the `Cancel` cascade, the
`processStep` terminal-flow guard). Without this, a step that was parked
when its flow was cancelled would sit terminal with non-zero `parked` - invisible to the selection index but never
re-leased. A `Fork` clone writes each step's `parked` explicitly (the re-parked ancestor callers to `parkedSubgraph`,
all other cloned steps to `parkedNone`), so cloned rows never inherit a stale non-zero `parked`.

## Metrics (`engine/metrics.go`)

The engine emits 33 `dwarf_*` instruments through the **OTEL metric API** (not the SDK). `SetMeterProvider`
injects the provider; it defaults to the global `otel.GetMeterProvider()` - no-op unless the host configures the
SDK, so unconfigured/standalone/test use pays nothing. Instruments are built once in `initMetrics` (from
`initRuntime`, so every `Startup` gets them) from `mp.Meter("github.com/microbus-io/dwarf")` - that
scope distinguishes dwarf's metrics; **service identity lives in the provider's Resource, not in per-metric
attributes** (no `service.name` on data points - cardinality explosion, off-spec). The only attributes attached are
the metric-specific labels: `workflow`, `status`, `task_name` (on `dwarf_steps_executed`), `priority`,
`park_type`, `role` (on the db-phase pair), `shard` (on `dwarf_steps_write_retried`), and `column` (on the
byte counters).

**8 counters, incremented inline** at their logical event sites: `dwarf_flows_started`
(start path), `dwarf_flows_terminated` (completeFlow), `dwarf_steps_executed` (every terminal step
disposition - completed/failed/interrupted/subgraph/retried/error_routed), `dwarf_steps_recovered`
(recoverExpiredLeases lease recovery), `dwarf_steps_unwedged{park_type}` (the parked-step wedge sweep; a
nonzero value flags a latent bug), `dwarf_flows_orphaned` (the orphan sweep's detection-only alarm - the
flow-level sibling of `dwarf_steps_unwedged`, counted at the same site as its error log, never auto-recovered),
and the two persist alarms `dwarf_steps_write_retried` / `dwarf_steps_write_failed` (detailed under "Persisting
a step's outcome"). The inline helpers no-op when `e.metrics == nil` (before Startup).

**3 refiller counters + 1 refiller histogram**, built by the PISTONS (`internal/piston`) from the meter
this engine resolves once and hands each of them - one instrumentation scope per engine, whoever records
into it. `initMetrics` deliberately does NOT build them: registering the same names twice on one meter
is a duplicate-instrument conflict, and the two copies had already drifted in description and bucket
boundaries. They are `dwarf_refill_candidates_selected` / `_discarded`, `dwarf_steps_stolen` and
`dwarf_refill_query_duration_seconds` {shard,phase}, where `phase` is four values - `band_keys`,
`fetch_steps`, and the two non-query phases `planning` and `pushing`. Recording planning matters because it
is the one cost that scales with fairness-key CARDINALITY (the lottery re-rolls per slot over every key).

**There is deliberately no end-to-end `dwarf_refill_duration_seconds`.** It existed to expose the MERGED
pass's straggler tax as its gap over the per-shard query max - a quantity the per-shard decoupling deleted
along with the barrier that produced it. What was left was a coarse duplicate of the four phases, which sum
to the same cycle and additionally say WHICH part was slow. Do not add it back without a question it
answers that the phase split does not.
These exist because **the refiller was the one hot-path subsystem with no timing instrument at all**, so the
question "what binds at the ceiling" could not be asked of it - and `docs/benchmark-cloud.md`'s
straggler-wait explanation for the flat 3-shard arm was inference, never a measurement (it has since been
retracted; the arm was load-generator-bound). They were placed to discriminate three hypotheses that look
identical from outside (the rules below record how each resolved):

- **One shard is slow** - `refill_query_duration{shard}` diverges *between* shards. Recorded per shard
  around each shard's own scan, timing query + row scan.
- **The cross-shard fan-out wait is the cost** - this one WON (max-over-shards measured 2.02x at 6 shards)
  and was then *removed* by the per-shard refiller decoupling, which dissolved the instrument that measured
  it: `dwarf_refill_duration_seconds` was the merged pass, and its gap over the per-shard query max was the
  straggler tax. With no barrier there is no merged pass and no gap, so the instrument was retired rather
  than left reporting a coarse duplicate of the phases (see above). A shard's own cycle time - which sets
  its partition's supply rate, `capacity_slice/max(cycle, floor)` - is the sum of its four phases.
- **The refiller oversupplies** - `discarded/selected` approaches 1. Every pass wholesale-replaces its
  shard's partition while being triggered after every `processStep` on that shard, so whenever it turns
  faster than the workers drain it throws away a batch it just paid to fetch. `Cache.Refill` returns the
  discarded count for this. (Measured 0-10% pre-decoupling: dead then. The instrument stays because it is
  the cheap readout that would catch the regime changing - and it is the gauge to re-tune the cycle interval
  against if the supply rate is revisited.)

The `phase` label (`band_keys` / `fetch_steps`) separates phase 1 from phase 3, which matters for a reason
found while writing the degradation harness: the band scan's plan flips between an index scan and a
**sequential** scan on Postgres statistics freshness - **0.29ms vs 99.8ms on identical data minutes apart**.
That is indistinguishable from a slow shard without the phase split, and phase 1 is where the measured cost
concentrates, so the split is what makes the band scan's backlog dependence visible at all.

Explicit second-valued bucket boundaries (`refillBuckets`, now in `internal/piston`) are mandatory: the
OTEL defaults are tuned for millisecond-valued instruments and would file every sample in bucket 0. **The
LOW end is load-bearing** - a warm band scan is ~0.29ms and the same query is ~100ms once its statistics go
stale, which is the flip the `phase` label exists to expose, so boundaries starting at 0.0005 hide the
healthy case in bucket 0.

**What the instruments measured, as rules.** The campaign detail (rigs, dates, artifacts, per-arm
numbers) is deliberately not here - it is measurement, it expires, and it lives with the benchmark
worklist. These are the parts that would make a future change WRONG if unknown:

- **The refiller is what binds** - candidate supply runs only 1.04-1.47x ahead of consumption. Treat it
  as a throughput-critical path, not a background chore.
- **The "independent of the backlog" claim above is true of WIRE cost only, and the SERVER scan is now
  CAPPED per key.** Phase 1's server-side scan was O(backlog) (`COUNT(*) OVER (PARTITION BY ...)` reads
  every due row of a key); it is now `MAX(rn)` under `rn <= capacity`, so per key it touches at most
  `capacity` rows (on Postgres 15+ a run-condition early-stop; elsewhere still a scan but without the
  extra COUNT pass) - see "The phase-1 scan count is CAPPED, not exact" above. Fragmented backlogs (many
  tiny keys) still cost O(distinct keys); do not cite the three-phase split as evidence that backlog depth
  is free.
- **The band scan's client-observed "fixed floor" is CONNECTION-POOL WAIT, not query work** — measured
  by decomposing the client clock against `pg_stat_statements` and the pool-wait gauges (additive,
  every run), and causally: a dedicated refill connection collapses the floor to server + RTT. Its
  magnitude scales with pool contention (the once-quoted "fixed ~46ms" was one saturated
  configuration), so no query-shaped change — index, rewrite, fetch cap — can touch it; that is WHY
  none ever moved it. Confirmed twice over by two independent removals — a reserved refill connection,
  and cutting N 4x — each collapsing the client-observed scan to server time while server time held.
- **Pool wait is silently RATE-LIMITING the refiller, and anything that reduces pool contention removes
  that limit.** Measured on both removals above: refill passes ~3-4x, discards 0.7% -> 43-45%. This is a
  property of the contention, not of either mechanism, so it applies equally to a reserved connection, a
  smaller N, a larger M, or a faster database. Any such change must arrive with the cycle interval re-derived
  against it, or the refiller spends server work and cache churn producing candidates nobody consumes.
- **Neither removal bought throughput, and neither improved latency.** Throughput was null in every
  regime (at deep backlog the scan is server-execution-bound; at shallow the refiller was not the
  constraint), and flow p99 did not move — the queue is deep because the DB is the constraint, and
  shortening the line does not speed up the server.
- **The pool queue is `workersPerConnBudget`'s deliberate margin, and it is FREE — do not "fix" it.**
  The resident set is 8 x conns against a measured `T/db ~= 1.5-3` for short tasks, so it overshoots
  ~4x by design. Cutting N 4x was measured: null throughput, null p99, flat host CPU across 288
  goroutines. The asymmetry is what settles it — overshoot buys a queue nobody pays for, undershoot
  idles connections and costs real throughput, so generous-and-fixed is correct and precision is
  worthless. Deriving N from a measured duty cycle (`M x T/db`) is a REGRESSION, not the next rung:
  it reintroduces the task-duration dependence `workerCeiling` exists to avoid, and under a mixed
  workload it fits a blend wrong for every class in it. The bound that matters is the lease margin,
  and `workerCeiling` already enforces it without knowing T.
- **Do NOT expect a win from capping phase 3's per-shard fetch.** `rn <= perKey` filters AFTER the
  window function, so it cuts rows *returned*, not rows *processed*. Built twice as an optimization,
  measured twice (n=3 then n=5), phase-3 time unchanged both times, reverted. The over-fetch is a wire
  cost and the wire is not where the time goes. (The slice rule now caps each shard's fetch at its own
  plan slice as a *correctness* consequence of the global-plan split - fine, but do not credit it with a
  phase-3 latency win, and do not add further caps chasing one.)

- **CAP the count, do NOT APPROXIMATE it - they are different, and only one is safe.** `count` becomes
  the planner's per-key remaining count (how many batch slots a key wins) AND sets the per-key fetch cap (phase 3's
  `rn <= ?`). A genuine *approximation* is wrong in either direction: over-estimating lets a key
  out-compete its peers *and* inflates phase 3's fetch; under-estimating caps a key below its weighted
  share. So an approximate count (e.g. a loose index scan enumerating distinct keys in O(keys x log n))
  is forbidden - the fairness allocation built on it breaks. A **cap** (`min(count, capacity)`, the
  current `MAX(rn) ... rn<=capacity`) is *not* an approximation: it is exact for `count < capacity` and
  saturated-correct above it, and lossless because the planner can never consume more than `capacity`
  from one key. The cap does not enable the loose-index-scan shortcut (it still visits every due row
  server-side off-Postgres, and up to `capacity` rows per key on PG); reviving a truly sub-linear phase 1
  still needs distinct-key enumeration (skip-scan) AND a proof the fairness contract survives.
- **The window functions are NOT the expensive part** (post-covering-index) - but note phase 1 now DOES
  use `GROUP BY` (over a windowed subquery, with `MAX(CASE WHEN rn=1 ...)` for the oldest row - NOT the
  rejected `HashAggregate`+per-key-`LATERAL` rewrite, which was slower). This `GROUP BY` measured *faster*
  than the old double-window (`COUNT(*) OVER` + `ROW_NUMBER`) because it drops a full aggregation pass, not
  because `GROUP BY` is cheap. General rule: re-derive the cost split after any change that moves the
  baseline.

**The oversupply hypothesis is dead** (`discarded/selected` measured 0-10%, never the ~100% it
predicted). Do not re-propose it without new evidence.

**17 counters** in total: the 8 event counters above, plus `dwarf_steps_offered` /
`dwarf_steps_claim_preempted` / `dwarf_steps_claim_lost` / `dwarf_peer_changes`, plus the three the pistons
own (`dwarf_refill_candidates_selected` / `_discarded`, `dwarf_steps_stolen`), plus the two byte counters,
`dwarf_state_write_bytes` / `dwarf_state_read_bytes` (labels `workflow` + `column`, unit `By`) - payload
bytes the engine writes to / reads from **step rows on the execution path**. The `column` label is the
dwarf_steps column the bytes moved through (`state` snapshots, `changes` task-output deltas,
`resume_data`, `interrupt_payload`, `subgraph_result` - bounded cardinality; sum across it for totals),
so a chart can split task data from engine snapshots from human-in-the-loop payloads. Counted: the claim
read (state+changes+resume_data+subgraph_result), completion/retry/park/interrupt `changes` writes, the
interrupt payload write, entry/successor/fan-in `state` snapshot writes, and the fan-in merge's cohort
reads. Deliberately NOT counted: introspection reads (`List`/`History`/`Snapshot`), flow-row payloads
(`final_state`, `baggage`, `graph` - per-flow context, not per-step data), Fork's clone (a DB-side
`INSERT…SELECT`; the bytes never pass through the engine), and the `Resume`/surgraph-revive *writes* of
`resume_data`/`subgraph_result` (cold paths where the workflow URL is not in hand - those bytes are
counted on the claim *read* at the re-dispatch they trigger, so every payload is counted in at least one
direction). The metric exists to track against the database's byte-throughput ceiling (disk/WAL), which
the cloud benchmarks showed binds separately from its steps ceiling (~46-60 MB/s incompressible on a
100GB disk vs ~3.3k steps/s on 8 vCPU). Transition-tx byte counts are accumulated in the closure
(`stateByteCount`) and emitted only after commit - a contention retry re-runs the closure, so counting
inline would double on every rollback. Unit-denominated instrument names end with the unit (`_bytes`),
per the Prometheus naming convention.

**Counter instrument names carry no `_total` suffix.** `_total` is a Prometheus naming convention, not an
OpenTelemetry one: a Prometheus exporter appends it to every counter at the scrape boundary (and
de-duplicates, so a name already ending in `_total` is not doubled), while the OTLP push path uses the
instrument name verbatim. So the instruments are named `dwarf_flows_started` etc., and a Prometheus query
references them as `dwarf_flows_started_total`. Do not bake `_total` into a counter's instrument name.

**12 gauges, observable (async)** via a single `RegisterCallback`. The callback runs at metric-collection
time and reads engine state, and all of that is in-memory EXCEPT one query: `dwarf_steps_pending` and
`dwarf_steps_oldest_pending_age_seconds` (per priority band) come from `observePendingByBand`, one statement
per shard. Keeping the query-backed set to those two is deliberate - a scrape should not be load.

**Two gauges were removed from here and should not come back without a new argument.**
`dwarf_task_concurrency_running` {task_url} cost a SECOND query per shard per scrape - on every replica, to
produce R copies of one cluster-wide number - carried the least bounded label in the set, and asked a
question that belongs to the host: per-downstream concurrency turns on the account/tenant identity the
engine structurally cannot see, which is the same rule that keeps backpressure out of the engine (see
"Backpressure is the task's or host's job"). `dwarf_steps_fairness_keys` was a per-replica sample of the
LAST plan's distinct-key count, read from `planner.LastBand()`; it drove no decision and alarmed on nothing
- a scheduling diagnostic that had served its purpose. `LastBand` itself stays, because the piston's and the
planner's own tests assert on it.

**`dwarf_state_in_flight_bytes` / `_steps` are the HELD-STATE pair, and three things about them are
load-bearing.** Two atomics on the Engine, bracketed in `processStep` around the `ExecuteTask` call.

- **The window is the HOST CALL, not `processStep`.** The carrier is live for the whole function, but only
  across the host call is a worker parked on something unbounded while holding no connection - which is what
  makes held state a memory question rather than a throughput one. The bracket is explicit rather than
  deferred, for the same reason the gate's turn is returned explicitly: a function-scoped defer would span
  the persist half. It
  is safe unbracketed by a defer because `errors.CatchPanic` converts a host panic into a return rather than
  unwinding past it. Pinned by `TestMetrics_InFlightStateGauges`, whose after-release assertion fails against
  a missing subtraction (measured: 8,204 bytes still held).
- **The ref bytes come from `resolveStateRefs`' return, NOT from `dwarf_state_read_bytes`.** The two counts
  answer different questions and disagree in both directions: the read metric counts whole anchor ROWS (the
  database's byte throughput) and is ZERO on a resolve-cache hit, while this needs the bytes that ended up in
  the carrier. A fan-out resolves the same anchor for every branch - one miss, N-1 hits - so using the read
  count would report a large carried document as nothing on all but the first branch, in the one measurement
  the gauge exists to make. This is why `staterefs.Linker.Resolve` returns a materialized-bytes count at all;
  every caller but the dispatch path discards it.
- **It measures the WIRE form, and that is the point, not a limitation.** The decoded maps are what occupy
  the heap, and their size cannot be had cheaply (re-marshalling costs a full encode per dispatch and still
  reports wire size; walking the structure is expensive and approximate). Measuring the input bytes makes the
  gauge *invariant to how state is represented in memory*, so read against the Go heap it yields the decode
  expansion factor - and a change to that representation shows up as **gauge flat, heap down**, which is
  strictly stronger evidence than a single number moving. A gauge that fell along with the heap could not
  distinguish "we removed the expansion" from "we did less work".

They also close a blind spot that predates any of this: `SetWorkers`' own rationale concedes that "the
in-flight state maps are a cost the engine cannot see", and the turnstile lets the crew grow to thousands of
goroutines for long tasks - so held state x crew size is a memory ceiling nothing else reports.

**The gauges are of two kinds and they aggregate differently - do not paper over this.** `queue_depth`,
the db-phase count and the two `state_in_flight` gauges are genuinely **per-replica** (read from this
replica's memory): sum them. The two query-backed ones are **cluster-wide** by construction - they are
computed by querying the SHARED shard databases, so every replica observes the *same* number. Summing them
multiplies by the replica count (a 1,000-step backlog reads as 3,000 on three replicas; a summed
`oldest_pending_age` is meaningless outright), so they aggregate with `max`/`avg`. A per-replica reading of
them is not obtainable from a shared database, and the engine does not pretend otherwise: each instrument's
*description* states its kind, because that is what an operator building a panel actually reads. The
callback is `Unregister`ed first thing in `drainRuntime` so the OTEL reader can't query a closing database.

**There is no `_peak` companion to `dwarf_db_phase_workers`, and adding one back is a step backwards.** A
high-water mark held since process start goes CONSTANT within hours on a long-lived replica, answering
nothing about now, while `max_over_time` on the plain gauge gives a WINDOWED peak the backend computes for
free. It also carried a footgun the gauge does not: the two roles peak at different moments, so summing
them reported a mark that never occurred.

**Fidelity choices:** `flows_terminated` counts ALL THREE terminal statuses - completed (`completeFlow`),
failed (`failStep`, and the cohort-resolution path in `processStep` that fails a flow without going through
it), and cancelled (`cancelSubtree`, per flow of the tree it terminalized). It once fired only on
`completed`, which broke it in two ways at once: the `status` attribute had a single value, so
`sum by (status)` silently answered a completed/failed/cancelled question with completions alone, and the
in-flight panel this metric exists for - `flows_started` minus this - drifted upward permanently by every
flow that did not finish cleanly. Both halves are pinned by
`TestMetrics_TerminatedCountsEveryTerminalStatus`, which fails on all three of its assertions against the
old behaviour. The cancel path counts the flows that were non-terminal when it scanned the tree, so a cancel
racing a concurrent completion can over-count by one; that window is microseconds against a miscount that
used to be every failed and cancelled flow, so it is not worth a round trip to close. **Every path that starts a flow must call
`metricFlowStarted`** - `Create`, `Continue`, AND `Fork` (which builds its new root through its own
`INSERT...SELECT` clone and so was silently missed): a fork's completion runs through the same `completeFlow`
that increments `flows_terminated`, so a missing start makes the standard in-flight panel
(`started - terminated`) drift negative by one per fork. Subgraph flows are counted too - the start
path and `completeFlow` run for them - so no `surgraph_flow_id` filter; the `workflow` label lets dashboards
slice root-vs-subgraph. `TestMetrics_EmittedOnRun` pins emission with an in-memory SDK `ManualReader`.

> Observability note: the per-priority/per-task gauges are aggregate-only by design - no
> per-`fairness_key` labels (unbounded cardinality).

## Tracing (`engine/tracing.go`)

The engine is OTEL-native for tracing, symmetric with metrics: `SetTracerProvider(tp)` overrides the global
`otel.GetTracerProvider()` (no-op unless the host configures the SDK), and the engine creates spans from
`tp.Tracer("github.com/microbus-io/dwarf")` (same scope as metrics; service identity lives in the provider's
Resource, not span attributes). The host injects **only** the provider - no span code, no `trace_parent` handling.
Resolved once in `initRuntime` (`initTracer`); under the no-op tracer every site below is free.

**Two span sites, persisted across replicas via the `trace_parent` column.** A flow's trace context is
minted once and reconstructed on every step dispatch (which may land on any replica), so it must survive
in the database - hence `trace_parent` is a **first-class dwarf-owned column** (the honest asymmetry vs.
metrics: spans need cross-replica continuity, metrics don't).

- **Root "workflow" span at `Create`** (`mintWorkflowSpan`, called from `createWithGraph`). The span is
  created, `End()`ed immediately, and its W3C context serialized into the flow's `trace_parent` column
  (`extractTraceParent`). Top-level `Create`/`Continue` mint it **detached**
  (`trace.ContextWithSpan(ctx, nil)` strips any ambient request span) so each flow - and each `Continue`
  turn - roots its own fresh trace rather than nesting under the request that created it.
- **Per-step span in `processStep`**, named by the task. The stored `trace_parent` is reconstructed
  (`injectTraceParent`) as the parent, the span is started with `workflow.id` and `workflow.name`
  attributes, and the span's context is **placed on the `ExecuteTask`'s ctx** so the task's own
  downstream spans nest under it automatically. The span records the dispatch error
  (`recordSpanError` → `RecordError`+`SetStatus(codes.Error)`) when the executor returns one.

**Telemetry carries the token-free correlation id, never the flowKey.** `workflow.id` is
`keys.CorrelationID(shard, flowID)` = `"{shard}-{flowID}"`, **not** the flowKey. The flowKey's third
segment is a random token that is a *bearer write-capability* (`Resume`/`Cancel`/`Fork`/… gate only on
`flow_id`+`flow_token`), and a trace backend is typically readable far more broadly than the workflow
data - so stamping the key onto every span would hand a write capability for every traced flow to every
trace reader. The correlation id uniquely identifies the flow ({shard} disambiguates the per-shard
sequential id) for grouping/pivoting/log-join, but grants nothing. The same rule governs logs and metric
labels: the token appears only on the task carrier (`flow.SetFlowKey`, trusted task code) and as an
in-memory waiter-match key (`signalStop`, never leaves the process). No log line emits a
flow key at all. Any new span/log/metric that names a flow must use `keys.CorrelationID`, never the raw key.

**`List` surfaces the flow's trace id.** `FlowSummary.TraceID` is the 32-hex W3C trace-id parsed from the
`trace_parent` column (`traceIDFromParent`), empty when no tracer was configured. It is a **token-free
correlation value** (like `keys.CorrelationID`, not a capability), safe to surface so an operator can pivot from
a listed flow to its trace backend - consistent with the trace-read < data-read privilege boundary above. It is
*not* the OTEL span attribute (`workflow.id` stays the `{shard}-{flowID}` correlation id); it is a distinct,
read-only observability field on the list projection.

**No correlationID→key lookup, by design.** The correlation id is deliberately *not* a valid engine key
(no operation accepts it) and the engine offers **no** operation that resolves it back to a key - that
would be a capability-minting oracle, re-leaking through the back door exactly what dropping the token
from telemetry closes (a trace reader, or any service that can reach the engine, could turn the id into
write access). Legitimate "act on the flow I found in a trace" resolves outside the engine: the host's own
store of the keys it minted at `Create` (under the host's authz), or database break-glass
(`SELECT flow_token …`, already the top privilege tier - it exposes everything, so it mints no *new*
capability). The friction of "a trace id is not accepted by any API" is the trace-read < data-read <
DB-read privilege boundary working, not a gap to fill.

**Subgraphs nest, they don't flatten.** A subgraph gets its **own** "workflow" span parented to the
**caller step's span**, not the parent flow's root - so the trace reads
`workflow → caller-step → workflow(subgraph) → subgraph-steps`, mirroring the call structure. The
mechanism: when a task arms `flow.Subgraph` and `processStep` creates the child flow, the caller step's
span is still live on `taskCtx`, so the engine extracts its context (`extractTraceParent(taskCtx)`) and
hands it to `createSubgraphFlow` → `createWithGraph` as the `parentTraceParent`; `mintWorkflowSpan` then
parents the subgraph's "workflow" span under it (rather than minting detached). Span IDs are fixed at
`Start`, so it does not matter that the caller span (and the subgraph "workflow" span) have already ended
by the time the subgraph's steps dispatch later - the children simply reference the recorded parent span
ID. `createSubgraphFlow` no longer reads the parent flow's `trace_parent` column (it uses the live caller
span instead); baggage is still inherited via its post-insert UPDATE.

**Reentrancy → one span per dispatch.** The per-step span is created inside each `processStep` call, so a
step that yields (`flow.Subgraph`/`flow.Interrupt`) and later re-dispatches produces **two** spans - one
per real execution attempt, each capturing that attempt's queue wait and body. This is intentional.

`TestTracing_SpansEmittedOnRun` pins all of the above (root detached, steps parented to root, subgraph
"workflow" parented to the caller step, subgraph step parented to the subgraph span, two `runInner` spans
for the yield+resume) using the trace SDK's in-memory `tracetest.SpanRecorder`. Test-only caveat: the
**last** step of the awaited flow is the one whose completion wakes `Await`, and its span ends in a
`defer` that fires just after that wake on the worker goroutine - so a synchronous `sr.Ended()` read right
after `Run` returns may miss it. The fixture keeps a trailing task last and asserts only on spans that
are deterministically flushed by then. Not an engine concern: a real exporter keeps flushing after
`Await` returns.

## Data Retention

The engine does not auto-purge flows on a timer: every row remains potentially-resurrectable - an `interrupted` flow via
`Resume`, a `completed` flow via `Continue`, a terminal flow via `Fork`. A retention *duration* was
rejected for two reasons: a clock-triggered delete reaps rows out from under those resurrection paths (a flow `failed`
for 30 days may still be wanted as a `Fork` source; one `interrupted` for 30 days is awaiting a human), and no single
duration fits both a 1-hour batch and a 30-day approval. The author also cannot know at `Create` "how long will this
be relevant after it ends." So retention is either operator-driven or an explicit author opt-in.

**Deletion is deferred: mark, then reap.** No path deletes rows inline. `DeleteOnCompletion`, `Delete`, and `Purge`
all **stamp `delete_after_ms`** on the target **root** flow (`0`=keep; `>0`=reap at `updated_at + delete_after_ms`);
a dedicated **reaper** goroutine (`reaperLoop`, ~1min ticker) later removes the whole subtree set-based, keyed on
`root_flow_id`. This closes the old strand race (a `Delete`/`Purge` deleting steps while a `Resume` revives the flow)
by construction - no rows are deleted where a lifecycle op could interleave (see "Deletion race gate" below) - and
makes a disposable flow's **outcome observable** during its grace window.

- **`FlowOptions.DeleteOnCompletion`** - the author declares a flow fire-and-forget (durable-execution jobs whose
  output and history are not needed). On success `completeFlow` stamps `delete_after_ms = deletionGrace` (hardcoded
  **1 min**; a per-flow duration would be retention policy, out of scope) **in the same transaction** that marks the
  flow `completed`. An *event* trigger on success, not a clock: `failed`/`cancelled`/`interrupted` flows are **never**
  scheduled (a failed disposable job is exactly the one to keep as a `Fork` source). Root-only (`surgraph_flow_id=0`),
  not inherited by children (the reaper sweeps descendants via `root_flow_id`). During the grace window the flow stays
  `completed` and its **outcome is observable**: `Snapshot`/`Await`/`Run` return the completed `FlowOutcome` - this is
  how a caller learns a disposable flow's result now that there is no stop callback. It is nonetheless *logically*
  gone: excluded from `List`, and `History` 404s (the full step detail is what the flow is discarding). After the
  window the reaper removes it and reads 404. (`Snapshot`/`Await` deliberately serve the outcome rather than 404 - a
  hard-immediate-404 for a redaction-critical `Delete` is a deferred sub-decision.)

For operator-driven retention (both mark, do not delete inline):

- **`Delete(flowKey)`** stamps `delete_after_ms = 1` (due immediately) on the root after the read-guards (404 on
  unknown key, 400 on a subgraph-child key, 409 on a `running` flow). An `interrupted` flow is terminalized in the
  same UPDATE (`status = CASE WHEN 'interrupted' THEN 'cancelled' ELSE status END`) - deleting a pending approval
  *is* cancelling it. Already-scheduled (`delete_after_ms > 0`) is idempotent-success. The reaper sweeps the subtree.
- **`Purge(Query)`** marks all matching roots with one set-based UPDATE per shard: it `SELECT DISTINCT`s candidate
  roots (`f.surgraph_flow_id=0 AND f.status<>'running' AND f.delete_after_ms=0`, capped at `purgeCap` **4096**, ids
  embedded as integer literals to dodge the per-driver bind-param ceiling), then
  `UPDATE ... SET delete_after_ms=1, status=CASE WHEN 'interrupted' THEN 'cancelled' ELSE status END WHERE flow_id
  IN (ids) AND status<>'running' AND delete_after_ms=0`. Returns the count **marked** (reaped shortly after). Same
  `Query` shape as `List`; **rejects** `IncludeSubgraphs` with 400.

**The reaper** (`reapDueFlows`, `reaper.go`) runs on its own dedicated goroutine + ~1min ticker (a single goroutine,
inherently non-overlapping), drained via `reaperStop` in `drainRuntime`. Per shard it loops `purgeCap`-sized batches:
`SELECT` due roots (`delete_after_ms>0 AND surgraph_flow_id=0 AND DATE_ADD_MILLIS(updated_at, delete_after_ms)<=NOW`),
then a two-statement tree delete (`DELETE dwarf_steps WHERE flow_id IN (SELECT flow_id FROM dwarf_flows WHERE
root_flow_id IN (ids))`, then `DELETE dwarf_flows WHERE root_flow_id IN (ids)`) - set-based, **no N+1**, removing each
root plus its descendants. It checks `reaperStop` between batches so `Shutdown` aborts a long drain promptly (between
whole-tree deletes, never mid-statement). No startup/shutdown/wake passes: deletion is latency-tolerant, so a flow
that came due while a replica was down is removed on the next tick (single-replica) or by a peer's tick.

**The tree delete is unconditional on descendant status - a deliberate change from the old inline `deleteFlow`.**
The former `deleteFlow` returned 409 if *any* subgraph descendant was `running`; the deferred path stamps only the
root (whose own status is 409-guarded, so a non-terminal root is never stamped) and the reaper deletes the whole
`root_flow_id` tree regardless of descendant status. The only running descendant a terminal-rooted tree can hold is
the **orphaned-child residue** (the Cancel-vs-spawn race: a live child whose parent already terminalized - see
`recoverOrphanedSubgraphChildren`), a bug-state row the wedge sweep would cancel anyway. Deleting it is safe: a
worker mid-dispatch on the orphan no-ops via the lease fence (claim/write matches zero rows once the tree is gone),
so no strand and no corruption. Whichever of the reaper and the wedge sweep reaches the orphan first wins. The
reaper therefore does **not** reguard on descendant status. Backed by a
partial index `idx_dwarf_flows_delete_after WHERE delete_after_ms > 0` (full on mysql) that narrows the due scan to
the small in-window subset. (`deletionGrace`/`reapInterval` are `var` not `const` only so a test can shorten them -
engine-package tests force a reap via `reapDueFlows`; fixtures verify the observable public contract via `List`/
`History`/`Snapshot`.)

**Deletion race gate.** Because deletion is now an *orthogonal column*, not a status, stamping `delete_after_ms`
alone does not serialize against `Resume`. The invariant that keeps the old strand bug closed is
**`delete_after_ms > 0 ⟹ terminal status`**, and the reaper reaps only terminal-rooted trees: DeleteOnCompletion
stamps a `completed` root (immutable); `Delete`/`Purge` of a terminal flow stamp an immutable row; `Delete`/`Purge`
of an `interrupted` flow flip it to `cancelled` in the same UPDATE, mutually exclusive with `Resume`'s
**root-flow gate** (row lock; exactly one wins, loser 409s). So a live `delete_after_ms` never coexists
with a resumable flow, and no steps are ever deleted where a `Resume` could interleave.

The mutual exclusion is not automatic - `resume`'s step writes (leaf `→pending`, ancestor re-park) are
*unconditional* on `WHERE status='interrupted'` at the **step** level, and `Delete`/`Cancel` terminalize the
**flow** row without touching steps, so those step writes still match after a `Delete` won. Left ungated,
`resume` would re-park + reset the leaf, match 0 rows on its `→running` flow update (the flow is now
`cancelled`), and still return success - a resume that did not take effect reported as if it had, leaving a
transient cancelled-flow-with-non-terminal-steps until the reaper mops it. So `resume`'s transaction carries a
dedicated **gate write** on the root flow (`UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=<root> AND
status='interrupted'`) after its step writes: a zero-row match means `Delete`/`Cancel` terminalized the root
first, so the whole transaction rolls back (undoing the step writes) and `resume` returns 409. `touch` flips
unconditionally, so `RowsAffected` reflects the status match on every driver (MySQL included), and the write is
placed *after* the step writes to preserve `resume`'s steps-first lock order (shared with `Cancel`). This gate is
distinct from the `→running` chain-flow update, which legitimately matches 0 rows when a *sibling* interrupt
still holds the flow (fan-out resume-one-at-a-time), so it cannot serve as the race gate.

**Reads/derivations of a `delete_after_ms > 0` flow.** `Snapshot`/`Await` serve the outcome (the observability win);
`List` and `History` exclude it; `Continue` **skips** deleting turns (its latest-turn query adds `delete_after_ms=0`,
so it builds on the latest *undeleted* turn - the copy is safe even if the source is later reaped); `Fork` **409s**
(it names a specific doomed flow, unlike `Continue`'s search); `Cancel`/`Resume` 409 (terminal); `Delete` is
idempotent-success.

Both share filter clauses with `List`. The `Query.TaskName` filter joins `dwarf_steps` and matches the current
step's `task_name` (excludes fan-out flows, `step_id=0`). `Query.OlderThan`/`NewerThan` are database-anchored
(`f.updated_at < DATE_ADD_MILLIS(NOW_UTC(), -ms)` etc.) and compose. `Query.FairnessKey` filters on the
engine-native `f.fairness_key` (the host typically sets it to the tenant, so "list tenant X" is "list
`fairness_key = X`"); `Query.Priority` narrows to one scheduling band. Empty key / zero priority disable their
filters. The engine models no tenant concept of its own - the `tenant_id` column was dropped.

## Concurrency and Crash Recovery

The engine uses SQL transactions for multi-statement operations and `lease_expires` for crash recovery.

### Host-call panic isolation

The host runs **in-process**, so a panic in host code propagates straight into the engine's own goroutines - and
that is the reason to defend: with no process boundary, one buggy task handler would otherwise take down every flow
sharing the replica. Both `Host` calls are wrapped in `errors.CatchPanic` at their **call boundary** (not
only at the worker loop):

- **`ExecuteTask`** (`execution.go`) - the panic becomes the call's `error` and flows through the **normal
  disposition**: routed via `onError` if one exists, else `failStep`. This is the load-bearing case. The worker loop
  *already* wraps `processStep` in `CatchPanic` (`scheduling.go`), so the process never died - but that catch is too
  coarse: a panic there unwinds the whole `processStep` stack *past* the error routing, leaving the leased step stuck
  `running` until lease expiry (`budget + leaseMargin`), which re-dispatches and re-panics - a slow crash-loop that
  also skips the author's `onError`. Catching at the boundary turns a panic into a clean, immediate step failure
  (lease released), identical to a returned error. `errors.CatchPanic` captures the stack trace into the error for
  diagnosis.
- **`LoadGraph`** (`operations.go` at `Create`, `execution.go` at subgraph spawn) - converted to an error
  return. The subgraph-spawn site runs inside `processStep`, so without this it would wedge the caller step
  exactly like `ExecuteTask`; the `Create` site fails the `Create` call cleanly instead of unwinding into the
  host's own frame.

The worker-loop and refiller `CatchPanic` wrappers stay as **defense-in-depth** for panics in *engine* code
(transition evaluation, fan-in, etc.) outside any host call. `fixtures/panicflow_test.go` pins both task-panic
outcomes (no `onError` → flow `failed`; with `onError` → routed to the handler and `completed`), using a bounded
`Await` that would time out if the panic had wedged the step instead of failing it.

### Worker context (the engine lifetime)

Workers, the timer, and the refiller share the engine's lifetime context (`e.lifetimeCtx`), created at Startup and
cancelled only after `Shutdown` drains all three. So by the time the lifetime ctx is cancelled, every DB operation
has committed - in-flight writes are never interrupted by ctx cancellation. The only *cancellable*, time-bounded ctx
is the `ExecuteTask` call: `executeTask` derives it from the lifetime ctx with the step's `time_budget_ms`.

### Shutdown ordering: workers, then timer, then recovery/reaper, then the pistons

`timerLoop` is terminated by a dedicated `timerStop` channel it selects on. It has no other channel: the
early-wake path is gone (see "The timer is a RECOVERY sweep"), which also retired the hazard an earlier
design carried, where a `wakeTimer` send could race a close and panic a worker mid-`processStep`.

```
close(reconcileStop); reconcileWorker.Wait() // in-memory only: nothing in flight to wait for
closeMetrics()                               // the gauge callback must not query a closing database
cache.close(); turnstiles.Close()            // BOTH: a worker parks on one or waits on the other
workers.Wait()
close(timerStop);    timerWorker.Wait()
close(recoveryStop); recoveryWorker.Wait()
close(reaperStop);   reaperWorker.Wait()
pistonCancel();      pistonPool.Wait()   // ctx is the pistons' only stop signal
sonarCancel();       sonarPool.Wait()    // ditto - and they must outlive the pistons
leaveFleet()                             // only now is the last possible beat behind us
```

**The Sonars drain LAST, after the pistons, and that ordering is load-bearing.** This replica must stay
registered while its workers are still executing steps, because every one of them drains against pools each
peer sized for a fleet that still includes us. Only once the Sonars' loops have returned is the last
possible beat behind us, which is what makes `leaveFleet`'s delete final: a beat only ever UPDATEs, so
nothing can resurrect a row deleted after its loop stopped.

Both run on **children** of the lifetime ctx (`pistonCancel`, `sonarCancel`), not the lifetime ctx itself:
that one is deliberately left live until every goroutine has drained, so in-flight database work always
commits, while `piston.Run` and `Sonar.Run` end on ctx alone. Neither needs a second signal - a cycle is a
pure read and a beat is one idempotent UPDATE, so abandoning either mid-flight strands nothing.

`leaveFleet` runs on `context.Background()` rather than the lifetime ctx, which is cancelled moments later.

A `cache.Refill` into an already-closed cache is a no-op. Never-closed nudge channels plus dedicated stop
signals keep the drain free of ordering hazards.

### An unchecked `tx.ExecContext` inside `Transact` is SAFE - do not "fix" it

Many statements inside `db.Transact` closures discard their error (`tx.ExecContext(ctx, "UPDATE ...", ...)` with no
`if err != nil`). This looks like a bug, has been filed as one twice, and is not: **`sequel.Transact` latches it.**

In `Transact` mode the `Tx` carries `autoErr: true`, so (1) every `Exec`/`Query` records the **first** statement error,
(2) every statement after it **short-circuits** - returning that error without touching the database, so no later write
in the closure can land, (3) when the closure returns `nil`, `transactOnce` sees `tx.err != nil`, returns it, and the
deferred `Rollback` fires, and (4) `Transact` **retries** the whole closure if it was lock contention. SQL Server also
gets `SET XACT_ABORT ON`. Sequel's `Tx` doc states the intent outright: a transaction *cannot* commit partial state
when a caller forgets to check a statement error, and a deadlock is surfaced rather than masked so `Transact` can
retry. Dwarf never calls `BeginTx` directly, so `autoErr` is always on.

The trap for anyone re-deriving this: raw `database/sql` behaves differently, and **that** is what makes the code look
broken. On MySQL and SQLite a failed statement does *not* poison a plain `sql.Tx` - the commit succeeds with that write
silently missing (measured). It is `sequel.Tx`, not the driver, that closes the hole. Any analysis that reasons about
driver semantics without accounting for the `autoErr` latch will "discover" a partial-commit bug that does not exist -
in `Delete`/`Cancel`, in the `cohort_arrivals` bump, in the resume leaf reset, in the reaper's deletes.

What is **not** delegated to the latch, and is still checked explicitly at each site: **`RowsAffected()==0`** - a
zero-row match is a *semantic* outcome (a lost CAS, a terminal-status guard, the resume race gate), not an error, and
the latch says nothing about it.

### Transactions

The multi-statement write paths each run under one `db.Transact`. **There is no single engine-wide row-lock
order** - two disciplines coexist deliberately, and any earlier claim of a uniform "steps-first-then-flow"
ordering was wrong:

- **Flow-row-first (write-first).** Every flow-*advancing* / flow-*terminating* transaction takes the
  `dwarf_flows` row's write lock as its **first** statement, before touching steps: `processStep`'s transition
  evaluation (`advanceFlow` - insert next steps + update the flow's `step_id`, `execution.go`), `completeFlow`,
  and `fireFanInDirect`. This is mandatory, not stylistic - see "Write-ordering & lock contention" above for the
  SQLite SHARED-upgrade deadlock and the `completed`-step-strands-`running` orphan failure mode it prevents (the
  terminating step is marked `completed` in a standalone UPDATE *before* the disposition tx, so the flow row must
  be locked first for the disposition to be recoverable). Do not reorder these to read-first.
- **Steps-first.** The lifecycle mutations update `dwarf_steps` before `dwarf_flows`: `Resume`, `Cancel`,
  `failStep`, `handleInterrupt`, and `Delete`/`Purge` (the deletes run steps-before-flows, ascending id). `handleInterrupt` belongs here despite advancing the flow (running→interrupted): interrupt is
  **non-terminating** and marks no step `completed` in a prior standalone UPDATE, so it carries **no** orphan-strand
  obligation - its only write-first requirement is that the *first* statement be a write (the `UPDATE dwarf_steps`
  satisfies it, keeping the SQLite deadlock closed). It is deliberately steps-first to match `Resume`/`Cancel`,
  which walk the *same* surgraph chain, so the two never lock that chain's flow+step rows in opposite order (the
  former deadlock cycle, now eliminated).

`Create` inserts the flow row, then the entry step, then updates the flow (`created→running`) - flow-row-first in
statement order, but into a **brand-new** flow whose rows no concurrent transaction can reach until commit, so it
honors no existing-row lock order and cannot cycle. `Fork` performs its entire recursive tree clone in one
transaction (so a crash mid-clone rolls back), and the leaf fork step is held `created` until the mapping is complete.

**The two disciplines still cross in one place on row-locking engines, and that is tolerated, not eliminated.**
On MySQL `REPEATABLE READ` and SQL Server without RCSI, a flow-row-first transaction and a steps-first one can
acquire the same flow and step rows in opposite orders and form a genuine lock cycle - the surviving case is
`Cancel` (steps + gap locks, then the flow row) vs the transition tx (flow row, then step insert/`successor_id`)
on one flow. It is **recoverable**: both paths run under `db.Transact`, whose lock-contention retry rolls the
loser back and re-runs the closure, so the cycle degrades to a transient rollback visible only on retry
exhaustion - and the transition side is further backstopped by the `processStep` lock-contention defer (rewinds
the leased step and re-polls) and, last-resort, lease recovery. The recommended `READ COMMITTED` (MySQL) / `RCSI`
(SQL Server) settings drop the gap locks and remove it outright. This last cross is *not* cheaply reconcilable:
forcing the flow-terminating transactions steps-first would reintroduce the SQLite deadlock and the orphan-strand,
and forcing `Cancel` flow-first buys nothing on SQLite (it serializes writes, so its only exposed deadlock is the
read-first-upgrade one the write-first rule already closes) while giving `Cancel` a wider flow-row lock hold.

The former `handleInterrupt` (flow-first) vs `Resume` (steps-first) cross on a shared interrupt chain - the more
dangerous of the two, because it locked *two* overlapping resources (chain flow rows **and** chain step rows) in
opposite order - was **eliminated** by making `handleInterrupt` steps-first (above): with `handleInterrupt`,
`Resume`, and `Cancel` all acquiring that chain's steps before its flows, none can cycle against another. The
move is safe precisely because interrupt is non-terminating (no orphan-strand obligation), and `handleInterrupt`
still shares only the *single* flow row with the flow-first cluster (`advanceFlow`/`completeFlow` never lock a
sibling's step row), which cannot form a cycle - the same reason steps-first `Resume` already coexisted with them.

Orthogonal to that tolerated cross (the deadlock stays retry-recovered; the lock order is **unchanged** - the
transition tx still takes the flow row write-first), the transition must not *extend* a flow `Cancel` already
terminalized. Whenever the race resolves in `Cancel`'s favor - or the transition simply re-runs after a
contention rollback against a since-cancelled flow - its opening write-first `UPDATE dwarf_flows` is **guarded on
non-terminal status** (`AND status NOT IN (completed, failed, cancelled)`) and bails on zero rows. Without the
guard it would insert `pending` successors (and bump `cohort_arrivals` / write a fan-in step) into the terminal
flow - orphan work reaped only later by the claim-time terminal-flow guard. The already-`completed` step is left
as a harmless tail on the final flow. The guard passes `interrupted` (a sibling interrupt must not stop a
completing sibling's transition), so it short-circuits only genuinely-terminal flows.

### Lease-Based Crash Recovery

Transactions don't help when a worker crashes during the `ExecuteTask` call (outside any transaction). The
`lease_expires` column is a crash-recovery lease: the claim CAS sets `lease_expires` to
`NOW + step.time_budget_ms + leaseMargin` (the step's own frozen budget, referenced self-referentially in the
UPDATE - see "Time Budgets"). If the worker crashes, the lease expires and `recoverExpiredLeases` resets the step to
`pending` for re-execution.

### Background Recovery

1. **`recoverExpiredLeases`** (`wedge.go`) - resets `running` steps whose lease expired to `pending`, one UPDATE
   per shard. It rings no doorbell: a reset step is due and `pending`, so its shard's next piston cycle selects it
   like any other. Runs on `recoveryLoop` with everything else here - there is no timer goroutine.
2. **Terminal flow check** in `processStep` - after loading flow data, if the flow is `cancelled`/`failed`/
   `completed`, sets the step to that status and returns. Catches races where the flow went terminal before the step
   was updated.
3. **Orphan flow detection** (`detectOrphanedFlows`, `wedge.go`) - logs an error for a `running` flow that is
   stranded: **every step terminal AND no step touched for `orphanFlowThreshold` (5m)**. Such a flow (every step
   terminal, no successor) is the shape the post-completion transition wedge produces (see "processStep - Normal
   Completion" below). A bug signal; **auto-recovery is intentionally not attempted** - re-driving the flow would
   duplicate transition-evaluation logic and a false positive could double-advance it. The real recovery is the
   `processStep` recovery defer (which rolls the just-`completed` step back to `pending` to re-dispatch); this detector
   is the last-resort alarm for the residual case the defer cannot cover (its own reset UPDATE losing to a contention
   storm). It runs on the same **dedicated `recoveryLoop`** as the wedge sweep (#4) - off `recoverExpiredLeases` for the
   same heavy-scan reason (its `NOT EXISTS` over `dwarf_steps` is latency-tolerant, while the poll is nudged
   sub-second). **Both correctness conditions are on `dwarf_steps`, deliberately.** The age guard used to *be* the
   flow row (`dwarf_flows.updated_at older than the threshold`), but the `touch`-column refactor froze that column
   at go-`running` time (it moves only on a status change), so it stopped tracking per-step progress: it was then
   *permanently* satisfied for any flow running past 5m, leaving the all-terminal check to trip on the brief
   completed->successor window of every ordinary transition (a healthy long-running flow would be alarmed on any
   sweep that sampled it). A step row's `updated_at` still moves on every `pending->running->terminal` transition,
   so the guard now anchors there. The all-terminal check excludes every legitimate long-wait (a `running` task -
   even a 10-minute one with no DB activity - a `pending` sleep/retry, a `running`+parked subgraph caller, an
   `interrupted` human wait), and the no-recent-step check excludes the completed->successor window (the
   just-`completed` step's `updated_at` is fresh, even under persist backoff). The frozen `f.updated_at` is kept
   only as a cheap index pre-filter (`idx_dwarf_flows_status`), narrowing the scan to flows old enough to qualify -
   it can never exclude a real orphan (one has been running longer than the threshold). Logs at error level
   (silent under the default discard logger) **and** increments `dwarf_flows_orphaned{workflow}` - the flow-level
   sibling of `dwarf_steps_unwedged`, on the same "nonzero = latent bug" footing, so an operator can alert on it
   without scraping logs. It is a detection-only alarm: unlike the wedge sweep it does **not** auto-recover.
4. **Parked-step wedge sweep** (`recoverWedgedSubgraphParks` + `recoverOrphanedSubgraphChildren`, `wedge.go`,
   called inline from `runRecoverySweep`) - defense in depth for the `parkedSubgraph` park, whose releasing
   condition could in principle never fire (a parked step is invisible to selection, and `parkedSubgraph` is
   invisible to lease recovery too). Runs on the **dedicated recovery goroutine** (`recoveryLoop`) on a plain
   `wedgeSweepInterval` (5m) ticker; the loop is drained before the pistons in `drainRuntime` since a recovered
   park can re-offer a step. The detector carries a `parkWedgeThreshold` (5m) age guard so steady-state operation never trips a
   false positive (the guard sits comfortably beyond normal subgraph-completion latency). Unlike orphan-flow detection
   this **does** auto-recover, because each recovery re-invokes a *normal, status-guarded* mechanism (the
   `parkedSubgraph` revive CAS, or a subtree `Cancel` guarded by `status NOT IN (terminal)`) rather than duplicating
   transition logic, so it is idempotent and harmless under a concurrent resolution, a false positive, or a peer
   replica sweeping the same shard. It runs **two mirror-image detectors** - a wedged caller (child gone) and an
   orphaned child (caller/parent gone):
   - **`parkedSubgraph`** (`recoverWedgedSubgraphParks`): a caller step `running`+`parkedSubgraph` with **no
     non-terminal child** (`surgraph_step_id = step_id`, status created/running/interrupted) is wedged - the child
     reached terminal but the revive was lost, or the child was deleted. The sweep re-drives the release on the
     latest child (`flow_id DESC`): `completeSurgraphFlow` for a completed child, `deliverSubgraphError` for a
     failed/cancelled/absent one. **The absent-child case (`childFlowID == 0`) is the one the whole sweep exists
     for** - a worker that committed the park and died before inserting the child leaves a step no lease can
     recover (`parkedSubgraph` carries no lease) - and it must skip every child-directed write: aiming them at
     id `0` made `computeFinalState` SELECT `WHERE flow_id=0`, hit `sql.ErrNoRows`, and roll the recovery back on
     every sweep, so the flow hung forever. There is no child to terminalize; the recovery IS failing the caller,
     which the parent re-arm does. Pinned by `TestWedgeSweep_SubgraphCallerWithNoChildFails`.
     `deliverSubgraphError` is **flow-first (write-first)**, not steps-first: it lock-grabs the child flow row
     (`touch=1-touch`, guarded non-terminal) before `computeFinalState` reads, so it can never be the read-first
     half of a SQLite SHARED-lock upgrade deadlock. With no child flow, its first statement is the parent-step
     re-arm - itself a write. (A fan-out has several caller steps, each its own `surgraph_step_id`, checked
     independently; `flow.Retry` leaves older terminal children whose latest sibling is still active - handled by
     the `NOT EXISTS` + latest-child logic.)
   - **orphaned subgraph child** (`recoverOrphanedSubgraphChildren`) - the **mirror image** of the above: a
     non-terminal child flow (`created`/`running`/`interrupted`) whose *parent flow* is already terminal
     (`completed`/`failed`/`cancelled`). Where the `parkedSubgraph` case is a live caller whose child vanished,
     this is a live child whose caller/parent vanished. It is the residue of a `Cancel` that terminalized the
     tree in the narrow window **after the caller step parked but before the child flow was inserted**
     (`execution.go`: the park UPDATE commits, then `createSubgraphFlow` runs), so the teardown - working from a
     scan taken before the child existed - missed it. (A fan-out sibling's `failStep` no longer produces this
     residue: a subgraph child now fails via cohort accounting after every branch settles, never eagerly while a
     sibling is live - see "Failure back to the parent".) The orphan has no path out on its
     own: the terminal root 409s `Resume`/`Cancel`, the child's own key is read-only (400), and
     `recoverWedgedSubgraphParks` is blind because the caller step is *terminal*, not `running`+`parkedSubgraph`.
     The sweep cancels the orphan's whole subtree (`cancelOrphanedSubtree`, which now *shares* `Cancel`'s
     transaction via `cancelSubtree` rather than cloning it - no surgraph up-walk is wanted or exists, since the
     ancestor chain is already terminal; it differs only in taking a zero-row flow UPDATE as a benign no-op rather
     than a 409), sharing the parent's terminal fate. An **`interrupted`** parent is deliberately *excluded* (not terminal - a `Resume` of the root
     revives that branch and a sibling child under it is healthy); the `parkWedgeThreshold` age guard excludes the
     sub-second window where a just-terminalized parent's sibling child is still being cleaned up by the normal
     completion/error path. It counts under `park_type="orphaned_child"`.
   Each unwedge increments `dwarf_steps_unwedged{park_type}` (the always-on alarm; a nonzero value means a
   latent bug let a step wedge) and logs at error level (silent under the default discard logger, surfaced once a
   host injects one).

### Per-Function Crash Analysis

- **Create** - one transaction: insert flow (`running`) -> insert entry step (`pending`) -> set the flow's
  `thread_id`/`step_id`, then ring the doorbell. A pre-commit crash rolls back entirely (no partial flow); a
  post-commit crash before the doorbell is recovered by `recoverExpiredLeases` picking up the `pending` entry step.
  There is no separate `Start`, so no inert `created` window. Self-healing.
- **Resume** - one transaction (steps -> `pending`, flow -> `running`). A crash after commit but before the
  doorbell is recovered by `recoverExpiredLeases`. Self-healing.
- **Fork** - one transaction clones the whole tree (new flow + step rows, id remap, re-parked ancestor callers), with
  the leaf fork step held `created` until the mapping completes, then enqueue. A pre-commit crash rolls back entirely
  (no partial clone); a post-commit crash before the doorbell is recovered by `recoverExpiredLeases`. The original flow is
  read-only throughout, so it is never at risk. Self-healing.
- **Cancel / failStep** - one transaction over the whole surgraph chain. A pre-commit crash rolls back; a post-commit
  crash leaves correct terminal state, `Await` callers discover it on the next poll. Self-healing.
- **processStep - Interrupt** - one transaction. A pre-commit crash rolls back and re-execution produces the interrupt
  again (interrupt-producing tasks should be idempotent). Self-healing.
- **processStep - Normal Completion (with next steps)** - step -> `completed` (a standalone UPDATE), then a separate
  transaction inserts the successors / bumps `cohort_arrivals` / updates `step_id`, then the doorbell. The gap is a
  wedge window: a `completed` step with no successor is invisible to lease recovery (`running`-only), the parked-step
  wedge sweep (`parkedSubgraph`-only), and the lock-contention reset (`status='running'`-guarded), so the flow would
  strand `running` forever. Not only a ~microsecond crash window - the follow-up transaction can fail *persistently*
  (Transact exhausting contention retries under load, or a non-retryable DB error). The **`processStep` recovery
  defer** closes it: on any error return after the step was marked `completed`, it rolls the step back
  `completed` -> `pending` (guarded `WHERE status='completed'`, retried via Transact) so the normal re-dispatch
  machinery re-runs the task and re-evaluates transitions (the failed transaction already rolled back its partial
  writes, so the re-run starts clean). Same reset idiom the defer applies to the `running` -> `pending` lock-contention
  case, generalized to post-completion. Re-execution on recovery is standard (lease recovery re-dispatches too), so
  completion tasks must tolerate re-running. Residual hole: the reset UPDATE can itself lose to a contention storm,
  leaving the step `completed` - surfaced (log-only) by `detectOrphanedFlows` (#3).
- **processStep - Flow Completion (no next steps)** - flow -> `completed` then step -> `completed`. A crash between
  leaves the step `running`; the lease expires, `recoverExpiredLeases` resets it, and the terminal-flow check marks it
  `completed`. Self-healing.

### Database Sharding

`SetShard(index, dsn)` (default: one shard) distributes flows across databases to scale write throughput and reduce
index contention. Indices are sparse (unique, >= 1, not necessarily contiguous - so arbitrary DSNs like cloud RDS
hostnames map cleanly, and a future drained shard's index could retire without renumbering). The sharding
*mechanics* — the sparse index->DB map, the always-parallel `OnEach` cross-shard fan-out,
the not-shard-fault-tolerant contract, and the DSN/test-mode resolution — live in
`internal/database/CLAUDE.md`. This section is the engine-side *semantics*.

**Shard routing & encoding:** external flow IDs encode the shard (`{shard}-{flowID}-{token}`); every operation parses
it and routes via `e.db.Shard(n)`. Indices are 1-based (`1..NumShards`); `0` is the "no shard / all shards" sentinel
used by `Query.Shard`.

**Shard affinity:** subgraph flows are created on the parent's shard (avoids cross-shard references during
subgraph completion and history reconstruction). Only top-level flow creation picks a shard - a
capacity-weighted random pick over non-cordoned shards (see "New-flow placement" above).

**`List` uses per-shard pagination, not cross-shard global order.** Each shard returns up to `ceil(limit/numShards)`
rows by its own `flow_id DESC`; the aggregate is shard-grouped. Cross-shard ordering by `created_at` would compare
different servers' clocks, and by `flow_id` alone is broken (a shard with fewer flows has lower ids). Pagination uses
an opaque cursor encoding each shard's smallest-returned `flow_id`. `List` is strict by design: any shard error fails
the whole call (the per-shard debug path is `ShardInfo` + `List(Shard=N)`).

*Say this out loud in the public docs, do not quietly promise "newest first".* The Operations summary above, the
`List` godoc, `Query.Limit`, and `docs/flows.md` all used to advertise a global newest-first order, which on a
two-shard fleet visibly is not: `List(Limit:100)` returns shard 1's 50 newest, then shard 2's 50 newest, so shard 2's
newest flow follows shard 1's oldest returned one - a reverse-chronological UI renders that as an interleaving
artifact. The fix is the **doc**, not the code: merging by `created_at` (the tempting one) is exactly what the
clock-comparison argument above rejects, and there is no other cross-shard key. A caller needing one ordered view
sorts the page itself - deciding what to trust - or pages a single shard with `Query.Shard`. Pinned by
`TestList_NewestFirstIsPerShardNotGlobal`, which also asserts the global-order violation is *present*, so the code is
never "fixed" back to the old promise by accident.

**`Query.Limit` is a per-shard cap, not a hard total - same honesty, same reason.** `perShardLimit =
ceil(Limit/shards)` (`history.go`) is applied to each shard's query and the results are concatenated with no final
truncation, so a multi-shard page returns up to `shards * ceil(Limit/shards)` rows (`Limit:10` on 4 shards → up to 12;
`Limit:1` → up to 4). `purgeFlows` divides identically, so `Purge` can mark more than `Limit` roots. This is
documented rather than clamped **on purpose**: truncating the assembled slice would advance each shard's cursor
(`nextPerShard[s]`, set from the last row scanned per shard) past rows never handed to the caller, silently dropping
them from the next page - the per-shard cursor is exactly why there is no global OFFSET to truncate against. A caller
needing a strict total truncates its own copy (leaving the returned cursor untouched) or pages one shard at a time.
The public `Query.Limit`/`List`/`Purge` godocs and `docs/flows.md` state this.

**The shard set is immutable at runtime.** `SetShard` is construction-time only: it records index->DSN pairs before
`Startup` (which opens+migrates exactly those shards) and is **rejected** on a running engine, so the set never
changes after `Startup`. Callers therefore size per-shard state from `e.db.Indices()`/`NumShards()` with no
concurrency concern - the sparse indices cannot index a slice directly, so per-shard scratch under `OnEach` uses
`shardOrdinals()` (index -> ordinal position) and writes distinct slice elements, race-free. Changing the set needs a
**coordinated restart** of every replica (a maintenance window), because it is not safe to grow live: a flow key
encodes its shard (`{shard}-{id}-{token}`), so a flow created on a shard unknown to a peer replica is unroutable
(404) there - and there is no cross-replica agreement on the set nor any rebalancing of existing flows (they stay on
their original shard). Doing dynamic growth *correctly* - cross-replica agreement plus rollout sequencing - was
designed but deliberately deferred; until then the set is fixed per process.

## Test-only instrumentation seams (`engine/engineundertest.go`)

Recovery and race paths are hard to trigger on demand (a lost revive, a commit that loses to a contention storm, a
Delete landing in a one-statement window inside another operation's transaction). Rather than forge DB rows or
hammer timing, the engine carries **two white-box seams** a same-package test uses to drive those paths
deterministically:

- **Fault injection** makes a site *misbehave* - a test arms a named fault, the engine consults it at a strategic
  site and simulates the failure (a synthetic error, a dropped signal/doorbell, a stale `lease_seq`, a reap aborted
  mid-tree).
- **Execution checkpoints** make a site *observable and pausable* - `Waiter`/`Wait`/`WaitTimeout` rendezvous with
  the engine reaching a named point; `Break`/`Resume` freeze the engine there until released. They compose to
  drive a concurrent op into a precise window with no timing hammer.

**Both are inert in production by construction:** every consult short-circuits on the `enabled` bool the engine
passes to `seamster.New` (cached from `testing.Testing()` in `NewEngine`), so a production binary pays a single
bool read per site and neither seam can arm or fire. The mechanism lives in the `seamster` package;
`engineundertest.go` holds **only the names** (a pure catalogue folded in beside the test-support constructor
and the `DB`/`Seams` accessors), so the valid set stays discoverable and a test cannot arm a fault or
checkpoint no site consumes. The names stay in `package engine` - unlike the shared test helpers, which belong
in a test-only package - because they are fired from production engine code, not only from tests.

**Consults are written inline at the site they affect, never wrapped in a helper.** A wrapper puts the fault's
effect a jump away from the code it perturbs, which is exactly backwards for a seam whose whole purpose is to be
readable at the point of perturbation. The two that were wrapped are now inline: the lost-delivery consult in
`deliverFlowFailureToParent` (`completion.go`) and the flow-row-write counter's two sites in `execution.go`. Both
guard on `e.seams.Enabled()` first - the lost-delivery one because building its scope needs a **DB read** (a live
binary must run neither the query nor the consult), the counter because formatting the flow id would otherwise
allocate a throwaway scope string on the hot path.

**A rendezvous is two steps - *arm*, then *receive* - and which spelling to use is a question about the CALLER,
not about taste.** Fusing the steps means a test can only arm AFTER the operation it wants to observe is already
running, so every checkpoint reached in that gap is lost. Both workarounds for that were in the tree and both
cost something real: a goroutine calling a blocking wait merely *moves* the race (it may not have registered
before the checkpoint fires) and leaks on timeout; a `Break` closes the race honestly but **freezes the engine**,
perturbing the timing under test and obliging a `Resume`. So:

- **`e.seams.Waiter(name)`** - arms and hands back a channel; receive after the trigger. The only form that works
  when the test's *own next statement* drives the engine to the checkpoint.
- **`e.seams.WaitTimeout(ctx, timeout, name, scope...)`** - arms and blocks, reporting arrival. Use it where the
  engine is *already* running into the checkpoint: after a `Break` froze it there, or with the trigger in flight.
  Spelled `assert.True(e.seams.WaitTimeout(ctx, 10*time.Second, cp), "engine never reached checkpoint cp")` so a
  miss fails with a diagnostic rather than hanging to the suite deadline. (`Wait(ctx, name, scope...)` is the
  same thing bounded only by ctx; `WaitTimeout` is sugar over it for the usual "wait this long" shape, and takes
  its timeout *before* the name so the scope stays variadic and trailing.)

Because `Waiter` makes a rendezvous race-free on its own, **`Break` is reserved for what it is actually for** -
genuinely *holding* the engine while a racing operation runs - and is never armed merely to make an observation
race-free.

**Prefer a checkpoint rendezvous over a status poll.** The one wait that still needs a dwarf-side helper is
`awaitFlowStatus(t, e, flowKey, status, timeout)` (`checkpointhelpers_test.go`), because a flow key is not known
until `Create` returns and the flow can stop before the test can arm. It therefore does both: arm **first**, then
check `Visits`. **That order is load-bearing** - a stop before the call is caught by the count, one after by the
channel, one landing between the two lines by the channel (the waiter is already registered). Reversing them
reintroduces precisely the race `Waiter` exists to remove.

**A targeted seam name is built by `On`** (`engineundertest.go`), which joins a base name with the entity it
targets. A consult site and the test that arms it both call it, which is what keeps the two from spelling the
join differently - the seam itself keys on a plain string and knows nothing about the parts. `signalStop` fires
the stop checkpoint **both bare and targeted at `(flowKey, status)`**: `Checkpoint(ctx, checkpointFlowStopped)`
and `Checkpoint(ctx, On(checkpointFlowStopped, flowKey, status))`. Both are needed because **the two are
different names, and firing one does not wake a waiter on the other** (the same contract faults carry) - the
bare name serves "any flow stopped", the targeted one lets a test wait for one flow while a subgraph child, a
fan-out sibling, or a peer replica's flows stop concurrently. The targeted form is meaningful only because
`signalStop` runs **post-commit**: when it fires the status is durable, so a test reading the row immediately
after sees it - the exact fact a status poll was spinning to sample.

**Every `seamsJoin` call outside a test is wrapped in `e.seams.Enabled()`, without exception.** The join
assembles a string, and a caller assembles it *before* the seam's own gate can short-circuit - so an unguarded
site allocates on every pass in production, for a seam that can never fire there. A consult reads
`if e.seams.Enabled() && e.seams.IsFault(seamsJoin(...))`, whose `&&` short-circuits before the join; a
checkpoint pair puts both fires inside one `if e.seams.Enabled()` block, since the bare fire is a no-op when
disabled anyway. A **bare constant** name costs nothing and needs no guard - that is most sites.

The join is package-local and unexported everywhere it appears (`engine`, `fixtures`, `internal/enginetest`,
`internal/piston`, `internal/peers`), because it exists for tests and has no business on any package's public
surface. The copies must agree on the separator; a drift makes a wait target a name nothing fires, so it times
out and fails loudly rather than going quietly wrong.

Two limits: only stops routed through `signalStop` have a rendezvous (completed/failed/cancelled/interrupted),
and because `Visits` is monotonic a **repeated** status (interrupt → resume → interrupt) is satisfied by the
earlier occurrence, so wait on those with a `Waiter` armed around the specific trigger.

