# Dwarf `engine` package — orchestrator internals

> Load when: editing anything under `engine/` - operations, execution/scheduling, subgraphs, fan-in,
> crash recovery, metrics/tracing, sharding, or the Host seam.
> Coupled with: root `CLAUDE.md` (conventions, Core Concepts vocabulary, radiating landmines) and the
> `workflow/`, `migrations/`, `fixtures/` package docs it cross-references.

### The Host interface (how the engine reaches the outside world)

The graph/task/peer seam is a single **`Host`** interface, registered once via `SetHost`; the
observability providers below are injected separately. A host must implement `LoadGraph` and `ExecuteTask`;
`SignalPeers` may be a no-op. The interface methods:

- **`LoadGraph(ctx, workflowURL string) (*workflow.Graph, error)`** - fetches a workflow graph by name.
  Called at `Create` (and on subgraph spawn); the graph JSON is then frozen on the flow row. The flow's opaque
  baggage rides on ctx (`workflow.BaggageFrom(ctx)`) for identity-dependent loading (authz, per-actor graphs).
- **`ExecuteTask(ctx, taskName string, flow *workflow.Flow) error`** - executes one task. Receives the flow
  carrier with state pre-populated; writes its changes back onto the flow. The engine never knows *how* the task is
  reached (local call, RPC, message bus). Any error the task returns is terminal for that attempt: routed via the
  graph's `onError` transition if one exists, else the step fails. A **panic** in the in-process handler is caught at
  the call boundary and treated as such an error (see "Host-call panic isolation"). The engine never sniffs status
  codes or error text; a task backing off on a transient failure detects that itself and arms `flow.Retry`.
- **`SignalPeers(ctx, op string, payload []byte)`** - delivers one cross-replica coordination signal to the
  other replicas, all fire-and-forget. `op` is an opaque routing key (usable as a topic); `payload` is opaque bytes
  the engine already serialized. The host ships `(op, payload)` to peers and, on the receiving side, hands them back
  via `Engine.DeliverSignal(ctx, op, payload)`, which parses `op` and applies the effect: a work doorbell (`enqueue`)
  or a cross-replica `Await`/status-change wake (`statusChange`). All signal kinds funnel through this one method, so
  adding a new kind needs no host change; the host never branches on `op` or inspects `payload`. A single-replica host
  does nothing here and none of this runs.
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
there, receiving the JSON-decoded form (typically `map[string]any`), like flow state. (Unlike W3C/OTEL request
baggage this is *flow*-scoped and frozen at `Create`, not per-request mutable.) See "Identity / baggage propagation".

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

- **Live** (take effect immediately, callable any time): `SetMaxOpenConns`, `SetWorkersPerConn`,
  `SetTimeBudget`, `SetDefaultPriority`. `SetTimeBudget`/`SetDefaultPriority` are read fresh at each `Create` (an
  existing flow keeps the budget/priority frozen at its own `Create`); `SetMaxOpenConns` (the per-shard connection
  ceiling) and `SetWorkersPerConn` (the pool-sizing divisor) recompute the sizing formula (`poolSizes`) and push the
  two results to every live shard via `ShardSet.SetMaxIdleConns`/`SetMaxOpenConns`; sequel's pool setters are
  hot/atomic - see "Connection pool sizing").
- **Construction-time only** (return an error if called after `Startup`): `SetDSN`, `SetWorkers`, `SetNumShards`,
  `SetHost`, `SetLogger`, `SetMeterProvider`, `SetTracerProvider` (plus the `SetDebugLogger` convenience). Applying
  these on a running engine would mean reopening live connections (`SetDSN`), resizing the worker pool + candidate
  cache (`SetWorkers`), changing a shard count that flow keys already encode (`SetNumShards` - see "Database
  Sharding"), or re-resolving a frozen provider - so the setter **rejects** it with an explicit error rather
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
clean 404 here (it would otherwise nil-deref in `EntryPoint()`); a structurally invalid graph is a 400. And
`Validate()`'s **side effect populates `fanOutToFanIn`**, which the empty-`forEach` fan-in shortcut reads via
`graph.FanInFor` - so without validation an empty `forEach` silently *completes the flow* instead of firing
the fan-in, skipping every downstream task (silent data loss). Because the validated graph (including that
populated map) is frozen into the flow's graph JSON, every later dispatch sees it; validation is once per
create, never per step. The **subgraph-spawn** path validates identically (a nil/invalid child graph fails the
caller step like any `LoadGraph` error), so the child's `fanOutToFanIn` is populated before its JSON is frozen.
One consequence for graph authors: a graph with no explicit transition to `END` (relying on "no matching
transition completes the flow") is now rejected at `Create` - `Validate` requires an explicit `END` edge. `FlowOptions.ThreadKey` (optional) joins the new flow into an existing thread
(any flowKey in that thread; a bad/stale key 404s). The engine has **no creation-time delay** (no `StartAt`):
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

**Cancel** - Aborts a created, running, or interrupted flow. Walks up (`surgraphChain`) and down (`allSubgraphFlows`)
the hierarchy, atomically cancels all steps across all flows, computes `final_state` per flow, and cancels all flows
with per-flow `final_state` via CASE - all in one transaction. Must be addressed by the **root** flow key (a
subgraph-child key is rejected with 400 - see "Subgraph keys are read-only"). Takes a reason string surfaced as
`FlowOutcome.CancelReason`.

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
from the cloned members' terminal states, **excluding the rewound branch** (so the existing fan-in path converges/
fails the fork with no special escalation). `cloneTree` drives this per flow (`cloneOneFlow`) over an explicit
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
task name, state, changes, status, error, timestamp. Subgraph-executing steps have `Subgraph=true` with nested
`SubHistory`. `Step` returns one step by key.

**List** - Queries flows by status, workflow URL, or thread key, with cursor pagination (newest first, default 100).
Returns `ThreadKey` and a `Subgraph` bool in each `workflow.FlowSummary`. By default it returns **root flows only**;
`Query.IncludeSubgraphs` adds subgraph children to the results. Combined with `WorkflowURL` (a graph that runs only
as a subgraph has no root flows under that URL) this locates every run of a graph that executed as a subgraph.
`Purge` ignores the flag and always targets roots only (deleting a subgraph child directly would strand its parent's
surgraph step).

**Continue** - `Continue(threadKey, additionalState)` creates and runs a new flow from the latest completed flow
in a thread, merged with `additionalState` using the graph's reducers - sugar over `Create` for the multi-turn
case. The `threadKey` accepts any **root** flowKey in the thread (a subgraph-child key is rejected with 400 - see
"Subgraph keys are read-only"); `Continue` resolves the thread via `thread_id`, finds the
latest **non-fork** flow (`ORDER BY flow_id DESC` with `forked_from_step=0`), validates it is completed, and
creates the new flow in the same thread with the same graph, returned **`running`** (like `Create`). The fork
exclusion keeps a debug `Fork` (which shares the thread's `thread_id` for `List` grouping) from ever becoming a
production continuation base. The prior turn's `final_state` passes through unfiltered as the new flow's initial
state; a workflow author wanting narrower carryover scrubs with an entry adapter task using
`flow.Delete`/`Transform`. As a **derived** operation `Continue` takes no `FlowOptions`: it **inherits the
thread's policy** (priority/fairness/budget/baggage) from the latest turn; a caller wanting different policy
uses `Create` with `FlowOptions.ThreadKey` (explicit policy, same thread).

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

**The inbound peer entry point `DeliverSignal(ctx, op, payload)`** is the receiving side of cross-replica
coordination: the host adapter calls it with the `(op, payload)` it received from a peer, and the engine parses `op`
and applies the effect. The outbound side is the host's `SignalPeers`. The **enqueue doorbell** (op `enqueue`) is the
most frequent: it signals that a step is pending. The receiving replica does one PK lookup for the announced step's
`priority` and `not_before`.
If `not_before` is in the future the doorbell defers to the poll timer (`shortenNextPoll(not_before)`); if due, the
priority drives the cache offer (refill or head-insert; see "Execution Model"). It does not enqueue a specific step
into a queue. Fire-and-forget - a missed doorbell is recovered by `pollPendingSteps`.

### Subgraph keys are read-only

A subgraph child flow has a real flowKey (a task inside it reads its own via `flow.FlowKey()`, and `List` with
`IncludeSubgraphs` surfaces it), but that key is a **read** handle, not a write unit: a child cannot be mutated
independently because its parent is parked waiting on it, and the unit for any lifecycle change is the whole tree.
So the **lifecycle mutations reject a subgraph-child key with 400** (`surgraph_flow_id != 0`): `Resume`, `Cancel`,
`Delete`, `Continue`. The rejection is folded into each operation's existing flow-row SELECT (no extra round-trip;
the 404-not-found check still takes precedence). The caller addresses the tree by the **root** key instead - which
it always holds (it came from `Create`/`Run`/`Continue`, or from `List` of roots). The rationale per op: `Resume`/
`Cancel` are inherently tree-wide (they walk up to the root and down), so a child key is just a confusing alias for
the root; `Delete` cascades *down* only, so deleting a child directly would strand the parent's surgraph step; and
`Continue` on a child's own (private) thread would spin up a detached top-level flow from the subgraph's final state,
not a thread turn. **Reject, not silently widen** - widening `Delete(childKey)` into a whole-tree delete is a
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
the payload into `State` was lossy (the caller could not tell workflow state from the resume request). Callers wanting
the merged view call `workflow.MergeState(out.State, out.InterruptPayload, graph.Reducers())` themselves.

### Flow-stop notification is not an engine concern

The engine has **no** stop-notification mechanism: there is no `FlowStopped` callback, no `NotifyOnStop`
option, and no `notify_on_stop` column (all removed). A caller that wants to learn a flow's outcome either
**`Await`s** it (blocking, bridges the workflow clock to a synchronous caller) or **composes** the
notification into the workflow itself - an orchestrating graph whose final task reports the outcome to the
upstream (with `flow.Retry` for durable delivery). This keeps notification policy and transport entirely in
the host/author, matching the engine's "carry facts, not policy" posture (baggage, signals). See `_FLOWSTOP.md`
for the rationale and the follow-on plans (cancel/interrupt-as-error, deferred deletion).

### Execution Model

The engine uses a **queue-as-cache execution model** with a configurable worker pool (`SetWorkers`) and a single
refiller goroutine per replica. The in-memory `candidatecache.Cache` is bounded and holds *hints*, not ownership. Each
worker pops a candidate and calls `processStep`:

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

**Selection (two-level priority + fairness).** The refiller, not the worker, decides *what* runs. (1) Each shard is
scanned for its strict-minimum `priority` band's due pending rows in one statement (the band is a
`priority=(SELECT MIN(priority) ... due)` subquery, so band and candidates are self-consistent within the statement;
not transactional vs concurrent worker CAS claims, which self-corrects via the post-completion refill and backlog
poll). (2) Rows are aggregated: the *global* minimum band across shards is taken (strict priority is cluster-wide)
and only rows at that band form one `fairness_key` population - shards with a worse band contribute nothing this
batch (lower bands are never materialized until the higher drains, by design). (3) Repeatedly weighted-random pick a
key (Efraimidis-Spirakis over the *keys*, not the rows) and take that key's *oldest* remaining step until the batch
is full - FIFO within a `fairness_key`. `created_at` (read as an age, comparable across shards) does two things per
key: fixes the key's `fairness_weight` from the key's oldest step (so a tenant cannot self-promote with newer
high-weight tasks) and orders dispatch oldest-first within the key. It is the only ordering signal comparable across
shards: `step_id` is a per-shard auto-increment, so a `(shard, step_id)` order would let a brand-new task on a low
shard jump an old task on a high shard for the *same* tenant (unbounded intra-tenant starvation). The age is
`DATE_DIFF_MILLIS(NOW_UTC(), created_at)` per shard, and `created_at` defaults to that shard's `NOW_UTC()` at insert
- both terms on one shard clock, so per-shard clock offset *cancels exactly*; no inter-shard clock-skew term in
`ageMs`. The only residual is the few-ms dispersion in *when* each shard runs its age query (the per-shard scans run
in parallel within one refiller pass), a soft, self-correcting reordering of one tenant's own queue - not a fairness
violation (the weighted *key* pick governs cross-tenant fairness) and not a correctness issue (the CAS arbitrates).
Same-age ties break by `(shard, step_id)` for determinism. The pick is re-rolled per step so expected dispatch share
is proportional to weight and independent of backlog depth or shard layout. Strict priority means no aging: a fed
higher-priority band starves lower bands by design.

**Queue-as-cache, doorbell, single-slot refiller.** The enqueue signal carries no step to a queue; it is a **doorbell**
(`candidatecache.Cache.Offer`). It resolves the announced step's priority *and* `not_before` in one PK lookup (off the
selection path). If `not_before` is in the future the doorbell short-circuits into `shortenNextPoll(not_before)` -
the work is not due, nothing to preempt, the cache stays untouched; the local poll timer wakes at the right moment.
This is also how cross-replica delayed-start propagates: every replica receiving the doorbell pulls its poll timer
forward, with no separate "wake at T" message. Otherwise the priority drives one of three cache paths. (1) Empty
cache: this replica is idle - request a refill so the refiller selects the strictly-best step. It deliberately does
**not** head-insert the first arrival, because an arbitrary-priority step jumping the queue on an idle replica can
run before a more important one (this exact inversion was observed; the cost is one refiller scan of idle-wake
latency). (2) Non-empty and not strictly more important than the cached band (priority >= floor): no-op - a steady
same-or-lower-priority stream is pure cache hits. (3) Non-empty and strictly more important (priority < floor):
**head-insert that exact step** so the next pop runs it without a refiller scan, lower the floor, wake one waiter,
and request a refill to top up the band. Case 3 - an urgent arrival preempting cached lower-priority work -
deliberately does **not** flush the existing candidates: a guiding principle is that high throughput trumps exact
priority ordering. Flushing would idle every worker through the refill scan to guarantee zero lower-priority
executions after a higher-priority arrival; instead the workers keep draining and the refiller's wholesale replace
re-establishes strict band order within one cycle. Exact ordering is soft anyway - with N replicas draining
independently there is no global order to preserve. The cache is bound to `size` by trimming the tail on insert; a
trimmed step stays `pending` and is re-selected. A single refiller goroutine plus a buffered(1), never-closed,
non-blockingly-sent `refillTrigger` is the single-slot selection gate: concurrent requests (worker low-water, timer
poll, doorbell) coalesce into at most one pending scan, and the send can never panic, even during shutdown drain.

**One pioneer is sufficient; the head-insert is a bridge, not a per-job fast path.** A head-insert is accepted at
most once per band-opening: it lowers `floor` to the pioneer's priority, so every subsequent arrival at that band
hits `priority >= floor` and is rejected (case 2). Deliberate, not starvation. The pioneer bridges the single
refiller-cycle gap so the *first* urgent step does not eat a refiller scan of latency. Its `requestRefill` makes the
refiller scan band MIN and `refill()` **wholesale-replace** the cache with the strict, weighted batch of that band,
*evicting* the cached lower-priority candidates (they stay `pending`, re-selected when the band drops back). After
that cycle the refiller serves the whole band correctly and fast - no further head-inserts. A non-pioneer
high-priority step that misses the head-insert (stale `floor`) is **not** stuck behind the backlog: the refiller
selects band MIN, so it is picked up after at most ~`lowWater` lower-priority pops plus one scan - a bounded
fast-path *miss*, never priority starvation.

**Bounded bridge-window leakage is deliberate and self-healing.** Between the pioneer head-insert and the async
`refill()` landing, workers keep popping the still-cached lower-priority steps. The leak is bounded by ~the worker
count, not the backlog: a refiller scan is one DB round-trip while a worker that pops a step is then busy executing
it for its full duration, so each worker leaks at most ~one lower-priority step before the replace evicts the rest;
the pioneer itself is at the head and never delayed. The head-insert also bypasses the weighted fairness for exactly
that one pioneer step (the first work of a just-opened band, bounded to one per escalation, restored by the next
batch). Both costs are smaller than the cross-replica fairness softness the design already accepts. Do not "fix"
these by flushing, per-item priority tracking, or re-floor-on-pop: each trades the latency win the head-insert
exists for and only shaves an already-bounded refiller cycle off a path the refiller already backstops.

**Liveness guarantee.** A worker requests a refill *after* `processStep` returns - i.e. after the step left `pending`
(acquired or completed) - not at pop time. Load-bearing: requesting before the CAS let the refiller re-select the
in-flight step and, under single-slot coalescing, never scan post-completion state, wedging a single-worker replica
with a backlog. Post-completion the next refiller scan always reflects every freed slot. The worker also requests at
the low-water mark so draining overlaps refilling. The cache holds 2x the worker count, low-water is half that.

`pollPendingSteps` does not enumerate the backlog onto a queue. It recovers expired-lease steps, sizes the wake
timer to the nearest future `not_before`, and rings the local doorbell each cycle. (Orphan-flow and parked-step
wedge detection are *not* here - their heavy `NOT EXISTS` scans run on the separate latency-tolerant `recoveryLoop`;
see "Background Recovery".) If a
due-pending backlog exists it caps `nextPoll` at `backlogPollInterval` (1 minute) so an idle replica that got no
doorbell still re-scans. This is a coarse safety net, not the primary wake path: due work is normally picked up
immediately by the completion doorbell, and `nextPoll` is shortened to anything sooner.

**Sizing-error clamp.** A sizing SELECT in `pollPendingSteps` can *fail* (a transient DB error - most commonly a
momentary connection-limit rejection under load; see "Per-test engine + sequential execution" in `fixtures/CLAUDE.md`). The error must not be swallowed into
a "nothing pending" reading, because that would size `nextPoll` to `maxPollInterval` (5 minutes) while a due step
sits undispatched - a multi-minute wedge that a transient blip turned into a stall. So when any shard's sizing query
errors, `pollPendingSteps` clamps `nextPoll` to `pollErrorRetryInterval` (1s): re-poll promptly, ring the doorbell
again, and let the step dispatch once the blip clears. The clamp is the engine's own resilience to transient DB
errors; it never *retries the connection* (that is left to the layer holding the resource - the task or host), it
just refuses to convert an unknown backlog into a long sleep.

**The refiller applies the same clamp.** `runRefill`'s `scanPriorityBand` can fail on the same transient DB error,
and swallowing it is the mirror wedge: the cache refills **empty**, every worker blocks in `Pop`, and nothing retries
until the next doorbell or the backlog backstop (up to 1m). So `runRefill` logs the scan error and
`shortenNextPoll(now + pollErrorRetryInterval)` (1s) - the doorbell fires again promptly and the refiller re-scans
once the blip clears, rather than idling the whole replica on a momentary blip. Same policy, same 1s interval as the
poll clamp above.

The timer waits on the `nextPoll` deadline, shortened to the nearest future `not_before` (`flow.Sleep` / retry
backoff) so a due step wakes the replica even when no doorbell arrives. The timer loop (`timerLoop`) runs
`pollPendingSteps` on the adaptive interval.

**`shortenNextPoll` treats a past `nextPoll` as "needs rescheduling," not just lower.** If `nextPoll` already lies in
the past (a fired deadline the timer is mid-poll on), a strictly-lower-only update would drop a wake request arming a
*future* `not_before`, and the in-flight poll then clobbers `nextPoll` with its far `maxPollInterval` default - an
intermittent sleep/retry **wedge** (every worker parked on the cache, the timer asleep on the stale far deadline). So
it updates when `tm` is sooner **or** `nextPoll` is already past (the exact predicate is at `shortenNextPoll` in
`engine.go`). Reproduced as ~1-in-40 timeouts of `fixtures/sleepretrycomposeflow_test.go` under stress before the fix.

### Query Parallelism

`processStep` is the hot path. Independent queries within it run in parallel (errgroup-style) to minimize latency on a
remote database:

- **Claim UPDATE + step SELECT** - on pgx/sqlite/mssql the claim and read are **one** round-trip via
  `RETURNING`/`OUTPUT` (reads the row *as updated* - a consistent snapshot). MySQL lacks RETURNING, so they are two
  statements run **serially, not in parallel** (claim first, read only on success): parallelizing on separate
  connections is unsafe - an independent read snapshot can observe the pre-transition row and deliver an empty resume
  payload (the reason is spelled out at the claim site in `execution.go`). The lease size comes from the step row's
  own `time_budget_ms` (referenced self-referentially in the claim UPDATE), not a pre-SELECT.
- **Flow data** - runs after the claim+read, since it needs the `flow_id`.
- **Fan-in sibling counts** - the unfinished and failed sibling COUNT queries run concurrently.
- **Subgraph status counts** - the active and completed subgraph COUNT queries run concurrently.

**Transaction constraint:** functions receiving a `sequel.Executor` (which may be a transaction) cannot parallelize
because SQL transactions are not safe for concurrent use. This applies to `computeFinalState` and code inside
`failStep`/`Cancel` transactions.

### Fan-Out and Fan-In

**Static fan-out** occurs when multiple transitions match from one task. All targets run in parallel, sharing a
`step_depth`. The flow's `step_id` is `0` during fan-out.

**Dynamic fan-out** uses `forEach` on a transition to iterate a state array and spawn one task instance per element,
each receiving the element under the `as` key. An empty array spawns nothing; when `forEach` is the only outgoing
transition, an empty array completes the flow there - downstream tasks (including the fan-in target) are never
reached.

**Branch state strip on dynamic fan-out.** When spawning `forEach` branches, the engine removes the source array
field from each branch's local `state` (only the local state - the spawn step's immutable snapshot keeps it). Without
this, an N-element forEach feeding `forEach -> A -> B -> C -> J` would write N copies of the array into every step row
in every branch, blowing storage up by N times the chain length. The fan-in step rebuilds its state from the spawn
step's `state + changes`, so the source array reappears at the fan-in and downstream - the absence is local to the
cohort. The strip is skipped when `as == forEach` (the alias named the same as the source). The engine also injects
two read-only fields per branch: `<as>Index` (position in the array) and `<as>Count` (cohort size), so the branch
reads its ordinal context without the source array.

**Downstream suppression via explicit clear.** A branch that wants to suppress the source array past the fan-in calls
`flow.Set(<forEach>, nil)` in its body. That writes the new value into the branch's `changes`, the replace reducer
at fan-in folds it over the spawn-step base, and the field is absent (or whatever the branch wrote) past the fan-in -
useful for a forEach over a very large array where downstream tasks only care about the per-element transformation.

**Fan-in strip on dynamic fan-out.** `insertFanInStep` deletes `<as>`, `<as>Index`, `<as>Count` from the merged state
after the cohort converges. The injected per-branch bookkeeping has no meaning past the fan-in: with the Replace
reducer, one branch's element value and index would otherwise win arbitrarily and ride forward. The names to delete
are recovered by walking the spawn task's outgoing `forEach` transitions (`tr.As`); static fan-outs have no `as`. A
workflow wanting the element value past the fan-in must forward it under a different key.

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
  `successor_id=Z`. The exit set is *logically* `lineage_id == cohortSpawnID AND task_name IN` the graph-predecessor
  tasks of the fan-in - **not** the whole lineage, so `A`/`B` in `forEach->{A->B->C}->J` are excluded and only the
  `C`s point at `J`. The `successor_id` write targets those exit steps **by primary key** (ids collected during
  `insertFanInStep`'s cohort-member merge scan), not by the `(flow_id, lineage_id, task_name)` predicate - which is
  unindexed and deadlocked concurrent fan-ins on SQL Server (the deadlock story is at that write in `execution.go`).
- **flow.Retry**: rewinds the step in place (same row), so `predecessor_id` is preserved. (A `Fork` copies the
  step into a new row and remaps `predecessor_id` to the cloned predecessor.)
- **Entry / subgraph-entry steps**: `predecessor_id` defaults to 0.

The Mermaid renderer ignores `step_depth` and `lineage_id`: it draws the deduped union of `{predecessor_id -> step}`
and `{step -> successor_id}`, exact for arbitrary nesting. Heads are nodes with no incoming edge, tails with no
outgoing.

`computeFinalState` also reads the DAG, not `step_depth`. The terminal state is the merge of the tail steps -
completed steps with `successor_id = 0` (`mergeTerminalSteps`). The earlier `MAX(step_depth)` heuristic was wrong for
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

**`lease_seq` is bumped only where a lease is *granted* — the claim CAS.** `pollPendingSteps`' expired-lease
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
- **fail** (`failStep`) — gated by the step-fail UPDATE, the transaction's first write, so a zero-row match
  wrote nothing: it commits the empty tx and returns `fenced=true`, and `failAndReturn` surfaces `nil` so the
  flow the peer is re-running is never failed. This is finding-#1's "late error → healthy-flow kill", closed.
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
and is readable by any flow reader (`History`/`Snapshot`/`List`), and internal stack traces are code-structure
disclosure; the handler still gets the message, status code, trace id and properties for routing. The handler task
becomes the next step, and the failed step is marked `completed` with its changes preserved. (The `error` *column*
already carries only `err.Error()` - the message, no frames.) Fan-out siblings are **not** cancelled - the errored branch
continues down its handler path and rejoins the cohort as a normal arrival (convergence is by cohort arrivals, not by
cancellation). If there is no `onError` transition, the step fails via `failStep`.

**Fan-out sibling constraint:** `Graph.Validate()` (via `validateLineage`) enforces that all branches of one
fan-out **converge on the same fan-in node**. The cohort shares a spawn step, and the cohort-resolution path in
`processStep` picks the fan-in target from whichever sibling completes *last*, so branches routing to different
fan-in nodes would make the convergence node depend on completion order (nondeterministic). The check rides the
existing `fanOutToFanIn` mapping: `validateLineage` records each fan-out source's fan-in as branches pop the
lineage frame, and a second branch popping the same source to a *different* fan-in is the violation. (Divergent
non-fan-in *immediate* targets that still converge on one fan-in are fine - each sibling spawns its own next step;
only the shared fan-in target must be single-valued.)

### State Across Subgraphs

**Subgraph is a function call.** The signature is `flow.Subgraph(url string, in any, out any) (yield bool, err
error)`. Only the explicit `in` passed in crosses into the child as its initial state; only the explicit `out`
target (the child's `final_state`) crosses back. The parent's state and accumulated changes do NOT auto-cross either
direction. `in` is any JSON-marshalable value (a struct or a `map[string]any`), normalized to a state map via
`toStateMap` (nil → "no arguments"); `out` is a pointer (a `*struct` or `*map[string]any`) the child's `final_state`
is unmarshaled into by JSON tag (`parseMapInto`), or nil to ignore the result. A typed struct on either side gives
field-level type safety without manual `map[string]any` casts.

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
the signal instead of idling to the lost-wake poll backstop - `failStep`'s subgraph-child branch signals exactly
as the top-level path does. The load-bearing
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
the backstop, woke it.

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
without importing `engine`. The create-time injection round-trips the value through JSON (`baggageMap`) so the
loader sees the same decoded shape every dispatch will.

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
logged, or shared key is a full write capability for that one flow. The amplifier is therefore `List`/
`Search`, which return keys wholesale (a separate concern) - not the token itself.

**Token entropy (64-bit) and the every-gate-is-`flow_id`+`flow_token` invariant that makes it adequate** live in
`internal/keys/CLAUDE.md`. Load-bearing for this section: the token is never a standalone lookup, so a leaked key
is a full write capability for exactly one flow and nothing more.

**Related engine-side hardening (all verified, none a substitute for host authz):** uniform not-found on
key mismatch (no existence oracle); telemetry carries only the token-free correlation id and there is no
correlation-id→key lookup (see "Tracing"); subgraph-child keys are read-only for lifecycle mutations (see
"Subgraph keys are read-only"). The engine cannot go further - it never sees the principal.

### Await

`Await` blocks until a flow stops (no longer `created`/`pending`/`running`); it returns on `completed`/`failed`/
`cancelled`/`interrupted`. It registers a buffered channel in the `waiters` map, then loops: check state, return if
stopped, otherwise `select` on the channel, a periodic `awaitPollInterval` (5s) ticker, or context cancellation.
Non-terminal notifications (e.g. `running` from `Create`) re-check state rather than returning early. The ticker is a
**safety net, not the primary wake path**: `signalStop` is a post-commit, in-memory, fire-and-forget wake that can be
*lost* - a worker crash between committing the terminal status and signaling, a dropped peer broadcast, or a no-op
`SignalPeers` on a multi-replica host - which would otherwise block the waiter until its ctx deadline (forever on a
deadline-less ctx) while the flow already sits stopped in the DB. The periodic re-snapshot bounds that worst-case
hang to one interval. It is deliberately coarse (5s) because the signal is the fast path and the poll only backstops
the rare lost wake, so its steady-state cost is one PK snapshot per interval per blocked `Await`. (`awaitPollInterval`
is a `var`, not a `const`, only so a test can shorten it.)

**Shutdown wakes waiters with a sentinel, not an empty string.** `drainRuntime` (after draining workers/timer/
refiller, so no goroutine will ever signal again) fans a single `awaitShutdownSignal` sentinel out to every
registered waiter channel. `await` returns a "shutting down" error (503) on receiving it. The sentinel matters
because the loop distinguishes real stop statuses via `stopped()`: an empty-string wake (the pre-fix value) reads
as "re-snapshot," so a waiter on a *still-running* flow re-snapshots (a `running` read on the not-yet-closed DB),
re-blocks on a channel nobody will signal again, and escapes only when its own ctx expires - or, incidentally, up
to a full `awaitPollInterval` later, when a ticker re-snapshot happens to hit the now-closed DB. The sentinel send
is non-blocking (`select … default`): a real stop status already buffered on the channel wins, and the waiter
returns that outcome instead (the snapshot at the loop top catches it). Shutdown drains waiters *before*
`e.db.Close()`, so the sentinel path never touches a closed DB. Pinned by `fixtures/awaitshutdownflow_test.go`.

**Cross-replica `Await`.** A flow created on one replica but completed on another wakes a local `Await` only via the
`SignalPeers` broadcast (op `statusChange`). Every flow-stop site calls an internal `signalStop` helper that does the
local waiter wake *and* the peer broadcast; the receiving replica's `DeliverSignal` routes it to `notifyStatusChange`, which wakes
its local waiters. Without this wiring, an `Await` on the replica that did not run the final step would rely solely
on the `awaitPollInterval` re-snapshot (above) to notice the DB-committed stop - the broadcast is the *fast* path
that avoids that up-to-5s wait, while the poll is the backstop when a broadcast is dropped (or a host leaves
`SignalPeers` a no-op). Non-terminal (`running`) transitions are notified locally only, matching the
broadcast-only-on-terminal-stops policy.

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
  then `shortenNextPoll(time.Now())` to re-poll immediately. Both halves are load-bearing: `pollPendingSteps` only
  recovers running steps whose lease has *already* expired, and a freshly leased step holds a minutes-long lease.
  Without the lease reset, the immediate poll finds nothing and the step (and its fan-in) stalls until the lease
  lapses. The reset is guarded by `WHERE status='running'`, so only the leased-and-uncommitted case is rewound.

### Database Choice and Configuration

Choosing a SQL dialect (Postgres/MySQL/SQL Server/SQLite tradeoffs under concurrent write load), the shard-count
guidance table, the shard-per-server production topology, and the per-server connection-budget reasoning all live in
`internal/database/CLAUDE.md`. The engine-side concern is the connection **pool sizing** — the *formula*, which is
engine policy — below.

**Connection pool sizing - worker-proportional, shard-aware.** The per-shard pool is **derived from the worker
pool**, not a flat constant, because a worker holds a connection only during the short DB segments of `processStep`
(the long `ExecuteTask` call holds none), so connection *demand* is bounded by the workers, not the shard count.
`calcConnPoolSizes` (in `poolsize.go`) computes, per shard:

```
idle    = max(2, ceil(workers / shards / workersPerConn))   // demand-sized; the 2 is poolFloor
maxOpen = min(idle*2 + 2, perShardCap)                      // warm core + burst headroom, under the ceiling
idle    = min(idle, maxOpen)                                 // clamp idle to a tight ceiling
```

- **`SetWorkersPerConn` (default 8)** is the formula divisor - the assumed workers sharing one connection (its
  inverse is the DB-hold duty cycle). Raise it for DB-light workloads (remote `ExecuteTask` dominates), lower it for
  DB-heavy ones. A *larger* value yields a *smaller* pool - the pool is derived from the workers, it is not a cap on
  them.
- **`SetMaxOpenConns` (default 8)** is a **per-shard ceiling** (= per-server budget in the distributed topology),
  not the pool size: the formula sizes the pool and then clamps to this. It **must be ≥ 1** - there is deliberately
  no "unlimited" sentinel (a `0` to `database/sql` means *unlimited*, a footgun; pass a high value like 1000 for an
  effectively unbounded ceiling). The default of 8 pins the common single-shard case to exactly `8/8`
  (`maxIdle == maxOpen == 8`) and keeps the new default **≤ the old flat `8×shards` at every shard count**. Worked
  table (workers=64, wpc=8, cap=8): `1 shard → 8/8 · 2 → 4/8 · 4 → 2/6 · 8 → 2/6`. The per-replica total grows with
  shards, correct under distributed PROD (more servers, each within its own budget). A high ceiling is harmless:
  connections open lazily on demand, and demand is bounded by the worker pool (~`workers` concurrent DB holders per
  shard), so the pool never opens more than that however high the ceiling - no explicit clamp needed.
- The pool floor (2) gives each shard a little warm headroom for balls-in-bins distribution variance and per-flow
  fan-out/subgraph-affinity bursts (a single flow's fan-out concentrates on its one shard). `maxOpen = idle*2 + 2`
  (the `+2` matches sequel's singleton sizing) supplies burst headroom; connections opened above `idle` during a
  burst close on return.
- **`MaxIdle == MaxOpen` no longer holds** (it was the old flat model) - there is now an idle core plus burst
  headroom. The "avoid reconnect churn" goal the old equal-sizing served is now met by the warm idle core plus the
  recycle/drain timers below.

The idle-drain / lifetime-recycle connection timers (`ConnMaxIdleTime` / `ConnMaxLifetime`, server-drivers only)
are a database-layer mechanism — see `internal/database/CLAUDE.md`.

**Live re-sizing.** `SetWorkersPerConn` and `SetMaxOpenConns` recompute the formula (`poolSizes`) and push the two
resulting integers to every open shard via `ShardSet.SetMaxIdleConns`/`SetMaxOpenConns`. The shard count itself is
not a live knob (`SetNumShards` is construction-time only), so it never triggers a re-size: each shard is sized once
at open using the final `numShards` (set before any shard opens), and stays that size for the engine's life.

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
- **Fairness weight is denormalized at `Create`, never resolved on the selection path** (a resolver hook would put
  synchronous I/O on the hot critical section). When a key's steps carry inconsistent weights, the oldest candidate
  step's weight is used; keeping weights consistent for a key is the caller's responsibility.
- **`Workers` is a generous static cap.** A worker blocked on a `ExecuteTask` call is just a goroutine stack plus a
  socket, so over-provisioning is cheap.
- **Completion writes are deliberately not gated by the refiller slot.** That slot bounds selection only; finishing
  in-flight work must outrank starting new work, so the post-execution advance is never serialized behind selection.

> Observability note: per-priority backlog/age and distinct-fairness-key counts are aggregate-only metrics by design
> (per-key labels would be unbounded cardinality). Metric emission is deferred in the engine and is a host concern;
> the engine exposes the underlying data through logging and return values.

### Step Parking (`parked` column)

`dwarf_steps.parked SMALLINT NOT NULL DEFAULT 0` takes a step out of the selection band without changing its
`status`. The selection index `(status, parked, priority, fairness_key)` and saturation index
`(status, parked, task_url)` lead with the partitioning columns, so parked rows are physically excluded from every
hot-path scan - no in-memory filter at refill time. The `parked` value labels *why* the step is held:

- `parked=0` (`parkedNone`, default) - active. Selection sees it; `pollPendingSteps` recovers it if its lease
  expires; saturation counts it as one in-flight slot. (Also the precondition the claim CAS requires.)
- `parked=1` (`parkedSubgraph`) - the step called `flow.Subgraph` and is waiting for the child. `status='running'`
  (logically running, blocked on its child) but excluded from selection, saturation, AND lease-expiry recovery. No
  lease deadline - the row sits until `completeSurgraphFlow` flips it back to `(pending, parked=0)`. This replaced an
  earlier `lease_expires = NOW + 7 days` "park" indicator that broke for subgraphs running longer than 7 days
  (the lease lapsed, the parent recovered, the task re-ran, launching a duplicate child).

**Terminal status implies `parked=parkedNone`.** The park value is meaningful only while a step is actively waiting.
Once terminal (`completed`/`failed`/`cancelled`), the park slot is gone, and the column must read `parkedNone`. Every
terminal-transition code path resets `parked` in the same UPDATE (the `failStep` write, the `deliverSubgraphError`
child-leaf write, the `Cancel` cascade, the `processStep` terminal-flow guard). Without this, a step that was parked
when its flow was cancelled would sit terminal with non-zero `parked` - invisible to the selection index but never
re-leased. A `Fork` clone writes each step's `parked` explicitly (the re-parked ancestor callers to `parkedSubgraph`,
all other cloned steps to `parkedNone`), so cloned rows never inherit a stale non-zero `parked`.

## Metrics (`engine/metrics.go`)

The engine emits 10 `dwarf_*` instruments through the **OTEL metric API** (not the SDK). `SetMeterProvider`
injects the provider; it defaults to the global `otel.GetMeterProvider()` - no-op unless the host configures the
SDK, so unconfigured/standalone/test use pays nothing. Instruments are built once in `initMetrics` (from
`initRuntime`, so both `Startup` and `RunInTest` get them) from `mp.Meter("github.com/microbus-io/dwarf")` - that
scope distinguishes dwarf's metrics; **service identity lives in the provider's Resource, not in per-metric
attributes** (no `service.name` on data points - cardinality explosion, off-spec). The only attributes attached are
the metric-specific labels: `workflow`, `status`, `task_name` (on `dwarf_steps_executed`), `task_url` (on
`dwarf_task_concurrency_running`), `priority`, and `park_type`.

**5 counters, incremented inline** at their logical event sites: `dwarf_flows_started`
(start path), `dwarf_flows_terminated` (completeFlow), `dwarf_steps_executed` (every terminal step
disposition - completed/failed/interrupted/subgraph/retried/error_routed), `dwarf_steps_recovered`
(pollPendingSteps lease recovery), and `dwarf_steps_unwedged{park_type}` (the parked-step wedge sweep; a
nonzero value flags a latent bug). The inline helpers no-op when `e.metrics == nil` (before Startup).

**Counter instrument names carry no `_total` suffix.** `_total` is a Prometheus naming convention, not an
OpenTelemetry one: a Prometheus exporter appends it to every counter at the scrape boundary (and
de-duplicates, so a name already ending in `_total` is not doubled), while the OTLP push path uses the
instrument name verbatim. So the instruments are named `dwarf_flows_started` etc., and a Prometheus query
references them as `dwarf_flows_started_total`. Do not bake `_total` into a counter's instrument name.

**5 gauges, observable (async)** via a single `RegisterCallback`. The callback runs at metric-collection
time and reads engine state: in-memory for
`dwarf_steps_queue_depth` (cache length) and `dwarf_steps_fairness_keys` (the last refill's selected band +
distinct-key count, stashed under `lastRefillLock` by the refiller); shard queries for `dwarf_steps_pending`
and `dwarf_steps_oldest_pending_age_seconds` (per priority band) and `dwarf_task_concurrency_running` (running
steps per task). Gauges emit **per replica**; cluster-wide aggregates are summed at the backend. The callback is
`Unregister`ed first thing in `drainRuntime` so the OTEL reader can't query a closing database.

**Fidelity choices:** `flows_terminated` fires only on `completed` (failed/cancelled are not counted here;
`steps_executed{status=failed}` still covers the failed-step case). Subgraph flows are counted too - the start
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
in-memory waiter-match key (`signalStop`/`notifyStatusChange`, never leaves the process). No log line emits a
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
that came due while a replica was down is removed on the next tick (single-replica) or by a peer's tick. Backed by a
partial index `idx_dwarf_flows_delete_after WHERE delete_after_ms > 0` (full on mysql) that narrows the due scan to
the small in-window subset. (`deletionGrace`/`reapInterval` are `var` not `const` only so a test can shorten them -
engine-package tests force a reap via `reapDueFlows`; fixtures verify the observable public contract via `List`/
`History`/`Snapshot`.)

**Deletion race gate.** Because deletion is now an *orthogonal column*, not a status, stamping `delete_after_ms`
alone does not serialize against `Resume`. The invariant that keeps the old strand bug closed is
**`delete_after_ms > 0 ⟹ terminal status`**, and the reaper reaps only terminal-rooted trees: DeleteOnCompletion
stamps a `completed` root (immutable); `Delete`/`Purge` of a terminal flow stamp an immutable row; `Delete`/`Purge`
of an `interrupted` flow flip it to `cancelled` in the same UPDATE, mutually exclusive with `Resume`'s
`WHERE status='interrupted'` (row lock; exactly one wins, loser 409s). So a live `delete_after_ms` never coexists
with a resumable flow, and no steps are ever deleted where a `Resume` could interleave.

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
sharing the replica. Each of the four `Host` calls is wrapped in `errors.CatchPanic` at its **call boundary** (not
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
- **`SignalPeers`** (`peers.go`) - fire-and-forget; the panic is
  caught and logged at error level only, so it can never derail the calling operation.

The worker-loop and refiller `CatchPanic` wrappers stay as **defense-in-depth** for panics in *engine* code
(transition evaluation, fan-in, etc.) outside any host call. `fixtures/panicflow_test.go` pins both task-panic
outcomes (no `onError` → flow `failed`; with `onError` → routed to the handler and `completed`), using a bounded
`Await` that would time out if the panic had wedged the step instead of failing it.

### Worker context (the engine lifetime)

Workers, the timer, and the refiller share the engine's lifetime context (`e.lifetimeCtx`), created at Startup and
cancelled only after `Shutdown` drains all three. So by the time the lifetime ctx is cancelled, every DB operation
has committed - in-flight writes are never interrupted by ctx cancellation. The only *cancellable*, time-bounded ctx
is the `ExecuteTask` call: `executeTask` derives it from the lifetime ctx with the step's `time_budget_ms`.

### Shutdown ordering: drain workers, then timer, then refiller

`nudgeTimer` (the sender behind `shortenNextPoll`) nudges the timer via
`select { case wakeTimer <- struct{}{}: default: }`. The `default` only guards a *full* channel - a send on a
*closed* channel still panics, even inside a `select`. The senders are worker goroutines (`processStep` and
its retry/sleep/recovery paths), so there is no drain point after which a `wakeTimer` send is
guaranteed impossible. `wakeTimer` is therefore **never closed** (the same rationale as `refillTrigger`); `timerLoop`
is terminated by a dedicated `timerStop` channel it selects on. So Shutdown drains the worker pool, then stops the
timer, then the refiller:

```
cache.close()        // unblocks blocked candidate pops independently of any channel
workers.Wait()       // no shortenNextPoll / requestRefill worker caller remains
close(timerStop)     // timerLoop's termination signal (wakeTimer is never closed)
timerWorker.Wait()   // timerLoop fully exited (last requestRefill caller gone)
close(refillStop)    // refiller's termination signal
refiller.Wait()      // refiller fully exited; its DB ops complete
```

The timer and refiller each have their own WaitGroup, separate from the worker pool, so the close-then-wait order can
be staged. `timerStop` is closed before `refillStop` because `timerLoop`'s final poll can still `requestRefill`;
stopping the refiller first would lose that work or race the trigger. `refillTrigger`, like `wakeTimer`, is never
closed and only sent to non-blockingly, so a late `requestRefill` from the timer's final poll is a harmless no-op
rather than a `send on closed channel` panic; the refiller is stopped via the separate `refillStop`. A `cache.refill`
into an already-closed cache is a no-op. Never-closed nudge channels plus dedicated `timerStop`/`refillStop` signals
remove the ordering hazard an earlier design carried (closing `wakeTimer` before draining the workers let a worker
mid-`processStep` race the close and panic).

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
  `failStep`, `deliverSubgraphError`, `handleInterrupt`, and `Delete`/`Purge` (the deletes run steps-before-flows,
  ascending id). `handleInterrupt` belongs here despite advancing the flow (running→interrupted): interrupt is
  **non-terminating** and marks no step `completed` in a prior standalone UPDATE, so it carries **no** orphan-strand
  obligation - its only write-first requirement is that the *first* statement be a write (the `UPDATE dwarf_steps`
  satisfies it, keeping the SQLite deadlock closed). It is deliberately steps-first to match `Resume`/`Cancel`,
  which walk the *same* surgraph chain, so the two never lock that chain's flow+step rows in opposite order (the
  former D2 cycle, now eliminated).

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
UPDATE - see "Time Budgets"). If the worker crashes, the lease expires and `pollPendingSteps` resets the step to
`pending` for re-execution.

### Background Recovery

1. **`pollPendingSteps`** - on a timer. Recovers `running` steps whose lease expired by resetting to `pending`; rings
   the doorbell for due pending steps.
2. **Terminal flow check** in `processStep` - after loading flow data, if the flow is `cancelled`/`failed`/
   `completed`, sets the step to that status and returns. Catches races where the flow went terminal before the step
   was updated.
3. **Orphan flow detection** (`detectOrphanedFlows`, `wedge.go`) - logs an error for any `running` flow with no
   non-terminal step and `updated_at` older than `orphanFlowThreshold` (5m). Such a flow (every step terminal, no
   successor) is stranded - the shape the post-completion transition wedge produces (see "processStep - Normal
   Completion" below). A bug signal; **auto-recovery is intentionally not attempted** - re-driving the flow would
   duplicate transition-evaluation logic and a false positive could double-advance it. The real recovery is the
   `processStep` recovery defer (which rolls the just-`completed` step back to `pending` to re-dispatch); this detector
   is the last-resort alarm for the residual case the defer cannot cover (its own reset UPDATE losing to a contention
   storm). It runs on the same **dedicated `recoveryLoop`** as the wedge sweep (#4) - off `pollPendingSteps` for the
   same heavy-scan reason (its `NOT EXISTS` over `dwarf_steps` is latency-tolerant, while the poll is nudged
   sub-second). Excludes a flow legitimately waiting (`running`+parked subgraph caller, a `pending` sleep/retry step,
   an `interrupted` step - all non-terminal), so steady-state never trips it. Logs at error level only (silent under
   the default discard logger); no metric.
4. **Parked-step wedge sweep** (`sweepWedgedParks`, `wedge.go`) - defense in depth for the `parkedSubgraph` park,
   whose releasing condition could in principle never fire (a parked step is invisible to selection, and
   `parkedSubgraph` is invisible to lease recovery too). Runs on a **dedicated recovery goroutine** (`recoveryLoop`)
   on a plain `wedgeSweepInterval` (5m) ticker - kept *off* `pollPendingSteps` because that poll is nudged sub-second
   under load while the sweep's `NOT EXISTS`/`GROUP BY` scans are heavy and the wedge it guards against is
   latency-tolerant; the recovery loop is drained before the refiller in `drainRuntime` since a recovered park can
   `requestRefill`. The detector carries a `parkWedgeThreshold` (5m) age guard so steady-state operation never trips a
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
     failed/cancelled/absent one. (A fan-out has several caller steps, each its own `surgraph_step_id`, checked
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
     The sweep cancels the orphan's whole subtree (`cancelOrphanedSubtree`: a subtree-scoped clone of `Cancel`'s
     transaction with no surgraph up-walk, since the ancestor chain is already terminal), sharing the parent's
     terminal fate. An **`interrupted`** parent is deliberately *excluded* (not terminal - a `Resume` of the root
     revives that branch and a sibling child under it is healthy); the `parkWedgeThreshold` age guard excludes the
     sub-second window where a just-terminalized parent's sibling child is still being cleaned up by the normal
     completion/error path. It counts under `park_type="orphaned_child"`.
   Each unwedge increments `dwarf_steps_unwedged{park_type}` (the always-on alarm; a nonzero value means a
   latent bug let a step wedge) and logs at error level (silent under the default discard logger, surfaced once a
   host injects one).

### Per-Function Crash Analysis

- **Create** - one transaction: insert flow (`running`) -> insert entry step (`pending`) -> set the flow's
  `thread_id`/`step_id`, then ring the doorbell. A pre-commit crash rolls back entirely (no partial flow); a
  post-commit crash before the doorbell is recovered by `pollPendingSteps` picking up the `pending` entry step.
  There is no separate `Start`, so no inert `created` window. Self-healing.
- **Resume** - one transaction (steps -> `pending`, flow -> `running`). A crash after commit but before the
  doorbell is recovered by `pollPendingSteps`. Self-healing.
- **Fork** - one transaction clones the whole tree (new flow + step rows, id remap, re-parked ancestor callers), with
  the leaf fork step held `created` until the mapping completes, then enqueue. A pre-commit crash rolls back entirely
  (no partial clone); a post-commit crash before the doorbell is recovered by `pollPendingSteps`. The original flow is
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
  leaves the step `running`; the lease expires, `pollPendingSteps` resets it, and the terminal-flow check marks it
  `completed`. Self-healing.

### Database Sharding

`SetNumShards` (default 1) distributes flows across databases to scale write throughput and reduce index contention.
The sharding *mechanics* — the 1-indexed `Shard(n)` translation, the always-parallel `OnEach` cross-shard fan-out,
the not-shard-fault-tolerant contract, and the DSN `%d` format / test-mode resolution — live in
`internal/database/CLAUDE.md`. This section is the engine-side *semantics*.

**Shard routing & encoding:** external flow IDs encode the shard (`{shard}-{flowID}-{token}`); every operation parses
it and routes via `e.db.Shard(n)`. Indices are 1-based (`1..NumShards`); `0` is the "no shard / all shards" sentinel
used by `Query.Shard`.

**Shard affinity:** subgraph flows are created on the parent's shard (avoids cross-shard references during
subgraph completion and history reconstruction). Only top-level flow creation picks a random shard.

**`List` uses per-shard pagination, not cross-shard global order.** Each shard returns up to `ceil(limit/numShards)`
rows by its own `flow_id DESC`; the aggregate is shard-grouped. Cross-shard ordering by `created_at` would compare
different servers' clocks, and by `flow_id` alone is broken (a shard with fewer flows has lower ids). Pagination uses
an opaque cursor encoding each shard's smallest-returned `flow_id`. `List` is strict by design: any shard error fails
the whole call (the per-shard debug path is `ShardInfo` + `List(Shard=N)`).

**Shard count is immutable at runtime.** `SetNumShards` is construction-time only: it records the target before
`Startup` (which opens+migrates exactly that many shards) and is **rejected** on a running engine, so `e.db.NumShards()`
never changes after `Startup`. Callers therefore size per-shard state from `e.db.NumShards()` and index it by shard
with no concurrency concern. Changing the count needs a **coordinated restart** (a maintenance window), because it is
not safe to grow live: a flow key encodes its shard (`{shard}-{id}-{token}`), so a flow created on a newly-added shard
is unroutable (404) on any replica still at the old count - and there is no cross-replica agreement on the count nor
any rebalancing of existing flows (they stay on their original shard). Doing dynamic growth *correctly* - lockstep
count across replicas plus rebalancing - is a larger problem left unsolved; until then the count is fixed per process.

