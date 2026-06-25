# Dwarf Engine Design Notes

## Overview

Dwarf is a standalone workflow-orchestration engine (`github.com/microbus-io/dwarf`). It executes workflow graphs by
dispatching tasks through an injected executor, managing state between steps, and handling fan-out/fan-in, interrupts,
retries, subgraphs, and failure recovery. It depends only on `sequel` (SQL), plus the pure-types sub-package
`dwarf/workflow`.

This document captures the engine's internal design rationale - the *why* behind the mechanics, which godoc does not
record. The engine is library code: it reaches tasks, fetches graphs, signals peers, and reports stops through a
single **injected `Host` interface** (plus separately injected observability providers) rather than a built-in
transport. A host application (for example a microservice) wires that interface to its own transport, identity, and
observability. Where this doc refers to "the host" or "the adapter," it means that wrapping layer.

> **Documentation convention.** Dwarf is a standalone package, published and consumed independently; in this repo's
> wider context it sits *downstream* of the Microbus Fabric framework, but it must never depend on or assume that
> particular host. Documentation and code comments in this module must therefore stay host-agnostic: do **not** name
> the Fabric, the Foreman, or any other specific host (or its types, like `sub.TimeBudget` or a "plane"). Use the
> generic term **"host"** for the upstream layer that embeds the dwarf engine, and describe what that host *does*
> (mints a token, enforces a per-call deadline, shares a per-test isolation key) rather than which product does it.

### The Host interface (how the engine reaches the outside world)

The graph/task/notify/peer seam is a single **`Host`** interface, registered once via `SetHost`; the
observability providers below are injected separately. A host must implement `LoadGraph` and `ExecuteTask`;
`FlowStopped` and `SignalPeers` may be no-ops. The interface methods:

- **`LoadGraph(ctx, workflowURL string) (*workflow.Graph, error)`** - fetches a workflow graph by name.
  Called at `Create` (and on subgraph spawn); the graph JSON is then frozen on the flow row. The flow's opaque
  baggage rides on ctx (`workflow.BaggageFrom(ctx)`) for identity-dependent loading (authz, per-actor graphs).
- **`ExecuteTask(ctx, taskName string, flow *workflow.Flow) error`** - executes one task. Receives the flow
  carrier with state pre-populated; writes its changes back onto the flow. The engine never knows *how* the task is
  reached (local call, RPC, message bus). The flow's baggage rides on ctx (`workflow.BaggageFrom(ctx)`). Any error the
  task returns is terminal for that attempt: the engine routes it via the graph's `onError` transition if one exists,
  else it fails the step. The engine never sniffs status codes or error text; a task that wants to back off on a
  transient failure detects that itself and arms `flow.Retry`.
- **`FlowStopped(ctx, flowKey string, outcome *workflow.FlowOutcome)`** - fired when a flow stops
  (completed/failed/cancelled/interrupted) for a flow created with `FlowOptions.NotifyOnStop=true`. `flowKey`
  identifies the stopped flow (it is *not* part of the outcome). The engine traffics in no delivery address:
  the flow's opaque baggage rides on `ctx` (`workflow.BaggageFrom(ctx)`) and the host resolves where/how to
  deliver from it. Optional: a host with no stop-notification need does nothing here.
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
`initialState`), stored on the flow row (the `baggage` column), and delivered to every `LoadGraph` and
`ExecuteTask` call for the flow's lifetime on the dispatch **context** - the host reads it with
`workflow.BaggageFrom(ctx)`. It is authored in one visible, typed place (`FlowOptions`) and observed ambiently
where used; most callbacks ignore it. The engine never interprets it; a host carries actor claims / tenant
identity there. `BaggageFrom` lives in the `workflow` package so task code can read baggage without importing
`engine`. The host always receives the JSON-decoded form (typically `map[string]any`), exactly like flow state.
(Unlike W3C/OTEL request baggage this is *flow*-scoped and frozen at `Create`, not per-request mutable - a host
adapter bridging to a bus maps between the two at the seam.)

### Configuration (`Set*` methods)

Configuration is applied through `Set*` methods, each returning an `error` rather than chaining a `*Engine` (so
every setter can surface an error). They split into two groups by whether the knob can change safely on a running
engine:

- **Live** (take effect immediately, callable any time): `SetNumShards`, `SetMaxOpenConns`, `SetTimeBudget`,
  `SetDefaultPriority`. `SetTimeBudget`/`SetDefaultPriority` are read fresh at each `Create` (an existing flow keeps
  the budget/priority frozen at its own `Create`); `SetMaxOpenConns`
  re-applies the pool size to every live shard (sequel's pool setters are hot/atomic); `SetNumShards`
  opens+migrates the added shards (see "Database Sharding" - grow-only at runtime).
- **Construction-time only** (return an error if called after `Startup`): `SetDSN`, `SetWorkers`, `SetHost`,
  `SetLogger`, `SetMeterProvider`, `SetTracerProvider` (plus the `SetDebugLogger` convenience). Applying these on a
  running engine would mean reopening live connections (`SetDSN`), resizing the worker pool + candidate cache
  (`SetWorkers`), or re-resolving a frozen provider - so the setter **rejects** it with an explicit error rather
  than silently no-op'ing. The error wording is `"<what> cannot be changed after Startup"`.

For the observability providers specifically (`SetLogger`/`SetMeterProvider`/`SetTracerProvider`): the engine
resolves the logger/tracer/meter once at startup (the logger feeds the worker hot path and is read lock-free; the
meter registers an async gauge callback) and wires all three into every shard's sequel DB in
`configureDBTelemetry`. Hot-swapping a provider on a live engine is deliberately unsupported: a half-hot version
that only re-pointed the DBs (sequel's setters are atomic/hot) but left the engine's own logger/tracer/metrics
frozen would be inconsistent, and a full-hot version (atomic logger + tracer re-resolve + meter rebuild/Unregister)
is real complexity for a need that does not arise in practice.

### Core Concepts

**Workflow graph** - A directed graph defining a workflow's structure: which tasks run, in what order, under what
conditions. Built in code with the `workflow.Graph` API via `NewGraph(name)`. Each graph has a human-friendly
display name (surfaced in rendering and denormalized onto the flow row as `workflow_name`), an entry point,
tasks, transitions, and optional reducers for fan-in state merging. The graph does **not** carry its own
resolve URL: the resolve key is a separate opaque `workflowURL` passed to `Create`/`Run`/`LoadGraph` and stored
on the flow (`workflow_url`); the engine never keeps it on the graph. Each node is bound to its dispatch
endpoint with `graph.SetEndpoint(nodeName, url)` (create-or-update); the same endpoint may be bound under
multiple node names.

**Naming convention.** Graph and task (node) names are PascalCase (`Reserve`, `Charge`) - graph-topology
identifiers, kept visually distinct from the lowercased dispatch URLs and the camelCase state fields. The engine
imposes no casing; this is a fixture/example convention only.

**Task** - A named unit of work within a workflow, identified by a task name/URL and executed via the injected
`ExecuteTask`. Tasks receive state via a `workflow.Flow` carrier, read input from state fields, perform work, and
write output back to state fields. Tasks are reusable across workflows.

**Flow** - A single execution of a workflow graph. Each flow has a unique ID, tracks its current position, and
maintains a state map that evolves as tasks execute. Statuses: `created` -> `running` -> `completed`/`failed`/
`cancelled`, with `interrupted` as a parked state for human-in-the-loop scenarios.

**Step** - A single task execution within a flow. Each step captures an immutable input snapshot (`state`), the output
delta (`changes`), and metadata (status, error, timing). Steps are numbered by `step_depth`; parallel fan-out
siblings share a `step_depth`. Once terminal (`completed`/`failed`/`cancelled`), a step is immutable.

**Reducer** - A merge strategy for state fields during fan-in. When parallel branches converge, each branch's changes
are merged using the reducer for that field: `replace` (last write wins, default), `append` (concatenate arrays),
`add` (sum numbers), `union` (deduplicate arrays), or `merge` (combine objects, new key wins). A field with no
registered reducer uses `replace`; every non-default fan-in field is wired explicitly with
`graph.SetReducer(name, reducer)` (the older `sum*`/`list*`/`set*` name-prefix inference was removed).

**Thread** - A chain of flows linked by `Continue`. Each flow has a `thread_id` grouping it with others in the same
multi-turn conversation; defaults to `flow_id` (each flow its own thread). `Continue` inherits the thread's
`thread_id`. Subgraph flows always start their own thread to avoid contaminating the parent's continuation chain. The
flowKey returned by the initial `Create` doubles as the threadKey.

### Flow Lifecycle

```
Create --> created --> Start --> running --> completed
                                  |  ^
                                  |  | Resume
                                  v  |
                              interrupted
                                  |
                                  v
                        failed <--+
                          |
                          | Restart / RestartFrom
                          v
                        running --> ...

                        cancelled (via Cancel)
```

1. **Create** (or `CreateTask`) inserts a flow and its first step in `created` status.
2. **Start** transitions the flow to `running` and its steps to `pending`, then rings the doorbell.
3. A worker picks up the step, executes the task, and evaluates transitions to create next steps.
4. Repeats until no transitions match (flow completes), a task errors (flow fails), or the flow is cancelled.
5. Tasks can call `flow.Interrupt()` to pause for external input; `Resume` continues.
6. Terminated flows can be re-run with `Restart` (from the entry step) or `RestartFrom` (from a chosen step),
   optionally with state overrides. A task can also re-run itself in place via `flow.Retry`.

### Engine Operations

These are methods on `*engine.Engine`.

**Create** - Creates a flow without starting it. Calls the `LoadGraph` to fetch the graph, creates the flow row, and
inserts the entry-point step - both in `created` status. The graph JSON is frozen at creation. `CreateTask(ctx,
name, taskURL, …)` wraps a single task in a trivial one-node graph (via `singleTaskGraph`) - `name` is the node's
display name (required, non-empty, placed before `taskURL` to match `NewGraph`/`SetEndpoint`/`flow.Subtask`),
`taskURL` the dispatch target.

**Start** - Transitions a `created` flow to `running`. Atomically updates all `created` steps to `pending` and the
flow to `running` in one transaction, then rings the doorbell for the current step. Whether the flow notifies the
host on stop is a Create-time property (`FlowOptions.NotifyOnStop`).

**Snapshot** - Returns a `*workflow.FlowOutcome` for a flow at the current moment. For terminal statuses
(`completed`/`failed`/`cancelled`) it returns the flow's `final_state` (plus `Error`/`CancelReason`); for
`interrupted` it returns the leaf interrupted step's merged `state+changes` and its `interrupt_payload`.
For a `running` or `created` flow it returns the status with an **empty `State`** - dwarf does not
currently reconstruct the live in-flight merged state (including the fan-out `step_id=0` case). Exposing a
live fan-out snapshot is a deliberately-deferred behavioral decision (it returns work-in-progress state
that no other path exposes); confirm against product intent before implementing.

**Resume** - Continues a flow paused by `flow.Interrupt`. Walks up the surgraph chain (`surgraphChain`) and down the
interrupted subgraph chain (`interruptedSubgraphChain`) to the leaf interrupted step. Records resume data on the
leaf's `resume_data` column (the leaf already has `interrupt_done=1`, set when the task armed `flow.Interrupt`); on
re-dispatch the task's `flow.Interrupt` call returns that data with `yield=false`. Resume data is **not** merged into
`state`/`changes` - the task receives it as the return value. Re-parks intermediate surgraph steps, resets the leaf
to `pending`, transitions all flows in the chain to `running`. Callable on any flow in the chain; propagation goes
both directions. If multiple fan-out siblings interrupt, each `Resume` handles one; the flow returns to `running`
only when no interrupted steps remain.

**ResumeBreak** - Continues a flow paused at a `BreakBefore` breakpoint. Shares `Resume`'s chain-walking and re-park
machinery (both wrap the private `resume`), but instead of recording resume data it merges the caller's
`stateOverrides` onto the leaf's `state` (replace semantics) so the about-to-run task observes the edits - the
breakpoint pauses *before* the task runs, so injecting state is the only way to influence it. `breakpoint_hit=1` is
left set so the re-dispatch skips the breakpoint.

**Resume and ResumeBreak are strictly separated, never auto-routed.** A breakpoint pause and a `flow.Interrupt` pause
both carry flow status `interrupted`; the discriminator is the *leaf step*, where exactly one of `interrupt_done=1`
(an interrupt park) or `breakpoint_hit=1` with `interrupt_done=0` (a breakpoint) holds. The private `resume` reads
those flags and fails with 409 when the leaf's kind does not match the entry point: `Resume` refuses a breakpoint,
`ResumeBreak` refuses an interrupt. This is deliberate - detection tells you *which leaf you are at*, not *what the
caller intended*, and the two have different observable effects (a resume payload is delivered to `flow.Interrupt`'s
return and is **not** merged into state; breakpoint overrides **are** merged). Auto-routing a mismatch would silently
merge an interrupt answer into state under field names the task may not read, let the task re-arm and re-pause, and
strand the caller. The kind stays a *step*-level distinction (not a flow status) because fan-out branches can pause
for different reasons at once.

**Cancel** - Aborts a created, running, or interrupted flow. Walks up (`surgraphChain`) and down (`allSubgraphFlows`)
the hierarchy, atomically cancels all steps across all flows, computes `final_state` per flow, and cancels all flows
with per-flow `final_state` via CASE - all in one transaction. Callable on any flow in the chain. Takes a reason
string surfaced as `FlowOutcome.CancelReason`.

**Restart / RestartFrom** - `Restart` re-runs a flow from its entry point as a fresh attempt (resets `created_at` and
`started_at`); `RestartFrom` surgically rewinds the subtree below a chosen step without resetting the flow's run
timestamps. Both re-zero `parked` on the target step (see "Step Parking").

**Recover** - the operator front door to "re-run the failures" without locating each failed step by hand. Requires
the flow in `failed` status (409 otherwise), and rewinds every `failed` step **in one transaction**. The failed set
is exactly the *unhandled* failures (an errored step routed by `onError` is
`completed`, not `failed`), so a completed sibling in a fan-out is excluded. A failed step has no DAG subtree
(transitions only run on success), so each rewind is in place: undo its cohort bump (`undoCohortBumps`), reap a
subgraph caller's terminal child (`deleteSubgraphFlowsRootedAt`, a no-op for a leaf), and reset the row to `pending`;
the flow flips to `running` and the steps enqueue after the single commit. A failure that originated in a subgraph
surfaces as a failed *caller* step in the parent flow, so `Recover` on the parent re-runs it (reaping the prior child
and spawning a fresh one) - no recursion into descendant flows. **One transaction, not a loop of `RestartFrom`**:
rewinding one failed step at a time would enqueue the first re-run while later siblings are still `failed`, and that
re-run completing into the fan-in `failures>0` branch (see "Fan-Out and Fan-In") re-fails the flow mid-recovery and
strands the rest. Zeroing every failed branch's cohort bump atomically before any re-dispatch closes that window - a
re-run then sees `failures==0` and cannot re-fail. Each rewind is a CAS on `status='failed'`, so two concurrent
`Recover` calls (or a `Recover` racing a `RestartFrom`) cannot double-undo a cohort - row locking lets exactly one
win each step. **Idempotent**: a step that fails
again is left `failed`, and a re-run of `Recover` picks it up. A flow with both a failed and an interrupted fan-out branch settles as `interrupted`
(a lone branch failure does not resolve the cohort), so `Recover` refuses it until the interrupt is cleared with
`Resume`.

**History / Step** - `History` returns the step-by-step execution as `[]workflow.FlowStep`; each includes key, depth,
task name, state, changes, status, error, timestamp. Subgraph-executing steps have `Subgraph=true` with nested
`SubHistory`. `Step` returns one step by key.

**List** - Queries flows by status, workflow URL, or thread key, with cursor pagination (newest first, default 100).
Returns `ThreadKey` in each `workflow.FlowSummary`.

**BreakBefore** - Sets/clears a breakpoint that pauses before the named task. Breakpoints live in a `breakpoints` JSON
column on the flow row (`map[taskName]string`). During `processStep`, if the current task matches a breakpoint and
`breakpoint_hit` is false, the engine interrupts the flow (same propagation as `flow.Interrupt`) and sets
`breakpoint_hit=1`. Continued with `ResumeBreak`, not `Resume` (which rejects a breakpoint with 409). Inherited by
subgraph flows.

**Continue** - Creates a new flow from the latest completed flow in a thread, merged with additional state using the
graph's reducers. The `threadKey` accepts any flowKey in the thread; `Continue` resolves the thread via `thread_id`,
finds the latest flow (`ORDER BY flow_id DESC`), validates it is completed, and creates the new flow in the same
thread with the same graph, returned `created`. The prior turn's `final_state` passes through unfiltered as the new
flow's initial state; a workflow author wanting narrower carryover scrubs with an entry adapter task using
`flow.Delete`/`Transform`. Scheduling (priority, fairness) comes from caller-supplied `FlowOptions`; nil opts uses
fresh defaults, not the prior flow's.

**Run** - Create + Start + Await in one call, returning `(flowKey string, *workflow.FlowOutcome, error)` -
the new flow's key alongside its outcome (the key is the flow's identity, not part of the outcome; callers
need it for later `History`/`Resume`/`Restart`). On error, `flowKey` is `""` and the outcome is nil.

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

### FlowOutcome and side-channel signals

`Snapshot`, `Await`, and `Run` return a `*workflow.FlowOutcome` (`Run` also returns the `flowKey` separately;
`Snapshot`/`Await` callers already hold it). The same struct is the `FlowStopped` payload (with `flowKey` as a
separate callback arg). The shape:

```go
type FlowOutcome struct {
    Status           string
    State            map[string]any
    Error            string         // populated when Status == "failed"
    InterruptPayload map[string]any // populated when Status == "interrupted"
    CancelReason     string         // populated when Status == "cancelled"
}
```

The flow key is delivered separately, not on the outcome: the caller passed it to `Snapshot`/`Await`, `Run`
returns it, and `FlowStopped` receives it as an argument.

Side-channel fields are populated only for the matching status. `Run`'s Go-level `error` return is reserved for
infrastructure failures (DB, timeout); a *workflow failure* surfaces as `Status == "failed"` with `Error` set, so
callers don't disambiguate "the workflow rejected my input" from "the engine is down."

The interrupt path is split from `State`: `Snapshot` of an `interrupted` flow returns `State` as the merged step
snapshot *at the time of the interrupt* and `InterruptPayload` as the raw `flow.Interrupt(payload)` argument. Folding
the payload into `State` was lossy (the caller could not tell workflow state from the resume request). Callers wanting
the merged view call `workflow.MergeState(out.State, out.InterruptPayload, graph.Reducers())` themselves.

### Flow Stop Notifications

When a flow is created with `FlowOptions.NotifyOnStop=true`, the engine invokes the `FlowStopped` callback with a
`*workflow.FlowOutcome` when the flow stops - terminal (`completed`/`failed`/`cancelled`) or `interrupted`. This
matches the statuses `Await` returns on. The outcome carries the same fields as a `Snapshot`/`Await` return at the
stop point. Delivery is fire-and-forget; flow execution never blocks on it.

The engine carries **no delivery address** - that is a transport concern owned by the host. Instead the flow's
opaque baggage rides on the `FlowStopped` ctx (`workflow.BaggageFrom(ctx)`), and the host resolves where to deliver
from it (e.g. a host adapter stuffs the caller's address into baggage at `Create` and reads it back here). The
persisted gate is a single `notify_on_stop` flag on the flow row; `notify_on_stop` is honored on the **root** flow
only (subgraph flows do not notify directly - interrupt/cancel notifications query the root's flag + baggage via the
surgraph chain). Keeping the delivery address out of the engine is deliberate: a hostname the engine merely stored
and handed back verbatim is exactly what baggage already carries, so the engine stays transport-agnostic.

### Execution Model

The engine uses a **queue-as-cache execution model** with a configurable worker pool (`SetWorkers`) and a single
refiller goroutine per replica. The in-memory `candidateCache` is bounded and holds *hints*, not ownership. Each
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
(`candidateCache.offer`). It resolves the announced step's priority *and* `not_before` in one PK lookup (off the
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

`pollPendingSteps` does not enumerate the backlog onto a queue. It recovers expired-lease steps, detects orphaned
flows, sizes the wake timer to the nearest future `not_before`, and rings the local doorbell each cycle. If a
due-pending backlog exists it caps `nextPoll` at `backlogPollInterval` (1 minute) so an idle replica that got no
doorbell still re-scans. This is a coarse safety net, not the primary wake path: due work is normally picked up
immediately by the completion doorbell, and `nextPoll` is shortened to anything sooner.

The timer waits on the `nextPoll` deadline, shortened to the nearest future `not_before` (`flow.Sleep` / retry
backoff) so a due step wakes the replica even when no doorbell arrives. The timer loop (`timerLoop`) runs
`pollPendingSteps` on the adaptive interval.

### Query Parallelism

`processStep` is the hot path. Independent queries within it run in parallel (errgroup-style) to minimize latency on a
remote database:

- **Claim UPDATE + step SELECT** - the lease-acquiring UPDATE and the step-data SELECT run concurrently where the
  driver lacks RETURNING (MySQL); the UPDATE only mutates `status`/`lease_expires`/`started_at`, the SELECT reads
  stable columns, so they race-read safely. The lease size comes from the in-memory `TimeBudget` config, not the step
  row, removing the dependency that forced a serial pre-SELECT. On pgx/sqlite/mssql the claim and read are one
  round-trip via `RETURNING`/`OUTPUT`.
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
evaluates, the flow is already being driven by another path: a sibling's `failStep` cascaded the flow to failed, an
external `Cancel` cancelled it, or an `OnError` sibling-cancel handed the depth to an error handler. The fan-in worker
returns `nil` instead of calling `failStep` on its own step. Calling `failStep` here races with the OnError handler
and would incorrectly fail an otherwise-recoverable flow (the handler's next step is in flight at depth N+1 while the
fan-in worker is still finishing depth N).

**Fan-in merge order and contribution (lineage `SetFanIn` path).** `insertFanInStep` reads cohort members
(`lineage_id = cohortSpawnID`) `ORDER BY fan_out_ordinal, step_id`. `fan_out_ordinal` is stamped at fan-out from the
branch's position in the spawn loop (the `forEach` array index or static declaration order), so `list`/`append`/
`sum`/`set` reducers fold in input-array order rather than completion order; `step_id` breaks ties. The firing gate is
`cohort_arrivals >= cohort_size`, a counter on the spawn step independent of the merge query, so the merge's status
filter cannot deadlock fan-in. Only `completed` members contribute `changes`; `failed`/`cancelled`/`pending`/
`running` contribute nothing.

**The fan-in does not escalate on failed/cancelled members.** It records a normal `pending` fan-in step regardless of
cohort composition - never marks itself terminal or cascades `failStep`. A `cancelled` member is the *expected*
OnError sibling-cancel case (one branch errored and routed to its handler, which cancelled the others; the flow must
recover via the handler -> fan-in path). Genuine terminal outcomes are handled elsewhere: an unhandled error cascades
via `failStep`, `Cancel` sets the flow terminal directly, and the terminal-flow check in `processStep` catches
siblings. An earlier revision had the fan-in *poison* itself when any member was failed/cancelled; that regressed the
OnError recovery invariant and made the fanouterrorflow fixture flaky, so it was removed.

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
  `successor_id=Z`. The exit set is `lineage_id == cohortSpawnID AND task_name IN` the graph-predecessor tasks of the
  fan-in - **not** the whole lineage, so `A`/`B` in `forEach->{A->B->C}->J` are excluded and only the `C`s point at
  `J`.
- **flow.Retry / RestartFrom**: rewind the step in place (same row), so `predecessor_id` is preserved.
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

`FlowOptions.TimeBudget` is **frozen at `Create`** (immutable for the flow's life, like `priority`) and **inherited by
subgraph children** (`createSubgraphFlow` reads the parent's `time_budget_ms` alongside priority/fairness). A
later `SetTimeBudget` change does not retro-edit existing flows; it only seeds flows created afterward. (`Continue`
resolves a fresh budget from its own options/the current default, matching how it treats priority/fairness today -
it does **not** inherit the prior turn's budget.) On a subgraph spawn the child's `LoadGraph` is bounded by the
caller flow's budget; the create-time `LoadGraph` runs on the caller's own request context instead.

**No engine-imposed ceiling.** `SetTimeBudget` is the default; the engine deliberately enforces **no** upper bound on
`FlowOptions.TimeBudget`, mirroring its refusal to own a flow-level deadline. Bounding the budget (e.g. an SLA
ceiling) is the responsibility of whoever creates flows: a host that wants to cap it validates `FlowOptions.TimeBudget`
against its own limit before calling `Create` and rejects an over-limit request there (a request to *exceed* the
ceiling, distinct from narrowing the deadline at dispatch, which the host may always do). A standalone caller that
sets a very large budget simply owns the consequences below.

The worker lease is sized from the **step's own `time_budget_ms`** + `leaseMargin` (30s), written self-referentially
in the claim CAS (`lease_expires = DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?)`, only the margin a bind param), so
the lease always outlasts the budget that bounds the `ExecuteTask` call - even for a flow that overrode its budget
above the engine default. Sizing from the row (not in-memory config) is what keeps lease and budget from diverging;
it needs no upfront SELECT because `time_budget_ms` is already on the step row at claim time, the same read-locality
reason `priority`/`fairness` are denormalized there. Consequence: a *crashed* worker is recovered no sooner than its
step's `budget + leaseMargin`, so a flow's budget directly bounds its worst-case crash-recovery latency - which is the
practical reason a host caps the budget. (The earlier config-sized lease and its "decrease `TimeBudget` mid-flight"
re-dispatch trade-off are retired: each step's lease now follows its own frozen budget.)

### Flow lifetime is the workflow author's responsibility

The engine imposes no flow-level deadline. Picking a max-lifetime that fits both a 1-hour batch and a 30-day approval
workflow is impossible, and a knob defaulting to "no deadline" is surface area without a customer. Workflows needing a
bound implement it in author space: a guard task reading `flow.CreatedAt()` that returns a 408 when too much time has
elapsed; a `flow.Retry` loop that exhausts after a chosen bound; an `OnError`/timeout transition; or an external
caller scheduling a `Cancel`. `Flow.CreatedAt()` and `Flow.UpdatedAt()` are populated on every dispatch, so the
elapsed-time guard is one call away inside any task.

### Task self-identity on the carrier

`Flow.FlowKey()` and `Flow.StepKey()` return the task's own flow/step keys (`{shard}-{id}-{token}`), populated by the
orchestrator on every dispatch alongside the timestamps. They let a task correlate logs/traces or call back into the
engine (`History`, `Step`, `Snapshot`) for its own flow without the host threading identity through baggage. `step_token`
is read alongside the claim/read in `processStep` (added to the RETURNING/OUTPUT/SELECT of all three driver branches);
`flow_token` already rides the flow-row load. Both keys also survive the `Flow` JSON round-trip (the `flowJSON` wire
format carries them), so a remote task reached over a transport sees the same identity as an in-process one. Empty when
read outside a dispatched task.

### State Model

Each step has three JSON columns: `state` (input snapshot), `changes` (output delta), `interrupt_payload` (from
`flow.Interrupt`). `state` is set at creation and normally immutable; `changes` is written after execution. The next
step's `state` is `merge(currentState, changes)`. This immutability enables checkpointing, restart, and recovery.

**State mutation on retry:** on `flow.Retry()`, the engine merges `state + changes` back into `state` so the task
sees its own prior output next attempt; `changes` is preserved. `Resume` does **not** mutate `state`: it writes the
caller's data to `resume_data`, which `flow.Interrupt` returns on re-dispatch.

**Reducer delta convention:** tasks writing to reducer-managed fields (append, add, union, merge) set only the
**delta**, not the accumulated value. E.g. for a field wired to the append reducer via
`graph.SetReducer("messages", workflow.ReducerAppend)`, set `flow.Set("messages", []string{newMessage})`, not the
whole history. Violating this duplicates during fan-in merge.

**forEach element injection:** the current element is injected into `state` only (under `as`), not `changes`, so it is
available to the task but does not participate in fan-in merge.

### Task-Initiated Control Signals

Tasks signal the engine via control methods on the `Flow` carrier (distinct from the operations above):

- **`flow.Retry(initialDelay, delayMultiplier, maxIntervalDelay, giveUpAfter) bool`** - re-execute this task with
  exponential backoff. The bound is wall-clock, not a count: returns `true` (task should return `nil`) while the next
  attempt would still land within `giveUpAfter` of the step's first creation, else `false` (task should return its
  error) - including when the next backoff delay alone would overshoot the horizon, so a doomed wait is never parked
  before failing. The give-up check is made client-side in `Retry` against `flow.StepCreatedAt()`; the engine only
  consumes the backoff shape.
  Pass `giveUpAfter <= 0` for unlimited; to bound by count instead, pass `0` and gate on `flow.Attempt()`. The step row
  is reused. The engine tracks `attempt` and computes the re-dispatch delay `min(initialDelay * delayMultiplier^attempt,
  maxIntervalDelay)`, merging `state + changes` back into `state` so the task sees its prior output. Both `flow.Retry`
  and the operator-facing `RestartFrom` rewind
  a step row in place; `RestartFrom` additionally sweeps the step's downstream subtree and merges optional state
  overrides, for operator-driven recovery after a flow has already terminated. **A retry clears the park
  slot** (`interrupt_done`/`subgraph_done` -> 0, `resume_data`/`subgraph_result` -> `'{}'`, `subgraph_error` -> `''`),
  so a retry after a resolved `flow.Subgraph` re-runs the child and after a resolved `flow.Interrupt` re-interrupts.
  **A retry of a step that launched a subgraph reaps the prior attempt's child flow, recursively, in the same
  transaction as the rewind** (`deleteSubgraphFlowsRootedAt(stepID)`). The child is always *terminal* at retry time
  (the park resolves only on a terminal child), so this is a delete of inert rows, not a cascade-cancel of live work.
  Leaving it would make the execution DAG claim two paths (`X -> iter1 -> iter2 -> Y`) when the model is single-path,
  and let `subgraphHistory` attach the discarded child's subtree to the caller. This mirrors `RestartFrom`, which
  likewise reaps the rewound step's own child (its `collectDAGSubtree` seeds `visited` with the target but never
  *collects* it, so the target's child needs an explicit reap alongside the subtree sweep) - the reap is **step-scoped**
  (only this caller's children) unlike `Restart`'s flow-scoped `allDescendantSubgraphFlows`. Defense in depth:
  `subgraphHistory` selects the latest child (`ORDER BY flow_id DESC`), matching `completeSurgraphFlow`/wedge/`Continue`,
  so even a stray dangling child never renders. `flow.Retry` carries no condition - the task writes the retryable condition explicitly in the surrounding `if`
  (retry-on-any-error is usually wrong). To retry only on a timeout, gate on
  `errors.StatusCode(err) == http.StatusRequestTimeout`.
- **No jitter on retry backoff:** the worker pool already throttles per-replica concurrency, so simultaneous retries
  queue in the pool rather than overwhelm downstream. Jitter would add latency for no throughput benefit.
- **`flow.Sleep(duration)`** - delay the *next* step's execution by setting its `not_before`. The timer adapts to
  wake when the sleep expires. In fan-out, only the last sibling's sleep affects the fan-in point.
- **`flow.Goto(target)`** - override transition routing: skip normal evaluation and follow the `withGoto` transition
  to `target`, if registered. Goto transitions are never taken during normal evaluation.
- **`flow.Interrupt(payload)`** - pause and park the flow. The payload is stored in `interrupt_payload` and propagated
  up the surgraph chain. The task should return normally after. The engine sets the flow `interrupted` and fires the
  `FlowStopped` callback when the root flow's `notify_on_stop` is set.

**Single-park guard.** A step parks at most once - interrupt XOR subgraph, never both and never the other kind on
re-entry. `processStep` enforces this after the task returns: a competing-signals check fails the step if more than
one control signal is set in one dispatch, and a second check fails the step when the returned flow arms a park while
the step row's materialized `interrupt_done`/`subgraph_done` shows the *other* kind already resolved. The `workflow`
package's parkers already reject a conflicting second park at the call site; this guard is the trust boundary for an
untrusted returned flow.

### Transition Evaluation

Transitions are evaluated after a task completes successfully:

1. If the task called `flow.Goto(target)`, only `withGoto` transitions matching that target are taken.
2. Otherwise, all non-goto, non-error transitions are evaluated: those without `when` always taken; those with `when`
   taken if the expression matches the merged state.
3. `forEach` transitions iterate a state array and spawn one task per element. `forEach` cannot combine with
   `withGoto`.
4. Multiple matches -> all taken in parallel (fan-out).
5. No matches -> the flow completes.

**Error transitions** are evaluated when a task returns an error. Only `onError` transitions from the failed task are
considered. If one matches, the error is serialized as a `TracedError` into state field `onErr` and the handler task
becomes the next step; the failed step is marked `completed` with its changes preserved. If the task was in a
fan-out, all siblings are cancelled. If no error transition matches, the flow fails via `failStep`. Error transitions
can have `when` but not `forEach` or `withGoto`.

**Fan-out sibling constraint:** `Graph.Validate()` enforces that fan-out siblings have the same set of non-goto,
non-error outgoing transition targets, because the engine evaluates outgoing transitions from only the last sibling
to complete - differing transitions would make the result depend on which finished last.

### State Across Subgraphs

**Subgraph is a function call.** The signature is `flow.Subgraph(url string, in any, out any) (yield bool, err
error)`. Only the explicit `in` passed in crosses into the child as its initial state; only the explicit `out`
target (the child's `final_state`) crosses back. The parent's state and accumulated changes do NOT auto-cross either
direction. `in` is any JSON-marshalable value (a struct or a `map[string]any`), normalized to a state map via
`toStateMap` (nil → "no arguments"); `out` is a pointer (a `*struct` or `*map[string]any`) the child's `final_state`
is unmarshaled into by JSON tag (`parseMapInto`), or nil to ignore the result. A typed struct on either side gives
field-level type safety without manual `map[string]any` casts.

**Subtask is the single-task front door.** `flow.Subtask(name, url string, in any, out any) (yield bool, err
error)` runs one task as an isolated child flow, the task-level sibling of `Subgraph`. The *only* difference is at
launch: instead of calling the host's `LoadGraph`, the engine synthesizes a trivial one-node graph around `url`
(`singleTaskGraph(name, url)` - the same wrap `CreateTask` uses), named `name`, dispatching to `url`. So any task
endpoint runs as a child flow with no graph definition. Everything after launch is **identical** to `Subgraph` - it
is *not* a new park kind: same `parked=parkedSubgraph`, same `subgraph_done`/`subgraph_result`/`subgraph_error`,
same `surgraph_*` chain, re-entry, history, cancel/interrupt propagation, and wedge recovery. Mechanically `Subtask`
is a thin wrapper over `Subgraph` that records the (required, non-empty) `name`; the carrier holds the request in
`subgraphURL` + `subgraphInput`, and `subgraphTaskName` is both the discriminator and the synthesized graph's name
(non-empty ⟹ subtask; empty ⟹ subgraph). `SubgraphRequested` returns that `taskName`, and `processStep` branches on
it: non-empty → `singleTaskGraph`, empty → `LoadGraph`. The launch disposition metric is `status="subtask"` vs
`"subgraph"`. (`Subtask`/`Subgraph` are the two mechanisms; "subflow" is the umbrella for any child flow and is the
name of the typed host client, not an engine primitive.)

**Into the child:** `SubgraphRequested` passes `subgraphInput` (the `toStateMap`-normalized `in`) directly to
`createSubgraphFlow` as the child's initial state (nil normalized to `{}`). No merge with parent state. A caller
wanting the parent's full state passes `flow.Snapshot()` as `in` - explicit opt-in.

**Back to the parent:** `completeSurgraphFlow` writes the child's `final_state` to the surgraph step's
`subgraph_result` column, sets `subgraph_done=1`, and re-dispatches the parent task. On re-entry `flow.Subgraph`
unmarshals that `final_state` into the caller's `out` (yield=false), and the task reads the fields it wants. The
child output is **not** merged into the parent's `changes`.

### Surgraph Step Identification

Each subgraph flow's row stores `surgraph_flow_id`, `surgraph_step_depth`, *and* `surgraph_step_id` - the PK of the
parked surgraph step it belongs to. `completeSurgraphFlow` looks the surgraph step up by primary key, so it can never
match a sibling at the same `(flow_id, step_depth)`. This matters for: (1) a fan-in race where a non-subgraph sibling
at the same depth is momentarily `running`; (2) parallel subgraphs at one depth, each parked at `parked=1`. The PK
lookup keeps each child flow bound to the step that launched it.

### Interrupt/Resume Propagation Across Subgraphs

**Interrupt propagation (up):** when a step inside a subgraph flow is interrupted, the engine uses `surgraphChain` to
walk to the root surgraph, collecting flow IDs and parked surgraph step IDs, then interrupts all flows and steps in
the chain with bulk `UPDATE ... WHERE flow_id IN (...)` / `WHERE step_id IN (...)`. This ensures the caller awaiting
the top-level flow sees `interrupted`.

**Resume propagation (both directions):** `Resume` walks up (`surgraphChain`) and down (`interruptedSubgraphChain`),
re-parks intermediate surgraph steps, records resume data on the leaf's `resume_data`, resets the leaf to `pending`,
transitions all flows to `running`, and rings the doorbell - all in one transaction.

**Fan-out interaction:** one sibling may interrupt while others continue. The flow is marked `interrupted` by the
first; others run to completion. `Resume` handles one interrupted sibling at a time; the flow returns to `running`
only when no interrupted steps remain at any level.

### Identity / baggage propagation

The opaque baggage set in `FlowOptions.Baggage` at `Create` (stored in the `baggage` column) is delivered on the
dispatch **context** to every `LoadGraph` and `ExecuteTask` call for the flow's lifetime, including dispatches
long after creation - the host reads it with `workflow.BaggageFrom(ctx)`. The engine never interprets it; a host
uses it to carry the original caller's identity (e.g. mint a fresh token inside its `ExecuteTask`). It is
**inherited** by subgraph flows (`createSubgraphFlow` copies the parent's `baggage`) and by `Continue` (the next
turn reads the prior flow's `baggage` column and carries it forward, unless the `Continue` call sets
`opts.Baggage` to override), so a multi-turn conversation keeps the caller's identity across turns. A turn wanting
narrower context scrubs it in an entry adapter task.

**Delivery is context, authoring is `FlowOptions`.** Baggage is *set* explicitly and visibly (a typed
`FlowOptions` field on `Create`/`CreateTask`/`Run`) but *read* ambiently (off ctx), so the value the engine never
interprets is not a parameter on every callback and task handler. The engine injects it into the ctx it hands the
callbacks (in `processStep` for the per-step executor, and at the create-time `LoadGraph` call); the
`ContextWithBaggage`/`BaggageFrom` helpers live in the `workflow` package so task-defining code reads baggage
without importing `engine`. The create-time injection round-trips the value through JSON (`baggageMap`) so the
loader sees the same decoded shape every dispatch will.

### Await

`Await` blocks until a flow stops (no longer `created`/`pending`/`running`); it returns on `completed`/`failed`/
`cancelled`/`interrupted`. It registers a buffered channel in the `waiters` map, then loops: check state, return if
stopped, otherwise `select` on the channel or context cancellation. There is **no periodic re-snapshot** - it wakes
only on a notification or ctx. Non-terminal notifications (e.g. `running` from `Start`) re-check state rather than
returning early.

**Cross-replica `Await`.** A flow created on one replica but completed on another wakes a local `Await` only via the
`SignalPeers` broadcast (op `statusChange`). Every flow-stop site calls an internal `signalStop` helper that does the
local waiter wake *and* the peer broadcast; the receiving replica's `DeliverSignal` routes it to `notifyStatusChange`, which wakes
its local waiters. Without this wiring, an `Await` on the replica that did not run the final step blocks until its
context deadline (there is no poll fallback). Non-terminal (`running`) transitions are notified locally only,
matching the broadcast-only-on-terminal-stops policy.

### SQLite Testing Support

`engine.RunInTest(t)` runs Startup with per-test SQLite databases and registers cleanup via `t.Cleanup`. An empty DSN
selects SQLite in-memory; each shard is routed through `sequel.CreateTestingDatabase` with a per-shard test ID so the
shards are isolated databases (folding the shard index into the test ID is what keeps a multi-shard `RunInTest` from
collapsing every shard onto one shared in-memory DB). Key SQLite differences from server databases:

`engine.StartupInTest(ctx, testID)` is the `*testing.T`-free sibling, for a **host that is itself under test** and so
has no `*testing.T` to hand `RunInTest` (e.g. a host whose lifecycle is driven by its own harness, not by `t`). Both
share the `openTestDatabaseWithID` core: the engine never learns the host's notion of "test mode" - it only receives a
concrete `testID` and opens isolated throwaway databases keyed by it. The id is hashed to a bounded 16 hex chars (SQL
identifier limits) and is **deterministic**, so peer replicas that pass the *same* id (e.g. a shared per-test isolation
key) resolve to the *same* isolated databases - exactly what shared-state multi-replica fixtures need - while a
different id is isolated. Unlike `RunInTest` it registers no cleanup; the host drives teardown by calling `Shutdown`.

- **Write-first transactions** - `advanceFlow` does an `UPDATE` as the first operation to immediately acquire a write
  lock. On MySQL/Postgres this serializes concurrent workers (like `SELECT ... FOR UPDATE`). On SQLite with
  `cache=shared`, starting with a write avoids the deadlock where two read-first deferred transactions both hold
  SHARED locks and neither can upgrade. **Every flow-terminating transaction must be write-first for the same
  reason**, and the failure mode is worse than a transient error: the terminal step is marked `completed` by
  `processStep` *before* the disposition runs, so once the disposition's `Transact` exhausts its lock-contention
  retries and errors, the lease recovery (which only resets `running` rows) can't re-dispatch the now-`completed`
  step, and the flow is stranded `running` with every step terminal — a permanent orphan flow. `failStep` and the
  fan-in transaction write first (the failed-step / `updated_at` UPDATE); `completeFlow` was the one read-first
  holdout (`computeFinalState`'s SELECTs before the status UPDATE) and now takes the flow row's write lock first.
  A high-volume soak (`fixtures/soakflow_test.go`) and `fixtures/completionraceflow_test.go` reproduce the wedge
  without the fix.
- **Busy timeout** - `sequel` applies `_pragma=busy_timeout(1000)` to SQLite DSNs without one, so concurrent workers
  hitting a write lock wait up to 1s instead of failing immediately with `SQLITE_BUSY`. Essential during fan-out.
- **Lock contention recovery** - `processStep` defers a check: on a lock-contention error
  (`sequel.IsLockContentionError`), it resets the step it had leased (`running` -> `pending`, `lease_expires=NOW`),
  then `shortenNextPoll(time.Now())` to re-poll immediately. Both halves are load-bearing: `pollPendingSteps` only
  recovers running steps whose lease has *already* expired, and a freshly leased step holds a minutes-long lease.
  Without the lease reset, the immediate poll finds nothing and the step (and its fan-in) stalls until the lease
  lapses. The reset is guarded by `WHERE status='running'`, so only the leased-and-uncommitted case is rewound.

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

### Database Choice and Configuration

The engine speaks four SQL dialects via `sequel`: SQLite, MySQL/MariaDB, PostgreSQL, SQL Server. They behave very
differently under the concurrent INSERT/UPDATE load. Pick by deployment shape.

**PostgreSQL - recommended for production.** MVCC means concurrent INSERTs do not lock each other on secondary
indexes; no gap locks at default `READ COMMITTED`; the fan-out/fan-in pattern runs deadlock-free at any worker
concurrency. Use Postgres 13+ for `JSONB` and partial indexes. For throughput, raise `max_connections` to at least
`(NumShards * MaxOpenConnsPerShard * replicas)` and `shared_buffers` to ~25% of host RAM.

**MySQL / MariaDB - supported, expect tuning.** InnoDB at default `REPEATABLE READ` takes next-key (row + gap) locks
on every secondary-index touch; two concurrent flow creations on a shard can deadlock on overlapping index ranges.
`createWithGraph` retries on `sequel.IsLockContentionError`, hiding most, but a sustained deadlock rate degrades
throughput. To minimize: `transaction-isolation = READ-COMMITTED` (drops gap locks; the largest single reduction);
`innodb_autoinc_lock_mode = 2` with `binlog_format = ROW`; `innodb_lock_wait_timeout` 5-10s; keep
`innodb_deadlock_detect = ON`. Per-shard databases: `SetDSN` must contain `%d` when `NumShards > 1` and every shard
DB must exist before startup (the engine migrates schema but does not `CREATE DATABASE`). MariaDB 10.5+ for `JSON`.

**SQL Server.** Enable `READ_COMMITTED_SNAPSHOT ON` per shard database for Postgres-like non-blocking reads and
near-zero deadlock risk. No other tuning mandatory.

**SQLite - testing and single-instance dev only.** Single-writer means deadlocks are structurally impossible (writes
serialize) but throughput tops out at one transaction at a time. Used automatically by `RunInTest` with an empty DSN.
The injected `busy_timeout` keeps workers from immediately failing on `SQLITE_BUSY` during fan-out; do not remove it.
Do not run SQLite in production.

**Sharding guidance.** `SetNumShards` partitions flows across databases (or schemas). Shard count should equal or
exceed steady-state concurrent flow-creating threads divided by the per-shard write contention the engine tolerates.
Rough sizing:

| Engine | Concurrent INSERT/sec per shard before contention | Suggested NumShards |
|---|---|---|
| PostgreSQL | 1000+ | 1-4 |
| SQL Server (RCSI) | 500-1000 | 2-4 |
| MariaDB/MySQL (RC) | 200-500 | 4-8 |
| MariaDB/MySQL (RR) | 50-200 | 8-16 |

`NumShards` can grow at runtime via `SetNumShards`; it cannot shrink (old shards drain naturally). New flows land on
new shards; existing flows stay on their original shard.

**Connection pool sizing (`SetMaxOpenConns`, default 8 per shard, MaxIdle == MaxOpen).** Workers spend most of their
time waiting on the `ExecuteTask` call, not holding a SQL connection - the DB-only segments of `processStep` are
short. So the per-shard pool needs only a small absolute number of connections. `MaxIdle == MaxOpen` matters more
than the absolute number: under bursty load the close/reopen churn (TCP+TLS+auth per cycle) dominates over query
time. Empirically pool=8 is a good default; pool=32 regresses (pool-mutex contention with no usable extra
concurrency). Operators with a different workload mix (longer DB-hold, larger shards) should tune explicitly.

### Flow Scheduling (priority / fairness)

The schema carries `priority`, `fairness_key`, `fairness_weight` on **both** `dwarf_flows` (authoritative) and
`dwarf_steps` (denormalized), so the two-level selection never joins `dwarf_flows` on the hot path - the same
split used for `time_budget_ms`/`baggage`.

`resolveFlowOptions` resolves a caller's `*workflow.FlowOptions` against the engine defaults: priority falls back to
`SetDefaultPriority`, the fairness key falls back to the host-supplied key (or `""`), the weight to `1`, and the time
budget to `SetTimeBudget` (the engine imposes no ceiling on it; see "Time Budgets"). These values are immutable for the
flow's life (switching a flow's fairness key mid-run would be a self-promotion abuse vector). `Create`/`CreateTask`
resolve from options. `createSubgraphFlow` **inherits** the parent flow's values (priority, fairness, *and*
time budget), so a high-priority parent never silently spawns default-priority descendants. `Continue`, by contrast,
**fresh-resolves** scheduling/budget from its own options (only `baggage` is inherited from the prior turn) - it
runs through `resolveFlowOptions` like a new `Create`, so a high-priority turn does not carry forward unless the
caller re-specifies it.

Propagation onto step rows: where the resolved values are in hand (the entry step), they are literal bind parameters
(`Restart`/`RestartFrom` rewind their target step in place via UPDATE, so the row's immutable priority/fairness/budget
values are already present and need no re-propagation); in the deep `processStep` paths (fan-out and the two fan-in
inserts), the values - including the flow's frozen `time_budget_ms` - are read once per step execution in the parallel
flow-row SELECT and threaded through the call chain into the INSERTs as bind parameters (vs. the previous scalar
subqueries, which meant 3N PK lookups per N-way fan-out).

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
re-leased, stranding any subsequent `Restart`/`RestartFrom` (which sets `status=pending` but the partitioned index
still excludes a `parked != 0` row). `Restart`/`RestartFrom` also re-zero `parked` as defense-in-depth.

## Metrics (`engine/metrics.go`)

The engine emits 10 `dwarf_*` instruments through the **OTEL metric API** (not the SDK). `SetMeterProvider`
injects the provider; it defaults to the global `otel.GetMeterProvider()` - the no-op provider unless the host
configures the SDK, so unconfigured/standalone/test use pays nothing. Instruments are built once in
`initMetrics` (called from `initRuntime`, so both `Startup` and `RunInTest` get them) from
`mp.Meter("github.com/microbus-io/dwarf")` - that scope distinguishes dwarf's metrics; **service identity lives
in the provider's Resource, not in per-metric attributes** (no `service.name` on data points - that would
explode cardinality and is off-spec). The only attributes the engine attaches are the metric-specific labels:
`workflow`, `status`, `task`, `priority`, `park_type`.

**5 counters, incremented inline** at their logical event sites: `dwarf_flows_started_total`
(start path), `dwarf_flows_terminated_total` (completeFlow), `dwarf_steps_executed_total` (every terminal step
disposition - completed/failed/interrupted/subgraph/retried/error_routed), `dwarf_steps_recovered_total`
(pollPendingSteps lease recovery), and `dwarf_steps_unwedged_total{park_type}` (the parked-step wedge sweep; a
nonzero value flags a latent bug). The inline helpers no-op when `e.metrics == nil` (before Startup).

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

The engine is OTEL-native for tracing, symmetric with metrics: `SetTracerProvider(tp)` overrides the
global `otel.GetTracerProvider()` (the no-op provider unless the host configures the SDK), and the engine
creates spans from `tp.Tracer("github.com/microbus-io/dwarf")` (same scope as the metrics; service
identity lives in the provider's Resource, not in span attributes). The host injects **only** the
provider - no span code, no `trace_parent` handling. Resolved once in `initRuntime` (`initTracer`); under
the no-op tracer every site below is free.

**Two span sites, persisted across replicas via the `trace_parent` column.** A flow's trace context is
minted once and reconstructed on every step dispatch (which may land on any replica), so it must survive
in the database - hence `trace_parent` is a **first-class dwarf-owned column** (the honest asymmetry vs.
metrics: spans need cross-replica continuity, metrics don't).

- **Root "workflow" span at `Create`** (`mintWorkflowSpan`, called from `createWithGraph`). The span is
  created, `End()`ed immediately, and its W3C context serialized into the flow's `trace_parent` column
  (`extractTraceParent`). Top-level `Create`/`CreateTask`/`Continue` mint it **detached**
  (`trace.ContextWithSpan(ctx, nil)` strips any ambient request span) so each flow - and each `Continue`
  turn - roots its own fresh trace rather than nesting under the request that created it.
- **Per-step span in `processStep`**, named by the task. The stored `trace_parent` is reconstructed
  (`injectTraceParent`) as the parent, the span is started with `workflow.id`=flowKey and `workflow.name`
  attributes, and the span's context is **placed on the `ExecuteTask`'s ctx** so the task's own
  downstream spans nest under it automatically. The span records the dispatch error
  (`recordSpanError` → `RecordError`+`SetStatus(codes.Error)`) when the executor returns one.

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
| `status` | Flow lifecycle: `created`/`running`/`interrupted`/`completed`/`failed`/`cancelled` |
| `step_id` | The flow's current step; `0` during fan-out (multiple steps active at one depth) |
| `surgraph_flow_id` | Parent (surgraph) flow id if this is a subgraph flow; `0` otherwise |
| `surgraph_step_depth` | The parent's `step_depth` that spawned this subgraph |
| `surgraph_step_id` | PK of the parent's parked surgraph step, so a subgraph flow identifies its surgraph step unambiguously when parallel parked surgraph steps coexist at one depth |
| `thread_id` | Groups flows in a `Continue` thread; defaults to `flow_id` (each flow its own thread) |
| `thread_token` | Token component of the thread's flowKey |
| `trace_parent` | W3C `traceparent` of the flow's root "workflow" span, minted at `Create` (or, for a subgraph, parented to the caller step's span). Reconstructed as the parent of every per-step span. Inherited by `Continue` only as a fresh trace (a new root span is minted per turn); a subgraph inherits the caller step's context, not this column. See "Tracing" |
| `notify_on_stop` | Set from `FlowOptions.NotifyOnStop` at `Create`; `1` fires the `FlowStopped` callback (with the flow's baggage on ctx) when the flow stops, `0` = no notification. The host resolves the delivery target from baggage - the engine stores no address |
| `delete_on_completion` | Set from `FlowOptions.DeleteOnCompletion` at `Create`; `1` makes the flow delete itself (and cascade to subgraph descendants) the instant it reaches `completed`. Root-only (not inherited by children); `failed`/`cancelled`/`interrupted` flows are never auto-deleted. See "Data Retention" |
| `final_state` | JSON state computed at termination - the full merged state of the terminal step(s), unfiltered. Narrowing happens in the workflow's terminal task via `flow.Delete`/`Transform` |
| `breakpoints` | JSON `map[taskName]string` of `BreakBefore` breakpoints |
| `created_at` | UTC creation time. Append-only and PK-correlated. Surfaced to tasks via `Flow.CreatedAt()`. Reset by `Restart` (a fresh attempt); NOT by `RestartFrom` (a surgical rewind) |
| `started_at` | UTC time this attempt began dispatching. Stamped by `Start` on `created` -> `running`. Reset by `Restart`, not `RestartFrom`. Distinct from `created_at` because a flow can sit `created` indefinitely before its entry dispatches. Drives `FlowSummary.Duration()` (`updated_at - started_at`) |
| `updated_at` | UTC time of the last status transition. Surfaced to tasks via `Flow.UpdatedAt()` |
| `priority` | Scheduling priority, integer >= 1, lower runs first. Resolved at `Create` from `FlowOptions` else `SetDefaultPriority`; inherited unchanged by `Continue`/subgraph. Immutable |
| `fairness_key` | Fairness bucket. From `FlowOptions`, else the host-supplied key, else `''`. Immutable |
| `fairness_weight` | Relative dispatch share of the `fairness_key`. From `FlowOptions`, else `1` |
| `error` | Task error string for `failed` flows. Written by `failStep` to every flow in the surgraph chain in the same UPDATE that sets `status='failed'`; the `WHERE status NOT IN (terminal)` clause makes the write first-failure-wins. Surfaced as `FlowOutcome.Error` |
| `cancel_reason` | Reason passed to `Cancel(flowKey, reason)`. Written to every flow in the cancellation chain in the same UPDATE that sets `status='cancelled'`, first-cancel-wins. Surfaced as `FlowOutcome.CancelReason` |
| `time_budget_ms` | Per-flow task time budget, resolved from `FlowOptions.TimeBudget` (else the `SetTimeBudget` default) and frozen at `Create`; the engine imposes no ceiling (a host bounds it before `Create`). Seeds every step's `time_budget_ms`. Inherited by subgraph children; **not** by `Continue` (fresh-resolved). Always stored concrete at `Create`; a `0` is unexpected and falls back to the live engine default at step insert (pure defense) |

#### `dwarf_steps`

| Column | Meaning |
|---|---|
| `step_id` | Per-shard auto-increment primary key. External stepKey is `{shard}-{step_id}-{step_token}` |
| `flow_id` | Owning flow |
| `step_depth` | Sequential transition depth; fan-out siblings share it. For history ordering, **not** the execution DAG |
| `step_token` | Random token component of the stepKey |
| `task_name` | Graph node name of the task this step executes |
| `state` | JSON input snapshot. Immutable except on retry/resume |
| `changes` | JSON output delta the task produced |
| `interrupt_payload` | JSON outbound payload from `flow.Interrupt()` - what the awaiting caller sees |
| `interrupt_done` | `1` once the interrupt park has been resumed; drives `flow.Interrupt`'s return-vs-arm decision. `0` for breakpoint pauses |
| `resume_data` | JSON inbound payload recorded by `Resume`; returned by `flow.Interrupt` on re-dispatch. `'{}'` until resumed |
| `subgraph_done` | `1` once a `flow.Subgraph` park resolved; drives `flow.Subgraph`'s return-vs-arm decision. A retry clears it to re-run the child |
| `subgraph_result` | JSON child `final_state` returned by `flow.Subgraph`. `'{}'` until resolved |
| `subgraph_error` | child error text for a failed `flow.Subgraph` park, returned as the `err`. `''` when none |
| `status` | Step lifecycle: `created`/`pending`/`running`/`interrupted`/`completed`/`failed`/`cancelled` |
| `goto_next` | Task-requested `flow.Goto` target; `''` = none |
| `error` | Error text when `failed`; `''` otherwise |
| `time_budget_ms` | Execution budget; the deadline on the `ExecuteTask` call context. Denormalized from the flow's `time_budget_ms` at step insert (frozen, not the live config), and also self-referenced in the claim CAS to size the crash-recovery lease (`time_budget_ms + leaseMargin`) |
| `breakpoint_hit` | `1` once a breakpoint on this step has fired, so it does not re-trigger on resume |
| `attempt` | `flow.Retry` attempt counter, drives the backoff |
| `not_before` | Earliest UTC time the step may execute (`flow.Sleep` / retry backoff) |
| `lease_expires` | Crash-recovery lease; `pollPendingSteps` reclaims `running` steps past this |
| `created_at` | UTC creation time |
| `started_at` | UTC time the *current attempt* first dispatched. The lease UPDATE stamps it via CASE only on a fresh attempt's first dispatch (`attempt=0 AND subgraph_done=0 AND interrupt_done=0`) and **preserves** it on a continuation (subgraph re-dispatch, interrupt/ResumeBreak re-dispatch, retry re-dispatch). A retried step's duration includes every attempt. Drives per-step body duration and inter-step wait labels in `FlowRenderer` |
| `updated_at` | UTC time of the last status transition |
| `lineage_id` | Cohort frame: the spawn step's `step_id` on a push, else inherited. Drives explicit `SetFanIn` arrival counting and merge. A cohort-counting device, **not** a DAG. `0` = no `SetFanIn` |
| `cohort_size` | On a fan-out spawn step: number of branches spawned |
| `cohort_arrivals` | On a fan-out spawn step: branches that reached the fan-in; fan-in fires when `arrivals >= size` |
| `fan_out_ordinal` | This branch's index in its fan-out; fan-in merges in this order so list/sum reducers are deterministic. Preserved across an in-place rewind (`flow.Retry`/`RestartFrom`). `0` = not part of a fan-out |
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
| `idx_dwarf_flows_status` | `(status, updated_at)` | `List` by status |
| `idx_dwarf_flows_workflow_url` | `(workflow_url)` | `List` by workflow URL |
| `idx_dwarf_flows_thread` | `(thread_id, flow_id)` | `Continue` (latest in thread) and `List` by thread |
| `idx_dwarf_flows_surgraph` | `(surgraph_flow_id)`, partial `WHERE surgraph_flow_id > 0` on pgx/sqlite | Walking the subgraph chain |
| `idx_dwarf_flows_created_at` | `(created_at)` | Time-window queries; append-only/monotonic |

#### `dwarf_steps`

| Index | Columns | Purpose |
|---|---|---|
| PK | `(step_id)` | Row lookups, lease acquisition in `processStep` |
| `idx_dwarf_steps_flow_id` | `(flow_id, step_id)` on MySQL; `(flow_id)` on pgx/mssql | Per-flow step queries |
| `idx_dwarf_steps_status` | `(status, updated_at)` - partial `WHERE status IN ('pending','running')` on pgx | `pollPendingSteps` recovery and pending discovery |
| `idx_dwarf_steps_created_at` | `(created_at)` | Time-window queries |
| `idx_dwarf_steps_selection` | `(status, parked, priority, fairness_key)` - partial on pgx/mssql/sqlite, full on mysql | Two-level priority+fairness candidate selection. The `parked` second column excludes parked rows without an in-memory filter |
| `idx_dwarf_steps_saturation` | `(status, parked, task_url)` - partial as above | Per-task in-flight count for the `dwarf_task_concurrency_running` gauge. Parked rows excluded so a surgraph parent doesn't inflate the executing-slot count |

### Data Retention

The engine does not auto-purge flows on a timer: every row remains potentially-resurrectable - an `interrupted` flow via
`Resume`, a `completed` flow via `Continue`, a `failed` flow via `Restart`/`RestartFrom`. A retention *duration* was
rejected for two reasons: a clock-triggered delete reaps rows out from under those resurrection paths (a flow `failed`
for 30 days may still be wanted for `Restart`; one `interrupted` for 30 days is awaiting a human), and no single
duration fits both a 1-hour batch and a 30-day approval. The author also cannot know at `Create` "how long will this
be relevant after it ends." So retention is either operator-driven or an explicit author opt-in:

- **`FlowOptions.DeleteOnCompletion`** - the author declares a flow fire-and-forget (durable-execution jobs that retry
  until success against a SaaS / under backpressure, whose output and history are not needed). The engine deletes the
  flow and its subgraph descendants the instant it reaches `completed` - an *event* trigger on success, not a clock,
  so there is no duration to pick and the resurrection paths are preserved: `failed`/`cancelled`/`interrupted` flows
  are **never** auto-deleted (a failed disposable job is exactly the one to keep for `Restart`/`Recover`). Honored on
  the **root** flow only (`surgraph_flow_id=0`); the delete reuses `Delete`'s cascade to sweep descendants, and the
  flag is not inherited by children. The delete is **inline** in `completeFlow`, after the `dwarf_flows_terminated_total`
  metric and any `FlowStopped` notification fire (so observability and the full outcome survive the row's deletion) and
  **before `signalStop`** (so a blocking `Await` woken by the stop signal observes a gone row uniformly, never a
  transient completed state). Best-effort - a delete failure only logs and leaves a stray row; no sweeper backstops it.
  **Await contract: once it completes, the flow is gone everywhere - uniformly.** A completed disposable flow has no
  row, so `Snapshot`/`History` and a blocking `Await`/`Run` return "flow not found" (404), the same regardless of
  timing (the delete-before-signal reorder removes the completed-then-deleted race). For an `Await` that 404 *is* the
  completion signal - and waiting still works: an `Await` started while the flow runs blocks until it finishes, then
  404s. A `failed`/`cancelled` disposable flow is *not* deleted, so `Await` returns its real terminal outcome (so a 404
  specifically means "completed"). Uniform 404 was chosen over a translated `completed` outcome (which was timing-
  dependent: in-time vs late await) and over a step-pruned tombstone (which only moves the inconsistency to `History`
  and, by keeping `final_state`, leaks the private data the deletion exists to remove). Callers wanting the outcome use
  `NotifyOnStop` (whose `FlowOutcome` is computed before the delete).

For operator-driven retention:

- **`Delete(flowKey)`** removes one flow and its steps in a transaction, **cascading into the flow's subgraph
  descendants recursively** (`allDescendantSubgraphFlows`, same-shard via parent-shard affinity) - a subgraph child's
  only inbound reference is its now-deleted surgraph step, so leaving it would strand it. Refuses a running flow (409),
  and likewise refuses the whole cascade if any descendant is still running (no partial delete that orphans a live
  child's parent). Thread descendants (separate `Continue` flows) are *not* swept - they are independent resurrectable
  flows, not children. (In practice a non-running root has no running descendant - subgraph children are terminal when
  the parent is - so the descendant guard is defense against a race.)
- **`Purge(Query)`** bulk-deletes flows matching the query, except running. Same `Query` shape as `List` (Status,
  WorkflowURL, ThreadKey, TaskName, FairnessKey, Priority, OlderThan, Shard, Limit). Capped at 10000 per call;
  returns the count deleted. The non-running guard is enforced inside the DELETE. **Purge deliberately does *not*
  cascade into subgraph descendants** - that would require a per-row recursive SELECT-then-DELETE descent, defeating
  the single bulk-DELETE that makes Purge a mass trim. A subgraph child orphaned by a purged parent is itself a
  terminal flow matching the same age/status filters, so the next Purge sweeps it; the dangling window is bounded and
  self-healing, acceptable for a retention sweep in a way it is not for a targeted `Delete`.

Both share filter clauses with `List`. The `Query.TaskName` filter joins `dwarf_steps` and matches the current
step's `task_name` (excludes fan-out flows, `step_id=0`). `Query.OlderThan`/`NewerThan` are database-anchored
(`f.updated_at < DATE_ADD_MILLIS(NOW_UTC(), -ms)` etc.) and compose. `Query.FairnessKey` filters on the
engine-native `f.fairness_key` (the host typically sets it to the tenant, so "list tenant X" is "list
`fairness_key = X`"); `Query.Priority` narrows to one scheduling band. Empty key / zero priority disable their
filters. The engine models no tenant concept of its own - the `tenant_id` column was dropped.

## Concurrency and Crash Recovery

The engine uses SQL transactions for multi-statement operations and `lease_expires` for crash recovery.

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
be staged. `timerStop` is stopped before `refillStop` because `timerLoop`'s final poll can still `requestRefill`;
stopping the refiller first would lose that work or race the trigger. `refillTrigger`, like `wakeTimer`, is **never
closed** and only sent to non-blockingly, so a late coalesced `requestRefill` from the timer's final poll is a
harmless no-op rather than a `send on closed channel` panic; the refiller is stopped by closing the *separate*
`refillStop`. A `cache.refill` into an already-closed cache is a no-op. Using never-closed nudge channels plus
dedicated `timerStop`/`refillStop` termination signals removes the ordering hazard an earlier design carried, where
closing `wakeTimer` before draining the workers let a worker mid-`processStep` race the close and panic.

### Transactions

`Start`, `Resume`, `Restart`, `RestartFrom`, and `Cancel` wrap their step and flow mutations in a transaction with
**steps-first-then-flow lock ordering** to prevent deadlocks. `processStep`'s transition evaluation (insert next steps
+ update flow's `step_id`) also runs in a transaction.

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
3. **Orphan flow detection** in `pollPendingSteps` - logs an error for any `running` flow with no non-terminal steps
   whose `updated_at` is older than 5 minutes. A bug signal; auto-recovery is intentionally not implemented (it would
   duplicate the transition logic and could double-advance on a false positive).
4. **Parked-step wedge sweep** (`sweepWedgedParks`, `wedge.go`) - defense in depth for the `parkedSubgraph` park,
   whose releasing condition could in principle never fire (a parked step is invisible to selection, and
   `parkedSubgraph` is invisible to lease recovery too). Runs on a **dedicated recovery goroutine** (`recoveryLoop`)
   on a plain `wedgeSweepInterval` (5m) ticker - kept *off* `pollPendingSteps` because that poll is nudged sub-second
   under load while the sweep's `NOT EXISTS`/`GROUP BY` scans are heavy and the wedge it guards against is
   latency-tolerant; the recovery loop is drained before the refiller in `drainRuntime` since a recovered park can
   `requestRefill`. The detector carries a `parkWedgeThreshold` (5m) age guard so steady-state operation never trips a
   false positive (the guard sits comfortably beyond normal subgraph-completion latency). Unlike orphan-flow detection
   this **does** auto-recover, because each recovery re-invokes the *normal* release mechanism - which is guarded by a
   CAS on the park state - rather than duplicating transition logic, so it is idempotent and harmless under a
   concurrent resolution, a false positive, or a peer replica sweeping the same shard:
   - **`parkedSubgraph`** (`recoverWedgedSubgraphParks`): a caller step `running`+`parkedSubgraph` with **no
     non-terminal child** (`surgraph_step_id = step_id`, status created/running/interrupted) is wedged - the child
     reached terminal but the revive was lost, or the child was deleted. The sweep re-drives the release on the
     latest child (`flow_id DESC`): `completeSurgraphFlow` for a completed child, `deliverSubgraphError` for a
     failed/cancelled/absent one. (A fan-out has several caller steps, each its own `surgraph_step_id`, checked
     independently; `flow.Retry` leaves older terminal children whose latest sibling is still active - handled by
     the `NOT EXISTS` + latest-child logic.)
   Each unwedge increments `dwarf_steps_unwedged_total{park_type}` (the always-on alarm; a nonzero value means a
   latent bug let a step wedge) and logs at error level (silent under the default discard logger, surfaced once a
   host injects one).

### Per-Function Crash Analysis

- **Create / CreateTask** - insert flow (`created`) -> insert step (`created`) -> update flow's `step_id`. A crash
  after the step insert leaves `step_id=0` with a `created` step; the flow is inert until `Start`, and
  `pollPendingSteps` picks up the orphaned `pending` step. Self-healing.
- **Start / Resume** - one transaction (steps -> `pending`, flow -> `running`). A crash after commit but before the
  doorbell is recovered by `pollPendingSteps`. Self-healing.
- **Restart / RestartFrom** - one transaction (rewind the target step in place, delete the rewound subtree, flow ->
  `running`). Self-healing.
- **Recover** - one transaction (rewind every failed step in place, undo their cohort bumps, flow -> `running`), then
  enqueue. A pre-commit crash rolls back; a post-commit crash before the doorbell is recovered by `pollPendingSteps`.
  Self-healing, and idempotent under a re-run.
- **Cancel / failStep** - one transaction over the whole surgraph chain. A pre-commit crash rolls back; a post-commit
  crash leaves correct terminal state, `Await` callers discover it on the next poll. Self-healing.
- **processStep - Interrupt** - one transaction. A pre-commit crash rolls back and re-execution produces the interrupt
  again (interrupt-producing tasks should be idempotent). Self-healing.
- **processStep - Normal Completion (with next steps)** - step -> `completed`, fan-in check, transaction (insert next
  + update `step_id`), doorbell. A crash in the narrow (~microsecond) window after step completion but before the
  transaction leaves the flow stuck - an accepted edge case for removing the `completing` intermediate status.
- **processStep - Flow Completion (no next steps)** - flow -> `completed` then step -> `completed`. A crash between
  leaves the step `running`; the lease expires, `pollPendingSteps` resets it, and the terminal-flow check marks it
  `completed`. Self-healing.

### Database Sharding

`SetNumShards` (default 1) distributes flows across databases to scale write throughput and reduce index contention.

**Shards are 1-indexed.** Valid indices are `1..NumShards`; `0` is a sentinel meaning "no shard / all shards" (used by
`Query.Shard`). The DSN's `%d`, the leading number in flow keys (`{shard}-{flowID}-{token}`), the `Query.Shard`
filter, and `ShardInfo` all use 1-based indexing. Internally `e.dbs` is a 0-based slice and `e.shard(n)` translates
with a bounds check.

**Shard routing:** external flow IDs encode the shard; every operation parses it and routes via `e.shard(n)`.

**Shard affinity:** subgraph flows are created on the parent's shard (avoids cross-shard references during
subgraph completion and history reconstruction). Only top-level flow creation picks a random shard.

**Cross-shard fan-out is always parallel, never sequential.** Any operation touching every shard builds a per-shard
job slice and runs them in parallel. A sequential per-shard loop would grow total latency linearly with `NumShards`
(at 8 shards a 10ms-per-shard query becomes 80ms wall-clock); the parallel shape stays at single-shard latency
regardless of shard count.

**Not shard-fault-tolerant by design.** Every cross-shard fan-out site fails the whole call on any shard's error. A
partial-tolerance attempt was rejected: real outages mostly manifest as hangs, not errors; classifying "shard down"
vs transient/data errors is driver-specific and brittle; and a helper that *claims* partial tolerance only in a
narrow subset of failure modes lies to operators about resilience. The cross-shard fan-out sites share one helper
(`eachShard`), invoked once per shard with the resolved DB and the 1-based index; any non-nil return fails the whole
call. Each caller retries on its next natural cycle (`pollPendingSteps` next tick, `scanPriorityBand` next refill), so
a transient hiccup heals within one cycle and a persistent outage degrades loudly.

**`List` uses per-shard pagination, not cross-shard global order.** Each shard returns up to `ceil(limit/numShards)`
rows by its own `flow_id DESC`; the aggregate is shard-grouped. Cross-shard ordering by `created_at` would compare
different servers' clocks, and by `flow_id` alone is broken (a shard with fewer flows has lower ids). Pagination uses
an opaque cursor encoding each shard's smallest-returned `flow_id`. `List` is strict by design: any shard error fails
the whole call (the per-shard debug path is `ShardInfo` + `List(Shard=N)`).

**Dynamic expansion:** `SetNumShards` can increase at runtime - new shards are opened, migrated, and immediately
available. Shrinking is rejected (old shards drain naturally).

**DSN format:** when `NumShards > 1`, the DSN must contain `%d` (replaced with the shard index). In test mode each
shard gets a separate in-memory SQLite database via a unique per-shard test ID.

## Flow Rendering (`workflow.FlowRenderer`)

`FlowRenderer` produces a Mermaid flowchart from a `History` result. Diagnostic intent: answer "where did the time go
in this flow?" at a glance. Defaults render top-down; `With*Colors` swap palettes; `WithLinks` enables per-node click
directives. `HistoryMermaid` wraps it as an engine method writing to an `io.StringWriter`.

### CSS-variable theming model

Color values flow through `classDef`/`style` directives; the renderer emits no themeVariables init block. Callers pass
either hex literals (static rendering) or CSS custom-property references like `var(--primary-container)` (host pages
that track a page-level theme). With `var()` values, browsers resolve through the SVG's CSS cascade and the diagram
re-colors on light/dark toggle without reinvocation. The CSS-mode pattern: pass `"currentColor"` for fill and `""`
for text - the generated classDef omits `color:`, host CSS sets it, and Mermaid inherits via `currentColor`.

### Color knobs and status groups

| Pair | Statuses |
|---|---|
| primary | `completed`, `running` (running gets a dashed border) |
| secondary | `pending` + chrome (`_start`, `_end`, fan-out cohort wrappers, subgraph block fills) |
| error | `failed`, `cancelled` |
| attention | `interrupted` (distinct from error - "needs human eyes," not hard failure) |

### DAG-edge model

The execution DAG is reconstructed from `PredecessorID`/`SuccessorID`, NOT `step_depth`. Every edge is recorded on at
least one endpoint - fan-out via each child's `PredecessorID`, fan-in via each cohort exit's `SuccessorID`, linear on
both - and the rendered edge set is their deduped union, exact for arbitrary nesting.

### Subgraph caller decomposition

A subgraph caller step renders as **two** visual elements: the caller's task node and a visible Mermaid subgraph
wrapper block containing the recursively-rendered SubHistory. The caller node's duration label is the **net** caller
cost: `net = (caller.UpdatedAt - caller.StartedAt) - subgraph_wall_time`, where `subgraph_wall_time = max(SubHistory*
.UpdatedAt) - min(SubHistory*.CreatedAt)` walked recursively. The net is the caller's own pre-call + post-return body
time; total call cost reconstructs as `net + subgraph_wall_time` without double-counting. Edges thread:

```
predecessor --> caller         (parent DAG, transition gap label)
caller --> innerHead           (the call, queue-wait label)
innerTail --> Y.entries        (the return, transition gap label)
```

`byID[caller].exits` is set to the subgraph's inner tails, so the existing `addEdge(caller, Y)` machinery emits one
edge per inner-tail x parent-DAG-successor combination. A terminal subgraph caller surfaces its inner tails as outer
tails, connected to `_end`.

### Node and edge label semantics

**Node label** = `UpdatedAt - StartedAt` (task body time) for any non-caller step that ran and reached a terminal
status. Pending/created/in-flight steps render with just the task name. Subgraph callers render the *net* cost.

**Edge label** = `to.StartedAt - from.UpdatedAt` (transition gap: DB commit + queue + dispatch). Computed from the
step records.

**Call edge label** = `entry.StartedAt - entry.CreatedAt` (queue wait on the subgraph's entry step). Without it, that
time would be inside `subgraph_wall_time` but invisible on any rendered edge.

### Fan-out cohort wrappers

Two-or-more steps sharing one `PredecessorID` get wrapped in an invisible Mermaid `subgraph` block (empty label, no
fill, no stroke) - purely a layout container so siblings cluster near their parent. Edges always go between actual
task nodes; nothing terminates at the cohort wrapper.

### Truncation in label formatting

`formatDuration` uses integer-millisecond truncation for sub-second values (`%dms`). A diagram with N labeled edges
accumulates up to ~N/2 ms of systematic underestimation in any path sum - diagnostically irrelevant (the goal is to
spot where time went, not reconcile to the millisecond).
