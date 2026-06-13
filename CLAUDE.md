# Dwarf Engine Design Notes

## Overview

Dwarf is a standalone workflow-orchestration engine (`github.com/microbus-io/dwarf`). It executes workflow graphs by
dispatching tasks through an injected executor, managing state between steps, and handling fan-out/fan-in, interrupts,
retries, subgraphs, backpressure, breakers, and failure recovery. It depends only on `sequel` (SQL) and `throttle`
(rate limiting), plus the pure-types sub-package `dwarf/workflow`.

This document captures the engine's internal design rationale - the *why* behind the mechanics, which godoc does not
record. The engine is library code: it reaches tasks, fetches graphs, signals peers, and reports stops through
**injected dependency interfaces** rather than a built-in transport. A host application (for example a microservice)
wires those interfaces to its own transport, identity, and observability. Where this doc refers to "the host" or "the
adapter," it means that wrapping layer.

### Dependency interfaces (how the engine reaches the outside world)

- **`GraphLoader`** `func(ctx, workflowName string, baggage map[string]any) (*workflow.Graph, error)` - fetches a
  workflow graph by name. Called once at `Create`; the graph JSON is then frozen on the flow row.
- **`TaskExecutor`** `func(ctx, taskName string, flow *workflow.Flow, baggage map[string]any) error` - executes one
  task. Receives the flow carrier with state pre-populated; writes its changes back onto the flow. The engine never
  knows *how* the task is reached (local call, RPC, message bus).
- **`FlowStoppedCallback`** `func(ctx, hostname string, outcome *workflow.FlowOutcome)` - fired when a flow stops
  (completed/failed/cancelled/interrupted) for a flow started via `StartNotify`. The hostname is whatever was passed
  to `StartNotify`.
- **`PeerNotifier`** - cross-replica coordination, all fire-and-forget: `Enqueue` (work doorbell), `SyncValve` (valve
  gossip), `TripBreaker` (breaker gossip), `NotifyStatusChange` (wake cross-replica `Await` waiters). The host
  publishes these to peers; inbound signals from peers are delivered back into the engine via `HandleEnqueue`,
  `HandleSyncValve`, `HandleTripBreaker`, `HandleNotifyStatusChange`. In a single-replica deployment the notifier is
  nil and none of this runs.
- **`*slog.Logger`** - structured logging sink (`WithLogger`); defaults to `slog.Default()`. The engine logs
  through the `…Context` variants so a context-aware handler (e.g. the `otelslog` bridge) can correlate records
  with the active step span.
- **`metric.MeterProvider`** - OTEL meter provider (`WithMeterProvider`); defaults to the global
  `otel.GetMeterProvider()` (no-op unless the host configures the SDK). The engine builds its `dwarf_*`
  instruments under the `github.com/microbus-io/dwarf` scope (see "Metrics" below). Tracing is deferred.

The **`baggage map[string]any`** is opaque to the engine: set once at `Create`, stored on the flow row (in the
`baggage` column), and passed through to every `GraphLoader` and `TaskExecutor` call for the flow's lifetime. A
host carries actor claims / tenant identity there; the engine never interprets it. (Unlike W3C/OTEL request
baggage this is *flow*-scoped and frozen at `Create`, not per-request mutable - a host adapter bridging to a bus
maps between the two at the seam.)

### Configuration (builder methods, callable before and after Startup)

`WithDSN`, `WithNumShards`, `WithWorkers`, `WithTimeBudget`, `WithDefaultPriority`, `WithMaxOpenConns`. They are
atomically applied, so they double as hot-reconfiguration knobs after `Startup`.

### Core Concepts

**Workflow graph** - A directed graph defining a workflow's structure: which tasks run, in what order, under what
conditions. Built in code with the `workflow.Graph` API. Each graph has a name, an entry point, tasks, transitions,
and optional reducers for fan-in state merging.

**Task** - A named unit of work within a workflow, identified by a task name/URL and executed via the injected
`TaskExecutor`. Tasks receive state via a `workflow.Flow` carrier, read input from state fields, perform work, and
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

**Create** - Creates a flow without starting it. Calls the `GraphLoader` to fetch the graph, creates the flow row, and
inserts the entry-point step - both in `created` status. The graph JSON is frozen at creation. `CreateTask` wraps a
single task name in a trivial graph.

**Start** - Transitions a `created` flow to `running`. Atomically updates all `created` steps to `pending` and the
flow to `running` in one transaction, then rings the doorbell for the current step. Sets up no notifications.

**StartNotify** - Like `Start`, but stores a `notify_hostname`. When the flow stops, the engine invokes the
`FlowStoppedCallback` with that hostname and a `*workflow.FlowOutcome`.

**Snapshot** - Returns the current state, status, task name, and step number. During fan-out (`step_id=0`), it queries
`dwarf_steps` directly for the active steps.

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

**History / Step** - `History` returns the step-by-step execution as `[]workflow.FlowStep`; each includes key, depth,
task name, state, changes, status, error, timestamp. Subgraph-executing steps have `Subgraph=true` with nested
`SubHistory`. `Step` returns one step by key.

**List** - Queries flows by status, workflow name, or thread key, with cursor pagination (newest first, default 100).
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

**Run** - Create + Start + Await in one call, returning the `*workflow.FlowOutcome`.

**Await** - Blocks until the flow stops (see "Await" below).

**Delete / Purge** - Operator-driven retention (see "Data Retention").

**ShardInfo** - Per-shard health/size summary.

**HistoryMermaid** - Writes the execution DAG as a Mermaid diagram to an `io.StringWriter`.

**The peer signals (`HandleEnqueue`, `HandleSyncValve`, `HandleTripBreaker`, `HandleNotifyStatusChange`)** are the
inbound side of cross-replica coordination: the host adapter calls them when it receives the corresponding signal from
a peer. The outbound side is the injected `PeerNotifier`. The **Enqueue doorbell** is the most frequent: it signals
that a step is pending. The receiving replica does one PK lookup for the announced step's `priority` and `not_before`.
If `not_before` is in the future the doorbell defers to the poll timer (`shortenNextPoll(not_before)`); if due, the
priority drives the cache offer (refill or head-insert; see "Execution Model"). It does not enqueue a specific step
into a queue. Fire-and-forget - a missed doorbell is recovered by `pollPendingSteps`.

### FlowOutcome and side-channel signals

`Snapshot`, `Await`, and `Run` return a single `*workflow.FlowOutcome`. The same struct is the `FlowStoppedCallback`
payload. The shape:

```go
type FlowOutcome struct {
    FlowKey          string
    Status           string
    State            map[string]any
    Error            string         // populated when Status == "failed"
    InterruptPayload map[string]any // populated when Status == "interrupted"
    CancelReason     string         // populated when Status == "cancelled"
}
```

Side-channel fields are populated only for the matching status. `Run`'s Go-level `error` return is reserved for
infrastructure failures (DB, timeout); a *workflow failure* surfaces as `Status == "failed"` with `Error` set, so
callers don't disambiguate "the workflow rejected my input" from "the engine is down."

The interrupt path is split from `State`: `Snapshot` of an `interrupted` flow returns `State` as the merged step
snapshot *at the time of the interrupt* and `InterruptPayload` as the raw `flow.Interrupt(payload)` argument. Folding
the payload into `State` was lossy (the caller could not tell workflow state from the resume request). Callers wanting
the merged view call `workflow.MergeState(out.State, out.InterruptPayload, graph.Reducers())` themselves.

### Flow Stop Notifications

When a flow is started via `StartNotify(flowKey, notifyHostname)`, the engine stores the hostname and invokes the
`FlowStoppedCallback` with a `*workflow.FlowOutcome` when the flow stops - terminal (`completed`/`failed`/`cancelled`)
or `interrupted`. This matches the statuses `Await` returns on. The outcome carries the same fields as a `Snapshot`/
`Await` return at the stop point. Delivery is fire-and-forget; flow execution never blocks on it. `notify_hostname`
is set only on the root flow; subgraph flows do not notify directly (interrupt notifications query the root's
hostname via the surgraph chain).

### Execution Model

The engine uses a **queue-as-cache execution model** with a configurable worker pool (`WithWorkers`) and a single
refiller goroutine per replica. The in-memory `candidateCache` is bounded and holds *hints*, not ownership. Each
worker pops a candidate and calls `processStep`:

1. Reserve the step (atomic CAS `UPDATE ... WHERE step_id=? AND status='pending' AND parked=parkedNone AND
   not_before<=NOW AND lease_expires<=NOW`).
2. Check for terminal flow status (abort if cancelled/failed/completed).
3. Load the flow's graph, config, and baggage.
4. Execute the task via the injected `TaskExecutor` with a time budget on the call context.
5. Persist changes, evaluate transitions, create next steps (in a transaction), ring the doorbell.

Acquisition is the atomic CAS, so a stale or duplicated candidate is harmless: the CAS loser gets zero rows and pops
the next. The cache holds hints, never ownership; only the CAS grants a step. **The CAS predicate includes
`parked=parkedNone`**: a step that was offered to the cache and then parked (subgraph or breaker) is rejected at
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

**Queue-as-cache, doorbell, single-slot refiller.** `Enqueue` carries no step to a queue; it is a **doorbell**
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

The timer waits on **two independent deadlines**, `nextPoll` and `nextProbe`, waking on whichever is sooner.
`nextProbe` is the soonest tripped-breaker probe time, owned by the breaker subsystem (`refreshNextProbeLocked`,
recomputed only on breaker trip/commit/close transitions, read by the timer as a lock-free atomic). Keeping it
separate is load-bearing: an earlier design folded the breaker probe into `nextPoll`, but `pollPendingSteps` resets
`nextPoll` to the backlog cadence every cycle, which clobbered the probe wake whenever a poll landed as the probe came
due - collapsing the breaker's 100ms..1m exponential schedule to the 1-minute cadence. The timer loop (`timerLoop`)
runs `pollPendingSteps` on the adaptive interval.

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

Each step has a `time_budget_ms` that bounds the `TaskExecutor` call's context deadline. It is the engine's single
`WithTimeBudget` config (default 2m), applied uniformly to every step - the graph carries no per-task timing. A host
that wants a tighter per-task bound enforces it inside its `TaskExecutor` (or the task itself), shortening the call
context; the engine's budget is the outer ceiling.

The worker lease duration is the current `TimeBudget` config + `leaseMargin` (30s), read from in-memory config rather
than the step row. This lets `processStep` claim the step without an upfront SELECT for `time_budget_ms`. The lease
always outlasts execution, preventing premature recovery by `pollPendingSteps`. Trade-off: because the lease is sized
to the ceiling rather than per task, a *crashed* worker that ran a tight-budget task is recovered no sooner than
`ceiling + leaseMargin`. A separate trade-off: if the operator decreases `TimeBudget` mid-flight below the largest
in-flight step's stored `time_budget_ms`, the new (shorter) lease may expire before the (longer) dispatch completes,
causing re-dispatch. Steps not idempotent under re-dispatch should not coexist with such config changes.

### Flow lifetime is the workflow author's responsibility

The engine imposes no flow-level deadline. Picking a max-lifetime that fits both a 1-hour batch and a 30-day approval
workflow is impossible, and a knob defaulting to "no deadline" is surface area without a customer. Workflows needing a
bound implement it in author space: a guard task reading `flow.CreatedAt()` that returns a 408 when too much time has
elapsed; a `flow.Retry` loop that exhausts after a chosen bound; an `OnError`/timeout transition; or an external
caller scheduling a `Cancel`. `Flow.CreatedAt()` and `Flow.UpdatedAt()` are populated on every dispatch, so the
elapsed-time guard is one call away inside any task.

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

- **`flow.Retry(maxAttempts, initialDelay, multiplier, maxDelay) bool`** - re-execute this task with exponential
  backoff. Returns `true` while attempts remain (task should return `nil`), `false` at `maxAttempts` (task should
  return its error). Pass zero delays for immediate retry, `math.MaxInt32` for unlimited. The step row is reused. The
  engine tracks `attempt` and computes `min(initialDelay * multiplier^attempt, maxDelay)`, merging `state + changes`
  back into `state` so the task sees its prior output. Both `flow.Retry` and the operator-facing `RestartFrom` rewind
  a step row in place; `RestartFrom` additionally sweeps the step's downstream subtree and merges optional state
  overrides, for operator-driven recovery after a flow has already terminated. **A retry clears the park
  slot** (`interrupt_done`/`subgraph_done` -> 0, `resume_data`/`subgraph_result` -> `'{}'`, `subgraph_error` -> `''`),
  so a retry after a resolved `flow.Subgraph` re-runs the child and after a resolved `flow.Interrupt` re-interrupts.
  `flow.Retry` carries no condition - the task writes the retryable condition explicitly in the surrounding `if`
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
  `FlowStoppedCallback` on the root's `notify_hostname`.

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

**Subgraph is a function call.** Only the explicit `input` map passed to `flow.Subgraph(url, input)` crosses into the
child as its initial state; only the explicit `out` map (the child's `final_state`) crosses back. The parent's state
and accumulated changes do NOT auto-cross either direction.

**Into the child:** `SubgraphRequested` passes `subgraphInput` directly to `createSubgraphFlow` as the child's
initial state (nil normalized to `{}`). No merge with parent state. A caller wanting the parent's full state passes
`flow.Snapshot()` as `input` - explicit opt-in.

**Back to the parent:** `completeSurgraphFlow` writes the child's `final_state` to the surgraph step's
`subgraph_result` column, sets `subgraph_done=1`, and re-dispatches the parent task. The task's `flow.Subgraph` call
returns that `final_state` as `out` (yield=false), and the task adopts the fields it wants. The child output is
**not** merged into the parent's `changes`.

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

The opaque `baggage` map captured at `Create` (stored in the `baggage` column) is passed to every
`GraphLoader` and `TaskExecutor` call for the flow's lifetime, including dispatches long after creation. The engine
never interprets it; a host uses it to carry the original caller's identity (e.g. mint a fresh token inside its
`TaskExecutor`).

### Await

`Await` blocks until a flow stops (no longer `created`/`pending`/`running`); it returns on `completed`/`failed`/
`cancelled`/`interrupted`. It registers a buffered channel in the `waiters` map, then loops: check state, return if
stopped, otherwise `select` on the channel or context cancellation. There is **no periodic re-snapshot** - it wakes
only on a notification or ctx. Non-terminal notifications (e.g. `running` from `Start`) re-check state rather than
returning early.

**Cross-replica `Await`.** A flow created on one replica but completed on another wakes a local `Await` only via the
`PeerNotifier.NotifyStatusChange` broadcast. Every flow-stop site calls an internal `signalStop` helper that does the
local waiter wake *and* the peer broadcast; the receiving replica routes it to `HandleNotifyStatusChange`, which wakes
its local waiters. Without this wiring, an `Await` on the replica that did not run the final step blocks until its
context deadline (there is no poll fallback). Non-terminal (`running`) transitions are notified locally only,
matching the broadcast-only-on-terminal-stops policy.

### SQLite Testing Support

`engine.RunInTest(t)` runs Startup with per-test SQLite databases and registers cleanup via `t.Cleanup`. An empty DSN
selects SQLite in-memory; each shard is routed through `sequel.CreateTestingDatabase` with a per-shard test ID so the
shards are isolated databases (folding the shard index into the test ID is what keeps a multi-shard `RunInTest` from
collapsing every shard onto one shared in-memory DB). Key SQLite differences from server databases:

- **Write-first transactions** - `advanceFlow` does an `UPDATE` as the first operation to immediately acquire a write
  lock. On MySQL/Postgres this serializes concurrent workers (like `SELECT ... FOR UPDATE`). On SQLite with
  `cache=shared`, starting with a write avoids the deadlock where two read-first deferred transactions both hold
  SHARED locks and neither can upgrade.
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
`innodb_deadlock_detect = ON`. Per-shard databases: `WithDSN` must contain `%d` when `NumShards > 1` and every shard
DB must exist before startup (the engine migrates schema but does not `CREATE DATABASE`). MariaDB 10.5+ for `JSON`.

**SQL Server.** Enable `READ_COMMITTED_SNAPSHOT ON` per shard database for Postgres-like non-blocking reads and
near-zero deadlock risk. No other tuning mandatory.

**SQLite - testing and single-instance dev only.** Single-writer means deadlocks are structurally impossible (writes
serialize) but throughput tops out at one transaction at a time. Used automatically by `RunInTest` with an empty DSN.
The injected `busy_timeout` keeps workers from immediately failing on `SQLITE_BUSY` during fan-out; do not remove it.
Do not run SQLite in production.

**Sharding guidance.** `WithNumShards` partitions flows across databases (or schemas). Shard count should equal or
exceed steady-state concurrent flow-creating threads divided by the per-shard write contention the engine tolerates.
Rough sizing:

| Engine | Concurrent INSERT/sec per shard before contention | Suggested NumShards |
|---|---|---|
| PostgreSQL | 1000+ | 1-4 |
| SQL Server (RCSI) | 500-1000 | 2-4 |
| MariaDB/MySQL (RC) | 200-500 | 4-8 |
| MariaDB/MySQL (RR) | 50-200 | 8-16 |

`NumShards` can grow at runtime via `WithNumShards`; it cannot shrink (old shards drain naturally). New flows land on
new shards; existing flows stay on their original shard.

**Connection pool sizing (`WithMaxOpenConns`, default 8 per shard, MaxIdle == MaxOpen).** Workers spend most of their
time waiting on the `TaskExecutor` call, not holding a SQL connection - the DB-only segments of `processStep` are
short. So the per-shard pool needs only a small absolute number of connections. `MaxIdle == MaxOpen` matters more
than the absolute number: under bursty load the close/reopen churn (TCP+TLS+auth per cycle) dominates over query
time. Empirically pool=8 is a good default; pool=32 regresses (pool-mutex contention with no usable extra
concurrency). Operators with a different workload mix (longer DB-hold, larger shards) should tune explicitly.

### Flow Scheduling (priority / fairness)

The schema carries `priority`, `fairness_key`, `fairness_weight` on **both** `dwarf_flows` (authoritative) and
`dwarf_steps` (denormalized), so the two-level selection never joins `dwarf_flows` on the hot path - the same
split used for `time_budget_ms`/`baggage`.

`resolveFlowOptions` resolves a caller's `*workflow.FlowOptions` against the engine defaults: priority falls back to
`WithDefaultPriority`, the fairness key falls back to the host-supplied key (or `""`), the weight to `1`. The three
values are immutable for the flow's life (switching a flow's fairness key mid-run would be a self-promotion abuse
vector). `Create`/`CreateTask` resolve from options; `Continue` and `createSubgraphFlow` **inherit** the parent/thread
flow's values, so a high-priority parent never silently spawns default-priority descendants.

Propagation onto step rows: where the resolved values are in hand (the entry step), they are literal bind parameters
(`Restart`/`RestartFrom` rewind their target step in place via UPDATE, so the row's immutable priority/fairness
values are already present and need no re-propagation); in the deep `processStep` paths (fan-out and the two fan-in
inserts), the values are read once per step execution in the parallel flow-row SELECT and threaded through the call
chain into the INSERTs as bind parameters (vs. the previous scalar subqueries, which meant 3N PK lookups per N-way
fan-out).

**Why the scheduling design is shaped this way:**

- **Priority is a property of the flow, not the task or workflow type.** Step order *within* a flow is dictated by the
  graph, not urgency; priority only arbitrates *between* flows competing for workers, so it is resolved once at
  `Create` and immutable (`workflow.FlowOptions` is flow-level for the same reason).
- **Fairness weight is denormalized at `Create`, never resolved on the selection path** (a resolver hook would put
  synchronous I/O on the hot critical section). When a key's steps carry inconsistent weights, the oldest candidate
  step's weight is used; keeping weights consistent for a key is the caller's responsibility.
- **`Workers` is a generous static cap, not the backpressure mechanism.** A worker blocked on a `TaskExecutor` call is
  just a goroutine stack plus a socket, so over-provisioning is cheap; throttling a pressured downstream is the
  adaptive per-task dispatch-rate control, not pool size.
- **Completion writes are deliberately not gated by the refiller slot.** That slot bounds selection only; finishing
  in-flight work must outrank starting new work, so the post-execution advance is never serialized behind selection.

> Observability note: per-priority backlog/age and distinct-fairness-key counts are aggregate-only metrics by design
> (per-key labels would be unbounded cardinality). Metric emission is deferred in the engine and is a host concern;
> the engine exposes the underlying data through logging and return values.

### Adaptive Per-Task Concurrency

A soft, cluster-wide cap on per-task **dispatch rate** (ops/sec), discovered from observed backpressure rather than
configured. Lives in `backpressure.go`; the `runRefill` integration in `scheduling.go`; the 429 ingestion in the
dispatch error path of `execution.go`. The controller controls *rate*; concurrent in-flight count is incidental (the
worker pool sets a separate hard ceiling, and concurrency for a given rate is Little's Law: `c = rate * latency`).

**Why a controller and not a config.** A static per-task cap is wrong the moment the downstream autoscales: too low
underutilizes, too high overwhelms, and a hardcoded value defeats the controller it would feed (the downstream emits
429 at exactly that count forever, so the controller never discovers grown capacity). A host that wants to self-limit
does so internally, emitting 429 above its threshold; from the engine's side that is any other organic 429.

**Why rate and not concurrent count.** Most 429-emitting backends are rate-bounded (third-party APIs publishing
"100 req/sec"), not concurrency-bounded. A concurrent-count controller has a floor problem with short tasks against
tight budgets: at rate budget `R` and latency `L`, the matching concurrent cap is `c = R*L`; for `R=10`, `L=10ms`,
`c=0.1`, below the `minLimit=1` floor, so the controller pins at 1 concurrent and issues 100 req/sec - ten times the
budget. A rate controller pushes the floor into ops/sec, matching the typical lower bound on real APIs (1 RPS). The
symmetric failure mode (variable-latency concurrency-bounded backends) still recovers via 429 feedback; we accept
that trade.

**The throttle library does the per-call gating.** Each task has its own `throttle.Throttle` (sliding-window counter,
1s window, ~64 bytes per task). It counts emitted dispatches (so a 429 can be anchored to the actual emission rate)
and gates further dispatches when the rate exceeds the controller's current opinion. Created lazily at first dispatch;
the gating limit is set lazily from the CUBIC curve's current value at each peek/commit. Allows are at commit time;
peeks gate the filter pass in `runRefill`.

**Additive-decrease per 429, re-anchored at burst start.** `valveRegulate` decrements `wCong` by exactly 1 on each
observed 429, so a burst of N 429s compounds linearly to a cut of N - the exact overage the downstream just rejected.
Only the *first* 429 of a fresh burst (tCong older than one throttle window) re-anchors `wCong` upward to the actual
emission rate; subsequent 429s just decrement. The re-anchor matters because the CUBIC curve has been growing the
effective rate above the last cut's `wCong` while the task ran cleanly.

**The increase is a pure function, not an action.** N replicas running independent additive-increase loops over one
downstream oscillate. Resolution: make the increase a pure function of `(wCong, tCong)` and wall-clock. Every replica
computes the same value from the same anchor; "doing the increase" is not an action anyone performs.

**The convergent register.** The only shared mutable state is `(wCong, tCong)` per task, gossiped over `SyncValve`.
The merge is "latest `tCong` wins, smaller `wCong` on tie" - associative, commutative, idempotent, tolerating
reorder, duplication, and loss. A missed cut self-heals: the over-admitting replica eats its own 429 and re-cuts.

**Sender-stamps `tCong`, not receiver.** The whole design rests on every replica computing the same
`recover(wCong, tCong, now)`. Receiver-local stamping would make each replica's curve advance by a different
`now - tCong`. Cross-replica clock skew (same datacenter, NTP-synced) is milliseconds; the curve operates over
seconds.

**Cross-task priority inversion is accepted.** When a high-priority flow's next task is rate-limited, that flow is
delayed (step stays pending) while lower-priority flows whose next task is unconstrained proceed. The `runRefill`
multi-band loop walks past a wholly-saturated band rather than idling the cluster. The flow is delayed, not failed -
the 429 path bounces the step to `pending` with `not_before = NOW_UTC()`, never `failed`.

**Each replica is independent; no peer count, no rate division.** With the per-replica throttle, each replica sees
only its own dispatches and 429s, so the `wCong` it cuts to is *already* the per-replica rate. The cluster aggregate
is the sum of independent per-replica controllers, each converging to its own share via its own 429 feedback.
Asymmetric load produces asymmetric `wCong`, which is correct.

**The recovery curve is TCP CUBIC** (RFC 9438). `recoverRate` evaluates `w(t) = cubicC*(t-K)^3 + wMax`, where
`K = cbrt(wMax * cubicBeta / cubicC)` and `wMax = wCong / (1 - cubicBeta)`. Below `K` concave (fast reclaim toward
the known-good `wMax`); above `K` convex (gentle probing above the pre-cut estimate). At `t=0` it equals `wCong`; at
`t=K`, `wMax`. The recovered rate is clamped to `[1, MaxInt]`: the floor prevents recovery deadlock (at rate zero no
step dispatches, no 429 fires, the cube term never lifts the curve); the ceiling is overflow insurance only.
`cubicBeta` (0.01) and `cubicC` (0.05) are package-level constants; the cut depth is a fixed 1 op/sec per 429 and the
throttle window a fixed 1 second - the controller converges on whatever rate the downstream tolerates regardless of
the downstream's stated unit.

**The bounced step has no added `not_before` delay.** A 429 returns the step to `pending` with `not_before = NOW`.
The throttle is the gate, not the row: the re-attempt hits `valvePeek` and waits unless the throttle has room.

**`runRefill`'s band loop is unbounded.** It walks priority bands ascending until admittable work is found or
`scanBand` returns `MaxInt` (no pending work anywhere). An earlier `BandFallthroughLimit` cap was removed because
giving up early just deferred the scan and introduced a stall when more than the cap's worth of bands were
simultaneously saturated.

**Fairness within a key is FIFO among admissible steps, not absolute FIFO.** Within a fairness key, the refiller walks
oldest-first but skips any step whose task's throttle currently refuses; the skipped step is re-considered next refill
once the throttle has room. This preserves strict-FIFO for the admissible subset while preventing head-of-line
blocking: one rate-limited task does not freeze the tenant's other admissible work. The key's weight follows the same
rule - taken from the oldest *admissible* step.

> Observability note: the rate-limit state per task would be a useful operator gauge (log y-axis spanning the cut
> state and the unbounded ceiling), but metric emission is deferred; the engine exposes the data via logging.

### Step Parking (`parked` column)

`dwarf_steps.parked SMALLINT NOT NULL DEFAULT 0` takes a step out of the selection band without changing its
`status`. The selection index `(status, parked, priority, fairness_key)` and saturation index
`(status, parked, task_name)` lead with the partitioning columns, so parked rows are physically excluded from every
hot-path scan - no in-memory filter at refill time. The `parked` value labels *why* the step is held:

- `parked=0` (`parkedNone`, default) - active. Selection sees it; `pollPendingSteps` recovers it if its lease
  expires; saturation counts it as one in-flight slot. (Also the precondition the claim CAS requires.)
- `parked=1` (`parkedSubgraph`) - the step called `flow.Subgraph` and is waiting for the child. `status='running'`
  (logically running, blocked on its child) but excluded from selection, saturation, AND lease-expiry recovery. No
  lease deadline - the row sits until `completeSurgraphFlow` flips it back to `(pending, parked=0)`. This replaced an
  earlier `lease_expires = NOW + 7 days` "park" indicator that broke for subgraphs running longer than 7 days
  (the lease lapsed, the parent recovered, the task re-ran, launching a duplicate child).
- `parked=2` (`parkedBreaker`) - the task has a tripped breaker and this step is on the held-back backlog.
  `breakerBulkPark` parks every pending step for the task to parked=2 in a single UPDATE, then elevates the oldest per
  shard back to parked=0 as the probe. Both UPDATEs run in one per-shard tx, so the elevated probe is guaranteed
  `pending` at commit (the park UPDATE's row locks block concurrent Cancel/Fail). The probe's `not_before` carries the
  exponential schedule. New steps inserted while tripped are born parked=2 (via `initialParkedFor` at INSERT) so they
  join the backlog without burning a dispatch. On probe success `breakerClose` runs `breakerBulkUnpark` scoped to
  *that shard only* - other shards stay held until their own probes succeed (rolling-wave recovery, not a unison
  flood).

**Terminal status implies `parked=parkedNone`.** The park value is meaningful only while a step is actively waiting.
Once terminal (`completed`/`failed`/`cancelled`), the park slot is gone, and the column must read `parkedNone`. Every
terminal-transition code path resets `parked` in the same UPDATE (the `failStep` write, the `deliverSubgraphError`
child-leaf write, the `Cancel` cascade, the `processStep` terminal-flow guard). Without this, a step that was parked
when its flow was cancelled would sit terminal with non-zero `parked` - invisible to the selection index but never
re-leased, stranding any subsequent `Restart`/`RestartFrom` (which sets `status=pending` but the partitioned index
still excludes a `parked != 0` row). `Restart`/`RestartFrom` also re-zero `parked` as defense-in-depth.

**Insert paths consult the in-memory breaker map** via `initialParkedFor(taskName)` and bind the result into `parked`,
so a new pending step for a tripped task is born parked=2. The CREATED-inserting entry step leaves `parked=0` (not yet
selectable); `Start` runs `parkTrippedSteps` inside its transition transaction to honor the breaker at the moment
CREATED becomes PENDING.

**No stale-dispatch race during trip.** `handleBreakerTrip` bounces the failed step to `parkedBreaker` (not
`parkedNone`), so the refiller's `parked=0` filter cannot pick up the just-bounced row before `breakerBulkPark` runs.
The bulk-park tx holds row locks across the SELECT and elevate UPDATE, so the chosen probe is guaranteed `pending`
at commit.

### Per-Task Breaker (404 ack-timeouts, 503 unavailable, 529 overloaded)

A per-task circuit breaker rides alongside the valve. The valve handles 429 backpressure ("the downstream is
rate-limiting"); the breaker handles "I cannot serve right now" signals: a 404 ack-timeout ("no one is home"), 503
Service Unavailable ("downstream not ready / in maintenance"), and 529 overloaded ("the downstream is collapsed"). The
valve gates via `valvePeek` in `runRefill`; the breaker gates via the `parked` column, which takes blocked rows out
of the selection index entirely. They operate independently; a task with both signals tripped has its rate cut AND
its backlog parked.

> The error classification (which `TaskExecutor` error is a 429 vs an ack-timeout 404 vs 503/529) is currently
> hardcoded in the dispatch error path (status code plus, for 404, an `ack timeout:` string prefix). That prefix is a
> Microbus-bus artifact; a standalone host using a different transport has no such signal. A cleaner design injects
> the classification as a callback so the host owns the mapping. Until then, a host must surface "downstream
> unreachable" as `404` with an `ack timeout:`-prefixed message to engage the breaker.

**Why 503 and 529 go to the breaker, not the valve.** Both mean the downstream is unavailable, not serving at a
reduced rate. The valve's additive-decrease-with-CUBIC-recovery is designed for the latter; applied to a downed
downstream, every dispatch keeps tripping, the rate drives toward zero, and the recovery curve then re-floods the
still-down service. The breaker's trip + exponential probe schedule (100ms, 200ms, 400ms, ..., capped at 1m) lets the
downstream actually recover before the next request. A handler-emitted 503 is a deliberate "back off and try later."

**Why per-task and not per-step.** A per-step exponential backoff scales with backlog depth: with 100 flows blocked
on a 404'ing task, every refill cycle would dispatch and bounce all 100 on individual schedules - an avalanche
displacing healthy tasks. The schedule lives at the *task name* level so one schedule governs all blocked flows.

**The parked column is the gate; the probe rides on `not_before`.** On trip, `breakerBulkPark` parks every pending
step to `parked=2`, then SELECTs the oldest and elevates it back to `parked=0` as the probe. The selection index is
partitioned on parked, so the refill never sees the held-back backlog. The probe's `not_before` is set to
`nextProbeAt`, so the existing selection machinery (which already honors `not_before` for `flow.Sleep` and retry
backoff) schedules the probe at the exponential cadence. On probe success, `breakerClose` runs `breakerBulkUnpark`
on the success shard only.

**Trip on the first signal.** Any single ack-timeout 404, 503, or 529 flips the breaker tripped - no consecutive-nack
counter, no threshold. The cost of an over-trip is one `breakerInitialProbeDelay` (100ms) on the next dispatch: trip
-> 100ms wait -> probe -> success (when transient) -> close. The cost of an under-trip is the avalanche. Matches TCP,
where one loss event triggers congestion response.

**Probe schedule is local, exponential, capped.** When tripped, exactly one step per shard is left unparked as the
probe, gated on `not_before <= NOW`. The schedule advances on each probe DISPATCH, not on failure: `processStep`
calls `breakerCommit(taskName)` just before executing any step whose task has a tripped breaker. **`breakerCommit`
advances only when the scheduled probe is due (`now >= nextProbeAt`), not unconditionally.** This is load-bearing: a
burst of in-flight dispatches admitted just before the trip stays in flight and returns 404s after the trip, and the
candidate cache can still hand a worker a step that was parked after being offered (the claim CAS rejects it, but
only after a pop). Advancing for every such call would ramp `probeAttempt` straight to the backoff cap in one window
and stall recovery; the probe-time gate keeps the schedule to one bump per real probe. The four constants
(`breakerInitialProbeDelay` = 100ms, `breakerProbeMultiplier` = 2.0, `breakerMaxProbeDelay` = 1m, trip-after-first)
are package constants. Sequence: 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 6.4s, 12.8s, 25.6s, 51.2s, 60s, 60s, ...;
full convergence to the cap takes ~2 minutes of continuous failure. `nextProbeAt` and `probeAttempt` are not gossiped
- each replica advances its own state on observed admissions, and the probe's `not_before` is a last-writer-wins
UPDATE across replicas.

**`breakerTrip` arms the schedule; `breakerCommit` advances it; `refreshNextProbeLocked` syncs the timer.**
`breakerTrip` is called for every observed signal - on a fresh trip it arms the first probe (`probeAttempt = 0`,
`nextProbeAt = now + 100ms`); on a repeat it is a no-op so concurrent in-flight 404s do not ratchet the schedule.
All three transitions call `refreshNextProbeLocked`, which recomputes the soonest probe into the timer's `nextProbe`
deadline. `nextProbe` is a *separate* deadline from `nextPoll` so `pollPendingSteps`' per-cycle `nextPoll` reset
cannot clobber the probe wake.

**`handleBreakerTrip` serializes bulk-park per task and runs it transactionally.** The failed probe is demoted to
`parkedBreaker` and a replacement probe is re-elected within one `sequel.Transact` per shard, so a rollback can never
leave the task with the probe demoted but no replacement elevated - which would wedge recovery permanently (no probe
dispatches, so no further 404 re-trips, and nothing re-runs bulk-park). `Transact` retries the park-and-elevate on
lock contention, and a per-task in-process lock (`breakerParkLocks`) serializes concurrent trips for the same task so
a burst of trips doesn't issue overlapping `task_name` UPDATEs that deadlock under pessimistic locking. A probe
always survives a trip.

**`TripBreaker` gossips only the trip event, payload-free.** The wire is just `(taskName)`. Each peer stamps
`time.Now()` on receipt, seeds its local probe schedule from that moment, AND runs `breakerBulkPark` so its view of
the task's pending set converges with the originator's. `breakerBulkPark` is idempotent SQL. A trip received while
already tripped is a no-op (re-parking would reset `probeAttempt` and undo accumulated backoff).

**Closures are deliberately NOT gossiped, and the unpark UPDATE is shard-scoped.** A replica that probes successfully
drops its local `trippedAt` to zero and unparks the success shard only; other shards stay parked=2 until their own
probes succeed (rolling-wave recovery). If downstream 404s again after a shard recovers, the next dispatch on that
shard re-trips. Small consistency window: when R1's probe on shard X succeeds and unparks shard X, R2's in-memory
breaker is still tripped until R2 also probes successfully (which happens when R2 picks up one of the newly-unparked
rows) - a one-dispatch-per-replica bounded window. The state is a single `trippedAt time.Time` (zero = closed).

**Reconstitution on startup.** A restarting replica's in-memory breaker map is empty, but `parked=parkedBreaker` rows
survive in the DB and are invisible to selection. `reconstituteBreakers` (called inside Startup between the
breakers-map init and the worker goroutines starting) scans each shard for `SELECT DISTINCT task_name FROM
dwarf_steps WHERE parked=parkedBreaker AND status=pending` and calls `breakerTrip(taskName)` for each. The schedule
starts fresh on this replica (first probe at `now+100ms`), independent of peers. No gossip - sending our `time.Now()`
would clobber peers' accumulated `probeAttempt`. The DB-side state is already what `breakerBulkPark` would produce,
so no SQL writes are issued. If the task was fixed during downtime, the first reconstituted probe succeeds and
`breakerClose` un-parks normally. The rejected alternative - a blanket `UPDATE parked=0` at startup - would mass-
unpark every replica's backlog in a multi-replica deployment, flooding a known-broken endpoint.

**No auto-give-up on a forever-tripped breaker.** Same argument as flow lifetime: a breaker tripped for hours might be
a long maintenance window, and the workflow's own elapsed-time guard is the right arbiter. Bulk remediation (cancel
everything blocked on task X) is operator surface area, out of scope for the breaker.

**Other 5xx (500, 502, 504, ...) are not the breaker's business.** A 5xx other than 503/529 means the downstream
exists, served the request, and answered with a server error - a task-level decision (`flow.Retry`, an `onError`
transition, or propagate as flow failure). The breaker is specifically for "I cannot serve right now" signals.

## Metrics (`engine/metrics.go`)

The engine emits 15 `dwarf_*` instruments through the **OTEL metric API** (not the SDK). `WithMeterProvider`
injects the provider; it defaults to the global `otel.GetMeterProvider()` - the no-op provider unless the host
configures the SDK, so unconfigured/standalone/test use pays nothing. Instruments are built once in
`initMetrics` (called from `initRuntime`, so both `Startup` and `RunInTest` get them) from
`mp.Meter("github.com/microbus-io/dwarf")` - that scope distinguishes dwarf's metrics; **service identity lives
in the provider's Resource, not in per-metric attributes** (no `service.name` on data points - that would
explode cardinality and is off-spec). The only attributes the engine attaches are the metric-specific labels:
`workflow`, `status`, `task`, `cause`, `outcome`, `priority`.

**8 counters, incremented inline** at the same logical event sites the foreman used: `dwarf_flows_started_total`
(start path), `dwarf_flows_terminated_total` (completeFlow), `dwarf_steps_executed_total` (every terminal step
disposition - completed/failed/interrupted/subgraph/retried/error_routed), `dwarf_steps_recovered_total`
(pollPendingSteps lease recovery), `dwarf_steps_skipped_saturated_total` (refill admission skips),
`dwarf_task_rate_cuts_total` (valveRegulate), `dwarf_task_breaker_trips_total` /
`dwarf_task_breaker_probes_total` (handleBreakerTrip + breakerClose). The inline helpers no-op when
`e.metrics == nil` (before Startup).

**7 gauges, observable (async)** via a single `RegisterCallback`, replacing the foreman's `OnObserve*`
endpoints. The callback runs at metric-collection time and reads engine state: in-memory for
`dwarf_steps_queue_depth` (cache length), `dwarf_task_rate_limit` (valves with an anchored `wCong`),
`dwarf_task_breaker_state` (breaker map), and `dwarf_steps_fairness_keys` (the last refill's selected band +
distinct-key count, stashed under `lastRefillLock` by the refiller); shard queries for `dwarf_steps_pending`
and `dwarf_steps_oldest_pending_age_seconds` (per priority band) and `dwarf_task_concurrency_running` (running
steps per task). Gauges emit **per replica**; cluster-wide aggregates are summed at the backend. The callback is
`Unregister`ed first thing in `drainRuntime` so the OTEL reader can't query a closing database.

**Fidelity choices vs. the foreman:** `flows_terminated` fires only on `completed` (the foreman never counted
failed/cancelled; `steps_executed{status=failed}` still covers the failed-step case). Subgraph flows are counted
like the foreman - the start path and `completeFlow` run for them too - so no `surgraph_flow_id` filter; the
`workflow` label lets dashboards slice root-vs-subgraph. `TestMetrics_EmittedOnRun` pins emission with an
in-memory SDK `ManualReader`.

> Observability note (deferred): tracing is not yet wired (no `WithTracerProvider`/`trace_parent` use). The
> per-priority/per-task gauges are aggregate-only by design - no per-`fairness_key` labels (unbounded
> cardinality).

## Schema Column Catalog

The `migrations/*.sql` migration files carry **no prose comments by design** - only the functional
`-- DRIVER: <dialect>` directives the `sequel` runner parses. All schema rationale lives here.

#### `dwarf_flows`

| Column | Meaning |
|---|---|
| `flow_id` | Per-shard auto-increment primary key. The external flowKey is `{shard}-{flow_id}-{flow_token}` |
| `flow_token` | Random token component of the flowKey, guards against id guessing |
| `workflow_name` | Name of the workflow graph this flow runs (the name passed to the `GraphLoader`) |
| `graph` | JSON of the workflow graph, frozen at `Create` time |
| `baggage` | JSON of the opaque `baggage` map captured at `Create` and passed to every `GraphLoader`/`TaskExecutor` call. Flow-scoped and frozen at `Create`; the engine does not interpret it |
| `status` | Flow lifecycle: `created`/`running`/`interrupted`/`completed`/`failed`/`cancelled` |
| `step_id` | The flow's current step; `0` during fan-out (multiple steps active at one depth) |
| `surgraph_flow_id` | Parent (surgraph) flow id if this is a subgraph flow; `0` otherwise |
| `surgraph_step_depth` | The parent's `step_depth` that spawned this subgraph |
| `surgraph_step_id` | PK of the parent's parked surgraph step, so a subgraph flow identifies its surgraph step unambiguously when parallel parked surgraph steps coexist at one depth |
| `thread_id` | Groups flows in a `Continue` thread; defaults to `flow_id` (each flow its own thread) |
| `thread_token` | Token component of the thread's flowKey |
| `trace_parent` | W3C `traceparent` for distributed-trace continuity (stored and carried; the engine does not currently create spans from it - tracing is deferred) |
| `notify_hostname` | Set by `StartNotify`; the `FlowStoppedCallback` fires with this hostname when the flow stops. `''` = no notification |
| `final_state` | JSON state computed at termination - the full merged state of the terminal step(s), unfiltered. Narrowing happens in the workflow's terminal task via `flow.Delete`/`Transform` |
| `breakpoints` | JSON `map[taskName]string` of `BreakBefore` breakpoints |
| `created_at` | UTC creation time. Append-only and PK-correlated. Surfaced to tasks via `Flow.CreatedAt()`. Reset by `Restart` (a fresh attempt); NOT by `RestartFrom` (a surgical rewind) |
| `started_at` | UTC time this attempt began dispatching. Stamped by `Start` on `created` -> `running`. Reset by `Restart`, not `RestartFrom`. Distinct from `created_at` because a flow can sit `created` indefinitely or be parked behind a tripped breaker before its entry dispatches. Drives `FlowSummary.Duration()` (`updated_at - started_at`) |
| `updated_at` | UTC time of the last status transition. Surfaced to tasks via `Flow.UpdatedAt()` |
| `priority` | Scheduling priority, integer >= 1, lower runs first. Resolved at `Create` from `FlowOptions` else `WithDefaultPriority`; inherited unchanged by `Continue`/subgraph. Immutable |
| `fairness_key` | Fairness bucket. From `FlowOptions`, else the host-supplied key, else `''`. Immutable |
| `fairness_weight` | Relative dispatch share of the `fairness_key`. From `FlowOptions`, else `1` |
| `error` | Task error string for `failed` flows. Written by `failStep` to every flow in the surgraph chain in the same UPDATE that sets `status='failed'`; the `WHERE status NOT IN (terminal)` clause makes the write first-failure-wins. Surfaced as `FlowOutcome.Error` |
| `cancel_reason` | Reason passed to `Cancel(flowKey, reason)`. Written to every flow in the cancellation chain in the same UPDATE that sets `status='cancelled'`, first-cancel-wins. Surfaced as `FlowOutcome.CancelReason` |

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
| `time_budget_ms` | Execution budget; the deadline on the `TaskExecutor` call context |
| `breakpoint_hit` | `1` once a breakpoint on this step has fired, so it does not re-trigger on resume |
| `attempt` | `flow.Retry` attempt counter, drives the backoff |
| `not_before` | Earliest UTC time the step may execute (`flow.Sleep` / retry backoff / breaker probe) |
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
| `parked` | Selection discriminator. `0` = active; `1` = surgraph park; `2` = breaker park. The selection and saturation indexes lead with `(status, parked)` and the claim CAS requires `parked=parkedNone`, so non-zero rows are excluded from the hot path. See "Step Parking" |

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
| `idx_dwarf_flows_workflow_name` | `(workflow_name)` | `List` by workflow name |
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
| `idx_dwarf_steps_saturation` | `(status, parked, task_name)` - partial as above | Per-task in-flight count for the adaptive controller. Parked rows excluded so a surgraph parent doesn't inflate the executing-slot count |

### Data Retention

The engine does not auto-purge flows: every row remains potentially-resurrectable - an `interrupted` flow via
`Resume`, a `completed` flow via `Continue`, a `failed` flow via `Restart`/`RestartFrom`. A fixed timer would silently break these,
and no single policy fits both a 1-hour batch and a 30-day approval workflow. For operator-driven retention:

- **`Delete(flowKey)`** removes one flow and its steps in a transaction. Refuses a running flow (409). Subgraph
  children and thread descendants are left dangling.
- **`Purge(Query)`** bulk-deletes flows matching the query, except running. Same `Query` shape as `List` (Status,
  WorkflowName, ThreadKey, TaskName, FairnessKey, Priority, OlderThan, Shard, Limit). Capped at 10000 per call;
  returns the count deleted. The non-running guard is enforced inside the DELETE.

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
is the `TaskExecutor` call: `executeTask` derives it from the lifetime ctx with the step's `time_budget_ms`.

### Shutdown ordering: drain workers, then timer, then refiller

`signalTimer` (the sender behind `shortenNextPoll` and `refreshNextProbeLocked`) nudges the timer via
`select { case wakeTimer <- struct{}{}: default: }`. The `default` only guards a *full* channel - a send on a
*closed* channel still panics, even inside a `select`. The senders are not just worker goroutines (`processStep` and
its retry/sleep/recovery paths): the breaker subsystem (`refreshNextProbeLocked`, including from the inbound
`HandleTripBreaker` signal) also signals the timer, so there is no drain point after which a `wakeTimer` send is
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

Transactions don't help when a worker crashes during the `TaskExecutor` call (outside any transaction). The
`lease_expires` column is a crash-recovery lease: a worker reserving a step sets `lease_expires` to
`NOW + TimeBudget + leaseMargin`. If the worker crashes, the lease expires and `pollPendingSteps` resets the step to
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

### Per-Function Crash Analysis

- **Create / CreateTask** - insert flow (`created`) -> insert step (`created`) -> update flow's `step_id`. A crash
  after the step insert leaves `step_id=0` with a `created` step; the flow is inert until `Start`, and
  `pollPendingSteps` picks up the orphaned `pending` step. Self-healing.
- **Start / Resume** - one transaction (steps -> `pending`, flow -> `running`). A crash after commit but before the
  doorbell is recovered by `pollPendingSteps`. Self-healing.
- **Restart / RestartFrom** - one transaction (rewind the target step in place, delete the rewound subtree, flow ->
  `running`). Self-healing.
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

`WithNumShards` (default 1) distributes flows across databases to scale write throughput and reduce index contention.

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

**Dynamic expansion:** `WithNumShards` can increase at runtime - new shards are opened, migrated, and immediately
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
