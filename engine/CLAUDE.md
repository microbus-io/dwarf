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
to provide one. Three signal kinds are ruled out by measurement: a per-step work doorbell (volume O(steps),
and it buys no latency because every piston is already scanning at its cycle interval), a per-flow stop
broadcast (the await latch's detector reads the shared rows on a tighter cadence than a broadcast could
beat), and a fleet-membership nudge (peers re-read the registry every 250ms, which is faster than a
broadcast converges). The rule behind each: a signal may only ACCELERATE a convergence the database already
guarantees, and once the poll is faster than the message, it accelerates nothing. Do not add one without
showing it beats the poll it would replace.

The interface methods:

- **`LoadGraph(ctx, workflowURL string) (graph *workflow.Graph, err error)`** - fetches a workflow graph by its workflow URL.
  Called at `Create` (and on subgraph spawn); the graph JSON is then frozen on the flow row. The flow's opaque
  baggage rides on ctx (`workflow.BaggageFrom(ctx)`) for identity-dependent loading (authz, per-actor graphs).
- **`ExecuteTask(ctx, taskURL string, flow *workflow.Flow) error`** - executes one task. Receives the flow
  carrier with state pre-populated; writes its changes back onto the flow. The engine never knows *how* the task is
  reached (local call, RPC, message bus). Any error the task returns is terminal for that attempt: routed via the
  graph's `onError` transition if one exists, else the step fails. A **panic** in the in-process handler is caught at
  the call boundary and treated as such an error (see "Host-call panic isolation"). The engine never sniffs status
  codes or error text; a task backing off on a transient failure detects that itself and arms `flow.Retry`.

The observability providers, injected separately (none is a `Host` method):

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
  `otel.GetTracerProvider()`, and OTel's untouched global stand-in is swapped for a true no-op at Startup (see
  "Tracing"). The engine mints the root
  "workflow" span at `Create` and a per-step span in `processStep`, both under the
  `github.com/microbus-io/dwarf` scope (see "Tracing" below). The host injects only the provider - no
  span code.

The **baggage** is opaque to the engine: set once at `Create` via `FlowOptions.Baggage` (an `any`, like
`initialState`), stored on the flow row (the `baggage` column), and delivered on the dispatch **context** to every
`LoadGraph`/`ExecuteTask` call for the flow's lifetime - the host reads it with `workflow.BaggageFrom(ctx)` (which
lives in the `workflow` package so task code needn't import `engine`). Authored in one typed place, observed
ambiently; most callbacks ignore it. The engine never interprets it; a host carries actor claims / tenant identity
there, receiving it back as a `workflow.State` (raw JSON per field, read through the `State` getters), like
flow state.
`BaggageFrom` returns a `State` directly (a nil State when none was set); the set value (`FlowOptions.Baggage`)
stays an `any` a host fills with a struct or map. (Unlike W3C/OTEL request baggage this is *flow*-scoped and frozen
at `Create`, not per-request mutable.) See "Identity / baggage propagation".

### Backpressure is the task's or host's job, never the engine's

The engine deliberately holds **no** backpressure machinery - no rate valve, no circuit breaker, no failure
dispositions - and none of the three may be added. The reason is structural:
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

Integers beyond ±2^53 and NULs in strings are the root landmine's deliberate punt; `workflow/CLAUDE.md` holds
the rationale, the workarounds and the shape any future guard must take. On the engine side: no ingress
(`Create`/`Run`, `Resume`, `Fork`'s overrides, `Continue`'s `additionalState`) returns a 400 for either, and
`workflow.NewState` normalizes each input without range-checking it.

### Size and count limits are the host's job, not the engine's

The engine enforces **no** size or count bound as policy (the storage limit below is the one bound, and it is not policy) - not on initial state, `baggage`, the per-flow-frozen
graph JSON, interrupt/resume payloads, `forEach` fan-out width (one step row + full state snapshot per array
element), or subgraph nesting depth. This is the **same structural reason** as backpressure and the absent
time-budget ceiling: the sizing that matters is workload-defined and keyed on identity the engine cannot see.
A cap that fits a small control flow would reject a document-processing workflow that legitimately channels
tens of megabytes of state per flow, and picking one number for both is impossible - so the engine refuses to
own the number. The host holds the caller/tenant identity a quota turns on, so it enforces limits where that
context lives: reject an over-large `initialState`/`Baggage` before `Create`, cap a `forEach` source array in
an author-space entry task, bound retention (`Purge` marks at most `purgeCap`, 4096, roots per call, so a
retention job loops). For a **pass-through host** that adds no policy of its own, the obligation flows through to the
application using that host - it is not silently absorbed anywhere.

**The one bound the engine does enforce is what one column can store** (`maxPayloadBytes`, 256,000,000 bytes,
`payloadsize.go`). It is a storage fact, not a policy: a value over it would be refused by the database anyway,
only after the task ran, the bytes were sent and the write retries ran out. Every payload
write checks the marshalled bytes first - a task's changes (before any outcome write), each successor's and
fan-in's minted state, the interrupt payload, the final state, and at the door the initial state, baggage,
`Continue`'s state, resume data and Fork's leaf. The error is a `permanentError` with a 413, so persist does not
retry it and `failFlowTx`/`terminalFinalState` take an oversized final state as `{}`. **What it means for a flow:**
refs keep carried fields out of `state`, so mid-flow the binding column is a step's `changes`; but every flow
boundary flattens (`final_state`, `Continue`, subgraph in/out, Fork's leaf), so the whole state must fit when the
flow ends. A fold under a reducer other than `append` is stored inline, so it can still
grow a `state` past the limit mid-flow; the anchor cap inlines only anchors below the bar for opening one
(`internal/staterefs`). Pinned by `engine/payloadsize_test.go`. A new payload column write must call `checkPayloadSize`.

Subgraph nesting depth is the one axis that is *also* a crash vector if handled carelessly: `Fork` clones the
tree with an **explicit LIFO worklist** (`cloneTree` drives `cloneOneFlow` per flow), not recursion, so
arbitrarily deep nesting costs O(1) goroutine stack and only one flow's clone state is live at a time. **Do not
make the clone recursive:** a recursive walk holds every ancestor's clone state (including each flow's
`graph`/`baggage` JSON) on the stack at once and, at pathological depth, overflows the goroutine stack - fatal,
since a Go stack overflow is *not* recoverable by `errors.CatchPanic`, unlike a host-call panic. Deep nesting
costs only bounded storage (one flow row + step rows per level), which falls back under the host-quota rule above.

### Configuration (`Set*` methods)

Configuration is applied through `Set*` methods, each returning an `error` rather than chaining a `*Engine` (so
every setter can surface an error). They split into two groups by whether the knob can change safely on a running
engine:

- **Live** (take effect immediately, callable any time): `SetTimeBudget` and the bench override
  `setRefillInterval`, reached through `internal/engineseams`. The pool lever is
  construction-time, not live: `ShardSpec.MaxConnections` declares a shard's fleet-wide budget (see "Connection
  pool sizing"). There is deliberately no seam that pins a per-replica pool: such a pool is never divided and
  makes every replica dispatch every shard, so a test or bench using one exercises a fleet shape no deployment
  can reach. Pin through `MaxConnections` instead (identical to a per-replica pin at R=1).
  `SetTimeBudget` is read fresh at each `Create` (an existing flow keeps the budget frozen at its own
  `Create`). Which replicas dispatch a shard, and so how its
  derived budget divides, is NOT a setter - it is **read** live from the shared `dwarf_peers` registry (see "Peer
  discovery" and "Dispatchers" below), and `recomputePools` pushes each shard's recomputed size through
  `ShardSet.SetShardPool` (hot/atomic, and it applies the test-mode cap). **The derived *worker* ceiling follows the
  pools, and EVERY path that changes a pool must re-derive it** (`recomputeWorkerCeiling`): the ceiling encodes how
  fast a completion storm drains through `M` connections, so a shrunken pool must never leave a stale, too-high bound
  on exactly the storm the ceiling exists to contain. Startup derives it, and `recomputePools` (a fleet change) is
  the one live path. Pinned by `TestPoolSizing_CeilingFollowsLivePoolChange`.
  `shardRTTMs` (the Startup RTT probes the ceiling is derived from) is published and read **under `shardsLock`** -
  its reader is the reconcile loop (`recomputePools`, `recomputeWorkerCeiling`), while `Startup` reassigns it on a
  restart, and an unsynchronized map read/write is a *fatal throw*, not a recoverable panic.
- **Construction-time only** (return an error if called after `Startup`): `SetShard(ShardSpec)`, `SetWorkers`,
  `SetEngineID`, `SetStateCacheSize`, `SetHost`, `SetLogger`, `SetMeterProvider`, `SetTracerProvider` (plus the
  `SetDebugLogger` convenience). Applying
  these on a running engine would mean opening live connections and mutating a shard set that flow keys already
  encode (`SetShard` - see "Database Sharding"), resizing the worker Crew + candidate
  cache (`SetWorkers`), or re-resolving a frozen provider - so the setter **rejects** it with an explicit error rather
  than silently no-op'ing. The error is a 409, worded `"<what> cannot be changed after Startup"`.

For the observability providers specifically (`SetLogger`/`SetMeterProvider`/`SetTracerProvider`): the engine
resolves the logger/tracer/meter once at startup (the logger feeds the worker hot path and is read lock-free; the
meter registers an async gauge callback) and passes all three into `ShardSet.Open` (via `database.Config`), which
wires them into every shard's sequel DB. Hot-swapping a provider on a live engine is deliberately unsupported: a
half-hot version
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
pick - and there, indexing the empty index slice (`rand.IntN(0)`) **panicked the host's process**.
Pinned by `TestPoolSizing_NoOpenShardsIs503`.

**Policy is set once at genesis and inherited by derivation; the operation is the inherit-vs-default selector.**
A flow's policy (priority, fairness, time budget, baggage) is authored
explicitly via `FlowOptions` only at **genesis** - `Create`/`Run`. **Derived** operations carry policy from
their source rather than taking `FlowOptions`: `Continue` inherits the policy of the flow it continues, `Fork`
inherits the origin flow's, and subgraph children inherit the parent's. So there is no `opts` on
`Continue`/`Fork`; choosing one of those operations *is* choosing "inherit." The explicit-policy escape hatch for
a continuation is `Create` with the previous flow's final state (from `Snapshot`).

**Create** - Creates a flow **and runs it**. Calls `LoadGraph` to fetch the graph, **validates it**
(`graph.Validate()` after a nil check), then inserts the flow row (already `running`) and its entry-point
step (`pending`) in one transaction and rings the doorbell - returning a running flow's key. The graph JSON is
frozen at creation. There is no separate start call; `created` is never an externally-visible resting state.

**The frozen graph is permanent: do not add a way to move a live flow onto a new graph.** A flow's position is a
set of step rows naming tasks in the graph it was created with, plus cohort counters sized from that graph's
edges; under an edited graph a pending step can name a task that no longer exists, a cohort can await branches
the new graph never spawns, and there is no general rule for where such a flow resumes. Only `Create` and
subgraph spawn call `LoadGraph`; `Fork` and `Continue` copy the stored graph verbatim. Task implementations may
change freely behind stable names - that, and cancel-and-recreate via `Snapshot`, is the whole migration story,
and `docs/upgrading.md` states it as the position.

*Create-time validation is load-bearing, not just doc-hygiene.* A `LoadGraph` returning `(nil, nil)` is a
clean 404 here (it would otherwise nil-deref in `EntryPoint()`); a structurally invalid graph is a 400. In
particular `Validate()` rejects a graph whose fan-out branches don't converge on a single fan-in - the shape
the empty-`forEach` fan-in shortcut depends on - so a malformed fan-in can't reach dispatch, where it would
silently *complete the flow* instead of firing the fan-in (skipping every downstream task: silent data loss).
`Validate()` is **pure**: it computes the fan-out-to-fan-in map into a local to run that check and stores
nothing on the graph, so only the author's definition is frozen into the flow's graph JSON. The routing map
itself is an engine-side optimization derived at dispatch from the frozen definition
(`internal/faninmap.New`, built beside the parse in the shared graph layer), not persisted - it is a pure
`O(V+E)` function of the structure, computed once per graph per replica rather than per step. The **subgraph-spawn** path
validates identically (a nil/invalid child graph fails the caller step like any `LoadGraph` error). One
consequence for graph authors: a graph with no explicit transition to `END` (relying on "no matching
transition completes the flow") is rejected at `Create` - `Validate` requires an explicit `END` edge.
The engine has **no creation-time delay** (no `StartAt`):
every flow runs as soon as it is created. A flow that should wait runs author-side - an entry **gate** task that
calls `flow.Sleep(until)` for a one-shot durable delay, or an interrupt-first entry task + `Resume` (staged
start) - and recurring schedules are an external concern (a host/cron service that calls `Create`/`Run` on a
schedule), never the engine's.

**Snapshot** - Returns a `*workflow.FlowOutcome` for a flow at the current moment. For terminal statuses
(`completed`/`failed`/`terminated`/`cancelled`) it returns the flow's `final_state` (plus
`Error`/`TerminateReason`/`CancelReason`); for
`interrupted` it returns the interrupted step's merged `state+changes` and its `interrupt_payload`. When the
flow has several interrupted steps, Snapshot picks the **same one the next `Resume` will resolve** - the one
the flow row records (`stopped_step_id`), then the earliest-`updated_at` (`step_id` tiebreak), *not* by
`step_depth` - so a Snapshot reports exactly the interrupt the next Resume acts on, and the one
`StoppedTaskName` names. **The record must come first:** the interrupt that commits its flow-row write first is
recorded, and a sibling can write its step earlier (or in the same millisecond) yet commit later, so ordering by
`updated_at` alone reports a different interrupt than `StoppedTaskName`. Pinned by
`fixtures/interruptselectionflow_test.go`'s `TestInterruptSnapshotFollowsTheRecordedInterrupt`. For a `running` flow it returns the status with an **empty `State`** -
dwarf does not reconstruct live in-flight merged state (a fan-out included). Exposing a
live fan-out snapshot is a deliberately-deferred decision; confirm against product intent before implementing.

**Resume** - Continues a flow paused by `flow.Interrupt`. Walks up the surgraph chain (`surgraphChain`) and down the
interrupted subgraph chain (`interruptedSubgraphChain`) to the leaf interrupted step. Records resume data on the
leaf's `resume_data` column (the leaf already has `interrupt_done=1`, set when the task armed `flow.Interrupt`); on
re-dispatch the task's `flow.Interrupt` call returns that data with `yield=false`. Resume data is **not** merged into
`state`/`changes` - the task receives it as the return value. Re-parks intermediate surgraph steps, resets the leaf
to `pending`, transitions all flows in the chain to `running`. Propagation goes both directions. Must be addressed
by the **root** flow key (400 on a subgraph-child key). If
multiple fan-out siblings interrupt, each `Resume` handles one; the flow returns to `running` only when no
interrupted steps remain.

**Terminate** - Aborts a created, running, or interrupted flow, forcefully and unconditionally: in-flight work is
abandoned immediately, with no chance for any task to react. Walks **down** (`allSubgraphFlows`) the hierarchy,
atomically terminates all steps across all flows, computes `final_state` per flow, and sets every flow to
`terminated` with its own UPDATE, batched - all in one transaction (`terminateSubtree`), built so that neither a
step created mid-terminate nor a subgraph spawned since its first scan escapes (the rules are under the
orphaned-child sweep in "Background Recovery"). Must be addressed by the **root** flow key
(400 on a subgraph-child key). Takes a reason string surfaced
as `FlowOutcome.TerminateReason`.

*Down only - Terminate never walks up.* It is addressed only by the **root** flow key (a mid-tree key is
rejected), so it has no ancestors to walk: `surgraphChain`, the up-walk
helper, has no role here and would find nothing above a root. Every termination the engine performs is
down-only or one-level: a root-addressed Terminate has no ancestors; the orphan sweep starts at a child whose
ancestor chain is already terminal; and a child terminalizing settles its *one* caller step by
`surgraph_step_id` (a PK), not by walking to the root. (`surgraphChain`'s users are `Resume`,
`handleInterrupt` and `Fork`.)

**`terminateSubtree` is shared with the orphan sweep** (`terminateOrphanedSubtree`), and both treat a tree
that is already terminal the same way: a no-op returning nil, as `Cancel` and a repeated `Delete` are. A
zero-row flow UPDATE (a racing terminalization) **rolls the transaction back** rather than committing: the step
writes would otherwise flip a straggler on a frozen tree, and the post-commit loop would count every flow in
it as ended a second time.

**Cancel** - The graceful counterpart of Terminate: a cancellation is delivered as an error on each covered
step's ordinary `onError` transition (detectable with `workflow.IsCancelled`), so an author can catch it and
recover. Root-only (400 on a subgraph-child key), 404 on an unknown key, a no-op on a terminal flow.

*The mechanism is a per-step mark, consumed lazily.* `cancel()` writes the tree's `cancel_reason` and
`cancel_watermark`, then sets `dwarf_steps.cancelling=1` on every `pending`/`running`/`interrupted` step of the tree,
and does nothing else. Each marked step consumes its own mark at one of exactly two checkpoints, both reading the
column off a write they make anyway, so an uncancelled flow pays no extra round trip - **except the completion
write on MySQL**, which lacks `RETURNING`/`OUTPUT` and reads the mark with a follow-up `SELECT`: +1 round trip per
completed step (+1 to `k` in the ceiling equation), accepted like the claim's own follow-up read (the site comment
says why not `LAST_INSERT_ID(expr)`). The successor's mark adds no round trip: each successor row reads the
watermark in its own INSERT, one primary-key lookup on the flow row the transaction already holds.

- **The claim.** The claim CAS returns `cancelling`; a marked step is preempted - `ExecuteTask` is skipped and
  `cancelmarker.New(reason)` takes the path a task error would (`onError`, else `failStep`). Retry-rewound,
  sleeping and resumed-interrupt steps all arrive here, which is why none of them needs a check of its own.
- **The completion write**, for a step that was already running when the mark landed. It reads `cancelling`
  back (`RETURNING`/`OUTPUT`, or a follow-up `SELECT` on MySQL - race-free because the row is already terminal
  and Cancel never marks a terminal row). A covered step's changes stand - it genuinely ran - but its next
  transition is not honored: with `onError` it routes to the handler with `onErr` added to its changes; without,
  `failStep` flips it `completed -> cancelled` and escalates exactly as a failure does.

*The one step the mark cannot reach is a step between its completion write and its transition* - terminal, so
never marked, with its successors not inserted yet. Left alone, a `Cancel` landing there is lost outright: the
successors are inserted unmarked and a linear flow runs to `completed` although `Cancel` returned nil. So every
successor is inserted marked when its source step's `step_id` is at or below the flow's `cancel_watermark` - the
flow's highest `step_id` when `Cancel` ran - and the source was not itself covered. Both halves of that condition
are load-bearing. The watermark is what exempts a handler that caught the cancellation and everything after it -
they were inserted after the `Cancel`, so their ids sit above it - and so a flow can still recover; the "not
covered" exemption covers the handler's own insert, whose source existed before. Among steps that existed at the
`Cancel`, "at or below the watermark and not covered" is exactly "terminal when the mark ran", which is the gap.
It is race-free and free of round trips because every insert of a flow's steps holds that flow row's lock and
`cancel()` writes the watermark under it: either the insert sees the new watermark, or `cancel()` waits and its
mark then finds the successors pending. Every successor row decides its own mark: its `cancelling` value is a scalar
subquery on the flow's `cancel_watermark` (`successorRowSQL`), evaluated by the INSERT itself, after the
transaction already holds the flow-row lock - so a transition's inserts can be batched with the lock grab without
waiting for anything the grab returns.
`cancel_watermark` defaults to 0 (never cancelled), and **Fork deliberately does not copy it**: a fork is not
under cancellation. Pinned by `engine/cancelgap_test.go` and by `ran_covered_with_onerror`'s handler successor.

**Do not order this by timestamp**, and **do not fold the watermark read into the lock-grab statement**: a
millisecond clock ties a fast source with the `Cancel` (~1 in 4 runs lost it), and a Postgres `UPDATE`'s subquery
reads the pre-lock snapshot. **Nor compute the watermark in a subquery of the UPDATE that writes it**: on MySQL a
subquery inside DML share-locks the steps it reads after the flow rows are locked, the reverse of Terminate's
order, and deadlocks against it. `cancel` reads the watermarks with a plain SELECT (a consistent read, no locks)
and binds them (pinned on MySQL and Postgres by `TestCancelWatermark_TakesNoStepLocks`). The site comments
(`successors.go`, `cancel` in `operations.go`) hold the detail.

*An error the task itself returns wins over the cancellation.* A covered step that returns a real error takes
the ordinary error path; with `onError` its handler receives that error, is inserted unmarked (its source was
covered) and the flow carries on under the author's control. Preempting the handler instead would hide the real
error.

*A subgraph, retry or interrupt request made by a covered step is honored, not intercepted.* The mark stays on
the row, so the next claim catches it: a rewound retry is preempted a backoff later, an interrupted step is
preempted when resumed. Do not intercept each request kind at its own write (a CASE on the park, on the rewind,
a rollback on the interrupt chain): built, it multiplied dialect branches at four sites, and its conditional
writes are the class that breaks the MySQL fence (see "Lease fencing"). The cost of honoring is
latency - a rare `Cancel` landing mid-task on a step that then arms a subgraph lets that child run to completion
before its caller is redirected - and a `Terminate` covers the rare long child. (Not a second `Cancel`: a
flow is cancelled once, and a repeat call only re-applies the first call's watermark-bounded mark - see "one
cancel per flow" on `cancel`.)

*Two rules at the claim are load-bearing:*

- **A subgraph caller resuming after its child (`subgraph_done=1`) is NOT preempted.** It runs once more so the
  child's result reaches its code, and its own mark is applied at its completion write. Preempting it would
  deliver the cancellation without the caller ever seeing what its child did.
- **An interrupted step resumed after Cancel IS preempted.** Its resume data is new input arriving after the
  cancellation; running on it would keep a cancelled flow acting on a human response.

*Parked callers are marked like any other running step - a caller's cancellation is its own, not its child's to
decide.* Excluding them loses the cancellation outright in the window after the child completes but before the
caller is revived (`completeFlow` and `completeSurgraphFlow` are separate transactions): the caller is skipped and
the child is terminal, so nothing is marked and the flow runs on as if never cancelled.

*The reason is written before the mark*, so a step that sees the mark finds the reason. A step already running
when Cancel landed read its flow row at claim time, before the reason existed, so the completion-write path
re-reads `cancel_reason` - one extra round trip, on that rare path only.

*The flow resolves `cancelled` only if EVERY unrecovered loss was a cancellation; one real error makes it
`failed`.* A real error must always win visibility over an incidental cancellation, and the terminal status is
immutable, so the decision is made per loss, never from "was Cancel called on this flow". `cohort_cancellations`
counts, beside `cohort_failures`, the losses that were cancellations; `propagateCohortFailure` carries a
`cancelled` flag up through nested cohorts, and a cohort that resolves passes on a cancellation only if
`cohort_cancellations == cohort_failures` - so a real failure anywhere below makes every enclosing resolution a
failure. A trunk step decides from its own error (`workflow.IsCancelled`). **A `cancelled` step counts as a
loss everywhere a `failed` one does** (`cohort_failures`, Fork's branch recompute), and Fork rebuilds
`cohort_cancellations` by the same rule the counters apply: a branch is a cancellation if it holds cancelled
steps and no failed or terminated one. Get that wrong and a fork resolves `cancelled`, or converges, where the original failed.

*`flowLossError` picks the error a resolving flow delivers to its surgraph caller.* It runs inside
`failFlowTx`, the one in-transaction flow-fail that `failStep` and the cohort-fail branch in `processStep` both
call (with `endLostFlow` as the shared post-commit tail), so the two paths cannot disagree on a lost flow. A cancelled flow delivers the
cancellation; a failed flow delivers a REAL failure even when the loss that resolved it was a cancellation (it
samples a `failed` step's error) - a cancellation marker on a failed flow would have `IsCancelled` report true
for it. The flow's own `error` column gets that failure's text for a failed flow and nothing for a cancelled
one (`flowErrorText`), whose explanation is its `cancel_reason` - as a terminated flow's is - so `List` and
`Snapshot` agree that a cancelled flow carries no error.

*`subgraph_error` holds the delivered error's `TracedError` JSON, stack stripped, not its message*, and
`flow.Subgraph` decodes it. That is what carries the cancellation marker across the boundary - a cancelled child
reaches its caller as an error `IsCancelled` recognizes, so a caller that returns it resolves `cancelled` - and
it keeps a real child error's status code and properties, which a bare message lost. The column is a JSON
column, `{}` for no error (`subgraphErrorText` maps it to none at the claim); its writer, inside the child's
terminating transaction, keeps only the message and status code when the properties would make the JSON
unstorable (over the payload limit, or holding a NUL a JSONB column refuses), since a failure write that cannot
land is read by the classifier as the database being down.

**Never pass a stored or operator-supplied string as `errors.New`'s pattern.** It counts every `%` as a format
verb consuming a trailing argument, so `errors.New(msg, errors.StatusCode)`-style calls silently lose the status
code or a property when `msg` contains `%`: `cancelmarker.New("50% off")` yielded `flow cancelled: 50%!o(...)` and
lost its marker. Pass the text as an argument, `errors.New("%s", msg, ...)`.

Pinned by `fixtures/gracefulcancelflow_test.go`, which the SQLite suite alone cannot vouch for - run it against
a MySQL-family server as well.

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
fails the fork with no special escalation). *Branches, not lineage members:* `lineage_id` is a
cohort-**counting** device, not a DAG - every step of a per-element sub-pipeline inherits the spawn's lineage, so a
3-branch cohort of 2-step branches has 6 members, and counting members overshoots `cohort_size`. The walk starts
at each direct child of the spawn that shares its lineage and descends the branch's sub-DAG; one arrival per
surviving branch, one failure if any step in it failed, was cancelled or was terminated. The whole rewound *branch* is excluded, not just the
rewind *step* (a mid-branch rewind keeps its earlier completed steps, and they are not arrivals). A source with a
direct edge into its own fan-in also arrives on itself as it spawns; no branch carries that arrival, so the
recount adds `cohort_size` minus the heads, or the clone never converges.

*Membership is the lineage CHAIN, not the lineage id.* A **nested** fan-out re-lineages its own children
(`childLineageID = stepID`), so in `Seed -forEach-> Cell -forEach-> Chunk -> JoinChunk -> JoinCell` the `Chunk`
steps carry `lineage_id = Cell` while `JoinChunk` carries `Seed` and is reachable only **through** the `Chunk`s.
Filtering the descent on `lineage_id == spawn` dead-ends the outer walk at the inner spawn: a rewind past the inner
frame goes unseen (arrivals overshoot) and a failure inside a **kept** branch's inner cohort goes unseen (a fork
that must re-fail COMPLETES instead). So a step is in spawn `S`'s cohort, at any depth, iff its lineage chain
upward reaches `S`. That costs no query - Fork holds every step in memory, and a kept step's lineage ancestors
are always kept - so do not materialize a `lineage_path` column (unbounded length, reindexed on every insert).
Pinned by `fixtures/forkcohort_test.go`; single-step, non-nested branches (members == branches) cannot show
either trap.

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
surgraph chain is **re-parked** (`running`/`parkedSubgraph`, `subgraph_done=0`, `changes='{}'`); when the re-run
child re-completes, `completeSurgraphFlow` revives the caller and execution bubbles back to the root. **Clear the
caller's `changes`**: the revived run re-executes the task from the top with `changes` as its prior changes, so
keeping the origin's would fold every post-`Subgraph` contribution twice. The fork's caller is therefore one run,
not the origin's runs plus one (pinned by `TestForkflow_InsideSubgraphCallerFoldsOnce`). Chain flows are set `running`;
off-path prefix subgraphs keep their `completed` status.

*Scheduling and identity.* Scheduling (priority/fairness/time-budget) is inherited from the **root's
frozen values**, resolved once and applied **uniformly to every cloned flow and step**, because the original
tree is uniform (subgraph children inherit the parent's scheduling) and a deep-subgraph fork's re-running leaf
lives in a descendant, so it must carry the same values. The new root mints a fresh detached trace (like
`Continue`), sets `forked_from_step` to the *original* fork step's id (provenance), and does **not** notify or
auto-delete. A failed-fan-out
fork-of-fork is the partial-recovery path: fork one failed branch at a time; the first fork re-fails cleanly via
cohort accounting (no limbo) until every failed branch is fixed. **A branch lost to a graceful `Cancel` is kept
exactly like a failed one** - Fork has one rewind point, so it re-runs the chosen step and inherits every sibling's
recorded loss - and a fork whose only remaining losses are cancelled branches resolves `cancelled`. **A branch a
`Terminate` stopped is a loss too, never an arrival**: nothing in the fork runs it again, and counted as an
arrival the fan-in converges on what the branch finished and silently lacks the rest, at any depth - an inner
cohort the branch had started has no fan-in row to carry its work. A fork that fails on one names it
(`flowLossError`'s fallback, since no step failed); re-running it means forking at or before its fan-out.
Pinned by `TestForkCohort_TerminatedBranchIsALoss`. Each cloned
flow therefore copies its origin's `cancel_reason`, so such a fork reports the reason for the cancellation it
inherited rather than a reasonless one - but NOT its `cancel_watermark` (see **Cancel**). Re-running every lost
branch in one fork would need multiple rewind points
in the cohort recount above; it is deliberately not offered.

*Caveat:* the cloned-prefix steps keep the **origin's** timestamps (the `INSERT…SELECT` copies `created_at`/
`started_at`/`updated_at` verbatim); only the **rewound** rows (the leaf fork step and re-parked ancestor
callers) are stamped fork-time (`NOW_UTC()`). So a cloned prefix's timings match the original, and only the
re-run boundary reads as new. Every cloned step still carries the CLONE's creation hour (`flow_created_hour`), so for
the prefix that hour and `created_at` disagree: the hour is the flow's, never the row's.

**History / Step** - `History` returns the step-by-step execution as `[]workflow.FlowStep`; each includes key, depth,
task name, status, error, timestamps, and DAG links - but **not** `state`/`changes` (its two-round-trip tree scan
deliberately omits the payload columns, so `FlowStep.State`/`.Changes` are nil on a History result). Subgraph-executing
steps have `Subgraph=true` with nested `SubHistory` and the child's `SubgraphKey` (the latest child after a
retry, the same one `SubHistory` shows), so a caller can `Snapshot` what the child returned. `Step` returns one step by key **with** its `state`/`changes`
(refs resolved) - it is the payload-bearing reader; use it when you need a step's actual data.

**List** - Queries flows by status, workflow URL and the other `Query` filters, with cursor pagination (newest first **per shard**
- see "`List` uses per-shard pagination" under Database Sharding; single-shard, the default, is globally
newest-first - default 100).
Returns a `Subgraph` bool in each `workflow.FlowSummary`. By default it returns **root flows only**;
`Query.IncludeSubgraphs` adds subgraph children to the results. Combined with `WorkflowURL` (a graph that runs only
as a subgraph has no root flows under that URL) this locates every run of a graph that executed as a subgraph.
`Purge` **rejects** the flag with a 400 (`purge cannot include subgraphs`) rather than silently ignoring it, and
always targets roots only (deleting a subgraph child directly would strand its parent's surgraph step).

**Continue** - `Continue(flowKey, additionalState)` creates and runs a new flow from the completed root flow
`flowKey` names, its `final_state` merged with `additionalState` through the graph's reducers - sugar over
`Create` for the multi-turn case. 404 on an unknown key, 400 on a subgraph-child key, 409 on a flow that is not
`completed` or is scheduled for deletion (as for `Fork`: the caller names a flow that is logically gone). The new
flow runs the same stored graph (its bytes and `graph_hash` copied, never reloaded) and is returned **`running`**
(like `Create`). The prior flow's `final_state` passes through unfiltered as the new flow's initial state; a
workflow author wanting narrower carryover scrubs with an entry adapter task using `flow.Del`. As a **derived**
operation `Continue` takes no `FlowOptions`: it **inherits the source's policy** (priority/fairness/budget/
baggage); a caller wanting different policy uses `Create` with the source's final state (from `Snapshot`).

*The engine keeps no chain.* Continue builds on exactly the flow it is given, so the caller holds the latest key,
and continuing one flow twice gives two successors (`TestContinue_BuildsOnTheNamedFlow`). **Do not add a chain
record** - a grouping column, a search for the latest turn, a threads table: the engine cannot know when a
chain is finished, so such a record can never be trimmed, and a search for the latest turn needs a lock on the
chain to keep two concurrent Continues from branching it. A Continue retried after a lost response makes a second
flow, as a retried `Create` does; neither is deduplicated.

*The source is read with no lock and outside the insert's transaction* (`continueFlow`): a completed flow is
immutable, and the new flow copies its state, graph and policy rather than referencing it, so a deletion racing
the read leaves the copy valid. The insert is `insertFlowTx` in a bare transaction, the one copy of the
flow+entry-step INSERTs it shares with `Create` and subgraph spawn.

*`additionalState` is marshalled at the door* (`workflow.NewState`), per the root landmine: a caller's struct
re-contributing an element the source already holds would otherwise `union` into a second copy, since
`DeepEqual` of a struct and its decoded `map[string]any` twin is false (pinned by
`fixtures/continuecanonicalflow_test.go`). A `NewState` fast path for a raw `map[string]any` would reopen it.

**Run** - Create + Await in one call, returning `(flowKey string, *workflow.FlowOutcome, error)` -
the new flow's key alongside its outcome (the key is the flow's identity, not part of the outcome; callers
need it for later `History`/`Resume`/`Fork`). Error semantics are phase-split: a **create** failure
returns `flowKey == ""` with a nil outcome (no flow exists); an **await** failure (usually the caller's
ctx expiring first) **leaves the flow running** and returns its **`flowKey`** with a nil outcome and the
error, so the caller retains a handle. `Run` never terminates the flow on the caller's behalf - tearing down
a healthy durable flow just because the caller stopped waiting is an availability footgun; a caller that
wants teardown-on-timeout calls `Terminate` itself. (Terminating from inside `Run` would also be issued on the
already-expired await ctx, so the termination would silently never run.)

**Await** - Blocks until the flow stops (see "Await" below).

**Poll** - Await bounded by the caller's ctx deadline: a deadline expiring is not an error, it returns the
current non-terminal outcome (`Stopped()` false) so the caller can answer within its budget and re-poll.

**Delete / Purge** - Operator-driven retention (see "Data Retention").

**ShardInfo** - Per-shard health: a trivial query's latency and error. It reads no table, so it stays safe to call
in an incident on a database of any size. Do not add row counts back: a `COUNT(*)` is a full scan on Postgres that
holds a snapshot for its whole run, feeding the vacuum slowdown below, on the call the runbook makes first.

**There is no inbound peer entry point** - see "THE ENGINE SENDS NOTHING TO ITS PEERS" at the top of this file.

### Subgraph keys are read-only

A subgraph child flow has a real flowKey (a task inside it reads its own via `flow.FlowKey()`, and `List` with
`IncludeSubgraphs` surfaces it), but that key is a **read** handle, not a write unit: a child cannot be mutated
independently because its parent is parked waiting on it, and the unit for any lifecycle change is the whole tree.
So the **lifecycle mutations reject a subgraph-child key with 400** (`surgraph_flow_id != 0`): `Resume`, `Terminate`,
`Cancel`, `Delete` and `Continue`. The rejection is folded into
each operation's existing flow-row SELECT (no extra round-trip;
the 404-not-found check still takes precedence). The caller addresses the tree by the **root** key instead - which
it always holds (it came from `Create`/`Run`/`Continue`, or from `List` of roots). The rationale per op: `Resume`/
`Terminate` are inherently tree-wide (they walk up to the root and down), so a child key is just a confusing alias for
the root (and `Cancel` likewise covers the whole tree); `Delete` cascades *down* only, so deleting a child directly would strand the parent's surgraph step; and
`Continue` of a child would spin up a detached top-level flow from the subgraph's final state. **Reject, not
silently widen** - widening
`Delete(childKey)` into a whole-tree delete is a
surprising blast radius, so the engine makes the caller name the root.

History hands a child key out on its caller step (`FlowStep.SubgraphKey`, read from the tree scan History
already makes). That grants a root-key holder nothing new: `History` already returns every child step's key, and
`Step` reads any of them. What a subgraph-child key *is* good for: **introspection** - `Snapshot`, `Fingerprint`, `History`,
`Step` all operate on that child's own subtree - and **`Fork`**, which intentionally accepts any recorded step,
including one deep inside a subgraph. `fixtures/subflowguardflow_test.go` pins both halves (the `Resume`/`Terminate`/`Delete`/`Continue`
400 rejections, introspection + `List`-by-subgraph-URL still working); `fixtures/gracefulcancelflow_test.go` pins
`Cancel`'s.

### FlowOutcome and side-channel signals

`Snapshot`, `Await`, and `Run` return a `*workflow.FlowOutcome` (`Run` also returns the `flowKey` separately;
`Snapshot`/`Await` callers already hold it). The shape:

```go
type FlowOutcome struct {
    Status           string
    State            workflow.State
    Error            string         // populated when Status == "failed"
    InterruptPayload workflow.State // populated when Status == "interrupted"
    TerminateReason  string         // populated when Status == "terminated"
    CancelReason     string         // populated when Status == "cancelled"
}
```

The flow key is delivered separately, not on the outcome: the caller passed it to `Snapshot`/`Await`, or `Run`
returns it.

Side-channel fields are populated only for the matching status. `Run`'s Go-level `error` return is reserved for
request and infrastructure failures (a bad request or unknown workflow, the database, a timeout); a *workflow
failure* surfaces as `Status == "failed"` with `Error` set, so
callers don't disambiguate "the workflow rejected my input" from "the engine is down."

The interrupt path is split from `State`: `Snapshot` of an `interrupted` flow returns `State` as the merged step
snapshot *at the time of the interrupt* and `InterruptPayload` as the raw `flow.Interrupt(payload)` argument. Folding
the payload into `State` would be lossy (the caller could not tell workflow state from the resume request). A caller that
wants the two combined applies the payload onto the state itself, field by field - a resume request is not fan-in
data, so it does not go through the graph's reducers.

### Flow-stop notification is not an engine concern

The engine has **no** stop-notification mechanism - no callback, no option, no column - and none may be added. A caller that wants to learn a flow's outcome either
**`Await`s** it (blocking, bridges the workflow clock to a synchronous caller) or **composes** the
notification into the workflow itself - an orchestrating graph whose final task reports the outcome to the
upstream (with `flow.Retry` for durable delivery). This keeps notification policy and transport entirely in
the host/author, matching the engine's "carry facts, not policy" posture (baggage).

### Execution Model

The engine uses a **queue-as-cache execution model** with a configurable worker Crew (`SetWorkers`) and **one
piston per shard** (`internal/piston`; never one merged refiller fanning out through `OnEach`: that is a barrier
whose max-over-shards wait measured 2.02x by 6 shards, on the path that is the engine's throughput ceiling).
The in-memory `candidates.Cache` is bounded, **partitioned by shard** (each
refiller wholesale-replaces its own partition; workers pop from the lowest-floor partition - see
`internal/candidates/CLAUDE.md`), and holds *hints*, not ownership. Each worker pops a candidate and calls
`processStep`:

1. Reserve the step (atomic CAS `UPDATE ... WHERE step_id=? AND status='pending' AND parked=parkedNone AND
   not_before<=NOW AND lease_expires<=NOW`).
2. Check for terminal flow status (abort if terminated/cancelled/failed/completed).
3. Load the flow's graph, config, and baggage.
4. Execute the task via the host's `ExecuteTask` with a time budget on the call context.
5. Persist changes, evaluate transitions, create next steps (in a transaction), ring the doorbell.

Acquisition is the atomic CAS, so a stale or duplicated candidate is harmless: the CAS loser gets zero rows and pops
the next. The cache holds hints, never ownership; only the CAS grants a step. **The CAS predicate includes
`parked=parkedNone`**: a step that was offered to the cache and then parked (waiting on a subgraph) is rejected at
claim time rather than dispatched, so a parked step in a stale cache entry never runs.

**Selection (two-level priority + fairness), in three phases, one PISTON per shard.** The pistons, not
the workers, decide *what* runs. Each shard's piston cycles against its own database on its own clock,
with **no barrier anywhere** between shards (a merged pass returns when the slowest shard does - the
order-statistics tax quoted above).
Each piston runs **two independently paced loops** (`internal/pipeline`), meeting only at the planner: a
**Tallier** (`sleep -> tallying`) and a **Supplier** (`sleep -> planning -> fetching -> pushing`). They are
separate because the band scan costs O(due rows at the band) and therefore grows with the backlog, so
fusing them lets the scan set the rate at which candidates reach the workers - supplying least exactly
when the backlog is deepest (measured fused: the scan was ~90% of a 634ms cycle at ~500k pending, capping supply
at 353 steps/s against ~3,340 for a Supplier alone at 67ms). What goes stale instead is the fairness tally,
which is the cheap thing to be stale about; strict *priority* is the real concession, since a peer-created
better band is seen up to one Tallier iteration late. The full accounting, and the two traps the split
forced (`Plan.Tallied`, and the rule that both refill queries run the identical residue predicate), are in
`internal/pipeline/CLAUDE.md` and
`internal/piston/CLAUDE.md`. (1) **Aggregate
scan** (`piston.ScanBand`): the shard returns *one row per fairness key per due band* - one plain `GROUP BY`:
the key's due **count, CAPPED at cache capacity** (see "The phase-1 scan count is CAPPED, not exact" below),
the age of its *oldest* due step, and the *lowest* `fairness_weight` among them. It is two statements whatever the
number of bands: per-band counts capped at capacity for every band `1..max` (`SetMaxPriority`), then the
`GROUP BY` for just the bands up to the one where those counts reach capacity, since no fill reaches past it.
(2) **Global merge + weighted fill** (`internal/planner`): the shard's tally is recorded in the planner - a
shared map holding each shard's LAST report, which is what makes a barrier unnecessary - and the plan is
computed over all of them, band by band from the best: counts summed per key within a band, the lowest
weight winning, then repeatedly weighted-random pick a key (Efraimidis-Spirakis over
the *keys*, not the rows) until the band is exhausted or the plan reaches `capacity`, and spill to the next
band only while capacity remains. **The plan is global; only the fetch is per-shard**, so fairness semantics are exactly the
barrier's - never re-derive this as per-shard lotteries, presence-scaled (`1/d`) weights, or capacity
allocated by shard counts (count-proportional capacity makes backlog drive share, repealing the "count
does not drive share" property that makes the design fair). Each piston rolls its own plan from its own
snapshot - independent rolls change nothing in expectation and need no coordination; the staleness is that
a piston sees the *other* shards up to one of their cycles old, which fairness (slowly-varying) tolerates
and dispatch (own-shard scan, always fresh) never sees. (3) **Sliced fetch** (`planner`'s slice rule +
`piston.FetchSteps`, one statement over every planned band): the plan's per-key demand is split across the shards holding the key - **first slot
to the shard with the key's oldest step** (preserves globally-oldest-first dispatch and prevents a purely
proportional split from rounding a quiet shard's lone old step to zero forever), remainder proportional to
per-shard counts, largest-remainder rounding, deterministic - and the piston fetches only its own slice
(at most its slice's max per-key demand, oldest first per key), replaying the plan's interleave for its
occurrences. Intra-key ordering across shards is approximate below the head.

**A cycle is UNCONDITIONAL, and that is what makes an entire class of liveness machinery unnecessary.** There
is no trigger, no nudge, no wake channel and no timer goroutine: a piston scans on its period whether or not
anything happened, so the scan after a completion always sees the freed slot, a sleeping or backed-off step
becomes visible on its own (`not_before<=NOW_UTC()` is in every scan predicate), and a failed cycle's retry
*is* the next cycle. A triggered refiller would need machinery to decide *when* to scan - single-slot triggers,
a census watch on a better band, a starvation self-re-arm - because a parked refiller is woken only by
shard-LOCAL activity, so a shard holding work nothing local announced would sleep. Do not add, each being
strictly dominated by the cycle:

- **a trigger to shave latency** - it shaves at most one interval, and the doorbell covers the case that
  matters. Request-driven refill would also need a subtle rule (request *after* `processStep` returns, never at
  pop time, or the refiller re-selects the in-flight step and wedges a single-worker replica);
- **an early-wake channel or a `MIN(not_before)`/`MIN(lease_expires)` deadline** - verified unnecessary by
  no-oping the wake with the full suite green, and a wake needs its own past-deadline-replace predicate
  (without one `sleepretrycomposeflow` wedged ~1 run in 40). The trade is that `flow.Sleep` and retry backoffs
  land within a cycle of their deadline (up to the 1s `refillIntervalCap`), acceptable for a durable sleep;
- **a due-backlog existence probe** for a missed doorbell - the next cycle covers it, per shard, with one scan;
- **an error clamp** - swallowing a scan error refills the partition **empty** and blocks the shard's workers.

**A shard that could not LOOK must clear itself, and this is the one thing a cycle must never skip.**
A failed scan leaves the planner holding that shard's last tally, which may claim a better band deep enough to
fill the batch; every peer then finds no capacity left to spill into and dispatches nothing - forever. So a scan error calls
`planner.Clear` and leaves the cache partition intact (an error means "unknown", not "nothing is due"; the
Supplier reads the distinction off `Plan.Tallied`). A fetch error clears neither: the tally is still true.
Participation is **declared, never inferred** - do not add a TTL scaled to the slowest pass; a merely SLOW shard
keeps its tally and stays counted (`internal/planner/CLAUDE.md`).

**Strict priority is over CAPACITY: a worse band gets exactly what the better bands leave unused.** The fill
takes the best band first and spills to the next only while slots remain, so a better band that fills the batch
holds every worse band back, and one that cannot fill it holds back nothing. **Do not draw the plan from the best
band alone.** It looks like the strict rule and is not work-conserving: a sparse but continuous better band - due
at nearly every scan, never enough to fill the cache - idles the rest of the batch every cycle while the worse
band never moves, with the workers idle (`fixtures/bandspillflow_test.go` pins the shape). The plan's `Reach` is the band where the fill ran out of
capacity; it is the cache's bar for the doorbell, and the dispatch-lag gauge's bar for what is held back.

A shard holding only work past the reach plans an **empty slice** and fetches nothing; its partition is
cleared, because those hints are for a band the plan does not reach. It is simply a cycle that fetched
nothing, and the next one re-evaluates on its own - no outcome of its own, no watch to avoid spinning. The same
is true of a shard that wins zero slots because a capacity-bound plan gave them all to shards holding more (or
older) of the planned keys. `planner.Plan` deliberately does not say WHICH of those happened - every case takes
the same action, hold no candidates.

`created_at` (read as an age, comparable across shards) does two things per
key: gives the key its age (its oldest due step's) and orders dispatch oldest-first within the key. (The key's
`fairness_weight` is its lowest due step's, so a tenant cannot self-promote by queueing heavier tasks.) It is the only ordering signal comparable across
shards: `step_id` is a per-shard auto-increment, so a `(shard, step_id)` order would let a brand-new task on a low
shard jump an old task on a high shard for the *same* tenant (unbounded intra-tenant starvation). The age is
`DATE_DIFF_MILLIS(NOW_UTC(), created_at)` per shard, and `created_at` defaults to that shard's `NOW_UTC()` at insert
- both terms on one shard clock, so per-shard clock offset *cancels exactly*; no inter-shard clock-skew term in
`ageMs`. The only residual is the dispersion in *when* each shard runs its age query (the per-shard refillers scan
on independent cadences, so one shard's tally ages can lag another's by up to one cycle), a soft, self-correcting
reordering of one tenant's own queue - not a fairness violation (the weighted *key* pick governs cross-tenant
fairness) and not a correctness issue (the CAS arbitrates).
Same-age ties break by `(shard, step_id)` for determinism. The pick is re-rolled per step so expected dispatch share
is proportional to weight and independent of backlog depth or shard layout. Strict priority means no aging: a
higher-priority band that keeps the batch full starves lower bands by design.

**The wire/heap cost scales with fairness-key CARDINALITY, not with the backlog, and this is load-bearing.** The
three-phase split exists to keep that promise, and the two simpler shapes it rules out are both traps:

- **Do not return every due row and pick in Go.** Each row is allocated as a Go struct only to be discarded down to
  `capacity` by the pick, so cost grows with the **backlog** - under a deep one, the case the refiller exists for,
  it re-reads hundreds of thousands of rows on every pass.
- **Do not stop at a per-key `ROW_NUMBER() ... rn <= capacity` cut returning rows.** It bounds rows *per key* but
  not the **cardinality** term: with thousands of tenants at the band it returns up to `capacity * keys` rows (e.g.
  768 x thousands) to pick only `capacity`. And a plain global `LIMIT n` is not an option at all - it lets one
  tenant's old backlog fill the window and starve every other key (a fairness bug traded for the waste).

So phase 1 collapses each key to *one* aggregate row (count, oldest age, lowest weight) server-side, so the scan returns
O(distinct keys) rows; phase 2 picks the per-key demand from those aggregates; phase 3 fetches only the selected
steps. Total rows crossing the wire are bounded by `capacity^2` (at most `capacity` distinct keys chosen, each
fetched at the uniform per-key cap `<= capacity`) - **independent of key cardinality**. At high cardinality the
per-key cap is ~1, so the fetch is ~`capacity`.

The uniform per-key fetch cap (phase 3's `perKey`, not each key's exact demand) is a deliberate simplicity choice:
an exact per-key cap would need a per-key limit carried into the join for each key, to shave an over-fetch that only
appears under extreme weight skew among many keys - the low-cardinality regime that never had a scaling problem. The
uniform cap stays complete for every key (a key's globally-oldest N steps sit at most N on any one shard, so a
per-shard cap of N captures them all), and the plan's slice rule then assigns each shard its share. Pinned by
`TestRefillScan_BoundedPerFairnessKey` (one aggregate row per key, the per-key fetch cut, oldest-first).

**The phase-1 scan count is CAPPED, not exact.** The three-phase split bounds the *rows crossing the wire* to
`capacity^2`; the count bounds what phase 1 reports. **Count with the plain `GROUP BY`, never a window.**
`COUNT(*) OVER (PARTITION BY fairness_key)` adds a second full window pass over every due row of a key, and a
single-key flood - a `forEach` fan-out's N branches all inherit the flow's `fairness_key`, so a 3M-way fan-out is
*one* key with 3M due steps - pays it on **every refiller pass** (measured on Postgres at 3M: ~15-23s per pass).
A `ROW_NUMBER()` ranking is ruled out too: it needs the band's rows in key order, so an under-estimated deep band
can be read off the due index and then sorted whole (PostgreSQL 18 did, spilling to disk, with the band a
subquery value sized from the average band). Nothing in a `GROUP BY` needs an order.

The tally `Count` is `min(count, capacity)` - the `GROUP BY`'s `COUNT(*)`, capped in Go (`piston.ScanBand`).
**The cap is LOSSLESS, and this is the whole point:** the planner builds a plan of at most
`capacity` slots, so a single key can be picked at most `capacity` times, and the per-key fetch cap is
`<= capacity` too. So a count above `capacity` is indistinguishable from `capacity` for every downstream consumer.
**This is NOT the forbidden "approximate count"** (below): an approximation is wrong in *either* direction and
distorts fairness; a cap is *exact* where it matters (`count < capacity`) and *saturated-correct* above it.
Pinned by `TestPiston_ScanBandCapsCountAtCapacity`.

**The cap bounds the reported COUNT, not the SCAN, on every dialect.** The `GROUP BY` reads every due row of the
band, so the scan stays O(due rows at the band); sub-linear phase 1 needs distinct-key enumeration (PG18+ skip-scan, or a
recursive-CTE loose index scan) and is deliberately deferred. The cap's correctness depends on none of this.

*Aurora / managed variants:* Aurora PostgreSQL runs the **stock PG planner/executor** (Aurora replaces only the
storage layer), so it behaves as the matching PG major version - verify with `EXPLAIN`. Aurora **Limitless**
(distributed) is unverified and a *poor fit* anyway - dwarf already shards at the application layer (`ShardSet`),
so a dwarf shard on Limitless is double-sharded, and its distributed planner is unknown territory. **Do not
assume; `EXPLAIN` on the actual target.**

**Phase 3 (`piston.FetchSteps`) is dialect-split, and the split is not optional:** a lateral `LIMIT`/`TOP` per
key on pgx and mssql (256x faster, reading 32 rows where the window reads 400,000), the window shape on mysql
(MariaDB has no lateral) and sqlite. Do not fetch with a UNION-ALL branch per key (SQLite's 500-term compound
wall; 121s on a 500k-key backlog). The dialect table, the measurements and the MySQL collation trap are in
`internal/piston/CLAUDE.md`.

**The work doorbell is PURELY LOCAL - it reaches this replica's candidate cache and nothing else.** A
broadcast to peers is ruled out by the standing rule at the top of this file; beyond buying no latency (every
peer already cycles on its own derived period), it was measured costing every receiver the PK lookup
`enqueueStepDue` exists to avoid, R-1 times per step, and offering UNPARTITIONED, so a peer raced the residue
class's owner to the claim CAS (`dwarf_steps_claim_lost` scaling with R).

Every piston cycles **unconditionally** on its own period instead: a peer discovers work by scanning, bounded,
with no message at all. The consequence to hold in mind when reading the origination sites: a step created here
is offered to **this replica's cache only**, so if `Offer` declines it (a worse band, or a non-empty partition
with no vacated slot) the step waits for the next cycle of whichever dispatcher's residue class holds it.

**Queue-as-cache: the doorbell is an `Offer`.** The doorbell carries no step to a queue; it is an
`Offer` into `candidates.Cache`. The generic path (`enqueueStep`) resolves the step's priority *and*
`not_before` in one PK lookup (off the selection path) - but the **hot-path origination sites skip the lookup**
(`enqueueStepDue`): the completing worker/creator just bound the step's priority into its INSERT/reset and its own
sleep branch already diverged, so it offers the step directly with the values in hand - one round-trip per completed
step that re-read a row this replica just wrote. The lookup path remains for the cold origination sites
(surgraph revive, a failed or cancelled child re-arming its caller in `endLostFlow`, resume, `Fork`'s leaf, and the
wedge sweep's `deliverSubgraphError`) where the priority is not in hand. If
`not_before` is in the future the doorbell short-circuits and does NOTHING - the work is not due, nothing to
preempt, the cache stays untouched, and nothing is scheduled: the step's own `not_before` keeps it invisible
to selection until it comes due, and visible to the next cycle after that.
Otherwise the step is offered to its own shard's PARTITION (an `Offer` routes by `Job.Shard`), which admits
it, within the plan's reach, into a **vacated slot**: an empty partition always, otherwise while the cache is
under its bound, in BAND ORDER - behind everything cached at its own band or better, so nothing the plan chose there is reordered, and
ahead of everything worse. The one priority test is against a band **worse than the plan's reach**, which is
declined - work at the reach is still waiting uncached, and running a worse band first is a strict inversion.
Admissions are counted as `dwarf_steps_offered` and subtracted from the refiller's discard signal, so waste
stays attributed to whoever caused it.

**An EMPTY partition ADMITS the arrival within the reach, and that is what makes a fixed-cadence refiller
viable.** Do not
make it decline (request a scan, cache nothing) on the reasoning that an arbitrary-priority step must not jump
an idle replica's queue. Declining is only affordable when something answers the decline within a fraction of
a cycle, and nothing does: a sequential chain holds exactly one pending step at a time, so its partition is
empty at *every* hop, and declining costs each hop a uniformly-random fraction of the cycle interval - half on
average, all of it at worst (~330ms over a 10-step flow at the derived ~67ms).

*This is not a fairness exception, and the framing matters:* the plan grants a fairness key a share of the
batch **for the cycle**, not a single dispatch, so a successor taking the slot its predecessor just vacated
is that key spending what it already won. It cannot amplify a share either - the successor exists only
because its predecessor freed the worker that will run it. Fairness governs **admission**, which flows get
started.

It is not a test-only trick: the most loaded fixtures gain most from it, because load empties partitions
faster than the period refills them (the table is in `internal/candidates/CLAUDE.md`).

**Band order in the cache is not preemption.** An offered better band runs before worse candidates that are
merely cached; nothing already running is interrupted. `docs/scheduling-and-reliability.md` promises the
matching contract: priority is never preemptive, and a new band is served within a snapshot cycle or two.

**The cycle period is the supply control, DERIVED per shard (`deriveRefillInterval` /
`recomputeRefillIntervals`, ~67ms at the reference config) - NOT a fixed constant.** `scheduling.go` carries
the formula (*refill when half the partition has been consumed*), its constants, the worker term, the share
clamp and the supply loop's `min(DefaultMinGap, period/3)` gap, each with its measurement. What radiates:

- **The period exists because an unpaced piston runs at a 100% duty cycle** (measured: every refiller scanning
  back to back for a whole 60s window, 3.4x the phase-1 load of a barriered build). Phase 1 costs per DUE ROW
  however many rows the pass then fetches, so sizing the batch never substitutes for scanning less often. The
  pipeline paces from the *start* of the previous scan, so a slow cycle pays for itself rather than stacking.
- **It reads no observed rate**, only static configuration (capacity, the shard's pool, the replica count), and
  it is re-derived on every path that changes a pool, like the dispatch count and the cache.
- **It depends on `workersPerConnBudget` (8) through the cache** (`capacity = 2 x 8 x conns`). Cutting the 8
  shrinks the buffer ~4x and leaves the period longer than the buffer can cover (measured -10% throughput), so
  the crew is shrunk through its own constants (`residentWorkersPerConn`, `turnstilePassesPerConn`, see
  "Worker sizing"), never this one.
- **Scanning less often helps more than the scan count suggests**, because the band scan's apparent fixed
  cost is largely connection-pool wait against worker traffic. The same pool wait is why per-shard pass
  durations diverge (28ms vs 125ms) on identical hardware; do not read the spread as a slow shard.
- **Do not replace the static formula with an adaptive one.** An adaptive fetch depth was inert (the batch is
  set by the backlog and the plan slice), an interval derived from observed consumption was harmful (~1,000x
  the discard, 2.4x the p99, because consumption is `min(demand, supply)`), and an AIMD crawl over the
  `discarded` signal could not see scan cost and over-crawled to the clamp. A control loop here must be a
  fuse, not feedback on its own actuation.
- A throughput number from an IOPS-throttled disk is bimodal and cannot resolve the optimum; see
  `bench/CLAUDE.md`.

**CROSS-SHARD PRIORITY IS STRICT ONLY WITHIN ONE OR TWO CYCLE INTERVALS**, and this must be stated rather
than assumed. Order inverts two ways, neither a correctness bug (the cache holds hints, the claim CAS grants
the step) and neither reachable on a single shard:

- **The publish gap.** Peers learn of a better band only from the arriving shard's next **tally**, so it is
  globally visible up to one Tallier interval (`derived x scanners`, capped at 1s) plus one supply interval
  later. Inside that window a peer fills from a stale picture and dispatches worse-band work: an inversion
  of dispatch ORDER, not merely latency. Do not claim "the rate limit can only delay dispatch, never invert
  order"; that holds only for a cycle planning from a current picture.
- **The local cache.** `Pop` ranks partitions by the band of the candidate it would take, which is only what that
  partition holds, never the current global picture. Each partition is reconciled only by its own shard's next
  pushing cycle, so order is as strict as the SLOWEST piston's reconciliation. The failure is asymmetric: one starved piston strands a stale hint, a uniformly slow
  fleet does not. A test releasing work across shards must wait for two pushing cycles per shard
  (`CheckpointRefillCycleDone`); no elapsed time substitutes (pinned at 1s with no wait, 5 of 5 runs swapped
  adjacent bands).

Ordering among work already tallied **and reconciled** is strict, and nothing is preemptive, in the cache
included. `docs/scheduling-and-reliability.md` states the contract publicly - keep the two in step. Pinned by
`fixtures/shardedflow_test.go` and `fixtures/crossshardpriorityflow_test.go`, which asserts urgent-burst
LATENCY because an ordering-only test passes a starved build (a 5s period ordered correctly, 22x slower). Both
are interval-relative: if either fails on timing, suspect the derived period (and `share`'s clamp at 1, see
`scheduling.go`) before the test.

**The period's fuse is the pipeline's `MinGap` - do not move it into the derivation as a clamp on the
period.** A period is a START-to-start bound, so a cycle that outruns it (the deep backlog, where the scan is
O(due rows)) starts its successor at once; `MinGap` bounds the quiet time from END to START, the stronger form
(unpaced, ~half of all pops at high backlog were stale). It is inert at the derived ~67ms. The bench override
(`setRefillInterval`) only ever lowers the gap to match a tighter pin. Pinned by
`TestRefillInterval_DeepBacklogLiveness`.

**Lease recovery is the ONLY thing that recovers a crashed worker**, because a piston never touches a
`running` row, and it is the backstop for nothing else: a missed doorbell leaves a `pending` row the next cycle
selects, and a `pending` step with a future `lease_expires` is invisible to both (the lease-extension guard in
`persist.go`). It runs on `recoveryLoop` (see "Background Recovery"), the one cadence for every background
repair; its worst case is `budget + leaseMargin + recoveryInterval`, and the constraints on shortening the tick
(never from the engine's `TimeBudget`, sized for one surviving sweeper, no crash-detection trigger) are at
`recoverExpiredLeases` in `wedge.go`.

### `processStep` and `dispatch` (`dispatch.go`)

`processStep` is a sequence of stages - `claim`, `load`, `startTask`, `execute`, `outcome`, then one of
`parkInterrupt`/`parkSubgraph`/`retry`, or `complete` then `transition` - each an `Engine` method taking the run's
`*dispatch`. A stage returning `done` ends the dispatch (a lost claim, a terminal flow, a deferral, a failed step).

- **`dispatch` is data only: no methods.** Its fields are grouped by the stage that sets them; a later stage reads a
  group and does not rewrite it, except where a field's comment says so. The claimed columns are the named field
  `row claimRow`, held by value, not embedded (embedding would promote `destinations()` onto it). The dispatch is
  the step's one per-dispatch allocation, so a value a stage needs later lives in it rather than escaping on its own.
- **Every defer that must outlive a stage is registered in `processStep`**, in this order: the entry-phase
  close, the claimed-dispatch checkpoint, the recovery (`recoverStep`), the span's `End`, the budget's cancel, the
  persist pass's return, the exit-phase close. A defer inside a stage runs when that stage returns.
- **The persist pass is `d.pass`, returned through `defer func() { d.pass.Return() }()`**, never
  `defer d.pass.Return()`: `yieldTurn` swaps it, and a bound method value would return the pass that existed when the
  defer was written.
- **`processStep` owns `ctx`.** It moves the claim stamp after the host call (`turnstile.Away`) and publishes the held
  turn (`withHeldTurn`), and the recovery reads the ctx it holds at the end. A stage that moves its own `ctx`
  (`parkSubgraph`'s `yieldTurnAway`) moves only its copy.
- The persist/fail family (`failStep`, `failAndReturn`, `failOnPersistError`, `settlePersist`, `persistStep`,
  `persistYielding`, `persistStepYielding`) and `deferNewerPayload`, `handleInterrupt` and `fireFanInDirect` take
  `*dispatch` too: every caller is a dispatch. The primitives under them (`persist`, `extendLease`, `releaseLease`)
  take `(db, stepID, leaseSeq)`; `extendLease` also serves the cohort stripe's keepalive (`lockCohortStripe`).

### Round-trip minimization in `processStep`

`processStep` is the hot path, and it is strictly **sequential** end to end - the engine spawns no goroutines outside
its lifecycle loops and imports no errgroup. What optimization there is collapses round-trips to a remote database,
not parallelism:

- **Claim UPDATE + step SELECT** - on pgx/sqlite/mssql the claim and read are **one** round-trip via
  `RETURNING`/`OUTPUT` (reads the row *as updated* - a consistent snapshot). MySQL lacks RETURNING, so they are two
  statements run **serially** (claim first, read only on success): running them on separate connections is unsafe -
  an independent read snapshot can observe the pre-transition row and deliver an empty resume payload (the reason is
  spelled out at the claim site in `dispatch.go`). The lease size comes from the step row's own `time_budget_ms`
  (referenced self-referentially in the claim UPDATE), not a pre-SELECT.
- **Flow data** - on PostgreSQL it comes back with the claim (below); elsewhere it is read after the claim,
  since it needs the `flow_id`.
- **A flow's graph is read once per graph per replica** (`internal/graphcache`, held as `e.graphs`; how it works
  is in `internal/graphcache/CLAUDE.md`). Dispatch and `flowGraph` read the `graph` column (dispatch on its own
  turn) only when both layers miss. Every intern goes through `internGraph`, which makes a parse failure a
  `permanentError`. `Create` seeds the cache only on a replica that dispatches the shard, and after the commit.
  `graph_hash` is carried verbatim by `Continue` and `Fork`, never recomputed: they copy the graph, and on
  Postgres and MySQL the copied bytes are re-spelled, so their hash would differ.
- **A transition's successors go in as multi-row INSERTs** (`successorBatch`), in power-of-two chunks of at
  most 64 rows (23 binds each, under SQL Server's 2,100-parameter ceiling). The ids come back by `RETURNING`/`OUTPUT`
  and are matched to rows by `step_token`, since neither promises VALUES order. MySQL inserts row by row: it has no
  `RETURNING`, and under the default `innodb_autoinc_lock_mode=2` a multi-row insert's ids are not guaranteed
  contiguous.
- **The transition, `completeFlow` and flow creation are statement batches** (`txBatch`: `queue` pairs a
  statement with the function that reads its result, `flush` sends and reads in order; one round trip per flush on
  PostgreSQL, other drivers run the same statements one by one). A flush happens where something must be read
  before the next statement can be written - an id, or the cohort bump - where nothing else may run while results
  are open, and every 64 successor rows or `maxPayloadBytes` of their payloads, to bound memory and the
  statement (`successorBatch.full`).
  Measured on PostgreSQL (`TestRoundTrips_*`, simulated RTT, about +-0.5): a linear step ~5 round trips, a one-step
  flow ~11, a fan-out cycle of two branches ~20.5.
- **On PostgreSQL the claim returns the flow row too** (`UPDATE dwarf_steps s ... FROM dwarf_flows f ...
  RETURNING s.*, f.*`), so dispatch reads the `graph` column only for a graph this replica has never parsed
  (the claim returns `graph_hash`). The join reads the flow row without locking it: Postgres locks only an
  UPDATE's target rows, so the claim cannot deadlock with Cancel/Terminate, which lock the flow row before its
  steps. **Do not extend it to SQL Server** as things stand: there the join takes a shared lock on the flow row
  while the claim holds the step row - the reverse of that order - unless the database runs
  `READ_COMMITTED_SNAPSHOT` (recommended, not enforced). SQLite's `RETURNING` cannot reference a joined table,
  and MySQL has no `RETURNING`; both keep the separate flow read.
- **An id a later statement needs is named in SQL on PostgreSQL, read first elsewhere** (`txBatch.insertedID`):
  `lastStepIDSQL`/`lastFlowIDSQL`, the table's sequence's `currval`, so the dependent statement shares the INSERT's
  batch; elsewhere the batch is flushed and the id bound, which costs nothing on a driver that does not pipeline.
  **Do not use a session-wide last id** (`lastval()`, SQL Server's `@@IDENTITY`): it reports the last id generated
  in ANY table, so a trigger a deployment adds would silently become the id written. Both ids are `BIGSERIAL`, so a
  role that can insert a row already holds the sequence privilege `currval` needs. **A `WHERE` clause compares
  with `currval` as a scalar subquery** - `flow_id=(SELECT currval(...))` - which PostgreSQL evaluates once as an
  InitPlan; compared directly, the volatile function cannot be an index key (measured on a 100k-row table: a
  sequential scan against a primary-key index scan). `SET` and `VALUES` take it bare. A flow INSERT that draws
  its own id (`selfFlowInsertSQL`) calls `nextval`, which sets `currval` as a default-filled id does.
- **The transition** queues the lock grab, the successor INSERTs (flushed every 64 rows or `maxPayloadBytes`, so a
  wide fan-out holds only one batch's branch states in memory) and the fenced own-row write. It writes nothing to
  the flow row.
  - A **lone successor** (every linear and goto step, every step inside a forEach branch) is named by
    `insertedID`, so on PostgreSQL the transition is BEGIN, one batch, COMMIT. Pinned by
    `TestSuccessorBatch_LoneSuccessorLinks` (concurrent flows, every link checked against `predecessor_id`, every
    flow stopped at its own last step).
  - **Several successors** are flushed and their ids read first: `successor_id` is the FIRST branch, which
    History's step navigation follows, and no dialect's last-id names the first row of a multi-row INSERT (MySQL's
    reports the first, the others the last). Do not replace the read with a lookup by `step_token`: the column is
    in no index, so the lookup visits every step of the flow created after the source - every concurrently
    committed step of a wide cohort - and run from every branch step it is quadratic.
  - The terminal-status guard is read after the statements batched with the grab ran, so a terminal flow bails by
    **rolling back** (`errTransitionFlowTerminal`, mapped to nil after `Transact`), and the bail shows in
    telemetry as a rolled-back transaction, as a lease-fenced one does. The DEFERRED grab of a resolving fan-in
    arrival is the exception: its bail returns nil, because that transaction's cohort bump must commit (below).
  - Never close batch results with a `defer`: on a driver that does not pipeline, `Close` runs every statement
    not read yet - every successor INSERT of a bailed transition, under the flow-row lock - while the rollback
    after an early return abandons them unrun.
- **The flow row records only the step a flow STOPPED at** (`dwarf_flows.stopped_step_id`), set by writes the flow row
  takes anyway: the completion and loss writes (`completeFlow`, `failFlowTx`) record the step that ended the flow
  (0 for a completing arrival that resolves a failed cohort, which is not the loss); the interrupt write records
  each chain flow's interrupted step, the FIRST interrupt winning; Resume clears it on the flow it sets running and
  re-points a flow left interrupted at its earliest remaining interrupted step; Terminate leaves
  it. A running flow records 0. `FlowSummary.StoppedTaskName` and `Query.StoppedTaskName` read it. **Do not advance it per step:** on
  PostgreSQL every UPDATE writes a full new copy of the row, and the flow row is ~1.2 KB with the graph held inline
  (TOAST moves a value out of line only past a ~2 KB row), so a per-step advance is a row version per step on the
  flow's hottest row - for a pointer `List` shows and that is stale by the time anyone reads it.
- **A fan-in arrival flushes the transition first**: the cohort bump runs alone, on no open batch, because whether
  the cohort resolved is decided from it. The ARRIVAL THAT RESOLVES the cohort then sends [the deferred flow-row
  grab + the spawn read + the cohort-members read (`queueCohortRead`)], folds them in Go (`fanInRow`), and queues
  [the fan-in INSERT + the cohort-exit `successor_id` write], naming the fan-in step by
  `insertedID`. A pure resolving arrival is BEGIN, bump, two batches, COMMIT: 5 round trips on PostgreSQL. The
  deferred grab's terminal bail **returns nil, committing the bump**, which is sound because everything batched
  with the grab is a read (`TestTerminateVsLastCohortArrival_Deterministic` pins arrivals == size); those reads
  still run before the bail, on every driver. The members' changes are decoded as each row is
  read, and only a completed member's. On PostgreSQL the pipelined batch reads every member's raw row when it is
  sent, and releases each one as the reader moves past it (sequel v1.12.1+), so a wide cohort is held raw and
  decoded only one row at a time - the peak is about the larger of the two, not their sum. A reduced field that arrived as a ref is
  resolved between the two batches, by its own query, only when there is one. The ref resolution and the
  cohort-failure statements run between flushes, with no batch open.
- **Do not return the spawn's payload from the cohort bump** to spare the resolving arrival its spawn read. The
  bump runs on EVERY arrival and holds the spawn row's write lock until COMMIT - the queue every sibling of the
  cohort waits in - so it would carry the spawn's `state` across the wire N times to save one round trip on the
  last, and lengthen the hold that is kept short on purpose: at fan-out width 64 a follow-up read inside that hold
  measured 10.1 ms for the cohort-row statement against 6.7 ms without it, and 11.3 ms against 7.9 ms for the
  arrival transaction - 7-13% of fan-out throughput and twice the statement's p99. The spawn read belongs where it
  is: in the resolving arrival's own batch, after the bump.
- **Do not collapse the transition into one PostgreSQL data-modifying CTE** (`WITH lock AS (UPDATE dwarf_flows
  ...), ins AS (INSERT INTO dwarf_steps ...) UPDATE ...`). Its sub-statements run against one snapshot with no
  defined order between them, so it cannot express write-first - the flow row's lock taken before any step is
  touched - which is what serializes concurrent workers on one flow. Statement batches already make the
  transition one round trip on PostgreSQL without giving that up. Any shape that reorders the transition's locks
  needs a concurrency test (`completionraceflow`, `soakflow` reproduce the wedge write-first prevents) and a
  lock-order check, not reasoning.
- **`completeFlow`'s lock grab returns the row it locked** (a locking read on PostgreSQL and MySQL,
  `RETURNING`/`OUTPUT` on the `touch` flip elsewhere), and goes out in one batch with the read of the flow's completed tails (`queueCompletedTails`), which
  the final state is merged from: BEGIN, one batch, the final-state UPDATE, COMMIT. The UPDATE depends on the Go
  merge, so it cannot join the batch; the fallback read of any tails, and a ref resolution, follow only when
  needed.
- **Creating a flow** (`insertFlowTx`, shared by `Create`, `Continue` and a subgraph spawn) is [flow INSERT,
  entry-step INSERT naming the flow by `insertedID`]: BEGIN, one batch, COMMIT on PostgreSQL. A new tree's
  `root_flow_id` is the row's own `flow_id`; a known root (a subgraph child) is bound. **On PostgreSQL the INSERT
  writes the self column itself** (`selfFlowInsertSQL`: `INSERT ... SELECT n, n, ... FROM (SELECT nextval(...) AS
  n) s`), so the row is written once. **Do not write it with an UPDATE after the INSERT there:** `root_flow_id` is
  indexed, so that UPDATE can
  never be HOT: a dead row version, and a new entry in each of the row's indexes, for every flow. The other
  dialects keep that fix-up UPDATE, which updates in place, after the entry step; it writes only the self
  column, since the INSERT stamped `updated_at` in the same transaction. Fork's clone builds its flow rows with
  the same helper. Pinned by `TestInsertFlowTx_ConcurrentCreates` (every new flow's root id points at its own
  row, and its entry step belongs to it) and `TestInsertFlowTx_SelfColumnsInTheInsert` (no fix-up on PostgreSQL;
  the root ids of Create, a subgraph spawn, Continue and both kinds of Fork clone).
- **A batch reader scans with the PostgreSQL driver's own type conversions** when the batch is pipelined, and
  they are stricter than `database/sql`'s: **scan a column into the Go type it is stored as.** A `SMALLINT` flag
  (`delete_on_completion`, `cancelling`, `advancing`, ...) does not scan into a `bool` - it scans into an `int`.
  A `bool` destination for a `SMALLINT` fails every such scan, so a batched `delete_on_completion` read into a
  `bool` fails every flow completion - and only on PostgreSQL, so the default SQLite run never shows it.

Fan-in accounting issues no sibling/subgraph COUNT queries at all - it reads the
`cohort_arrivals`/`cohort_failures`/`cohort_size` counter columns.

**Transaction constraint (do not reintroduce parallelism here):** a function receiving a `sequel.Executor` - which may
be a `*sequel.Tx` - must not run concurrent statements on it, because a SQL transaction is not safe for concurrent
use. This is exactly what forbids wrapping an errgroup around, say, the flow read and `resolveStateRefs` (which takes
a `sequel.Executor`). It applies to `computeFinalState` and code inside `failStep`/`Terminate` transactions.

### Fan-Out and Fan-In

**Static fan-out** occurs when multiple transitions match from one task. All targets run in parallel, sharing a
`step_depth`.

**Dynamic fan-out** uses `forEach` on a transition to iterate a state array and spawn one task instance per element,
each receiving the element under the `as` key. An **empty array** spawns no branches but does **not** stop the flow:
the transition path routes straight to the fan-in task (`fireFanInDirect`), which runs on the source step's own
`changes` folded onto its `state` - the base `fanInRow` folds a populated cohort onto, with nothing more to fold
(`fixtures/emptyforeachreducerflow_test.go`). The
fan-in target comes from the routing map the engine derives once per graph per replica (`internal/faninmap`),
not from `Validate`. The arm that completes the flow at the source serves two cases: a source that routed to
`END` (`routedToEnd`, below - reachable and deliberate), and `fanInOfSource == ""`, which is effectively
unreachable for any graph the engine accepted: `Validate` (run at `Create`) requires a fan-out source to converge on a `SetFanIn`
task. It is a defensive fallback, not the documented behavior of an empty array.

**An explicit route to END beats the empty-cohort convergence, and the two are told apart by `routedToEnd`, not
by the cohort size.** A fan-out source is free to also carry a `Goto`/`onError` edge to `END` - "fan out over
the array, or end the flow when the task decides there is nothing left to do" is the shape every task-driven
loop takes. Both that and a genuinely empty array leave `cohortSize` at 0, because the END candidate is filtered
out of `realTasks`, so a `cohortSize == 0` test alone reads a deliberate termination as an empty cohort and sends
the flow to the fan-in. Where the fan-in loops back to the source - which is the whole point of the shape - that is
not a wrong-but-quiet outcome: the flow never terminates and writes a step row per lap (**measured: 21,735 steps in
~6 seconds** before an external deadline killed it). Pinned by `fixtures/fanoutgotoendflow_test.go`, which asserts
the step COUNT and not merely the terminal status, since a loop that ends only when something outside gives up
still reports `completed` in the shapes where it eventually does.

**An override edge out of a fan-out source pushes NO lineage frame, because the runtime spawns no cohort for
one.** `isPushTransition` is already false for a `Goto`/`onError`, so `Graph.validateLineage` mirrors it (the
`tr.Kind != ""` arm; why that arm sits below the fan-in arm is argued at the fan-in arm
itself, and `Switch` is inert there - a fan-out source cannot carry a switch edge). What the mirror buys is that
each of these is settled at `Create` rather than at dispatch:

- **Permitted:** the override may go to `END` (the loop above), STRAIGHT to the source's own fan-in (the source
  IS the spawn, so `fireFanInDirect` converges on its own state), or onto any path that never reaches the fan-in.
- **Rejected:** it may not route into one of the branches, nor onto a path that later transitions INTO the
  fan-in. Neither spawns a cohort, so that arrival lands with `cohortSpawnID == 0` - the trunk case fails the
  step on the not-in-a-cohort guard, and a nested source bumps arrivals on its OUTER spawn and silently drops
  the branch. A graph frozen before the check still hits those runtime guards, which is what they are for.

Pinned by `TestLineage_GotoEndFromFanOutSource` and `TestLineage_OverrideRejoiningFanInRejected`.

**A branch sees its flow's state, plus its element.** Each `forEach` branch's local `state` is the flow state with
three injected fields: `<as>` (the element), `<as>Index` (its position), `<as>Count` (the cohort size). Nothing is
removed. **Do not strip the source array from each branch's local state** as a special case (an N-element
forEach feeding `forEach -> A -> B -> C -> J` otherwise writes N copies of the array into every step row of every
branch, which is what tempts it). A strip is a byte optimization for the one carried field the engine happens to
know the name of, while every *other* large carried field pays the same N x chain-length cost unaddressed; it makes
a branch's state a lie (the branch cannot see the array its own element came from); and it makes a failed fan-out's
`final_state` come back *missing* the array - a failed cohort never reaches its fan-in, so the terminal-state merge
bases on a completed sibling's branch-local snapshot, which is precisely the stripped one. De-duplicating large
carried state is a general mechanism - **state refs**, below - not a per-field deletion. The source array is
*ref'd* (one stored copy, every branch pointing at it), which is the same byte saving without deleting anything or
lying to the branch.

**Downstream suppression via explicit clear** is the author-space way to drop a large source array past the
fan-in. A branch that wants to suppress the source array past the fan-in calls
`flow.Set(<forEach>, nil)` in its body. That writes the new value into the branch's `changes`, the replace reducer
at fan-in folds it over the spawn-step base, and the field is absent (or whatever the branch wrote) past the fan-in -
useful for a forEach over a very large array where downstream tasks only care about the per-element transformation.

**Fan-in strip on dynamic fan-out.** The injected per-branch bookkeeping (`<as>`, `<as>Index`, `<as>Count`) has no
meaning once the cohort is behind the flow: with the Replace reducer, one branch's element value and index would
otherwise win arbitrarily and ride forward - the flow's state would say which element it saw by *completion order*.
So `stripForEachBookkeeping` (`finalstate.go`) deletes those three names. A workflow wanting the element value past
the fan-in must forward it under a different key. **Two paths call it and both must**, or they disagree on what a
fan-out's state means:

- `fanInRow` - the cohort **converged**; the merged fan-in snapshot is stripped, scoped to the **spawn's own
  task** (`tr.From == spawnTaskName`).
- `computeFinalState` - the cohort **failed** and so never reached its fan-in, so the engine folds it as the fan-in
  would have (`mergeCohortState`) and strips **that cohort's** names, the outermost on the tail's lineage chain
  (`outermostSpawn`). Every inner cohort that never converged is folded and stripped of its own names the same way,
  one level down (`simulatedFanInPart`, below). **Do not strip the inner cohorts' names from the final state:** the
  rebuild's base is trunk state, which never held them, so the strip hits a trunk field that shares an inner `as`
  name (`pageCount` under `forEach ... as "page"`) - a field a convergence keeps.

**The strip is scoped to the cohort being closed, and BOTH halves of that scoping are load-bearing.** Stripping
"every `forEach` in the graph" breaks two things at once:

- **Name collision (silent data loss).** It makes the three injected names of *every* `as` globally reserved. A graph
  with `forEach … as "page"` reserves `pageCount` for the whole workflow, so a task writing its own `pageCount` -
  even one downstream of the fan-in, outside the cohort entirely - has it deleted from `final_state` while `History`
  still shows the step wrote it, and the author is never told the name was reserved. Scoping by the merge base's
  *lineage* prevents it: a tail outside any cohort (the ordinary completed flow, whose terminal step is downstream of
  every fan-in) is inside no cohort and strips **nothing**.
- **Nesting.** At an *inner* fan-in it also deletes the *outer* cohort's bookkeeping, so a step converging out of the
  inner cohort - still inside the outer branch - loses which outer element it is working on. Scoping by
  `tr.From == spawnTaskName` prevents it: each cohort's names die at its **own** fan-in and no earlier.

The names are reserved only *within* their own cohort. Pinned by `fixtures/foreachstrip_test.go` (collision, nesting,
and the failed-fan-out case the strip exists for), each verified to fail against the unscoped version.

**Fan-in** is implicit. When the last sibling at a cohort completes, the engine merges all siblings' changes using
reducers and creates the next step(s) in a transaction that prevents duplicate next steps when multiple workers
finish siblings simultaneously.

**Every place a step's changes meet its state folds them through the reducers (`State.Apply`).** That covers
the dispatch view (input ⊕ earlier runs' changes), the successor base (`successorBase`, which routing also
reads), the fan-in's base (the spawn's changes folded onto its state - the value every branch was handed),
`fireFanInDirect`, the final state (`mergeTerminalSteps`, each tail's changes onto its state), a failed
cohort's rebuild, and the interrupted `Snapshot` (through the reducers of the flow's graph). A
fold that fails is a pure function of the data: a successor-base failure fails the step before anything is
written, and the final-state and cohort folds are `permanentError`s. Pinned by
`fixtures/terminalreducerflow_test.go`, `fixtures/emptyforeachreducerflow_test.go` (branches handed 15, the
fan-in 15 plus their contributions) and `TestHandOff_ReducerErrorFailsTheStep`.

- **The step's whole delta is what the carrier returns** (it was seeded with the earlier runs' changes); persist
  it as is, never composed onto them again (`workflow/CLAUDE.md`, "A step's runs compose").
- **A fold's result is in no row, so it is never anchored at the folding step** - a ref there would resolve to
  the delta alone. Under `append`, and for an addition, the mint stores it as a list naming the rows that hold
  its elements: the base where it already is, then each contribution in the `changes` of the step that made it
  (`appendFolds` at the linear, branch and `fireFanInDirect` mints; `cohortAppendFolds` at a trunk fan-in, from
  the spawn's base and part and each member's). Any other reducer, and a base that is not an array, are stored
  inline (the rest of `foldedFields`, passed as `inlineOnly`). A fan-in INSIDE an outer cohort keeps everything its cohort wrote inline (below), and so does a
  field an inner fan-in's part touches: that part is a projection of the inner fan-in's state, not a
  contribution in a `changes` column. Pinned by `TestArrayRefs_AppendLoopStoresEachElementOnce`,
  `TestArrayRefs_ListAtTrunkFanInAfterInnerCohort`, `TestStateRefs_ReducedFieldIsResolvedAndReanchored`,
  `TestStateRefs_SpawnWrittenReducedFieldIsItsOwnValue`, `TestStateRefs_EmptyForEachFoldsAReffedBase` and
  `fixtures/arrayrefsflow_test.go`.
- **A folded field's BASE may be carried by a ref**, so the fan-in paths resolve it before folding:
  `fanInRow` resolves every reduced field the spawn did not overwrite, `fireFanInDirect` the fields it folds
  (`TestStateRefs_EmptyForEachFoldsAReffedBase` fails without the resolve).
- **A null on a reduced field is dropped from a delta as it enters** (`dropIdentityNulls`: the task's result,
  `fireFanInDirect`'s and the spawn's stored changes). It contributes nothing, but the state-refs mint reads any
  key a delta writes as a rewrite: it would drop the carried ref and re-anchor the field at a row holding the
  null. The carrier never writes one; a remote host or a row stored before overwrites existed can.
- **Known cost: a folded field under any reducer but `append` cannot be shared across a fan-out's branches.** A fan-out source that folds into a large one stores it inline in every branch's state,
  N copies. Under `append` each branch holds the list instead.

**Fan-in merge order and contribution (lineage `SetFanIn` path).** `queueCohortRead` reads cohort members
(`lineage_id = cohortSpawnID`) `ORDER BY fan_out_ordinal, step_depth, step_id` and `fanInRow` folds them in that
order. `fan_out_ordinal` is stamped at fan-out from the branch's position in the spawn loop (the `forEach` array
index or static declaration order), so order-sensitive reducers (`append`/`union`/`concat`/`merge`) fold in input-array order rather than
completion order; `step_id` breaks ties. The firing gate is
`cohort_arrivals >= cohort_size`, a counter on the spawn step independent of the merge query, so the merge's status
filter cannot deadlock fan-in. Only `completed` members contribute; `failed`/`terminated`/`cancelled`/
`pending`/`running` contribute nothing.

**A member's contribution is its `changes`, EXCEPT an inner fan-in's (`cohortfold.go`).** In a nested fan-out
an inner fan-in carries its spawn's lineage, so it is a member of the next cohort out, and its `changes` hold only
its own task's writes. Everything its cohort added sits in its `state`. So:

- **A fan-in inside an outer cohort records which part of each field its cohort added** (`foldCohort`,
  stored per field in the fan-in step's `state_reduced` column), as a description of the folded value, never a
  second copy of it. A
  fan-in on the trunk (its spawn's `lineage_id` is 0) is never read as a member, so it skips the work:
  - `from` for `append`/`union`/`concat` - they only add to the end, so the cohort's part is a suffix;
  - `keys` for `merge` - new wins, so the folded value under those keys IS the cohort's;
  - `value` for `add`/`min`/`max`/`and`/`or` - a sum or a minimum cannot be split back apart, so the
    combined contribution is kept (one scalar);
  - `keys: []` for a field with no reducer, meaning "the cohort wrote it"; it projects as its value, or as
    a delete if absent.
- **The outer fold takes an inner fan-in's part ⊕ its own `changes`** (`memberContribution`). The part counts
  WHATEVER the inner fan-in's own status: its row exists only because its cohort converged, and every step of
  that cohort completed. Only its own writes need it to have completed. Gating the part on completion lost a
  converged cohort whole when the fan-in task itself failed, or when a `Terminate` landed before it ran.
- **The cost is one extra query per resolving fan-in that HAS inner fan-in members** (`cohortParts`, by primary
  key, padded to power-of-two arities so the statement text takes few forms), and it never reads a row below an
  inner fan-in, at any depth: each fan-in's state already holds its whole subtree. Each row is projected as it
  is read and then dropped, so memory holds the parts. The bytes READ are the inner fan-ins' whole rows, which
  hold the accumulated value from their base for an `append`/`union`/`concat` field, not just the cohort's part:
  N inner fan-ins over a long carried history read it N times.
  Rejected: re-folding the subtree (N x M reads, N x M x O three deep); storing the cohort's delta beside the
  folded value (every contribution stored twice per level); a `replace` of the folded value (the second
  branch's replace wipes the first's).
- **`state_reduced` belongs to its fan-in step alone.** `fanInRow` writes it (`successorRow.reducedJSON`)
  and every other row holds `{}`, so a step after the inner fan-in never carries a boundary that does not
  describe its own input. `cohortParts` reads it, and Fork reads a leaf's record and writes it back frozen
  (`forkLeafReduced`, next bullet); Fork's clone otherwise copies it with the row it describes.
  Pinned by `TestChanges_ReducedStaysWithTheInnerFanIn`.
- **A Fork leaf's record is FROZEN to `value`s** (`forkLeafReduced` -> `frozenReduced`): each field's part is
  projected from the ORIGINAL state and stored as a literal, before the overrides replace the state (a part the
  cohort deleted is a null `value`). An override edits a step's input and never what its cohort wrote, exactly
  as overriding an ordinary branch step leaves its `changes` alone. Left unfrozen, the outer fold projects from
  the overridden state: a `from` offset into the new array, the override's values under the old `keys`, and a
  plain field's override in place of what the cohort wrote. Pinned by the frozen half of
  `TestCohortFold_ProjectionEqualsContributions` and `fixtures/nestedreducerflow_test.go`'s
  `fork_at_inner_fan_in_with_overrides`.
- **Every field a cohort wrote must be stored INLINE in an inner fan-in's `state`, never as a ref or a list.**
  The projection reads the raw `state` and `state_reduced` columns: a ref'd field reads as absent, which projects
  a plain field as a DELETE. `fanInRow` guarantees it by passing the contributed fields to `Mint` as
  `inlineOnly` (`memberWrites`) and listing folds only at a trunk fan-in (`c.spawnLineageID == 0`). Keep it so.
- **The reducer comes from the graph,** never from the stored record.

Pinned by `TestCohortFold_ProjectionEqualsContributions` (every reducer: the projected part folds exactly as
the contributions one by one; it fails with the offset dropped) and `fixtures/nestedreducerflow_test.go`
(three levels deep, a step after each inner fan-in, converged, failed at either level, and a Fork repairing
the inner failure). **A reducer error fails the flow** - a value a reducer cannot fold (a
string under `add`) errors `fanInRow` and the resolving step fails naming the field; never
discard it, or the fan-in runs with that field's accumulated contributions silently dropped. It is wrapped as a
`permanentError` - a deterministic failure of the engine's own computation inside a persisted write - which
persist does NOT retry and `settlePersist` fails without `dwarf_steps_write_failed`: routing it through the
storage classifier would page an operator for a type mistake after a minute of pointless backoff. Wrap only a failure
that is a pure function of stored data; a database error from the same computation is transient. The failed flow's `final_state` is `{}` - `computeFinalState` folds the
same values and hits the same error, and `failFlowTx` falls back. (An unknown reducer name never gets this far:
`Graph.Validate` rejects it at `Create`.) Pinned by `TestFanIn_ReducerErrorFailsTheFlow`, which also asserts neither
write metric moves. `mergeCohortState`'s member fold - a failed fan-out's final state - is wrapped the same way, and
a path that terminalizes a flow whatever it holds - `terminateSubtree` (Terminate, Delete, Purge, the orphan
sweep) - reads the final state through `terminalFinalState`, which takes a `permanentError` as `{}`; propagating it would leave the flow impossible to stop (`TestTerminate_ReducerErrorDoesNotBlockTerminate`).
`isPermanentError` also counts a state ref the stored rows cannot satisfy (`staterefs.IsUnresolvable`: a ref to a
missing step, a field or element not where it says, an unreadable refs or state column), raised from resolves no
engine wrapper reaches, and a stored graph that cannot be parsed (`internGraph`). A failed READ is neither, and
stays transient. Pinned by `TestTerminate_UnresolvableRefDoesNotBlockTerminate` (Terminate and Delete of a flow
whose tail refs a missing step) and `TestInternGraph_UnreadableGraphIsPermanent`.

**Escalation is counter-based (`cohort_failures`), not status-based, and happens *before* `fanInRow`.**
`fanInRow` itself never marks the flow terminal - but it is only reached when the cohort resolves with
`cohort_failures == 0`. The escalation decision lives one level up, in the cohort-arrival path of `processStep`:
when a branch's own `failStep` runs with no `onError` handler, `propagateCohortFailure` bumps the spawn step's
`cohort_arrivals` **and** `cohort_failures` - and `cohort_cancellations` too when the loss is a cancellation -
(walking up nested cohorts); and when a cohort fully arrives with
`cohort_failures > 0`, the flow is **failed** instead of creating a fan-in step (`fullyResolved && failures > 0`).
The signal is a *counter* incremented only by a genuine `failStep`. **Do not escalate on member *statuses*
read in the merge** (fail on any `failed`/`cancelled` member): that races with OnError recovery, and made the
fanouterrorflow fixture flaky. A branch that errored but has an `onError` handler
does **not** bump `cohort_failures` - its step is marked `completed` and routed to the handler, so the cohort still
converges and the flow recovers via the handler -> fan-in path. A `terminated` member (from an external
`Terminate`) does not escalate: `Terminate` has already made the flow terminal. A **`cancelled` member DOES
escalate**: a cancellation reaches the branch through `failStep`, which settles it `cancelled` and bumps both
`cohort_failures` and `cohort_cancellations` - so a new cancellation path must go through `failStep` too, or it
skips the bump and lets the cohort fan in on a cancelled flow. So the invariant is precise: escalate on every
unhandled branch loss that reaches `failStep` with no `onError` (a failure or a cancellation), never on an
onError-handled error or an externally terminated sibling.

**Retry rejoins its cohort naturally.** `flow.Retry` rewinds the failed step in place - same `step_id`, `lineage_id`,
`fan_out_ordinal`, just `status='pending'` and the prior error/park slot cleared. The merge query sees one row per
branch regardless of attempts, so retry can't double-count.

**Do NOT replace the shared `cohort_arrivals` counter with per-member arrival state.** Every sibling bumps the
same spawn row and holds its write lock until COMMIT, so siblings serialize, and the idea of marking each
member's own row (`cohort_arrived`, plus a `cohort_resolved` exactly-one-resolver claim) keeps re-suggesting
itself. Built and measured, it cut contention 12-40x and LOST throughput at five of six A/B points (to -29%),
worse as load got realistic: it must commit before counting (counting inside the arrival lets the last two
members miss each other's uncommitted marks under READ COMMITTED and strand the cohort forever), so it trades
a lock wait for a second transaction, and a blocked lock is connection occupancy, which a second transaction
adds to. It also only redistributes row versions (5.59 -> 6.18 updates per step), and production branches,
finishing at different times, convoy less than any zero-delay benchmark (~100ms of spread halves the
contention). Eliminating the lock did not raise the measured fan-out ceiling. **Before trying again, show that
the spawn-row lock is the BINDING constraint on the target workload** - sample `pg_stat_activity` from one
long-lived `psql` session (`wait_event='transactionid'` is a row-lock wait, `'tuple'` the queue behind it;
exclude `ClientRead`).

**The lock's connection occupancy is relieved per-peer with a Go lock instead.** Each worker takes a fixed
striped Go lock (`e.cohortLocks`, keyed on the cohort's spawn via `cohortLockStripe`) BEFORE opening the
transition (`processStep`) or failure (`failStep`) transaction, so a losing sibling parks holding NO
connection; the row stays the cross-peer source of truth, and spawn-row occupancy drops from cohort width to R.
It adds no round trip (measured locally at width 64: `tuple` waiters 146 -> 0, throughput up). It is not a
ceiling fix: at width 256 the binding wait is relation extension from the successor INSERTs.

*Implementation load-bearers (`processStep`, `failStep`, `cohortLockStripe`):*
- **Fixed striped array, never resized.** A stripe array's mutual exclusion rests on a key always mapping to the
  same lock instance; a resize (e.g. to track a changing worker ceiling) would hand two goroutines different
  locks for one key and silently lose exclusion. Sized generously (8192), not tuned: a collision is benign - two
  live cohorts sharing a stripe cost one arrival a brief connection-free wait, never correctness.
- **Acquired before any DB work, at most one per worker, leaf-level only.** The stripe is the direct (bottom)
  cohort level; `propagateCohortFailure`'s ancestor bumps walk UP and stay DB-serialized. Because every worker
  takes its one stripe BEFORE holding any database lock and never takes a second, no Go/DB lock cycle can form.
- **Released before `processStep`'s post-commit switch.** A non-contention persist error routes to
  `failOnPersistError` -> `failStep`, which re-locks the SAME stripe, and the stripe is not reentrant - so the
  normal release is explicit there; the deferred unlock is only the panic backstop.
- **A waiter keeps its lease alive, so each stripe is a FIFO lock with an explicit queue (`cohortStripe`), not a
  `sync.Mutex`.** A parked arrival's step is already `completed` and flagged `advancing`, so a queue that outlasts
  its lease lets the orphan sweep take it back: correct (the waiter's transition is fenced, the step replayed) but
  the queue's work is done twice. So a waiter wakes every `min(leaseMargin, extension)/2` - derived from both, since
  the tick must land before the extension it last made runs out - and extends its lease (`extendLease`, on a turn
  of its own), abandoning as fenced if the extension finds the step taken back.
  - *Why not the obvious locks.* A `sync.Mutex` has no timed acquire. A one-slot channel has one, but a waiter that
    leaves its send queue to extend rejoins at the BACK, so with a queue longer than one interval the oldest
    waiters are pushed back forever. And the extension cannot run on a helper goroutine while the waiter stays
    blocked, since `processStep` spawns none. The explicit queue lets a waiter tick without giving up its entry,
    and unlock HANDS the stripe to the head, so service order is arrival order.
  - *A panic in a tick withdraws the waiter before propagating.* Left queued, its channel would be handed the
    stripe by the next unlock with nobody to take it, and every later arrival would wait - and keep extending its
    lease - forever, beyond even the sweep's reach.
  - *`extendLease` only moves a lease forward.* A step claimed with a long budget that finishes early keeps it;
    resetting to `NOW + extension` would cut it short. Its UPDATE matches only a lease that needs extending (so the
    value always changes and MySQL's `RowsAffected` stays honest), and only a zero-row match pays a read to tell
    "taken back" from "already long enough".
  - *The first tick comes one interval after parking*, so a waiter that parks with less lease than that left - a
    task that overran its budget - can still lapse before it. The orphan sweep then takes the step back and it is
    replayed: correct, the waiter's own transition fenced and redone once. Lease recovery can likewise take a
    `running` step waiting in `failStep`. Ticking on arrival would close it at one more UPDATE on every
    contended arrival, on the width-64 hot path; declined.
  - *Cost.* The uncontended path allocates nothing; a waiter allocates a channel and a timer. A handoff admits no
    barging, but `sync.Mutex` hands off too once a waiter has blocked past 1ms (starvation mode), which millisecond
    transaction holds reach routinely - so the width measurements above, taken with `sync.Mutex`, were very likely
    already in handoff mode. This lock has not been benchmarked at width. 8192 stripes are ~320KB per engine.
  Pinned by `TestCohortStripe_FIFOAcrossTicks`, `TestCohortStripe_Abandon`, `TestCohortStripe_TickPanic` and
  `TestCohortStripe_QueuedArrivalsKeepTheirLeases` (fails with the extension removed).
- **Coalescing was considered and rejected.** Folding a peer's arrivals into one `+k` write would split a member's
  result-commit from its arrival-count, and either order leaves a crash window that strands (result committed,
  in-memory arrival lost on a peer crash -> the cohort never reaches `cohort_size`) or double-handles a member. The
  single-transaction coupling of (own result + arrival) is the same invariant the per-member redesign's
  commit-before-count has to wrestle with. Serialize-only keeps it intact, and connection occupancy was the whole cost.

### Candidate de-duplication: the partition (cross-peer) and the claim map (intra-peer)

A candidate the refiller selects can be one another worker is already claiming, and every such duplicate
costs a **full dispatch round trip to be told it lost the CAS**. There are two distinct sources, they need
two distinct mechanisms, and each is useless against the other's case:

- **Cross-peer.** The planner's key pick is independently random per replica, but *within* a key the fetch
  is deterministic (`ORDER BY created_at, step_id`), so every replica that picks a key fetches the SAME
  oldest rows. Handled by `partitionPredicate` - `AND step_id % R = ordinal` in **both** phase 1 (so the
  tally reflects the owned slice) and phase 3 (so the fetched ids actually are owned). Phase 1 alone is
  not enough: it only shapes per-key counts, while phase 3 returns the ids workers claim.
- **Intra-peer.** A claim issued but not committed still reads `pending`, so this replica's own next pass
  re-selects a step it is mid-claim on. Handled by the `claims` tracker (`internal/claimstracker`, which owns
  the 1-2s window, the two-generation roll and the `(shard, stepID)` key): `TryClaim` in the worker loop,
  before `processStep`.

Both are **advisory**: the claim CAS remains the only thing that grants a step, so a stale or missing entry
costs a wasted round trip or a deferred dispatch, never correctness.

**Three metrics price the three stages of candidate waste, and they are not interchangeable:**
`dwarf_refill_candidates_discarded` (selected, never popped - refiller oversupply),
`dwarf_steps_claim_preempted` (popped, skipped before any claim - round trips SAVED by the map), and
`dwarf_steps_claim_lost` (popped, claimed, lost the CAS - round trips WASTED). A healthy engine converts
what would have been *lost* into *preempted*; comparing the two is how you tell the map is working rather
than over-suppressing.

**Engine-side load-bearers of the claim map:**

- **Check in the worker loop, not inside `processStep`.** A skip then costs nothing but the next `Pop` (which
  blocks on an empty cache, so no busy-spin), and skips are self-limiting. Do **not** ask for an extra scan
  on the skip path - measured at a 0ms interval, it fed an already-spinning refiller (throughput 1,632 -> 941).
- **EVERY path that re-offers a step to THIS replica must `RelinquishClaim` first**, since the window outlives
  the step's previous dispatch. Four paths do: the recovery-defer reset and the `flow.Retry` rewind (both
  `dispatch.go`), `deferNewerPayload` (`payloadver.go`), and **`enqueueStep`** (`operations.go`), which covers
  every cold re-offer site (surgraph revive, a lost child re-arming its caller, `deliverSubgraphError`, the
  resume leaf, Fork's leaf). `enqueueStep`'s is the easy one to miss, because only the empty-partition `Offer`
  exposes it: a revived caller is popped at once and skipped until the window ages out (189 of 500 flows in
  `fixtures/completionraceflow_test.go` failed to drain in 30s without it).

**The partition divides DISPATCHERS, not the pool divisor** - counted per shard from `dwarf_peers.dispatched_at`
freshness, which is EVIDENCE that a piston turned rather than a claim of intent (`internal/peers/CLAUDE.md`
owns the evidence rules: registration does not stamp it, the busy term is duration-qualified, and the count is
the minimum of the piston's two loops). Over-counting the pool divisor collapses a database, while over-counting
dispatchers strands work, so the pool errs toward keeping a replica and the partition toward dropping one. A
registered replica that dispatches nothing on a shard must get no residue class there:
`fixtures/crossreplicaawait_test.go` (an await-only replica that registers) hangs if it does.

**Everything fails open.** Solo dispatcher, unknown ordinal, an ordinal out of range, a Sonar gone blind, or no
Sonar at all - each disables partitioning on that shard, because partitioning EXCLUDES rows: a wrong pair
strands a residue class, while declining only restores overlapping selection. The pair is published as ONE
value, so a reader never sees an ordinal against the wrong count, and both come from one shard's rows.

**The doorbell is deliberately NOT partitioned** - it is gated only by role (see "Dispatchers"). Partitioning
it would delay exactly the sequential-hop case it exists for; admissions are bounded by vacated slots, and the
worst case is one lost claim against the owner's fetch. It concerns **only this replica's own** origination
sites: there is no cross-replica `enqueue` - **do not add one**, since a peer's offer is the one case that
genuinely races the residue class's owner.

**Known residual: replica death strands a slice for up to the dispatch window (5s) plus a read cadence**, until
its row ages out and ordinals re-seat. Self-healing, and accepted rather than covered by an age-based fuse (a
threshold above normal queueing delay - seconds under load - would not fire when needed, and below it would
disable partitioning under exactly the load it is for).

**A replica that is SLOW rather than dead keeps beating and keeps its class, so the partition alone strands
its steps; `internal/piston`'s STEAL is the answer** (measured: a crippled peer capped a fleet commanded to
500 steps/s at ~150, worse than removing it; with the steal, ~460-500). The mechanism and measurements are in
`internal/piston/CLAUDE.md`. Three things stay the engine's:

- **The pair is still `partitionOn(shard)`** - the steal relaxes its predicate, not how it is derived, and every
  fail-open case still disables partitioning outright.
- **`dwarf_steps_stolen{shard}` is the operator's signal that a peer is alive but not serving its share** - zero
  in a healthy fleet by construction (the grace blocks a fleet that is keeping up); read
  `dwarf_steps_claim_lost` against it, since stealing trades exclusivity for coverage.
- **The doorbell bypasses all of it, so a fixture must create from an await-only (`SetWorkers(0)`) replica to
  test any of this**: a chain created on a dispatcher walks hop by hop there and never consults a class. The
  fixtures keep the production doorbell and still exercise the steal.

### Reducers come from the graph; `changes` holds every kind of write

**Every place that combines writes reads the reducers from the flow's frozen graph** (`graph.Reducers()`), and
no row stores them. Dispatch hands them to the carrier (`flow.SetReducers`, the error-route carrier too, so its
`onErr` composes onto the earlier runs' writes); every fold takes them as an argument; the interrupted
`Snapshot` loads the graph (`flowGraph`) to fold the step's changes. A reducer stored on a row could go stale
or be forged by a remote host; one read from the graph cannot. Pinned by `TestChanges_ReducersComeFromTheGraph`
(including a remote host returning other reducers), `TestChanges_InterruptedSnapshotFoldsThroughTheReducers`
and `TestChanges_ErrorRouteComposesThroughTheReducers`, each failing with its site's reducers removed.

- **The `changes` column is `workflow.Changes` JSON** - contributions under their names, overwrites under
  `name!`, additions under `name+` (`workflow/CLAUDE.md`). Read it with `changesOf`, never `workflow.NewState`: a
  plain decode reads an overwrite as a field named `name!`. An unreadable column is a `permanentError`, never no
  changes: read as none, every fold, final state and re-dispatch built from it drops the step's writes silently
  (`TestChanges_UnreadableColumnIsAnErrorNotNoChanges`). Write it with `json.Marshal`. The state-refs anchor
  loader is the one raw reader, and `internal/staterefs` looks up `field!` before `field` (and `field+` for a list
  entry reading `changes`).
- **A task's result is checked, not trusted** (`checkChanges`): no name ending with either suffix, no overwrite
  or delete of a reduced field inside a fan-out branch, and no addition but one the engine could have recorded
  (no reducer, outside a cohort). Each fails the step, naming the field. Pinned by
  `TestChanges_RemoteResultIsChecked`.
- **An addition is recorded at persist time, outside a cohort** (`recordAdditions` in `processStep`, before
  `changesJSON`): an addition is relative to the step's own input, and the fan-in folds a member onto the spawn's.
  The persist turn is already held there, so only a byte compare runs under it (the input array, minus its closing
  bracket, as a prefix of the new one); decoding both arrays, when that misses, runs with the turn handed back
  (`yieldTurn`) - measured ~2.8 ms per step at 100 respelled 1 KB messages and ~27 ms at 1,000, too long to hold a
  connection's turn. Its field is a fold like an `append` contribution (`foldedFields`, `appendFolds`,
  `cohortAppendFolds`), so it is a list or inline and never anchored at the step. `fanInBase` resolves its base
  (`TestArrayRefs_FanOutSourceAddition` loses the history without it), and a branch that writes the field replaces
  the spawn's addition at the fan-in (`TestArrayRefs_BranchRewriteOfASpawnAddition`, a same-length rewrite the
  list's count check cannot see). `Step` shows the whole value written (`changesView` applies it onto the step's
  state). Pinned by `TestRecordAdditions`, `TestArrayRefs_ReplaceLoopStoresOnlyWhatEachStepAdds` and
  `fixtures/arrayrefsflow_test.go`'s replace loop.
- **`Step` shows a step's changes as a `State`** (`changesView`): an overwrite as its value, a delete as a
  null.
- **`state_reduced` holds a fan-in's `reduced` record and nothing else** (see "A member's contribution is its
  `changes`, EXCEPT an inner fan-in's" above).

### State refs — a large carried field is stored once (`internal/staterefs`)

A step's `state` is a full input snapshot, so a field that is *carried* but not *changed* is re-serialized into
every row it survives into: D copies down a chain of depth D, and **N·D across a fan-out of N branches**. A field
above a size bar is therefore not written into the successor's `state` at all: it is omitted, and the
`dwarf_steps.state_refs` column records `{"<field>": <anchor step_id>}` — the step that physically holds the bytes
(or, for a forEach element, `"<anchor> <array field>[<index>]"` — an item ref into the array's anchor, with one
index per level when a nested array is fanned out level by level).
The doc-extraction shape (a document fanned out over pages, then chunks) measures **~29x** fewer stored state bytes.

**The mechanism, the size policy, and the invariants that cost bugs to find live in `internal/staterefs/CLAUDE.md`
— read it before changing any of them.** Three things stay the engine's, because each is engine state rather than
ref policy, and they are what `engine/staterefs.go` holds:

- **Which Linker.** One per shard, built in `initRuntime` from the shard's own handle; the driver (the policy's
  one dialect-dependent term) is a property of that handle. Every Linker reads through the replica's one
  `stateCache`, keyed by shard because a `step_id` is only unique within one (`internal/staterefs/CLAUDE.md`,
  "One Linker per shard, one Cache per replica").
- **How an anchor is read.** `anchorLoader` turns a shard handle into the `staterefs.Loader` the Linker calls once
  per resolve. It **may be a transaction**, and at the populated fan-in (`fanInRow`) it is: anchors never cross a flow and a
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
what makes the write-side win free of round-trips. Three cases per key: a literal drops the ref (and may
re-anchor here, unless it is a fold's result - see the reducer section above), a delete drops field and ref,
absence carries the ref forward.

**Engine-side invariants** (the storage-side ones are in `internal/staterefs/CLAUDE.md`):

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
everything accumulated. A field is materialized iff the graph registers a reducer for it — a safe superset — and
the spawn did not overwrite it: an overwrite replaces the base, so reading it would be a wasted anchor read inside
the transaction. A
merely *carried* ref is passed through untouched, still pointing at its original anchor: materializing it at every
fan-in would re-anchor the payload and hand back the win in exactly the fan-out graphs the design exists for. The
fan-in mints against the **spawn** step, while member-contributed fields and the spawn's own folded fields are
inlined into the fan-in's own `state`.

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

**The forEach ordinal context is never ref'd** (`<as>Index`/`<as>Count`) — the engine synthesizes it per branch,
so its bytes are in no step row and a ref to it would dangle. The engine passes those names as the mint's
`inlineOnly` set, which is a distinct signal from a field appearing in `changes` (see `internal/staterefs/CLAUDE.md` — a nil
tombstone cannot substitute for it). **The element itself (`<as>`) is passed as the mint's `Item`** (its key, the
`forEach` field, its index): its bytes are one element of the source array, so when the array is stored by a
plain ref the element becomes an *item ref* into the same anchor rather than a second copy of the array's bytes
spread across the branches. Each branch step carries it forward, so no step of the branch copies the element.

Pinned by `internal/staterefs` (the tiers, both-column resolve, one-hop assert, the batched load, the
bytes-not-values cache), `engine/staterefs_test.go` and `engine/itemref_test.go` (the fan-out, fan-in and item-ref
storage assertions against a real database) and `fixtures/staterefsflow_test.go` (end-to-end: a carried document across fan-out/fan-in, `Continue`,
`Fork`, overwrite and tombstone).

### Payload format versions (`payload_ver`, `payloadver.go`)

`dwarf_flows.payload_ver` is the format of every payload column of every step row of the flow (`state`, `changes`,
`state_refs`, `state_reduced`), and `payloadVer` is the newest format this build reads - and the one it creates
flows in. It is what tells **"newer than me"** (a newer replica will read it: wait) from **"corrupt"** (none will:
`Parse` and friends fail as they always did).

**The version is the FLOW's, fixed at creation, and every writer writes into a flow in the flow's format.** That
is the whole design, and it is what makes the read side one check. A step reads far beyond its own row - its
anchors through refs, its cohort at the fan-in, the flow's tails at completion - and every one of those rows
belongs to the same flow (anchors never cross a flow; a cohort is one flow). So "this replica reads the flow's
format" is decided once, at the claim, and holds for everything the dispatch reaches afterwards.

- **Stamped at creation, never raised.** `insertFlowTx` (`Create`, `Continue`, subgraph spawn) stamps `payloadVer`;
  Fork's clone copies its origin's, since it copies the rows. **Do not let a newer replica raise a live flow's
  version when it writes**: an older replica that passed the claim check could then meet the newer rows mid-
  transition - at the fan-in, the anchor resolve, the tails - and none of those can wait. **Do not move the version
  onto the step either**: a step's version says nothing about its anchors or its siblings, so every read site needs
  its own check, and the fan-in's must run after a pure arrival's terminal bail or it rolls back a cohort bump the
  bail must commit.
- **Separate flows are separate formats.** A subgraph child, a `Continue` turn and a Fork clone of a subgraph
  each carry their own. What crosses between flows - `in`, `final_state`, `subgraph_result`, `Continue`'s
  initial state - is plain JSON, readable in any format, so a v1 parent may run a v2 child.
- **Dispatch defers; the other readers refuse.** The claim reads the flow's `payload_ver` (the joined flow row
  on Postgres, the flow read on the other dialects - free on both), and a newer flow sends the step to
  `deferNewerPayload`: back to `pending` with a doubling backoff (1s to 1min), fenced on `lease_seq` and guarded
  to `running`, before anything decodes it. The reads of a flow's steps that are not one of its dispatches -
  `Snapshot` (interrupted), `Step`, `Fork` and the final-state tails (Terminate, the wedge sweep, a `failStep` past
  the bound) - cannot wait and return a `permanentError` via `checkPayloadVer`. Fork checks twice: the fork step's
  flow at the leaf read in `forkFlow`, before the leaf is decoded, and every cloned flow in `cloneOneFlow`, since a
  tree can mix formats. Permanent is load-bearing: `terminalFinalState` and `failFlowTx` take it as `{}`, so a
  newer flow can still be stopped. The tails query joins the flow row for its version rather than paying a read.
- **Nothing routes a deferred step to a newer replica.** It keeps its `step_id`, so it stays in this replica's
  residue class, and stealing never fires because its owner keeps taking it. It moves only when the fleet does:
  the old replica leaves `dwarf_peers` and the classes re-divide. So the bound is sized to a rolling upgrade
  (`maxPayloadDeferrals` 65, at least an hour), not to a number of re-claims. Past it the step fails naming the
  format. A fixed schedule, not a setting: too long only delays a broken rollout's failures, too short fails a
  step a newer replica could have run, and every replica would have to agree on the value.
- **Deferrals are not attempts.** `attempt` reaches the task as `flow.Attempt()`; counting deferrals there would
  show a task attempts that never ran and trip its own give-up policy. Hence `dwarf_steps.payload_deferrals`,
  which Fork does not copy. It counts over the step's whole life, never reset: a step that runs between two
  rollouts (a `flow.Retry`, a resumed interrupt) has less of the hour left in the second.
- **Do not filter on `payload_ver` in the claim predicate.** An unclaimable step never reaches the deferral, so
  the bound never fires and the step strands silently. Claim-then-defer costs one claim per backoff, during a
  rollout only.
- **Do not create flows in the fleet's oldest format instead** (peers advertising what they read in
  `dwarf_peers`, writers holding back to it). It would let a rollout run with no waiting at all, but every format
  change would then have to keep its predecessor's writer alive for as long as an un-upgraded peer might exist,
  fleet-wide; the per-flow rule keeps an old writer alive only for the flows created before the change.
- **Do not put the version inside a payload column** (`state_refs` included). Those are keyed by state field
  names, which are unconstrained, so a version key collides with a field of the same name.

**Introducing payload format 2 (or any next version) - the checklist.** The mechanism above is built and tested
at version 1; the release that first needs it does all of this:

1. **Decide whether the change needs a version at all.** It does if a build at the current version would misread a
   row holding the new form - read it as something else, drop it, or fail on it. A change every existing reader
   already handles (a column it ignores, a form it already parses) does not.
2. **Bump `payloadVer`.** New flows are created at the new version from then on.
3. **Gate every writer of the new form on the FLOW's version**, not on `payloadVer`: a build at version N keeps
   writing version-M rows (M < N) into flows created at M, for as long as such flows exist - and an interrupted
   flow can rest for months. The writers and where each gets the flow's version:
   - **everything a dispatch writes** - the completion, retry and interrupt writes, successors, fan-in rows,
     `fireFanInDirect` - from the claim (`claimRow.flowPayloadVer`);
   - **Fork's leaf rewrite** (`mergedLeafState`, `forkLeafReduced`, computed in `forkFlow`) from the leaf read
     there;
   - **Fork's ref remap** in `cloneOneFlow`, which re-marshals every kept step's `state_refs` with this build's
     encoder, from that function's per-flow read - a respelled ref form would otherwise land in a clone stamped
     with the old version;
   - **a new flow** (`insertFlowTx`) is always written in `payloadVer`, so its first rows need no gate.

   Emit the old form below the new version. For an additive form this is one
   condition per mint site; a change to the meaning of an existing encoding needs both writers kept.
4. **Keep every reader able to read every version it may meet**: N reads 1..N. A reader never needs the version to
   decode an older row if the new form is additive; otherwise branch on the flow's version.
5. **Test at both versions**: a flow created at N-1 runs to completion on an N build, writing only N-1 forms (pin
   it by reading the rows back), and an N-1 build defers an N flow (`TestPayloadVer_NewerFlowIsDeferredUnrun`
   simulates that by raising the column).
6. **Say in the release notes** what a mixed fleet does: steps of flows created at N wait, up to an hour, for a
   replica of the new build, and a rollback below N stalls, then fails, those flows.

**Protection starts one release late.** A build predating the column ignores it, so the first version that can
defer anything is a release after the one that shipped the column.

Pinned by `engine/payloadver_test.go`: the deferral (task unrun, `attempt` untouched, backed off, then run once a
reader is in place), the bound (failing with an empty final state), `Snapshot`/`Step`/`Fork` refusing (a newer
subgraph in the tree included), `Terminate` still stopping the flow, Create stamping and Fork copying the version,
and the schedule's arithmetic. The deferral write's `lease_seq` fence and `running` guard are not pinned
separately; they are the same predicate as the retry rewind's.

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
  the fan-in (by an edge of ANY kind - a branch reaching the fan-in only by `Goto` or `onError` is an exit too, or
  its step stays a tail and `final_state` loses the fan-in's output; `fixtures/overridefaninflow_test.go`) **AND `successor_id == 0`** - **not** the whole lineage, so `A`/`B` in `forEach->{A->B->C}->J` are
  excluded and only the `C`s point at `J`. The `successor_id == 0` conjunct is load-bearing, not incidental: a
  branch that LOOPS through its exit task via `flow.Goto` (`forEach->{Work -Goto-> Work -> J}`) leaves several
  `Work` steps in the cohort, but only the last transitioned to the fan-in - the earlier iterations already wrote
  a forward edge to their next loop step. A step that transitions *into* a fan-in inserts no successor of its own
  (the fan-in step is created here; an earlier arriver's transition was only a counter bump), so its `successor_id`
  is still 0, while a loop iteration or a multi-step branch's interior step set `successor_id` when it created that
  successor. Without the conjunct, the task-name match alone overwrote those interior edges with the fan-in id,
  corrupting the recorded DAG (spurious `step -> fan-in` edges in `History` and its rendered diagram). Pinned by
  `TestFanIn_GotoLoopBranchKeepsInteriorDAGEdges`. The `successor_id` write targets the exit steps **by primary
  key** (ids collected from `queueCohortRead`'s cohort-member read), not by the
  `(flow_id, lineage_id, task_name)` predicate - which is unindexed and deadlocked concurrent fan-ins on SQL Server
  (the deadlock story is at that write in `dispatch.go`).
- **flow.Retry**: rewinds the step in place (same row), so `predecessor_id` is preserved. (A `Fork` copies the
  step into a new row and remaps `predecessor_id` to the cloned predecessor.)
- **Entry / subgraph-entry steps**: `predecessor_id` defaults to 0.

The Mermaid renderer ignores `step_depth` and `lineage_id`: it draws the deduped union of `{predecessor_id -> step}`
and `{step -> successor_id}`, exact for arbitrary nesting. Heads are nodes with no incoming edge, tails with no
outgoing.

**A failed fan-out's terminal state is rebuilt the way the convergence would have built it, not from the tails.**
When the merge base tail is a cohort member (`lineage_id != 0`), the cohort resolved with failures and never reached
its fan-in, and `computeFinalState` switches to `mergeCohortState` - the *same* merge `fanInRow` runs: the
**spawn** step's `state + changes` as the base, then every **completed member's** `changes` folded through the
reducers in `fan_out_ordinal, step_depth, step_id` order. Merging the tails instead loses data, because **a branch's
intermediate output lives in the NEXT step's `state`, not in any tail's `changes`**: in
`forEach -> Cell -> Enrich -> Join`, only the *first* tail's `state` is consulted, so branch 0's `Cell` output rides
in via the base and branches 1..N-1's is silently dropped (measured: 1 of 3 branches surviving, and *which* one is
arbitrary). The spawn's row is the only base common to the whole cohort, and every member's `changes` is exactly the
set of per-branch outputs - every step of a per-element sub-pipeline inherits the spawn's lineage, so one indexed
scan covers `Cell` *and* `Enrich`, for every branch. Reducer order matters (`append`/`union`/`concat` are not
commutative), which is why the scan is ordered by the branch's position in the spawn loop rather than by completion
order. Pinned by `TestFailedFanOut_KeepsEveryBranchesIntermediateOutput`.

**Nested, the rebuild starts at the OUTERMOST cohort on the tail's lineage chain** (`outermostSpawn`), whose
spawn is on the trunk. Do not rebuild from the tail's own cohort: the first completed tail can sit deep inside
an inner cohort that failed, and that cohort alone is not the flow's state - its base and every other branch at
every level are outside it. Members contribute as at a convergence (`cohortContributions`): an inner fan-in
brings its projected part whatever its status, and a completed member that spawned an inner cohort which never
converged (`cohort_arrivals < cohort_size` or `cohort_failures > 0`) is followed by **the part its fan-in would have
carried** (`simulatedFanInPart`): the same base as `fanInRow` (`fanInBase`), the same fold (`foldCohort`), the strip
of that cohort's own names, and the same projection a real fan-in's row gives (`projectReduced`). Only the fan-in
task is missing. **Do not hand the inner members' raw contributions to the outer fold instead:** a member's write to
its own cohort's `as` name then survives where a convergence projects it as a delete. That costs, per such cohort, a read of
the inner spawn's row, one read of its members, and any anchor reads its reduced fields' refs need - on this
terminal path only. Pinned by
`TestForEachStrip_FailedNestedFanOutMatchesConvergence` (three levels, failed at the innermost and the middle
level, with and without a write to an inner `as` name; it fails against either shortcut above).

`computeFinalState` also reads the DAG, not `step_depth`. The terminal state is the merge of the tail steps -
completed steps with `successor_id = 0` (`mergeTerminalSteps`) - for a flow that is *not* a failed fan-out. **Do not
pick the terminal step by `MAX(step_depth)`**: it is wrong for
any graph where a `flow.Goto` self-loop sits inside a fan-out: each loop iteration pushes
`step_depth + 1`, so the looping branch can outrun the fan-in/terminal step in depth, and `MAX(step_depth)` selected
the dangling loop step (empty state). The tail-step merge is depth-agnostic: loop iterations carry
`successor_id = <fan-in step>` (set by the cohort-exit UPDATE), so only the real terminal step qualifies. Two-tier
and depth-free: the completed tail (`successor_id = 0 AND status = completed`) for a normal finish; if none, the
non-completed tail (`successor_id = 0`, any status) for a flow force-terminated by `Terminate`/`failStep` before any
step completed. An empty map is returned for a flow with no steps.

### Time Budgets

Each step has a `time_budget_ms` that bounds the `ExecuteTask` call's context deadline. It defaults to the engine's
`SetTimeBudget` config (default 2m) but is **per-flow overridable** via `FlowOptions.TimeBudget`: the value is
resolved at `Create`, frozen onto the `dwarf_flows.time_budget_ms` column, and denormalized onto every step's
`time_budget_ms` (the entry step at `Create`, fan-out/fan-in steps from the flow-row value read in `processStep`).
The graph still carries no per-task timing - the budget is a per-*flow* default every step inherits. A host that
wants a tighter per-task bound enforces it inside its `ExecuteTask` (or the task itself), shortening the call context;
narrowing the deadline at dispatch is always allowed. The engine's budget is the outer ceiling.

It is immutable for the flow's life, like `priority`, and **inherited by subgraph children** (`createSubgraphFlow` reads the parent's `time_budget_ms` alongside priority/fairness) and by
`Continue` (from the flow it continues, since it takes no `FlowOptions`). A later `SetTimeBudget` change does not
retro-edit existing flows; it only seeds flows created afterward. On a subgraph spawn the child's `LoadGraph` is
bounded by the caller flow's budget; the create-time `LoadGraph` runs on the caller's own request context instead.

**No engine-imposed SLA ceiling** on `FlowOptions.TimeBudget`, for the reason it owns no flow-level deadline: a host
wanting an SLA cap rejects an over-limit budget before `Create`, and a caller setting a very large one owns the
consequences below. The only bound is storage: `maxTimeBudget`, just under 2^31-1 ms, because the claim's
`time_budget_ms + ?` is computed as a 32-bit integer on Postgres.

The worker lease is sized from the **step's own `time_budget_ms`** + `leaseMargin` (30s), written self-referentially
in the claim CAS (`lease_expires = DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?)`, only the margin a bind param), so
the lease always outlasts the budget that bounds the `ExecuteTask` call - even for a flow that overrode its budget
above the engine default. Sizing from the row (not in-memory config) is what keeps lease and budget from diverging;
it needs no upfront SELECT because `time_budget_ms` is already on the step row at claim time, the same read-locality
reason `priority`/`fairness` are denormalized there. Consequence: a *crashed* worker is recovered no sooner than its
step's `budget + leaseMargin`, so a flow's budget directly bounds its worst-case crash-recovery latency - which is the
practical reason a host caps the budget. **Do not size the lease from engine config**: a config-sized lease lets a
mid-flight `SetTimeBudget` decrease expire the lease under a still-running step and re-dispatch it; each step's
lease follows its own frozen budget.

### Persisting a step's outcome: retry the WRITE, never the task (`persist.go`)

The task has **already run** when its outcome is persisted, so this write is the only record that it ran; an
unhandled database error here leaves the step `running` and looking healthy while lease recovery re-executes the
task every `budget + leaseMargin`, forever (`persist.go`'s header has the loop; reproduced with a `\u0000` in
state on Postgres).

The rule is: **retry the WRITE, never the task.** Re-dispatching is the one recovery that re-fires side effects,
and it must not be used for failures the task had nothing to do with. So **every post-execution write of a
step's outcome runs through `persist`** (`persistStepYielding`, or `persistYielding` + `settlePersist` where the
caller acts only on a landed write): the completion write, the Cancel-covered reroute's follow-up write, the
transition, `fireFanInDirect`, flow completion, the `flow.Retry` rewind, the subgraph spawn and `handleInterrupt`. `failStep` is the exception by design: it is
the classifier's own clean, payload-free write (below), so it must not route back through the classifier. A new
outcome write added outside `persist` reopens the loop above. A test fails the rewind
through it with `FaultRetryRewindErr` and the reroute write with `FaultRerouteWriteErr`, as `FaultPersistErr` does
the completion write.

- **In-place retry, holding the lease:** an exponential ladder (1s doubling to 32s, a minute in all), because the
  errors it exists for clear in seconds to tens of seconds (`persist.go`'s header). A longer one is slow to report a
  permanent failure.
- **The lease is renewed before each retry**, to the backoff plus `persistLeaseExtension` (60s): half for the write,
  half for `failOnPersistError`, whose `failStep` reads the final state. Without it, a task that consumed most of its
  budget has only `leaseMargin` left, so sleeping through the backoff would put the worker *past* its lease: a peer
  re-claims, **re-executes the task**, and the late write is fenced away - exactly the re-execution this prevents.
  The renewal is itself a fenced, payload-free write, so a **zero-row** match means the step is no longer ours and we
  abandon silently. Renew per retry, not once: a single renewal cannot cover a minute-long ladder of slow attempts.
  **A renewal that errors does not end the ladder** - a failover fails the renewal too, and breaking to the
  classifier would cost every blip a re-execution (`TestPersist_FailedRenewalStillRetries`).
- **What the ladder does not cover: a write that takes longer than ~30s every time** (measured: 300 MB rows rejected
  after ~25-70s). It outlives the lease, lease recovery re-runs the task, the classifier's `failStep` is fenced into a
  silent no-op, and the next run repeats it. That is a payload far past any sensible size on a link too slow for it;
  it shows as `dwarf_steps_recovered` climbing and a `logLostLease` warning (a warning, because a write that landed
  with its acknowledgement lost, or a racing Terminate, also loses the step and nothing runs again). Do not predict a
  retry's duration to stop the ladder early instead: it fails a step that a later retry would have landed, and it
  needs per-dispatch timing carried through `processStep`.
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
- **`failStep` fences on the state the failed write STARTS from** (`stepHeld`, passed by each persist site), not on
  `running` or `completed`. A write that committed but reported an error has moved the step out of it, so `failStep`
  matches nothing and lease recovery settles the landed outcome without re-running the task; the broad guard
  matches a landed completion and fails a flow whose outcome is stored. Flow completion writes nothing to the step
  row, so an ambiguous one still fails the step; the flow stays completed, and `failFlowTx`, whose flow UPDATE then
  matches nothing, returns no loss - no failure delivered to a parent, no second `dwarf_flows_ended`. Pinned by
  `TestPersist_LandedWriteReportingAnErrorIsNotFailed` (`FaultPersistLandedErr`) and `TestFailStep_HeldStates`.

The two attempts are milliseconds apart and differ only in what they carry, which is what makes the classification
sound. It needs **no per-driver error taxonomy** (the part that would rot). Guessing the other way - terminalizing
on any unknown error - would kill live flows on every routine failover, and a terminal flow is **immutable**
(recovery is a human running `Fork`).

**Lock contention is excluded, and that exclusion is load-bearing.** It is retried to exhaustion before it
reaches `persist` (`Transact`, or `retryContention` for a write outside a transaction - which every such persist
write must use; without it `TestPersist_CompletionWriteContentionIsAbsorbedWithoutReExecution` ran the task 4
times). Past exhaustion contention reaches the recovery **defer** (rewind + re-poll), never the classifier -
terminalizing a flow because the database was busy is exactly backwards, which is why `processStep` tests
`IsLockContentionError` *before* `failOnPersistError`. So the defer's `completed→pending` arm is reached only by
contention or an unwritable classifier, which is what `TestLeaseFence_RecoveryResetFenced` drives it with.

**A fuse that can itself be poisoned is not a fuse.** `failStep` is the only way out, and it reads other steps'
payloads via `computeFinalState`. If an unreadable value had already landed in a row, that merge would die the same
way, `failStep` would error, and the classifier would misread it as "the database is down" - restoring the very
loop this closes. So `computeFinalState` failing on this path falls back to `final_state='{}'`, and the error
message passes `sanitizeErrorMessage` before it goes into a text column: control bytes and invalid UTF-8 replaced
(Postgres rejects both in `text` as in `jsonb`, so a driver error quoting the offending value back could kill the
clean write), and cut to `maxErrorBytes` (MySQL's `TEXT` refuses a value over 65,535 bytes; measured on MySQL 8,
an uncut 100 KB task error left the step for lease recovery and the task re-running).

Two counters: `dwarf_steps_write_retried` (a blip absorbed - the database is flaky, the workflow is fine) and
`dwarf_steps_write_failed` (an **alarm**, like `dwarf_steps_unwedged`: the database was reachable and the outcome
still could not be stored, so the payload is at fault and a latent bug exists). Pinned by `engine/persist_test.go` -
a transient error absorbed with the task running **once**, a permanent one terminalized with the task running
**once**, and a drain during the backoff releasing the lease instead of sleeping it out. Without `persist`, both of
the first two tests **hang forever**.

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
genuine `WHERE` filter (not a value-changing `SET`), so no `touch`-column trick is needed on steps - **but only
because every fenced step write changes at least one column on every row it matches.** MySQL's `RowsAffected`
counts CHANGED rows, not matched ones, so a fenced write that can leave its row unchanged - a `SET col=CASE WHEN
... THEN col ELSE ? END` whose fallback is the current value, or a flat bind of the status the row already has -
reports zero on MySQL and is read as a lost lease. The failure it produces: an escalation writing
`status=CASE WHEN status='running' THEN ? ELSE status END` onto an already-`completed` step matches the row,
changes nothing, reads "fenced", and returns without failing the flow - a permanent `running` orphan on MySQL
alone, invisible to the SQLite suite. Keep every fenced step write moving a value (in practice: the status), and
run anything touching these writes against a MySQL-family server (`fixtures/CLAUDE.md`).

**`lease_seq` is bumped where a lease is *granted* — the claim CAS — and where the orphan sweep takes a
`completed` step back.** The sweep's rewind (`recoverOrphanedFlows`, `completed`→`pending`) must bump it: the
worker it takes the step from is already past its completion gate, so without a new generation nothing would
stop a slow worker's transition committing beside the re-dispatch's. `recoverExpiredLeases`' expired-lease
reset (`running`→`pending`) leaves it untouched: the reset does not grant a lease, it only makes the step
claimable, and the next claim bumps the generation. Consequence: a step reset-but-not-yet-reclaimed still
carries the prior worker's generation, so that worker's write is *not* fenced — but this is the benign
"before a peer re-claims" case (the peer's claim CAS still arbitrates: it cannot claim until `pending`, and
once it does it bumps the generation and redoes the work correctly). The dangerous case — a peer already
re-claimed and is running concurrently — is exactly the one the bumped generation fences.

**One gate per path; downstream is protected by the gate, not separately fenced.** Each post-execution path
has exactly one fenced write to the dispatched step, and everything after it is safe:

- **complete / goto / fan-out / fan-in-direct / flow-complete** — gated by the completion UPDATE
  (`WHERE step_id=? AND status NOT IN ('terminated', 'cancelled') AND lease_seq=?`). Past it the step is `completed`,
  so no claim can take it (claim needs `pending`). The one thing that can is the orphan sweep's rewind, which
  bumps `lease_seq` - so the transition transaction carries ONE fenced write of its own, folded into a statement
  it already makes on the dispatched row: the successor-link/cohort-size write, the cohort bump
  (`bumpCohortArrivals`, a two-row statement), or `fireFanInDirect`'s successor-link/cohort-size write. Each also clears
  `advancing`; a zero-row match rolls the transaction back via `errLeaseFenced`, which `persist` turns into
  `errPersistFenced` and a quiet abandon. Each fenced clear also requires `advancing=1`, which fences one more case:
  persist re-running the transition after a COMMIT that landed but reported an error finds the flag cleared and
  abandons instead of advancing the flow twice. (On MySQL the cohort bump's fence is `RowsAffected`, honest there
  because every row it matches changes: arrivals rise, the flag goes 1 -> 0.) Pinned by
  `TestOrphanSweep_ReplaysSlowWorkersStep` and `..._OtherTransitions`, which fail with the fence removed.
  `completeFlowSequential` carries no fence, **because the re-dispatch is a replay**: it re-derives the slow
  worker's own decision from the recorded completion rather than re-running the task, so both reach "complete the
  flow" from the same state, and whichever commits second no-ops on the flow-row guard. Were the re-dispatch to
  re-run the task instead, a non-deterministic task could route to a successor the slow worker's completion then
  strands - a flow completed early, its decision from one run and its state from another. One transient remains:
  if the slow worker completes the flow while the step sits rewound, the step is `pending` on a terminal flow until
  a claim reaps it to the flow's status (seconds). Fencing the flow completion instead would close it at one more
  statement on every flow completion, for a case that needs a worker stalled past its lease; declined.
  `completeFlowSequential` in particular makes **no step write at all**: the gate already completed the step.
  **Do not add a trailing `UPDATE dwarf_steps SET status='completed'` there** - it re-writes the same value, costs
  a transaction on every flow completion, is a post-execution step write with neither a status guard nor a fence,
  and bumps the step's `updated_at` to *after* `completeFlow`'s transaction, inflating the recorded task duration
  (`History` and the `FlowRenderer` compute it as `updated_at - started_at`) by the cost of completing the flow.
- **fail** (`failStep`) — gated by the step-fail UPDATE, the transaction's first write, so a zero-row match
  wrote nothing: it commits the empty tx and returns `fenced=true`, and `failAndReturn` surfaces `nil` so the
  flow the peer is re-running is never failed. This closes the "late error → healthy-flow kill" case.
- **retry** and **subgraph park** — gated by their `status='running' AND lease_seq=?` UPDATE (both already
  bailed on zero rows); their in-tx follow-ups (`deleteSubgraphFlowsRootedAt`, child spawn) are protected by
  the gate's row lock.
- **interrupt** (`handleInterrupt`) — the leaf fence needs care: the combined step UPDATE must run **first**
  and lock the whole surgraph chain in PK order (matching `resume`'s lock order, so an interrupt and a concurrent
  `Resume` on the same chain cannot deadlock by taking its rows in opposite orders), so
  the fence cannot be the leaf's first write. Instead the combined UPDATE runs unchanged, then an **in-tx read
  of the leaf's `lease_seq`** decides: on mismatch the whole transaction **rolls back** (undoing the ancestor
  re-park, which would otherwise flip callers out of `parkedSubgraph` and strand the parent revive) via the
  `errLeaseFenced` sentinel, which `persist` turns into a quiet abandon - the interrupt is neither counted in
  `dwarf_steps_executed` nor signalled. The check reads the leaf directly rather than a
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
caller scheduling a `Terminate`. `Flow.CreatedAt()` and `Flow.UpdatedAt()` are populated on every dispatch, so the
elapsed-time guard is one call away inside any task.

### Transition Evaluation

Transitions are evaluated after a task completes successfully:

1. If the task called `flow.Goto(target)`, only the `goto` transition to that target is taken.
2. Otherwise, all non-goto, non-error transitions are evaluated: those without `when` always taken; those with `when`
   taken if the expression matches the merged state.
3. `forEach` transitions iterate a state array and spawn one task per element. Only an ordinary transition (empty
   `Kind`) carries `forEach`.
4. Multiple matches -> all taken in parallel (fan-out).
5. No matches -> the flow completes.

**Error transitions.** When a task returns an error, the failed task's `onError` transition fires and preempts any
other transition (an `error` transition carries no `when` or `forEach`, so it is unconditional). The error is
serialized as a `TracedError` into state field `onErr` **with its stack frames stripped** (`Stack=nil` on a
shallow copy, so the shared error object keeps its stack for logs) - `onErr` rides into `changes`->`final_state`
and is readable by a state-bearing flow reader (`Snapshot`'s `final_state`, `Step`'s `state`/`changes` - not
`History` or `List`, which carry no state), and internal stack traces are code-structure
disclosure; the handler still gets the message, status code, trace id and properties for routing. The handler task
becomes the next step, and the failed step is marked `completed` - **with the task's own changes DROPPED**. (The
`error` *column* already carries only `err.Error()` - the message, no frames.)

**An error voids the task's changes**, and this is the contract on **both** error paths: the `onError` route builds a
**fresh** `RawFlow` seeded with the input snapshot plus `onErr` (`dispatch.go`), so anything the task wrote with
`flow.Set` before returning its error never reaches the handler, the step's `changes` column, or `final_state`; and
`failStep` writes only `status`/`parked`/`error`, never the changes. The two paths agree, so the contract does not
depend on whether the author happened to declare a handler.

The rationale is the same one that governs everything else here: **execution is at-least-once**. If a worker loses
its lease mid-task, a peer re-runs the task from the same input snapshot and *recomputes* its changes - so "what the
failing attempt wrote before it died" is not a stable fact about the flow; it depends on which attempt you observed.
Preserving it would hand authors a record that looks dependable and is not, and compensation logic built on it would
pass its tests and be wrong under lease recovery. It matches Go's own convention (an error voids the other returns).
The deliberate channels for a task that has something to say to its handler: put it **in the error** (`onErr` carries
message, status code, trace id, and properties), or give an external side effect **its own task**, so its success is
durably recorded before anything downstream can fail. Pinned by `fixtures/errorchangesflow_test.go` (both error
paths, plus a success control proving the write is only lost *because* of the error). Fan-out siblings are **not**
cancelled - the errored branch
continues down its handler path and rejoins the cohort as a normal arrival (convergence is by cohort arrivals, not by
cancellation). If there is no `onError` transition, the step fails via `failStep`.

**Fan-out sibling constraint:** `Graph.Validate()` (via `validateLineage`) enforces that all branches of one
fan-out **converge on the same fan-in task**. The cohort shares a spawn step, and the cohort-resolution path in
`processStep` picks the fan-in target from whichever sibling completes *last*, so branches routing to different
fan-in tasks would make the convergence task depend on completion order (nondeterministic). The check walks a
local fan-out-to-fan-in mapping: `validateLineage` records each fan-out source's fan-in as branches pop the
lineage frame, and a second branch popping the same source to a *different* fan-in is the violation. (That map
is validation-only and discarded; the engine recomputes the same mapping for routing via `internal/faninmap`.)
(Divergent
non-fan-in *immediate* targets that still converge on one fan-in are fine - each sibling spawns its own next step;
only the shared fan-in target must be single-valued.)

**A fan-in arrival must have a cohort to arrive at, and this is enforced at BOTH layers.** `cohortSpawnID` is the
dispatched step's `lineage_id`, which is `0` for any step *outside* a cohort - so a trunk step routed into a fan-in
task bumps `cohort_arrivals` on `step_id=0` (zero rows, no error) and then SELECTs that step: `sql.ErrNoRows`, which
aborts the transition transaction. The `processStep` recovery defer then rewinds the just-`completed` step to
`pending`, it is replayed, and it fails identically - **an unbounded hot loop** that hammers the database and never
advances the flow (measured with a re-dispatch that re-ran the task: 2,735 failed transitions in 2 seconds).

- **`validateLineage` rejects the edges that produce it.** Its `switch` tests `g.fanInTasks[tr.To]` **before**
  the `tr.Kind != ""` arm - **the order is load-bearing**. The engine treats *any* transition into a
  fan-in task as a cohort arrival and does not care which kind of edge carried it, so testing the goto/error/switch
  arm first skips the frame-pop check for precisely the edge kinds that can reach a fan-in from
  outside a cohort. A goto into the fan-in from *inside* a cohort stays legal - that branch has a frame to pop.
- **`processStep` guards it at runtime** (`fanInArrivals > 0 && cohortSpawnID == 0` -> `failAndReturn`). Not
  redundant with the validator: a flow's graph is frozen at `Create` and replayed from the flow row on every
  dispatch, **never re-validated**, so a graph frozen by a build whose validator accepted this shape has only the
  guard between it and the hot loop.
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

The engine has no single-task front door: a bare task is only ever a task in a graph, never an independently
invocable child. To run one unit of work in isolation, declare a one-task workflow and `Subgraph` it. ("Subflow" is
the umbrella term for any child flow and is the name of the typed host client, not an engine primitive.)

**Into the child:** `SubgraphRequested` passes `subgraphInput` (the `NewState`-normalized `in`) directly to
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
eager terminalization on the first branch error. **Do not fail the child eagerly** (a `failStep` short-circuit
straight into `deliverSubgraphError`, bypassing cohort accounting): it strands the child's *other* live branches
and any subgraph descendants they parked on, because every tree walk skips a terminal flow (`Resume`'s down-walk
descends only `interrupted` children; `Terminate`/`allSubgraphFlows` stop at terminal nodes; the parked-caller
wedge sweep sees a *terminal* caller step, not `running`+`parkedSubgraph`), so the stranded sub-tree has no path
out but `Delete`. Deferring the child's failure to cohort resolution means every branch has settled (completed or failed)
before the child terminalizes, so there is no live sibling to strand - and a sibling parked on a grandchild that
*interrupts* propagates up normally, so the whole tree parks `interrupted` and a root `Resume` still threads down
to it (rather than the grandchild's approval being silently cancelled). `deliverSubgraphError` is used **only**
by the wedge sweep (a wedged caller whose child already went terminal); the live failStep path does not call it.
Defense in depth for any residual orphan is
`recoverOrphanedSubgraphChildren` (see "Background Recovery"). `fixtures`/`engine`
`TestSubgraphCohortFail_NoStrandOnBranchFailure` pins the child staying `running` after one branch failed while a
sibling is parked on a live grandchild, then converging to a clean terminal tree with the branch error surfaced
to the root. `fixtures/subgrapherrorwaitflow_test.go` pins the `Await(childKey)` wake: with an `Await` parked on a
still-running child, the child's failure fires the targeted `CheckpointFlowStopped` for `(childKey, failed)` and
the `Await` returns that failure. The checkpoint proves `signalStop` was RAISED for the child's key - it fires at
the top of `signalStop`, before `FaultDropSignalStop` and `Board.Release` - not that the release was delivered;
the `Await` returning cannot prove the signal either, because the detector finds the committed stop too, and no
time bound can tell the two apart. Delivery is pinned at unit level (`internal/latch`
`TestRelease_WakesWithoutASweep`), and the detector-only case by
`TestSubgraphChildStopSignal_DroppedThenBackstopped`. `fixtures/subgraphcohortfailwaitflow_test.go` pins the same
for the other settle path (a completing last-arriver resolving a failed cohort).

### Surgraph Step Identification

Each subgraph flow's row stores `surgraph_flow_id` *and* `surgraph_step_id` - the PK of the parked surgraph step
it belongs to. `completeSurgraphFlow` (and the interrupt/resume chain walks) look the surgraph step up by primary
key, so they can never match a sibling at the same `(flow_id, step_depth)`. This matters for: (1) a fan-in race
where a non-subgraph sibling at the same depth is momentarily `running`; (2) parallel subgraphs at one depth, each
parked at `parked=1`. The PK lookup keeps each child flow bound to the step that launched it. **Do not link by the
caller's step depth**: matching on depth is ambiguous across parallel callers at one depth; `surgraph_step_id` is
the sole, precise link.

### Denormalized root pointer (`root_flow_id`)

Every flow carries `root_flow_id`, the `flow_id` of the root of its subgraph tree, so a whole tree (a root plus
all its subgraph descendants, recursively) is reachable in **one query** - `WHERE root_flow_id=?` - instead of a
recursive `surgraph_flow_id` walk that issues one round-trip per tree level. It is **write-once at creation,
immutable** thereafter:

- A top-level flow (`Create`/`Run`/`Continue`) is its **own** root: `root_flow_id = its own flow_id` (written by the
  flow's INSERT on PostgreSQL and by a fix-up UPDATE in the same transaction elsewhere - see "Creating a flow").
  The own-id convention - rather than `0`-means-self - keeps the root's *own* row matching the membership scan with no `COALESCE`/`OR` predicate.
- A subgraph child **inherits** the parent's `root_flow_id` (`createSubgraphFlow` reads it alongside the inherited
  scheduling/budget and passes it into `createWithGraph`).
- A `Fork` clone is a fresh self-contained tree: the new root is its **own** root, and the fork's cloned subgraph
  descendants inherit the **new** root's id (`cloneOneFlow` stamps `cc.newRootFlowID`, set right after the root
  insert and before any descendant is taken off the worklist). The fork does **not** inherit the origin's
  `root_flow_id`.

Single-shard by construction: subgraph flows have parent-shard affinity, so a whole tree lives on one shard and
`root_flow_id` never crosses shards. (If that affinity ever changed, tree scans would need cross-shard fan-out.)

**Membership, not structure.** `root_flow_id` answers *which flows are in this tree* (the set); it does **not**
encode *who is whose parent* (the structure). So it **augments**, never replaces, the `surgraph_flow_id`/
`surgraph_step_id` links: `root_flow_id` is the **membership index** that loads the tree's rows in one scan, and
the surgraph links - read from those *in-memory* rows by the down-walks, and by primary key by the up-walk - are
what supplies the parent/caller/child *structure* every walk follows.

**Where it is used.** The three *down*-walks fetch the whole tree in one `root_flow_id` scan
(`WHERE root_flow_id = (SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)`) and derive their result **in
memory** by following the loaded `surgraph_flow_id`/`surgraph_step_id` pointers - the same result a
level-by-level recursion gives, at a fixed number of round-trips regardless of nesting depth instead of one or two
per level. The subquery resolves the
tree first, so each walk works whether its starting flow is the root or a mid-tree node:

- `allDescendantSubgraphFlows` (`Fingerprint`) - BFS *down* from the given flow over the loaded rows (any
  status). Delete needs no walk: it marks the root, and the reaper deletes by `root_flow_id`.
- `allSubgraphFlows` (the descendant set of `terminateSubtree` and of `Cancel`) - BFS *down* through
  **non-terminal** nodes only, stopping at a terminal node. Started from a mid-tree flow, as the orphan sweep
  does, it means "descendants of *this* node", not "the whole tree."
- `interruptedSubgraphChain` (Resume's *down*-walk) - one tree scan plus **one** batched query for every flow's
  interrupted leaf (`status=interrupted`, ordered by flow, then Snapshot's order - the recorded step, then the
  earliest `updated_at` - first row per flow taken in memory), then descends by `surgraph_step_id`. SQL does the
  ordering, so there is no fragile Go-side timestamp comparison (the leaf is the one Snapshot reports).

**The *up*-walk, `surgraphChain` (Resume, Fork, interrupt propagation), does NOT ride the tree scan.** It
follows `surgraph_flow_id`/`surgraph_step_id` up to the root by primary key, one round trip per level: each
query reads a flow's caller link joined to its parent's token and own link, so a top-level or depth-1 flow costs
one round trip. Do not put it on the tree scan to save the round trips at depth: the chain is as long as the
nesting is deep, the tree as wide as its widest fan-out, and every leaf interrupt walks the chain - so a scan makes
interrupting a fan-out of W subgraph leaves read O(W^2) rows (`TestWideflow/subgraph_tree_past_the_ceiling`, at
W=2,200, is where it shows).

`Fork`'s own tree-discovery uses `surgraphChain` + per-flow child queries while it walks its worklist (it needs the
structure to clone, not just membership), and `deleteSubgraphFlowsRootedAt` stays step-scoped (`surgraph_step_id`).

`History` assembly rides the same principle with its own two inline `root_flow_id` queries rather than a named
walk (it needs a *caller-step*-keyed child map with each child's `workflow_url`/`workflow_name`, a shape the
`[]int` membership helpers don't supply): one query builds `surgraph_step_id -> latest child flow`
(`ORDER BY flow_id`, last wins = the child the caller used), a second loads every tree flow's steps
(`WHERE flow_id IN (SELECT ... root_flow_id ...)`), and the nested history is stitched **in memory**
(`assembleHistory` recurses caller-step -> child). **Do not look children up per step** (`WHERE
surgraph_step_id=?` for each caller) - that is an N+1 over every step in the tree; `History` is a
fixed two round-trips regardless of nesting depth. (`idx_dwarf_flows_surgraph_step` backs the remaining single-row
`surgraph_step_id=?` lookups
elsewhere - the retry-reap, the wedge sweep, and `Step`-navigation.)

**Consistency.** Denormalized + write-once-at-create is low-risk, but a creation path that forgot to set it (or set
it wrong on a fork descendant) would silently drop rows from tree scans - and, because the down-walks ride
the same scan, mis-route a Terminate/Resume. `TestRootFlowID_*` (engine package, white-box) pins the three
population paths: top-level self-root, subgraph inheritance, and Fork self-root + non-inheritance; `Continue`
starting a fresh root. `fixtures/deepsubgraphflow_test.go` pins the walks themselves at depth 5: a leaf interrupt
propagating up to the root, Resume descending back to the leaf with state threaded through every level, and a
root Terminate tearing down the whole interrupted tree.

### Interrupt/Resume Propagation Across Subgraphs

**Interrupt propagation (up):** when a step inside a subgraph flow is interrupted, the engine uses `surgraphChain` to
walk to the root surgraph, collecting flow IDs and parked surgraph step IDs, then interrupts all flows and steps in
the chain with bulk `UPDATE ... WHERE flow_id IN (...)` / `WHERE step_id IN (...)`. This ensures the caller awaiting
the top-level flow sees `interrupted`.

**Resume propagation (both directions):** `Resume` walks up (`surgraphChain`) and down (`interruptedSubgraphChain`),
re-parks intermediate surgraph steps, records resume data on the leaf's `resume_data`, resets the leaf to `pending`,
transitions all flows to `running`, and rings the doorbell - all in one transaction. The down-walk picks each
flow's interrupted step by Snapshot's selection - the one the flow row records, then **earliest `updated_at`**
(`step_id` tiebreak) - so a Snapshot reports exactly the interrupt the next Resume resolves - and descends into the child subgraph spawned by
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
turn reads the prior flow's `baggage` column and carries it forward - `Continue` takes no options, so the
baggage always flows on), so a multi-turn conversation keeps the caller's identity across turns. A turn wanting
narrower context scrubs it in an entry adapter task, or starts a fresh flow with `Create` and its own
`FlowOptions.Baggage`.

**Delivery is context, authoring is `FlowOptions`.** Baggage is *set* explicitly and visibly (a typed
`FlowOptions` field on `Create`/`Run`) but *read* ambiently (off ctx), so the value the engine never
interprets is not a parameter on every callback and task handler. The engine injects it into the ctx it hands the
callbacks (in `processStep` for the per-step executor, and at the create-time `LoadGraph` call); the
`ContextWithBaggage`/`BaggageFrom` helpers live in the `workflow` package so task-defining code reads baggage
without importing `engine`; both are typed on `workflow.State` (delivery and read), while the *set* value
(`FlowOptions.Baggage`) stays an `any` the host fills with a struct or map. The create-time injection is
`workflow.NewState(opts.Baggage)`, which marshals the value once and keeps each field's raw JSON, so the loader
sees the same `State` every dispatch will; the dispatch and inherit paths read the `baggage` column the same way,
via `workflow.NewState`.

### Keys are capabilities, not authorization

A flow key (`{shard}-{hour}-{flowID}-{token}`) or step key (`{shard}-{hour}-{flowID}-{stepID}-{token}`) is an
**unguessable bearer capability**, not an authorization
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

**Token entropy (64-bit) and the every-gate-is-`flow_id`+`flow_token`+`created_hour` invariant that makes it adequate** live in
`internal/keys/CLAUDE.md`.

**Related engine-side hardening (all verified, none a substitute for host authz):** uniform not-found on
key mismatch (no existence oracle); telemetry carries only the token-free correlation id and there is no
correlation-id→key lookup (see "Tracing"); subgraph-child keys are read-only for lifecycle mutations (see
"Subgraph keys are read-only"). The engine cannot go further - it never sees the principal.

### Await

`Await` blocks until a flow stops (no longer `created`/`pending`/`running`); it returns on `completed`/`failed`/
`terminated`/`cancelled`/`interrupted`. Its shape is **read, then park and read until a read sees a stop**:
snapshot and return if the flow has already stopped; otherwise park on the **latch board** (`internal/latch`,
held as `e.latches`) until it settles, then snapshot to build the outcome. **The park repeats only when that
read finds the flow running again** - an interrupted flow Resumed between its release and the read - and the
caller's ctx is still live. Each iteration follows a release, never a timer, so it is not a poll (a sweep that
read the row before a Resume can release the re-parked caller once more for the same stop - bounded); returning
the running flow instead hands the caller a false 408 (pinned by `TestAwait_StaleStopReparks`). **Both reads map
an error caused by the caller's own deadline to the not-stopped outcome**, never return it raw: each takes a turn,
so a read can straddle the deadline on a saturated shard, and a raw error breaks `Poll`'s re-pollable answer and
`Await`'s 408 (pinned by `TestAwait_ReadAfterReleasePastTheDeadline`).

**Each read earns its place, and the post-release read is not a re-check.**

- **The first** is what lets an already-settled flow answer AT ONCE. Parking instead would be *correct* - the
  detector reports the current state of every parked key, so a stop that landed before the caller arrived is
  found anyway - but it would be found on the sweep's cadence, turning every `Await` on a finished flow into a
  wait of up to one full `latchSweepInterval`. Measured: removing this read **doubled** the `engine` package's
  test time (≈8-11s → ≈17-20s), while `Run` was unaffected (a `Run` caller parks long before its flow stops).
  Keep it.
- **The post-release read** runs only after a release, and a release is a promise that the row **reached a
  stop** (or stopped resolving). `signalStop` **enforces** it by dropping any non-stopped status rather than
  delivering it. Do not wake a caller for a status a flow is merely passing through: every such wake would cost
  the caller a read and a re-park for nothing. (Nothing needs such a wake anyway - nobody is parked on an
  `interrupted` flow, since that is a stop, and nobody can be parked on a key `Fork` has not returned yet.)

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
  **stop rate** instead, so the pass is not throughput-independent. See the pre-scan bullet below for
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
nothing and would cut short waits callers legitimately asked for. Note the consequence at the `Await` and `Run`
boundaries: when the budget (rather than the caller's ctx) expires, **`ctx.Err()` is nil**, so both wrap
`cmp.Or(ctx.Err(), context.DeadlineExceeded)` rather than tracing `ctx.Err()` - tracing a nil returns a nil error
beside a nil outcome. Either way the 408 satisfies `errors.Is(err, context.DeadlineExceeded)`. Pinned by
`TestAwait_DefaultBudgetIsADeadlineError`.

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
  (1s, say) buys nothing the next pass would not have caught. No clock term needs padding
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

- **Write-first transactions** - `processStep`'s transition transaction takes the flow row's lock
  (`lockFlowRows`) as its first operation (a pure cohort arrival defers that grab, and its cohort bump is the
  first write instead - see "Transactions"). On MySQL/Postgres this serializes concurrent workers. On SQLite with
  `cache=shared`, starting with a write avoids the deadlock where two read-first deferred transactions both hold
  SHARED locks and neither can upgrade. **Every lock-only grab of a flow row goes through `lockFlowRows`** (or
  `lockedFlowRows` in a batch), which returns the rows it locked, so a guarded grab
  (`WHERE ... status NOT IN (terminal)`) bails on zero. **On PostgreSQL and MySQL the grab is a locking read**
  (`FOR NO KEY UPDATE` / `FOR UPDATE`), not an UPDATE: an UPDATE writes a new row version even when it changes
  nothing, and the flow row is grabbed on every step, so the grab would be the row's only per-step write - a row
  version per step on a row that otherwise changes a few times per flow, which fills its page and pushes its
  writes onto new pages (see "`dwarf_flows` updates stay on their page only while it has room" in
  `internal/migrations/CLAUDE.md`). `FOR NO KEY UPDATE` is the lock a non-key UPDATE takes, so it conflicts with
  exactly what the UPDATE did. **On SQLite and SQL Server it flips the non-indexed `touch` column**
  (`touch=1-touch`): SQLite needs a write to take its lock, and SQL Server's `UPDLOCK` read is unverified under
  `READ_COMMITTED_SNAPSHOT`. Never bump `updated_at` to take a lock: it sits in
  `idx_dwarf_flows_status (status, updated_at)`, so it would move a running flow's index entry on every step, and
  it must stay the flow's last genuine *status* transition. `touch=1-touch` (not a `SET col=col` self-assign) is
  deliberate: it always changes the value, so `RowsAffected()` counts the match on MySQL, which counts *changed*
  rows. A `dwarf_flows` UPDATE whose matched count is read must change a value on every row it matches; the
  status-transition writes flip `touch` alongside `updated_at=NOW_UTC()`. (This applies to `dwarf_flows` only - `dwarf_steps` has no such per-step
  churn: a step row is short-lived and its `updated_at` moves track its own `pending→running→terminal` transitions,
  which are genuine and unavoidable.) **Every flow-terminating transaction must be write-first for the same
  reason**, and the failure mode is worse than a transient error: the terminal step is marked `completed` by
  `processStep` *before* the disposition runs, so if the disposition's `Transact` exhausts its retries and errors,
  lease recovery (which only resets `running` rows) can't re-dispatch the now-`completed` step - the flow strands
  `running` with every step terminal until the orphan sweep rewinds the step, minutes later. The fan-in transaction
  and `completeFlow` take the flow row first; `failStep` is steps-first, but its transaction still opens with a write
  (its step UPDATE). A high-volume soak (`fixtures/soakflow_test.go`) and `fixtures/completionraceflow_test.go`
  reproduce the wedge without write-first. This write-first rule governs the flow-*advancing*/*terminating* transactions only; the
  lifecycle mutations (`Resume`/`Terminate`/`failStep`/`Delete`) run the **opposite** (steps-first) order on purpose, so
  the two disciplines cross on row-locking engines - see "Transactions" for why that crossing is tolerated (retry-
  recovered) rather than reconciled, and do not "fix" one side into matching the other.
- **Busy timeout** - `sequel` applies `_pragma=busy_timeout(2000)` to SQLite DSNs without one, so concurrent workers
  hitting a write lock wait up to 2s instead of failing immediately with `SQLITE_BUSY`. Essential during fan-out.
- **Lock contention recovery** - `processStep` defers a check: on a lock-contention error
  (`sequel.IsLockContentionError`), it resets the step it had leased (`running` -> `pending`, `lease_expires=NOW`),
  so the step is claimable again; and when the step was already marked `completed` but the follow-up transaction
  (successors, cohort arrival, flow completion) failed to commit, it rewinds `completed` -> `pending` the same way,
  so re-dispatch replays the advance. The reset is load-bearing on its own: `recoverExpiredLeases` only recovers
  running steps whose lease has *already* expired, and a freshly leased step holds a minutes-long lease, so without
  the rewind the step (and its fan-in) would stall until the lease lapsed. It needs no paired re-poll - the
  step goes back to `pending` and the next cycle selects it. Guarded on the from-status, fenced on `lease_seq`
  once one was captured (so a zombie cannot rewind a peer's re-claim), and skips a parked step (`parked=0` in the
  predicate - a rewound park would be pending AND parked, which nothing selects or recovers).

### Database Choice and Configuration

Choosing a SQL dialect (Postgres/MySQL/SQL Server/SQLite tradeoffs under concurrent write load), the shard-count
guidance table, the shard-per-server production topology, and the per-server connection-budget reasoning all live in
`internal/database/CLAUDE.md`. The engine-side concern is the connection **pool sizing** — the *formula*, which is
engine policy — below.

**THE CEILING EQUATION, AND WHY THE POOL IS ONLY HALF OF IT.** A saturated shard serves
`M / (k·RTT + s)` steps/s: `M` connections divided by how long one step OCCUPIES one. `k` is the round
trips a step makes (a code-path property, counted per driver: a linear step is 5 on PostgreSQL, whose claim
returns the flow row and whose transition is one pipelined batch, 8 on SQLite, 9 on SQL Server, and 10 on MySQL, whose claim is two statements -
`completionRoundTrips` plus the claim and the flow read), `s` is the per-step time that is not a round trip
(server execution, WAL, queueing at the pool). Three consequences that govern engine changes:

- **Workers are NOT a term.** A worker inside `ExecuteTask` holds no connection, so adding workers cannot
  move this - it moves the queue. It is why a "grow the crew" fix for a throughput problem is always
  wrong.
- **`k` is the ONLY term the engine controls, and it MULTIPLIES RTT.** At 10 round trips, 0.1 ms of RTT
  costs ~1 ms of occupancy on EVERY step. That is what makes the round-trip collapsing work
  throughput-critical rather than cosmetic - the single-round-trip claim+read via `RETURNING`/`OUTPUT`,
  the `priority`/`fairness_key`/`time_budget_ms` denormalization that keeps selection off `dwarf_flows`,
  `enqueueStepDue` skipping the PK lookup, dropping `completeFlowSequential`'s redundant step UPDATE.
  **Adding a round trip to the hot path costs `RTT × throughput`, permanently. Price any new one this
  way.**
- **RTT is an operator fact the engine cannot see or influence**, and it is not small: two same-zone
  Cloud SQL instances measured **0.32 ms and 0.82 ms**, a **29% throughput difference** on identical
  hardware and build. Never treat an absolute throughput number as portable across rigs without it.

`k` does not vary with WORKLOAD SHAPE: fitted on PostgreSQL with an unpipelined transition, `linear` 9.11
against `fanout` 8.67 on one rig at one pool, a gap inside the fanout arms' own spread. The intuition that a fan-out costs more
per step is the wrong unit - it costs more per FLOW, while most of its STEPS are branches that insert no
successor. Constants, their portability, and the normalization formula are in
`bench/CLAUDE.md`; the operator-facing version is in `docs/benchmark-cloud.md` and `docs/deployment.md`.

### Connection pool sizing - a constant budget, ramped by distance, clamped by the server (`poolsize.go`)

Each shard has a fleet-wide BUDGET that its dispatching replicas divide (`shardBudget`, sized once per run at
Startup into `shardSizing`):

- **`ShardSpec.MaxConnections`, when set, is the budget exactly** - the operator's one lever, used as given
  with no ramp and no clamp. Declared fleet-wide, like the shard set.
- **Otherwise 250, ramping linearly to 500 between 0.25 and 1 ms of probed RTT, clamped to half the server's
  `max_connections`, floored at 2. SQLite takes a fixed `sqliteBudget` (8)** - one writer at a time, so more
  connections only queue on its write lock, and it reports no limit to clamp against. An unprobed shard takes
  the near budget (a failed probe must not inflate a pool). `idle = open`; `ConnMaxIdleTime` sheds a pool load
  has left (an idle cap below the ceiling churns connections).

  **Do not size the pool per vCPU, and do not make the budget a fraction of `max_connections`** - the
  measurements behind both, and behind the far end of the ramp, are on the constants in `poolsize.go`. Do not
  cap the pool on a measured collapse past a knee without checking the claim's plan: a claim served by a
  partial index on pending rows (what `claimEligible`'s `CASE` prevents) produces stalls of exactly that shape.
  **Distance is why the ramp exists:** the load a database sees is the duty cycle `M · s/(k·RTT+s)`, not `M`,
  so a distant shard needs more connections for the same work. Past ~1.5-2 ms no pool fully compensates, so
  the ramp holds 500 beyond 1 ms and the answer at distance is co-location or more shards.

  ⚠️ **THE STARTUP RTT PROBE IS TAKEN IDLE AND IS ~HALF THE LOADED VALUE, AND ITS VARIANCE MOVES THE
  BUDGET.** On one 8-vCPU rig: idle probe **0.157-0.182 ms**, RTT under load **p50 0.34-0.38 ms**. Idle probes on
  one same-zone rig drifted 0.13-0.21 ms across an evening, and same-zone placements span 0.05-0.96 ms, so a
  shard near the ramp's start can land either side of it. The probe is taken ONCE and held, so a replica
  that starts during a latency blip keeps that budget for its life. Do not "fix" it by probing under
  load - that would move every near shard up the ramp for no measured gain. **Pin the pool in any sweep whose
  axis is not RTT.**

  How to measure a pool without fooling yourself - count backends rather than connections, treat collapse as a
  transient the window catches, command above saturation and read the mean, establish the noise floor, and
  never fit `k` from a ladder that moves the pool with the RTT - is in `bench/CLAUDE.md`.

- **The fleet is known BEFORE any worker dispatches, so pools are sized right the first time - there is no
  async grace window.** Reading the registry needs open connections, so `Startup` opens the shards at a small
  **bootstrap** pool (`startupBootstrapConns`, 4 - enough to register the row, probe the RTT and read the fleet,
  which is all any pre-dispatch work needs), joins the fleet ("Startup announces before it consumes", under
  §"Peer discovery"), and only then resizes each pool to its derived share and computes `workersDispatch` - all
  before `initRuntime` spawns a worker.

  **The bootstrap pool is deliberately tiny** because a cold-starting fleet's only connections during that window
  are these (roughly one per shard: everything in the window runs sequentially on one goroutine per shard), so
  even N replicas starting together stay far under any server's `max_connections`.

  **`lastAppliedSlots` starts EMPTY (`"nothing derived yet"` per shard), and Startup records each shard's role**
  after sizing its pool directly, so the first reconcile `recomputePools` dedupes against the values the pools
  actually hold.

- **The derived budget is per DATABASE, so the shard's DISPATCHERS split it**: each of the `X` replicas
  that dispatch a shard takes `max(2, round(budget/(X+D)))`, where `D` counts the replicas draining a share
  from outside the cut (see "Dispatchers"), and every other replica holds the reader pool (1 idle, 2 open).
  The knee belongs to the shard's server, not to one replica. Which replicas dispatch is
  never declared: the engine reads it from the shared `dwarf_peers` registry (see "Dispatchers"), and every
  role change pushes recomputed pools to the open shards immediately.
- **Nothing detects a wrong `MaxConnections`** - a declared lever is trusted exactly like the shard set.
  The asymmetry is what makes the derived default safe to leave alone: under-connecting costs throughput but
  stays healthy (excess load queues client-side), while the half clamp keeps the default off the server's
  limit.
- Heterogeneous fleets get heterogeneous pools: sizing is per shard (`database.ShardConfig`), resolved at
  `Startup` from each spec.

**Do not add adaptive/AIMD budgeting.** Discovering each shard's knee online (TCP-style probe-up/back-off)
would chase a curve that has no knee - throughput rises slowly past the budget (group commit) - and a control
loop keyed on what the engine can see is keyed on the wrong axis (see "Backpressure is the task's or host's
job"). The remaining exposure is honest and documented: a *wrong*
`MaxConnections`, or a deployment that isolates each replica's databases from the others (each then reads a
registry containing only itself, sees a fleet of one, and over-connects). Both are declared-fact failures,
the same contract the shard set already carries. Do not add a controller without a much stronger reason
than tidiness.

The idle-drain / lifetime-recycle connection timers (`ConnMaxIdleTime` / `ConnMaxLifetime`, server-drivers only)
are a database-layer mechanism — see `internal/database/CLAUDE.md`.

**Live re-sizing.** Derived sizing **is** live. `recomputePools` re-derives each shard's pool from the observed
replica count and pushes it to every open shard through `ShardSet.SetShardPool`, `Resize`s the candidate cache, and
re-derives the worker ceiling and the refill cadences - on every reconcile tick that finds a shard's dispatcher
slots moved.
See the live-vs-derived split under "Configuration" and "Dispatchers", and the load-bearing rule there - "EVERY
path that changes a pool must re-derive it," and "the cache follows the pool split": a new pool-derived quantity
must be wired into `recomputePools`, not merely computed once at `Startup`.

### Peer discovery (`peers.go` + `internal/peers`)

Everything the engine knows about its fleet comes from the shared **`dwarf_peers`** registry, NOT from the host transport, and it comes **per shard**: one
`peers.Sonar` per open shard owns this replica's row in that shard's registry, reads the whole registry on
its own cadence, and publishes what the reading implies. The mechanism - the two timestamps, the two windows
and their opposite postures, the blindness rules, the id-list prune - lives in `internal/peers/CLAUDE.md`.
What belongs here is the engine's side of it.

**Two decisions, from one reading, with opposite risk profiles.** `replicasOn(shard)` and the Sonar's rank
choose which replicas **dispatch** the shard and so how its connection budget divides (see "Dispatchers"
below). `partitionOn(shard)` divides that shard's **work** - the residue class of `step_id` each replica
selects - across the replicas that demonstrably serve it (see §"Candidate de-duplication"). Getting the first
wrong toward too many dispatchers over-connects the database by a share each; toward too few, it leaves a
shard short of dispatchers. Over-counting the second hands a residue class to a replica that never selects it
and strands the work in it. They are therefore never the same number: a registered replica that does not
dispatch a shard idles its piston there, stops stamping its dispatch evidence, and drops out of the partition
with no rule of its own.

**PER SHARD is the point, and a fleet-global count cannot express it.** A peer whose piston wedges on shard 3
drops out of shard 3's work divisor and stays in every other shard's; a peer whose beats to shard 3 fail
stops dispatching only shard 3. It also retires cross-shard comparison entirely - every timestamp in the
registry is stamped by the shard holding it, so ages are comparable within a shard and timestamps are
comparable nowhere. Consequently **every derivation that consumes a replica count does so per shard**:
`recomputePools`, `recomputeWorkerCeiling` (worst shard wins, since a storm drains through the tightest
pool) and `recomputeRefillIntervals` (the period is measured against the pool that shard drains through).
`lastAppliedSlots` is a per-shard map under `poolsLock` for the same reason: one shard's change must not re-push
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
drift no notification could express - a push that errored, a count moving while a
previous push was in flight. An edge would have to be fired from every path that can move a published value
(a confirmed fall, a recovery from blindness, a registration repair, a prune); missing one would leave the
derived sizes stale forever with no backstop, and it would run pool policy on a Sonar's goroutine, coupling
one shard's beat to another shard's slow push. **The tick must stay a small fraction of the read cadence,
and half is not small enough** - the apply spends part of the same budget `Join`'s two-cadence wait is
already spending (detection alone costs a cadence plus a pass), so at half a cadence a loaded rollout
(four replicas, budget 48) priced the fleet at 56 connections against a 52 bound, two survivors still holding the pre-join
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
fleet's guarantee is **budget + one post-join share**, not budget + bootstrap - pinned at that bound by
`TestPeerRollingRestart_FleetNeverExceedsTheShardBudget` (two replicas at a 48 budget: a bound of 72), which
still catches an ordering regression because that prices the joiner on its R-1 share too (96).

What the wait buys is peers having STOPPED ACQUIRING beyond their new cap, not connections
already closed: lowering a pool's limit closes nothing, so any surplus drains as connections are returned.

**Shutdown drains the Sonars LAST**, after the pistons - see "Shutdown ordering".

**A shard with no Sonar** (one that failed to build, or any lookup before Startup) sizes for a solo replica -
the pool-safe direction - and declines to partition, as the Sonar's own uncertain cases do (see "Everything
fails open" under "Candidate de-duplication"); `internal/piston` validates the pair a third time. Three guards,
deliberately: each package answers for what it knows.

**Under test the cadence is shortened** (`testPeerCadence`, `buildSonars`), because every engine pays
`Join`'s two cadences on the way up and a suite standing one up per test would spend most of its time there.
It must not be shortened too far: the blindness grace is TWO cadences, and a Sonar that reads as blind
declines to partition, so a grace inside ordinary goroutine-scheduling jitter disables partitioning at random
under a parallel suite - the same cliff a slow pass produces in production, reached through the test knob.

**A note on tests sharing the registry (`peers.go` / `poolsizing_test.go`).** Because the count is DB-backed, two
engines given the same `NewEngineUnderTest` name (typically `t.Name()`) share one test database, so they share one
`dwarf_peers` and count each other - which is exactly right for a genuine multi-replica test
(`fixtures/crossreplicaawait_test.go`), and exactly wrong for a single test that spins up several *independent*
engines to assert their solo pool sizes. The latter (`TestPoolSizing_DerivedWorkers`) uses `newSolo`, which names
each engine `t.Name()+"#"+key`, so each reads R=1. The
sizing tests drive fleet size by writing fake peer rows (`addPeerRow`/`delPeerRow`, which wait for the Sonars to
observe the change).

Design posture: a **lookup, not a control loop** (the counts are exact and discrete and
independent of the actuation - a shrunk pool still beats), and they are **tuning** numbers (a wrong count
mis-sizes pools, corrupts nothing).

**Every APPLICATION of a pool size is serialized under `poolsLock`** (`recomputePools`, driven by the reconcile
loop). The `lastAppliedSlots` dedupe skips a no-op recompute but does **not order two live
ones**: two readings microseconds apart during a rolling deploy each derive a different split, and with nothing
serializing read through push, the **half-budget sizes can land AFTER the third-budget sizes** - every replica
then holds a pool sized for a fleet smaller than the one it is in, over-connecting the shard's server, and it is
*sticky* (the next recompute sees the role unchanged and skips). With the lock spanning read through push, the
second recompute sees a settled world. Lock order:
`poolsLock` -> `shardsLock` (the counts are lock-free reads of the Sonars' published state, so they drop out of the
order). Pinned by `TestPoolSizing_ConcurrentRecomputeAppliesLatestR`, which drives the interleaving with the
`slowPoolPush` seam rather than racing for it - staging the two counts through the registry itself and waiting for
each to be OBSERVED before the recompute that must read it; without the lock it measures 48 instead of 24.

### `SetEngineID` - a stable replica identity

`engine_id` (a `BIGINT`, minted `rand.Uint64()>>1` per instance in `NewEngine`) is the `dwarf_peers` PK, and is
also **stamped on every flow/step INSERT** (creator) **and overwritten by the claim CAS** (claimer) - forensic
provenance, deliberately unindexed. Random is correct for the common case, *including several engines in one
process* (each counts as a distinct peer); its only cost is stale rows, since a crash-restart mints a new id and a
fast crashloop accumulates orphans faster than the fresh window ages them out, inflating R.
**`SetEngineID(id int64)`** lets a host pin a value stable across restarts (a StatefulSet ordinal, `os.Hostname()`),
so a restart re-upserts its one row. Construction-time only; rejects `id <= 0` (0 is the no-engine sentinel). The
host owns uniqueness among live replicas - a collision counts two replicas as one (under-count -> over-size, the
dangerous direction) - so it is opt-in, and a wrong stable id is worse than the random default. A host bundling
several engines in one process keeps the random default on purpose, and clears its leftover rows with a clean
`Shutdown` (which deletes the row) and a fresh database per run, not with a pinned identity.

### Dispatchers: a shard's budget divides by a bounded count, not by the fleet (`poolsize.go`)

Dividing the budget across every registered replica has a floor, and the floor is what breaks: once `budget/R` drops below
two, each further replica adds its minimum pool to a budget that does not grow - a budget of 48 at `R=100`
becomes 200, **4.2x over** - and even a large budget split a hundred ways gives every replica a pool too
small to dispatch from. The realistic fleet that reaches it is not a large dwarf deployment but a host scaled wide
for its own traffic, every instance paying for connections to shards it barely uses. So each shard has
`X = dispatcherSlots(C)` **dispatchers** that split the budget, and every other replica holds a
**reader pool** (1 idle, 2 open) and runs no step there - except a demoted one still finishing the steps
it claimed, which holds a share until they are done (below). Connections per shard become `budget + ~R` held,
most of the `R` idle, instead of `max(budget, 2R)` active.

- **Two budgets, and they must not be conflated.** The **budget** (`shardBudget`) bounds *active*
  connections and is what dispatchers divide. **`max_connections`** bounds *held* connections, idle
  included, and is what readers count against. Subtracting the readers from the budget
  (`(budget - 2(R-X))/X`) goes negative on a small budget at `R=10`, and shrinks the active pool to make room
  for connections that are not active.
- **`X = min(maxDispatchers, C)`, where `C` is the CANDIDATES** - the fresh replicas registered `working`,
  from `Sonar.Rank`, not `Replicas`. Dividing by every registered replica would split the budget among
  dispatchers that do not exist: one worker beside await-only frontends would run on a fraction of its shard
  (pinned by `TestDispatchers_ZeroWorkersPeersDoNotDivideTheBudget`). **`X` must agree across the fleet**
  (it is the width of every shard's cut: two replicas disagreeing would both claim, or both leave, one slot), and it does
  because `C` is read from the one registry every replica reads. **Give it no input a replica derives for
  itself** - its probed RTT, its read of the server's limit - or replicas in different zones split the cut.
- **`maxDispatchers` is 2: the availability floor, and the most a shard should pay for.** Two means one
  replica dying leaves the other dispatching while the slot passes; each further dispatcher's piston walks the
  shard's whole band (the residue predicate cannot seek), so refill cost grows with `X` (measured on the
  constant in `poolsize.go`). The TALLY loop is paced by the slot count for the same reason
  (`recomputeRefillIntervalsWith`); the supply loop is not, since a fetch walks rows in proportion to what it
  takes.
  **Consequence: two SLOW dispatchers on one shard are not compensated for, and that is accepted.** A slow
  replica keeps beating and turning its piston, so it keeps its slot, and a reader scans nothing there. With
  one slow dispatcher the other steals its class; with both slow, nobody healthy looks at the shard and it
  runs at their pace until one leaves the cut. A restart does that only if it is GRACEFUL (`leaveFleet`
  deletes the row; a crash under a pinned `SetEngineID` keeps it) AND a third working replica exists to
  take the slot - `ordering_seq` is never rewritten, so with exactly two candidates the restarted one ranks
  straight back in. Replacing the slow host is the general fix. It is degraded, not
  stranded - each still serves its own class - and it takes two independent replica faults on the same
  pair, while a third dispatcher's refill cost is paid on every shard, all the time. A shared cause (a slow
  database, a bad build) slows every replica and no dispatcher count helps. **Do not demote a replica from
  the cut on a measured slowness:** throughput or steal counts move with the load placed on it, so a control
  steered by them measures its own actuation.
- **Which replicas: a window over one shared order** (`inWindow`, handed to each Sonar as its cut in
  `buildSonars`; the window math, the join/leave behaviour and why entering a cut takes two agreeing readings
  while leaving takes one are in `internal/peers/CLAUDE.md`, "Who dispatches"). A mid-order leave demotes a
  live replica on each later shard, and a handoff errs a dispatcher short.
- **The role flags are PUBLISHED, not assigned** (`roles` is an `atomic.Pointer` to a per-run map): a
  host still serving through a Shutdown/Startup restart reads them from `Create` while `Startup` builds the
  next run's set, and a map assigned in place under that read is a fatal throw. Pinned under `-race` by
  `TestDispatchers_RoleFlagsSurviveARestartUnderLoad`.
- **The role changes in a fixed order** (`applyRole` under `poolsLock`): a replica leaving a shard idles its
  piston and closes its doorbell *before* its pool shrinks; one joining grows its pool *before* it
  dispatches. Idling the piston is `piston.SetIdle`, which withdraws the shard from the planner and empties
  its cache partition; its evidence then goes idle and it leaves the work partition on its own.
- **THE DOORBELL IS GATED BY ROLE, and without that the design is wrong.** `cache.Offer` admits a step for
  any shard, and the worker that pops it offers every successor back into the same cache - so a replica that
  rang its own bell for a shard it does not dispatch would run the entire chain through its two-connection
  reader pool. `enqueueStep`/`enqueueStepDue` return early unless `dispatchesOn(shard)`; the step is committed
  and due, so a dispatcher's scan takes it and the chain then stays on that dispatcher. The cost is one scan
  cycle once per `Create`/`Resume`/`Fork`/`Continue` issued on a replica that does not dispatch the shard, not
  once per hop. Pinned by `TestDispatchers_ReaderReplicaHoldsTheReaderPoolAndRunsNothing`.
- **`pickShard` places among dispatched shards when there are any**, by `Weight`, so a new flow's first
  step rings a bell that is answered. A replica dispatching nothing places across every non-cordoned shard.
  **Any declared weight other than 1 drops the preference** and places by weight over every non-cordoned
  shard: when each replica dispatches a subset, preferring its own would hand each shard its dispatchers'
  creates whatever the weights say. The cost is a scan's wait before the first step of a flow placed on a
  shard this replica does not dispatch. Pinned by `TestDispatchers_WeightsOverrideTheOwnShardPreference`.
- **Only dispatched shards feed the workers.** `workersDispatch`, the cache resize, the worker ceiling (worst
  *dispatched* shard) and the refill share all skip reader shards: a reader pool runs no step, and a 2-connection
  pool entering the ceiling's `min` would cap the whole crew.
- **Readers hold ONE, not zero.** A reader beats the registry every second per shard and polls it every 50 ms
  for a parked `Await`, so `MaxIdleConns=0` would reconnect for each - a new server process per beat on
  Postgres, twenty a second per parked caller. Going to zero needs, as a package, a solution for the beat, an
  enforced zero-hold invariant, a promotion out of ephemeral mode for a reader holding parked keys, and a host
  routing API that makes both rare. None of it is built; the one held connection is what makes all of it
  unnecessary. The burst to two keeps a `Create` transaction from queueing behind an `Await` poll, and is
  safe because no code path holds one pooled connection while acquiring a second from the same pool.
- **`recomputePools`' derivations all take its one role reading** rather than re-read the roles, so a reading
  landing mid-pass cannot give the cache, the refill periods and the worker ceiling different answers.
- **An await-only replica (`SetWorkers(0)`) registers `working=0`**, so it is counted in the fleet (its reader
  pools are real connections, and the fleet check prices them) but takes no rank. Registered as working, it
  could win a slot it will never serve - leaving a shard with fewer working dispatchers than `X`, or none.
  **`working` is intent, not evidence of serving**, and the registry must never carry a claim to be serving (a
  replica claiming it and then wedging would keep its residue class forever): it feeds only the ranking, and
  the partition divides on `dispatched_at` evidence alone. A dispatcher
  that wedges still holds its slot until its row goes stale - the residual below - but it strands no class.
- **Background sweeps run only on dispatched shards.** The reaper and every recovery repair (expired leases,
  wedged parks, orphaned children, orphan detection) skip a shard this replica only reads. Each is
  idempotent, so one sweeper per shard suffices, and every shard always has a dispatcher; a reader running
  them would put heavy scans through its two-connection pool once per replica in the fleet. Pinned by
  `TestDispatchers_SweepsSkipShardsThisReplicaOnlyReads`.
- **`dwarf_shard_dispatching{shard}`** reports the role (1/0) per replica; summed across the fleet it is the
  shard's dispatcher count, and zero means the shard runs nothing. Replica identity is the host's OTel
  resource (`service.instance.id`), never an engine attribute - engine ids are random per restart, so an
  attribute would open a new series on every deploy.
- **The fleet check is advisory and edge-triggered** (`checkFleetFits`, every reconcile tick, since a reader
  joining moves no pool and so would never reach the deduped push). It warns when `budget + 2(R-X-D)` passes 80%
  (`R` every registered replica, await-only included)
  of `max_connections`, read once per shard at Startup (`pg_settings`, `@@max_connections`,
  `@@MAX_CONNECTIONS`; SQLite and a failed read give 0 and skip). SQL Server reports 32767 unless "user
  connections" is configured - always, on Azure SQL - so there the check effectively never fires. It never resizes
  anything: shrinking
  dispatchers to make room for idle readers is the wrong trade, and a fleet that size wants a pooler.
- **A draining replica withdraws if its drain outlasts one registry read cadence** (`drainRuntime`, a timer
  armed before `crew.Drain` and joined after it): `Sonar.Withdraw` sets its rows' `working=0`, so its peers
  re-rank without it on their next reading while it finishes its in-flight tasks. Without it the row stays
  fresh - and keeps its rank - for the whole drain, which can be minutes, and a small shard whose dispatchers
  all drained at once (a scale-in, a rollout with `maxUnavailable >= 2`) would run nothing until the slowest
  task finished. **The withdrawal has a cost, which is why it waits:** the peers grow into the freed share
  while the drainer still holds it. `Withdraw` therefore also states `draining` on every shard where the
  replica holds a dispatcher's share, and peers add the draining count to the divisor - the slot passes,
  but the share is divided around rather than grown into (the drainer keeps the pool it had, so the shard
  is over by the difference between that share and the narrower one, not by a whole share). A drain that
  ends at once pays even that for nothing (`leaveFleet` hands the slot on as fast), and in a rolling restart
  the overlap stacks
  with the replacement's bootstrap pool - withdrawing unconditionally measured 71 connections against a 67
  bound on Postgres, in `TestPeerRollingRestart_FleetNeverExceedsTheShardBudget` rolling four replicas. **Do not gate it on a count of
  tasks in flight instead:** read at the instant the drain starts, it misses a worker that has claimed a step
  and not yet entered the task. Pinned by `TestDispatchers_DrainGivesUpTheSlotBeforeWaiting` and
  `TestDispatchers_DrainingDispatcherHandsItsSlotToAPeer`.
- **A DEMOTED dispatcher keeps a dispatcher's share until the steps it claimed have finished**
  (`recomputePools`, `heldDivisors`). Demotion is routine: a mid-order leave demotes one live replica on each
  shard whose window lies after it (and a join does too once the fleet is small enough for the windows to wrap,
  `C < S*X`), and the replacement - or the same replica back under a pinned `SetEngineID` - displaces whoever took its
  place. Shrunk straight to the reader pool, the in-flight completions queue on two
  connections: about 12,000 of them at ~5 ms each fill the 30 s lease margin, stalling every flow behind
  them and letting leases lapse into re-execution if the recovery sweep lands in the window.
  - It stops claiming at once (`applyRole(false)`), but its pool and turnstile are sized at the held
    divisor (`roleReading.held`, remembered per shard in `heldDivisors`) - the dispatcher count plus every
    draining replica, itself included - and it states
    `draining` so its peers divide by the same count. Once every peer has read it the shard is back on
    budget (the window before that is in `internal/peers/CLAUDE.md` under `Draining`); the dispatchers lose
    a slice of their pool for the length of the drain, which is the trade being made.
  - **The count is `shardRole.inFlight`, incremented in the crew callback after `TryClaim` and decremented
    when `processStep` returns** - claim through transition, nothing asynchronous after it. A worker that
    popped a candidate and has not reached the callback is not counted; it is one step through a narrower
    pool, not a backlog.
  - **Released when the count reaches ZERO**, checked on each reconcile tick (`recomputePools` re-pushes when
    a held shard drained or the draining count moved, even with no role change). **All at once, by
    decision:** one long task holds the share - and keeps every dispatcher on the shard a slice smaller - for
    its whole duration, and holds stack across a rolling deploy. A release threshold (shrink once the reader
    pool could clear the rest within N seconds) would end holds sooner, but it is a fixed constant, a
    tunable, and a stall bounded by it; the simple rule was chosen over it. Revisit with a measurement of
    hold durations under a long-task workload, not by argument. Do not hold the full pre-demotion pool
    instead of dividing: that leaves the shard a share over budget for as long as the slowest task.
  - **Ordering:** the pool is pushed first and `SetDraining` follows it, both ways, so the statement never
    lowers a peer's divisor while this replica still holds a share from outside the cut. It cannot make the
    handoff instantaneous: until every peer has read the statement the shard runs over by about the held
    share, for up to about two scan intervals - an accepted residual whose bound, and the alternative not to
    build, are in `internal/peers/CLAUDE.md` under `Draining`.
  - A re-promotion while held clears the hold and takes the dispatcher pool directly.
  Pinned by `TestDispatchers_DemotedReplicaHoldsItsPoolUntilInFlightStepsFinish`,
  `TestDispatchers_PromotionWhileHeldClearsTheHold` and `TestDispatchers_DrainingReplicasDivideTheBudget`.
- **Known residual: a crashed dispatcher holds its slot for the fresh window (40s).** The rank is drawn from
  rows still counted by `Replicas`, so a dispatcher that dies without shutting down keeps its rank until its
  row ages out, and the shard runs on `X-1` meanwhile. With `X >= 2` that is slower, not stopped; **all `X`
  crashing together stops the shard for up to the window**. Ranking only rows with fresh dispatch evidence would
  shorten it, but a reader never
  stamps that evidence, so it could never be promoted - the fix is not free. **On top of the window, the
  promotion waits a beat per stale row ahead of the promoted replica** (the cut-entry stagger,
  `internal/peers/CLAUDE.md`), so a zone loss or a kill-style roll that leaves k stale rows makes each handoff
  take about k beats until the prune removes them - up to about five minutes.

### Worker sizing: the lease-margin ceiling, with a grow-on-demand crew (`poolsize.go`, `engine.go`)

The worker count is split into two numbers, because they answer different questions:

- **`workersDispatch` (half the candidate cache's size: the cache holds `2 x min(workers, workersDispatch)`)** =
  `max(64, 8 x sum(per-shard open))`
  (`workersPerConnBudget`). Dispatch is DATABASE-bound, so the connection budget is what sizes it; the 8
  is a generous `T/db` allowance (measured ~3 for no-op tasks).
- **`workersResident` (eagerly spawned, and the floor the crew never shrinks below)** =
  `max(64, 2 x sum(per-shard open))` (`residentWorkersPerConn`). A floor, not a target: under saturation
  the crew grows to the turn count whatever the floor is (below), so the floor only sets the idle crew.
  **The cache - and therefore the refiller's selection, which fills at most the cache capacity of candidates
  and fetches per fairness key capped by it (see "Selection") - must never be sized from the maximum below**:
  a worker parked in a long `ExecuteTask` holds no connection and dispatches nothing, so a ceiling-sized cache
  would select far more of the backlog than the replica can ever claim.
  **The CACHE follows the pool split; the RESIDENT WORKER COUNT does not, and that asymmetry is deliberate.**
  Both are computed once in `Startup`, from the pools already sized for the R **discovered there** (for
  a solo start that is R=1, so `sum(per-shard open)` is the full per-database budget). Every later fleet change
  re-divides the pools among the shard's dispatchers (`recomputePools`), which **also re-derives the dispatch count from the post-split
  budget and `cache.Resize`s to it** - the same "every path that changes a pool must re-derive what depends on it"
  rule the worker ceiling obeys - but does **not** re-spawn the resident worker set.
  - The **cache** had to follow, because it was the half that costs throughput. The refiller selects up to the
    cache's capacity of candidates and wholesale-replaces it, so a dispatcher holding a cache sized for the
    whole budget while a second dispatcher shares the shard is handed twice the candidates it can claim: stale
    hints whose claim CAS loses to a peer, and wasted round-trips, exactly when the fleet is busiest. It is the same "never size the
    cache from more than the replica can claim" rule that keeps it away from the worker *ceiling*, reached
    through a different door. Pinned by `TestPoolSizing_CacheFollowsTheReplicaSplit` (a 48-connection shard: a 768
    capacity solo, 384 once seven peers register and the budget splits between two dispatchers).
    `Cache.Resize` trims the tail; a trimmed candidate stays `pending` and is re-selected by a later cycle.
  - The **resident worker set is not re-derived, and it does not need to be** - the crew shrinks itself. Each
    worker times what fraction of its own recent wall clock it spent HOLDING a candidate and retires on a coin
    flip when that falls under the threshold, never below the resident set `Start` was given
    (`internal/workers`). So a fleet change that halves the pools does not have to resize anything: the
    surplus workers find themselves idle against the smaller budget and go, at `required_concurrency /
    threshold`. **Do not add a resident-count push to `recomputePools`** - it would be a second, coarser
    actuator on a quantity that already self-corrects, and it would have to serialize against a shrink in
    flight. What `recomputePools` re-derives is the cache and the ceiling, which do NOT self-correct.
    **The resident floor is deliberately left at the Startup value** (derived at R=1), so a large-R fleet keeps
    an over-provisioned floor. That is the safe direction: surplus workers queue on the pool, and the
    shrink rule collects their stacks without any protocol on the hot path.
- **`workers` (the MAXIMUM the crew may grow to)** = `workerCeiling` = `M x margin / txTime x safety`,
  the largest pool that keeps a **synchronized completion storm** inside the crash-recovery lease margin: every
  in-flight task released at once (a downstream outage ending), N completions draining through M connections,
  and any that out-waits its margin re-executes after a peer re-claims - duplicating the most expensive work at
  the worst moment. `txTime` is counted from `completionRoundTrips` (a subset of the ceiling equation's `k`;
  recount it when the post-task phase changes) times the RTT probed at Startup; the fleet takes the worst
  shard's number. The derivation and per-driver counts are at `workerCeiling`/`completionRoundTrips` in
  `poolsize.go`. Every input is engine-visible - **no task duration T anywhere**, which is exactly what makes
  this derivable where `N = M x T/db` is not.
- **`turnstiles` (how many database calls may be admitted on a shard at once)** = `turnstilePassesPerConn x
  open`, two turns per connection - deliberately MORE than the pool, so a queue forms in front of it (see
  "Turn-taking on the database"; sizing it to the pool measured a 6x collapse). This is what lets the crew grow
  freely for long tasks without the
  growth turning into pool contention, and the turn taken BEFORE a candidate is picked up is what regulates
  crew growth - by blocking, not by being consulted. See "Turn-taking on the database" below.

- **The crew grows on demand** (`internal/workers`): a worker that takes a candidate and finds **nobody
  idle** adds one, keeping a standing reserve of one. The turnstile's turn is what makes so easy a trigger
  safe - by BLOCKING, not by being consulted: a worker waiting for a turn holds no candidate, so it counts idle
  and the next check declines. Two traps not to re-introduce, each with its measurement: gating growth on
  every worker being inside `ExecuteTask`, which grows only logarithmically (`internal/workers/CLAUDE.md`), and
  a caller-maintained idleness counter, which reads saturation as idleness (the `idle` field in
  `internal/workers/crew.go`). Both directions are pinned:
  `TestPoolSizing_PoolGrowsForLongTasks` and `TestPoolSizing_SaturationDoesNotGrowThePool`. Shutdown is the
  crew's two-phase `Drain`, and the engine's half is closing the CACHE **and the TURNSTILES** first: a worker
  with nothing to run is parked on one of them, and closing the cache alone leaves `Drain` waiting forever.
- **`SetWorkers` pins the maximum** (deterministic tests use `SetWorkers(1)`, which also disables growth;
  benchmark sweeps; hosts wanting a smaller global bound, e.g. for memory - the in-flight state maps are
  a cost the engine cannot see). Setting it ABOVE the ceiling is allowed and warned about at Startup: an
  operator may consciously trade storm-re-execution risk for long-task throughput. `SetWorkers(0)` is the
  await-only replica.

The worker count is deliberately **not** a backpressure knob: a global cap cannot express "64 concurrent
LLM calls, 1000 concurrent DB lookups, 200 Jira writes", and a per-task-URL cap is the rate valve's
wrong axis all over again (see "Backpressure is the task's or host's job"). Per-downstream concurrency
belongs in the host's `ExecuteTask` (a semaphore keyed on the real provider/account) with `flow.Retry`
when the downstream says no. The engine sizes workers for throughput, bounded only by what it can see:
the lease margin.

### Turn-taking on the database (`internal/turnstile`) — order the wait, do not remove it

**Every database call a step makes takes a turn at its shard's turnstile, and turns are served by band and
then by how long the JOB asking has been at the database.** The claim is stamped once, when the candidate is
picked up, and rides the context (`turnstile.Set.ContextWithPriority`) so every later call of that step
presents the same age - except that the host call does not count: when it returns, the stamp is moved
forward by its duration (`turnstile.Away`). A step already under way therefore outranks work that arrived
after its own work began, and finishing work stays ahead of starting work under saturation. Do not carry the
pickup stamp across the host call unmoved (once tasks outlast the queue every exit outranks every entry: strict
precedence, measured as the long-task regime's `Create` p99 of seconds), and do not re-stamp it fresh (the
finished step waits behind work not yet started). `internal/turnstile/CLAUDE.md` owns the sizing (two turns
per connection; one per connection measured a 6x collapse, and past the first queued caller the multiple buys
queue depth and a bigger saturated crew, not throughput), the enclosure rule, the fail-open posture, `Resize`
by delta, and why entering and completing work must not get separate reservations or bands.

```
park for work                       <- holding NOTHING
  -> peek: which shard has work?
  -> Gate.Acquire: stamp the claim, take the FIRST turn (blocking)
  -> TryPopFrom that partition       (empty? return the turn, retry)
  -> claim CAS                       <- the turn covers exactly this
  -> return the turn
  -> flow/graph load                 <- its own turn, on the same claim
  -> ExecuteTask                     <- unbounded, holds nothing, takes no turn
  -> Away(host call's duration)      <- the task's time does not age the claim
  -> persist / transitions           <- its own turn, on the pickup claim moved forward
```

**A pass must ENCLOSE the connection, AND NOTHING ELSE.** A cursor holds its connection until closed, so the
turn is held until the reading is done, and a transaction takes one turn for the whole `Transact`. It is handed
BACK across any wait that is not the database - `yieldTurn` does that around the persist retry backoff and the
cohort stripe lock, and `yieldTurnAway` around the subgraph `LoadGraph`, which as a host call also moves the claim
forward by its duration, as `ExecuteTask`'s does. The stripe is the sharpest: its whole purpose is that a
losing sibling waits holding no connection, and a turn is that occupancy one level up. Callers holding
connections without turns and callers holding turns without connections deadlock each other, which is why
nothing wraps `sequel.DB` - the call site is what knows when the connection is released.

**Two bands, and band 0 holds only the piston and its shard's peer Sonar.** `priorityRefill` (0) is above
`priorityCommon` (10; the values are ordinals only), and bands are strict — a band is exhausted before the
next is looked at. The piston has it because it is bounded by construction (a derived period, and two loops
each holding at most one turn at a time - two statements per scan, one per fetch - against a turn count of 2x
the connections), so it cannot starve what sits below it; and it needs it so a cycle runs on its derived period rather than at the depth of the pool queue -
pool wait otherwise rate-limits the refiller (see "What the instruments measured, as rules" under Metrics).
The Sonar is there because a Sonar that cannot beat reads as BLIND and its replica stops partitioning, so
throttling the registry would let load reshape the fleet; it beats on a fixed cadence, so it cannot flood
the band either. Band 0 rather than an exemption: a bypasser ranks above every band and drops out of the
population the turn count bounds (`dbturn.go`).

**Everything else shares one band and is separated by age alone - a decision, not an unfinished state.**
Age ordering cannot starve, while a second band for any caller that can arrive faster than it is served starves
the band below (completions given precedence measured a 3x short-task collapse). So if completions are
measurably waiting, the shard's connections are saturated by work at least as old, which no split can fix.

**THE GATE'S TURN IS THE CREW'S GROWTH BRAKE.** A worker blocked in `Gate.Acquire` has not popped, so it counts
**idle** and `considerGrowth` declines; the crew overshoots a saturated shard by exactly one worker. **Do NOT
move the turn after the pop:** a worker that pops and then blocks holds a candidate and counts busy, so every
take spawns a peer up to the (huge) `workerCeiling` (measured 64 -> 398), and the candidate is stranded in a
worker that cannot proceed. Pinned by `TestPoolSizing_SaturationDoesNotGrowThePool` and
`TestCrew_SaturationDoesNotGrowThePool`.

**Per shard, resolved by peek-then-acquire.** Connections are per shard, so turnstiles must be — but the pop
is what chooses the partition, so the shard is not known until after a pop, which is the ordering above
rules out. Hence: peek the shard, acquire, then pop **from that partition only**, returning the turn and
retrying on a lost race. **Do NOT make the turnstiles GLOBAL** — that is a control keyed on the wrong
resource: one shard's work would consume every turn and deepen exactly the pool
queue this exists to keep empty.

**Everything that takes a connection takes a turn.** A bypasser competes for connections without
having queued for the right to, so it is served ahead of turn-holders that are older than it - which is
precisely the ordering this exists to impose. It also puts the contending population back above the turn
count. The rule is positional (`dbturn.go`): an operation takes its turn at its entry point (`opTurn`), a
background loop (the reaper, the recovery sweeps, the backlog gauge loop, the latch poll) takes one per shard
per pass, and the worker path takes one per database call between the phases it has already released. Take
it at the OUTERMOST database operation of a path, never inside a shared helper - a turn taken while one is
held is the nesting the enclosure rule forbids. Startup's RTT probe is the one exemption: it runs before any
worker exists.

**Fail open** (an unstamped context, an unsized shard, an expired ctx, a closed set): the turnstile only orders
access the pool already bounds, so do not harden it into failing closed - but a sizing mistake is therefore
**silent**, visible only as ordering that is not happening. **Every path that changes a pool must re-size the
turnstiles** (`Startup`, `recomputePools`).

**Head-of-line blocking across shards is closed in two steps, and a replica dispatching several shards
needs both.** Left alone, every worker that peeks at once routes to the same shard and queues at its gate
while another shard sits with work and free turns - and a worker queued at a gate counts *idle*, so
`considerGrowth` declines to spawn for the other shard either. The two steps:

- **Routing reserves a candidate** (`internal/candidates`): workers spread across shards by the work no
  routed worker is already on its way to take, rather than all choosing the deepest.
- **A full shard is passed over** (`internal/workers`): the take loop tries the routed shard's gate without
  waiting (`Gate.TryAcquire`), and on a miss moves its reservation to the next shard with work
  (`Cache.Reroute`), waiting only when every shard it tried is full. Routing by candidates cannot see that
  one shard's turns are all taken.

The signature of either step missing is a shard's turnstile reporting free turns and waiters in the same
window: workers are queued on one shard while another's turns sit unused.

### Flow Scheduling (priority / fairness)

The schema carries `priority`, `fairness_key`, `fairness_weight` on **both** `dwarf_flows` (authoritative) and
`dwarf_steps` (denormalized), so the two-level selection never joins `dwarf_flows` on the hot path - the same
split used for `time_budget_ms`/`baggage`.

`resolveFlowOptions` resolves a caller's `*workflow.FlowOptions` against the engine defaults: priority falls back to the
midpoint of `1..max` (`SetMaxPriority`), the fairness key to `""`, the weight to `1`, and the time budget to
`SetTimeBudget` (no ceiling; see "Time Budgets"). These values are immutable for the flow's life (switching a flow's
fairness key mid-run would be a self-promotion abuse vector). This is the **genesis** path (`Create`/`Run`); the
**derived** operations inherit instead - `createSubgraphFlow` from the parent (so a high-priority parent never
silently spawns default-priority descendants), `Continue` from the flow it continues, `Fork` from the origin. See
"Policy is set once at genesis" under Engine Operations.

Propagation onto step rows: where the resolved values are in hand (the entry step), they are literal bind parameters;
in the deep `processStep` paths (fan-out and the two fan-in inserts), the values - including the flow's frozen
`time_budget_ms` - are read once per step execution with the flow row (the claim's join on PostgreSQL, the
flow-row read elsewhere) and threaded through the call chain into the INSERTs as bind parameters. **Do not read
them by a scalar subquery on `dwarf_flows` per inserted row** - that is 3N PK lookups per N-way fan-out. `Fork` resolves scheduling once for the whole cloned tree and binds it on every cloned flow and step
(see "Fork").

**Why the scheduling design is shaped this way:**

- **Priority is a property of the flow, not the task or workflow type.** Step order *within* a flow is dictated by the
  graph, not urgency; priority only arbitrates *between* flows competing for workers, so it is resolved once at
  `Create` and immutable (`workflow.FlowOptions` is flow-level for the same reason).
- **Workers default to the lease-margin ceiling with a grow-on-demand crew** (see "Worker sizing"); an
  explicit `SetWorkers` pins the maximum. A worker blocked on an `ExecuteTask` call is just a goroutine
  stack plus a socket, so the derivation errs generous - and the crew only grows into it when work is
  actually parked.
- **Fairness weight is denormalized at `Create`, never resolved on the selection path** (a resolver hook would put
  synchronous I/O on the hot critical section). When a key's steps carry inconsistent weights, the lowest due step's
  weight is used, for as long as that step is due; keeping weights consistent for a key is the caller's
  responsibility.
- **Completion writes share the common band with every other non-piston caller, ordered by age at the
  database** - no precedence over starting new work (see "Turn-taking on the database" for why a band for
  either side collapses throughput).

> Observability note: per-priority backlog and age are emitted as aggregate-only gauges - no per-`fairness_key`
> labels (unbounded cardinality). See "Metrics".

### Step Parking (`parked` column)

`dwarf_steps.parked SMALLINT NOT NULL DEFAULT 0` takes a step out of the selection band without changing its
`status`. The selection index `idx_dwarf_steps_selection` `(status, parked, priority, fairness_key, created_at,
step_id)` and the due index `idx_dwarf_steps_due` `(parked, priority, not_before)` (`status`-led on SQL Server and
MySQL) lead with the partitioning columns, so parked rows are physically excluded from every hot-path scan - no in-memory filter at
refill time. (The selection index's trailing `(created_at, step_id)` serves
the refiller's per-key oldest-first ordering; see `internal/migrations/CLAUDE.md`.) The `parked` value labels *why*
the step is held:

- `parked=0` (`parkedNone`, default) - active. Selection sees it; `recoverExpiredLeases` recovers it if its lease
  expires. (Also the precondition the claim CAS requires.)
- `parked=1` (`parkedSubgraph`) - the step called `flow.Subgraph` and is waiting for the child. `status='running'`
  (logically running, blocked on its child) but excluded from selection AND lease-expiry recovery. No
  lease deadline - the row sits until `completeSurgraphFlow` flips it back to `(pending, parked=0)`. **Do not
  model a park as a far-future lease** (`lease_expires = NOW + 7 days`): it breaks for any subgraph running longer
  than the lease - the lease lapses, the parent is recovered, the task re-runs, launching a duplicate child.

**Terminal status implies `parked=parkedNone`.** The park value is meaningful only while a step is actively waiting.
Once terminal (`completed`/`failed`/`terminated`/`cancelled`), the park slot is gone, and the column must read
`parkedNone`. Every
terminal-transition code path resets `parked` in the same UPDATE (the `failStep` write, the `Terminate` cascade, the
`processStep` terminal-flow guard). Without this, a step that was parked
when its flow reached a terminal status would sit terminal with non-zero `parked` - invisible to the selection index but never
re-leased. A `Fork` clone writes each step's `parked` explicitly (the re-parked ancestor callers to `parkedSubgraph`,
all other cloned steps to `parkedNone`), so cloned rows never inherit a stale non-zero `parked`.

## Metrics (`engine/metrics.go`)

The engine emits 37 `dwarf_*` instruments through the **OTEL metric API** (not the SDK). `SetMeterProvider`
injects the provider; it defaults to the global `otel.GetMeterProvider()` - no-op unless the host configures the
SDK, so unconfigured/standalone/test use pays nothing. Instruments are built once in `initMetrics` (from
`initRuntime`, so every `Startup` gets them) from `mp.Meter("github.com/microbus-io/dwarf")` - that
scope distinguishes dwarf's metrics; **service identity lives in the provider's Resource, not in per-metric
attributes** (no `service.name` on data points - cardinality explosion, off-spec). The only attributes attached are
the metric-specific labels: `workflow`, `status`, `task_name` (on `dwarf_steps_executed`), `priority` (on the
flow lifecycle counters and the per-band backlog gauges), `park_type`, `role` (on the db-phase pair), `op` (on
`dwarf_operation_seconds`), `shard`, and `column` (on the byte counters). `priority` and `workflow` assume the host uses a bounded set of each;
the host drops either with a metric View if it does not.

**8 counters, incremented inline** at their logical event sites: `dwarf_flows_started`
(start path), `dwarf_flows_ended` (completeFlow), `dwarf_steps_executed` (every terminal step
disposition - completed/failed/interrupted/subgraph/retried/error_routed), `dwarf_steps_recovered`
(recoverExpiredLeases lease recovery, and the orphan sweep's rewinds), `dwarf_steps_unwedged{park_type}` (the parked-step wedge sweep; a
nonzero value flags a latent bug), `dwarf_flows_orphaned` (a stranded flow the orphan sweep could NOT repair
because no step carries the `advancing` flag - the flow-level sibling of `dwarf_steps_unwedged`, counted at the
same site as its error log; the flows it does repair count in `dwarf_steps_recovered`),
and the two persist alarms `dwarf_steps_write_retried` / `dwarf_steps_write_failed` (detailed under "Persisting
a step's outcome"). The inline helpers no-op when `e.metrics == nil` (before Startup).

**3 refiller counters + 1 refiller histogram**, built by the PISTONS (`internal/piston`) from the meter
this engine resolves once and hands each of them - one instrumentation scope per engine, whoever records
into it. `initMetrics` deliberately does NOT build them: registering the same names twice on one meter
is a duplicate-instrument conflict, and two copies drift apart in description and bucket boundaries. They are
`dwarf_refill_candidates_selected` / `_discarded`, `dwarf_steps_stolen` and
`dwarf_refill_query_duration_seconds` {shard,phase}, where `phase` is four values - `band_keys`,
`fetch_steps`, and the two non-query phases `planning` and `pushing`. Recording planning matters because it
is the one cost that scales with fairness-key CARDINALITY (the lottery re-rolls per slot over every key).

**There is deliberately no end-to-end `dwarf_refill_duration_seconds`.** An end-to-end figure is only
informative against a MERGED pass, whose straggler tax is its gap over the per-shard query max; with no
barrier there is no such gap, and an end-to-end timer is a coarse duplicate of the four phases, which say
WHICH part was slow. Do not add it without a question it answers that the phase split does not.

**Do not sum the four phases** - they come from two loops on independent cadences (`bench/CLAUDE.md` has the
trap). These exist because without them the refiller is the one hot-path subsystem with no timing instrument at all,
and "what binds at the ceiling" cannot be asked of it - without them a flat shard arm cannot be told from a
load-generator-bound one. They discriminate three hypotheses that look identical from outside
(the rules below record how each resolved):

- **One shard is slow** - `refill_query_duration{shard}` diverges *between* shards. Recorded per shard
  around each shard's own scan, timing query + row scan.
- **The cross-shard fan-out wait is the cost** - true of a barriered design (see "Execution Model"), which is why there is no barrier and no merged pass to time (see above). A shard's own cycle
  time - which sets
  its partition's supply rate, `capacity_slice/max(supply period, floor)` - is set by the SUPPLY loop alone
  and has nothing to do with the band scan. Summing the four phases overstates it by the scan, which is the
  larger term by one to two orders of magnitude at depth (measured: a 1.6-2.1s scan against a 20-67ms supply
  period), so the sum answers ~0.5 steps/s per shard where the real figure is ~50.
- **The refiller oversupplies** - `discarded/selected` approaches 1. Every supply cycle wholesale-replaces
  its shard's partition, so whenever it turns faster than the workers drain it throws away a batch it just paid to fetch. `Cache.Refill` returns the
  discarded count for this. It is the cheap readout that catches the regime changing, and the gauge to
  re-tune the cycle interval against - see "Oversupply is live" in the rules below.

The `phase` label (`band_keys` / `fetch_steps`) separates phase 1 from phase 3, which matters for a reason
found while writing the degradation harness: the band scan's plan flips between an index scan and a
**sequential** scan on Postgres statistics freshness - **0.29ms vs 99.8ms on identical data minutes apart**.
That is indistinguishable from a slow shard without the phase split, and phase 1 is where the measured cost
concentrates, so the split is what makes the band scan's backlog dependence visible at all.

Explicit second-valued bucket boundaries (`refillBuckets`, in `internal/piston`) are mandatory, and their low
end is load-bearing (`internal/piston/CLAUDE.md`).

**What the instruments measured, as rules** - each one makes a future change WRONG if unknown:

- **Backlog depth is not free server-side.** The three-phase split and the count cap bound WIRE cost only; the
  band scan is O(due rows at the band) on every dialect (see "The phase-1 scan count is CAPPED").
- **The band scan's client-observed "fixed floor" is connection-pool wait, not query work** - shown by
  decomposing the client clock against `pg_stat_statements`, and by two independent removals (a reserved refill
  connection, a 4x smaller crew) each collapsing it to server time. No query-shaped change can touch it.
- **Pool wait silently rate-limits the refiller**, so anything that reduces pool contention (a reserved
  connection, more connections, a faster database) must arrive with the cycle interval re-derived against it,
  or the refiller spends server work producing candidates nobody consumes (measured: discards 0.7% -> 43%).
  Neither removal bought throughput or latency.
- **A queue at the pool is needed, and its depth is not free at scale** - size it by turns per connection,
  and never derive the crew from a measured `M x T/db`, which reintroduces the task-duration dependence
  `workerCeiling` exists to avoid.
- **Do not chase a phase-3 win with a tighter `rn <=` cap on the window-shaped fetch** (mysql, sqlite): it
  filters after the window function, so it cuts rows returned, not processed (measured unchanged twice).
- **CAP the count, never APPROXIMATE it.** `count` sets both a key's batch share and its fetch cap, so an
  approximation is wrong in either direction and breaks fairness; `min(count, capacity)` is exact below the cap
  and lossless above it. Sub-linear phase 1 needs distinct-key enumeration AND a proof the contract survives.
- **Keep phase 1 a single `GROUP BY` with no per-key lookup.** Reading the oldest step's columns per key (a
  `LATERAL`, or a correlated subquery on `created_at = MIN(created_at)`) lost to the aggregate every time it was
  tried, and a fan-out's same-millisecond siblings make the lookup match thousands of rows per key. Re-derive the
  cost split after any change that moves the baseline.
- **Oversupply is live with the loops split** (a fused loop hid it): a high `discarded/selected` names a supply
  loop running ahead of the workers, a cadence-derivation question (`deriveRefillInterval`'s worker term).
- **Supply is not what binds at saturation, and a slow Tallier does not make it so.** Under a deep backlog the
  band scan inflates ~25x and the tally loop collapses, while the supply loop keeps its cadence and runs ahead
  of consumption with the cache full. The scan is a **resource tax, not a supply chokepoint**: tripling its
  cost by deepening the backlog moved throughput -1% (noise). Do not narrow the selection index or revive
  skip-scans on throughput grounds; they need another justification (server CPU headroom).

**17 counters** in total: the 8 event counters above, plus `dwarf_steps_offered` /
`dwarf_steps_claim_preempted` / `dwarf_steps_claim_lost` / `dwarf_peer_changes`, plus the three the pistons
own (`dwarf_refill_candidates_selected` / `_discarded`, `dwarf_steps_stolen`), plus the two byte counters,
`dwarf_state_write_bytes` / `dwarf_state_read_bytes` (labels `workflow` + `column`, unit `By`) - payload
bytes the engine writes to / reads from **step rows on the execution path**. The `column` label is the
dwarf_steps column the bytes moved through (`state` snapshots, `changes` task-output deltas,
`resume_data`, `interrupt_payload`, `subgraph_result`, on writes `state_refs` - the refs written beside each
snapshot, where a field under `append` is stored as a list, so it grows with a loop's length while `state` stays
flat - and on reads `state_ref` - the anchor rows a state-refs resolve scans; bounded cardinality; sum across it
for totals),
so a chart can split task data from engine snapshots from human-in-the-loop payloads. Counted: the claim
read (state+changes+resume_data+subgraph_result), completion/retry/park/interrupt `changes` writes, the
interrupt payload write, successor/fan-in `state` snapshot and `state_refs` writes, the entry `state` write, and the fan-in merge's cohort
reads. Deliberately NOT counted: introspection reads (`List`/`History`/`Snapshot`), flow-row payloads
(`final_state`, `baggage`, `graph` - per-flow context, not per-step data), Fork's clone (a DB-side
`INSERT…SELECT`; the bytes never pass through the engine - though its leaf's flatten does count, as `state_ref`
reads), and the `Resume`/surgraph-revive *writes* of
`resume_data`/`subgraph_result` (cold paths where the workflow URL is not in hand - those bytes are
counted on the claim *read* at the re-dispatch they trigger, so every payload is counted in at least one
direction). The metric exists to track against the database's byte-throughput ceiling (disk/WAL), which
binds separately from its steps ceiling (`docs/benchmark-cloud.md`). Transition-tx byte counts are accumulated in the closure
(`stateByteCount`) and emitted only after commit - a contention retry re-runs the closure, so counting
inline would double on every rollback. Unit-denominated instrument names end with the unit (`_bytes`),
per the Prometheus naming convention.

**Counter instrument names carry no `_total` suffix.** `_total` is a Prometheus naming convention, not an
OpenTelemetry one: a Prometheus exporter appends it to every counter at the scrape boundary (and
de-duplicates, so a name already ending in `_total` is not doubled), while the OTLP push path uses the
instrument name verbatim. So the instruments are named `dwarf_flows_started` etc., and a Prometheus query
references them as `dwarf_flows_started_total`. Do not bake `_total` into a counter's instrument name.

**15 gauges, observable (async)** via a single `RegisterCallback`. The callback runs at metric-collection
time and reads engine state from memory only - **it makes no query, and must not.** A collector may scrape at
any rate and every replica runs the callback, so a query here costs `R x shards x scrape rate`: an O(backlog)
aggregate at the bench's 50 ms collection is 20 aggregates a second per replica per shard, against the database
being measured. The gauges that need the database get it another way (`backlog.go`):

- **`dwarf_steps_pending` / `dwarf_steps_oldest_pending_age_seconds` {shard, priority}** come from one
  `GROUP BY priority` per shard, run every `backlogInterval` (30s, fixed) by the shard's LEADER - the first
  replica in its dispatcher window (`leadsBacklog`) - and cached. One aggregate per shard every 30s is small
  against a band scan reading the same rows several times a second. The loop runs nothing until a collection
  has set `backlogWanted`, so an engine with no meter provider pays nothing. A band reported once and now empty
  is cached as ZERO, not dropped: an absent series holds its last value on a chart until it goes stale. A failed
  or timed-out refresh keeps the previous reading, but only for `backlogStaleIntervals` (three); past that the
  series goes absent rather than holding a count that froze while the backlog grew. Each replica decides
  leadership from its own registry reading on its own tick, so a handoff can leave two reporters for up to an
  interval (the documented `max` absorbs it) or none (the series is absent for up to an interval).
- **`dwarf_steps_dispatch_lag_seconds` {shard}** rides the band scan: `ScanBand` selects
  `DATE_DIFF_MILLIS(NOW_UTC(), MIN(not_before))` beside the aggregates it already computes and the piston publishes the max
  (`Piston.DispatchLag`), plus the time since the scan was sent, so a stalled tally loop reads as rising lag.
  Reported by each dispatcher of the shard; the max across them covers the shard, because a class its owner is
  not serving enters a peer's scan past the steal grace. The bands the scan walked, up to the plan's reach as
  the standing tallies give it (`planner.Reach`) - the per-band pair covers the rest - so a band past the reach
  is left out, since strict priority, not dispatch, is holding that work. A healthy shard reads up to one tally
  period, not zero.
- **Both ages are measured from `not_before`, not `created_at`.** A step that slept or backed off has been
  waiting only since it came due; from `created_at` an hour-long sleep reads as an hour of backlog the moment
  it wakes. `created_at` stays the fairness ordering key in the scan. This holds only because every write that
  returns a step to `pending` stamps `not_before` (`internal/migrations/CLAUDE.md`).

**Two gauges are deliberately absent and should not be added without a new argument.**
`dwarf_task_concurrency_running` {task_url} costs another per-shard aggregate, carries the least bounded
label in the set, and asks a
question that belongs to the host: per-downstream concurrency turns on the account/tenant identity the
engine structurally cannot see, which is the same rule that keeps backpressure out of the engine (see
"Backpressure is the task's or host's job"). `dwarf_steps_fairness_keys` would be a per-replica sample of the
LAST plan's distinct-key count, read from `planner.LastBand()`; it drives no decision and alarms on nothing.
`LastBand` itself exists because the piston's and the planner's own tests assert on it.

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
- **The ref bytes come from the dispatch resolve's return (`resolveDispatchRefs`, over
  `Linker.ResolveShared`), NOT from `dwarf_state_read_bytes`.** The two counts
  answer different questions and disagree in both directions: the read metric counts whole anchor ROWS (the
  database's byte throughput) and is ZERO on a resolve-cache hit, while this needs the bytes that ended up in
  the carrier. A fan-out resolves the same anchor for every branch - one miss, N-1 hits - so using the read
  count would report a large carried document as nothing on all but the first branch, in the one measurement
  the gauge exists to make. This is why the Linker's `Resolve`/`ResolveShared` return a materialized-bytes count
  at all; every caller but the dispatch path discards it.
- **It measures the WIRE form, and that is the point, not a limitation.** The decoded maps are what occupy
  the heap, and their size cannot be had cheaply (re-marshalling costs a full encode per dispatch and still
  reports wire size; walking the structure is expensive and approximate). Measuring the input bytes makes the
  gauge *invariant to how state is represented in memory*, so read against the Go heap it yields the decode
  expansion factor - and a change to that representation shows up as **gauge flat, heap down**, which is
  strictly stronger evidence than a single number moving. A gauge that fell along with the heap could not
  distinguish "we removed the expansion" from "we did less work".

They also close a blind spot: `SetWorkers`' godoc concedes that each in-flight step holds its state map, a
bound the engine cannot see, and the turnstile lets the crew grow to thousands of
goroutines for long tasks - so held state x crew size is a memory ceiling nothing else reports.

**The gauges are of two kinds and they aggregate differently - do not paper over this.** `queue_depth`,
the db-phase count and the two `state_in_flight` gauges are genuinely **per-replica** (read from this
replica's memory): sum them. The three backlog gauges are **per-shard**: a fact about a shard's database,
reported only by the replicas responsible for that shard, and possibly by more than one of them (two
dispatchers; an old and a new leader mid-handoff). Summing them double-counts a shard, so they aggregate with
`max` per shard - and `dwarf_steps_pending` is then summed across shards. Each instrument's *description*
states its kind, because that is what an operator building a panel actually reads. The callback is
`Unregister`ed first thing in `drainRuntime`, and the backlog loop is cancelled right after it, so neither
touches a closing database.

**There is no `_peak` companion to `dwarf_db_phase_workers`; do not add one.** A
high-water mark held since process start goes CONSTANT within hours on a long-lived replica, answering
nothing about now, while `max_over_time` on the plain gauge gives a WINDOWED peak the backend computes for
free. It also carried a footgun the gauge does not: the two roles peak at different moments, so summing
them reported a mark that never occurred.

**Fidelity choices:** `flows_ended` counts every terminal status it is called with - completed
(`completeFlow`), failed (`failStep`, and the cohort-resolution path in `processStep` that fails a flow
without going through it), and terminated (`terminateSubtree`, per flow of the tree it terminalized). Do not
narrow it to `completed`: that breaks it in two ways at once - the `status` attribute has a single value, so
`sum by (status)` silently answers a completed/failed/terminated question with completions alone, and the
in-flight panel this metric exists for - `flows_started` minus this - drifts upward permanently by every flow
that did not finish cleanly. Both halves are pinned by `TestMetrics_EndedCountsEveryTerminalStatus`, which
fails on all three of its assertions against a completions-only counter (it exercises the
completed/failed/terminated shape). The terminate path counts only the flows whose own UPDATE matched (the
batch reads each flow's `RowsAffected`), so a flow that completed between the tree scan and the lock is counted
once, by its own path (`TestMetrics_TerminateCountsOnlyTheFlowsItEnded`).
`cancelled` (a graceful `Cancel` whose every loss was a cancellation) is tallied the same way, from both
`failStep` and the cohort-fail path. **Every path that starts a flow must call
`metricFlowStarted`** - `Create`, `Continue`, AND `Fork` (which builds its new root through its own
`INSERT...SELECT` clone and so is the easy one to miss): a fork's completion runs through the same `completeFlow`
that increments `flows_ended`, so a missing start makes the standard in-flight panel
(`started - ended`) drift negative by one per fork. **Fork counts every flow it clones `running`** - the new root
AND each subgraph flow on the path down to a fork step inside a subgraph, since each of them ends through
`completeFlow` (`TestMetrics_StartedAndEndedBalanceAcrossTrees`). Subgraph flows are counted too - the start
path and `completeFlow` run for them - so no `surgraph_flow_id` filter; the `workflow` label lets dashboards
slice root-vs-subgraph. `TestMetrics_EmittedOnRun` pins emission with an in-memory SDK `ManualReader`.
Both lifecycle counters carry the flow's `priority`, which is immutable for the flow's life, so the panel
subtracts per band. **Every end path must label with it too**, and each reads it from a flow-row read it
already makes rather than a query of its own: `completeFlow`'s lock grab, `dynamicSubgraphParent` (failStep),
`allSubgraphFlows`' tree scan (terminate), and `processStep`'s flow read (the cohort-fail path). An end counted
under a different priority than its start leaves both bands drifting.

## Tracing (`engine/tracing.go`)

The engine is OTEL-native for tracing, symmetric with metrics: `SetTracerProvider(tp)` overrides the global
`otel.GetTracerProvider()` (no-op unless the host configures the SDK), and the engine creates spans from
`tp.Tracer("github.com/microbus-io/dwarf")` (same scope as metrics; service identity lives in the provider's
Resource, not span attributes). The host injects **only** the provider - no span code, no `trace_parent` handling.

**Resolved once per Startup, before the shards open, and the same provider goes to sequel** (`initTracer` ->
`resolveTracerProvider`). The injected provider wins, else the global; **OTel's untouched stand-in - what
`otel.GetTracerProvider()` returns before any SDK is installed - is replaced by `noop.NewTracerProvider()`,
wherever it came from** (the README's `SetTracerProvider(otel.GetTracerProvider())` passes it explicitly).
- **Why:** the stand-in can start forwarding to a provider installed later, so it builds a span per call while
  nothing records - 3 allocations per span against at most 1 for the no-op (`TestTracing_ResolveTracerProvider`),
  and sequel opens one per query, transaction and batch, which is most of the cost.
- **A stand-in injected before the host installed its SDK forwards to it by Startup,** so the resolve reads the
  global for it as well, and an installed SDK wins.
- **The cost:** a tracer installed after Startup is never reached (the stand-in would have forwarded to it). The
  public docs tell hosts to install it first; a host that truly needs late binding wraps the global in its own type.
- **The stand-in is matched by OTel's internal type name, which fails safe.** Do not match it by a pointer
  captured at package init: a tracer another package's `init` installed before this one's would be captured as
  "the default" and replaced by the no-op, silently. A renamed type only fails to match, keeping the stand-in.
  `TestTracing_OTelStandInIsRecognized` catches a rename at upgrade; it reads the process-global provider, so no
  engine-package test may install one.

Under the no-op tracer every site below is free.

**Two span sites, persisted across replicas via the `trace_parent` column.** A flow's trace context is
minted once and reconstructed on every step dispatch (which may land on any replica), so it must survive
in the database - hence `trace_parent` is a **first-class dwarf-owned column** (the honest asymmetry vs.
metrics: spans need cross-replica continuity, metrics don't).

- **Root "workflow" span at `Create`** (`mintWorkflowSpan`, called from `createWithGraph`, and by `Fork` for
  its new root flow). The span is created, `End()`ed immediately, and its W3C context serialized into the
  flow's `trace_parent` column (`extractTraceParent`). Top-level `Create`/`Continue` and `Fork` mint it
  **detached** (`trace.ContextWithSpan(ctx, nil)` strips any ambient request span) so each flow - each
  `Continue` turn and each fork - roots its own fresh trace rather than nesting under the request that created it.
- **Per-step span in `processStep`**, named by the task. The stored `trace_parent` is reconstructed
  (`injectTraceParent`) as the parent, the span is started with `workflow.id` and `workflow.name`
  attributes, and the span's context is **placed on the `ExecuteTask`'s ctx** so the task's own
  downstream spans nest under it automatically. The span records the dispatch error
  (`recordSpanError` → `RecordError`+`SetStatus(codes.Error)`) when the executor returns one.

**Telemetry carries the token-free correlation id, never the flowKey.** `workflow.id` is
`keys.CorrelationID(shard, flowID)` = `"{shard}-{flowID}"`, **not** the flowKey. The flowKey's last
segment is a random token that is a *bearer write-capability* (`Resume`/`Terminate`/`Fork`/… gate only on
`flow_id`+`flow_token`+`created_hour`), and a trace backend is typically readable far more broadly than the workflow
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
ID. `createSubgraphFlow` does not read the parent flow's `trace_parent` column (it uses the live caller span
instead); the parent's baggage is passed into the child's INSERT.

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
`root_flow_id`. This closes the strand race (a `Delete`/`Purge` deleting steps while a `Resume` revives the flow)
by construction - no rows are deleted where a lifecycle op could interleave (see "Deletion race gate" below) - and
makes a disposable flow's **outcome observable** during its grace window.

- **`FlowOptions.DeleteOnCompletion`** - the author declares a flow fire-and-forget (durable-execution jobs whose
  output and history are not needed). On success `completeFlow` stamps `delete_after_ms = deletionGrace` (hardcoded
  **1 min**; a per-flow duration would be retention policy, out of scope) **in the same transaction** that marks the
  flow `completed`. An *event* trigger on success, not a clock: `failed`/`terminated`/`cancelled`/`interrupted` flows
  are **never**
  scheduled (a failed disposable job is exactly the one to keep as a `Fork` source). Root-only (`surgraph_flow_id=0`),
  not inherited by children (the reaper sweeps descendants via `root_flow_id`). During the grace window the flow stays
  `completed` and its **outcome is observable**: `Snapshot`/`Await`/`Run` return the completed `FlowOutcome` - this is
  how a caller learns a disposable flow's result, since there is no stop callback. It is nonetheless *logically*
  gone: excluded from `List`, and `History` 404s (the full step detail is what the flow is discarding). After the
  window the reaper removes it and reads 404. (`Snapshot`/`Await` deliberately serve the outcome rather than 404 - a
  hard-immediate-404 for a redaction-critical `Delete` is a deferred sub-decision.)

For operator-driven retention (both mark, do not delete inline):

- **`Delete(flowKey)`** stamps `delete_after_ms = 1` (due immediately) on the root after the read-guards (404 on
  unknown key, 400 on a subgraph-child key, 409 on a `running` flow). An `interrupted` flow is first terminated
  through `terminateSubtree`, exactly as `Terminate` does it - deleting a pending approval *is* terminating it. **Do
  not flip only the root's status instead:** the tree's interrupted subgraph children, and any live sibling child,
  would stay non-terminal until reaped - never counted in `dwarf_flows_ended`, so `started - ended` drifts up by
  one per flow, and a live child keeps executing in the meantime. Pinned by
  `TestMetrics_StartedAndEndedBalanceAcrossTrees`. The stamp then matches only a terminal root. Already-scheduled
  (`delete_after_ms > 0`) is idempotent-success. The reaper sweeps the subtree.
- **`Purge(Query)`** marks all matching roots with one set-based UPDATE per shard: it `SELECT DISTINCT`s candidate
  roots (`f.surgraph_flow_id=0 AND f.status<>'running' AND f.delete_after_ms=0`, capped at `purgeCap` **4096**, ids
  embedded as integer literals to dodge the per-driver bind-param ceiling), terminates each `interrupted` one
  through `terminateSubtree` as `Delete` does, then `UPDATE ... SET delete_after_ms=1 WHERE flow_id IN (ids) AND
  status IN (terminal) AND delete_after_ms=0`, where a root selected as interrupted must now be `terminated` - one a
  Resume revived may have completed since, and no longer matches the query. Returns the count **marked** (reaped
  shortly after). Same `Query` shape as `List`; **rejects** `IncludeSubgraphs` with 400.

**The reaper** (`reapDueFlows`, `reaper.go`) runs on its own dedicated goroutine + ~1min ticker (a single goroutine,
inherently non-overlapping), stopped by `drainRuntime` cancelling its ctx. On each shard this replica DISPATCHES (a
reader replica reaps nothing) it loops `reapBatch`-sized batches (512 roots, kept small because a batch's
transaction holds back vacuum while it runs: with 64 KB states a flow took ~3.6 ms to delete on PostgreSQL 16, so a
4096-root batch held one for tens of seconds):
`SELECT` due roots (`delete_after_ms>0 AND surgraph_flow_id=0 AND DATE_ADD_MILLIS(updated_at, delete_after_ms)<=NOW`),
then a two-statement tree delete (`DELETE dwarf_steps WHERE flow_id IN (SELECT flow_id FROM dwarf_flows WHERE
root_flow_id IN (ids))`, then `DELETE dwarf_flows WHERE root_flow_id IN (ids)`) - set-based, **no N+1**, removing each
root plus its descendants. The cancel abandons a pass mid-statement; the batch in flight is one transaction and rolls
back whole (see "Shutdown ordering"). No startup/shutdown/wake passes: deletion is latency-tolerant, so a flow
that came due while a replica was down is removed on the next tick (single-replica) or by a peer's tick.

**The tree delete is unconditional on descendant status, deliberately** - do not 409 on a `running` subgraph
descendant. The deferred path stamps only the root (whose own status is 409-guarded, so a non-terminal root is never
stamped) and the reaper deletes the whole
`root_flow_id` tree regardless of descendant status. The only running descendant a terminal-rooted tree can hold is
an **orphaned child** (a live child whose parent already terminalized, which only a bug leaves - see
`recoverOrphanedSubgraphChildren`), a bug-state row the wedge sweep would terminate anyway. Deleting it is safe: a
worker mid-dispatch on the orphan no-ops via the lease fence (claim/write matches zero rows once the tree is gone),
so no strand and no corruption. Whichever of the reaper and the wedge sweep reaches the orphan first wins. The
reaper therefore does **not** reguard on descendant status. Backed by a
partial index `idx_dwarf_flows_delete_after WHERE delete_after_ms > 0` (full on mysql) that narrows the due scan to
the small in-window subset. (`deletionGrace`/`reapInterval` are Engine fields, not constants, only so a test can
shorten them -
engine-package tests force a reap via `reapDueFlows`; fixtures verify the observable public contract via `List`/
`History`/`Snapshot`.)

**Deletion race gate.** Because deletion is an *orthogonal column*, not a status, stamping `delete_after_ms`
alone does not serialize against `Resume`. The invariant that keeps the strand race closed is
**`delete_after_ms > 0 ⟹ terminal status`**, and the reaper reaps only terminal-rooted trees: DeleteOnCompletion
stamps a `completed` root (immutable); `Delete`/`Purge` of a terminal flow stamp an immutable row; `Delete`/`Purge`
of an `interrupted` flow terminate its tree first (`terminateSubtree`, which writes the root's terminal status under
its row lock), mutually exclusive with `Resume`'s **root-flow gate** (row lock; exactly one wins, loser 409s), and
the stamp matches only a terminal root. So a live `delete_after_ms` never coexists
with a resumable flow, and no steps are ever deleted where a `Resume` could interleave.

The mutual exclusion is not automatic - `resume`'s step writes (leaf `→pending`, ancestor re-park) are
*unconditional* on `WHERE status='interrupted'` at the **step** level, and `Delete`/`Terminate` terminalize the
**flow** row without touching steps, so those step writes still match after a `Delete` won. Left ungated,
`resume` would re-park + reset the leaf, match 0 rows on its `→running` flow update (the flow is now
`terminated`), and still return success - a resume that did not take effect reported as if it had, leaving a
transient terminated-flow-with-non-terminal-steps until the reaper mops it. So `resume`'s transaction carries a
dedicated **gate lock** on the root flow (`lockFlowRows` on `flow_id=<root> AND status='interrupted'`) after its
step writes: a zero-row match means `Delete`/`Terminate` terminalized the root first, so the whole transaction
rolls back (undoing the step writes) and `resume` returns 409. The lock is placed *after* the step writes to
preserve `resume`'s steps-first lock order (shared with `Terminate`). This gate is
distinct from the `→running` chain-flow update, which legitimately matches 0 rows when a *sibling* interrupt
still holds the flow (fan-out resume-one-at-a-time), so it cannot serve as the race gate.

**Reads/derivations of a `delete_after_ms > 0` flow.** `Snapshot`/`Await` serve the outcome (the observability win);
`List` and `History` exclude it; `Continue` and `Fork` **409** (each names a specific doomed flow); `Resume` 409s and `Terminate` is a nil no-op
(terminal); `Delete` is idempotent-success.

Both share filter clauses with `List`. The `Query.StoppedTaskName` filter joins `dwarf_steps` and matches the `task_name`
of the step the flow stopped at, so it matches no running flow. `Query.OlderThan`/`NewerThan` are database-anchored
(`f.updated_at < DATE_ADD_MILLIS(NOW_UTC(), -ms)` etc.) and compose. `Query.FairnessKey` filters on the
engine-native `f.fairness_key` (the host typically sets it to the tenant, so "list tenant X" is "list
`fairness_key = X`"); `Query.Priority` narrows to one scheduling band. Empty key / zero priority disable their
filters. The engine models no tenant concept of its own.

## Concurrency and Crash Recovery

The engine uses SQL transactions for multi-statement operations and `lease_expires` for crash recovery.

### Host-call panic isolation

The host runs **in-process**, so a panic in host code propagates straight into the engine's own goroutines - and
that is the reason to defend: with no process boundary, one buggy task handler would otherwise take down every flow
sharing the replica. Both `Host` calls are wrapped in `errors.CatchPanic` at their **call boundary** (not
only at the worker loop):

- **`ExecuteTask`** (`dispatch.go`) - the panic becomes the call's `error` and flows through the **normal
  disposition**: routed via `onError` if one exists, else `failStep`. This is the load-bearing case. The crew's
  worker loop *already* wraps its callback (and so `processStep`) in `CatchPanic` (`internal/workers/crew.go`), so
  the process never died - but that catch is too coarse: a panic there unwinds the whole `processStep` stack *past* the error routing, leaving the leased step stuck
  `running` until lease expiry (`budget + leaseMargin`), which re-dispatches and re-panics - a slow crash-loop that
  also skips the author's `onError`. Catching at the boundary turns a panic into a clean, immediate step failure
  (lease released), identical to a returned error. `errors.CatchPanic` captures the stack trace into the error for
  diagnosis.
- **`LoadGraph`** (`operations.go` at `Create`, `dispatch.go` at subgraph spawn) - converted to an error
  return. The subgraph-spawn site runs inside `processStep`, so without this it would wedge the caller step
  exactly like `ExecuteTask`; the `Create` site fails the `Create` call cleanly instead of unwinding into the
  host's own frame.

The crew's worker-loop `CatchPanic` stays as **defense-in-depth** for panics in *engine* code on the worker
path (transition evaluation, fan-in, etc.) outside any host call. **Nothing else contains a panic**: the
piston and its pipeline loops, the planner, the peer Sonars, and the engine's background loops (reconcile,
reaper, recovery sweeps, backlog gauge, latch poll) run without a recover, so a panic in any of them takes down the
replica. **That is deliberate; do not add a catch there.** Those loops call no `Host` method - the only `Host` code
the engine runs is the three wrapped call sites above - so a panic in them is an engine bug, and a crash is what
gets it found and fixed; a caught-and-logged panic in a loop that retries every cycle would hide it. The one
exception is the injected observability providers: the logger's `slog.Handler` and the meter and tracer providers
are host code, and they run on those goroutines too (a Sonar's log line, a piston's histogram record, a recovery
sweep's span). A panic there is not caught either - a host that injects a panicking handler owns that crash.
`fixtures/panicflow_test.go` pins both task-panic
outcomes (no `onError` → flow `failed`; with `onError` → routed to the handler and `completed`), using a bounded
`Await` that would time out if the panic had wedged the step instead of failing it.

### Worker context (the engine lifetime)

The engine's lifetime context (`e.lifetimeCtx`) is created at Startup and cancelled only after `Shutdown` has
drained everything. The workers run on a non-cancelable copy of it, so a step's writes are never interrupted by
ctx cancellation. The backlog refresh, the recovery loop, the reaper, the latch detector, the pistons and the
Sonars each run on a child of it that the drain cancels when it stops that goroutine (see "Shutdown ordering"). The only *cancellable*, time-bounded ctx on a
worker is the `ExecuteTask` call: `executeTask` derives it from the worker's ctx with the step's `time_budget_ms`.

**The crew runs on `context.WithoutCancel(e.lifetimeCtx)`, and it must stay non-cancelable.** Since no worker
can observe the lifetime ctx's cancellation, cancelability buys the crew nothing - and it costs per QUERY: pgx
registers a `context.AfterFunc` watch on a cancelable ctx for every query, and `database/sql` gives every `Rows`
its own `WithCancel` plus an `awaitDone` goroutine (a `Tx` gets both regardless). On one shared cancelable parent
every one of those registrations, from every worker, takes that parent's mutex: a per-process serial point that
gets SLOWER with more cores. Measured on that per-step pattern (6 queries, one Tx, one task timeout; M1 Pro, 10
procs): a shared parent ran 80-92k steps/s, 2.3x slower than at 1 proc, with 44 allocs and 7 goroutines per
step; a per-worker child parent 330-450k at the same allocations; non-cancelable 1.2-2.7M at 8 allocs and 1
goroutine. On a c4a-48 at 4 shards this lock was ~10% of CPU and 74% of mutex time, at ~0.9M registrations/s
against a ~1.5M/s ceiling - inside the connection hold, since pgx watches during the query. **Do not give the
crew a cancelable ctx (per-worker children included) to "let shutdown reach the workers"**: the drain's stop
signals are the cache, the gate and `drainStop`, and a per-step deadline belongs on a per-step ctx.

### Shutdown ordering: workers, then recovery/reaper/latch, then the pistons, then the Sonars

There is no timer goroutine and no wake channel to close (see "A cycle is UNCONDITIONAL"), so no send can
race a close and panic a worker mid-`processStep`. `drainRuntime`:

```
close(drainStop)                             // a worker asleep in a persist backoff releases its lease and exits
close(reconcileStop); reconcileWorker.Wait() // in-memory only: nothing in flight to wait for
SetIdle(true) on every piston                // stops the dispatch evidence, so peers take this replica's residue class
arm withdrawal timer                         // gives up dispatcher slots if the drain outlasts a read cadence
closeMetrics()                               // no collection may run against a closing engine
backlogCancel(); backlogWorker.Wait()        // nor the backlog refresh against a closing database
cache.Close(); turnstiles.Close()            // BOTH: a worker parks on one or waits on the other
crew.Drain(); withdrawal.Wait()
recoveryCancel();    recoveryWorker.Wait() // abandons a sweep statement in flight
reaperCancel();      reaperWorker.Wait()   // ditto, a reap batch
latchCancel();       latchWorker.Wait()    // the detector stops before the board closes
pistonCancel();      pistonPool.Wait()   // ctx is the pistons' only stop signal
sonarCancel();       sonarPool.Wait()    // ditto - and they must outlive the pistons
leaveFleet()                             // only now is the last possible beat behind us
latches.Close(); lifetimeCancel()
```

**The Sonars drain LAST, after the pistons, and that ordering is load-bearing.** This replica must stay
registered while its workers are still executing steps, because every one of them drains against pools each
peer sized for a fleet that still includes us. Only once the Sonars' loops have returned is the last
possible beat behind us, which is what makes `leaveFleet`'s delete final: a beat only ever UPDATEs, so
nothing can resurrect a row deleted after its loop stopped.

Both run on **children** of the lifetime ctx (`pistonCancel`, `sonarCancel`), not the lifetime ctx itself:
that one is deliberately left live until every goroutine has drained, while `piston.Run` and `Sonar.Run` end
on ctx alone. Neither needs a second signal - a cycle is a pure read and a beat is one idempotent UPDATE, so
abandoning either mid-flight strands nothing.

**The recovery loop, the reaper and the latch detector each run on a child of their own, and the drain stops
each by cancelling it**, so the drain stops waiting for the statement a pass has in flight. Do not stop them with a signal while
they run on the lifetime ctx: the drain then waits out the slowest background statement (measured: a stop held
9+ minutes by the orphan, lease-recovery and reap statements on a bloated `dwarf_steps`), and `Shutdown` is
bounded by a sweep rather than by the steps in flight, as its godoc promises.
- **Abandoning a pass strands nothing.** Each write a pass makes is one statement or one transaction - a lease
  reset, an orphan rewind, a subtree terminate, a caller revive or re-arm, a reap batch - so an abandoned one
  lands whole or not at all, and every repair is idempotent and redone by a peer or the next start. A commit the
  cancel races can land and still report an error; the cost is a skipped signal, doorbell or counter on a
  replica that is stopping. The detector only reads, and a chunk it could not finish settles nothing.
- **The cancel stops the CLIENT, not necessarily the server.** On SQL Server and SQLite it interrupts the
  statement. The PostgreSQL and MySQL drivers close the connection instead and send no cancel, so the server can
  run an abandoned statement to its end: a long sweep read keeps its snapshot (and the vacuum horizon) for the
  rest of its run, a lock-waiting reset can still be granted and commit, and a reap batch runs before it rolls
  back. All of that is safe - each statement repeats its own guard - but it outlives `Shutdown`, so a rolling
  restart can overlap it with the replacement's entry sweep.
- **A cancelled pass is not a failure.** It stops at the next hit, and the failures the cancellation causes are not
  logged (`logSweepError`, and the detector's own `ctx.Err()` check), so a routine stop raises no error lines.
- **This is cancellation on stop, not a deadline**, so the decision against deadlines on background reads ("A long
  read holds back vacuum") does not reach it: a deadline fails a pass the running engine depends on, whereas
  nothing after the stop depends on it.

Pinned by `TestShutdown_AbandonsASweepInFlight`: lease recovery held at a checkpoint between its two statements,
and (on the row-locking dialects) its reset blocked on a row lock the test holds.

`leaveFleet` runs on `context.Background()` rather than the lifetime ctx, which is cancelled moments later.

**The pistons idle at the top of the drain, and are cancelled only near its end.** Their output goes nowhere
once the cache closes, and a turning piston keeps the beat stamping `dispatched_at`, which keeps this replica in
every peer's work divisor: its residue class would reach a worker only through a peer's steal, after the steal
grace, for as long as an in-flight task holds the drain open. Idle, its `Liveness` stops the stamp and the class
passes within the dispatch window. Idling must follow the reconcile loop's stop, which is the only thing that
re-derives the mode. Pinned by `TestDispatchers_DrainStopsClaimingDispatch` and, from the peer's side with its
steal switched off, `TestDispatchers_DrainingReplicasClassPassesToAPeer`.

A `cache.Refill` into an already-closed cache is a no-op. Dedicated stop signals, and no wake channels at all,
keep the drain free of ordering hazards.

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
in `Delete`/`Terminate`, in the `cohort_arrivals` bump, in the resume leaf reset, in the reaper's deletes.

What is **not** delegated to the latch, and is still checked explicitly at each site: **`RowsAffected()==0`** - a
zero-row match is a *semantic* outcome (a lost CAS, a terminal-status guard, the resume race gate), not an error, and
the latch says nothing about it.

### Transactions

The multi-statement write paths each run under one `db.Transact`. **There is no single engine-wide row-lock
order** - two disciplines coexist deliberately; do not describe or "restore" a uniform "steps-first-then-flow"
ordering:

- **Flow-row-first (write-first).** Every flow-*advancing* / flow-*terminating* transaction takes the
  `dwarf_flows` row's write lock as its **first** statement, before touching steps: `processStep`'s transition
  transaction (insert next steps, `dispatch.go`), `completeFlow`, and
  `fireFanInDirect`. The one exception is a **pure cohort arrival** (no successor): its first
  write is the cohort bump on the spawn step row, and only the arrival that resolves the cohort grabs the flow row,
  after the bump - still write-first, and grabbing for every arrival would serialize the whole cohort on one row
  (the rationale is at `pureArrival` in `dispatch.go`). Mandatory - the failure modes are under "Write-ordering &
  lock contention" above. Do not reorder these to read-first.
- **Steps-first.** The lifecycle mutations update `dwarf_steps` before `dwarf_flows`: `Resume`, `Terminate`,
  `failStep`, `handleInterrupt`, and `Delete`/`Purge` (the deletes run steps-before-flows, ascending id).
  `handleInterrupt` belongs here despite advancing the flow (running→interrupted): interrupt is
  **non-terminating** and marks no step `completed` in a prior standalone UPDATE, so it carries **no** orphan-strand
  obligation - its only write-first requirement is that the *first* statement be a write (the `UPDATE dwarf_steps`
  satisfies it, keeping the SQLite deadlock closed). It is deliberately steps-first to match `Resume`/`Terminate`,
  which walk the *same* surgraph chain, so the two never lock that chain's flow+step rows in opposite order (a
  flow-first `handleInterrupt` forms a deadlock cycle with `Resume` - see below).

`Create` inserts the flow row (already `running`), then the entry step, then - off PostgreSQL - fixes a new root's
root id - flow-row-first in statement order, but into a **brand-new** flow whose rows no concurrent transaction can reach until commit, so it
honors no existing-row lock order and cannot cycle. `Fork` performs its entire tree clone (an iterative worklist) in one
transaction (so a crash mid-clone rolls back), and the leaf fork step is held `created` until the mapping is complete.
**Fork's transaction opens with `lockFlowRows` on the origin root, because the clone reads before it writes.** On
SQLite a read-first transaction cannot upgrade against a concurrent writer and gets `SQLITE_BUSY` (`SQLITE_LOCKED`
under a shared cache, as in the test suite) at once on every `Transact` retry - measured as ~1 Fork in 8 failing under the bench soak. Pinned by
`TestFork_TransactionOpensWithAWrite`, which reads each attempt's first statement off the database spans; the soak
in `fixtures/forkconcurrentflow_test.go` catches a read-first Fork only now and then. Do not try to make that race
deterministic with a checkpoint inside the clone: modernc waits on a shared-cache lock with `sqlite3_unlock_notify`,
which cannot be interrupted, so a probing writer blocks rather than being refused, and blocked cannot be told from
held without a timer. Every other `Transact` in the engine already opens with a write; a new one
that reads first needs the same. The lock also re-checks what Fork read before its transaction: it matches only
`delete_after_ms=0`, so a Delete or Purge that stamped the origin in between is still a 409, and an origin the
reaper removed is a 404 (`TestFork_DeletionAfterThePreCheck`).

**The two disciplines still cross in one place on row-locking engines, and that is tolerated, not eliminated.**
On MySQL `REPEATABLE READ` and SQL Server without RCSI, a flow-row-first transaction and a steps-first one can
acquire the same flow and step rows in opposite orders and form a genuine lock cycle - the surviving case is
`Terminate` (steps + gap locks, then the flow row) vs the transition tx (flow row, then step insert/`successor_id`)
on one flow. It is **recoverable**: both paths run under `db.Transact`, whose lock-contention retry rolls the
loser back and re-runs the closure, so the cycle degrades to a transient rollback visible only on retry
exhaustion - and the transition side is further backstopped by the `processStep` lock-contention defer (rewinds
the leased step and re-polls) and, last-resort, lease recovery. The recommended `READ COMMITTED` (MySQL) / `RCSI`
(SQL Server) settings drop the gap locks and remove it outright. This last cross is *not* cheaply reconcilable:
forcing the flow-terminating transactions steps-first would reintroduce the SQLite deadlock and the orphan-strand,
and forcing `Terminate` flow-first buys nothing on SQLite (it serializes writes, so its only exposed deadlock is the
read-first-upgrade one the write-first rule already closes) while giving `Terminate` a wider flow-row lock hold.
`Terminate`'s flow-row lock (below) widens this same cross: on MySQL at `REPEATABLE READ` a transition holding the
flow row and waiting on `Terminate`'s gap lock cycles with `Terminate` waiting on that flow row - observed in
`TestTerminateVsSubgraphSpawn_Deterministic`, retry-recovered like the rest.

**`Terminate` takes steps, then flows, then steps AGAIN** (why, under the orphaned-child rules in "Background
Recovery"), **and the second pass is the one exception to steps-first.** It can only reach a step created after the first one - every older step is already locked by it - so it crosses
only a transaction that took such a just-created step first and then wants the flow row: a worker that claimed it
and is already in `failStep` or `handleInterrupt` (both steps, then flows). That needs the step inserted, claimed,
executed and failed or interrupted inside `Terminate`'s own transaction; the cycle is retry-recovered by
`db.Transact` like the cross above. Do not drop the second pass to remove it - it is what keeps a step created
mid-terminate from running on, and spawning, under a terminated flow.

**Do not make `handleInterrupt` flow-first.** A flow-first `handleInterrupt` against steps-first `Resume` on a
shared interrupt chain is the more dangerous cross, because it locks *two* overlapping resources (chain flow rows
**and** chain step rows) in opposite order. With `handleInterrupt`, `Resume`, and `Terminate` all acquiring that
chain's existing steps before its flows, none can cycle against another over them (`Terminate`'s second step pass,
above, reaches only steps created after its first). Steps-first is safe for `handleInterrupt`
precisely because interrupt is non-terminating (no orphan-strand obligation), and it shares only the *single* flow
row with the flow-first cluster (whose step writes - its own row, new successors, a cohort's spawn and exit rows -
land on completed or new steps, never on the running leaf and parked callers `handleInterrupt` locks), which cannot
form a cycle - the same reason steps-first `Resume` already coexisted with them.

Orthogonal to that tolerated cross (the deadlock stays retry-recovered; the lock order is **unchanged** - the
transition tx still takes the flow row write-first), the transition must not *extend* a flow `Terminate` already
terminalized. Whenever the race resolves in `Terminate`'s favor - or the transition simply re-runs after a
contention rollback against a since-terminated flow - its opening flow-row lock grab (`lockFlowRows`) is **guarded on
non-terminal status** (`AND status NOT IN (completed, failed, terminated, cancelled)`) and bails on zero rows. Without the
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

1. **`recoverExpiredLeases`** (`wedge.go`) - resets `running` steps whose lease expired to `pending`: a plain SELECT
   of the expired ids per shard, then a by-id UPDATE that repeats the predicate. It rings no doorbell: a reset step is due and `pending`, so its shard's next piston cycle selects it
   like any other. Runs on `recoveryLoop` with everything else here - there is no timer goroutine.
2. **Terminal flow check** in `processStep` - after loading flow data, if the flow is `terminated`/`cancelled`/`failed`/
   `completed`, sets the step to that status and returns. Catches races where the flow went terminal before the step
   was updated.
3. **Orphan flow repair** (`recoverOrphanedFlows`, `wedge.go`) - finds a `running` flow that is stranded:
   **every step terminal**, and rewinds to `pending` every `completed` step still carrying **`advancing=1`** whose
   lease has lapsed. That is the shape a worker leaves when
   it dies between the completion write and the transition transaction (see "processStep - Normal Completion"
   below) - the only shape lease recovery cannot reach, since the step is `completed`. Dispatch then **replays** it
   (see *A rewound step is REPLAYED* below): the task does not run again.

   *The `advancing` flag is what makes this safe, and it costs no round trip.* The completion write sets it in the
   statement it already runs; every transition transaction that commits while the flow stays live clears it in a
   statement that already writes the dispatched row (see "Lease fencing"), and the transaction that terminalizes
   the flow leaves it - harmless, since the sweep only rewinds a `running` flow and a terminal flow never becomes
   running again. So `advancing=1` on a completed step of a running flow means exactly "this transition never
   committed", and the sweep re-derives nothing: it never decides which transition should have been taken, and a
   flow with no flagged step is never touched, so a false positive cannot double-advance it. **Do not re-drive a
   stranded flow from the sweep by evaluating its transitions** - that duplicates `processStep`'s transition logic
   and, without the flag, cannot tell a lost transition from a committed one (a counted cohort arrival is also
   `completed` with `successor_id=0` on a running flow).

   *A rewound step is REPLAYED, not re-run.* The transition is a pure function of the frozen graph, the step's
   `state`, its `changes`, and three control facts the task's return carried only in memory: its `Goto` target, its
   `Sleep`, and whether it was error-routed. The completion write records those three in `advance_goto`,
   `advance_sleep_until` and `advance_on_error`, in the statement that sets the flag (the Cancel-covered reroute's
   follow-up write records the route). The claim returns the flag and the three columns; when the flag is set,
   `processStep` builds the carrier as usual - the claim reads `changes` as the prior changes, so the recorded
   output is already in it - sets the recorded `Goto`/`Sleep` on it, skips the host call and the claim-time Cancel
   preempt, and resumes at the completion write. Everything from there is the ordinary path, so the replay commits
   exactly what the dead worker would have. Three consequences hold the design up:
   - **The task's side effects fire once**, and the outcome cannot mix a decision from one run with state from
     another - which is what lets `completeFlowSequential` stay unfenced (see "Lease fencing").
   - **The task's own output is not merged into its input a second time.** A re-run would see its previous
     `changes` as `priorChanges`, so a task computing `x = state.x + 1` would apply twice.
   - **A Cancel that marked the rewound step lands at the completion write**, as for a step that was running when
     Cancel landed: its changes stand and it routes to its `onError` handler, or is cancelled without one.
     Preempting it at the claim would treat a step that ran as one that never did. A step already error-routed
     needs nothing more, the same as on the live path.

   The recovery defer's `completed`->`pending` reset and `releaseLease` on drain leave the flag set too, so they
   replay as well. A replay is not counted in `dwarf_steps_executed` (the task ran once), its span carries
   `workflow.replay=true`, and the claim keeps the step's `started_at` (as it does for a retry or a resumed park),
   so its recorded duration and wait are the task's. The `Sleep` is recorded as a DEADLINE (`DATE_ADD_MILLIS(NOW_UTC(), ?)`, on the database
   clock like every timestamp), and the claim returns what remains of it, so a replay keeps the original wake time
   rather than restarting the sleep; one that has already passed applies none. Routing reads the engine's fold
   of the recorded changes (`successorBase`) on a live run and a replay alike, so both route on the same state.

   *A slow worker is excluded, then fenced.* The scan skips a flow whose flagged step still holds an unexpired
   lease (it is still its worker's), and the rewind bumps `lease_seq`, so a worker that outlived its lease rolls
   its transition back at its fenced step-row write. The rewind runs behind a write-first flow-row lock guarded on
   `running`, so a `Terminate` between the scan and the rewind wins. Rewinds count in `dwarf_steps_recovered` (a
   dead worker's step, like lease recovery). A stranded flow with **no** flagged step is a bug signal: logged at
   error level and counted by `dwarf_flows_orphaned{workflow}`, the flow-level sibling of `dwarf_steps_unwedged`,
   and not repaired. That verdict is re-taken under the flow-row lock - still running, every step terminal, none
   flagged - so a flow that went terminal, was repaired by a peer's sweep (every dispatcher of a shard sweeps it), or
   moved on after the unlocked scan is not miscounted. Pinned by
   `TestOrphanSweep_RecoversLostTransition` (a simulated dead worker, repaired by a surviving replica),
   `TestOrphanSweep_ReplaysSlowWorkersStep` (plain, `Goto`, error-routed and `Sleep` replays) and
   `..._OtherTransitions` (`fireFanInDirect`, flow completion) - the task runs once in each -
   `TestFaultSite_RecoveryResetErr` (the defer's own reset failing, then the sweep's repair),
   `TestBumpCohortArrivals_Fence`, and `enginetest.AssertInvariants`' check that no quiesced live flow holds a
   `completed` step still flagged - the signature of a transition path that forgot to clear it.

   **The lease is the repair's only wait, and it runs on every recovery tick**, so the worst case is
   `budget + leaseMargin + recoveryInterval` - the bound lease recovery gives a claimed step. **Do not put an age
   guard on step activity in front of the repair.** A step mid-transition still holds its lease, so the scan
   already skips it, and a live worker that keeps its step past its lease is fenced and its step replayed - one
   transition redone, never the task. Reaching that takes a task that overran its budget or a transition stalled
   past `leaseMargin`: the completion write leaves the lease as the claim set it, so a task finishing within its
   budget reaches its transition with the whole margin, and persist's backoff and the cohort stripe extend the
   lease before waiting. The all-terminal check excludes every legitimate long-wait (a `running` task - even a
   10-minute one with no DB activity - a `pending` sleep/retry, a `running`+parked subgraph caller, an
   `interrupted` human wait, a live fan-out sibling).

   **The alarm keeps an age guard** (`orphanFlowThreshold`, 5m): a stranded flow with no flagged step is counted
   only once no step of it has been touched for that long, so a transient all-terminal state nobody has named is
   not reported as a bug. It is raised only on the heavy pass (every `heavyRepairEvery`-th tick), so an
   unrepairable flow is counted every 5 minutes per dispatcher, not every minute. **Anchor that guard on `dwarf_steps.updated_at`, never on the flow row**: no
   per-step write moves `dwarf_flows.updated_at`, which stays at go-`running` time, so it tracks no per-step progress and is
   permanently satisfied for any flow running past 5m.

   The scan lists the running flows and checks them `inListChunk` at a time, so every chunk probes
   `idx_dwarf_steps_flow_id` per flow and the pass costs running flows x their steps, not the size of `dwarf_steps`. A sleeping flow is a running
   flow, so the sleepers are probed on every pass too: O(running + sleeping).
   **Do not check every running flow in one statement:** once running flows are a large share of
   the table, PostgreSQL hashes them and reads all of `dwarf_steps` once per `NOT EXISTS` (the measurements are at
   `recoverOrphanedFlows`). The flow row offers no shortcut to the flagged step: `stopped_step_id` is 0 while a
   flow runs.
4. **Parked-step wedge sweep** (`recoverWedgedSubgraphParks` + `recoverOrphanedSubgraphChildren`, `wedge.go`,
   called inline from `runRecoverySweep`) - defense in depth for the `parkedSubgraph` park, whose releasing
   condition could in principle never fire (a parked step is invisible to selection, and `parkedSubgraph` is
   invisible to lease recovery too). Runs on the **dedicated recovery goroutine** (`recoveryLoop`) on a plain
   5m cadence (every `heavyRepairEvery`-th tick of `recoveryInterval`); the loop is drained before the pistons in `drainRuntime` since a recovered
   park can re-offer a step. The detector carries a `parkWedgeThreshold` (5m) age guard so steady-state operation
   never trips a
   false positive (the guard sits comfortably beyond normal subgraph-completion latency). Like the orphan-flow repair,
   this auto-recovers because each recovery re-invokes a *normal, status-guarded* mechanism (the
   `parkedSubgraph` revive CAS, or a subtree `Terminate` guarded by `status NOT IN (terminal)`) rather than duplicating
   transition logic, so it is idempotent and harmless under a concurrent resolution, a false positive, or a peer
   replica sweeping the same shard. It runs **two mirror-image detectors** - a wedged caller (child gone) and an
   orphaned child (caller/parent gone):
   - **`parkedSubgraph`** (`recoverWedgedSubgraphParks`): a caller step `running`+`parkedSubgraph` with **no
     non-terminal child** (`surgraph_step_id = step_id`, status created/running/interrupted) is wedged - the child
     reached terminal but the revive was lost, or the child was deleted. The sweep re-drives the release on the
     latest child (`flow_id DESC`): `completeSurgraphFlow` for a completed child, `deliverSubgraphError` for a
     failed/terminated/cancelled/absent one. `deliverSubgraphError` only re-arms the caller with the error, and
     its one statement is that write, so it is write-first. It is never handed a live child: the sweep skips a
     latest child that is non-terminal again (a spawn that landed after its scan), so there is no child to
     terminalize. **The absent-child case** cannot be reached by a crash - the park and the child's insert commit
     in one transaction - so it is defense against a bug or a hand-deleted row; a parked caller carries no lease,
     so nothing else would recover it, and the recovery IS failing the caller. Pinned by
     `TestWedgeSweep_SubgraphCallerWithNoChildFails`. (A fan-out has several caller steps, each its own `surgraph_step_id`, checked
     independently; `flow.Retry` leaves older terminal children whose latest sibling is still active - handled by
     the `NOT EXISTS` + latest-child logic.)
   - **orphaned subgraph child** (`recoverOrphanedSubgraphChildren`) - the **mirror image** of the above: a
     non-terminal child flow (`created`/`running`/`interrupted`) whose *parent flow* is already terminal
     (`completed`/`failed`/`terminated`/`cancelled`). Where the `parkedSubgraph` case is a live caller whose child
     vanished,
     this is a live child whose caller/parent vanished. **Nothing in the engine leaves one, and the sweep is
     defense in depth for a bug that would.** A `Terminate` racing a subgraph spawn cannot, and four rules hold
     that up:
     - **The spawn parks its caller in the transaction that inserts the child.** A park committed on its own
       leaves a window in which the child is inserted under a parent the teardown already finished.
     - **`terminateSubtree` terminates the steps, locks the flow rows, then terminates the steps AGAIN.** A
       transition locks its flow row before inserting a step, so once the rows are held no step can be added -
       but one that committed between the first step UPDATE and the lock was not covered by it, and left running
       it can still spawn (measured on Postgres, SQL Server and MySQL at READ COMMITTED; at REPEATABLE READ
       MySQL's gap locks block the insert instead).
     - **It scans for children inside the transaction, after those writes.** Every live step of the tree is then
       locked, so a spawn has either committed (the scan sees its child) or will find its caller terminated.
     - **ONE scan per transaction; new flows mean roll back and re-run.** On MySQL's REPEATABLE READ the first
       plain read fixes the transaction's snapshot, so a second scan in the same transaction misses a grandchild
       spawned in between - measured as a live grandchild under a terminated child on MySQL and MariaDB. The
       re-run's first write covers the new flows before its own scan takes a fresh snapshot.
     `TestTerminateVsSubgraphSpawn_Deterministic` has a subtest that fails with each rule removed. (A fan-out sibling's
     `failStep` does not produce an orphan either: a subgraph child fails via cohort accounting after every branch
     settles, never eagerly while a sibling is live - see "Failure back to the parent".) The orphan has no path out on its
     own: the terminal root 409s `Resume` and makes `Terminate` a no-op, the child's own key is read-only (400), and
     `recoverWedgedSubgraphParks` is blind because the caller step is *terminal*, not `running`+`parkedSubgraph`.
     The sweep terminates the orphan's whole subtree (`terminateOrphanedSubtree`, which *shares* `Terminate`'s
     transaction via `terminateSubtree` rather than cloning it - no surgraph up-walk is wanted or exists, since the
     ancestor chain is already terminal; it differs only in taking a zero-row flow UPDATE as a benign no-op rather
     than a 409), sharing the parent's terminal fate. An **`interrupted`** parent is deliberately *excluded* (not
     terminal - a `Resume` of the root
     revives that branch and a sibling child under it is healthy); the `parkWedgeThreshold` age guard excludes the
     sub-second window where a just-terminalized parent's sibling child is still being cleaned up by the normal
     completion/error path. It counts under `park_type="orphaned_child"`.
   Each unwedge increments `dwarf_steps_unwedged{park_type}` (the always-on alarm; a nonzero value means a
   latent bug let a step wedge) and logs at error level (silent under the default discard logger, surfaced once a
   host injects one).

### A long read holds back vacuum, and on `dwarf_steps` the slowdown feeds itself

A trap to recognize, not a mechanism: the engine's background reads carry no deadline, by decision. The one
exception is the backlog-gauge aggregate (`backlogQueryTimeout`, 10s): it feeds a gauge, not dispatch, so a
refresh that times out only keeps the previous reading.

**What happens:**
- **Every claim leaves an index entry behind.** The `pending -> running -> terminal` updates change `status`, an
  indexed column, so none is HOT, and each step leaves an entry in the selection index's pending range and its
  running range.
- **An entry becomes removable only once it is dead to EVERY open snapshot.** That holds for marking it LP_DEAD
  during a scan (nbtree README) and for vacuum. So while any statement in the database holds an old snapshot -
  a long read counts, not only an open write transaction - the entries for steps that moved on pile up at the
  HEAD of each fairness key's range, ahead of the live rows.
- **Every background read then slows down, and each one holds back cleanup in turn.** The band scan, the fetch
  and the sweeps all step over those entries with a heap visit each, and each visit grows more expensive as the
  hint bits and commit-log pages it needs go cold. The slow reads then hold their own snapshots, so the loop can
  outlive whatever started it.
- **When it ends, the WAL floods.** Pruning, vacuum and hint-bit setting dirty a backlog of pages, each the first
  touch since a checkpoint, so each writes a full-page image. Checkpoints balloon, WAL segments run out of
  recycled files, and commits queue behind segment creation.
- **Many fairness keys are hit hardest.** The fetch's per-key cap is the HEAVIEST key's demand, so a light key
  never reaches its `LIMIT` and the fetch walks every planned key's whole range. A smaller `LIMIT` does not help,
  since the dead entries come first, and neither does a `created_at <= now` bound, which trims only the tail.

**The engine recovers on its own** once the long statement ends, losing throughput meanwhile and nothing else.

**Why no deadline on background reads (decided):**
- Every value is an assumption about database size. The band scan costs O(due rows at the band) and the orphan
  sweep O(running flows), so a bound that fits one deployment fails every attempt on a larger one.
- A failed tally clears the shard from planning, so a scan that can never finish stops that shard's supply and
  the backlog never drains - a wedge where today there is a slowdown.
- Even the fetch, normally bounded by keys x per-key cap, fails every attempt while something outside the engine
  holds the horizon.
- A fuse (bound only N attempts in a row) removes the wedge but turns a too-small bound into permanent wasted
  work.
- Revisit only with evidence of what starts the loop.

**To diagnose:** read the oldest snapshot holder in `pg_stat_activity` - `backend_xmin` age, `xact_start`,
`query_start`, the query - and the "dead but not yet removable" counts in autovacuum's log. A long read of the
engine's own, an operator session, a host query or a replication slot can each start it. **Server-side bounds
(`statement_timeout`, `idle_in_transaction_session_timeout`, `transaction_timeout`) on the engine's database, role
or DSN are the same deadline set out of the engine's sight, and the docs warn against them:** no value can
anticipate how deep the backlog will grow, so a timeout reproduces the tally wedge above. An idle-in-transaction
timeout also kills transactions that sit idle while a busy replica is slow.

### Per-Function Crash Analysis

- **Create** - one transaction: insert flow (`running`) -> insert entry step (`pending`) -> off PostgreSQL, set
  a new root's own-id `root_flow_id`, then ring the doorbell. A pre-commit crash rolls back entirely (no partial flow); after a
  post-commit crash before the doorbell, the committed `pending` entry step is due, so its shard's next piston cycle
  selects it.
  There is no separate `Start`, so no inert `created` window. Self-healing.
- **Resume** - one transaction (steps -> `pending`, flow -> `running`). A crash after commit but before the
  doorbell leaves a due `pending` leaf for the next piston cycle. Self-healing.
- **Fork** - one transaction clones the whole tree (new flow + step rows, id remap, re-parked ancestor callers), with
  the leaf fork step held `created` until the mapping completes, then enqueue. A pre-commit crash rolls back entirely
  (no partial clone); after a post-commit crash before the doorbell, the next piston cycle selects the `pending` leaf. The original flow is
  read-only throughout, so it is never at risk. Self-healing.
- **Terminate / failStep** - one transaction over the whole surgraph chain. A pre-commit crash rolls back; a post-commit
  crash leaves correct terminal state, `Await` callers discover it on the next poll. Self-healing.
- **processStep - Interrupt** - one transaction. A pre-commit crash rolls back and re-execution produces the interrupt
  again (interrupt-producing tasks should be idempotent). Self-healing.
- **processStep - Normal Completion (with next steps)** - step -> `completed` (a standalone UPDATE), then a separate
  transaction inserts the successors / bumps `cohort_arrivals`, then the doorbell. The gap is a
  wedge window: a `completed` step with no successor is invisible to lease recovery (`running`-only), the parked-step
  wedge sweep (`parkedSubgraph`-only), and the lock-contention reset (`status='running'`-guarded), so the flow would
  strand `running` forever. Not only a ~microsecond crash window - the follow-up transaction can fail *persistently*
  (Transact exhausting contention retries under load, or a non-retryable DB error). The **`processStep` recovery
  defer** closes it: on any error return after the step was marked `completed`, it rolls the step back
  `completed` -> `pending` (guarded `WHERE status='completed'`, retried via Transact) so the normal re-dispatch
  machinery replays the step from its recorded completion and re-evaluates transitions (the failed transaction
  already rolled back its partial writes, so the replay starts clean) - the task does not run again. Same reset
  idiom the defer applies to the `running` -> `pending` lock-contention case, generalized to post-completion. The
  defer runs only on an error return, so a worker that DIES in the window (or whose reset UPDATE loses to a
  contention storm) leaves the step `completed` with `advancing=1`; the orphan sweep (#3) rewinds it on the first
  recovery tick after its lease lapses, and it is replayed the same way.
- **processStep - Flow Completion (no next steps)** - step -> `completed` (the completion write, which sets
  `advancing=1`), then `completeFlow`'s transaction sets the flow `completed`. A crash between leaves a completed,
  flagged step on a `running` flow; the orphan sweep (#3) rewinds it and the replay completes the flow. Self-healing.

### Database Sharding

`SetShard(ShardSpec)` (default: one shard) distributes flows across databases to scale write throughput and reduce
index contention. Indices are sparse (unique, >= 1, not necessarily contiguous - so arbitrary DSNs like cloud RDS
hostnames map cleanly, and a future drained shard's index could retire without renumbering). The sharding
*mechanics* — the sparse index->DB map, the always-parallel `OnEach` cross-shard fan-out,
the not-shard-fault-tolerant contract, and the DSN/test-mode resolution — live in
`internal/database/CLAUDE.md`. This section is the engine-side *semantics*.

**Shard routing & encoding:** external flow IDs encode the shard (`{shard}-{hour}-{flowID}-{token}`); every operation parses
it and routes via `e.db.Shard(n)`. Indices are the declared ones (`>= 1`, sparse); `0` is the "no shard / all
shards" sentinel used by `Query.Shard`.

**Shard affinity:** subgraph flows are created on the parent's shard (avoids cross-shard references during
subgraph completion and history reconstruction). Only top-level flow creation picks a shard - a
weighted random pick over the non-cordoned shards this replica dispatches, when there are any (below, and
"Dispatchers").

**New-flow placement is weighted by `ShardSpec.Weight` (`pickShard`, default 1).** Placement is the engine's
only load-balancing moment - flows are shard-pinned for life (subgraph affinity, continuations, forks)
- so a heterogeneous fleet must be weighted or the smallest shard saturates first while larger ones idle. The
weight is DECLARED, not inferred from the server's size: capacity is not proportional to vCPUs (16 -> 32 vCPU
measured x2.46, 32 -> 64 x1.2), so a vCPU-derived weight would misplace exactly the mixed fleets that need
one. **Do not steer placement by observed shard load** (the planner's tallies, a "power of two choices"
pick). Built and measured locally on two shards weighted 3:1: comparing due-step counts per unit of weight
placed 94/6 instead of 75/25 and starved the lighter shard (p99 3x), because a tally count is capped at the
cache capacity, so a deep shard reads shallow; and any load-steered pick shifts load in the direction that
changes its own reading, so it oscillates. **Steering by `Create` latency fails the same way, fuse or
not.** Latency cannot say why a shard is slow - a fault, more load than its peers, or a pinned backlog it is
working through - and demoting it is right for only the first. And the harm it would prevent (a slow shard
holding a bounded caller's slots) exists only for callers with bounded concurrency, which the engine cannot
see, so no threshold, step or hold time has a derivation. Near saturation a 20% speed gap reads as ~3x
latency, and one 10-point shift between two shards flips which one reads slow. A slow shard is the
operator's to weight down; `dwarf_operation_seconds{op,shard}` is what lets them see it. `Cordoned` shards are excluded from
placement entirely (everything resident proceeds - execution, subgraph children, `Continue`, `Fork`); all
shards cordoned is a loud 503 at `Create`. Pinned by `engine/poolsizing_test.go`.

**`List` uses per-shard pagination, not cross-shard global order.** Each shard returns up to `ceil(limit/numShards)`
rows by its own `flow_id DESC`; the aggregate is shard-grouped. Cross-shard ordering by `created_at` would compare
different servers' clocks, and by `flow_id` alone is broken (a shard with fewer flows has lower ids). Pagination uses
an opaque cursor encoding each shard's smallest-returned `flow_id`. `List` is strict by design: any shard error fails
the whole call (the per-shard debug path is `ShardInfo` + `List(Shard=N)`).

*Say this out loud in the public docs, do not quietly promise "newest first".* The Operations summary above, the
`List` godoc, `Query.Limit`, and `docs/flows.md` must not advertise a global newest-first order, which on a
two-shard fleet visibly is not: `List(Limit:100)` returns shard 1's 50 newest, then shard 2's 50 newest, so shard 2's
newest flow follows shard 1's oldest returned one - a reverse-chronological UI renders that as an interleaving
artifact. The answer is the **doc**, not the code: merging by `created_at` (the tempting one) is exactly what the
clock-comparison argument above rejects, and there is no other cross-shard key. A caller needing one ordered view
sorts the page itself - deciding what to trust - or pages a single shard with `Query.Shard`. Pinned by
`TestList_NewestFirstIsPerShardNotGlobal`, which also asserts the global-order violation is *present*, so the code is
never "fixed" into a global-order promise by accident.

**`Query.Limit` is a per-shard cap, not a hard total - same honesty, same reason.** `perShardLimit =
ceil(Limit/shards)` (`query.go`) is applied to each shard's query and the results are concatenated with no final
truncation, so a multi-shard page returns up to `shards * ceil(Limit/shards)` rows (`Limit:10` on 4 shards → up to 12;
`Limit:1` → up to 4). `purge` divides identically, so `Purge` can mark more than `Limit` roots. This is
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
encodes its shard (`{shard}-{hour}-{id}-{token}`), so a flow created on a shard unknown to a peer replica is unroutable
(404) there - and there is no cross-replica agreement on the set nor any rebalancing of existing flows (they stay on
their original shard). Doing dynamic growth *correctly* - cross-replica agreement plus rollout sequencing - was
designed but deliberately deferred; until then the set is fixed per process.

## Test-only instrumentation seams (`engine/engineundertest.go`)

Fault injection and execution checkpoints, inert in production: the rules for consulting, naming and waiting on
them live in `internal/engineseams/CLAUDE.md`.
