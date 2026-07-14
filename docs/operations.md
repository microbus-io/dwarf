# Engine operations

Every interaction with a flow is a method on `*engine.Engine`. They fall into a few groups: creating &
running, inspecting, pausing & resuming, terminating & recovering, threading, and retention. Flows are
addressed by their **flow key** (`{shard}-{flowID}-{token}`), returned at creation; steps by their **step
key**.

## Creating and running

### Create and Run

```go
flowKey, err := eng.Create(ctx, workflowURL, initialState, opts)       // makes a running flow
flowKey, outcome, err := eng.Run(ctx, workflowURL, initialState, opts) // Create + Await
```

`Create` calls your host's `LoadGraph`, inserts the flow and its entry step, freezes the graph, and starts
running it immediately — the flow is `running` when `Create` returns. `Run` does the whole round-trip and
returns the new flow's key alongside its final `*workflow.FlowOutcome` (the key is the flow's identity, not
part of the outcome — you need it for later `History`/`Resume`/`Fork`).

`initialState` is any JSON-marshalable value (typically `map[string]any`). `opts` is a
`*workflow.FlowOptions` (nil for defaults):

```go
&workflow.FlowOptions{
    Priority:    10,                             // lower runs first; 0 uses the engine default
    FairnessKey: "tenant-42",                    // fair-scheduling bucket
    FairnessWeight: 4,                           // relative share within the key
    Baggage:     map[string]any{"actor": "ada"}, // opaque host context; read with BaggageFrom(ctx)
    ThreadKey:   "1-42-abc",                     // join an existing thread (any flow key in it)
}
```

Setting `ThreadKey` joins the new flow into an existing thread (identified by any flow key in that thread)
while specifying its scheduling/baggage explicitly — the explicit-policy counterpart to `Continue`, which
inherits the thread's policy. A nonexistent `ThreadKey` is rejected with a 404, and a *subgraph child's* key with a
400 — a child runs on its own private thread so it cannot contaminate its parent's continuation chain, so address the
root flow instead.

To run a single unit of work with the engine's durability and scheduling, declare a one-node workflow and
create a flow for it like any other. A bare task is only ever a node in a graph, not an independently
invocable unit.

> `Run`'s Go `error` is reserved for infrastructure failures (database, context deadline). A *workflow*
> failure surfaces as `outcome.Status == "failed"` with `outcome.Error` set — so you never have to
> disambiguate "the workflow rejected my input" from "the engine is down."

### Detecting completion

The engine has **no** stop-notification callback. To learn a flow's outcome you **`Await`** it (below),
**`Poll`** it when your own deadline is shorter than the flow's (below), or **compose** the notification into
the workflow itself (an orchestrating graph whose follow-up tasks report the outcome). The three approaches
and when to use each are covered in [Detecting flow completion](detecting-completion.md).

### Deferring work

`Create` runs a flow immediately — there is no separate start step and no creation-time delay (no `StartAt`).
Deferral is expressed in author space:

- **Wait until a wall-clock time (durably):** make the entry task a **gate** that calls `flow.Sleep(until)`
  and returns; the real work is the next step. The delay is persisted on the step's `not_before`, so it
  survives restarts — and the flow's status honestly reflects that it ran its gate, not that it's idle.
- **Wait for an external signal:** make the entry task call `flow.Interrupt(...)`; the flow parks as
  `interrupted`, and the caller resumes it with `Resume(ctx, flowKey, data)` when ready. (This replaces the
  old "create now, start later" staging.)

Recurring schedules (cron) are not an engine concern: run a separate scheduler that calls `Create`/`Run` on
its schedule.

### Await

```go
outcome, err := eng.Await(ctx, flowKey)
```

Blocks until the flow stops — `completed`, `failed`, `cancelled`, or `interrupted` — and returns the
outcome. It wakes on a status-change notification or context cancellation, and also re-checks on an internal
ticker: the notification is a fire-and-forget in-memory wake that can be lost, so the ticker is a load-bearing
safety net that bounds the wait to one interval — not a redundant polling layer to build atop, nor one to
optimize away. Across replicas, `Await` relies on the host's `SignalPeers` broadcast (see
[Deployment](deployment.md)).

If the ctx deadline fires first, `Await` returns the error and the flow **keeps running** — it is durable and
not bound to your call. You still hold the key, so you can `Await` again.

### Poll

```go
outcome, err := eng.Poll(ctx, flowKey)   // ctx timeout is NOT an error
if !outcome.Stopped() {
    // still running - answer now, ask again later
}
```

`Poll` waits exactly like `Await`, but a **ctx timeout is not an error**: it returns the flow's current,
non-terminal outcome, whose `Stopped()` reports `false`. That makes it the right call for a caller bounded by
its own deadline — an HTTP status endpoint long-polling a flow that may run for hours — without hand-rolling a
`Snapshot` loop. A genuine failure (unknown flow, database down) still returns an error.

## The outcome

`Snapshot`, `Await`, and `Run` all return a `*workflow.FlowOutcome`:

```go
type FlowOutcome struct {
    Status           string
    State            map[string]any  // final_state when terminal; the interrupted step's merged snapshot when interrupted; empty while running/created
    Error            string          // set when Status == "failed"
    InterruptPayload map[string]any  // set when Status == "interrupted"
    CancelReason     string          // set when Status == "cancelled"
}
```

The flow key is **not** on the outcome — it is delivered separately: you passed it to `Snapshot`/`Await`, or
`Run` returns it alongside the outcome.

Side-channel fields are populated only for the matching status.

## Inspecting

```go
outcome,  err := eng.Snapshot(ctx, flowKey)            // current status + state, without blocking
fp, status, err := eng.Fingerprint(ctx, flowKey)       // cheap change-detection token + status
steps,    err := eng.History(ctx, flowKey)             // []workflow.FlowStep, the full execution record
step,     err := eng.Step(ctx, stepKey)                // one step by key
summaries, next, err := eng.List(ctx, query)           // paginated flow listing
err := eng.HistoryMermaid(ctx, flowKey, w)             // write the execution DAG as a Mermaid diagram
```

`History` returns each step's task, depth, status, error, and timings — metadata only, **not** `state`/`changes`
(those columns are deliberately not fetched); use `Step` to read a single step's `state`/`changes`. Subgraph-executing
steps carry nested `SubHistory`. `List` takes a `workflow.Query` (status, workflow name, thread, task,
fairness key, priority, time window, shard, free-text `Search`, `Limit`) and returns newest-first with an
opaque pagination cursor as its second return; see [Retention](#retention) for the same query shape.

**"Newest first" is per shard, not global.** On a single-shard engine — the default — a page is in one
descending time order. On a multi-shard fleet each shard contributes its own newest flows and the page is
grouped by shard, so shard 2's newest flow follows shard 1's oldest returned one. There is no cross-shard
order to give: flow ids are per-shard sequences (a shard with fewer flows has lower ids, so they don't
compare), and `created_at` would compare different database servers' clocks. If you need one ordered view,
sort the page yourself — you decide what to trust — or page a single shard with `Query.Shard`.

## Pausing and resuming

A flow pauses in two distinct ways, and each has its own continuation operation — they are never
auto-routed.

### Resume

Continues a flow paused by a task's `flow.Interrupt`. The data you pass is delivered to the task as the
return value of its `Interrupt` call (it is **not** merged into state):

```go
err := eng.Resume(ctx, flowKey, map[string]any{"approved": true})
```

## Terminating, and recovering with Fork

A terminal flow (`completed`/`failed`/`cancelled`) is **immutable** — it is never re-run in place. To
recover or explore, `Fork` clones a terminal flow up to a chosen step into a *new*, self-contained flow and
re-runs from there, optionally with state overrides; the original is never touched.

```go
err := eng.Cancel(ctx, flowKey, "superseded by newer order") // abort; surfaced as CancelReason

// Re-run from a chosen step (its key comes from History) with an edit that lets it succeed.
newFlowKey, err := eng.Fork(ctx, stepKey, map[string]any{"amount": 0})
```

`Cancel` aborts a running or interrupted flow (and its subgraph hierarchy). `Fork`'s step may be
**any recorded step**, including one inside a subgraph; the clone re-runs from that step and bubbles back up
to the root. The fork inherits the origin flow's scheduling and baggage, and does
not auto-delete. Because the fork is an ordinary new flow, recover a partially-failed fan-out by forking one
failed branch at a time.

## Continue a thread

`Continue` starts a new flow from the latest completed flow in a thread, carrying its final state and
identity forward — the basis for multi-turn conversations and iterative processes:

```go
nextKey, err := eng.Continue(ctx, threadKey, additionalState)
```

The `threadKey` is any flow key in the thread (the original `Create` key works). The prior turn's final
state passes through, merged with `additionalState` using the graph's reducers. `Continue` inherits the
thread's policy from the latest completed flow — priority, fairness, time budget, and baggage. The new flow
is returned already `running`. (To join a thread but set policy explicitly
instead of inheriting it, use `Create`/`Run` with `FlowOptions.ThreadKey`.)

## Retention

The engine never auto-purges — every flow is potentially resurrectable (resume, continue, fork). Manage
retention explicitly:

```go
err := eng.Delete(ctx, flowKey)          // schedule one flow (and its subtree) for deletion; refuses a running flow
count, err := eng.Purge(ctx, query)      // schedule matching flows (except running) for deletion, capped at 4096
```

`Delete` and `Purge` **mark** flows for deletion (they do not delete inline); a background reaper removes each
flow's whole subgraph subtree shortly after. The marked flow drops out of `List` and `History` immediately, so
it is logically gone even though the row lingers briefly. `Purge` returns the count of roots marked.

`Purge` takes the same `workflow.Query` as `List`. `OlderThan` / `NewerThan` are database-anchored and
compose (e.g. "completed, older than 30 days"):

```go
eng.Purge(ctx, workflow.Query{
    Status:    workflow.StatusCompleted,
    OlderThan: 30 * 24 * time.Hour,
})
```

A flow created with `FlowOptions.DeleteOnCompletion` schedules its own deletion when it completes successfully
(failed/cancelled flows are kept). Its outcome stays observable via `Await`/`Snapshot` for a short grace window
before the reaper removes it — so a fire-and-forget caller can still `Run` it and read the result.

## Operational

```go
summaries, err := eng.ShardInfo(ctx)    // per-shard health and size
```

## Cross-replica inbound signals

When running multiple replicas, your host's `SignalPeers` publishes coordination signals; the receiving replica
feeds them back in via `DeliverSignal(ctx, op, payload)`. This is the inbound half of multi-replica
coordination, not part of the day-to-day API — see
[Deployment → Running multiple replicas](deployment.md#running-multiple-replicas).

Next: [Fan-out & subgraphs](fan-out-and-subgraphs.md).
