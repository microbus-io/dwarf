# Engine operations

Every interaction with a flow is a method on `*engine.Engine`. They fall into a few groups: creating &
running, inspecting, pausing & resuming, terminating & recovering, threading, and retention. Flows are
addressed by their **flow key** (`{shard}-{flowID}-{token}`), returned at creation; steps by their **step
key**.

## Creating and running

### Create and Run

```go
flowKey, err := eng.Create(ctx, workflowURL, initialState, opts) // makes a running flow
outcome,  err := eng.Run(ctx, workflowURL, initialState, opts)   // Create + Await
```

`Create` calls your host's `LoadGraph`, inserts the flow and its entry step, freezes the graph, and starts
running it immediately — the flow is `running` when `Create` returns. `Run` does the whole round-trip and
returns the final `*workflow.FlowOutcome`.

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
inherits the thread's policy. A nonexistent `ThreadKey` is rejected with a 404.

To run a single unit of work with the engine's durability and scheduling, declare a one-node workflow and
create a flow for it like any other. A bare task is only ever a node in a graph, not an independently
invocable unit.

> `Run`'s Go `error` is reserved for infrastructure failures (database, context deadline). A *workflow*
> failure surfaces as `outcome.Status == "failed"` with `outcome.Error` set — so you never have to
> disambiguate "the workflow rejected my input" from "the engine is down."

### Stop notifications

```go
// To be notified on stop instead of blocking on Await, opt in at Create:
flowKey, _ := eng.Create(ctx, workflowURL, state, &workflow.FlowOptions{NotifyOnStop: true})
```

When a flow is created with `FlowOptions.NotifyOnStop`, the engine invokes your host's
`FlowStopped(ctx, outcome)` when the flow stops — useful for push notification instead of blocking on
`Await`. The engine carries no delivery address: the flow's baggage rides on the callback's `ctx`
(`workflow.BaggageFrom(ctx)`), so your host decides where to deliver (e.g. read a target you stored in
baggage at `Create`).

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
outcome. It wakes on a status-change notification or context cancellation; there is no polling. Across
replicas, `Await` relies on the host's `SignalPeers` broadcast (see [Deployment](deployment.md)).

## The outcome

`Snapshot`, `Await`, and `Run` all return a `*workflow.FlowOutcome`:

```go
type FlowOutcome struct {
    FlowKey          string
    Status           string
    State            map[string]any  // final_state when terminal; current snapshot otherwise
    Error            string          // set when Status == "failed"
    InterruptPayload map[string]any  // set when Status == "interrupted"
    CancelReason     string          // set when Status == "cancelled"
}
```

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

`History` returns each step's task, depth, state, changes, status, error, and timings; subgraph-executing
steps carry nested `SubHistory`. `List` takes a `workflow.Query` (status, workflow name, thread, task,
fairness key, priority, time window, shard, free-text `Search`, `Limit`) and returns newest-first with an
opaque pagination cursor as its second return; see [Retention](#retention) for the same query shape.

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
to the root. The fork inherits the origin flow's scheduling and baggage, forces notify-on-stop off, and does
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
thread's policy from the latest completed flow — priority, fairness, time budget, baggage, and
notify-on-stop. The new flow is returned already `running`. (To join a thread but set policy explicitly
instead of inheriting it, use `Create`/`Run` with `FlowOptions.ThreadKey`.)

## Retention

The engine never auto-purges — every flow is potentially resurrectable (resume, continue, fork). Manage
retention explicitly:

```go
err := eng.Delete(ctx, flowKey)          // remove one flow and its steps (refuses a running flow)
count, err := eng.Purge(ctx, query)      // bulk-delete matching flows (except running), capped at 10000
```

`Purge` takes the same `workflow.Query` as `List`. `OlderThan` / `NewerThan` are database-anchored and
compose (e.g. "completed, older than 30 days"):

```go
eng.Purge(ctx, workflow.Query{
    Status:    workflow.StatusCompleted,
    OlderThan: 30 * 24 * time.Hour,
})
```

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
