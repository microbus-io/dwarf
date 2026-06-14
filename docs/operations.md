# Engine operations

Every interaction with a flow is a method on `*engine.Engine`. They fall into a few groups: creating &
running, inspecting, pausing & resuming, terminating & recovering, threading, and retention. Flows are
addressed by their **flow key** (`{shard}-{flowID}-{token}`), returned at creation; steps by their **step
key**.

## Creating and running

### Create and Run

```go
flowKey, err := eng.Create(ctx, workflowName, initialState, opts) // makes a flow in "created"
outcome,  err := eng.Run(ctx, workflowName, initialState, opts)   // Create + Start + Await
```

`Create` calls your host's `LoadGraph`, inserts the flow and its entry step, and freezes the graph — but does
not start it. `Run` does the whole round-trip and returns the final `*workflow.FlowOutcome`.

`initialState` is any JSON-marshalable value (typically `map[string]any`). `opts` is a
`*workflow.FlowOptions` (nil for defaults):

```go
&workflow.FlowOptions{
    Priority:    10,                             // lower runs first; 0 uses the engine default
    FairnessKey: "tenant-42",                    // fair-scheduling bucket
    FairnessWeight: 4,                           // relative share within the key
    StartAt:     time.Now().Add(time.Hour),      // delay the entry step
    Baggage:     map[string]any{"actor": "ada"}, // opaque host context; read with BaggageFrom(ctx)
}
```

`CreateTask(ctx, taskName, initialState, opts)` is a shortcut that wraps a single task in a trivial graph —
handy for running one unit of work with the engine's durability and scheduling.

> `Run`'s Go `error` is reserved for infrastructure failures (database, context deadline). A *workflow*
> failure surfaces as `outcome.Status == "failed"` with `outcome.Error` set — so you never have to
> disambiguate "the workflow rejected my input" from "the engine is down."

### Start and StartNotify

```go
err := eng.Start(ctx, flowKey)                       // created -> running
err := eng.StartNotify(ctx, flowKey, "my-hostname")  // also fire the host's FlowStopped on stop
```

`Start` transitions a created flow to running and signals the workers to pick it up. `StartNotify`
additionally
records a hostname; when the flow stops, the engine invokes your host's `FlowStopped` with that
hostname and the outcome — useful for push notification instead of blocking on `Await`.

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

### Breakpoints and ResumeBreak

`BreakBefore` sets or clears a breakpoint that pauses a flow *before* a named task runs. Continue it with
`ResumeBreak`, which merges your overrides into the about-to-run step's state (the breakpoint pauses before
the task, so injecting state is how you influence it):

```go
err := eng.BreakBefore(ctx, flowKey, "charge", true)        // pause before "charge"
// ... flow pauses at the breakpoint ...
err = eng.ResumeBreak(ctx, flowKey, map[string]any{"amount": 0}) // edit state, then proceed
```

`Resume` rejects a breakpoint pause and `ResumeBreak` rejects an interrupt pause (both return 409) — the
two carry different semantics and the engine refuses to guess.

## Terminating and recovering

```go
err := eng.Cancel(ctx, flowKey, "superseded by newer order")  // abort; surfaced as CancelReason
err := eng.Restart(ctx, flowKey, stateOverrides)              // re-run a terminated flow from the entry
err := eng.RestartFrom(ctx, stepKey, stateOverrides)          // surgically rewind from a chosen step
```

`Cancel` aborts a created, running, or interrupted flow (and its subgraph hierarchy). `Restart` re-runs a
terminated flow as a fresh attempt from the entry point; `RestartFrom` rewinds the subtree below a chosen
step without resetting the flow's run timestamps — for operator-driven recovery. Both accept optional state
overrides.

## Continue a thread

`Continue` starts a new flow from the latest completed flow in a thread, carrying its final state and
identity forward — the basis for multi-turn conversations and iterative processes:

```go
nextKey, err := eng.Continue(ctx, threadKey, additionalState, opts)
```

The `threadKey` is any flow key in the thread (the original `Create` key works). The prior turn's final
state passes through, merged with `additionalState` using the graph's reducers; baggage is inherited unless
`opts.Baggage` overrides it. The new flow is returned in `created` — call `Start` to run it.

## Retention

The engine never auto-purges — every flow is potentially resurrectable (resume, continue, restart). Manage
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
tripped := eng.BreakerTripped(taskName) // is this task's breaker open right now?
```

## Cross-replica inbound signals

When running multiple replicas, your host's `SignalPeers` publishes coordination signals; the receiving replica
feeds them back in via `DeliverSignal(ctx, op, payload)`. This is the inbound half of multi-replica
coordination, not part of the day-to-day API — see
[Deployment → Running multiple replicas](deployment.md#running-multiple-replicas).

Next: [Fan-out & subgraphs](fan-out-and-subgraphs.md).
