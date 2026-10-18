# Driving flows

> **For developers** integrating dwarf into an application. This is the reference for the calls your code
> makes against a running engine — starting flows, reading them back, resuming and terminating them, and
> managing retention. It assumes you have an engine configured; standing one up in production is
> [Operating dwarf](operations.md).

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

To run a single unit of work with the engine's durability and scheduling, declare a one-task workflow and
create a flow for it like any other. A task only ever runs as part of a graph, not as an independently
invocable unit.

> `Run`'s Go `error` means the call itself did not go through: a malformed request, no graph for the URL, a
> wait that ended before the flow did, or an infrastructure failure (see [Errors](#errors)). A *workflow*
> failure is not an error — it surfaces as `outcome.Status == "failed"` with `outcome.Error` set.

### Detecting completion

The engine has **no** stop-notification callback. To learn a flow's outcome you **`Await`** it (below),
**`Poll`** it when your own deadline is shorter than the flow's (below), or **compose** the notification into
the workflow itself (an orchestrating graph whose follow-up tasks report the outcome). The three approaches
and when to use each are covered in [Detecting flow completion](detecting-completion.md).

### Deferring work

`Create` runs a flow immediately. Deferral is expressed in author space:

- **Wait a duration (durably):** make the entry task a **gate** that calls `flow.Sleep(duration)` and
  returns; the real work is the next step. The delay survives restarts — and the flow's status honestly
  reflects that it ran its gate, not that it's idle.
- **Wait for an external signal:** make the entry task call `flow.Interrupt(...)`; the flow parks as
  `interrupted`, and the caller resumes it with `Resume(ctx, flowKey, data)` when ready. Such a flow rests
  as `interrupted` rather than idling unstarted, so its status honestly reports that it is waiting.

Recurring schedules (cron) are not an engine concern: run a separate scheduler that calls `Create`/`Run` on
its schedule.

### Await

```go
outcome, err := eng.Await(ctx, flowKey)
```

Blocks until the flow stops — `completed`, `failed`, `terminated`, `cancelled`, or `interrupted` — and returns the
outcome, or returns without it when the caller's context ends first. The wait is bounded by a short internal
cadence rather than by anything the caller or the host has to arrange: a flow that stops on another replica
wakes its waiters just as one that stops locally does, and nothing has to be delivered between replicas for
that to happen.

If the ctx deadline fires first, `Await` returns the error and the flow **keeps running** — it is durable and
not bound to your call. You still hold the key, so you can `Await` again.

**Give the ctx a deadline.** It is the only bound on the wait — a flow runs for as long as its work takes.
Without one, `Await` blocks on a long fixed budget and then times out; that budget is a guard against blocking
forever, not a wait to build on.

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
    State            workflow.State  // the final state when terminal; the interrupted step's merged snapshot when interrupted; empty while running
    Error            string          // set when Status == "failed"
    InterruptPayload workflow.State  // set when Status == "interrupted"
    TerminateReason  string          // set when Status == "terminated"
    CancelReason     string          // set when Status == "cancelled"
}
```

`terminated` means `Terminate` stopped the flow outright; `cancelled` means a graceful `Cancel` stopped it (see
[Cancelling gracefully](#cancelling-gracefully)).

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
```

`History` returns each step's task, depth, status, error, and timings — metadata only, **not** `state`/`changes`;
use `Step` to read a single step's `state`/`changes`. Subgraph-executing
steps carry nested `SubHistory` and the child flow's `SubgraphKey`: pass it to `Snapshot` for what the child
returned, or to `Await`, `Poll`, `Fingerprint` or `History`. It is a read handle - operations that change a flow
take the root's key. `List` takes a `workflow.Query` (status, workflow name, thread, the task a flow stopped at,
fairness key, priority, time window, shard, free-text `Search`, `Limit`) and returns newest-first with an
opaque pagination cursor as its second return; see [Retention](#retention) for the same query shape.

**"Newest first" is per shard, not global.** On a single-shard engine — the default — a page is in one
descending time order. On a multi-shard fleet each shard contributes its own newest flows and the page is
grouped by shard, so shard 2's newest flow follows shard 1's oldest returned one. There is no cross-shard
order to give: flow ids are per-shard sequences (a shard with fewer flows has lower ids, so they don't
compare), and creation times would compare different database servers' clocks. If you need one ordered view,
sort the page yourself — you decide what to trust — or page a single shard with `Query.Shard`.

**`Query.Limit` is a per-shard cap, not a hard total.** It is divided across shards, so on a multi-shard
fleet each shard returns up to `ceil(Limit/shards)` of its own newest flows and a page can hold as many as
`shards * ceil(Limit/shards)` results (`Limit: 10` on 4 shards returns up to 12; `Limit: 1` returns up to 4).
A single-shard engine returns at most `Limit`. This per-shard division is why pagination is a cursor rather
than a global offset — each shard advances its own position independently. `Purge` divides `Limit` the same
way. For a strict total, truncate the returned page yourself or query one shard at a time with `Query.Shard`.

## Pausing and resuming

A flow pauses in two distinct ways, and each has its own continuation operation — they are never
auto-routed.

### Resume

Continues a flow paused by a task's `flow.Interrupt`. The data you pass is delivered to the task as the
return value of its `Interrupt` call (it is **not** merged into state):

```go
err := eng.Resume(ctx, flowKey, map[string]any{"approved": true})
```

## Cancelling gracefully

`Cancel` asks a flow to stop and lets it react, where `Terminate` (below) stops it outright:

```go
err := eng.Cancel(ctx, flowKey, "customer withdrew the order") // surfaced as CancelReason
```

- **Every step in progress when you call it is covered.** A step that has not started yet does not run. A step
  that is already running finishes, and then its next transition is not taken.
- **A covered step receives the cancellation as an error on its task's `onError` transition**, like any task
  error, except that the task's own changes are kept, because it ran. `workflow.IsCancelled(onErr)` tells a
  cancellation apart from a real failure, so the handler can compensate and let the flow carry on, or stop.
- **An error the task itself returns wins over the cancellation.** It takes the ordinary error path - its
  `onError` transition, or failing the flow - and the handler receives that error, not the cancellation.
- **With no `onError` transition, the flow stops.** It ends `cancelled` if every unrecovered loss in it was a
  cancellation, and `failed` if a real error is mixed in - a real error is never reported as a cancellation.
- **A task that arms something as it finishes:** a retry does not run the task again; an interrupt still parks the
  flow, and the step is cancelled when it is resumed, without acting on the resume data; a subgraph still runs.
  A task waiting on a subgraph runs once more when its child returns, so it sees what the child did. If the child
  was itself cancelled, the error `flow.Subgraph` returns satisfies `workflow.IsCancelled`, and returning that
  error from the task keeps its loss a cancellation.
- **There is no deadline.** `Cancel` returns once the request is recorded - `Await` the flow to see it stop. A
  sleeping or backed-off step is reached when it next wakes, and an interrupted flow stays `interrupted` until
  someone calls `Resume` or `Terminate`.
- Address the flow by its root key; calling `Cancel` on a flow that has already ended does nothing.
- **A flow is cancelled once.** A second `Cancel` on a flow already cancelled records nothing new: the first
  reason stands, and a flow whose `onError` handler caught the first cancellation and carried on is left
  alone. It does finish delivering the first cancellation, so a `Cancel` that returned an error, or whose
  response was lost, is safe to retry. To stop a flow that recovered, `Terminate` it.

## Terminating, and recovering with Fork

A terminal flow (`completed`/`failed`/`terminated`/`cancelled`) is **immutable** — it is never re-run in
place. To recover or explore, `Fork` clones a terminal flow up to a chosen step into a *new*, self-contained
flow and re-runs from there, optionally with state overrides; the original is never touched.

```go
err := eng.Terminate(ctx, flowKey, "superseded by newer order") // abort; surfaced as TerminateReason

// Re-run from a chosen step (its key comes from History) with an edit that lets it succeed.
newFlowKey, err := eng.Fork(ctx, stepKey, map[string]any{"amount": 0})
```

Fork's overrides **replace** the fields they name in the forked step's input, reduced fields included - they
are an edit for repairing a run, not a task's contribution - and a `nil` override deletes the field.

`Terminate` forcefully, unconditionally aborts a running or interrupted flow (and its subgraph hierarchy) -
in-flight work is abandoned immediately, not awaited. `Fork`'s step may be
**any recorded step**, including one inside a subgraph; the clone re-runs from that step and bubbles back up
to the root. Each step that called a subgraph on the way up runs its task again from the top, and that run's
writes take the place of the original's. The fork inherits the origin flow's scheduling and baggage, and does
not auto-delete. Because the fork is an ordinary new flow, recover a partially-failed fan-out by forking one
failed branch at a time. A branch lost to a `Cancel` is recovered the same way: the fork re-runs only the step you
chose and keeps every other branch as it was, so while a cancelled branch remains, the fork ends `cancelled`,
with the original `CancelReason`. A branch that `Terminate` stopped before it completed is a loss too, so a fork
that keeps it ends `failed`. To re-run it, fork at or before a fan-out step whose branches include every branch
`Terminate` stopped - in a nested fan-out, the outermost one when stopped branches sit under different outer
branches. Everything after the step you choose re-runs, every branch of that fan-out included.

To re-run a terminal flow from the beginning, fork it at its first step. `History` lists steps in execution
order, so the first entry is the flow's entry step, and the fork starts from the original's initial state with
your overrides applied:

```go
hist, err := eng.History(ctx, flowKey)
newFlowKey, err := eng.Fork(ctx, hist[0].StepKey, map[string]any{"amount": 0})
```

This is still a fork: it reuses the original's graph, scheduling and baggage. To start over with the graph
loaded fresh, or with different options, call `Create` instead.

## Continue a thread

`Continue` starts a new flow from the latest completed flow in a thread, carrying its final state and
identity forward — the basis for multi-turn conversations and iterative processes:

```go
nextKey, err := eng.Continue(ctx, threadKey, additionalState)
```

The `threadKey` is any flow key in the thread (the original `Create` key works). The prior turn's final
state passes through, combined with `additionalState` through the graph's reducers: a reduced field adds what
you pass, and a `nil` there adds nothing, so a new turn cannot clear a reduced field this way (the turn's first
task can, with `Overwrite`); on any other field a value replaces and a `nil` deletes. `Continue` inherits the
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
it is logically gone even though the row lingers briefly. An `interrupted` flow is first terminated exactly as
`Terminate` would, subgraphs included; if a `Resume` gets there first, the flow is running again, so `Delete`
returns 409 and `Purge` skips it. `Purge` returns the count of roots marked, at most 4096 per call (fewer if
`Query.Limit` is lower), divided across shards as `List` divides it, so a multi-shard call can mark slightly
more.

`Purge` takes the same `workflow.Query` as `List`. `OlderThan` / `NewerThan` are database-anchored and
compose (e.g. "completed, older than 30 days"):

```go
eng.Purge(ctx, workflow.Query{
    Status:    workflow.StatusCompleted,
    OlderThan: 30 * 24 * time.Hour,
})
```

A flow created with `FlowOptions.DeleteOnCompletion` schedules its own deletion when it completes successfully
(failed/terminated/cancelled flows are kept). Its outcome stays observable via `Await`/`Snapshot` for a short grace window
before the reaper removes it — so a fire-and-forget caller can still `Run` it and read the result.

## Operational

```go
summaries, err := eng.ShardInfo(ctx)    // per-shard health: reachability and latency
```

## Errors

Every error the engine returns carries an HTTP-style status code. Read it with `errors.StatusCode(err)` from
`github.com/microbus-io/errors`; a host serving the engine over HTTP can pass it through as the response
status. A workflow's own failure is not an error: it is the outcome's `Status` (see [The outcome](#the-outcome)).

| Code | Meaning | Retry? |
|---|---|---|
| 400 | The request is malformed: a key that does not parse, a subgraph child's key where a root key is required, invalid options or query, a value that is not JSON-marshalable, an invalid graph | No - fix the request |
| 404 | The key names no flow or step (including one already deleted), or `LoadGraph` found no graph | No |
| 408 | `Await` or `Run` returned before the flow stopped; the flow keeps running | Await again |
| 409 | The flow is in the wrong state for the operation, or a concurrent operation got there first | Re-read the flow, then decide |
| 413 | A value is larger than the [size limit](data-handling.md#the-size-limit-256-mb-per-value) of 256 MB; nothing was created or changed | No - send less |
| 501 | The flow's data is in a newer format than this replica reads: replicas of a newer release wrote it | Yes, on a replica of the newer release |
| 503 | The engine is not started, is shutting down, or every shard is cordoned; or a `Terminate` did not settle because the flow's tree kept spawning subgraphs | Yes |
| 500 | A database error or an engine fault | Yes, if transient |

A 408 also satisfies `errors.Is(err, context.DeadlineExceeded)`, or wraps the caller's own context error
when the context was cancelled. An error returned by your host's `LoadGraph` reaches `Create` and `Run` with
its own status code.

Each operation adds these to the 503 and 500 every call can return:

| Operation | 400 | 404 | 408 | 409 |
|---|---|---|---|---|
| `Create`, `Run` | invalid options, state or baggage; invalid graph; `ThreadKey` malformed or a subgraph child's | no graph for the URL; unknown `ThreadKey` | `Run` only: timed out awaiting the flow | |
| `Await`, `Poll` | malformed key | unknown flow | `Await` only: timed out | |
| `Snapshot`, `Fingerprint` | malformed key | unknown flow | | |
| `History` | malformed key | unknown flow, or one scheduled for deletion | | |
| `Step` | malformed key | unknown step | | |
| `List`, `Purge` | malformed `ThreadKey`, invalid status or shard, a negative `OlderThan` or `NewerThan`; `List` with a malformed cursor; `Purge` with no filter (`Shard` and `Limit` are not filters) or with `IncludeSubgraphs` | unknown `ThreadKey` | | |
| `Resume` | malformed key, a subgraph child's key, data not JSON-marshalable | unknown flow | | the flow is not interrupted, including when a `Delete` or `Terminate` won the race |
| `Cancel`, `Terminate` | malformed key, a subgraph child's key, a reason over 4,096 bytes or not valid UTF-8 without NUL | unknown flow | | |
| `Fork` | malformed key, overrides not JSON-marshalable | unknown step | | the flow is not terminal, is scheduled for deletion, or holds an unresolved interrupt off the fork path |
| `Continue` | malformed key, a subgraph child's key, `additionalState` not JSON-marshalable | unknown flow | | the thread's latest flow is not completed, including when a concurrent `Continue` won |
| `Delete` | malformed key, a subgraph child's key | unknown flow | | the flow is running, including when a `Resume` won the race |

A 413 comes from the operations that take a value in: `Create` and `Run` (the initial state or the baggage),
`Continue` (the new flow's initial state, with `additionalState` folded in), `Resume` (the resume data) and
`Fork` (the fork step's state, with the overrides applied).

A 501 comes from the operations that read a flow's step data: `Step`, `Fork`, and `Snapshot`, `Await`, `Poll`
and `Run` on an interrupted flow. The 503 from a `Terminate` that did not settle can also come from `Delete` and
`Purge`, which terminate an interrupted flow first.

Stopping or deleting what is already stopped or deleted is not an error. `Cancel` and `Terminate` on a flow
that has ended, and `Delete` on a flow already scheduled for deletion, return nil and change nothing, so a
call whose response was lost is safe to repeat.

Configuration returns 400 for an invalid value (a setter's argument, a `ShardSpec`, a missing host at
`Startup`) and 409 for a call in the wrong state: a construction-time setter after `Startup`, `Startup` on a
started engine, or a shard index set twice.

Next: [Fan-out & subgraphs](fan-out-and-subgraphs.md).
