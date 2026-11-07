# Driving flows

> **For developers** integrating dwarf into an application. This is the reference for the calls your code
> makes against a running engine — starting flows, reading them back, resuming and terminating them, and
> managing retention. It assumes you have an engine configured; standing one up in production is
> [Operating dwarf](operations.md).

Every interaction with a flow is a method on `*engine.Engine`. They fall into a few groups: creating &
running, inspecting, pausing & resuming, terminating & recovering, continuing, and retention. Flows are
addressed by their **flow key** (`{shard}-{hour}-{flowID}-{token}`), returned at creation; steps by their **step
key** (`{shard}-{hour}-{flowID}-{stepID}-{token}`), which names its flow too.

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
    Priority:    2,                              // 1..5, lower runs first; 0 takes the midpoint, 3
    FairnessKey: "tenant-42",                    // fair-scheduling bucket
    FairnessWeight: 4,                           // relative share within the key
    Baggage:     map[string]any{"actor": "ada"}, // opaque host context; read with BaggageFrom(ctx)
}
```

To run a single unit of work with the engine's durability and scheduling, declare a one-task workflow and
create a flow for it like any other. A task only ever runs as part of a graph, not as an independently
invocable unit.

> `Run`'s Go `error` means the call itself did not go through: a malformed request, no graph for the URL, a
> wait that ended before the flow did, or an infrastructure failure (see [Errors](#errors)). A *workflow*
> failure is not an error — it surfaces as `outcome.Status == "failed"` with `outcome.Error` set.

### Detecting completion

To learn a flow's outcome you **`Await`** it (below),
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

Blocks until the flow stops — closes (`completed`, `failed`, `terminated`, `cancelled`) or is `interrupted`, waiting
for `Resume` — and returns the outcome, or returns without it when the caller's context ends first. The wait is
bounded by a short internal cadence rather than by anything the caller or the host has to arrange: a flow that stops
on another replica wakes its waiters just as one that stops locally does, and nothing has to be delivered between
replicas for that to happen.

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

`Poll` waits exactly like `Await`, but a **ctx timeout is not an error**: it returns the flow's current outcome, which
has not stopped (`Stopped()` reports `false`). That makes it the right call for a caller bounded by
its own deadline — an HTTP status endpoint long-polling a flow that may run for hours — without hand-rolling a
`Snapshot` loop. A genuine failure (unknown flow, database down) still returns an error.

## The outcome

`Snapshot`, `Await`, and `Run` all return a `*workflow.FlowOutcome`:

```go
type FlowOutcome struct {
    Status           string
    State            workflow.State  // the final state when closed; the interrupted step's merged snapshot when interrupted; empty while running
    Error            string          // set when Status == "failed"
    InterruptPayload workflow.State  // set when Status == "interrupted"
    TerminateReason  string          // set when Status == "terminated"
    CancelReason     string          // set when Status == "cancelled"
}
```

An error message over 16 KB is cut to that size and ends with ` ... [truncated]`, in `Error` and in `History`'s
steps alike.

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
open,     err := eng.ListOpen(ctx, query)              // running and interrupted flows, the most recently updated first
closed,   err := eng.ListClosed(ctx, query)            // closed flows, the most recently archived first
```

`History` returns each step's task, depth, status, error, and timings — metadata only, **not** `state`/`changes`;
use `Step` to read a single step's `state`/`changes`. Subgraph-executing
steps carry nested `SubHistory` and the child flow's `SubgraphKey`: pass it to `Snapshot` for what the child
returned, or to `Await`, `Poll`, `Fingerprint` or `History`. It is a read handle - operations that change a flow
take the root's key.

### Listing flows

Two lists take a `workflow.Query` (status, workflow name, the task a flow stopped at, fairness key, priority, time
window, shard, free-text `Search`, `Limit`) and return the matching flows, at most `Limit` of them across every
shard. A flow is **open** while it is running or interrupted, and **closed** once it has completed, failed, been
terminated or been cancelled:

- **`ListOpen`** lists the open flows, the most recently updated first. Its window, optional, is on `UpdatedAt`,
  when a flow last changed status. A `Status` that is not open is a 400.
- **`ListClosed`** lists the closed flows, the most recently archived first. A closed flow is archived about a
  minute after it closes, and appears here then. Its window, required, is on `ArchivedAt`, when the flow was
  archived. A `Status` that is not closed is a 400.

**A flow that closed in the last minute is in neither list** - longer while archiving lags behind - and one deleted
without archiving (`DeleteOnCompletion`, a retention of 0) is never in `ListClosed`. It is still read by its key
meanwhile: `Snapshot`, `Await` and `History` find it, and `Delete` removes it. To learn when one flow closes, `Await`
it rather than polling a list.

Ties go by shard, then the newest flow. Every call needs `Limit` (at most 65,536). `ListClosed`'s window needs both
ends: `FromTime`, inclusive, and `ToTime`, exclusive.

```go
failed, err := eng.ListClosed(ctx, workflow.Query{
    Status:   workflow.StatusFailed,
    FromTime: time.Now().Add(-24 * time.Hour),
    ToTime:   time.Now(),
    Limit:    100,
})
```

`ListClosed` reads every archive day its window reaches, on every shard. A window is cut to what the archive can
hold - nothing before the retention's first day, nothing after now - and what is left may reach at most 31 days:
split a longer range. A search over many days wants a low `Limit`; a walk through a time range wants short windows.
At a retention of 0 nothing is archived, so `ListClosed` lists nothing new.

**Neither list has pagination or a cursor.** An open flow changes under you, so to see other open flows, narrow the
filters or the window. A closed flow never changes, so you can walk `ListClosed` back through a range a window at a
time, each ending just after the oldest `ArchivedAt` you hold, dropping the flows you already have: flows archived
together share an `ArchivedAt`. Polling forward from your last check up to now sees each flow once, about a minute
after it closed, provided a walk's newest bound stays behind now by more than an archiving batch takes - seconds,
longer for a very large tree: a batch archived together can appear after a later one. Each shard records its own
times, so flows within the clock skew between two shard servers may be listed out of order.

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
- Address the flow by its root key; calling `Cancel` on a flow that has already closed does nothing.
- **A flow is cancelled once.** A second `Cancel` on a flow already cancelled records nothing new: the first
  reason stands, and a flow whose `onError` handler caught the first cancellation and carried on is left
  alone. It does finish delivering the first cancellation, so a `Cancel` that returned an error, or whose
  response was lost, is safe to retry. To stop a flow that recovered, `Terminate` it.

## Terminating, and recovering with Fork

A closed flow (`completed`/`failed`/`terminated`/`cancelled`) is **immutable** — it is never re-run in
place. To recover or explore, `Fork` clones a closed flow up to a chosen step into a *new*, self-contained
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

To re-run a closed flow from the beginning, fork it at its first step. `History` lists steps in execution
order, so the first entry is the flow's entry step, and the fork starts from the original's initial state with
your overrides applied:

```go
hist, err := eng.History(ctx, flowKey)
newFlowKey, err := eng.Fork(ctx, hist[0].StepKey, map[string]any{"amount": 0})
```

This is still a fork: it reuses the original's graph, scheduling and baggage. To start over with the graph
loaded fresh, or with different options, call `Create` instead.

## Continue a flow

`Continue` starts a new flow from a completed flow, carrying its final state and identity forward — the basis
for multi-turn conversations and iterative processes:

```go
nextKey, err := eng.Continue(ctx, flowKey, additionalState)
```

`flowKey` is the completed flow to build on. Its final state passes through, combined with `additionalState`
through the graph's reducers: a reduced field adds what you pass, and a `nil` there adds nothing, so a new flow
cannot clear a reduced field this way (its first task can, with `Overwrite`); on any other field a value replaces
and a `nil` deletes. The new flow runs the same graph and inherits the policy of the flow it continues —
priority, fairness, time budget, and baggage. It is returned already `running`. To start the next flow with
different options instead, call `Create` with the previous flow's final state (from `Snapshot`).

Continue builds on exactly the flow you name, so a conversation is a chain of keys your application keeps:
continue the key the last `Continue` returned. Continuing one flow twice gives two flows, each built on it. Like
`Create`, `Continue` is not idempotent: a call retried after a lost response makes a second flow.

## Retention

An open flow is never removed by the engine, unless the engine is set to expire flows (below). About a minute after a
flow closes it moves to the archive, which keeps each day's closed flows for `SetRetention` days (30 by default) and
then drops the day whole; `PurgeBefore(t)` drops every archived day before t's. With a retention of 0 a closed flow
is deleted instead.
An archived flow reads as it did before it moved: `Snapshot`, `Fingerprint`, `Await`, `Poll`, `Run`, `History`,
`Step` and `Continue` find it by its key, `Fork` clones it, `ListClosed` lists it, and a flow whose day was dropped
reads as not found.

```go
err := eng.Delete(ctx, flowKey)                // remove one closed flow and its subtree
removed, done, err := eng.Purge(ctx, query)    // remove what ListClosed lists for query, oldest first, Limit at a time
err = eng.PurgeBefore(ctx, t)                  // drop every archived day before t's, on every shard
```

`Delete` and `Purge` remove closed flows sooner than the retention, each with its whole subgraph subtree. Both
remove only closed flows: `Delete` of a running or interrupted flow returns 409, so `Terminate` a flow first to
delete it.

`Delete` takes one flow by its key, archived or not. An archived flow is deleted at once. One not archived yet, in the
minute after it closes, is **marked** for deletion: a background reaper removes it shortly after, without archiving
it, and `History` returns 404 for it at once, so it is logically gone even though its records linger briefly. `Delete` of
a flow already deleted from the archive returns 404.

`Purge` removes what `ListClosed` would list for the same `workflow.Query`, oldest archived first, at most `Limit`
roots in a call, each deleted at once, and says when it is done: call it until it is. It needs `Limit` (at most
65,536) and both ends of the window, which is `ListClosed`'s, on `ArchivedAt`, cut and capped at 31 days - purge a
longer range a month at a time. A `Status` that is not closed (`running`, `interrupted`) is a 400.

**`Purge` removes only what is archived, and `done` covers only that.** A flow that closed shortly before the call,
still on its way to the archive, is not purged until a call after it is archived. For an erasure that must take every
flow - a tenant's, say, after terminating its open ones - wait about two minutes after the last of them closed (a closed flow is archived
60 to 75 seconds after it closes, longer while `dwarf_archive_lag_seconds` is above zero) and call `Purge` again, or
`Delete` them by key.

```go
for {
    _, done, err := eng.Purge(ctx, workflow.Query{
        Status:   workflow.StatusFailed,
        FromTime: time.Now().Add(-30 * 24 * time.Hour),
        ToTime:   time.Now().Add(-7 * 24 * time.Hour), // failed and archived more than a week ago
        Limit:    4096,
    })
    if err != nil || done {
        break
    }
}
```

A row delete writes in proportion to the rows and leaves dead rows for the database to vacuum: `Delete` and `Purge`
are for removing specific records, and bulk removal is the retention's and `PurgeBefore`'s, which drop whole days.

A flow created with `FlowOptions.DeleteOnCompletion` schedules its own deletion when it completes successfully
(a failed, terminated or cancelled flow is archived like any other). Its outcome stays observable via `Await`/`Snapshot` for a short grace window
before the reaper removes it — so a fire-and-forget caller can still `Run` it and read the result.

### Expiring flows that never end

`SetFlowExpiration(d)` caps how long after its creation a flow may still be running or interrupted. A flow still
running or interrupted once it is older than `d` is terminated, with its subgraphs, and its `TerminateReason` says it
expired; it is then archived like any other terminated flow. It bounds what an abandoned interrupt or a flow polling
forever leaves open. The check runs every 5 minutes, so a flow is terminated up to about 5 minutes
after it expires. It is off by default, and a flow created by `Continue` or `Fork` counts from its own creation, so a
long conversation carried on with `Continue` is never expired as a whole.

```go
err := eng.SetFlowExpiration(30 * 24 * time.Hour) // before Startup; the same value on every replica
```

Terminating is forceful: a step running when its flow expires is abandoned, as with `Terminate`. Set `d` well above
the longest a flow is meant to live.

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
| 400 | The request is malformed: a key that does not parse, a subgraph child's key where a root key is required, invalid options or query, a value that is not JSON-marshalable, state holding an invalid [field name](graphs.md#field-names), an invalid graph | No - fix the request |
| 404 | The key names no flow or step (including one already deleted), or `LoadGraph` found no graph | No |
| 408 | `Await` or `Run` returned before the flow stopped; the flow keeps running | Await again |
| 409 | The flow is in the wrong state for the operation, or a concurrent operation got there first | Re-read the flow, then decide |
| 413 | A value is larger than the [size limit](data-handling.md#the-size-limit-256-mb-per-value) of 256 MB; nothing was created or changed | No - send less |
| 501 | The flow's data is in a newer format than this replica reads: replicas of a newer release wrote it | Yes, on a replica of the newer release |
| 503 | The engine is not started, is shutting down, or every shard is cordoned; or a `Terminate` did not settle because the flow's tree kept spawning subgraphs | Yes |
| 500 | A database error or an engine fault | Yes, if transient |

A 408 also satisfies `errors.Is(err, context.DeadlineExceeded)`, or wraps the caller's own context error
when the context was cancelled. An error returned by your host's `LoadGraph` reaches `Create` and `Run` with
its own status code; have the host return a 503 when the graph's source is unreachable, so the caller knows to try
again. At a subgraph spawn the same error reaches the calling task instead (see
[When the child fails, or cannot start](fan-out-and-subgraphs.md#when-the-child-fails-or-cannot-start)).

Each operation adds these to the 503 and 500 every call can return:

| Operation | 400 | 404 | 408 | 409 |
|---|---|---|---|---|
| `Create`, `Run` | invalid options, state or baggage; invalid graph | no graph for the URL | `Run` only: timed out awaiting the flow | |
| `Await`, `Poll` | malformed key | unknown flow | `Await` only: timed out | |
| `Snapshot`, `Fingerprint` | malformed key | unknown flow | | |
| `History` | malformed key | unknown flow, or one scheduled for deletion | | |
| `Step` | malformed key | unknown step | | |
| `ListOpen`, `ListClosed` | invalid status or shard, no `Limit` or one over 65,536, a `FromTime` not before `ToTime`; `ListOpen` with a status that is not open; `ListClosed` with a status that is not closed, no `FromTime` or `ToTime`, or a window reaching more than 31 days of the archive | | | |
| `Purge` | invalid status or shard, no `Limit` or one over 65,536, no `FromTime` or `ToTime`, `IncludeSubgraphs`, a `Status` that is not closed, or a window reaching more than 31 days of the archive | | | |
| `Resume` | malformed key, a subgraph child's key, data not JSON-marshalable | unknown flow | | the flow is not interrupted, including when a `Terminate` won the race |
| `Cancel`, `Terminate` | malformed key, a subgraph child's key, a reason over 4,096 bytes or not valid UTF-8 without NUL | unknown flow | | |
| `Fork` | malformed key, overrides not JSON-marshalable or holding an invalid field name | unknown step | | the flow is not closed, is scheduled for deletion, or holds an unresolved interrupt off the fork path |
| `Continue` | malformed key, a subgraph child's key, `additionalState` not JSON-marshalable | unknown flow | | the flow is not completed, or is scheduled for deletion |
| `Delete` | malformed key, a subgraph child's key | unknown flow | | the flow is open, or it was archived while being deleted (call again) |

A 413 comes from the operations that take a value in: `Create` and `Run` (the initial state or the baggage),
`Continue` (the new flow's initial state, with `additionalState` folded in), `Resume` (the resume data) and
`Fork` (the fork step's state, with the overrides applied).

A 501 comes from the operations that read a flow's step data: `Step`, `Fork`, and `Snapshot`, `Await`, `Poll`
and `Run` on an interrupted flow.

Ending or deleting what has already closed or been deleted is not an error. `Cancel` and `Terminate` on a closed
flow, and `Delete` on a flow already scheduled for deletion, return nil and change nothing, so a
call whose response was lost is safe to repeat.

Configuration returns 400 for an invalid value (a setter's argument, a `ShardSpec`, a missing host at
`Startup`) and 409 for a call in the wrong state: a construction-time setter after `Startup`, `Startup` on a
started engine, or a shard index set twice.

Next: [Fan-out & subgraphs](fan-out-and-subgraphs.md).
