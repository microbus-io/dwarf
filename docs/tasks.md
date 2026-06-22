# Writing tasks

A task is a function your host's `ExecuteTask` runs. It receives a `*workflow.Flow` — the carrier holding the
step's state and control signals — does its work, writes outputs back onto the flow, and returns. This
guide covers the Flow API a task uses.

```go
func charge(ctx context.Context, f *workflow.Flow) error {
    amount := f.GetFloat("amount")
    if amount <= 0 {
        return errors.New("nothing to charge")
    }
    receipt, err := billing.Charge(ctx, amount)
    if err != nil {
        return err // an ordinary failure: routes to onError or fails the flow
    }
    f.SetString("receipt", receipt)
    return nil
}
```

The engine populates the flow's state from the step's input before the call, records whatever you change
as the step's `changes`, and persists it. Returning a non-nil error fails the step (unless an `onError`
transition matches; see [Building graphs](graphs.md#error-handling-onerror-ontimeout)).

## Reading state

The flow arrives with the merged input state of the step. Read it with typed accessors:

```go
name   := f.GetString("name")
count  := f.GetInt("count")
amount := f.GetFloat("amount")
ok     := f.GetBool("approved")
ttl    := f.GetDuration("ttl")
tags   := f.GetStrings("tags")

if f.Has("coupon") { ... }               // presence check

var order Order
if err := f.Get("order", &order); err != nil { ... }   // decode a field into a struct
```

`f.Snapshot()` returns a read-only copy of the entire current state. `f.ParseState(&v)` decodes the whole
state into a struct.

## Writing state

Mutations are recorded as the step's output delta:

```go
f.SetString("status", "charged")
f.SetInt("attempts", n)
f.SetFloat("balance", b)
f.SetBool("approved", true)
f.SetStrings("errors", msgs)
f.Set("order", order)         // any JSON-marshalable value

f.Delete("coupon", "scratch") // remove fields
f.Clear()                     // remove everything
```

`f.Transform(newKey, oldKey, ...)` clears all state and re-introduces only the listed fields under new
names (absent or null source fields are skipped) — handy as a small adapter task just upstream of a
[subgraph](fan-out-and-subgraphs.md#subgraphs) to reshape state into the child's expected input.

### Deltas, not totals, for reducer fields

If a field is managed by a reducer (append, add, union, merge, …) at a fan-in, write only your **delta** —
the increment, not the accumulated value:

```go
// "messages" is wired to ReducerAppend. Write the new message only.
f.Set("messages", []string{newMessage})   // correct
// f.Set("messages", entireHistory)        // WRONG: fan-in would duplicate
```

See [Concepts → Reducer](concepts.md#reducer).

## Control signals

Beyond reading and writing state, a task can steer the engine. These are methods on the flow; after
calling one, the task should return as instructed.

### Retry

Re-execute this task with exponential backoff. The bound is wall-clock, not a count: `Retry` returns `true`
(return `nil`) while the next attempt would still land within `giveUpAfter` of the step's first creation, and
`false` (return your error) once the horizon is reached — including when the next backoff delay alone would
overshoot it, so a wait already known to be doomed is not parked before failing. It carries no condition of its
own — you decide what's retryable:

```go
if err := callFlaky(ctx); err != nil {
    if isTransient(err) && f.Retry(100*time.Millisecond, 2.0, 10*time.Second, time.Hour) {
        return nil // will re-run after backoff
    }
    return err // not retryable, or horizon exceeded
}
```

The delay before attempt N is `min(initialDelay * delayMultiplier^N, maxIntervalDelay)`. Pass a zero
`initialDelay` for immediate retries, a zero `maxIntervalDelay` for no per-interval cap, and `delayMultiplier`
`1.0` to hold the delay constant. Pass `giveUpAfter <= 0` for unlimited retry. To bound by count instead, pass
`giveUpAfter` `0` and gate on `f.Attempt()` at the call site. On retry the engine merges the task's prior
output back into its input, so the next attempt sees what the last one wrote.

### Sleep

Delay the *next* step:

```go
f.Sleep(30 * time.Minute) // the next step won't dispatch for 30 minutes
return nil
```

The engine sets the next step's earliest-run time and wakes precisely when it's due — durable across
restarts, no goroutine held open.

### Goto

Override routing: skip normal transition evaluation and follow the `goto` transition to a target (it must
be registered with `AddTransitionGoto`):

```go
if needsReview {
    f.Goto("manualReview")
}
return nil
```

This is how you build loops and computed branching.

### Interrupt (human-in-the-loop)

Park the flow to await external input, then receive that input when the flow is resumed. `Interrupt`
follows a two-call pattern within the same task body:

```go
func approve(ctx context.Context, f *workflow.Flow) error {
    var resume map[string]any
    yield, err := f.Interrupt(map[string]any{
        "question": "Approve refund of $" + f.GetString("amount") + "?",
    }, &resume)
    if err != nil {
        return err
    }
    if yield {
        return nil // first call: flow is now parked; return immediately
    }
    // re-entry after Resume: 'resume' holds the caller's data
    f.SetBool("approved", resume["approved"] == true)
    return nil
}
```

On the first call the flow goes to `interrupted`, the payload is surfaced to whoever is awaiting it, and
the engine fires the stop notification. When the operator calls `eng.Resume(flowKey, data)`, the task
re-runs and `Interrupt` returns `(false, nil)` with the caller's data unmarshaled into your `&resume`
pointer. The resume data is delivered through that pointer — it is **not** merged into state. See
[Engine operations → Resume](operations.md#resume).

### Subgraph

Call another workflow as a child and get its result back. Like `Interrupt`, it's a two-call,
park-and-resume pattern — and semantically a function call: only the explicit `in` crosses into the
child, only the child's final state (`out`) crosses back.

```go
func enrich(ctx context.Context, f *workflow.Flow) error {
    var out struct {
        Profile map[string]any `json:"profile"`
    } // or: var out map[string]any
    yield, err := f.Subgraph("enrichment.workflow", map[string]any{
        "id": f.GetString("customerID"),
    }, &out)
    if err != nil {
        return err
    }
    if yield {
        return nil // child launched; this step parks until it completes
    }
    f.Set("profile", out.Profile) // adopt the fields you want
    return nil
}
```

The result is unmarshaled into the trailing `out` pointer (a `*struct` for type safety, a `*map[string]any`
for dynamic access, or `nil` to ignore it). Pass `nil` input for "no arguments," or `f.Snapshot()` to forward
the parent's whole state. See [Fan-out & subgraphs → Subgraphs](fan-out-and-subgraphs.md#subgraphs).

### Subtask

Run a **single task** as an isolated child flow — the task-level sibling of `Subgraph`. There is no graph
for the child: the engine synthesizes a one-node graph named by the first argument (shown in diagrams and
history). Same park-and-resume, out-pointer shape; only the explicit input and output cross the boundary.

```go
func score(ctx context.Context, f *workflow.Flow) error {
    var out struct {
        Score int `json:"score"`
    }
    yield, err := f.Subtask("Score", "risk.score", map[string]any{
        "id": f.GetString("customerID"),
    }, &out)
    if yield || err != nil {
        return err
    }
    f.SetInt("riskScore", out.Score)
    return nil
}
```

Use `Subgraph` to invoke a whole workflow; use `Subtask` to invoke one task without defining a graph for it.

## Baggage (host context)

The opaque, host-defined value set in `FlowOptions.Baggage` at create time rides on the dispatch context
of every task. Read it with `workflow.BaggageFrom(ctx)` — it's how a host carries identity/claims, tenant,
or locale to task code without the engine interpreting it:

```go
func charge(ctx context.Context, f *workflow.Flow) error {
    if claims, ok := workflow.BaggageFrom(ctx).(map[string]any); ok {
        token := mintToken(claims) // e.g. act as the original caller
        ...
    }
    ...
}
```

Baggage is set once at `Create`, frozen on the flow, inherited by subgraphs and `Continue`, and delivered
as the JSON-decoded form (typically `map[string]any`). See
[Engine operations → Create](operations.md#create-and-run).

## Handling transient failures

The engine never inspects an error's status code or text. Any error you return is terminal for that
attempt: the engine routes it via the graph's `onError` transition if one exists, otherwise it fails the
step. There is no engine-side rate-limit or unavailability handling to engage.

To back off on a transient failure (a rate limit, a downstream that's momentarily unavailable), detect it
yourself and arm `flow.Retry` — the task owns the decision because it owns the resource identity:

```go
err := callDownstream(ctx)
switch {
case isRateLimited(err), isUnavailable(err): // e.g. HTTP 429 / 503
    if f.Retry(100*time.Millisecond, 2.0, 10*time.Second, time.Hour) {
        return nil // re-run after wall-clock-bounded backoff
    }
    return err // horizon exceeded: onError or fail
default:
    return err // ordinary failure: onError or fail
}
```

The retry bound is wall-clock, not a count; see [Retry](#retry).

## Timestamps

`f.CreatedAt()` and `f.UpdatedAt()` are populated on every dispatch. Use `CreatedAt` to implement a
workflow-level deadline in author space — the engine imposes none:

```go
if time.Since(f.CreatedAt()) > 24*time.Hour {
    return errors.New("workflow exceeded its 24h budget")
}
```

Next: [Engine operations](operations.md).
