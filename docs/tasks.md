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

Re-execute this task with exponential backoff. `Retry` returns `true` while attempts remain (return `nil`)
and `false` once exhausted (return your error). It carries no condition of its own — you decide what's
retryable:

```go
if err := callFlaky(ctx); err != nil {
    if isTransient(err) && f.Retry(5, 100*time.Millisecond, 2.0, 10*time.Second) {
        return nil // will re-run after backoff
    }
    return err // not retryable, or attempts exhausted
}
```

The delay for attempt N is `min(initialDelay * multiplier^N, maxDelay)`. Pass zero delays for immediate
retry; pass `math.MaxInt32` as `maxAttempts` for unlimited. On retry the engine merges the task's prior
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
    resume, yield, err := f.Interrupt(map[string]any{
        "question": "Approve refund of $" + f.GetString("amount") + "?",
    })
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
re-runs and `Interrupt` returns `(data, false, nil)`. The resume data is delivered as the return value —
it is **not** merged into state. See [Engine operations → Resume](operations.md#resume).

### Subgraph

Call another workflow as a child and get its result back. Like `Interrupt`, it's a two-call,
park-and-resume pattern — and semantically a function call: only the explicit `input` crosses into the
child, only the child's final state (`out`) crosses back.

```go
func enrich(ctx context.Context, f *workflow.Flow) error {
    out, yield, err := f.Subgraph("enrichment.workflow", map[string]any{
        "id": f.GetString("customerID"),
    })
    if err != nil {
        return err
    }
    if yield {
        return nil // child launched; this step parks until it completes
    }
    f.Set("profile", out["profile"]) // adopt the fields you want
    return nil
}
```

Pass `nil` input for "no arguments," or `f.Snapshot()` to forward the parent's whole state. See
[Fan-out & subgraphs → Subgraphs](fan-out-and-subgraphs.md#subgraphs).

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

## Signaling backpressure and breakers

A plain returned error is an ordinary failure. To engage the engine's adaptive mechanisms instead, wrap
the error from your transport:

```go
err := callDownstream(ctx)
switch {
case isRateLimited(err): // e.g. HTTP 429
    return workflow.ErrBackpressure(err, "")
case isUnavailable(err): // e.g. HTTP 503 / unreachable
    return workflow.ErrBreakerTrip(err, "unavailable")
default:
    return err // ordinary failure: onError or fail
}
```

- `ErrBackpressure` → the engine bounces the step back to pending and cuts the task's adaptive dispatch
  rate (the *valve*). For "you're going too fast."
- `ErrBreakerTrip` → the engine parks the task's whole backlog and probes on an exponential schedule (the
  *breaker*). For "this downstream can't serve right now." The `cause` string is an opaque metric label.

The engine classifies via `IsBackpressure` / `IsBreakerTrip`; it never inspects status codes itself, so
your host owns the mapping. See
[Scheduling & reliability](scheduling-and-reliability.md#backpressure-the-valve).

## Timestamps

`f.CreatedAt()` and `f.UpdatedAt()` are populated on every dispatch. Use `CreatedAt` to implement a
workflow-level deadline in author space — the engine imposes none:

```go
if time.Since(f.CreatedAt()) > 24*time.Hour {
    return errors.New("workflow exceeded its 24h budget")
}
```

Next: [Engine operations](operations.md).
