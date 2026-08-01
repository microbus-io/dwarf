# Writing tasks

> **For developers** writing the code that runs inside a workflow. It covers the `workflow.Flow` carrier a
> task receives: reading and writing state, the control signals, and handling failure. The graph that decides
> *which* task runs is [Building graphs](graphs.md).

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
transition matches; see [Building graphs](graphs.md#error-handling-onerror)).

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
err := f.Get("order", &order)
if err != nil { ... }   // decode a field into a struct
```

`f.Snapshot()` returns a read-only copy of the entire current state. `f.ParseState(&v)` decodes the whole
state into a struct.

An **absent** field reads as the zero value, so an optional field needs no guard. A field that is **present
but holds the wrong type** panics — reading a string as an int is a bug in the workflow, and handing back a
`0` the task never wrote is how a `GetInt("retryAfter")` over a `1.5` becomes a zero-delay hot loop against
a downstream. The panic is not a crash: the engine catches it at the task-call boundary and fails the step
like any returned error, routing to `onError` if the graph has one. Use `f.Get(key, &v)`, which returns an
error, to handle a mistyped field instead of failing on it.

## Writing state

Mutations are recorded as the step's output delta:

```go
f.SetString("status", "charged")
f.SetInt("attempts", n)
f.SetFloat("balance", b)
f.SetBool("approved", true)
f.SetStrings("errors", msgs)
f.Set("order", order)         // any JSON-marshalable value

f.Del("coupon", "scratch") // remove fields
f.Clear()                     // remove everything
```

### Binary data must be base64-encoded

**Known limitation:** a **NUL character** (`U+0000`) in a string cannot be stored. It is valid UTF-8 and
marshals to legal JSON, but PostgreSQL's `JSONB` rejects it outright — so it works on SQLite and kills the flow
on the recommended production database. Dwarf does **not** currently detect or reject this, so it is your
responsibility: raw bytes belong in state **base64-encoded**.

```go
f.SetString("payload", string(rawBytes))                        // FAILS on Postgres if rawBytes holds a 0x00
f.SetString("payload", base64.StdEncoding.EncodeToString(raw))  // correct
```

Nothing else about strings is constrained — tabs, newlines, other control characters, and emoji all
round-trip fine. (Invalid UTF-8 needs no rule: `encoding/json` replaces it with `U+FFFD` on the way in.)

### Large integers: exact in storage, rounded by untyped reads

An integer beyond **2^53** (about 9.0e15) is **stored exactly** — it round-trips digit-for-digit into the
next step's state, into `final_state`, and through `Fork` and `Continue`. Reading one does not damage it
either. What matters is *how you read it back*:

```go
f.SetInt("orderID", 1234567890123456789)
f.GetInt("orderID")                     // exact — reads straight into an int

var order struct{ ID int64 `json:"orderID"` }
f.Get("orderID", &order.ID)             // exact — a typed target

var loose any
f.Get("orderID", &loose)                // ROUNDS to ...800 — untyped lands in a float64
```

**Typed accessors are exact at any magnitude.** `GetInt`, `GetFloat`, `GetString` and a `Get` into a typed
target all decode straight into that type. **Untyped reads round**, because JSON's number type decodes into
`float64` when there is nothing narrower to decode into: `Get` into an `any`, and the whole-state readers
that produce maps.

**One more place rounds, and it is easy to miss:** a `when` expression on a transition. Condition evaluation
re-decodes state into untyped values, so a `when` comparing a >2^53 id compares floats no matter how the
field was stored.

```go
g.AddTransition("A", "B").When("orderID == 1234567890123456789")  // unreliable at this magnitude
```

So: use typed accessors, and **carry the value as a string if you need to branch on it** — the same reason
APIs that mint 64-bit ids publish an `id_str` alongside them. A `time.Duration` is nanoseconds, so a
duration past ~104 days is in the same territory.

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
err := callFlaky(ctx)
if err != nil {
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
the engine fires the stop notification. When the operator calls `eng.Resume(ctx, flowKey, data)`, the task
re-runs and `Interrupt` returns `(false, nil)` with the caller's data unmarshaled into your `&resume`
pointer. The resume data is delivered through that pointer — it is **not** merged into state. See
[Driving flows → Resume](flows.md#resume).

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

To invoke a single unit of work as an isolated child, declare a one-node workflow and `Subgraph` it. A bare task
is only ever a node in a graph; it is not an independently invocable unit.

## Baggage (host context)

The opaque, host-defined value set in `FlowOptions.Baggage` at create time rides on the dispatch context
of every task. Read it with `workflow.BaggageFrom(ctx)` — it's how a host carries identity/claims, tenant,
or locale to task code without the engine interpreting it:

```go
func charge(ctx context.Context, f *workflow.Flow) error {
    claims := workflow.BaggageFrom(ctx) // a workflow.State; nil (safe to index) when none was set
    if actor, ok := claims["actor"].(string); ok {
        token := mintToken(actor) // e.g. act as the original caller
        ...
    }
    ...
}
```

Baggage is set once at `Create` (as any JSON object — a struct or map), frozen on the flow, inherited by
subgraphs and `Continue`, and delivered back as a `workflow.State` (the JSON-decoded form, numbers as
float64). See [Driving flows → Create](flows.md#create-and-run).

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

### Keep payload data out of error text

**An error message is not a private log line.** Whatever you return is stored on the flow and the step, and
it comes back from the two readers that expose no state payloads at all:

- **`List`** returns each flow's error and cancel reason on the summary.
- **`History`** returns each step's error, alongside metadata — it deliberately omits `state` and `changes`,
  but not the error.
- **`Query.Search`** substring-matches the error text, so flows can be *found* by what their errors contain.

An operator console built on `List` and `History` is normally the low-privilege surface — it shows what
happened without showing what was in it. Error text is the one channel that crosses that line, so anything
you interpolate is visible to everyone who can list flows, which is usually a wider group than those allowed
to read workflow data.

**Name the failure; don't quote the payload.**

```go
// Leaks: the card number and the email are now listable and searchable.
return fmt.Errorf("declined card %s for %s: %w", cardNumber, customerEmail, err)

// Fine: the failure is identifiable and the sensitive values stay in state.
return fmt.Errorf("charge declined for customer %s: %w", customerID, err)
```

Reference data by an internal identifier you can look up, rather than by its value. Be careful with `%w` and
`%v` on downstream errors too — an API client's error often quotes the request body it sent.

**This costs you nothing in diagnosability, and it makes triage better.** Errors that name a *class*
(`charge declined`, `upstream 503`) group across flows, which is what a search during an incident is
actually for. Errors that embed unique payload values match one flow each, so they are simultaneously the
least useful to search and the most sensitive to expose.

One related surface: an error's structured properties ride into the `onErr` state field for handler routing,
so they land in state like any other field rather than in the error column. Treat them with the same care
you would give any state value.

## Idempotency

**Execution is at-least-once: your task may run more than once for the same step.** The engine guarantees
that the flow's *persisted state* reflects exactly one execution — a late worker's writes are fenced off —
but it cannot fence your side effects, because it does not own your downstream. Charging a card, sending an
email, or posting to an API is yours to make safe.

This is not an edge case to design around later. It happens whenever:

- a replica dies, or is killed before its drain window finishes, and the step's lease lapses;
- a task overruns its `TimeBudget` and a peer re-claims the step while the first is still running;
- the database is unreachable long enough that the step's outcome cannot be recorded;
- a database is restored from backup, re-dispatching everything that was in flight.

**Two of those produce *concurrent* attempts, not merely sequential ones.** A slow task that loses its lease
keeps running while its replacement starts, so a dedupe that reads before it writes can be raced by its own
retry. Make the check and the effect atomic.

### `StepKey()` is your idempotency key

```go
func chargeCard(ctx context.Context, f *workflow.Flow) error {
    return payments.Charge(ctx, payments.Request{
        Amount:         f.GetInt("amountCents"),
        Customer:       f.GetString("customerID"),
        IdempotencyKey: f.StepKey(),   // stable across every re-execution of this step
    })
}
```

`f.StepKey()` is `{shard}-{stepID}-{token}` and is **stable across every re-execution of one step** — a
lease recovery, a `flow.Retry`, a re-dispatch after a database blip all rewind the same step row and hand
back the same key. It is also unique per step, so two steps of one flow never collide.

The other identifiers are the wrong choice, each for its own reason:

| | Why not |
|---|---|
| `f.Attempt()` | **Changes on every retry** — using it as a key defeats deduplication entirely. It is for *bounding* retries and for logs. |
| `f.FlowKey()` | Shared by every step in the flow, so two different steps would dedupe against each other. |
| A value you generate | A fresh UUID per execution is a new key on every attempt — the same bug as `Attempt()`. |

**A loop is not a retry.** A branch that revisits a task via `flow.Goto` creates a *new* step each time
around, so each iteration gets its own `StepKey` and is correctly treated as separate work. Likewise `Fork`
clones steps into new rows with new keys — a fork is a deliberate re-run and its side effects fire again.

### If the downstream has no idempotency key

Make the effect naturally repeatable, or record the effect atomically in a store you control:

```go
// A unique constraint on step_key turns "have I done this?" into one atomic write.
_, err := db.ExecContext(ctx,
    `INSERT INTO sent_emails (step_key, recipient) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
    f.StepKey(), recipient)
if err != nil {
    return err
}
// ... then send. A duplicate attempt inserts nothing and can skip.
```

Prefer shapes that are repeatable by construction: `PUT` over `POST`, "set the balance to X" over "add X",
upserts over inserts. An effect that is naturally idempotent needs no key at all.

**Give an unavoidably non-idempotent effect its own task.** A step's outcome is durably recorded before its
successor runs, so isolating the effect in a single-purpose task narrows the window in which a re-run can
duplicate it, and keeps the surrounding logic free to be retried without restriction.

### One thing you cannot rely on

**A task's `flow.Set` writes are discarded when it returns an error.** Both failure paths agree: the
`onError` handler receives the step's *input* state plus the error, never anything the failing attempt
wrote. So you cannot record "I already did the side effect" by writing it into state and then returning an
error — the record is dropped. Use an external store, as above.

## Timestamps

`f.CreatedAt()` and `f.UpdatedAt()` are populated on every dispatch. Use `CreatedAt` to implement a
workflow-level deadline in author space — the engine imposes none:

```go
if time.Since(f.CreatedAt()) > 24*time.Hour {
    return errors.New("workflow exceeded its 24h budget")
}
```

Next: [Driving flows](flows.md).
