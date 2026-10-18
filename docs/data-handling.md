# Data handling

> **For developers and operators** who have to answer for what the engine stores: a compliance review, a
> deletion request, a decision about who may hold a flow key. It covers what lands in the database, which of
> it is reachable by free-text search, what telemetry carries, and how deletion actually works.

## What the engine stores

Everything durable is in the SQL databases you provide. There is no other store, no local disk state, and no
external service. Two tables carry data you supply.

**Per flow:**

| What | Where it comes from | Notes |
|---|---|---|
| Workflow URL and display name | `Create` | Used for routing and listing |
| The workflow graph | Your host's `LoadGraph`, **frozen at creation** | A full JSON copy per flow; it never changes afterward |
| Baggage | `FlowOptions.Baggage` | Opaque to the engine and handed back to every task. **This is where hosts typically put caller identity or tenant claims** |
| Final state | Computed at termination | The full merged state of the terminal step, unfiltered |
| Error text | A failing task's error | Free-text, and **searchable** — see below |
| Terminate / cancel reason | Your `Terminate` or `Cancel` call | Free-text, and **searchable** |
| Fairness key | `FlowOptions.FairnessKey` | Commonly a tenant id |
| Trace context | The tracer, if configured | A W3C trace parent |

**Per step:**

| What | Notes |
|---|---|
| Input state | A full snapshot of the state the task received |
| Changes | The output delta the task produced |
| Interrupt payload | What a task published when it parked for human input |
| Resume data | What was passed to `Resume` |
| Subgraph result and error | A child flow's output as returned to its caller |
| Error text | The task's error, if it failed |

Two things worth stating plainly. **Step history is a full audit trail by design** — every intermediate state
a flow passed through is retained, not just the final one, which is what makes `Fork` and post-hoc debugging
possible. And **the graph is copied per flow**, so a graph carrying anything sensitive in its structure or
task names is duplicated once per flow that runs it.

A large field carried unchanged across many steps is stored once and referenced, rather than copied into
every row. That is a storage optimisation with no bearing on access: readers resolve it transparently, so it
neither hides data from a query nor exposes any.

## Free-text search reaches errors

`Query.Search`, consumed by `List`, is a case-insensitive substring match. It covers exactly:

- the workflow URL and display name
- the name of the task the flow stopped at (`StoppedTaskName`), so a running flow never matches on a task
- **the flow's error text**
- **the terminate or cancel reason**
- the flow key

**It does not reach state, changes, final state, baggage, interrupt payloads, or resume data.** Workflow
payloads are not searchable.

The exposure to weigh is the error text, and it has two layers.

**Errors are returned outright.** `List` returns each flow's error and terminate or cancel reason, and `History` returns
each step's error — including in the payload-free readers. Task errors routinely quote the input that caused
them (an address, an account number, a downstream API's rejection message quoting the record it rejected),
so whatever your tasks interpolate into an error string is visible to anyone who can list flows, whether or
not they can read state.

**Search then makes them discoverable.** Without it you see errors for flows you already found; with it you
can find flows *by* their error content — including by a fragment of the data inside it. That turns the
column into a matching oracle: someone who cannot read payloads can still confirm whether a given card
number, email or account id exists in the system by searching for it and seeing whether anything comes back.

**Searching errors is deliberate, and the engine does not scrub them.** Finding flows by failure class is
how an operator establishes the blast radius of an incident, so the capability is kept as-is. That places
two obligations outside the engine:

- **Task authors keep PII out of error text.** Name the failure, don't quote the payload — reference data by
  an internal identifier instead of by its value. See
  [Writing tasks → Keep payload data out of error text](tasks.md#keep-payload-data-out-of-error-text) for
  the rule and examples. The same applies to whatever you pass as a terminate or cancel reason, which is stored verbatim.
- **Your host restricts who may call what.** The engine has no notion of a caller, so gating `List` — and
  `Search` in particular — by credential is yours to do, in the layer that holds the principal.

**This costs you nothing in diagnosability.** Errors that name a *class* (`charge declined`,
`upstream 503`) group across flows, which is what an incident search is actually for. Errors that embed
unique payload values match a single flow each, making them simultaneously the least useful to search and
the most sensitive to expose. Keeping payload data out improves triage and closes the exposure at once.

If you inherit a system whose errors already quote payloads, treat that as a task-code fix rather than an
access-control one: the text is returned by `List` and `History` regardless of whether search is available,
so restricting search alone does not close it.

## Access control is the host's job

**The engine has no authentication, no authorization, and no notion of a caller.** Its only vantage is the
flow reference and the task name. Ownership and tenancy — the axes any real access check turns on — are
structurally invisible to it. Every access decision belongs to the layer you build around it.

**A flow key is a capability, not a permission.** The key's random token makes the reference unguessable, so
a caller cannot enumerate flows by counting: without it, the numeric part of a flow key is a sequential
integer. What the token buys is that an authorization bug alone is not enough to walk the table — you would
also need each flow's token. What it is *not* is access control. **Anyone holding a flow key can resume,
terminate, fork and read that flow.** Treat a leaked, logged, or forwarded key as a full write capability for
that one flow.

**`List` is the amplifier.** It is the only operation that returns keys wholesale, so it converts "can call
the engine" into "holds a capability for every flow returned." Gate it accordingly, and prefer scoping it —
`Query.FairnessKey` is how a tenant-scoped listing is normally expressed, since hosts typically set the
fairness key to the tenant.

**Which reads return payloads:**

| Operation | Returns state payloads? | Returns error text? |
|---|---|---|
| `List` | No — summaries only | **Yes** — the flow's `Error`, `TerminateReason` and `CancelReason` |
| `History` | No — `State` and `Changes` are left empty | **Yes** — each step's `Error` |
| `Step` | **Yes** — that step's input state and changes | **Yes** |
| `Snapshot` / `Await` / `Run` | **Yes** — final state, or the interrupt payload | **Yes** |

So a read-only operator console built on `List` and `History` shows structure and failures without exposing
state payloads; adding `Step` or `Snapshot` exposes the payloads too.

**But note the second column.** Error text is returned by *every* reader, including the two that are
otherwise payload-free — so it is the one channel that crosses the boundary between the metadata tier and
the data tier. That is why [what goes into an error string](#free-text-search-reaches-errors) is an access
question, not just a debugging one.

**Subgraph child keys are read-only.** Lifecycle changes must be addressed to the root flow key, so holding a
child's key does not confer the ability to terminate or resume its tree.

## What telemetry carries

Telemetry deliberately carries a **token-free correlation id** — the shard and numeric flow id, without the
token — rather than the flow key. Trace backends are typically readable far more broadly than workflow data
is, and stamping keys onto spans would hand a write capability for every traced flow to every trace reader.

- **No log line emits a flow key.** Correlation ids only.
- **Span attributes carry the correlation id**, never the key.
- **There is no operation that converts a correlation id back into a key.** That is deliberate: such a
  lookup would mint capabilities. Resolving "the flow I found in a trace" happens outside the engine — from
  your own record of the keys you minted, under your own authorization.
- **`List` surfaces the trace id** on each summary so an operator can pivot to a trace backend. Like the
  correlation id, it grants nothing.

Metric labels are bounded on purpose: there are no per-fairness-key labels, so tenant identifiers never reach
your metrics backend as label cardinality.

## Retention and deletion

**The engine never deletes on a timer.** Every flow stays until you say otherwise, because every flow is
potentially resurrectable — an `interrupted` flow via `Resume`, a `completed` one via `Continue`, any
terminal one via `Fork`. No single retention duration fits both an hour-long batch and a 30-day approval, so
the engine refuses to pick one.

Three ways to remove data:

```go
// One flow and its whole subgraph tree. Refuses a running flow; terminates an interrupted one first.
eng.Delete(ctx, flowKey)

// Everything matching a query, except running flows; interrupted ones are terminated first.
count, err := eng.Purge(ctx, workflow.Query{
    Status:    workflow.StatusCompleted,
    OlderThan: 30 * 24 * time.Hour,
})
```

```go
// Or have a flow schedule its own removal when it completes successfully.
&workflow.FlowOptions{DeleteOnCompletion: true}
```

**Deletion is marked, then reaped.** No path deletes rows inline: the flow is stamped for deletion, drops out
of `List` and `History` immediately, and a background reaper removes the whole subgraph tree within about a
minute. A flow marked by `DeleteOnCompletion` keeps serving its outcome to `Snapshot` and `Await` for a
one-minute grace window first, so a fire-and-forget caller can still read its result.

**For a deletion request, that grace matters.** A flow is logically gone the moment it is marked, but its
rows exist for up to about a minute afterward — up to about two for a `DeleteOnCompletion` flow, whose
one-minute window comes before the reaper's pass — and `Snapshot` still answers until the reaper removes
them. If you need a hard immediate erasure guarantee, the marking is not it — verify the rows are gone.

**A scheduled retention job is the normal pattern.** `Purge` marks at most 4,096 roots per call (fewer if
`Query.Limit` is lower), divided across shards as `List` divides it, so a multi-shard call can mark slightly
more. A retention sweep is a loop until it returns 0, not a single call. `OlderThan` is anchored to the database's clock and measured
from the flow's finish time, which is the right signal for "keep for 30 days after it ended."

**Deleting a flow deletes its whole subgraph tree**, keyed on the tree root — children are not left behind.
You cannot delete a subgraph child on its own; that would strand its parent.

## The size limit: 256 MB per value

The one limit is what the database can store. No single JSON value dwarf writes may be larger than
**256 MB (256,000,000 bytes)**, on every database. This limit is per value, not per flow. In practice:

- **A task's changes in one step** must fit in 256 MB. That is what one step writes.
- **The flow's state while it runs** can be somewhat larger than 256 MB. A large field is stored once, in the
  step that wrote it, and later steps refer to it instead of copying it, so each step's own row holds only
  its small fields. The exception is a field under a reducer other than `append` (`concat`, `merge`,
  `union`, `add` and the rest): the step after one that contributes to it stores the combined value whole, so
  a large one counts toward the limit in that step's row. Steps that do not touch it refer to it as before. A
  field under `append` is stored as references to the steps that hold its elements once it is past about 1 KB,
  and so is a list with no reducer that a task rewrites each step but only ever extends: each step stores only
  the elements it added. That holds outside a fan-out's branches; a branch that rewrites such a list stores its
  whole value, as it always did.
- **The flow's state when it ends** must fit in 256 MB, because the final state holds every field at once.
  The same is true wherever a whole state crosses from one flow to another: a `Continue` turn's starting
  state, a subgraph's input and its result, and the step a `Fork` restarts from. A flow should drop large
  fields it no longer needs (`flow.Del`) before it ends.
- **Initial state, baggage, resume data and an interrupt payload** must each fit in 256 MB.

A value over the limit is refused before it is written. `Create`, `Continue`, `Resume` and `Fork` return
a 413 error and create nothing. A step whose changes are too large fails at once, with an error naming the
largest field, and its task is not run again. A flow whose final state is too large fails, and its final
state is stored empty.

PostgreSQL is the one database that can refuse a value below the limit: it stores JSON in a binary form that,
for an array of many small numbers, is several times larger than the text. Such a value fails when it is
written.

On MySQL and MariaDB the limit holds only if the server's `max_allowed_packet` is at least 512M; see
[Deployment](deployment.md#mysql--mariadb--supported-expect-tuning).

## What the engine does not bound

Below the size limit, the engine enforces **no** size or count limits: not on initial state, baggage, the frozen graph, interrupt
payloads, fan-out width, or subgraph nesting depth. This is the same reasoning as access control — a limit
that fits a small control flow would reject a document-processing workflow that legitimately carries tens of
megabytes per flow, and the identity a quota turns on is invisible to the engine.

**So quotas are yours to enforce**, before `Create`: reject an over-large initial state or baggage, cap a
fan-out's source array in an entry task, and bound your retention sweeps.

Two values are known not to survive storage, and both fail silently rather than loudly:

- **An integer beyond ±2^53, read untyped.** Storage itself is exact — the value round-trips through steps,
  the final state, `Fork` and `Continue` — and typed accessors like `GetInt`, and a `when` expression comparing
  it, are exact too. But a read into an untyped `any` (`Get` into an `any`, `Parse` into a map) comes back
  **rounded**, with no error anywhere. On MySQL, storage is exact only within 64 bits: an integer above
  2^64-1 or below -2^63, including an `add` sum that grows past it, is stored as a floating-point number.
  Carry large ids as strings if that applies.
- **A `NUL` byte (`U+0000`) in a string.** PostgreSQL rejects it; other dialects accept it. Base64-encode
  binary data. On PostgreSQL this surfaces as `dwarf_steps_write_failed_total` — see the
  [Runbook](runbook.md#dwarf_steps_write_failed-is-non-zero).

## Encryption

**At rest** is your database's job — transparent disk or tablespace encryption applies to everything above
with no engine involvement, and is the option that costs nothing operationally.

**In transit** is your connection string's job: enable TLS in the DSN. The engine uses the connection string
exactly as given and never rewrites it.

**Field-level encryption** is not supported by the engine and would have to happen in your tasks, encrypting
values before writing them into state. Doing so means those fields are opaque to transition conditions, to
reducers, and to anyone reading `Step` or `Snapshot` for debugging.

Encrypting the `error` column is not an option even in author space: it is written by the engine from your
task's returned error, and ciphertext there would break `Query.Search` (a substring match cannot run against
ciphertext) while making errors unreadable to the operators who need them. Keep sensitive values out of
error text instead, as above.
