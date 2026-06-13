# Building graphs

A `workflow.Graph` is the definition of a workflow. You build one in code and hand it to the engine via
your `GraphLoader`. This guide covers the full graph-authoring API.

## Tasks

A graph is a set of tasks connected by transitions. Add a task with a **node name** (its identity in the
graph) and a **URL** (the address your `TaskExecutor` resolves to reach it):

```go
g := workflow.NewGraph("checkout")
g.AddTask("reserve", "inventory.reserve")
g.AddTask("charge", "billing.charge")
g.AddTask("ship", "fulfillment.ship")
```

The node name and URL can differ, which lets the same task URL appear at several positions under
different names. The engine passes the URL to your executor; your executor decides what that address means
(a function key, an HTTP endpoint, a message topic).

The **entry point** defaults to the first task added. Override it with `g.SetEntryPoint("reserve")`.

`workflow.END` is the sentinel terminal target — a transition to `END` ends the flow.

## Transitions

Transitions are the edges. After a task completes, the engine evaluates its outgoing transitions to decide
what runs next.

### Unconditional

```go
g.AddTransition("reserve", "charge")   // reserve always -> charge
g.AddTransition("ship", workflow.END)  // ship ends the flow
```

### Conditional (`when`)

`AddTransitionWhen` fires only if a boolean expression over the merged state is true. Expressions use
[boolexp](https://github.com/microbus-io/boolexp) syntax — infix comparisons and boolean operators over
state fields:

```go
g.AddTransitionWhen("triage", "escalate", "severity >= 3")
g.AddTransitionWhen("triage", "autoResolve", "severity < 3")
```

If **several** `when` transitions match, **all** of them fire in parallel — that's a fan-out (see
[Fan-out & subgraphs](fan-out-and-subgraphs.md)). If none match, the flow ends at the source task.

### First-match-wins (`switch`)

When you want exactly one branch — a router — use `AddTransitionSwitch`. Switch transitions from the same
source are evaluated in registration order; only the **first** whose expression is true fires, the rest
are skipped:

```go
g.AddTransitionSwitch("router", "handleHigh", "amount >= 10000")
g.AddTransitionSwitch("router", "handleMid", "amount >= 1000")
g.AddTransitionSwitch("router", "handleLow", "true") // catch-all
```

Because only one branch ever runs, switch branches need no fan-in. A node that uses switch transitions must
declare *every* success-path outgoing edge as switch — the validator rejects mixing switch with
when/plain/forEach/goto from the same source. (Error transitions are orthogonal and still allowed.)

### Dynamic fan-out (`forEach`)

`AddTransitionForEach` iterates an array field in the state and spawns one instance of the target task per
element, each receiving its element under the `as` key:

```go
// For each element of state["lineItems"], run "process" with the element bound to state["item"].
g.AddTransitionForEach("split", "process", "lineItems", "item")
```

Each branch also gets `<as>Index` (its position) and `<as>Count` (the cohort size) injected. An empty
array spawns nothing; if `forEach` is the only outgoing transition, an empty array ends the flow there.

### Explicit jumps (`goto`)

A `goto` transition is taken only when a task calls `flow.Goto(target)` — never during normal evaluation.
Use it for loops and computed routing:

```go
g.AddTransitionGoto("evaluate", "retry")   // taken only if a task calls flow.Goto("retry")
g.AddTransitionGoto("evaluate", "finish")
```

See [`flow.Goto`](tasks.md#goto).

### Error handling (`onError`, `onTimeout`)

When a task returns an ordinary error, the engine looks for a matching error transition before failing the
flow:

```go
g.AddTransitionOnError("charge", "refund")     // any error from charge -> refund
g.AddTransitionOnTimeout("charge", "slowPath")  // only when the error carries HTTP 408
```

If an error transition matches, the error is serialized into the state field `onErr` (as a structured
error), the failed step is marked completed with its changes preserved, and the handler task runs next. If
no error transition matches, the flow fails. Error transitions may carry a `when` but not `forEach`,
`goto`, or `switch`. If the failing task was part of a fan-out, its siblings are cancelled. `AddTransitionOnTimeout` is
shorthand for "on an error with HTTP status 408."

> Backpressure and breaker signals are **not** errors in this sense — a task wraps those with
> `workflow.ErrBackpressure` / `workflow.ErrBreakerTrip` and the engine handles them before error routing.
> See [Writing tasks → Signaling backpressure and breakers](tasks.md#signaling-backpressure-and-breakers).

## Fan-in and reducers

When parallel branches (from multiple `when` matches or a `forEach`) converge on a single task, that task
is a **fan-in**: the engine waits for all siblings, merges their `changes` field-by-field, and runs the
target once. Mark the convergence node and wire non-default merges:

```go
g.AddTransitionForEach("split", "process", "lineItems", "item")
g.AddTransition("process", "summarize")
g.SetFanIn("summarize")                          // opts into fan-in validation
g.SetReducer("results", workflow.ReducerAppend)  // each branch appends its result
g.SetReducer("total", workflow.ReducerAdd)       // each branch adds its subtotal
```

A field with no reducer uses `replace` (last write wins). Remember: a task writes only its **delta** to a
reducer-managed field. See [Fan-out & subgraphs](fan-out-and-subgraphs.md) for the full treatment.

## Validation

Call `g.Validate()` to check structural integrity before use — unreachable tasks, dangling transition
targets, inconsistent fan-out siblings, illegal mixes (switch with non-switch), and so on. The engine also
validates at create time. Validate early in tests:

```go
if err := g.Validate(); err != nil {
    t.Fatalf("invalid graph: %v", err)
}
```

## Annotations and inspection

`g.Annotate(name, note)` attaches documentation to a node, retrievable with `g.Annotation(name)` and
rendered in diagrams. Read-only inspectors include `Name`, `EntryPoint`, `Nodes`, `Transitions`,
`Reducers`, `URLOf`, `NamesForURL`, `IsFanIn`, `IsFanOutSource`, and `FanInFor`. Graphs marshal to and from
JSON (`MarshalJSON`/`UnmarshalJSON`), which is how the engine freezes a graph onto a flow.

Next: [Writing tasks](tasks.md).
