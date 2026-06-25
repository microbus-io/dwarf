# Building graphs

A `workflow.Graph` is the definition of a workflow. You build one in code and hand it to the engine via
your host's `LoadGraph`. This guide covers the full graph-authoring API.

## Tasks

A graph is a set of tasks connected by transitions. Add a task with a **node name** (its identity in the
graph) and a **URL** (the address your host's `ExecuteTask` resolves to reach it):

```go
g := workflow.NewGraph("Checkout", "checkout")
g.AddTask("Reserve", "inventory.reserve")
g.AddTask("Charge", "billing.charge")
g.AddTask("Ship", "fulfillment.ship")
```

The node name and URL can differ, which lets the same task URL appear at several positions under
different names. The engine passes the URL to your executor; your executor decides what that address means
(a function key, an HTTP endpoint, a message topic).

By convention, **graph and task (node) names are PascalCase** (`Reserve`, `Charge`) — they are
graph-topology identifiers, distinct from the lowercased URLs they dispatch to and from the camelCase
**state fields** tasks read and write (`lineItems`, `loopsLeft`). Keeping the three namespaces visually
distinct makes graphs easier to read; the engine itself imposes no casing.

The **entry point** defaults to the first task added. Override it with `g.SetEntryPoint("Reserve")`.

`workflow.END` is the sentinel terminal target — a transition to `END` ends the flow.

## Transitions

Transitions are the edges. After a task completes, the engine evaluates its outgoing transitions to decide
what runs next.

### Unconditional

```go
g.AddTransition("Reserve", "Charge")   // Reserve always -> Charge
g.AddTransition("Ship", workflow.END)  // Ship ends the flow
```

### Conditional (`when`)

`AddTransitionWhen` fires only if a boolean expression over the merged state is true. Expressions use
[boolexp](https://github.com/microbus-io/boolexp) syntax — infix comparisons and boolean operators over
state fields:

```go
g.AddTransitionWhen("Triage", "Escalate", "severity >= 3")
g.AddTransitionWhen("Triage", "AutoResolve", "severity < 3")
```

If **several** `when` transitions match, **all** of them fire in parallel — that's a fan-out (see
[Fan-out & subgraphs](fan-out-and-subgraphs.md)). If none match, the flow ends at the source task.

### First-match-wins (`switch`)

When you want exactly one branch — a router — use `AddTransitionSwitch`. Switch transitions from the same
source are evaluated in registration order; only the **first** whose expression is true fires, the rest
are skipped:

```go
g.AddTransitionSwitch("Router", "HandleHigh", "amount >= 10000")
g.AddTransitionSwitch("Router", "HandleMid", "amount >= 1000")
g.AddTransitionSwitch("Router", "HandleLow", "true") // catch-all
```

Because only one branch ever runs, switch branches need no fan-in. A node that uses switch transitions must
declare *every* success-path outgoing edge as switch — the validator rejects mixing switch with
when/plain/forEach/goto from the same source. (Error transitions are orthogonal and still allowed.)

### Static fan-out (`fanOut`)

When several **statically named** tasks should run in parallel off one source, `AddTransitionFanOut` wires
an unconditional edge from the source to each destination — the convenience form of repeated
`AddTransition` calls:

```go
// Verify, ScoreCredit, and FraudCheck all fire after Intake and run in parallel.
g.AddTransitionFanOut("Intake", "Verify", "ScoreCredit", "FraudCheck")
```

It creates only the outgoing edges; if the branches rejoin, the join node still needs `SetFanIn` (and
usually a reducer). Its linear sibling is `AddTransitionChain("A", "B", "C")`, which wires consecutive
pairs (`A -> B -> C`). Use `AddTransitionForEach` below instead when the parallel branches are *dynamic* —
one instance of a single task per runtime array element.

### Dynamic fan-out (`forEach`)

`AddTransitionForEach` iterates an array field in the state and spawns one instance of the target task per
element, each receiving its element under the `as` key:

```go
// For each element of state["lineItems"], run "Process" with the element bound to state["item"].
g.AddTransitionForEach("Split", "Process", "lineItems", "item")
```

Each branch also gets `<as>Index` (its position) and `<as>Count` (the cohort size) injected. An empty
array spawns nothing; if `forEach` is the only outgoing transition, an empty array ends the flow there.

### Explicit jumps (`goto`)

A `goto` transition is taken only when a task calls `flow.Goto(target)` — never during normal evaluation.
Use it for loops and computed routing:

```go
g.AddTransitionGoto("Evaluate", "Retry")   // taken only if a task calls flow.Goto("Retry")
g.AddTransitionGoto("Evaluate", "Finish")
```

See [`flow.Goto`](tasks.md#goto).

### Error handling (`onError`)

When a task returns an ordinary error, the engine routes to an `onError` handler if one is declared,
before failing the flow:

```go
g.AddTransitionOnError("Charge", "Refund") // any error from Charge -> Refund
```

The error from `Charge` routes to `Refund` rather than failing the flow. The error is serialized into the
state field `onErr` (as a structured error the handler can read), the failed step is marked completed with
its changes preserved, and the handler runs next. If no `onError` handler is declared, the flow fails. An
`onError` transition can't combine with `forEach`, `goto`, or `switch`. If the failing task was part of a
fan-out, its siblings are cancelled.

> The engine routes on *any* error — it never inspects the error's HTTP status or text. To handle a
> specific failure kind (e.g. a timeout), branch inside the task: `flow.Retry` for transient failures,
> `flow.Goto` for computed recovery, or return the error and let the `onError` handler deal with it.

## Fan-in and reducers

When parallel branches (from multiple `when` matches or a `forEach`) converge on a single task, that task
is a **fan-in**: the engine waits for all siblings, merges their `changes` field-by-field, and runs the
target once. Mark the convergence node and wire non-default merges:

```go
g.AddTransitionForEach("Split", "Process", "lineItems", "item")
g.AddTransition("Process", "Summarize")
g.SetFanIn("Summarize")                          // opts into fan-in validation
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
err := g.Validate()
if err != nil {
    t.Fatalf("invalid graph: %v", err)
}
```

## Annotations and inspection

`g.Annotate(name, note)` attaches documentation to a node, retrievable with `g.Annotation(name)` and
rendered in diagrams. Read-only inspectors include `Name`, `EntryPoint`, `Nodes`, `Transitions`,
`Reducers`, `URLOf`, `NamesForURL`, `IsFanIn`, `IsFanOutSource`, and `FanInFor`. Graphs marshal to and from
JSON (`MarshalJSON`/`UnmarshalJSON`), which is how the engine freezes a graph onto a flow.

Next: [Writing tasks](tasks.md).
