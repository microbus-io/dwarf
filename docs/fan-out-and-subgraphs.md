# Fan-out & subgraphs

Two features let a workflow do more than a straight line: **fan-out/fan-in** runs work in parallel and
merges the results, and **subgraphs** call another workflow as a child.

## Fan-out

A fan-out runs several branches concurrently from one task. There are two kinds.

### Static fan-out

When more than one outgoing transition matches, all matching targets run in parallel:

```go
g.AddTransition("ingest", "validate")    // both fire after ingest -> parallel
g.AddTransition("ingest", "index")
```

Or with conditions — every matching `when` fires:

```go
g.AddTransitionWhen("triage", "notifyOps", "severity >= 3")
g.AddTransitionWhen("triage", "logIncident", "severity >= 1")
```

(Contrast `AddTransitionSwitch`, where only the first match fires — see
[Building graphs](graphs.md#first-match-wins-switch).)

### Dynamic fan-out (`forEach`)

`forEach` spawns one instance of a task per element of a state array — the count is data-driven:

```go
// state["lineItems"] = [ {...}, {...}, {...} ]  -> three parallel "process" branches
g.AddTransitionForEach("split", "process", "lineItems", "item")
```

Each branch receives:

- its element under the `as` key (`item` here),
- `<as>Index` — the element's position in the array,
- `<as>Count` — the cohort size.

The source array (`lineItems`) is stripped from each branch's local state, so an N-element fan-out feeding
a long chain doesn't copy the whole array into every branch's every step. The array reappears at the fan-in
(rebuilt from the spawning step). An empty array spawns nothing; if `forEach` is the only outgoing
transition, an empty array ends the flow.

To suppress the source array past the fan-in, a branch can call `f.Set("<source>", nil)`.

## Fan-in

When parallel branches converge on a single task, that task is a **fan-in**. The engine waits for every
sibling, merges their `changes` field-by-field, and runs the target once. Mark the convergence node with
`SetFanIn` and wire each non-default field with a reducer:

```go
g.AddTransitionForEach("split", "process", "lineItems", "item")
g.AddTransition("process", "summarize")
g.SetFanIn("summarize")

g.SetReducer("results", workflow.ReducerAppend)  // each branch contributes one result
g.SetReducer("subtotal", workflow.ReducerAdd)    // summed across branches
g.SetReducer("seenSkus", workflow.ReducerUnion)  // deduplicated union
```

Inside a branch, write only your **delta**:

```go
func process(ctx context.Context, f *workflow.Flow) error {
    item := /* read f.Get("item", ...) */
    f.Set("results", []any{transform(item)}) // ONE result; reducer appends across branches
    f.Set("subtotal", item.Price)            // this branch's amount; reducer adds them up
    return nil
}
```

Merge order is the input-array order (deterministic), not completion order, so `append`/`add`/`union`
results are stable. The reducers available are listed in [Concepts → Reducer](concepts.md#reducer).

A few rules worth knowing:

- **Fan-out siblings must share the same outgoing transition targets.** The engine evaluates transitions
  from the last sibling to finish; `Validate()` enforces this so the result can't depend on finish order.
- **The per-branch bookkeeping (`item`, `itemIndex`, `itemCount`) is stripped at the fan-in.** Forward an
  element value past the fan-in under a different key if you need it.
- **A failed or cancelled sibling doesn't poison the fan-in.** It contributes nothing to the merge; the
  flow is driven by the failure/error path instead.

## Subgraphs

A subgraph runs another workflow as a child and returns its result — composition and reuse for workflows.
A task launches one with `flow.Subgraph`, which is **semantically a function call**: only the explicit
`input` crosses into the child, and only the child's final state crosses back. The parent's state does not
auto-cross either direction.

```go
func enrich(ctx context.Context, f *workflow.Flow) error {
    out, yield, err := f.Subgraph("enrichment.workflow", map[string]any{
        "id": f.GetString("customerID"),
    })
    if err != nil {
        return err
    }
    if yield {
        return nil // child launched; this step parks until the child completes
    }
    f.Set("profile", out["profile"]) // adopt the fields you want from the child's result
    return nil
}
```

The two-call shape mirrors [`Interrupt`](tasks.md#interrupt-human-in-the-loop): the first call launches the
child and parks the parent step (`yield == true` — return immediately); when the child completes the parent
re-runs and the call returns the child's final state (`yield == false`).

- Pass `nil` input for "no arguments"; pass `f.Snapshot()` to forward the parent's entire state.
- A small upstream adapter task using `f.Transform(newKey, oldKey, …)` is a clean way to reshape parent
  state into the child's expected input shape.
- Subgraphs inherit the parent's baggage and run on the parent's shard. They start their own thread, so
  they don't contaminate the parent's `Continue` chain.
- Subgraphs can interrupt: an interrupt inside a child propagates up so the caller awaiting the top-level
  flow sees `interrupted`, and a single `Resume` continues the leaf.

Next: [Scheduling & reliability](scheduling-and-reliability.md).
