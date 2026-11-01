# Fan-out & subgraphs

> **For developers** building workflows that branch. Both features here are graph-authoring plus task-side
> API, so read [Building graphs](graphs.md) and [Writing tasks](tasks.md) first.

Two features let a workflow do more than a straight line: **fan-out/fan-in** runs work in parallel and
merges the results, and **subgraphs** call another workflow as a child.

## Fan-out

A fan-out runs several branches concurrently from one task. There are two kinds.

### Static fan-out

When more than one outgoing transition matches, all matching targets run in parallel:

```go
g.AddTransition("Ingest", "Validate")    // both fire after Ingest -> parallel
g.AddTransition("Ingest", "Index")
```

Or with conditions — every matching `when` fires:

```go
g.AddTransitionWhen("Triage", "NotifyOps", "severity >= 3")
g.AddTransitionWhen("Triage", "LogIncident", "severity >= 1")
```

(Contrast `AddTransitionSwitch`, where only the first match fires — see
[Building graphs](graphs.md#first-match-wins-switch).)

### Dynamic fan-out (`forEach`)

`forEach` spawns one instance of a task per element of a state array — the count is data-driven:

```go
// state["lineItems"] = [ {...}, {...}, {...} ]  -> three parallel "Process" branches
g.AddTransitionForEach("Split", "Process", "lineItems", "item")
```

Each branch receives:

- its element under the `as` key (`item` here),
- `<as>Index` — the element's position in the array,
- `<as>Count` — the cohort size.

A branch otherwise sees the flow's state as it was at the fan-out, including the source array (`lineItems`)
its own element came from.

An **empty array spawns no branches, but the flow does not stop there** — it routes straight to the fan-in,
which runs with the source task's own output and no branch contributions. So write the fan-in to tolerate a
cohort of zero (a reducer-managed field just keeps its incoming value; any per-element output it expected is
absent), and expect the branch task to sometimes never run at all.

A large carried field above a size threshold is stored once and referenced rather than copied into every
branch's stored steps, so an N-element fan-out over a chain of depth D does not cost N×D copies of it. To drop
a field entirely once it has been fanned out, a branch can still call `f.Set("<source>", nil)` to remove
it from the flow's state past the fan-in.

## Fan-in

When parallel branches converge on a single task, that task is a **fan-in**. The engine waits for every
sibling, merges their `changes` field-by-field, and runs the target once. Mark the convergence task with
`SetFanIn` and wire each non-default field with a reducer:

```go
g.AddTransitionForEach("Split", "Process", "lineItems", "item")
g.AddTransition("Process", "Summarize")
g.SetFanIn("Summarize")

g.SetReducer("results", workflow.ReducerAppend)  // each branch contributes one result
g.SetReducer("subtotal", workflow.ReducerAdd)    // summed across branches
g.SetReducer("seenSkus", workflow.ReducerUnion)  // deduplicated union
```

On a field with a reducer, every write is a contribution, in a branch or anywhere else in the flow, so write
only your own **delta**, never the value you read:

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

- **All branches of one fan-out must converge on the same fan-in task**, and `Validate()` enforces it at
  `Create`. The engine picks the convergence task from whichever sibling finishes last, so branches routed
  to *different* fan-ins would make the result depend on finish order. Branches may take different
  intermediate paths, and may reach the fan-in by any kind of edge (a `goto` or `onError` included) — only
  the fan-in they end at must agree.
- **A goto or error transition out of the fan-out task bypasses the fan-out, so it must not rejoin it.**
  Taking one means no branches are spawned at all, so `Validate()` accepts it only where that holds together:
  to `workflow.End`, straight to the fan-in task (which then runs once on the source's own state, exactly as an empty
  array does), or onto a path that never reaches the fan-in. Routing it into a *branch*, or onto a path that
  later leads back into the fan-in, is rejected at `Create` — the fan-in would be waiting on a set of branches
  that was never created. This is what makes the "fan out over the array, or end the flow when there is
  nothing left to do" loop expressible: give the source an `AddTransitionGoto(source, workflow.End)` alongside its
  `AddTransitionForEach`, and the task picks per round.
- **The per-branch bookkeeping (`item`, `itemIndex`, `itemCount`) does not survive the fan-out.** It is gone
  from the flow's state once the cohort is behind it — at the fan-in, and in the final state of a flow whose
  fan-out failed. (Otherwise one arbitrary branch's element would ride forward as the flow's own, picked by
  whichever branch happened to finish last.) Forward an element value under a different key if you need it.
- **Fan-outs nest.** A branch may fan out again, to any depth. Each inner fan-out converges at its own
  fan-in, and everything its branches wrote reaches the outer fan-in as part of that branch's contribution,
  folded through the outer reducers in branch order. A flow whose nested fan-out fails keeps, in its final
  state, everything its completed steps wrote, at every level.
- **A failed, terminated, or cancelled sibling doesn't poison the fan-in.** It contributes nothing to the
  merge; the flow is driven by the failure/error path instead.
- **A value its reducer cannot fold fails the flow.** A branch writing a string to an `add` field, say, fails
  the flow at the fan-in with an error naming the field; the fan-in task does not run. The step recorded as
  failed is the branch step whose arrival resolved the cohort, which need not be the one that wrote the value,
  and the failed flow's final state is empty, since it cannot be merged either.

## Subgraphs

A subgraph runs another workflow as a child and returns its result — composition and reuse for workflows.
A task launches one with `flow.Subgraph`, which is **semantically a function call**: only the explicit
`in` crosses into the child, and only the child's final state crosses back. The parent's state does not
auto-cross either direction.

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
        return nil // child launched; this step parks until the child completes
    }
    f.Set("profile", out.Profile) // adopt the fields you want from the child's result
    return nil
}
```

The two-call shape mirrors [`Interrupt`](tasks.md#interrupt-human-in-the-loop): the first call launches the
child and parks the parent step (`yield == true` — return immediately); when the child completes the parent
re-runs and the call returns `yield == false` with the child's final state unmarshaled into the trailing
`out` pointer (`*struct`, `*map[string]any`, or `nil` to ignore).

To invoke a single unit of work in isolation, declare a one-task workflow and `Subgraph` it. A task only
ever runs as part of a graph, never as an independently invocable child on its own.

- Pass `nil` input for "no arguments"; pass `f.Snapshot()` to forward the parent's entire state.
- Writing the child's result into a reduced field of the parent is a contribution like any other. So have the
  child return only what it adds — its new messages, say — and `Set` those. Passing the history in and
  `Set`ting the history the child returns would add it twice.
- Pass a struct or `map[string]any` as `in` to hand the child exactly the fields it expects — no need to
  reshape the parent's state first.
- Subgraphs inherit the parent's baggage and run on the parent's shard. A subgraph child cannot
  be continued: `Continue` takes a root flow's key.
- Subgraphs can interrupt: an interrupt inside a child propagates up so the caller awaiting the top-level
  flow sees `interrupted`, and a single `Resume` continues the leaf.

Next: [Scheduling & reliability](scheduling-and-reliability.md).
