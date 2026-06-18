# Dwarf documentation

Dwarf is a standalone, embeddable workflow-orchestration engine for Go. These guides cover how to use it
in your own project. For the full API reference, see
[pkg.go.dev/github.com/microbus-io/dwarf](https://pkg.go.dev/github.com/microbus-io/dwarf).

## Reading order

If you're new, read in this order:

1. **[Getting started](getting-started.md)** — install, the dependency-injection model, and your first
   running flow with the in-process test harness.
2. **[Concepts](concepts.md)** — the mental model: graph, task, flow, step, thread, reducer, and the flow
   lifecycle. Read this once and the rest will click.
3. **[Building graphs](graphs.md)** — the `workflow.Graph` API: tasks, transitions, conditions, fan-out,
   error handling, and reducers.
4. **[Writing tasks](tasks.md)** — the `workflow.Flow` carrier: reading and writing state, the control
   signals (retry, sleep, goto, interrupt, subgraph, subtask), baggage, and signaling backpressure / breakers.

Then dip into the topic guides as you need them:

- **[Engine operations](operations.md)** — every method on the engine: creating, running, inspecting,
  pausing/resuming, cancelling, restarting, continuing a thread, and retention.
- **[Fan-out & subgraphs](fan-out-and-subgraphs.md)** — running work in parallel and calling
  sub-workflows.
- **[Scheduling & reliability](scheduling-and-reliability.md)** — priority, fairness, adaptive
  backpressure, and circuit breakers.
- **[Observability](observability.md)** — structured logs, OpenTelemetry metrics, and distributed tracing.
- **[Deployment](deployment.md)** — choosing and tuning a database, sharding, connection pools, and
  running multiple replicas.
- **[Testing](testing.md)** — `RunInTest` and `TestProxy` patterns.

## The one-paragraph summary

You build a `workflow.Graph` of tasks and transitions. You implement a `Host` whose **`LoadGraph`**
returns a graph by name and whose **`ExecuteTask`** runs one task. The engine creates a **flow** (one
execution of a graph), runs each task in turn, persists state to SQL between steps, follows transitions
to decide what runs next, merges parallel branches, and recovers from crashes. You drive it with a handful
of operations — `Create`, `Start`, `Await`, `Run`, `Resume`, `Cancel` — and observe it through logs,
metrics, and traces.
