# Dwarf

[![Go Reference](https://pkg.go.dev/badge/github.com/microbus-io/dwarf.svg)](https://pkg.go.dev/github.com/microbus-io/dwarf)

**Dwarf is a standalone, embeddable workflow-orchestration engine for Go.**

You describe a workflow as a graph of tasks; dwarf runs it — dispatching one task at a time per step,
persisting state between steps in a SQL database, and handling the hard parts of durable orchestration:
parallel fan-out/fan-in, conditional routing, retries with backoff, timed sleeps, subgraphs,
human-in-the-loop pauses, adaptive backpressure, and circuit breakers.

Dwarf has **no built-in transport**. It doesn't know how your tasks are reached (a local function call, an
RPC, a message bus) or where your graphs live. You wire it to your world through a few small dependency
interfaces, and it handles scheduling, state, durability, and recovery.

```go
proxy := engine.NewTestProxy()

g := workflow.NewGraph("greet")
g.AddTask("hello", "hello")
g.AddTransition("hello", workflow.END)
proxy.HandleGraph("greet", g)

proxy.HandleTask("hello", func(ctx context.Context, f *workflow.Flow) error {
    f.SetString("greeting", "hello "+f.GetString("name"))
    return nil
})

eng := dwarf.NewEngine().
    WithGraphLoader(proxy.LoadGraph).
    WithTaskExecutor(proxy.ExecuteTask)
eng.RunInTest(t) // SQLite in-memory, auto-cleanup

out, _ := eng.Run(ctx, "greet", map[string]any{"name": "ada"}, nil)
fmt.Println(out.State["greeting"]) // hello ada
```

## Why dwarf

- **Durable by construction.** Every step is checkpointed to SQL. A crashed worker's in-flight step is
  recovered by lease expiry; a flow can be inspected, resumed, restarted, or continued days later.
- **Parallelism that merges cleanly.** Static and dynamic (`forEach`) fan-out run branches concurrently;
  fan-in merges their state with per-field reducers (append, add, union, merge, …).
- **Human-in-the-loop.** A task can `Interrupt` to park the flow for external input and `Resume` later —
  approvals, manual review, async callbacks.
- **Adaptive under load.** A per-task valve discovers each downstream's safe dispatch rate from observed
  `429`s (TCP CUBIC recovery); a per-task circuit breaker parks and probes a downstream that's
  unreachable, in maintenance, or overloaded.
- **Fair and prioritized.** Two-level scheduling: strict priority bands across the cluster, weighted
  fairness within a band so one tenant can't starve another.
- **Scales horizontally.** Run many replicas against sharded databases; replicas coordinate through
  fire-and-forget peer signals you publish however you like.
- **OTEL-native observability.** Structured logs (`slog`), 15 `dwarf_*` metrics, and distributed tracing,
  all through standard providers you inject.
- **Four SQL dialects.** PostgreSQL, MySQL/MariaDB, SQL Server, and SQLite (testing / single-instance).

Dwarf depends only on [`sequel`](https://github.com/microbus-io/sequel) (SQL) and
[`throttle`](https://github.com/microbus-io/throttle) (rate limiting), plus the OpenTelemetry API.

## Install

```bash
go get github.com/microbus-io/dwarf
```

Requires Go 1.26+.

## Packages

| Import | Role | Who imports it |
|---|---|---|
| `github.com/microbus-io/dwarf` | Thin convenience: `NewEngine()` | The host process |
| `github.com/microbus-io/dwarf/engine` | The engine: lifecycle, operations, config, dependency interfaces | The host process only |
| `github.com/microbus-io/dwarf/workflow` | Pure types: `Graph`, `Flow`, `FlowOptions`, reducers, error helpers | Any code that defines tasks or graphs |

The split matters: `dwarf/workflow` is a lightweight type package. Code that *defines* tasks and graphs
imports only `dwarf/workflow`, never the engine, so the engine's heavy dependencies (SQL drivers, the
scheduler) stay out of those builds.

## The dependency-injection model

The engine reaches the outside world through four injection points. Only the first two are required.

```go
// Fetch a workflow graph by name. Called once at Create; the graph is then frozen on the flow.
type GraphLoader func(ctx context.Context, workflowName string) (*workflow.Graph, error)

// Execute one task. The Flow carrier arrives with its input state populated; write outputs to it.
type TaskExecutor func(ctx context.Context, taskName string, flow *workflow.Flow) error

// Optional: fired when a flow stops, for flows started via StartNotify.
type FlowStoppedCallback func(ctx context.Context, hostname string, outcome *workflow.FlowOutcome)

// Optional: cross-replica coordination (nil for single-replica).
type PeerNotifier interface { ... }
```

A standalone host backs `GraphLoader` with an in-memory registry / file / database, and `TaskExecutor`
with a local function table or an RPC client. A bus-based host (for example a microservice mesh) bridges
them to its transport. The engine never learns how tasks are reached.

## Production wiring

```go
eng := dwarf.NewEngine().
    WithDSN("postgres://user:pass@db:5432/dwarf").
    WithNumShards(2).
    WithWorkers(64).
    WithGraphLoader(loadGraph).
    WithTaskExecutor(runTask).
    WithLogger(slog.Default()).
    WithMeterProvider(otel.GetMeterProvider()).
    WithTracerProvider(otel.GetTracerProvider())

if err := eng.Startup(ctx); err != nil {
    log.Fatal(err)
}
defer eng.Shutdown(ctx)

flowKey, err := eng.Create(ctx, "checkout", initialState, &workflow.FlowOptions{
    Priority:    10,
    FairnessKey: tenantID,
    Baggage:     actorClaims,
})
eng.Start(ctx, flowKey)
outcome, err := eng.Await(ctx, flowKey)
```

The `With*` methods are atomic and may be called after `Startup` for hot reconfiguration.

## Database support

| Engine | Use | Notes |
|---|---|---|
| **PostgreSQL** 13+ | Recommended for production | MVCC, no gap locks; fan-out runs deadlock-free at any concurrency |
| **SQL Server** | Production | Enable `READ_COMMITTED_SNAPSHOT` for non-blocking reads |
| **MySQL / MariaDB** | Production, expect tuning | Prefer `READ-COMMITTED` isolation to drop gap locks |
| **SQLite** | Testing & single-instance dev only | Used automatically by `RunInTest`; do not run in production |

See [docs/deployment.md](docs/deployment.md) for tuning, sharding, and connection-pool guidance.

## Documentation

Full guides live in [`docs/`](docs/):

- [Getting started](docs/getting-started.md) — install, wiring, your first flow
- [Concepts](docs/concepts.md) — graph, task, flow, step, thread, reducer, lifecycle
- [Building graphs](docs/graphs.md) — transitions, conditions, fan-out, error handling, reducers
- [Writing tasks](docs/tasks.md) — the Flow carrier, state, control signals, baggage, error dispositions
- [Engine operations](docs/operations.md) — create, run, inspect, resume, cancel, restart, continue, retain
- [Fan-out & subgraphs](docs/fan-out-and-subgraphs.md) — parallelism, dynamic `forEach`, calling sub-workflows
- [Scheduling & reliability](docs/scheduling-and-reliability.md) — priority, fairness, backpressure, breakers
- [Observability](docs/observability.md) — logs, metrics, tracing
- [Deployment](docs/deployment.md) — database choice, sharding, config, multi-replica
- [Testing](docs/testing.md) — `RunInTest` and `TestProxy`

API reference: [pkg.go.dev/github.com/microbus-io/dwarf](https://pkg.go.dev/github.com/microbus-io/dwarf).

## License

Apache License 2.0. See [LICENSE](LICENSE).
