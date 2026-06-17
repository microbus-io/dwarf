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

g := workflow.NewGraph("Greet")
g.SetEndpoint("Hello", "http://example/hello") // node "Hello" dispatches to this endpoint URL
g.AddTransition("Hello", workflow.END)
proxy.HandleGraph("http://example/greet", g)

proxy.HandleTask("http://example/hello", func(ctx context.Context, f *workflow.Flow) error {
    f.SetString("greeting", "hello "+f.GetString("name"))
    return nil
})

eng := dwarf.NewEngine()
eng.SetHost(proxy) // TestProxy implements the Host interface
eng.RunInTest(t)   // SQLite in-memory, auto-cleanup

_, out, _ := eng.Run(ctx, "http://example/greet", map[string]any{"name": "ada"}, nil) // Run returns (flowKey, outcome, err)
fmt.Println(out.State["greeting"]) // hello ada
```

> By convention, graph and task names are PascalCase (`Greet`, `Hello`) — topology identifiers kept distinct
> from the lowercased dispatch URLs and the camelCase state fields. The engine imposes no casing.

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
| `github.com/microbus-io/dwarf/engine` | The engine: lifecycle, operations, config, the `Host` interface | The host process only |
| `github.com/microbus-io/dwarf/workflow` | Pure types: `Graph`, `Flow`, `FlowOptions`, reducers, error helpers | Any code that defines tasks or graphs |

The split matters: `dwarf/workflow` is a lightweight type package. Code that *defines* tasks and graphs
imports only `dwarf/workflow`, never the engine, so the engine's heavy dependencies (SQL drivers, the
scheduler) stay out of those builds.

## The host model

The engine reaches the outside world through a single `Host` interface, registered once with
`SetHost`. Only the first two methods are required; an implementation does nothing in the rest when it
has no stop-notification need or runs single-replica.

```go
type Host interface {
    // Required. Fetch a workflow graph by name (called at Create; the graph is then frozen on the flow,
    // and on subgraph spawn).
    LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error)

    // Required. Execute one task. The Flow carrier arrives with its input state populated; write outputs.
    ExecuteTask(ctx context.Context, taskName string, flow *workflow.Flow) error

    // Optional. Fired when a flow stops, for flows created with FlowOptions.NotifyOnStop.
    // The flow's baggage is on ctx; resolve where to deliver from it (the engine carries no address).
    FlowStopped(ctx context.Context, flowKey string, outcome *workflow.FlowOutcome)

    // Optional. Ship one cross-replica coordination signal to the other replicas (no-op for
    // single-replica). op is a routing key; payload is opaque bytes. Peers hand it back via
    // eng.DeliverSignal(ctx, op, payload).
    SignalPeers(ctx context.Context, op string, payload []byte)
}
```

A standalone host backs `LoadGraph` with an in-memory registry / file / database, and `ExecuteTask`
with a local function table or an RPC client. A bus-based host (for example a microservice mesh) bridges
them to its transport. The engine never learns how tasks are reached.

## Production wiring

Each `Set*` returns an `error` (there is no fluent `With*` builder — dropping the chained return is what
lets every setter surface its error, so misconfiguration fails loudly at wiring time):

```go
eng := dwarf.NewEngine()
check := func(err error) {
    if err != nil {
        log.Fatal(err)
    }
}
check(eng.SetDSN("postgres://user:pass@db:5432/dwarf"))
check(eng.SetNumShards(2))
check(eng.SetWorkers(64))
check(eng.SetHost(host))
check(eng.SetLogger(slog.Default()))
check(eng.SetMeterProvider(otel.GetMeterProvider()))
check(eng.SetTracerProvider(otel.GetTracerProvider()))

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
