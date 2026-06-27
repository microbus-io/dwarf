# Observability

Dwarf emits structured logs, OpenTelemetry metrics, and distributed traces. All three go through standard
providers you inject — the engine owns no exporter and adds no network connections. With nothing injected,
everything degrades to a no-op, so unconfigured and test use pays nothing.

## Logging

Inject a standard library `*slog.Logger`:

```go
eng.SetLogger(slog.Default())
```

It defaults to a discard logger — the engine (and its sequel DB layer) stay silent until you inject one,
rather than writing to the application-owned `slog.Default()`; a nil logger resets to that silent default.
The engine logs through the `…Context`
variants (`InfoContext`, `DebugContext`, …), so a context-aware handler can correlate each record with the
active step span. To route logs to OpenTelemetry, pass a logger whose handler bridges there — e.g. the
`otelslog` bridge — optionally fanned out to a stdout handler for container logs. The bridge stamps each
record with the active trace and span IDs, giving you trace↔log correlation for free.

## Metrics

Inject an OpenTelemetry `metric.MeterProvider`:

```go
eng.SetMeterProvider(otel.GetMeterProvider())
```

It defaults to the global `otel.GetMeterProvider()` — the no-op provider unless your process configures the
OpenTelemetry SDK. The engine builds its instruments under the scope `github.com/microbus-io/dwarf`;
service identity comes from the provider's Resource, not from per-metric attributes.

The engine emits 10 instruments — 5 counters and 5 gauges. Counter instrument names carry **no** `_total`
suffix; a Prometheus exporter appends it at the scrape boundary, so the names below are what you query in
PromQL **with** `_total` (e.g. the `dwarf_flows_started` instrument is queried as `dwarf_flows_started_total`):

| `dwarf_*` instrument | Type | Labels | Measures | PromQL name |
|---|---|---|---|---|
| `dwarf_flows_started` | counter | `workflow` | flows started | `dwarf_flows_started_total` |
| `dwarf_flows_terminated` | counter | `workflow`, `status` | flows reaching a terminal status | `dwarf_flows_terminated_total` |
| `dwarf_steps_executed` | counter | `task`, `status` | steps executed, by disposition | `dwarf_steps_executed_total` |
| `dwarf_steps_recovered` | counter | — | steps recovered after a lease expiry | `dwarf_steps_recovered_total` |
| `dwarf_steps_unwedged` | counter | — | wedged subgraph parks recovered by the sweep | `dwarf_steps_unwedged_total` |
| `dwarf_steps_queue_depth` | gauge | — | steps in the local worker cache | `dwarf_steps_queue_depth` |
| `dwarf_steps_pending` | gauge | `priority` | due pending steps per priority band | `dwarf_steps_pending` |
| `dwarf_steps_oldest_pending_age_seconds` | gauge | `priority` | age of the oldest due pending step | `dwarf_steps_oldest_pending_age_seconds` |
| `dwarf_steps_fairness_keys` | gauge | `priority` | distinct fairness keys in the last refill | `dwarf_steps_fairness_keys` |
| `dwarf_task_concurrency_running` | gauge | `task` | running steps per task | `dwarf_task_concurrency_running` |

The counters increment inline at their event sites; the gauges are observable (async) and read engine state
at collection time. Gauges emit **per replica** — sum them at the backend for cluster-wide totals. Labels
are deliberately bounded: there are no per-`fairness_key` labels (that would be unbounded cardinality), so
fairness/priority metrics are aggregate-only.

## Tracing

Inject an OpenTelemetry `trace.TracerProvider`:

```go
eng.SetTracerProvider(otel.GetTracerProvider())
```

It defaults to the global `otel.GetTracerProvider()` (no-op unless the SDK is configured). The host injects
only the provider — there is no span code to write and no trace context to thread by hand.

The engine creates two kinds of span, under the `github.com/microbus-io/dwarf` scope:

- A **root "workflow" span** at `Create`. It's detached (its own trace, not nested under the request that
  created the flow — a flow is a long-lived, async thing), and its W3C context is persisted on the flow so
  it survives across replicas and time.
- A **per-step span** in each dispatch, named by the task, parented to the flow's root span and **placed on
  the context handed to your host's `ExecuteTask`** — so any spans your task creates (the downstream call it
  makes) nest under it automatically.

Subgraphs nest naturally: a subgraph gets its own "workflow" span parented to the *caller step's* span, so
a trace reads `workflow → caller-step → workflow(subgraph) → subgraph-steps`, mirroring the call structure.
A step that yields and re-dispatches (after an interrupt or subgraph) produces one span per execution
attempt.

## SQL layer

The same three providers are handed to the engine's `sequel` database layer, so the SQL underneath your
workflows shows up in the same pipeline: `sequel_*` query/transaction/lock-contention metrics, per-operation
spans (nested under the active step span), and migration logs — all under the scope
`github.com/microbus-io/sequel`. The logger is forwarded only when you explicitly set one, so an
unconfigured engine stays silent here too.

## Configuration timing

All three observability knobs — `SetLogger`, `SetMeterProvider`, `SetTracerProvider` — are
**construction-time**: set them before `Startup`. The engine resolves the providers and wires them into the
worker hot path and the shard DBs once at startup, so a call after `Startup` is a deliberate no-op (it
keeps the hot-path reads lock-free). Hot-swapping a provider on a live engine is not supported.

## Putting it together

A typical host configures the OpenTelemetry SDK once and injects all three providers:

```go
eng := dwarf.NewEngine()
eng.SetLogger(slog.New(otelslog.NewHandler("myapp")))
eng.SetMeterProvider(otel.GetMeterProvider())
eng.SetTracerProvider(otel.GetTracerProvider())
eng.SetHost(host)
```

Next: [Deployment](deployment.md).
