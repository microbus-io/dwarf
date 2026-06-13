# Observability

Dwarf emits structured logs, OpenTelemetry metrics, and distributed traces. All three go through standard
providers you inject — the engine owns no exporter and adds no network connections. With nothing injected,
everything degrades to a no-op, so unconfigured and test use pays nothing.

## Logging

Inject a standard library `*slog.Logger`:

```go
eng.WithLogger(slog.Default())
```

It defaults to `slog.Default()`; a nil logger is coerced to it. The engine logs through the `…Context`
variants (`InfoContext`, `DebugContext`, …), so a context-aware handler can correlate each record with the
active step span. To route logs to OpenTelemetry, pass a logger whose handler bridges there — e.g. the
`otelslog` bridge — optionally fanned out to a stdout handler for container logs. The bridge stamps each
record with the active trace and span IDs, giving you trace↔log correlation for free.

## Metrics

Inject an OpenTelemetry `metric.MeterProvider`:

```go
eng.WithMeterProvider(otel.GetMeterProvider())
```

It defaults to the global `otel.GetMeterProvider()` — the no-op provider unless your process configures the
OpenTelemetry SDK. The engine builds its instruments under the scope `github.com/microbus-io/dwarf`;
service identity comes from the provider's Resource, not from per-metric attributes.

The engine emits 15 instruments — 8 counters and 7 gauges:

| `dwarf_*` metric | Type | Labels | Measures |
|---|---|---|---|
| `dwarf_flows_started_total` | counter | `workflow` | flows started |
| `dwarf_flows_terminated_total` | counter | `workflow`, `status` | flows reaching a terminal status |
| `dwarf_steps_executed_total` | counter | `task`, `status` | steps executed, by disposition |
| `dwarf_steps_recovered_total` | counter | — | steps recovered after a lease expiry |
| `dwarf_steps_skipped_saturated_total` | counter | `task` | admissions skipped at the rate ceiling |
| `dwarf_task_rate_cuts_total` | counter | `task` | adaptive-rate cuts (backpressure) |
| `dwarf_task_breaker_trips_total` | counter | `task`, `cause` | breaker trips |
| `dwarf_task_breaker_probes_total` | counter | `task`, `outcome`, `cause` | breaker probe attempts |
| `dwarf_steps_queue_depth` | gauge | — | steps in the local worker cache |
| `dwarf_steps_pending` | gauge | `priority` | due pending steps per priority band |
| `dwarf_steps_oldest_pending_age_seconds` | gauge | `priority` | age of the oldest due pending step |
| `dwarf_steps_fairness_keys` | gauge | `priority` | distinct fairness keys in the last refill |
| `dwarf_task_rate_limit` | gauge | `task` | adaptive per-task dispatch-rate ceiling |
| `dwarf_task_concurrency_running` | gauge | `task` | running steps per task |
| `dwarf_task_breaker_state` | gauge | `task` | 0 = closed, 1 = tripped |

The counters increment inline at their event sites; the gauges are observable (async) and read engine state
at collection time. Gauges emit **per replica** — sum them at the backend for cluster-wide totals. Labels
are deliberately bounded: there are no per-`fairness_key` labels (that would be unbounded cardinality), so
fairness/priority metrics are aggregate-only.

## Tracing

Inject an OpenTelemetry `trace.TracerProvider`:

```go
eng.WithTracerProvider(otel.GetTracerProvider())
```

It defaults to the global `otel.GetTracerProvider()` (no-op unless the SDK is configured). The host injects
only the provider — there is no span code to write and no trace context to thread by hand.

The engine creates two kinds of span, under the `github.com/microbus-io/dwarf` scope:

- A **root "workflow" span** at `Create`. It's detached (its own trace, not nested under the request that
  created the flow — a flow is a long-lived, async thing), and its W3C context is persisted on the flow so
  it survives across replicas and time.
- A **per-step span** in each dispatch, named by the task, parented to the flow's root span and **placed on
  the context handed to your `TaskExecutor`** — so any spans your task creates (the downstream call it
  makes) nest under it automatically.

Subgraphs nest naturally: a subgraph gets its own "workflow" span parented to the *caller step's* span, so
a trace reads `workflow → caller-step → workflow(subgraph) → subgraph-steps`, mirroring the call structure.
A step that yields and re-dispatches (after an interrupt or subgraph) produces one span per execution
attempt.

## Putting it together

A typical host configures the OpenTelemetry SDK once and injects all three providers:

```go
eng := dwarf.NewEngine().
    WithLogger(slog.New(otelslog.NewHandler("myapp"))).
    WithMeterProvider(otel.GetMeterProvider()).
    WithTracerProvider(otel.GetTracerProvider()).
    WithGraphLoader(loadGraph).
    WithTaskExecutor(runTask)
```

Next: [Deployment](deployment.md).
