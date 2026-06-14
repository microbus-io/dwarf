# Scheduling & reliability

Dwarf decides *which* pending step runs next (scheduling) and protects downstreams from overload (adaptive
backpressure and circuit breakers). Most of this is automatic; this guide explains the knobs you control
and the signals you send.

## Priority

Priority arbitrates *between* flows competing for workers. It's a property of the flow, set once at create
and immutable for its life:

```go
eng.Create(ctx, "report", state, &workflow.FlowOptions{Priority: 5})
```

Lower numbers run first. An unset priority (0) uses the engine default (`WithDefaultPriority`, 100 by
default). Priority is **strict and cluster-wide**: as long as any priority-5 work is due, no priority-10
work runs. There is no aging — a steady stream of high-priority flows will starve lower bands by design, so
reserve the top bands for genuinely urgent work.

Step order *within* a flow is dictated by the graph, not by priority.

## Fairness

Within a priority band, **fairness** prevents one busy tenant from monopolizing the workers. Tag flows with
a `FairnessKey` (typically the tenant) and an optional `FairnessWeight`:

```go
eng.Create(ctx, "import", state, &workflow.FlowOptions{
    FairnessKey:    "tenant-42",
    FairnessWeight: 4, // 4x the dispatch share of a weight-1 key
})
```

The scheduler dispatches in proportion to weight across the distinct keys at the active band — a weight-4
key gets ~4x the share of a weight-1 key, independent of how much backlog each has. Within a single key,
dispatch is oldest-first (FIFO). Both `FairnessKey` and `FairnessWeight` are immutable for the flow's life
(changing them mid-run would be a self-promotion vector).

## Backpressure (the valve)

When a task's downstream is rate-limiting, you don't want the engine to keep hammering it. Signal
backpressure by wrapping the error:

```go
if isRateLimited(err) { // e.g. HTTP 429
    return workflow.ErrBackpressure(err, "")
}
```

The engine then:

- **bounces** the step back to `pending` (it is *not* failed — the task never "saw" the rejection), and
- **cuts** the task's adaptive dispatch-rate ceiling.

Each task has its own rate controller that *discovers* the downstream's safe rate from observed
backpressure, using TCP CUBIC recovery: each signal cuts the rate by a small fixed amount; between signals
the rate probes back up toward the last known-good level and then gently beyond it, so it tracks a
downstream that autoscales. You configure nothing — there's no per-task rate to set. A host that wants to
self-limit simply emits backpressure above its own threshold.

This is per-replica: each replica converges its own rate from its own feedback, and the cluster total is
the sum.

## Circuit breakers

Backpressure is for "slow down." A breaker is for "this downstream can't serve right now at all" —
unreachable, in maintenance, or collapsed. Signal it by wrapping the error, with a cause label:

```go
switch {
case isUnreachable(err):  return workflow.ErrBreakerTrip(err, "ack_timeout")
case isUnavailable(err):  return workflow.ErrBreakerTrip(err, "unavailable") // HTTP 503
case isOverloaded(err):   return workflow.ErrBreakerTrip(err, "overloaded")  // HTTP 529
}
```

On the first such signal the task's breaker **trips**: every pending step for that task is parked out of
the scheduler, and exactly one probe step is allowed through on an exponential schedule (100ms, 200ms,
400ms, … capped at 1 minute). When a probe succeeds the breaker **closes** and the backlog resumes. This
lets a downed downstream actually recover instead of being re-flooded.

- The breaker is **per task name**, so one schedule governs all flows blocked on that task — no avalanche.
- New steps created while tripped are born parked, so they join the backlog without burning a probe.
- The breaker survives restarts: parked rows in the database re-arm the breaker on startup.
- Query the live state with `eng.BreakerTripped(taskName)`.

The `cause` string is opaque to the engine — it's forwarded only as a metric label
(`dwarf_task_breaker_trips_total{cause=…}`). You choose the vocabulary.

### Why two mechanisms

The valve drives the rate toward a sustainable level for a downstream that's *up but busy*. The breaker
backs off entirely for a downstream that's *down*. Applying rate-cutting to a downed service would drive
the rate to zero and then re-flood on recovery; applying a breaker to a merely-busy service would stall
throughput unnecessarily. They operate independently — a task can have both engaged at once.

## What the engine does *not* classify

The engine never inspects status codes or error text. A plain returned error is an ordinary failure
(`onError` route, or fail the flow). `ErrBackpressure` and `ErrBreakerTrip` are the *only* way to engage
the valve and breaker, and your host owns the mapping from its transport's signals to them. This keeps the
engine transport-agnostic. See [Writing tasks](tasks.md#signaling-backpressure-and-breakers).

## Time budgets

Each step's `ExecuteTask` call runs under a context deadline set by `WithTimeBudget` (default 2 minutes),
applied uniformly to every step. Your executor must honor `ctx` cancellation. For a tighter per-task bound,
enforce it inside your executor (shorten the call context); the engine's budget is the outer ceiling. There
is no engine-imposed *flow* deadline — implement one in author space with a `CreatedAt()` guard (see
[Writing tasks → Timestamps](tasks.md#timestamps)).

## Workers

`WithWorkers` (default 64) caps per-replica concurrency. It's a generous static ceiling, not the
backpressure mechanism — a worker blocked on an `ExecuteTask` call is just a goroutine and a socket, so
over-provisioning is cheap. The adaptive valve, not the pool size, is what throttles a pressured downstream.

Next: [Observability](observability.md).
