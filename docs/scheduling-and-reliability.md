# Scheduling & reliability

Dwarf decides *which* pending step runs next (scheduling) and recovers steps that a crashed worker left
behind. Most of this is automatic; this guide explains the knobs you control and how errors are handled.

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

## Error handling

The engine never inspects status codes or error text. Any error a task returns is **terminal for that
attempt**: the engine routes it via the graph's `onError` transition if one is declared, otherwise it fails
the step. This keeps the engine transport-agnostic — there is no rate-limit or unavailability signal to
wrap an error with.

Backpressure belongs to the layer that owns the downstream's resource identity, not the engine. A task that
wants to back off on a transient failure (a `429`, a momentarily unavailable downstream) detects that
condition itself and arms `flow.Retry`, whose bound is wall-clock rather than a count:

```go
err := callDownstream(ctx)
switch {
case isRateLimited(err), isUnavailable(err): // e.g. HTTP 429 / 503
    if f.Retry(100*time.Millisecond, 2.0, 10*time.Second, time.Hour) {
        return nil // re-run after backoff
    }
    return err // horizon exceeded
default:
    return err // ordinary failure
}
```

See [Writing tasks → Handling transient failures](tasks.md#handling-transient-failures).

## Time budgets

Each step's `ExecuteTask` call runs under a context deadline set by `WithTimeBudget` (default 2 minutes),
applied uniformly to every step. Your executor must honor `ctx` cancellation. For a tighter per-task bound,
enforce it inside your executor (shorten the call context); the engine's budget is the outer ceiling. There
is no engine-imposed *flow* deadline — implement one in author space with a `CreatedAt()` guard (see
[Writing tasks → Timestamps](tasks.md#timestamps)).

## Workers

`WithWorkers` (default 64) caps per-replica concurrency. It's a generous static ceiling — a worker blocked
on an `ExecuteTask` call is just a goroutine and a socket, so over-provisioning is cheap.

Next: [Observability](observability.md).
