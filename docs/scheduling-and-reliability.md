# Scheduling & reliability

> **For developers and operators.** Developers set priority and fairness per flow and decide how tasks handle
> failure; operators need the same model to read a backlog. The behaviour described here is what the
> [Runbook](runbook.md) assumes you know.

Dwarf decides *which* pending step runs next (scheduling) and recovers steps that a crashed worker left
behind. Most of this is automatic; this guide explains the knobs you control and how errors are handled.

## Priority

Priority arbitrates *between* flows competing for workers. It's a property of the flow, set once at create
and immutable for its life:

```go
eng.Create(ctx, "report", state, &workflow.FlowOptions{Priority: 5})
```

Priorities run from 1 to the engine's maximum - 5 by default, configurable up to 100 with `SetMaxPriority` -
and lower numbers run first. An unset priority (0) takes the midpoint of the range, 3 by default. `Create` and `Run` reject a priority outside the range. Use the same
maximum on every replica sharing a database. Work that carries a priority beyond a replica's range - a flow
created before the range existed, or by a replica configured with a larger one - still runs there, after all of
its own levels. Priority is **strict over capacity**: lower-priority work gets only the capacity higher-priority
work leaves unused. Once the engine observes due priority-1 work, priority-4 work is selected only for the
workers priority-1 work cannot keep busy, so a light stream of urgent flows never idles the engine while a
backlog waits below it.

The guarantee is over *observed* work, and observation is not instantaneous — the scheduler works from
periodic snapshots of what is due. When priority-1 work arrives, it takes a snapshot cycle or two before
every part of a sharded cluster has observed it, and a small amount of priority-4 work may start in that
window. With several replicas, each one serves its own share of the due work first, so priority-4 work
can start on one replica while priority-1 work waits a few cycles for another, after which any replica takes
it ahead of its own lower-priority work. These effects are bounded by the snapshot cycle, not by the size of
the backlog: strict ordering is what you get between arrivals separated by more than a cycle or two, which is
the normal case.

Priority is never preemptive: anything already running continues to completion. The worker pool grows on demand,
so newly arrived higher-priority work normally starts as soon as it is observed; only when every worker the
engine may run is busy with lower-priority tasks does it wait for one of them to finish. Where that can happen
and urgent latency matters, keep lower-priority tasks short - split a long job into steps - so a worker frees up
often.
There is no aging — a stream of high-priority flows that keeps every worker busy will starve lower bands by
design, so reserve the top bands for genuinely urgent work. To see it happening, chart `dwarf_steps_oldest_pending_age_seconds`
by `priority`: a lower band whose age climbs while `dwarf_steps_dispatch_lag_seconds` stays low is being held
back by better-priority work, not by a slow engine ([Operations](operations.md#alerting-on-lag)).

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

The scheduler dispatches in proportion to weight across the distinct keys within each priority band — a weight-4
key gets ~4x the share of a weight-1 key, independent of how much backlog each has. If flows sharing a key carry
different weights, the key dispatches at the lowest weight among its due work, so give one key one weight. Within a single key,
dispatch is oldest-first (FIFO). Both `FairnessKey` and `FairnessWeight` are immutable for the flow's life
(changing them mid-run would be a self-promotion vector).

## Error handling

The engine never inspects status codes or error text. Any error a task returns is **terminal for that
attempt**: the engine routes it via the graph's `onError` transition if one is declared, otherwise it fails
the step. This keeps the engine transport-agnostic — there is no rate-limit or unavailability signal to
wrap an error with.

Backpressure belongs to the layer that owns the downstream's resource identity, not the engine. A task that
wants to back off on a transient failure (a `429`, a momentarily unavailable downstream) detects that
condition itself and arms `flow.Retry`, whose bound is the total backoff rather than a count:

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

Each step's `ExecuteTask` call runs under a context deadline: the flow's `FlowOptions.TimeBudget` if it set
one, otherwise the engine default from `SetTimeBudget` (2 minutes unless changed). The budget is fixed when
the flow is created and applies to every step of it, its subgraphs included. It is at most just under 24.8 days
(a 32-bit count of milliseconds); a step whose replica crashes is not run again until its budget has passed, so a task that waits for
days should `Sleep` or `Interrupt` rather than hold one long call. Your executor must honor `ctx` cancellation. For a tighter per-task bound,
enforce it inside your executor (shorten the call context); the engine's budget is the outer ceiling. A
*flow* deadline is the workflow's own: implement one in author space with a `CreatedAt()` guard (see
[Writing tasks → Timestamps](tasks.md#timestamps)). `SetFlowExpiration` is an operator's backstop against flows that
never end, and terminates them without running any task ([Expiring flows that never
end](flows.md#expiring-flows-that-never-end)).

## Workers

Per-replica concurrency needs no configuration: the engine sizes a resident worker set from each shard's
connection budget and grows it on demand, up to a ceiling derived from the crash-recovery lease margin. A
worker blocked in an `ExecuteTask` call holds no database connection, so a workload of long tasks grows to
fit its own concurrency. `SetWorkers(n)` is an expert override — mainly for bounding memory, since each
in-flight step carries its state map — and pins the maximum rather than the resident set. See
[Deployment → Workers](deployment.md#workers).

## Crash recovery

A step a crashed worker was running is recovered on its own: it is run again once its lease lapses, within the
flow's time budget plus about a minute and a half, and a step whose outcome was recorded but whose next step was
not is picked up from that record without running its task again. Execution is therefore at least once, so make
tasks idempotent. The timings, and what an operator sees, are in
[Deployment → Crash recovery](deployment.md#crash-recovery).

Next: [Observability](observability.md).
