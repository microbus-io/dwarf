# Observability

> **For developers and operators.** Developers inject the three providers; operators read what comes out.
> This is the full instrument catalogue and how to interpret each one. For which of them to alert on, see
> [Operating dwarf](operations.md); for what to do when one fires, the [Runbook](runbook.md).

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

For development and test runs, `SetDebugLogger()` is a one-liner that wires a human-readable text logger to
**stderr** at debug level — so the engine's (and sequel's) internal logging is visible without standing up an
OTEL pipeline. It routes through `SetLogger`, so it counts as an explicitly-set logger:

```go
eng.SetDebugLogger()   // shorthand; stderr, text, debug level
```

## Metrics

Inject an OpenTelemetry `metric.MeterProvider`:

```go
eng.SetMeterProvider(otel.GetMeterProvider())
```

It defaults to the global `otel.GetMeterProvider()` — the no-op provider unless your process configures the
OpenTelemetry SDK. The engine builds its instruments under the scope `github.com/microbus-io/dwarf`;
service identity comes from the provider's Resource, not from per-metric attributes.

The engine emits 37 instruments — 17 counters, 15 gauges, and 5 histograms. Counter instrument names carry **no** `_total`
suffix; a Prometheus exporter appends it at the scrape boundary, so the names below are what you query in
PromQL **with** `_total` (e.g. the `dwarf_flows_started` instrument is queried as `dwarf_flows_started_total`):

| `dwarf_*` instrument | Type | Labels | Measures | PromQL name |
|---|---|---|---|---|
| `dwarf_flows_started` | counter | `workflow`, `shard`, `priority` | flows started. `shard` is where the flow was **placed**, so a skew across shards means work is not spread the way the capacity weights intended | `dwarf_flows_started_total` |
| `dwarf_flows_ended` | counter | `workflow`, `status`, `shard`, `priority` | flows reaching a terminal status — completed, failed, terminated or cancelled. All four are counted, so `started − ended` does not drift up by every flow that did not finish cleanly; see [Flows in flight](#flows-in-flight) | `dwarf_flows_ended_total` |
| `dwarf_steps_executed` | counter | `task_name`, `status`, `shard` | steps executed, by disposition. `shard` makes this per-shard dispatch throughput — selection, the connection pool and the work split are all per shard, so a fleet-wide total hides one shard falling behind | `dwarf_steps_executed_total` |
| `dwarf_steps_recovered` | counter | — | steps recovered because their worker was lost: a lease that expired (the task re-runs), or a completed step whose successor was never created (the step is resumed from its recorded output; the task does not re-run) | `dwarf_steps_recovered_total` |
| `dwarf_steps_unwedged` | counter | `park_type` | wedged subgraph parks recovered by the sweep (nonzero = latent bug) | `dwarf_steps_unwedged_total` |
| `dwarf_flows_orphaned` | counter | `workflow` | running flows the orphan sweep found stranded and could not repair — all steps terminal, no successor, no step it can safely resume (nonzero = latent bug; needs manual recovery) | `dwarf_flows_orphaned_total` |
| `dwarf_steps_write_retried` | counter | `shard` | in-place retries of a step's persistence write after a database blip (the task is not re-executed) | `dwarf_steps_write_retried_total` |
| `dwarf_steps_write_failed` | counter | `task_name` | steps terminalized because their outcome could not be stored while the database was reachable (nonzero = latent bug) | `dwarf_steps_write_failed_total` |
| `dwarf_state_write_bytes` | counter | `workflow`, `column` | payload bytes written to step rows on the execution path | `dwarf_state_write_bytes_total` |
| `dwarf_state_read_bytes` | counter | `workflow`, `column` | payload bytes read from step rows on the execution path | `dwarf_state_read_bytes_total` |
| `dwarf_refill_candidates_selected` | counter | `shard` | step candidates selected into the local worker cache | `dwarf_refill_candidates_selected_total` |
| `dwarf_refill_candidates_discarded` | counter | `shard` | cached candidates replaced un-popped (cost, not loss — the steps stay pending and are re-selected) | `dwarf_refill_candidates_discarded_total` |
| `dwarf_steps_offered` | counter | — | steps admitted to the local cache by the doorbell rather than found by a scan | `dwarf_steps_offered_total` |
| `dwarf_steps_claim_preempted` | counter | — | candidates skipped before any claim because a sibling worker on this replica already had one in flight — round trips SAVED | `dwarf_steps_claim_preempted_total` |
| `dwarf_steps_claim_lost` | counter | — | candidates claimed that lost the CAS to a peer — round trips WASTED. Read against `_preempted`: a healthy engine converts what would have been lost into preempted | `dwarf_steps_claim_lost_total` |
| `dwarf_steps_stolen` | counter | `shard` | steps selected from outside this replica's own share of the work because the replica that owns that share was not serving it. Zero in a healthy fleet by construction; a sustained rate names a peer that is alive but not dispatching | `dwarf_steps_stolen_total` |
| `dwarf_peer_changes` | counter | `shard` | times this replica observed that shard's replica count change. Zero in a settled fleet by construction, so nonzero during a steady-state window means the fleet churned | `dwarf_peer_changes_total` |
| `dwarf_refill_query_duration_seconds` | histogram | `shard`, `phase` | one shard's candidate-selection query | `dwarf_refill_query_duration_seconds` |
| `dwarf_steps_queue_depth` | gauge | — | steps in the local worker cache | `dwarf_steps_queue_depth` |
| `dwarf_steps_pending` | gauge | `shard`, `priority` | due pending steps in each priority band of the shard, refreshed every 30 seconds | `dwarf_steps_pending` |
| `dwarf_steps_oldest_pending_age_seconds` | gauge | `shard`, `priority` | how long the longest-due pending step in each priority band of the shard has been due, refreshed every 30 seconds | `dwarf_steps_oldest_pending_age_seconds` |
| `dwarf_steps_dispatch_lag_seconds` | gauge | `shard` | how long the most overdue step in the priority bands being served has been due on that shard, current to within a selection cycle. Under a second while dispatch keeps up | `dwarf_steps_dispatch_lag_seconds` |
| `dwarf_peer_replicas` | gauge | `shard` | replicas this one currently sees holding connections to that shard, from which the fleet picks that shard's dispatchers | `dwarf_peer_replicas` |
| `dwarf_shard_dispatching` | gauge | `shard` | 1 when this replica dispatches that shard (holds a share of its connection budget and runs its steps), 0 when it does not dispatch there (it holds a two-connection reader pool, or a share of the budget only until steps it already started there finish). Sum across the fleet for how many replicas dispatch each shard; a shard summing to zero runs nothing | `dwarf_shard_dispatching` |
| `dwarf_peer_blind_seconds` | gauge | `shard` | time since that shard's peer registry was last read successfully; zero when healthy | `dwarf_peer_blind_seconds` |
| `dwarf_refill_tally_age_seconds` | gauge | — | how long ago the stalest shard still counted in this replica's selection last reported | `dwarf_refill_tally_age_seconds` |
| `dwarf_turnstile_gate_wait_seconds` | histogram | `shard` | time a worker waited for its FIRST turn, taken before it picks a step up. It is the queue in front of dispatch, and the only wait a worker holds no work through | `dwarf_turnstile_gate_wait_seconds` |
| `dwarf_turnstile_wait_seconds` | histogram | `shard` | time a worker waited for the turn that RECORDS a finished step. A finished step is ranked by how long it has been at the database, the task itself not counted, so it is served ahead of work that arrived after its own began — a sustained wait here means the shard's connections are saturated by work at least as old, not that completions are queued behind dispatch | `dwarf_turnstile_wait_seconds` |
| `dwarf_turnstile_available` | gauge | `shard` | turns free on that shard. Turns are a fixed multiple of the connection pool, so this is how much admission is unspoken for; sustained zero means the shard's connections are the binding constraint | `dwarf_turnstile_available` |
| `dwarf_turnstile_waiting` | gauge | `shard` | callers queued for a turn on that shard. Unlike the free count this has no ceiling, so it is the one that shows how DEEP a queue has become rather than merely that it is full; read the pair together | `dwarf_turnstile_waiting` |
| `dwarf_db_phase_seconds` | histogram | `role` | wall clock a worker spent INSIDE a database phase — `enter` is dispatch up to the task call, `exit` is the task call to the end of the step. It is the phase's true residence, which neither the turn waits (the queue in FRONT of each call) nor the sum of query times (which misses connection wait and the Go-side work between them) can supply | `dwarf_db_phase_seconds` |
| `dwarf_db_phase_workers` | gauge | `role` | worker goroutines inside a database phase right now. This is what sets connection-pool pressure, and nothing else reports it: `dwarf_workers_resident` counts every worker that exists, and a worker parked in a task call still holds its step | `dwarf_db_phase_workers` |
| `dwarf_operation_seconds` | histogram | `op`, `shard` | time an API operation spent on a shard: the wait for its turn plus its database work. `op` is one of `create`, `continue`, `fork`, `resume`, `terminate`, `cancel`, `delete`, `snapshot`, `fingerprint`, `history`, `step`, `list_live`, `list_archived`, `purge`, `purge_before`, `shard_info`. Work outside the shard is not counted (`Create`'s graph load, the wait in `Await`, `Poll` and `Run`, whose reads of the flow record as `snapshot`). `ListLive`, `ListArchived`, `Purge`, `PurgeBefore` and `ShardInfo` record one sample per shard they touch. Compare one `op` across shards that are up to find a slow one; a shard that refuses connections fails fast and reads as fast | `dwarf_operation_seconds` |
| `dwarf_workers_resident` | gauge | — | worker goroutines that exist. It grows on demand under load and falls again as workers that have been mostly idle retire, never below the connection-derived floor. Rising and settling back is that working; wide oscillation at steady load is not | `dwarf_workers_resident` |
| `dwarf_state_in_flight_bytes` | gauge | — | state payload this replica is holding across host calls right now, summed over the tasks currently inside a task call | `dwarf_state_in_flight_bytes` |
| `dwarf_state_in_flight_steps` | gauge | — | tasks currently inside a task call — the denominator for the line above | `dwarf_state_in_flight_steps` |
| `dwarf_state_cache_bytes` | gauge | — | memory the state cache is charged for right now. Each field counts as its size plus a few hundred bytes. The cache is bounded by `SetStateCacheSize` and by 1,048,576 fields, so it is full when this is pinned at that size, or below it once 1,048,576 small fields are held | `dwarf_state_cache_bytes` |

The counters increment inline at their event sites; the gauges are observable (async) and read engine state
at collection time. No gauge queries the database when it is collected, so a short scrape interval costs
the database nothing.

**Is dispatch keeping up?** `dwarf_steps_dispatch_lag_seconds` is the live answer: how long the most overdue
step in the priority bands being served has been due, per shard. A step that slept, backed off, or waited on a
subgraph or an interrupt is measured from when it became runnable, not from when it was created. The lag covers
only the bands being served: a band that better-priority work leaves no capacity for is left out, and a shard
holding only such bands reads zero. When better-priority work leaves capacity over, the served bands reach
further, and if a band that has waited a long time comes into reach the lag jumps to its age. It falls back
when the better bands fill the capacity again. A jump of that kind is priority at work, not a stalled dispatcher:
read it beside the per-band ages. Strict priority holds those bands back by design, and that shows in `dwarf_steps_oldest_pending_age_seconds`,
one reading per band, refreshed every 30 seconds along with `dwarf_steps_pending`. While the lead for a shard
passes from one replica to another, those two may be missing for up to 30 seconds.

### Flows in flight

`dwarf_flows_started` and `dwarf_flows_ended` carry the same `shard` and `priority`, so the difference of their
increases over a window is how many flows each band gained or shed in flight:

```promql
sum by (priority) (increase(dwarf_flows_started_total[5m]))
  - sum by (priority) (increase(dwarf_flows_ended_total[5m]))
```

Read it as a trend, not as a level. Each replica's counters start from zero when the replica does, so a running
total of `started − ended` drifts across restarts. It also counts flows rather than steps: a flow waiting on a
sleep, an interrupt or a long task is in flight without being in any backlog.

**Read the two in-flight state gauges as a quotient, not separately.** `bytes / steps` is the mean state a
task carries, and that is what distinguishes the two ways a replica ends up holding a lot: a few tasks each
carrying a large document (a big mean) versus a wide fan-out of tasks each carrying a little (a small mean
against a large count). They call for different responses — the first for narrowing what the workflow
carries, the second for the fan-out width itself — and neither number alone tells them apart.

It measures the JSON a task's state was built from, not the in-memory form, which is several times larger.
That is deliberate: read against your runtime's heap it gives the expansion factor, and it stays comparable
across engine versions that represent state differently. It is also the only reading that covers a cost
nothing else reports — a task holds no database connection while it runs, so the worker pool grows freely
for long tasks, and held state times pool size is a memory ceiling `dwarf_workers_resident` cannot show on
its own.

**Aggregating the gauges across replicas — two kinds, and mixing them up inflates your dashboard by the
replica count:**

| Gauge | Kind | Aggregate with |
|---|---|---|
| `dwarf_steps_queue_depth` | **per-replica** (this replica's in-memory cache) | `sum` |
| `dwarf_refill_tally_age_seconds` | **per-replica** (this replica's selection) | `max` |
| `dwarf_turnstile_available` | **per-replica** (this replica's gate) | `sum` |
| `dwarf_turnstile_waiting` | **per-replica** (this replica's gate) | `sum` |
| `dwarf_db_phase_workers` | **per-replica** (this replica's workers) | `sum` |
| `dwarf_workers_resident` | **per-replica** (this replica's workers) | `sum` |
| `dwarf_state_in_flight_bytes` | **per-replica** (this replica's in-flight tasks) | `sum` |
| `dwarf_state_in_flight_steps` | **per-replica** (this replica's in-flight tasks) | `sum` |
| `dwarf_state_cache_bytes` | **per-replica** (this replica's state cache) | `sum` |
| `dwarf_steps_pending` | **per-shard** (reported by the shard's leading replica) | `max` per shard and priority, then `sum` across shards |
| `dwarf_steps_oldest_pending_age_seconds` | **per-shard** (reported by the shard's leading replica) | `max` |
| `dwarf_steps_dispatch_lag_seconds` | **per-shard** (reported by each replica dispatching the shard) | `max` |

The per-shard three describe a shard's database, and only the replicas responsible for a shard report them:
each replica dispatching the shard reports the lag, and one of them, the shard's leader, reports the per-band
pair. More than one replica can report the same shard: two dispatchers, or an outgoing and an incoming leader
for a moment while the lead passes. So never `sum` across replicas. Take the `max` per shard first:

```promql
sum by (priority) (max by (shard, priority) (dwarf_steps_pending))
```

`dwarf_db_phase_workers` carries a `role` label that aggregates across replicas but **not across roles**:
`enter` and `exit` are separate populations. For the pressure a phase actually reached rather than whatever
the scrape happened to catch, take `max_over_time` of it.

Labels are deliberately bounded: there are no per-`fairness_key` labels (that would be unbounded
cardinality), so fairness metrics are aggregate-only. `priority` and `workflow` are labels on the assumption
that a deployment uses a small, fixed set of each: every value is a separate series, multiplied by the
other labels. A deployment that mints priorities or workflow URLs per request should drop those labels with
a metric View in its OpenTelemetry pipeline.

**Reading the two refill histograms.** They answer "is candidate selection costing me throughput, and if so
where." Each shard's selection runs on its own cycle, against its own database, so:

- `dwarf_refill_query_duration_seconds` split **by `shard`** shows whether one database is slower than its
  peers. A single shard's tail dragging while the others stay flat is the shard, not the engine. It slows
  that shard's own dispatch; the other shards keep selecting on their own cycles, planning from the slow
  shard's last report (`dwarf_refill_tally_age_seconds` shows how old that report is).
- The same metric split **by `phase`** separates the two queries a pass runs. A tail that appears only on
  `band_keys` and only intermittently, on a backlog that has not changed, points at the database's query
  planner rather than at load (on PostgreSQL, stale table statistics can flip this query between an index scan
  and a sequential scan — measured on one rig at 0.3 ms versus 100 ms on identical data). `ANALYZE` on
  `dwarf_steps`, or more aggressive autovacuum settings for it, is the fix.
- Do **not** sum the phases. Selection runs as two independently paced loops — one scanning the priority
  band, one turning the resulting plan into cached candidates — so `band_keys` and the other three phases are
  measured on different clocks and their sum describes nothing. Read each phase on its own; the rate at which
  candidates reach the workers is set by the second loop, so a slow `band_keys` does not hold it back.

`dwarf_refill_candidates_discarded` over `dwarf_refill_candidates_selected` is the selection **waste ratio**.
Each pass replaces the cache wholesale, so anything the workers had not yet picked up is dropped and
re-selected later — always safe, never lost work, but a high ratio means selection is running far ahead of
what the workers can consume and most of its database round-trips are being thrown away. A ratio near 0
means the opposite: selection is the slower half.

A **sustained high ratio is a cadence question, not a fault**: selection paces itself from the shard's
connection budget, assuming each connection drains steps at a steady rate, so a workload whose steps drain
more slowly than that will be over-supplied. It costs database work rather than correctness — no step is
lost. A large budget on a database that is already saturated is the usual cause, since extra connections
there add little throughput.

## Tracing

Inject an OpenTelemetry `trace.TracerProvider`:

```go
eng.SetTracerProvider(otel.GetTracerProvider())
```

It defaults to the global `otel.GetTracerProvider()`. The host injects only the provider — there is no span
code to write and no trace context to thread by hand.

**Install the OpenTelemetry SDK before `Startup`.** The engine resolves its tracer once, at `Startup`. If no
SDK is installed by then, it traces through a no-op provider, so an engine running without tracing spends
nothing building spans. A provider installed after `Startup` is not picked up until the next `Startup`.

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
workflows shows up in the same pipeline: per-operation spans (nested under the active step span), migration
logs, and nine `sequel_*` instruments — all under the scope `github.com/microbus-io/sequel`. The logger is
forwarded only when you explicitly set one, so an unconfigured engine stays silent here too.

**These are not dwarf's instruments and are not part of the 37 above**, but you will see them on the same
scrape, and two of them answer questions no `dwarf_*` metric can:

| `sequel_*` instrument | Type | Labels | Measures |
|---|---|---|---|
| `sequel_pool_open_connections` | gauge | `database` | open connections (in use plus idle) |
| `sequel_pool_in_use_connections` | gauge | `database` | connections currently in use |
| `sequel_pool_idle_connections` | gauge | `database` | idle connections |
| `sequel_pool_waits` | counter | `database` | connections waited for |
| `sequel_pool_wait_duration_seconds` | counter | `database` | time blocked waiting for a connection |
| `sequel_query_duration` | histogram | `db.operation.name`, `db.system.name`, `status` | query duration |
| `sequel_transaction_duration` | histogram | `db.system.name`, `outcome` | `Transact` duration, **including retries** |
| `sequel_lock_contention` | counter | `db.system.name`, `db.operation.name` | operations that failed on lock contention or deadlock |
| `sequel_migration_runs` | counter | `db.migration.sequence`, `db.system.name`, `status` | schema migrations executed (already-completed ones excluded) |

**The pool pair is the counterpart to the turnstile metrics, and it is where the two views meet.**
`dwarf_turnstile_*` shows the queue dwarf *orders*; `sequel_pool_wait_duration_seconds` shows the wait that
remains at the pool itself, which the turnstile deliberately does not remove — a turn admits a caller to
compete for a connection, it does not reserve one. Pool wait at saturation is the expected steady state, and
this is the number that says how much of it there is. It also silently rate-limits candidate selection, so
it is worth watching whenever throughput moves for no visible reason.

**The two wait instruments are cumulative counters** - they come straight from Go's `sql.DBStats`, which only
ever increases, so they are observable counters rather than gauges and a Prometheus exporter appends `_total`
to both. Take `rate()` or `increase()` over them; a raw value is a since-process-start total, and averaging it
across replicas is meaningless.

**The `database` label is the DSN's database name, not the shard index** — it falls back to the driver name
when the DSN has none. In the recommended shard-per-server topology every shard often holds a database of
the *same* name, in which case all of them report under one label value and their series merge. If you shard
and want these split per shard, give each server's database a distinct name; `dwarf_*` metrics label by
`shard` and are unaffected either way.

## Configuration timing

All three observability knobs — `SetLogger`, `SetMeterProvider`, `SetTracerProvider` — are
**construction-time**: set them before `Startup`. The engine resolves the providers and wires them into the
worker hot path and the shard DBs once at startup, so a call after `Startup` is rejected with a 409 (it
keeps the hot-path reads lock-free).

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
