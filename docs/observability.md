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

The engine emits 33 instruments — 17 counters, 12 gauges, and 4 histograms. Counter instrument names carry **no** `_total`
suffix; a Prometheus exporter appends it at the scrape boundary, so the names below are what you query in
PromQL **with** `_total` (e.g. the `dwarf_flows_started` instrument is queried as `dwarf_flows_started_total`):

| `dwarf_*` instrument | Type | Labels | Measures | PromQL name |
|---|---|---|---|---|
| `dwarf_flows_started` | counter | `workflow`, `shard` | flows started. `shard` is where the flow was **placed**, so a skew across shards means work is not spread the way the capacity weights intended | `dwarf_flows_started_total` |
| `dwarf_flows_terminated` | counter | `workflow`, `status`, `shard` | flows reaching a terminal status — **all three** of completed, failed and cancelled, which is what makes `started − terminated` a stable in-flight count rather than one that drifts up by every flow that did not finish cleanly | `dwarf_flows_terminated_total` |
| `dwarf_steps_executed` | counter | `task_name`, `status`, `shard` | steps executed, by disposition. `shard` makes this per-shard dispatch throughput — selection, the connection pool and the work split are all per shard, so a fleet-wide total hides one shard falling behind | `dwarf_steps_executed_total` |
| `dwarf_steps_recovered` | counter | — | steps recovered after a lease expiry | `dwarf_steps_recovered_total` |
| `dwarf_steps_unwedged` | counter | `park_type` | wedged subgraph parks recovered by the sweep (nonzero = latent bug) | `dwarf_steps_unwedged_total` |
| `dwarf_flows_orphaned` | counter | `workflow` | running flows detected stranded by the orphan sweep — all steps terminal, no successor (nonzero = latent bug; detection-only, not recovered) | `dwarf_flows_orphaned_total` |
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
| `dwarf_steps_pending` | gauge | `priority` | due pending steps per priority band | `dwarf_steps_pending` |
| `dwarf_steps_oldest_pending_age_seconds` | gauge | `priority` | age of the oldest due pending step | `dwarf_steps_oldest_pending_age_seconds` |
| `dwarf_peer_replicas` | gauge | `shard` | replicas this one currently sees holding connections to that shard — the divisor its pool is sized by | `dwarf_peer_replicas` |
| `dwarf_peer_blind_seconds` | gauge | `shard` | time since that shard's peer registry was last read successfully; zero when healthy | `dwarf_peer_blind_seconds` |
| `dwarf_refill_tally_age_seconds` | gauge | — | how long ago the stalest shard still counted in this replica's selection last reported | `dwarf_refill_tally_age_seconds` |
| `dwarf_turnstile_gate_wait_seconds` | histogram | `shard` | time a worker waited for its FIRST turn, taken before it picks a step up. It is the queue in front of dispatch, and the only wait a worker holds no work through | `dwarf_turnstile_gate_wait_seconds` |
| `dwarf_turnstile_wait_seconds` | histogram | `shard` | time a worker waited for the turn that RECORDS a finished step. Work that has already run presents an older claim than anything admitted while it ran, so it is served first — a sustained wait here means the shard's connections are busy with work at least as old, not that completions are queued behind dispatch | `dwarf_turnstile_wait_seconds` |
| `dwarf_turnstile_available` | gauge | `shard` | turns free on that shard. Turns are a fixed multiple of the connection pool, so this is how much admission is unspoken for; sustained zero means the shard's connections are the binding constraint | `dwarf_turnstile_available` |
| `dwarf_turnstile_waiting` | gauge | `shard` | callers queued for a turn on that shard. Unlike the free count this has no ceiling, so it is the one that shows how DEEP a queue has become rather than merely that it is full; read the pair together | `dwarf_turnstile_waiting` |
| `dwarf_db_phase_seconds` | histogram | `role` | wall clock a worker spent INSIDE a database phase — `enter` is dispatch up to the task call, `exit` is the task call to the end of the step. It is the phase's true residence, which neither the turn waits (the queue in FRONT of each call) nor the sum of query times (which misses connection wait and the Go-side work between them) can supply | `dwarf_db_phase_seconds` |
| `dwarf_db_phase_workers` | gauge | `role` | worker goroutines inside a database phase right now. This is what sets connection-pool pressure, and nothing else reports it: `dwarf_workers_resident` counts every worker that exists, and a worker parked in a task call still holds its step | `dwarf_db_phase_workers` |
| `dwarf_workers_resident` | gauge | — | worker goroutines that exist; grows on demand, never shrinks | `dwarf_workers_resident` |
| `dwarf_state_in_flight_bytes` | gauge | — | state payload this replica is holding across host calls right now, summed over the tasks currently inside a task call | `dwarf_state_in_flight_bytes` |
| `dwarf_state_in_flight_steps` | gauge | — | tasks currently inside a task call — the denominator for the line above | `dwarf_state_in_flight_steps` |

The counters increment inline at their event sites; the gauges are observable (async) and read engine state
at collection time.

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
| `dwarf_steps_pending` | **cluster-wide** (queries the shared database) | `max` |
| `dwarf_steps_oldest_pending_age_seconds` | **cluster-wide** (queries the shared database) | `max` |

The cluster-wide two are computed by querying the shard databases, which every replica shares — so each
replica reports the *same* number, and summing over three replicas shows a 1,000-step backlog as 3,000. A
summed `oldest_pending_age` is meaningless outright. Use `max` (or `avg`); a per-replica reading of these
is not obtainable from a shared database, and the engine does not pretend otherwise. They are also the
*only* instruments that cost a query at collection time, which is why the cluster-wide set is kept to what
genuinely cannot be answered another way.

`dwarf_db_phase_workers` carries a `role` label that aggregates across replicas but **not across roles**:
`enter` and `exit` are separate populations. For the pressure a phase actually reached rather than whatever
the scrape happened to catch, take `max_over_time` of it — there is deliberately no peak instrument, since a
watermark held since process start goes constant within hours while a windowed max does not.

Labels are deliberately bounded: there are no per-`fairness_key` labels (that would be unbounded
cardinality), so fairness/priority metrics are aggregate-only.

**Reading the two refill histograms.** They answer "is candidate selection costing me throughput, and if so
where." Selection queries every shard concurrently and proceeds only when the last one answers, so:

- `dwarf_refill_query_duration_seconds` split **by `shard`** shows whether one database is slower than its
  peers. A single shard's tail dragging while the others stay flat is the shard, not the engine — and because
  the whole pass waits for it, that one shard bounds dispatch for the entire replica.
- The same metric split **by `phase`** separates the two queries a pass runs. A tail that appears only on
  `band_keys` and only intermittently, on a backlog that has not changed, points at the database's query
  planner rather than at load (on PostgreSQL, stale table statistics can flip this query between an index scan
  and a sequential scan — measured on one rig at 0.3 ms versus 100 ms on identical data). `ANALYZE` on
  `dwarf_steps`, or more aggressive autovacuum settings for it, is the fix.
- Summed across its four `phase` values, the same metric reconstructs a whole selection cycle; there is no
  separate end-to-end histogram, because the phase split says both that a cycle happened and which part of
  it was slow.

`dwarf_refill_candidates_discarded` over `dwarf_refill_candidates_selected` is the selection **waste ratio**.
Each pass replaces the cache wholesale, so anything the workers had not yet picked up is dropped and
re-selected later — always safe, never lost work, but a ratio approaching 1 means selection is running far
ahead of what the workers can consume and most of its database round-trips are being thrown away. A ratio
near 0 means the opposite: selection is the slower half.

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
workflows shows up in the same pipeline: per-operation spans (nested under the active step span), migration
logs, and nine `sequel_*` instruments — all under the scope `github.com/microbus-io/sequel`. The logger is
forwarded only when you explicitly set one, so an unconfigured engine stays silent here too.

**These are not dwarf's instruments and are not part of the 33 above**, but you will see them on the same
scrape, and two of them answer questions no `dwarf_*` metric can:

| `sequel_*` instrument | Type | Labels | Measures |
|---|---|---|---|
| `sequel_pool_open_connections` | gauge | `database` | open connections (in use plus idle) |
| `sequel_pool_in_use_connections` | gauge | `database` | connections currently in use |
| `sequel_pool_idle_connections` | gauge | `database` | idle connections |
| `sequel_pool_wait_count` | gauge | `database` | **cumulative** count of connections waited for |
| `sequel_pool_wait_duration_seconds` | gauge | `database` | **cumulative** time blocked waiting for a connection |
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

**`wait_count` and `wait_duration_seconds` are cumulative totals published as gauges** (they come straight
from Go's `sql.DBStats`, which only ever increases). Take `rate()` or `increase()` over them; a raw value is
a since-process-start total, and averaging it across replicas is meaningless.

**The `database` label is the DSN's database name, not the shard index** — it falls back to the driver name
when the DSN has none. In the recommended shard-per-server topology every shard often holds a database of
the *same* name, in which case all of them report under one label value and their series merge. If you shard
and want these split per shard, give each server's database a distinct name; `dwarf_*` metrics label by
`shard` and are unaffected either way.

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
