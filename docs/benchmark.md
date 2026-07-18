# Benchmarks

`fixtures/benchmark_test.go` measures end-to-end flow throughput and latency through the **public API**
(`Run` = create + await) against whatever SQL dialect `SEQUEL_TESTING_DSN` points at. It measures what code
review cannot tell you: how many flows and steps per second each dialect sustains at each shard count, and the
dispatch-latency percentiles.

> **These numbers are reference points for relative comparison, not a capacity guarantee.** They were taken on a
> single developer laptop with every database in a local Docker container on the same host — a *co-located*
> setup that is a dev/test shape, not production. Read the ratios and trends (which dialect is faster, how
> latency changes with load, what sharding does), not the absolute numbers, which reflect one laptop on a single
> run.

## Running

Pick one dialect per invocation (the dialect is the DSN, not a Go flag). `-run=X` selects no unit tests so
only benchmarks run; `-benchtime=1000x` fixes the flow count so each shard count does one measured pass.

```sh
# SQLite in-memory (no env)
go test ./fixtures/ -run=X -bench=BenchmarkFlowThroughput -benchtime=1000x

# PostgreSQL
SEQUEL_TESTING_DSN='postgres://USER:PASS@127.0.0.1:5432/dwarfbench_%d?sslmode=disable' \
  go test ./fixtures/ -run=X -bench=BenchmarkFlowThroughput -benchtime=1000x

# MySQL / MariaDB
SEQUEL_TESTING_DSN='USER:PASS@tcp(127.0.0.1:3306)/dwarfbench_%d' \
  go test ./fixtures/ -run=X -bench=BenchmarkFlowThroughput -benchtime=1000x

# SQL Server  (enable READ_COMMITTED_SNAPSHOT on the model database first to avoid deadlocks;
#              add &encrypt=disable against a container using a self-signed certificate)
SEQUEL_TESTING_DSN='sqlserver://sa:PASS@127.0.0.1:1433?database=dwarfbench_%d&encrypt=disable' \
  go test ./fixtures/ -run=X -bench=BenchmarkFlowThroughput -benchtime=1000x
```

> **SQL Server TLS.** The stock SQL Server container ships a self-signed certificate whose serial number
> is *negative*, which Go's `crypto/x509` refuses to parse — every connection then dies with
> `TLS Handshake failed: x509: negative serial number`, long before any query runs. `&encrypt=disable`
> (or a real certificate) is required against such a server. This is a driver/TLS constraint, not a dwarf
> one.

Swap `-bench=BenchmarkFlowThroughput` for `-bench=BenchmarkStatePayload` (use a smaller `-benchtime`, e.g.
`100x`, since large payloads are slow) to measure byte throughput across payload sizes — see the section below.

The `%d` is required — the harness creates an isolated, auto-dropped database per shard (and drops it on
shutdown), so no manual provisioning is needed; the connecting user only needs `CREATE DATABASE` rights.

The custom metrics are what to look at: `flows/s`, `steps/s`, and `p50_ms` / `p95_ms` / `p99_ms` end-to-end
latency. Go's built-in `ns/op` is elapsed time per flow averaged across the concurrent submitters, which is
less useful here.

## Workload and configuration

| Knob | Value | Why |
|---|---|---|
| Graph | linear `T0 → T1 → T2 → END` | 3 trivial (no-op) tasks, so **3 steps/flow** and each step is essentially its own DB transaction |
| Client concurrency | 32 goroutines | offered load; matched to the worker pool so the pool stays saturated |
| Worker pool | 32 | fixed across dialects/shard counts |
| Per-shard connection pool | 30, pinned via the `SetMaxOpenConns` override | the test default (8) starves 32 workers and would measure the pool, not the engine; 30 keeps 3 co-located shards under PostgreSQL's default `max_connections=100` (≈90) |
| Flows per pass | 1000 (`-benchtime=1000x`) | one measured pass per shard count |

Because the tasks are no-ops, essentially all of each step's time is its **durable DB transaction** (claim CAS
+ transition commit). So these numbers isolate the engine's own per-step cost — the database work and commit
latency, with almost nothing else. A real workload does actual work in each task, so its task bodies, not this
overhead, would dominate the time; these numbers are the engine's floor.

## Results

Hardware: Apple M1 Pro (arm64, macOS); PostgreSQL, MariaDB 10, and SQL Server 2019 each in a local Docker
container on the same host. One representative run; expect ±10–15% run-to-run variance.

### SQLite (in-memory)

| Shards | flows/s | steps/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| 1 | 417 | 1252 | 72.7 | 111.8 | 130.8 |
| 2 | 639 | 1918 | 47.5 | 86.7 | 106.0 |
| 3 | 682 | 2045 | 41.9 | 86.9 | 114.5 |

### PostgreSQL

| Shards | flows/s | steps/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| 1 | 364 | 1093 | 80.9 | 160.8 | 195.3 |
| 2 | 333 | 1000 | 91.8 | 129.6 | 159.2 |
| 3 | 290 | 869 | 106.9 | 147.8 | 170.2 |

### MySQL / MariaDB

| Shards | flows/s | steps/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| 1 | 248 | 743 | 126.4 | 153.0 | 165.9 |
| 2 | 200 | 600 | 135.5 | 314.0 | 585.5 |
| 3 | 201 | 603 | 145.3 | 246.8 | 383.9 |

### SQL Server

| Shards | flows/s | steps/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| 1 | 152 | 455 | 195.5 | 346.0 | 435.7 |
| 2 | 137 | 411 | 211.5 | 380.5 | 506.5 |
| 3 | 135 | 405 | 222.2 | 357.8 | 571.4 |

## How to read these

- **Dialect ranking (1 shard):** PostgreSQL is the fastest real server (≈1093 steps/s), then MariaDB (≈743),
  then SQL Server (≈455) — consistent with the deployment guidance that recommends PostgreSQL for production.
  SQLite in-memory is fastest of all but is single-instance dev/test only.

- **Sharding on a single host does not add throughput; for server databases it actually reduces it.** Every
  server dialect is fastest at 1 shard and slower at 2–3. When the shards are separate databases on the *same*
  server, they add connection and coordination overhead without adding any hardware, so throughput drops. This
  is expected: sharding is a way to scale *across* servers (one shard per database server — see
  [deployment](deployment.md)), and a single-host benchmark can only show its cost, never its benefit. So treat
  the shard results here as a reliability check — proof that a multi-database engine still routes and completes
  every flow correctly — rather than as a scaling demo.

- **SQLite is the exception, and for an instructive reason.** It gets *faster* with more shards (417 → 682
  flows/s) because each shard is a separate in-memory database with its own single-writer lock, so sharding
  parallelizes the one-writer-at-a-time bottleneck that is SQLite's defining constraint. That gain is specific
  to independent in-memory databases and does not transfer to a shared server.

- **Latency comes mainly from durable commits.** A ~80 ms p50 for a 3-step flow on PostgreSQL works out to
  ~27 ms per step, and each step is a synchronously-committed transaction (claim + transition) running under
  32-way concurrency on laptop Docker. A real workload adds task time on top of this, so treat these figures as
  the engine's baseline latency, not the total you would see in production.

## State payload throughput (bytes/sec)

`BenchmarkStatePayload` seeds each flow's initial state with a JSON string of a given size and carries it
through every step (each step persists `merge(prevState, changes)`, and no-op tasks forward state unchanged),
so the engine serializes, writes, and re-merges the whole payload once per step — 3× per flow here. The
headline metric is **payloadMB/s = payload size × steps/sec**: the rate at which caller state bytes move
through durable storage. Persisted bytes are ~3–4× that, since every step stores its own copy. Fixed at 1
shard. (Same hardware/config as above; one representative run at `-benchtime=100x`.)

payloadMB/s by payload size:

| Payload | SQLite | PostgreSQL | MariaDB | SQL Server |
|---|---|---|---|---|
| 4 KB | 4.0 | 3.1 | 2.3 | 1.2 |
| 64 KB | 24.1 | 45.5 | 22.4 | 12.9 |
| 256 KB | 30.5 | 124.4 | 45.3 | 21.3 |
| 1 MB | 31.8 | 157.4 | 72.6 | 7.8 |

What a 1 MB payload costs — throughput drops sharply and latency rises:

| | SQLite | PostgreSQL | MariaDB | SQL Server |
|---|---|---|---|---|
| flows/s | 10.6 | 52.5 | 24.2 | 2.6 |
| p50 latency | 2.4 s | 0.5 s | 1.2 s | 12.2 s |

Reading it:

- **The byte rate rises with payload size, then levels off.** With a tiny payload, each step's fixed overhead
  dominates, so few caller bytes move per second. A larger payload spreads that fixed cost over more bytes, so
  the rate climbs — until it reaches the engine-plus-database byte-bandwidth limit and levels off.
- **The dialect ranking flips compared with small state.** SQLite led on tiny-state flow rate, but at 1 MB
  **PostgreSQL is well ahead (~157 MB/s)**, then MariaDB (~73), SQLite (~32), and SQL Server (~8). For large
  payloads, what matters is how well the database parallelizes big writes: PostgreSQL and MySQL spread them
  across many connections, whereas SQLite has a single writer that handles them one at a time (so its byte rate
  levels off early), and SQL Server's `NVARCHAR(MAX)` large-object handling is the slowest here (a 1 MB flow
  takes ~12 s at the median). So the best dialect depends on how large your state is, not just on the raw flow
  rate.
- **Large state is expensive.** A flow carrying 1 MB of state takes seconds, not milliseconds. Because every
  step stores its own copy of the state, a large payload is written many times over per flow. Workflows that
  move large documents should trim state early with `flow.Del`, and keep big blobs in object
  storage — passing only a key through flow state. (`final_state` is `JSON`/`JSONB`/`NVARCHAR(MAX)`/`TEXT` per
  dialect, so it holds arbitrarily large output on all four — there is no 64 KB cap.)

## What this does *not* measure

A single-host laptop run cannot show what production hardware does — those dimensions are measured by the
[cloud benchmarks](benchmark-cloud.md): true horizontal scale on shard-per-server hardware, connection-pool
and worker sizing (the sizing formula), the effect of real network latency, byte-throughput ceilings, and
throughput at high volume (100M+ accumulated rows). Still open anywhere: a sustained multi-hour soak for
drift (goroutine/memory/connection growth). This benchmark remains the per-dialect throughput/latency
baseline the cloud numbers build on — the cloud campaign runs PostgreSQL only, on the ranking measured here.
