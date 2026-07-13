# Cloud benchmarks

`bench/` (the standalone benchmark host in this repository) measured the engine against managed cloud
PostgreSQL across a real network hop — the production shape the in-repo
[laptop benchmarks](benchmark.md) deliberately cannot represent. Five measurement sessions, ~220 valid runs, zero
reliability events (no flow errors, no lease recoveries, no wedges) across all of them.

> **Environment.** Engine host: GCE `c4a-standard-4` (4 vCPU, ARM). Database: Cloud SQL for
> PostgreSQL 16, `db-custom-{1,2,4,8}` tiers (1–8 vCPU), 100 GB SSD, private-IP, same-zone
> (RTT ≈ 0.3–0.6 ms). One engine replica; one shard per database instance (two for the scale-out
> runs). Fresh database per configuration (accumulated volume costs a one-time settling — measured
> [below](#volume-accumulated-rows-and-bytes-cost-a-one-time-settling)). Closed-loop
> load; 60 s measured windows after warmup; a run is invalid if any error/recovery/unwedge counter
> fires. Raw artifacts: one self-contained JSON per run.

## Terms and notation

- **Worker (`N`)** — one of the engine's worker goroutines; `N` is the worker-pool size (`SetWorkers`).
- **Connection pool size (`M`)** — the number of open SQL connections the engine may hold to one shard's
  database. The benchmark pins it exactly via the `SetMaxOpenConns` override.
- **`L`** — the network round-trip time (RTT) from the engine host to the database, measured
  continuously during every run by a dedicated sampler connection.
- **`db`** — the database portion of one step's cost: the total time a step spends holding a connection
  (its statements plus their round-trips). **`exec`** — the task's own run time. **`T = db + exec`** —
  one step's total worker wall time.
- **`k`** and **`s`** — the two constants of `db = k·L + s`: the number of database round-trips per
  step, and the server-side execution + commit-fsync time.
- **`C_db`** — a database instance's steps/s ceiling: the throughput at which its CPU saturates.
- **Knee** — the point on a curve beyond which adding more of the swept resource stops helping.
- **Closed-loop concurrency** — the load model: that many submitters each run one flow and immediately
  start the next when it completes. "Concurrency 512" = 512 flows in flight.
- **Workloads** — `linear`: a 10-step chain of no-op tasks (isolates the engine + database cost per
  step); `state`: a 5-step chain whose every task rewrites a payload of a chosen size (measures byte
  throughput).
- **Repeat runs** — key configurations were run three times (`n=3`); tables report mean ± standard
  deviation for those.

## The cost model, with every constant measured

One step costs the engine a slice of database time and a slice of task time. The worker wall time is
`T = db + exec`, and the database time is linear in the network round-trip: `db = k·L + s`
(engine-internal time was measured and is negligible). Every constant was measured:

| Constant | Value | How measured |
|---|---|---|
| `k` — DB round-trips per step | **~11** | the slope of connection-held time vs RTT in the latency sweep below |
| `s` — server-side execution + group-committed fsync | **~4.4 ms** | the latency fit's intercept |
| `db` at low utilization, same-zone | **7–8 ms/step** | measured at M=8, a deliberately small pool that keeps the database far from saturation (4/8-vCPU tiers) |
| Connection knee | **~6 × DB vCPUs** (range 4–8) | the tier table below |
| Steps ceiling `C_db` | [per tier](#steps-throughput-by-database-tier) | the tier table at saturation |
| Byte ceiling (incompressible payloads) | **~46–60 MB/s** per instance (100 GB disk); **~88 MB/s** at 500 GB | the `state` workload |
| Volume settling (mature vs fresh database) | **one-time ~15–20%**, then flat | the [volume fills](#volume-accumulated-rows-and-bytes-cost-a-one-time-settling) |

## Steps throughput by database tier

![Steps throughput by database tier and connection pool size](benchmark-cloud-tiers.png)

512 workers; closed-loop concurrency 512; a fresh database per measurement. Rows are
database tiers, columns are the connection pool size `M`; cells are steps/s. The last column reports
the tier's peak configuration re-run three times:

| DB tier \ pool `M` | 8 | 16 | 32 | 48 | 64 | 96 | ceiling (mean ± sd, n=3) |
|---|---|---|---|---|---|---|---|
| **1 vCPU** | 852 | **856** | 447 | 427 | 400 | 385 | 585 ± 235 (high variance) |
| **2 vCPU** | 712 | **942** | 889 | 855 | 847 | 809 | 772 ± 133 |
| **4 vCPU** | 1042 | 1684 | **1931** | 1903 | 1873 | 1258 | 1819 ± 208 |
| **8 vCPU** | 1213 | 2300 | 3773 | 4351 | **4594** | 4596 | 4413 ± 72 |

- **Headline: one shard on an 8-vCPU instance sustains ~4,600 steps/s ≈ 400M steps/day** at roughly
  $400/month of database. Repeat runs of each tier's peak configuration were tight on the large tiers
  (±1.6% at 8 vCPU) and noisy on the smallest (±40% at 1 vCPU) — treat small-tier numbers as
  indicative, not precise.
- **Over-connection collapses small tiers.** 1 vCPU falls from 856 to 385 steps/s as M grows 16→96;
  4 vCPU shows the onset at M=96. Beyond the knee, connections queue inside PostgreSQL (connection-held
  time grows linearly with M at flat throughput) and then actively harm. This is why the engine treats
  the connection budget as a hard cap derived from `ShardSpec.VirtualCPUs`, and why an unknown CPU
  count falls back to a measured-safe pool of 8 rather than a guess.
- **The DB is CPU-bound at the ceiling** (~91–100% DB CPU; disk write-IOPS peaked at half the 100 GB
  budget, refuting a WAL/IOPS explanation). The 1→2 vCPU step scales poorly (a known commit-heavy
  PostgreSQL pattern, suspected WAL-insert-lock serialization; unconfirmed — needs pg wait-event
  sampling). From 2→4→8 vCPU the ceiling scales ~×2, ~×2.4.

## Latency: it costs connections, not throughput

![Per-step database time vs round-trip latency](benchmark-cloud-latency.png)

These runs pin the pool small (M=8) so connections are the binding resource — then the per-step
database time can be read directly as `M ÷ throughput`. Artificial network delay is added on the
engine host with `tc netem` (a Linux traffic-control tool) so RTT becomes a finely-steppable variable:

| added delay (ms) | measured RTT (ms) | steps/s | connection-held time (ms/step) |
|---|---|---|---|
| +0 | 0.28 | 1021 | 7.8 |
| +0.5 | 0.82 | 529 | 15.1 |
| +1 | 1.34 | 372 | 21.5 |
| +2 | 2.35 | 244 | 32.8 |
| +5 | 5.34 | 116 | 69.1 |

The points fall on a straight line (R² ≈ 1): `db = 12.1·RTT + 4.4 ms`. The slope is `k` — each extra
millisecond of round-trip latency costs one millisecond per database round-trip the step makes, and
this build made ~12 of them. (The engine has since eliminated one of those round-trips when it
dispatches a freshly created step, so the current count is ~11.) The intercept is `s`, the server-side
execution and commit time that remains when the network is free.

The practical reading: per-connection throughput halves as `k·L` doubles — and total throughput
recovers by raising M (until the knee/ceiling), so latency is a connections tax, not an absolute cap.
Cross-zone placement (~1.1 ms RTT) roughly doubles `db` vs same-zone; co-locating the engine with its
shard's zone is the single cheapest win available.

## Workers

![Workers vs throughput at two task durations](benchmark-cloud-workers.png)

When workers are the binding resource, throughput is exactly `N/T` — the worker count divided by the
per-step wall time (validated to 1%): 32 workers running no-op tasks (T ≈ 72 ms against a saturated
pool) gave 443 steps/s; the same 32 workers with a 100 ms artificial task delay (T ≈ 175 ms) gave
183 steps/s. Adding workers beyond `M × T/db` buys nothing — they only queue on the connection pool.
Task time therefore moves the worker knee: with 100 ms tasks it shifted from ~64–96 workers to ~192,
matching the formula.

## Bytes: a separate, per-instance ceiling

The `state` workload, rewriting an incompressible 1 MB / 8 MB payload at every step, sustained
**46 MB/s / 60 MB/s per database instance** — bound by the instance's disk/WAL path. (Compressible payloads
measured 3× higher because PostgreSQL's TOAST compression shrank them to almost nothing on disk —
beware benchmarks with repetitive payloads.) Byte throughput scales out with shards: two instances gave
×1.77 / ×1.82. It also scales with disk size, sublinearly: the same 8 MB probe on a 500 GB disk
sustained **87.5 MB/s** (+46% for 5× the disk) — Cloud SQL raises disk throughput with size, but not in
proportion, so byte-bound deployments should measure before buying disk. The engine's
`dwarf_state_write_bytes` metric (labelled by workflow and by the column
written) is the operational gauge against this ceiling.

## Scale-out

![Two shards vs one: measured scaling factors](benchmark-cloud-scaleout.png)

Two 8-vCPU shards (each its own database instance) at saturation scaled to **×1.81** over a single
shard. The ~19% shortfall from perfect ×2 scaling is unattributed (engine-host-side or a
closed-loop-load artifact) and worth revisiting at four shards. Shard-per-server scales both steps and
bytes near-linearly at its first test.

## Volume: accumulated rows and bytes cost a one-time settling

![Per-step DB time vs accumulated rows and accumulated bytes](benchmark-cloud-volume.png)

Two fills on identical 8-vCPU / 32 GB / 500 GB instances isolate the two ways a database grows, with a
connection-bound probe (M=8, the `linear` workload) between fill stages so per-step DB time is read
directly as `M ÷ throughput` at every checkpoint:

- **Rows** — the `linear` workload to **100M `dwarf_steps` rows** (53 GB of small rows: row count grows,
  bytes stay far below RAM per byte of index). Per-step DB time rose from 7.3 ms to a ~8.7 ms plateau by
  ~10M rows and **never worsened again** — the 100M checkpoint (8.0 ms) sits within 10% of the empty
  baseline.
- **Bytes** — the `state` workload with 64 KB payloads to **405 GB** (only 3.7M rows: bytes grow, row
  count stays in a range the rows fill proved cheap). Per-step DB time rose from 8.9 ms to a
  ~10.7 ms plateau by ~32 GB and stayed in a 10–11.3 ms band to 405 GB.

Both axes tell one story: **a one-time ~15–20% settling as the database matures from empty, then a
plateau — no cliff, no compounding decay.** Three mechanisms were checked directly at every checkpoint:

- **Cache pressure barely materializes.** Buffer-cache hit ratios stayed ≥ 99.7% (index) and ≥ 99.2%
  (heap/TOAST) even at 13× RAM — the access pattern is recency-dominated (active flows touch recent
  rows), so cold volume is evicted harmlessly. The bytes curve shows no break at the `shared_buffers`
  (~11 GB) or RAM (32 GB) boundaries.
- **B-tree depth is a non-event.** Four of the five `dwarf_steps` indexes gained a level during the
  rows fill (verified with `pageinspect`); the probe curve did not move when they did — an extra
  *cached* page per descent is unmeasurable.
- **Bloat is held by autovacuum.** Dead tuples oscillated between 4% and 30% with vacuum cycles, with
  no upward trend at either fill's write rate; probe scatter (±5–7%) tracks the vacuum cycle.

The saturated ceiling settles by the same factor: filling at M=48 started at ~3,400 steps/s on the
empty database and plateaued at ~2,750 ± 150 from ~20M rows onward. For capacity planning, derate a
fresh-database ceiling by ~20% for a mature deployment: the worked example's 4,600 steps/s reads as
~3,700 sustained. Retention (`Purge` / `DeleteOnCompletion`) caps volume, but these fills show the
penalty for carrying history is bounded and flat, not a slow death.

## The sizing formula

Inputs: `V` = the shard database's vCPU count, `L` = RTT to the shard, `exec` = mean task time,
`Y` = the database's `max_connections` setting. `headroom` is a small reserve of connections left for
monitoring and other clients.

```
M  = min(Y − headroom, ~6·V) ÷ R    # connections per replica (R observed via peer signals)
db = k·L + s ≈ 12·L + 4.4ms         # per-step DB time
T  = db + exec                      # per-step worker time
N  = M × T/db                       # workers
ceiling ≈ min(N/T, M/db, C_db)      # steps/s
```

Worked example — an 8-vCPU shard, same-zone (L = 0.5 ms), 50 ms tasks: M = 6×8 = 48 connections;
db ≈ 12×0.5 + 4.4 ≈ 10.4 ms; T ≈ 60 ms; N = M × T/db ≈ 48 × 5.8 ≈ 280 workers. Predicted ceiling
≈ min(4600, 4600, C_db ≈ 4600) steps/s — the database binds, as designed.

**The engine applies this automatically**: provide `ShardSpec.VirtualCPUs` and it derives each
shard's connection pool (divided by the replica count it observes live over the peer-signal channel —
nothing to declare), its capacity-proportional share of new-flow placement, and — in aggregate — the
default worker count (a generous 8× the summed
connection budget, since `T/db` is a runtime quantity and idle workers are cheap while an
under-provisioned pool caps throughput). `SetWorkers` and `SetMaxOpenConns` survive as expert
overrides for tests, benchmark sweeps, and externally-constrained hosts — and `SetWorkers` is the
right tool when tasks are long: `N = M × T/db` grows with task time, so a workload of minutes-long
tasks (an LLM call, a human-latency integration) wants orders of magnitude more workers than the
derived default, and blocked workers are cheap (a goroutine and a socket each).

## Known gaps

- **Replicas (R) untested**: the observed-R division and multi-replica coordination await the
  multi-replica campaign. An adaptive connection-budgeting design under consideration would eliminate
  R from configuration entirely.
- **Volume was measured under accumulation, not steady-state retention**: the fills grew monotonically
  with autovacuum trailing inserts. A create-and-purge equilibrium (the reaper deleting as fast as flows
  arrive) exercises vacuum differently and is the province of a long soak test, along with slow drift
  (memory, goroutines, connection recycling) that no 60 s window can see.
- **The 1→2 vCPU non-scaling** is hypothesized (WAL-insert-lock), not confirmed.
- **Small-tier variance** (±40% at 1 vCPU) is unexplained; treat 1-vCPU numbers as indicative.
- Absolute numbers are one cloud, one dialect (PostgreSQL — the fastest per the
  [in-repo benchmarks](benchmark.md)), one region; ratios and shapes are the durable findings.

## Reproducing

```sh
# Provision on GCP (knobs documented in bench/gcp/provision.sh), then:
GOOS=linux GOARCH=arm64 go build -o dwarf-bench ./bench
./dwarf-bench -dsn 'postgres://USER:PASS@PRIVATE_IP:5432/dwarf?sslmode=disable' \
  -workload linear -workers 512 -max-open-conns 48 \
  -concurrency 512 -window 60s -warmup 15s -label my-run
# Tear down with bench/gcp/teardown.sh. Fresh database per configuration; never reuse tables.
```
