# Cloud benchmarks

`bench/` (the standalone benchmark host in this repository) measured the engine against managed cloud
PostgreSQL across a real network hop — the production shape the in-repo
[laptop benchmarks](benchmark.md) deliberately cannot represent. Four measurement sessions, ~120 valid runs, zero
reliability events (no flow errors, no lease recoveries, no wedges) across all of them.

> **Environment.** Engine host: GCE `c4a-standard-4` (4 vCPU, ARM). Database: Cloud SQL for
> PostgreSQL 16, `db-custom-{1,2,4,8}` tiers (1–8 vCPU), 100 GB SSD, private-IP, same-zone
> (RTT ≈ 0.3–0.6 ms). One engine replica; one shard per database instance (two for the scale-out
> runs). Fresh database per configuration (accumulated tables depress throughput 17–29%). Closed-loop
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
- **Pre-fix / post-fix engine** — before/after the two engine improvements this campaign produced (see
  "Validated engine changes" below); post-fix is the engine as shipped.

## The cost model, with every constant measured

One step costs the engine a slice of database time and a slice of task time. The worker wall time is
`T = db + exec`, and the database time is linear in the network round-trip: `db = k·L + s`
(engine-internal time proved negligible — see "History" below). Every constant was measured:

| Constant | Value | How measured |
|---|---|---|
| `k` — DB round-trips per step | **~12** (~11 with the doorbell fix, one of the engine changes below) | the latency sweep: connection-held time vs RTT is linear with R² ≈ 1 (`db = 12.1·RTT + 4.4 ms`) |
| `s` — server-side execution + group-committed fsync | **~4.4 ms** | the latency fit's intercept |
| `db` at low utilization, same-zone | **7–8 ms/step** | measured at M=8, a deliberately small pool that keeps the database far from saturation (4/8-vCPU tiers) |
| Connection knee | **~6 × DB vCPUs** (range 4–8) | the tier table below |
| Steps ceiling `C_db` | see tier table | the tier table at saturation |
| Byte ceiling (incompressible payloads) | **~46–60 MB/s** per instance (100 GB disk) | the `state` workload |

## Steps throughput by database tier

![Steps throughput by database tier and connection pool size](benchmark-cloud-tiers.png)

Post-fix engine; 512 workers; closed-loop concurrency 512; a fresh database per measurement. Rows are
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

| added delay | measured RTT | steps/s | connection-held ms/step |
|---|---|---|---|
| +0 | 0.28 ms | 1021 | 7.8 |
| +0.5 | 0.82 ms | 529 | 15.1 |
| +1 | 1.34 ms | 372 | 21.5 |
| +2 | 2.35 ms | 244 | 32.8 |
| +5 | 5.34 ms | 116 | 69.1 |

Per-connection throughput halves as `k·L` doubles — and total throughput recovers by raising M (until
the knee/ceiling). Latency is a connections tax, not an absolute cap. Cross-zone (~1.1 ms) roughly
doubles `db` vs same-zone; co-locating the engine with its shard's zone is the single cheapest win.

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
**46 / 60 MB/s per database instance** — bound by the instance's disk/WAL path. (Compressible payloads
measured 3× higher because PostgreSQL's TOAST compression shrank them to almost nothing on disk —
beware benchmarks with repetitive payloads.) Byte throughput scales out with shards: two instances gave
×1.77 / ×1.82. The engine's `dwarf_state_write_bytes` metric (labelled by workflow and by the column
written) is the operational gauge against this ceiling.

## Scale-out

![Two shards vs one: measured scaling factors](benchmark-cloud-scaleout.png)

Two 8-vCPU shards (each its own database instance) at saturation: **6,719 steps/s = ×1.81** over a
single shard. These runs used the pre-fix engine, so the absolute numbers are conservative; the ~19%
shortfall from perfect ×2 scaling is unattributed (engine-host-side or a closed-loop-load artifact) and
worth revisiting at four shards. Shard-per-server scales both steps and bytes near-linearly at its
first test.

## The sizing formula

Inputs: `V` = the shard database's vCPU count, `L` = RTT to the shard, `exec` = mean task time,
`Y` = the database's `max_connections` setting. `headroom` is a small reserve of connections left for
monitoring and other clients.

```
M  = min(Y − headroom, ~6·V)        connections   (per shard; ÷ replicas when R > 1)
db = k·L + s ≈ 12·L + 4.4ms         per-step DB time
T  = db + exec                       per-step worker time
N  = M × T/db                        workers
ceiling ≈ min(N/T, M/db, C_db)       steps/s
```

Worked example — an 8-vCPU shard, same-zone (L = 0.5 ms), 50 ms tasks: M = 6×8 = 48 connections;
db ≈ 12×0.5 + 4.4 ≈ 10.4 ms; T ≈ 60 ms; N = M × T/db ≈ 48 × 5.8 ≈ 280 workers. Predicted ceiling
≈ min(4600, 4600, C_db ≈ 4600) steps/s — the database binds, as designed.

**The engine applies this automatically**: provide `ShardSpec.VirtualCPUs` and it derives each shard's
connection pool and its capacity-proportional share of new-flow placement; `SetWorkers` remains the one
manual knob (deriving it needs the task-time profile, a runtime quantity — see the adaptive design
below). `SetMaxOpenConns` survives only as an expert override that pins pools exactly.

## Validated engine changes

The campaign's profiling work produced two engine fixes, then validated them with interleaved
before/after runs on identical infrastructure:

- **Refill pacing**: under deep backlog, the engine's scheduler was re-scanning the full pending
  backlog every few milliseconds and re-offering steps whose claims were still in flight, wasting
  ~half of all claim attempts; a short pause after each full refill lets claims land first.
- **Due-doorbell fast path**: dispatching a just-created step no longer re-reads a row the same
  replica just wrote, saving one database round-trip per step.

Measured effect: saturated throughput **+11.7%**, connection-bound **+5.1%**, mid-load +2.9%, latency
improved in all regimes — and the post-fix tier table above shows the gains compound at higher
connection counts (8-vCPU ceiling 3,262 → 4,596, +41%).

### History: the "50 ms engine overhead" that wasn't

Session 1 measured ~50 ms/step of apparent engine-side time. A profiling investigation attributed it
to sql.DB pool queueing against a silently mis-derived pool (a ceiling flag that never bound), not
engine work — worker idle time was 0.00 ms and engine CPU negligible. True engine-internal overhead is
small. Moral, twice over: pool sizing errors masquerade as engine overhead, and explicit facts
(`VirtualCPUs`) beat derived guesses.

## Known gaps

- **Replicas (R) untested**: the formula's ÷R division and multi-replica coordination await the
  multi-replica campaign. An adaptive connection-budgeting design under consideration would eliminate
  R from configuration entirely.
- **Disk-size axis unmeasured**: the byte ceiling has one point (100 GB); Cloud SQL scales disk
  throughput with size, so larger disks should raise it — asserted, not measured.
- **The 1→2 vCPU non-scaling** is hypothesized (WAL-insert-lock), not confirmed.
- **Small-tier variance** (±40% at 1 vCPU) is unexplained; treat 1-vCPU numbers as indicative.
- Absolute numbers are one cloud, one dialect (PostgreSQL — the fastest per the
  [in-repo benchmarks](benchmark.md)), one region; ratios and shapes are the durable findings.

## Reproducing

```sh
# Provision (GCP; see bench/gcp/provision.sh for knobs), then:
GOOS=linux GOARCH=arm64 go build -o dwarf-bench ./bench
./dwarf-bench -dsn 'postgres://USER:PASS@PRIVATE_IP:5432/dwarf?sslmode=disable' \
  -workload linear -workers 512 -max-open-conns 48 \
  -concurrency 512 -window 60s -warmup 15s -label my-run
# Tear down with bench/gcp/teardown.sh. Fresh database per configuration; never reuse tables.
```
