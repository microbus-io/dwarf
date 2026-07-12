# Cloud benchmarks

`bench/` (the standalone benchmark host in this repository) measured the engine against managed cloud
PostgreSQL across a real network hop — the production shape the in-repo laptop benchmarks
(`benchmark.md`) deliberately cannot represent. Four measurement sessions, ~120 valid runs, zero
reliability events (no flow errors, no lease recoveries, no wedges) across all of them.

> **Environment.** Engine host: GCE `c4a-standard-4` (4 vCPU, ARM). Database: Cloud SQL for
> PostgreSQL 16, `db-custom-{1,2,4,8}` tiers (1–8 vCPU), 100 GB SSD, private-IP, same-zone
> (RTT ≈ 0.3–0.6 ms). One engine replica; one shard per database instance (two for the scale-out
> runs). Fresh database per configuration (accumulated tables depress throughput 17–29%). Closed-loop
> load; 60 s measured windows after warmup; a run is invalid if any error/recovery/unwedge counter
> fires. Raw artifacts: one self-contained JSON per run.

## The cost model, with every constant measured

A step's cost decomposes into `db` (connection-held database time) and `exec` (task time); worker wall
time `T = db + exec` (engine-internal time proved negligible — see "History" below). The database time
is `db = k·L + s`:

| Constant | Value | How measured |
|---|---|---|
| `k` — DB round-trips per step | **~12** (~11 after the doorbell fix) | netem latency sweep: conn-held time vs RTT is linear with R² ≈ 1 (`db = 12.1·RTT + 4.4 ms`) |
| `s` — server-side execution + group-committed fsync | **~4.4 ms** | netem fit intercept |
| `db` at low utilization, same-zone | **7–8 ms/step** | M=8 calibration points, 4/8-vCPU tiers |
| Connection knee | **~6 × DB vCPUs** (range 4–8) | tier ladder |
| Steps ceiling `C_db` | see tier table | tier ladder at saturation |
| Byte ceiling (incompressible state) | **~46–60 MB/s** per instance (100 GB disk) | state workload |

## Steps throughput by database tier

Post-fix engine, workers=512, concurrency 512, fresh DB per point, steps/s:

| | M=8 | M=16 | M=32 | M=48 | M=64 | M=96 | ceiling (mean ± sd, n=3) |
|---|---|---|---|---|---|---|---|
| **1 vCPU** | 852 | **856** | 447 | 427 | 400 | 385 | 585 ± 235 (high variance) |
| **2 vCPU** | 712 | **942** | 889 | 855 | 847 | 809 | 772 ± 133 |
| **4 vCPU** | 1042 | 1684 | **1931** | 1903 | 1873 | 1258 | 1819 ± 208 |
| **8 vCPU** | 1213 | 2300 | 3773 | 4351 | **4594** | 4596 | 4413 ± 72 |

- **Headline: one shard on an 8-vCPU instance sustains ~4,600 steps/s ≈ 400M steps/day** at roughly
  $400/month of database. Repeats are tight on large tiers (±1.6% at 8 vCPU) and noisy on the smallest
  (±40% at 1 vCPU — small-tier numbers are indicative, not precise).
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

Connection-bound regime (M=8), artificial delay added with `tc netem`:

| added delay | measured RTT | steps/s | conn-held ms/step |
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

Throughput in the worker-bound regime is exactly `N/T` (validated to 1%): 32 workers with no-op tasks
(T ≈ 72 ms at a saturated pool) gave 443 steps/s; with 100 ms task delay (T ≈ 175 ms), 183 steps/s.
Workers beyond `M × T/db` only deepen the pool queue. With a 100 ms task, the knee moved from ~64–96
workers (no delay) to ~192 — matching the formula.

## Bytes: a separate, per-instance ceiling

Incompressible 1 MB / 8 MB payloads rewritten at every step: **46 / 60 MB/s per instance** — disk/WAL
bound (compressible payloads measured 3× higher because TOAST compressed them to almost nothing;
beware benchmarks with repetitive payloads). Byte throughput scales with shards: 2 instances gave
×1.77 / ×1.82. The `dwarf_state_write_bytes` metric (by `workflow` and `column`) is the operational
gauge against this ceiling.

## Scale-out

Two 8-vCPU shards at saturation: **6,719 steps/s = ×1.81** over one shard (pre-fix engine binaries;
the residual ~19% is unattributed — host-side or closed-loop artifacts — and worth revisiting at 4
shards). Shard-per-server scales both steps and bytes near-linearly at its first test.

## The sizing formula

Inputs: `V` = shard DB vCPUs, `L` = RTT to the shard, `exec` = mean task time, `Y` = DB max_connections.

```
M  = min(Y − headroom, ~6·V)        connections   (per shard; ÷ replicas when R > 1)
db = k·L + s ≈ 12·L + 4.4ms         per-step DB time
T  = db + exec                       per-step worker time
N  = M × T/db                        workers
ceiling ≈ min(N/T, M/db, C_db)       steps/s
```

Worked example — 8-vCPU shard, same-zone (L = 0.5 ms), 50 ms tasks: M = 48, db ≈ 10.4 ms, T ≈ 60 ms,
N ≈ 48 × 5.8 ≈ 280 workers; predicted ceiling ≈ min(4600, 4600, C_db ≈ 4600) — DB-bound, as designed.

**The engine applies this automatically**: provide `ShardSpec.VirtualCPUs` and it derives each shard's
connection pool and its capacity-proportional share of new-flow placement; `SetWorkers` remains the one
manual knob (deriving it needs the task-time profile, a runtime quantity — see the adaptive design
below). `SetMaxOpenConns` survives only as an expert override that pins pools exactly.

## Validated engine changes

An A/B (pre/post) of two fixes found by the campaign's pool investigation: deep-backlog refill pacing
and the due-doorbell fast path. Saturated throughput **+11.7%**, connection-bound **+5.1%**, mid-load
+2.9%, latency improved in all regimes — and the post-fix ladder above shows the gains compound at
higher connection counts (8-vCPU ceiling 3,262 → 4,596, +41%).

### History: the "50 ms engine overhead" that wasn't

Session 1 measured ~50 ms/step of apparent engine-side time. A profiling investigation attributed it
to sql.DB pool queueing against a silently mis-derived pool (a ceiling flag that never bound), not
engine work — worker idle time was 0.00 ms and engine CPU negligible. True engine-internal overhead is
small. Moral, twice over: pool sizing errors masquerade as engine overhead, and explicit facts
(`VirtualCPUs`) beat derived guesses.

## Known gaps

- **Replicas (R) untested**: the formula's ÷R division and multi-replica coordination await the
  multi-replica campaign. Adaptive budgeting would eliminate R entirely (`_CONGESTION_DETECTION.md`).
- **Disk-size axis unmeasured**: the byte ceiling has one point (100 GB); Cloud SQL scales disk
  throughput with size, so larger disks should raise it — asserted, not measured.
- **The 1→2 vCPU non-scaling** is hypothesized (WAL-insert-lock), not confirmed.
- **Small-tier variance** (±40% at 1 vCPU) is unexplained; treat 1-vCPU numbers as indicative.
- Absolute numbers are one cloud, one dialect (PostgreSQL — the fastest per `benchmark.md`), one
  region; ratios and shapes are the durable findings.

## Reproducing

```sh
# Provision (GCP; see bench/gcp/provision.sh for knobs), then:
GOOS=linux GOARCH=arm64 go build -o dwarf-bench ./bench
./dwarf-bench -dsn 'postgres://USER:PASS@PRIVATE_IP:5432/dwarf?sslmode=disable' \
  -workload linear -workers 512 -max-open-conns 48 \
  -concurrency 512 -window 60s -warmup 15s -label my-run
# Tear down with bench/gcp/teardown.sh. Fresh database per configuration; never reuse tables.
```
