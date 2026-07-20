# Cloud benchmarks

`bench/` (the standalone benchmark host in this repository) measured the engine against managed cloud
PostgreSQL across a real network hop — the production shape the in-repo
[laptop benchmarks](benchmark.md) deliberately cannot represent. Six measurement sessions, ~300 valid runs, zero
reliability events (no flow errors, no lease recoveries, no wedges) across all of them — including the
later campaign's 76 runs / 116 measurement points, which recorded zero client errors, zero lease
recoveries and zero unwedged steps while deliberately driving the engine into throughput collapse.

> **Environment.** Engine host: GCE `c4a-standard-4` (4 vCPU, ARM). Database: Cloud SQL for
> PostgreSQL 16, `db-custom-{1,2,4,8}` tiers (1–8 vCPU), 100 GB SSD, private-IP, same-zone
> (RTT ≈ 0.3–0.6 ms). One engine replica; one shard per database instance (up to three for the
> scale-out and fan-out runs). Fresh database per configuration (accumulated volume costs a one-time
> settling — measured [below](#volume-accumulated-rows-and-bytes-cost-a-one-time-settling)). Closed-loop
> load; 60 s measured windows after warmup; a run is invalid if any error/recovery/unwedge counter
> fires. Raw artifacts: one self-contained JSON per run.

> **Two campaigns.** The [fan-out ceiling](#fan-out-the-engines-own-ceiling) and
> [scale-out](#scale-out) sections are from the later campaign, run against three 8-vCPU shards after
> several engine changes (most relevant here: a deferred flow-row write on fan-in arrival, a
> three-phase refill, and an extended selection index). Everything else — the tier ladder, the
> latency sweep, workers, volume, and self-tuning — is from the earlier campaign and was **not**
> re-measured; those sections are marked where it matters. Where the two overlap, the later numbers win.

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
  the connection budget as a hard cap derived from `ShardSpec.VirtualCPUs`, and why an undeclared CPU
  count assumes only 2 vCPUs (a pool of 12 — under the 1-vCPU tier's own knee, so even a wrong
  assumption cannot reach the collapse zone).
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

## Fan-out: the engine's own ceiling

![Fan-out throughput ceiling on a 4 vCPU engine](benchmark-cloud-fanout.png)

The tier table above measures what a *database* supplies. This section measures what one **4 vCPU
engine host** can drive, using fan-out to raise the pending-step backlog far above the submitter
count (a linear flow holds one pending step per in-flight flow, so a closed-loop generator can never
build a deep backlog; a `forEach` of width `W` makes the backlog roughly `concurrency × W`). Three
8-vCPU shards, so the database is not the constraint.

**The highest sustained rates measured were ~9,400 steps/s, and the shape of the work did not change
that:**

| workload | concurrency | steps/s | engine CPU | steps/core |
|---|---|---|---|---|
| linear, 3 shards | 4096 | 9,392 | 3.16 / 4 (79%) | 2,972 |
| fan-out width 16 | 1024 | 9,402 | 3.16 / 4 (79%) | 2,976 |

The two agree to 0.1%, at the same CPU. So **fan-out costs the engine nothing extra per step** — the
cohort bookkeeping is not a throughput tax — and `steps/core ≈ 2,970` is the number to size with.

**Read the rate as a planning limit, not a reproducible constant.** Across the campaign the same
configuration measured anywhere from ~3,000 to ~9,400 steps/s depending on how much data the database
had accumulated (see the caveat below), so the honest operating guidance is: **do not plan a 4 vCPU
engine above ~10,000 steps/s**, expect less on a mature database, and size from `steps/core` with
your own measurement. What *is* stable is `steps/core`, which stayed within 2,600–2,980 across every
shard count, pool size, width, key layout and volume measured — which is why the sizing rule is
expressed in those terms.

**Nothing is saturated at the top rate.** At 9,400 steps/s the engine sits at 79%, the databases at
~70% CPU, and the connection pool at roughly 60% of what its per-step database time would support.
Throughput nonetheless stops climbing, and pushing concurrency higher *lowers* it.

### What limits it: not yet identified

An earlier version of this document attributed the ceiling to one row: the fan-in arrival counter that
every sibling of a cohort updates on a single shared row, whose lock is held across the holder's
commit. **That diagnosis was wrong, and it was tested directly rather than merely doubted.** It is
recorded here because the wrong answer is instructive and because it should not be re-derived.

Two claims were made and both failed:

- *"The great majority of active backends are blocked on that row lock."* Re-sampling
  `pg_stat_activity` on the same configuration puts it at **~15%** of active backends — real, and the
  largest single identifiable wait, but not a majority.
- *"A width-`W` cohort pays `W` serialized fsyncs."* Holding cohort width fixed while varying only the
  worker count moved lock wait **31×**, which a width-driven mechanism cannot explain. The serialized
  quantity is round trips *inside* the lock hold, not flushes; the queue length is
  `min(width, workers)`. Disabling synchronous commit changed the picture very little.

The decisive test was to **remove the contention entirely** — replacing the shared counter with
per-member arrival state, so siblings write disjoint rows. That reduced row-lock waits by 4.4× on this
rig (and 12–40× on a laptop rig at widths 64 and 256), and **throughput did not improve**: it was flat
to 29% *worse* across six A/B points spanning two concurrency levels and three task-latency profiles,
degrading most in the most production-like condition, where the replacement's extra round trip costs
more than the lock time it saves. The change was reverted.

So **the cohort row lock is real, removable, and not what caps throughput.** What does cap it is
currently unknown, and the elimination table below should be read as genuinely open-ended rather than
as pointing at a residual explanation.

The degraded regime looks like queueing rather than load: engine CPU *falls* (to as little as 0.86 of
4 cores) while median latency climbs into the tens of seconds.

Two further behaviours are measured and hold regardless of what the underlying cause turns out to be:

- **Fairness-key cardinality does not help**, at either width, over a 1000× range. At width 16 /
  concurrency 256, medians across 1, 8, 32, 256 and 1024 keys span 6,622–7,056 steps/s — a 6.5%
  range, inside the 1.06–1.21× spread *within* each cardinality, with no latency penalty. At width 64
  the between-cardinality medians span 16% (4,553–5,266) while runs at a *single* cardinality span
  2.1–2.2×, so the group differences sit an order of magnitude beneath the noise. Spreading keys
  interleaves different flows; it cannot de-synchronize one flow's own siblings, which is exactly who
  contends.
- **Wider is worse.** Width 16 was the best of the widths measured; width 64 peaked lower and degraded
  at lower concurrency. Narrower fan-outs are cheaper per step than one wide one covering the same
  work. (This was originally read as evidence for the counter-row explanation. It survives that
  explanation's retirement as an empirical result, but it is no longer evidence *for* any particular
  mechanism.)

Adding per-task time (5 ms plus 10 ms jitter) did not raise throughput either — at fixed concurrency
it lowered it 7–15%, since task time adds directly to flow latency. It did keep latency well behaved
(p99 ≈ 1.1 s at concurrency 256) and produced no collapse, but a benchmark at fixed concurrency
cannot show contention relief it never gets close enough to need.

### What the ceiling is *not*

Every resource that could plausibly bind was measured and eliminated. No capacity limit accounts for
the ceiling, and neither does the serialization point this list once pointed at:

| candidate | evidence it is not binding |
|---|---|
| engine CPU | 79% at the ceiling; *falls* to 20–40% in the collapsed regime |
| database CPU | 51–89%, versus the 91–100% a saturated instance shows |
| connection pool | ~60% of what the measured per-step database time would support |
| worker goroutines | flat at 1,167 across every concurrency point, never growing |
| candidate-cache size | doubling the connection budget (M 48→96) doubles the cache but bought only ×1.09 |
| fairness-key layout | flat over a 1000× cardinality range, at two widths |
| fan-in cohort row lock | removed outright (per-member arrival state); waits fell 4.4×, throughput did not rise |

The fairness-key row deserves emphasis because it is the one operators can control: **no amount of key
spreading helps**, because a cohort's siblings share one flow and therefore one key. That is a fact
about the workload, not about any particular contention point, so it stands unchanged.

**Practical guidance is unaffected by the open question.** Size from `steps/core` (2,600–2,980, stable
across every shard count, pool size, width, key layout and volume measured), do not plan a 4 vCPU
engine above ~10,000 steps/s, prefer narrower fan-outs, and expect less on a mature database. What
remains unknown is *why* the ceiling sits where it does — not where to plan against it.

### Caveat: fan-out churn is far more volume-sensitive than linear load

This campaign reused one database per shard across its runs, so volume grew monotonically to **15.2M
step rows (13.9 GB) with 2.8M dead tuples**. That turned out to matter enormously, and it is a
finding rather than only a flaw. Repeating one configuration eight times on the loaded database
against four times on a freshly created one:

| database | runs (steps/s) | spread |
|---|---|---|
| accumulated, 15.2M rows | 3,021 · 3,204 · 3,254 · 3,347 · 3,986 · 4,061 · 4,076 · 7,029 | **2.33×** |
| fresh per run | 6,333 · 6,465 · 7,549 · 7,579 | **1.20×** |

Two things follow. A fresh database is **reproducible to about ±10%**, so wide scatter is a symptom
of accumulated volume rather than an intrinsic property of the workload. And the penalty is large —
roughly halved throughput at 15M rows — which is **not** what the [volume fills](#volume-accumulated-rows-and-bytes-cost-a-one-time-settling)
below found. Those measured a `linear` workload, whose pending count stays flat, and found only a
one-time 15–20% settling out to 100M rows. Fan-out churn is a different regime: the pending set
swings by orders of magnitude, autovacuum trails the deletes, and the cost does not plateau the same
way. **Do not carry the "volume is a one-time 15–20%" result across to fan-out-heavy workloads**; it
was measured on linear load and does not transfer.

Consequently every cross-time comparison in this section is untrustworthy, and only the interleaved
A/B results above (key cardinality, pool size, task delay, shard count) support conclusions.

## Scale-out

![Two shards vs one: measured scaling factors](benchmark-cloud-scaleout.png)

Two 8-vCPU shards (each its own database instance) at saturation scaled to **×1.81** over a single
shard, and byte throughput scaled ×1.77/×1.82 — shard-per-server scales both steps and bytes
near-linearly at its first test.

![Steps and refiller headroom by shard count](benchmark-cloud-shardladder.png)

An earlier campaign measured this axis to three shards on a **4-vCPU** engine and reported that the
second shard scaled perfectly (×2.04) while the third added only 7% — attributing the flattening to
cross-shard straggler waits. **That conclusion was wrong, and the cause was the load generator.** On
that host the engine was itself the bottleneck: per-shard database CPU fell from ~82% at one shard to
~51% at three while the engine saturated, and `stepsPerCore` stayed flat at ~2,100–2,900 across every
arm — the signature of an engine-bound system. Re-running the same ladder on a 16-vCPU engine moved the
knee from three shards to five. A plateau that moves when you resize the client is a property of the
client.

Re-measured on a 16-vCPU engine, n=3 interleaved, one dedicated Cloud SQL instance and a fresh database
per run, 256 fairness keys and a 5 ms task delay (`linear`, concurrency 4096):

| shards | steps/s | replicate range | vs 1 shard | refiller headroom |
|---|---|---|---|---|
| 1 | 3,458 | 3,301–3,631 | — | 1.32× |
| 2 | 6,944 | 6,592–7,171 | ×2.01 | 1.39× |
| 3 | 8,079 | 6,894–10,095 | ×2.34 | 1.47× |
| 4 | 12,461 | 11,436–13,448 | ×3.60 | 1.29× |
| 5 | 14,737 | 14,153–15,520 | ×4.26 | 1.15× |
| 6 | 14,913 | 14,512–15,196 | ×4.31 | **1.04×** |

**Sharding keeps paying well past three shards** — ×3.6 at four, ×4.3 at six — at roughly 90% of linear
through four shards and tapering after. What binds at the top is the **refiller**, not the databases.
The headroom column is its candidate supply divided by what the workers consume: at 1.04× it is handing
out almost exactly what is taken, so throughput is set by how fast it can select rather than by how fast
the shards can execute.

Two things this table does *not* establish. The flat step at three shards is unexplained: the refiller
has its **most** headroom there (1.47×), so the ceiling above does not account for it, and that arm's
replicate range is wide enough (6,894–10,095) that its position is uncertain. And the refiller's own
cost model — measured per shard as roughly `46 ms + 0.0085 ms per due row` — means a shard's scan cost
falls as sharding divides the backlog, while the *fixed* term is paid per shard per pass and does not.
Whether that fixed floor is query time or connection-pool queueing is not yet distinguished; the
`dwarf_refill_*` instruments time the whole call.

Note this is one engine replica driving all the shards. Scaling *replicas* alongside shards is a
different axis and remains [untested](#known-gaps).

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

Both axes tell one story **for this workload**: a one-time ~15–20% settling as the database matures
from empty, then a plateau — no cliff, no compounding decay. Both fills used `linear`/`state` shapes
whose pending count stays flat; a later campaign found fan-out churn far more volume-sensitive
(roughly halved throughput by 15M rows), so this result does
[not transfer to fan-out-heavy workloads](#caveat-fan-out-churn-is-far-more-volume-sensitive-than-linear-load).
Three mechanisms were checked directly at every checkpoint:

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

## Self-tuning, measured against hand-tuning

The engine derives its own connection pool and worker count. Two runs on one 8-vCPU instance ask
whether that derivation is as good as tuning by hand — and whether it holds when tasks are long.

**Short tasks: the derived configuration matches a hand-tuned one.** Three repeat runs of each
configuration, alternating, on a fresh database each (`linear`, closed-loop concurrency 512):

| Configuration | steps/s (3 runs) | mean | worker goroutines |
|---|---|---|---|
| Hand-tuned (`SetWorkers(512)`, `SetMaxOpenConns(48)`) | 3454, 3439, 3623 | **3,505** | 512 |
| Derived (`ShardSpec.VirtualCPUs: 8`, nothing else) | 3408, 3379, 3536 | **3,441** | 384 |

Within 2% — and with fewer goroutines, because the connection budget is what actually bounds
dispatch. One declared integer replaces two tuned ones.

**Long tasks: the pool grows to fit the work in flight.** A single-step workflow whose task sleeps 60
seconds (an LLM call, a human-latency integration), same derived configuration — the resident worker
set is 384, so anything beyond that requires the pool to grow:

| Closed-loop concurrency | flows/s | `concurrency ÷ 60 s` | worker goroutines | errors / lease recoveries |
|---|---|---|---|---|
| 500 | 8.3 | 8.3 | 514 | 0 |
| 2,000 | 33.3 | 33.3 | 2,011 | 0 |
| 10,000 | 166.6 | 166.7 | 10,011 | 0 |

Throughput tracks `concurrency ÷ task duration` exactly at every level: the engine holds 10,000
minutes-long tasks in flight against a 48-connection pool, and median flow latency is 60.0 s — the
task duration itself, with no queueing. Workers parked in a task hold no connection, which is why the
worker maximum can be this large without touching the database's budget.

## The sizing formula

Inputs: `V` = the shard database's vCPU count, `L` = RTT to the shard, `exec` = mean task time,
`Y` = the database's `max_connections` setting. `headroom` is a small reserve of connections left for
monitoring and other clients.

```
M  = min(Y − headroom, ~6·V) ÷ R    # connections per replica (R = live replica count, read from the shared databases)
db = k·L + s ≈ 12·L + 4.4ms         # per-step DB time
T  = db + exec                      # per-step worker time
N  = M × T/db                       # workers
ceiling ≈ min(N/T, M/db, C_db)      # steps/s
```

Worked example — an 8-vCPU shard, same-zone (L = 0.5 ms), 50 ms tasks: M = 6×8 = 48 connections;
db ≈ 12×0.5 + 4.4 ≈ 10.4 ms; T ≈ 60 ms; N = M × T/db ≈ 48 × 5.8 ≈ 280 workers. Predicted ceiling
≈ min(4600, 4600, C_db ≈ 4600) steps/s — the database binds, as designed.

### How many engine hosts, and how much database behind them

The formula above sizes one shard. Two further rules size the *engine* fleet, both measured on a
4 vCPU engine host:

```
engine steps/s   ≈ 2,970 × engine vCPUs × utilisation   # steps/core, stable across every configuration
plan limit       ≈ 10,000 steps/s per 4 vCPU engine     # do not exceed; add replicas past this
database vCPUs   ≈ 6 × engine vCPUs                     # at a ~70% database utilisation target
```

`steps/core ≈ 2,970` is the durable constant — it held within 2,600–2,980 across shard counts, pool
sizes, fan-out widths, key layouts and volumes, and is identical for linear and fan-out work. The
plan limit is the operating ceiling: past roughly 10,000 steps/s per 4 vCPU host, more offered load
stopped buying throughput and began costing latency.

**The database ratio is only meaningful together with the utilisation it assumes.** The measured
point was 9,400 steps/s from a 4 vCPU engine (79% busy) against three 8-vCPU shards running ~70% CPU.
Sizing so the database *stays* near 70% — the sane target, since a database at 100% has nothing left
for autovacuum, checkpoints or a traffic spike — gives **1 engine vCPU : 6 database vCPUs**. If you
were instead willing to drive the database to saturation the same measurement reads as ~1:4. Prefer
1:6: an under-provisioned engine merely queues, while an over-driven database degrades non-linearly.

**The engine applies this automatically**: provide `ShardSpec.VirtualCPUs` and it derives each
shard's connection pool (divided by the replica count it reads live from the shared databases —
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
- **The fan-out ceiling is bounded but not positively attributed.** Engine CPU, database CPU,
  connection pool, worker count, candidate-cache size and fairness-key layout were each measured and
  eliminated as the binding resource; the direct observation is that most active backends block on
  the shared cohort-arrival row, whose lock is held across commit. What has *not* been done is a
  before/after measurement of the structural fix (per-member arrival state replacing the shared
  counter), which is the only way to confirm that row is the cause rather than a symptom.
- **Fan-out volume sensitivity is measured but not explained.** Throughput roughly halved by 15M rows
  under fan-out churn while the linear fills plateaued at −15–20% out to 100M. Whether the difference
  is autovacuum lag, index bloat on the churned selection index, or planner drift is untested.
- **Absolute fan-out rates on a mature database are unquantified.** The fresh-database runs are
  reproducible to ±10%, but the campaign lacks a controlled volume ladder for fan-out, so "expect less
  than 10,000 steps/s as the database matures" is a direction, not a number.
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
