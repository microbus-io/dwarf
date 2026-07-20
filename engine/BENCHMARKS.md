# Engine benchmark findings — measured results, for the dwarf coding agent

> **Audience: the dwarf coding agent, not module users.** The public benchmark write-up is
> `docs/benchmark-cloud.md`; it covers the same campaigns at the how-fast-is-it altitude and names no
> internals. This file is free to name functions, phases, columns, and instruments.
>
> **This file does NOT auto-load.** `engine/CLAUDE.md` does. Read this one when you are:
> - about to change the **refiller** (`scanBandKeys` / `planBatch` / `fetchBandSteps` / `runRefill`),
>   the **candidate cache**, or the **selection index** — several plausible changes here have already
>   been built and measured, and two of them lost;
> - about to **explain or optimize a throughput ceiling** — the numbers below say which subsystem
>   actually binds, and at least three confident diagnoses in this repo's history were wrong;
> - about to **design a benchmark** — the rig's failure modes (bimodality, load-generator saturation,
>   degenerate workload corners) have eaten several campaigns, including recent ones.
>
> **Everything here is measurement, so it expires.** Each entry carries its date and rig. A result
> measured on a 4-vCPU engine host says nothing about a 16-vCPU one — that mistake is the subject of
> the first section.

## The load generator has invalidated more campaigns than any engine bug

**2026-07-19.** `docs/benchmark-cloud.md` reported a shard ladder of ×2.04 at two shards and ×2.19 at
three — "the second shard doubles, the third adds nothing" — and explained it as cross-shard straggler
waits. **Both the finding and the explanation were wrong.** The campaign was n=1 per arm on a **4-vCPU
engine host that was itself the bottleneck.**

The tells, in order of how quickly they would have caught it:

- **`stepsPerCore` flat across every arm** (~2,100–2,900 in *both* the old campaign and the new one).
  A fixed steps-per-core with changing shard count is the signature of a client-bound system. This is
  the cheapest possible check and it is already in every artifact.
- **Per-shard database CPU falling as shards are added** — ~82% at one shard, ~72% at two, ~51% at
  three — while engine CPU climbed 36% → 60% → 77%. The shards were idling; the generator was maxed.
- **The knee moves when the generator is resized.** 4-vCPU engine knees at 3 shards; 16-vCPU at 5. A
  plateau that follows the client is a property of the client.

Re-measured (16-vCPU engine, 6× 8-vCPU Cloud SQL shards each on its own instance, n=3 interleaved,
fresh database per run, 256 fairness keys, 5 ms task delay, `linear`, concurrency 4096):

| shards | steps/s | replicate range | vs 1 shard | refiller headroom |
|---|---|---|---|---|
| 1 | 3,458 | 3,301–3,631 | — | 1.32× |
| 2 | 6,944 | 6,592–7,171 | ×2.01 | 1.39× |
| 3 | 8,079 | 6,894–10,095 | ×2.34 | 1.47× |
| 4 | 12,461 | 11,436–13,448 | ×3.60 | 1.29× |
| 5 | 14,737 | 14,153–15,520 | ×4.26 | 1.15× |
| 6 | 14,913 | 14,512–15,196 | ×4.31 | **1.04×** |

**Open:** the flat step at three shards is unexplained. Refiller headroom is at its *highest* there
(1.47×), so the ceiling below does not account for it, and that arm's replicate range is wide enough
that its position is uncertain. A targeted experiment would run arms on shard sets `{1,2,4}` vs
`{1,2,3}` to separate "the count" from "those instances."

## The refiller is what binds, and its cost model

**2026-07-19, same rig.** Three results, from the `dwarf_refill_*` instruments.

**1. The refiller sets throughput.** Candidate supply runs only **1.04–1.47×** ahead of consumption and
falls toward 1.0 as shards are added. It is not a background cost.

**2. Band scan cost ≈ `46 ms fixed + 0.0085 ms per due row on that shard`.** This corrects a claim in
`engine/CLAUDE.md`: the three-phase split made the scan independent of the backlog in **wire and heap**
cost (phase 1 returns O(keys) rows), **not** in server-side cost — `COUNT(*) OVER (PARTITION BY
fairness_key)` and the `MIN(priority)` subquery both touch every due row. Consequences:

- Sharding divides the *variable* term: 185 ms → 69 ms mean as backlog/shard fell 16,384 → 2,730.
- The *fixed* term is paid per shard per pass and does not divide.
- 4× the backlog on one shard made the scan **2.85× slower** and cost **34% of throughput**
  (3,640 → 2,404 steps/s). That is a positive feedback loop — slower scans deepen the backlog, which
  slows scans — and a plausible mechanism for the bistable collapse `bench/gcp/degradation.sh` records.

**3. The pass pays `max` over shards, not `mean`, and max-of-N diverges.** The penalty grew 1.00× →
2.02× from one to six shards, so dividing the backlog 6× improved what the pass *actually waits on* by
only 24% (185 ms → 140 ms). Within a single arm, identical RTT-matched shards spread up to 2.1× — so
this is **order statistics, not a persistently slow shard.** This is the strongest evidence for giving
each shard its own independent refill.

**Dead hypothesis — the refiller oversupplies the workers.** `discarded/selected` measured 0–10% on the
cloud rig and 0% on a local SQLite smoke run, never the ~100% it predicted. Do not re-propose without
new evidence.

## Refiller changes that were built and measured

**Per-shard fetch cap — BUILT, MEASURED, REVERTED (2026-07-19). Do not rebuild.**

Phase 3 fetches ~`S × capacity` rows to use `capacity`: `fetchBandSteps` asks *every* shard for the
global max per-key demand, because a key's oldest steps could all sit on one shard. Capping each shard
at its own share therefore looks like free savings.

It is not, and the reason is subtle: **`rn <= perKey` filters AFTER the window function.** PostgreSQL
computes `ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id)` across every
matching due row before the outer query cuts it, so a smaller cap reduces rows *returned over the wire*,
not rows *processed on the server*. The over-fetch is real and is a **wire** cost; the wire is not where
the time goes.

Two A/Bs (fan-out width 1024, 256 keys, 6 shards) measured phase-3 time **unchanged**:

| variant | n | fetch ms (old → new) | throughput |
|---|---|---|---|
| cap = `min(demand, what the shard holds)` | 3 | 50.2 → 50.6 | inside noise |
| cap = `min(holdings, ⌈demand/spread⌉ × 2)` | 5 | 46.2 → 47.8 | inside noise |

The first variant is a no-op by construction under any backlog (every shard holds far more of every key
than the plan wants, so `min(demand, holdings) == demand`). The second genuinely cut the cap 18 → 6 rows
per key per shard and *still* moved nothing — which is the measurement that identifies the window
function as the cost.

**The two changes worth trying next, in order:**

1. **Try to make phase 1 heap-free: `not_before`, `lease_expires` AND `fairness_weight` in
   `idx_dwarf_steps_selection`.** Due-ness is tested against `NOW()` and neither timestamp is in the
   index, so every candidate entry fetches its heap row just to learn whether the step is due; the scan
   also *selects* `fairness_weight`, which is likewise absent — so including only the two timestamps
   buys nothing, the weight still drags the row in. All three or none.

   **MEASURED locally (2026-07-19, PostgreSQL 18.1, 200k due rows over 256 keys) — it works, and the
   visibility-map objection does not bite the way it first appears.** Baseline is an `Index Scan` with
   the two timestamps applied as a post-fetch `Filter` and **203,280 buffer hits for 200,000 rows** —
   about one heap touch per row, which is the `0.0085 ms/row` coefficient. With
   `INCLUDE (not_before, lease_expires, fairness_weight)`:

   | table state | plan | scan node | buffers |
   |---|---|---|---|
   | baseline, no covering index | Index Scan | 249.7 ms | 203,280 |
   | **head-churned (realistic queue)** | **Index Only Scan** | **107.1 ms** | **49,463** |
   | clean / just vacuumed | Index Only Scan, `Heap Fetches: 0` | 108.4 ms | 2,542 |
   | uniformly churned, no vacuum | Index Only Scan, `Heap Fetches: 200000` | ~250 ms | 203,793 |

   **The row that matters is the second, and the reason is a nice inversion.** A queue does not churn
   uniformly — it churns at the HEAD, where steps are being claimed and completed. The deep backlog
   behind it sits `pending` and untouched, so those pages go all-visible and the scan reads them from
   the index alone. Which means the index-only scan works *precisely in the state where the band scan
   is most expensive*. (The last row is a uniform-churn test — every 10th row updated, so every page
   dirtied. It is included because it was my first attempt and it is NOT how a queue behaves; it
   nonetheless shows the useful property that the change degrades to exactly baseline, never worse.)

   Costs: the index doubles (15 MB → 30 MB per 200k rows). Write amplification is near-free — a claim
   already updates `status`, a key column, so those writes are already non-HOT, and none of the three
   added columns is touched by the `cohort_arrivals` bumps that can still be HOT.

   **Still to check before shipping:** all four dialects (MySQL has no `INCLUDE`, so there the columns
   must be appended as key columns, which bloats more), and whether autovacuum keeps up on a real
   workload — the local test vacuumed explicitly. Verify with `EXPLAIN (ANALYZE, BUFFERS)` that you get
   `Index Only Scan` with
   `Heap Fetches: 0`, and confirm against `dwarf_refill_query_duration_seconds{phase="band_keys"}`
   before and after. If the visibility map defeats it, that is a real answer worth recording here —
   and it would point at (2) as the only remaining lever.
2. **Replace phase 1's window functions with a loose index scan** (recursive CTE — no native skip-scan
   on PostgreSQL). The index is already ordered `(status, parked, priority, fairness_key, created_at,
   step_id)`, so each key's oldest step is the first entry of its run; enumerating distinct keys becomes
   O(keys × log n) instead of O(backlog). This needs only an **approximate** count, because `count` only
   bounds `remaining[i]` in `planBatch` and both error directions are already benign (over → phase 3
   comes up short and the replay skips; under → one pass mildly under-serves a key).

Neither touches the **fixed ~46 ms floor**, which must be attributed first: `metricRefillQuery` times
the whole `QueryContext`, so connection-pool wait and query execution are not separable. A 46 ms floor
on a query returning a few hundred rows, against 2,304 workers contending for 288 connections, smells
like pool queueing. Settling it needs server-side timing (`pg_stat_statements`) or a dedicated refiller
connection.

## Rig hazards that have corrupted campaigns

- **Deep-backlog runs are bimodal.** A control arm at fixed configuration spanned 6,422–13,486 (2.1×);
  a 1-shard deep arm spanned 2,404–3,299. Two replicates agreeing means nothing here — a "reproducible"
  3-shard plateau and a "+19.9%" A/B result both evaporated on the third run. Use n≥3, report the
  spread, and prefer a low-variance *phase-level* metric (fetch ms) over throughput when one exists.
- **Accumulated databases on one Cloud SQL instance depress throughput ~2.6× uniformly.** Arms that
  share an instance are not comparable. Use a fresh instance per arm, not just a fresh database.
- **Same-zone, same-tier Cloud SQL instances are NOT RTT-matched** — 1.7–2.3× spread is normal
  (0.076–0.208 ms measured across six). This is placement, not misconfiguration, and it does **not**
  matter for throughput: wire time is overlapped, so by Little's law a 0.1 ms delta costs ~2.5 extra
  workers in flight against a pool of ~384. Gate on **absolute** delta (`bench/gcp/shardladder.sh` uses
  1 ms), never a ratio — a ratio on sub-millisecond values aborts perfectly good campaigns.
- **Timing RTT with one `psql` process per sample measures ~29 ms of process + TCP + TLS startup**, three
  orders of magnitude above the signal. Use one session with `\timing` and take the minimum.
- **Cloud SQL provisions IOPS by DISK SIZE (~30 IOPS/GB), so a small disk silently becomes the ceiling
  on any write-heavy or large-volume run.** The default 100 GB in `bench/gcp/provision.sh` yields ~3,000
  IOPS ≈ **23 MB/s** of random 8 KB I/O; a width-64 fan-out fill measured **~27 MB/s** of checkpoint +
  backend writes and sat on that wall (2026-07-20). The signature is a *cliff*, not a slope — the fill
  rate held 4,489→2,247 steps/s over 2.2M→14.8M rows and then fell to **382** in a single leg — plus
  `checkpoints_req` far exceeding `checkpoints_timed` (151 requested in 98 min, one per ~39 s, against
  `max_wal_size` 1504 MB) and a large `buffers_backend` (backends evicting their own dirty pages because
  the checkpointer cannot keep up). **Size the disk so IOPS is not binding** (1 TB ≈ 30,000 IOPS ≈
  230 MB/s) before reading anything into a volume curve. Matching disk size across arms is necessary for
  fairness but NOT sufficient — if the disk binds in every arm, every arm measures the disk.
- **Workload corners that flatter or wreck a result:** a single fairness key is the degenerate
  single-partition case for the refiller; zero task delay is the least production-like point and is
  where the reverted cohort redesign got its one positive A/B; single-key fan-out is bistable (healthy
  ~2,400 steps/s or collapsed ~550). Prefer many keys and a non-zero delay.
- **A linear workload ties backlog depth to submitter count** (one in-flight step per flow), so "deeper
  backlog" and "more client goroutines" cannot be separated — and 16k submitter goroutines depress
  throughput on their own. Use fan-out width to move backlog independently of client cost.

## Volume degradation is a BYTES question, not a row-count one (2026-07-19/20)

Rig: GCP `dwarf-bench-mbus`, two independent Cloud SQL `db-custom-8-32768` (100 GB PD-SSD, `shared_buffers`
measured 10.96 GB), `c4a-standard-4` engine host, same zone, RTT p50 0.32–0.35 ms. Build `ed27983`, i.e.
BEFORE the fan-out state-ref fix (`2048622`).
Driver: `bench/gcp/degradation.sh` axis A (linear fill) and axis F (fan-out fill, width 64, 256 keys).

Raw artifacts were kept at `bench/results/degradation-20260720/` but that path is **gitignored**
(`.gitignore:3:bench/results/`), so treat this section as the durable record — every number needed to
act on the findings is reproduced here, and the rig is gone.

- **Row count is the wrong x-axis.** A linear fill was **flat to 30M rows** (1,126 / 1,056 / 1,090 steps/s
  at 10M / 20M / 30M) while a width-64 fan-out fill fell **1,124 → 666** by 10M. The two were never on the
  same axis: fan-out stored **1,124 B/row vs linear's 256 B**, so it reached any given byte volume at ~4.4×
  fewer rows. Plot throughput against database size (or better, working-set size), never row count.
- **862 B of that 1,124 was the un-ref'd `forEach` source array**, since the ref policy's floor made the
  width scaling dead code. Fixed in `2048622`: 862 → 77 B of state, 1,124 → 381 B/row. **Every number in
  this section predates that fix**, so a re-run should show the byte crossings move out ~3×.
- **A cross-probe separates workload SHAPE from table STATE, and it is the durable result here.** At the
  same checkpoint on the same fan-out-built database (10.45M rows), back-to-back: fan-out probe **520**
  steps/s, linear probe **914**. The linear arm on its own database read ~1,069–1,126. So **~43% of the
  loss is the fan-out shape itself** (cohort bookkeeping and spawn-row serialization — same database, same
  moment, only the shape differs, so this comparison is immune to every rig confound below) and **≤17% is
  the degraded table** (weaker: it crosses two instances and two points in time). `degradation.sh` runs
  this automatically on axis F via `CROSS_PROBE_ARGS`.
- **Bloat is NOT the mechanism.** Dead-tuple ratio anti-correlates with throughput: the 10M point had the
  *lowest* dead ratio (20.2%) and the worst throughput; the 4M point had the highest (43.1%) and was fast.
- **A connection-bound probe is the right instrument for volume, and its modest decline is not a
  contradiction of a large fill decline.** At `-max-open-conns 8` throughput *is* `M / per-step DB time`,
  so the probe reads latency directly: 7.1 ms → 15.4 ms per step (**2.2×**). The saturating fill at M=48
  over the same range went 10.7 ms → 126 ms (**11.8×**). The gap is queueing — the fill runs the database
  past its knee and is itself generating the WAL and dirty pages that trigger the checkpoint storms it then
  waits on. Quote the probe for latency-vs-volume, the fill only for "what a saturating writer does."
- ⚠️ **The absolute volume curves past ~10M are CONFOUNDED and should not be re-used**: the 100 GB disk
  saturated (see the IOPS hazard above). Two consequences. (1) The fan-out cliff at 16.2M is a disk wall,
  not a RAM wall. (2) **The linear arm's flatness may be "linear is light" rather than "linear stays
  cached"** — at ~250 B/row it never approached the disk ceiling, so its flat curve is not evidence that
  a working set larger than `shared_buffers` is free. Both need a re-run on a non-binding disk.
- **Planned, not run: a `shared_buffers`-cliff sweep across 16 / 32 / 64 GB instances** at fixed vCPU
  (12 is the cheapest count Cloud SQL allows at all three, given its 0.9–6.5 GB-per-vCPU rule), identical
  large disks, and checkpoints expressed as *fractions of RAM* so the three curves can be overlaid on
  `db_size / RAM`. Fill with sub-TOAST rows (~1.8 KB): a 64 KB payload lands in a TOAST table the probe
  never reads, so total size would grow while the working set stayed small and the cliff would not appear.

## Instruments available

`dwarf_refill_query_duration_seconds{shard,phase}` (phase = `band_keys` | `fetch_steps`),
`dwarf_refill_duration_seconds`, `dwarf_refill_candidates_selected`, `dwarf_refill_candidates_discarded`.
Descriptions and the operator-facing reading guide are in `docs/observability.md`.

Derived quantities used above, and how to compute them from an artifact:
- **headroom** = `selected / windowSec / stepsPerSec` — approaching 1.0 means the refiller is the ceiling.
- **straggler cost** = `(bandMax − bandMean) + (fetchMax − fetchMean)` — what a pass pays to wait for the
  slowest shard rather than the average one; this is what per-shard refills would reclaim.
- **go-side cost** = `passMs − bandMax − fetchMax` — `planBatch` plus the cross-shard merge/sort.

`bench/` records these: `collectHistograms` keeps per-attribute distributions as window deltas.
Before it existed, `collectCounters` extracted only `Sum[int64]` and silently discarded every gauge and
histogram — which is why no refiller hypothesis had ever been testable from a recorded run.
