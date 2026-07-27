# Cloud benchmarks

`bench/` (the standalone benchmark host in this repository) measures the engine against managed cloud
PostgreSQL across a real network hop — the production shape the in-repo
[laptop benchmarks](benchmark.md) deliberately cannot represent. Several hundred valid runs across many
sessions, with zero reliability events in all of them (no flow errors, no lease recoveries, no wedged
steps) — including sessions that deliberately drove the engine into throughput collapse.

This page is a capacity-planning document. It answers four questions: **how much can one database
serve**, **what does the engine cost on top of it**, **what happens when you add databases or engine
replicas**, and **what does the engine do for you automatically**.

## How to read these numbers

Vocabulary used throughout, including a few load-testing terms that change what a number means:

- **Step** — one task execution inside a flow. Throughput is quoted in **steps/s**, because a flow's cost
  is the sum of its steps. A 10-step flow at 1,000 steps/s is 100 flows/s.
- **Shard** — one database instance the engine dispatches against. **Shard means a separate database
  *server*, not a separate database on a shared server** — that distinction is load-bearing and
  [measured below](#the-single-shard-write-ceiling-is-a-stack-of-locks).
- **Replica (`R`)** — one engine process. Several replicas share the same shards; the engine discovers
  how many there are and divides each shard's connection budget between them.
- **Open-loop load, at a commanded arrival rate** — the benchmark creates flows at a fixed rate
  (`-open-loop -arrival-rate N`) and does not wait for them to finish. It is the only method that can show
  what an overloaded system does: offered load is an independent variable, so a database that cannot keep
  up falls *behind* the command rather than quietly throttling its own input. **Every database-capacity
  number here is measured this way, and new measurements should be too.**
- **Closed-loop load, at a concurrency** — the older method: `N` submitters each run one flow and start
  the next when it completes, so exactly `N` flows are in flight. It is convenient (it needs no rate
  calibration) but it **cannot measure saturation**: a database that slows down slows its own arrivals to
  match, so the curve flattens instead of collapsing, and the peak it reports is not the peak. Sections
  measured this way say so; treat them as shape, not as ceilings.
- **Arm** — one configuration of one experiment (e.g. "4 vCPU, pool 24"). **Ladder** — a series of arms
  sweeping one variable (shard count, replica count, database size). **Interleaved** — arms run in
  rotation rather than in blocks, so drift over the session hits every arm equally.
- **Knee** — the point on a curve past which more of the swept resource stops helping.
- **p99 latency** — the 99th-percentile flow completion time. A rising p99 at flat throughput means work
  is queueing, which throughput alone will not show you.
- **steps/core** — steps/s divided by engine-host CPU cores actually consumed. It is a capacity constant
  **only when the engine host is the saturated resource**; measured while the database is the constraint
  it merely reports how little the engine had to do.
- **`M`** — the connection pool size: how many SQL connections the engine may hold open to one shard.
  **`L`** — round-trip network time to the database. **`db`** — the part of a step that holds a
  connection. **`exec`** — the task's own run time. **`T = db + exec`** — one step's total worker time.
- **Workloads** — `linear`: a 10-step chain of no-op tasks, which isolates the engine + database cost per
  step. `fanout`: a `forEach` of a chosen width, which makes the queue of runnable steps much deeper than
  the flow count. `state`: a 5-step chain whose every task rewrites a payload of a chosen size, which
  measures bytes rather than steps.
- **A run is invalid** if any error, lease-recovery or unwedge counter fires. Invalid runs are discarded,
  not averaged in. Every run writes one self-contained JSON artifact.

**Absolute numbers belong to the rig that produced them; ratios travel.** Instances of the same spec
differ measurably (two same-spec 64-vCPU instances measured 28,502 vs ~23,500 steps/s at an identical
pool), so a comparison is only trustworthy when the two arms ran interleaved against the same instance.
Comparisons quoted here are of that form unless stated otherwise.

> **Environment.** Google Cloud, single region, same zone as the database (round-trip ≈ 0.3–0.9 ms).
> Databases are Cloud SQL for PostgreSQL 16, from 1 to 64 vCPUs, on SSD sized for IOPS headroom
> (1 TB except where a section says otherwise — [disk IOPS matter](#benchmark-against-a-fresh-database-and-size-disk-iops-to-the-write-rate)).
> Engine hosts are 4, 16 and 32-vCPU ARM VMs; each section names the one it used, because the engine host
> is itself a resource that can bind. Fresh database per configuration. 60 s measurement windows after a
> warmup that is discarded.

## The cost model, with every constant measured

One step costs the engine a slice of database time and a slice of task time. The worker time is
`T = db + exec`, and the database part is linear in the network round-trip: `db = k·L + s`, where `k` is
how many round-trips a step makes and `s` is the server-side work plus commit. Engine-internal CPU time
is negligible next to both. Every constant is measured:

| Constant | Value | How measured |
|---|---|---|
| `k` — database round-trips per step | **~11** | the slope of connection-held time vs round-trip time in the [latency sweep](#latency-costs-connections-not-throughput) |
| `s` — server-side execution + group-committed fsync | **~4.4 ms** | that sweep's intercept |
| `db` at low utilization, same-zone | **7–8 ms/step** | measured at `M=8`, a deliberately tiny pool that keeps the database far from saturation |
| Connections the engine opens | **6 × database vCPUs**, or **12 ×** at 32 vCPUs and above | [the knee and the safe ratio](#connections-the-throughput-knee-and-the-safe-ratio-are-different-numbers) |
| Steps ceiling, per database | **~555 to ~23,000 steps/s** for 1 to 64 vCPUs | [vertical scaling](#database-size-vertical-scaling-from-1-to-64-vcpus) |
| What binds one database at the top | **WAL write-lock contention**, then buffer-page contention | [the write ceiling](#the-single-shard-write-ceiling-is-a-stack-of-locks) |
| Byte ceiling (incompressible payloads) | **~46–60 MB/s** per instance (100 GB disk); **~88 MB/s** at 500 GB | [the `state` workload](#bytes-a-separate-per-instance-ceiling) |
| Volume settling (mature vs fresh database) | **one-time ~15–20%**, then flat | [the volume fills](#volume-accumulated-rows-and-bytes-cost-a-one-time-settling) |

## Database size: vertical scaling from 1 to 64 vCPUs

![Achieved throughput vs offered load, by database size](benchmark-cloud-vertical.png)

Seven database sizes, one session, one 32-vCPU engine host, **1 TB SSD and ~4 GB RAM per vCPU on every
tier** so vCPU count is the only variable. Open-loop `linear` load at a commanded arrival rate; 20 s
warmup, 60 s window, fresh database per arm. Each tier runs the connection pool the engine derives for it.

Every curve tracks `offered = achieved` until its instance runs out, then departs. Reading where each one
leaves the diagonal is the whole chart: that is the load the instance actually serves.

![Vertical scaling: peak throughput by database vCPU count](benchmark-cloud-vertical-scaling.png)

| database | peak steps/s | vs previous | DB CPU at peak | pool |
|---|---|---|---|---|
| 1 vCPU | 555 | — | 100% | 6 |
| 2 vCPU | 1,101 | ×1.98 | 85% | 12 |
| 4 vCPU | 1,971 | ×1.79 | 84% | 24 |
| 8 vCPU | 3,679 | ×1.87 | 85% | 48 |
| 16 vCPU | 7,491 | ×2.04 | 78% | 96 |
| 32 vCPU | 21,059 | ×2.81 | 49%¹ | 384 |
| 64 vCPU | 23,290 | ×1.11 | 24%¹ | 768 |

¹ *at the servable point, not at peak — see the caveat below.*

- **Scaling is near-linear from 1 to 16 vCPUs** — four consecutive doublings return ×1.79 to ×2.04. A
  16-vCPU instance sustains ~7,500 steps/s ≈ 650M steps/day.
- **The 16→32 step (×2.81) is superlinear and partly an artifact**: the connection ratio also changes
  there (6× → 12×), so it is not a clean size comparison. Read it as "32 vCPU is worth more than double
  a 16" without trusting the exact factor.
- **32→64 returns only ×1.11.** Vertical scaling has clearly bent by 64 vCPUs — but see the caveat.

**The most useful operating number is not a connection count — it is database CPU.** Every tier's
servable point lands at **70–90% database CPU**, and every arm above ~90% is either collapsed or running
multi-second p99. Size to keep the database in that band and the connection arithmetic mostly takes care
of itself.

**⚠️ The 32 and 64-vCPU points are bounds, not measured ceilings.** Database CPU at their best arms was
49% and 24% — neither instance was ever driven to its own limit, because the 32-vCPU engine host driving
them ran out first. **Those two points measure the load generator as much as the database.** A larger
engine fleet would be needed to find where vertical scaling really ends.

### The collapse mode

Past saturation an instance can enter a state that is not a slowdown. It is worth naming because it is
invisible in an average and because a connection ratio chosen for throughput makes it more likely:

| | healthy | collapsed |
|---|---|---|
| throughput (16 vCPU) | 8,629 steps/s | **486 steps/s** |
| active backends | ~140 | **177** |
| WAL share of backend waits | ~69% | **~14%** |
| `CPU:running` share | ~6% | **~80%** |

The backends stop committing and burn CPU instead. It recovers on its own — a mode, not a wedge — but a
minute in it is an incident. It appeared **only past saturation**, never on an instance with headroom.

### Connections: the throughput knee and the safe ratio are different numbers

Holding the database fixed and sweeping only the pool, the *throughput* knee is at **12 connections per
vCPU** on every size from 16 up: +11.7% at 16 vCPU, +5.0% at 32, +14.0% at 64, over a 6× pool. On a
16-vCPU instance with IOPS headroom the same comparison reads +31% (5,355 → 7,016 steps/s) with *better*
latency (p99 17.6 s → 7.4 s), and past the knee it **plateaus rather than falling**: 192, 384 and 768
connections are statistically tied at ~6,400–7,000 steps/s.

![Steps throughput by database tier and connection pool size](benchmark-cloud-tiers.png)

Small instances behave differently, and that difference is why the engine's default is conservative. They
do not plateau past the knee, they fall off a cliff — a 1-vCPU instance drops **856 → 385 steps/s** as its
pool grows from 16 to 96 connections, and a 4-vCPU instance loses ~35% at the same over-provisioning.
(Those arms were measured closed-loop on 100 GB disks, so read the *shape* — where each tier turns over —
rather than the ceilings; ~3,000 IOPS is itself part of what makes a small instance brittle.)

**And on large instances the knee is not the safe point.** Counting how many arms entered the collapse
state above, across the vertical-scaling session:

| database | 6× | 12× |
|---|---|---|
| 8 vCPU | 1 / 13 | 2 / 11 |
| 16 vCPU | 0 / 13 | 2 / 9 |
| 32 vCPU | 0 / 14 | 0 / 9 |
| 64 vCPU | 0 / 7 | 1 / 14 |
| **total** | **1 / 47 (2%)** | **5 / 43 (12%)** |

A ~6× higher chance of a ~15× throughput drop, to buy 5–14% of peak. **So the engine picks the ratio by
instance size**: 6× below 32 vCPUs, 12× at 32 and above — the smallest size at which the throughput knee
was reached with no measured increase in instability. An operator raising it by hand with
`SetMaxOpenConns` should know which of the two numbers they are choosing.

Connection ratios can only be compared open-loop, by interleaved A/B against the **same** instance:
closed-loop load never reaches the collapse state, and two same-spec instances differ by more than the
effect being measured.

## The single-shard write ceiling is a stack of locks

Every measurement above is bounded, in the end, by how fast one PostgreSQL instance can write. That limit
is not CPU and not disk, and it is worth knowing precisely because it decides *what to buy*. Measured on
a 16-vCPU instance with 1 TB SSD by sampling `pg_stat_activity` for what backends were waiting on:

| pool | steps/s | `LWLock/WALWrite` waiters | `LWLock/BufferContent` | on CPU | `IO/WALSync` |
|---|---|---|---|---|---|
| 96 | 5,355 | 33 | 0.1 | 1.5 | 0.8 |
| 192 | 7,016 | 70 | 2.6 | 7.2 | ~0 |
| 384 | ~6,400 | 99 | 76 | 24 | ~0 |

- **The wall is the WAL write lock** — the lock serializing writes into the write-ahead log. A step makes
  several writes (a successor insert, flow and step updates, the commit), and all of them funnel through
  that one lock.
- **Not CPU**: at the 7,016 steps/s peak only 7.2 of 16 cores were doing useful work.
- **Not disk**: WAL *fsync* waits were ~0 and IOPS were not binding. It is the in-memory write lock, not
  the flush.

**Removing the top of the stack exposes the next one about 5% higher.** Turning off synchronous commit —
the zero-code operator lever aimed exactly at this — eliminated WAL-write waits entirely (67.8 → 0.0
concurrent waiters) and bought **+4%** throughput, because buffer-page contention immediately more than
doubled (14.5 → 33.2) and capped it there. `wal_compression` is worse than neutral (−5%): it reduces WAL
*bytes*, and bytes are not what is scarce. **The single-shard write ceiling is a stack —
WAL write → buffer contention → relation extension — each within single-digit percent of the next**, so
attacking any one of them individually returns almost nothing.

**Therefore: shard means a separate database *server*.** A PostgreSQL cluster has exactly one WAL, shared
by every database inside it, so two dwarf shards as two databases on one instance share the ceiling. Two
matched pairs, same total connections, measured directly:

| topology | steps/s | vs one database on one instance |
|---|---|---|
| 2 databases, **one** instance | 6,916 | **×1.03** (flat) |
| 2 databases, **two** instances | 13,995 | **×2.08** |

with the mechanism visible in the wait counts: the one-instance arm concentrated both databases' WAL
traffic on a single lock (113 concurrent waiters, the same as the baseline's 115), while the two-instance
arm split it across two independent locks (110 + 69). **Horizontal sharding across servers is the write
scaling lever**; there is no way to buy it inside one instance.

## Latency costs connections, not throughput

![Per-step database time vs round-trip latency](benchmark-cloud-latency.png)

These runs pin the pool small (`M=8`) so connections are the binding resource — then the per-step
database time can be read directly as `M ÷ throughput`. Artificial network delay is added on the engine
host with `tc netem` (a Linux traffic-control tool) so round-trip time becomes a finely-steppable
variable:

| added delay (ms) | measured RTT (ms) | steps/s | connection-held time (ms/step) |
|---|---|---|---|
| +0 | 0.28 | 1021 | 7.8 |
| +0.5 | 0.82 | 529 | 15.1 |
| +1 | 1.34 | 372 | 21.5 |
| +2 | 2.35 | 244 | 32.8 |
| +5 | 5.34 | 116 | 69.1 |

The points fall on a straight line (R² ≈ 1): `db = 12.1·RTT + 4.4 ms`. The slope is `k`, the number of
round-trips a step makes — each extra millisecond of network latency costs one millisecond per round-trip.
A step makes ~11 of them (a freshly created step is dispatched without a re-read), so **plan with
`k ≈ 11`** and treat the fitted 12.1 as the sweep's own configuration. The intercept is `s`, the
server-side execution and commit time that remains when the network is free.

The practical reading: per-connection throughput halves as `k·L` doubles — and total throughput recovers
by raising `M` (until the knee), so latency is a **connections tax, not an absolute cap**. Cross-zone
placement (~1.1 ms) roughly doubles `db` against same-zone; co-locating the engine with its shard's zone
is the single cheapest win available.

## Workers: throughput is `N/T` when workers bind

![Workers vs throughput at two task durations](benchmark-cloud-workers.png)

When workers are the binding resource, throughput is exactly the worker count divided by the per-step
wall time (validated to 1%): 32 workers running no-op tasks (`T ≈ 72 ms` against a saturated pool) gave
443 steps/s; the same 32 workers with a 100 ms artificial task delay (`T ≈ 175 ms`) gave 183 steps/s.
Adding workers beyond `M × T/db` buys nothing — they only queue for a connection. Task time therefore
moves the worker knee: with 100 ms tasks it shifted from ~64–96 workers to ~192, matching the formula.

You do not normally set this number; the engine derives it and grows into it on demand
([below](#self-tuning-measured-against-hand-tuning)).

## Bytes: a separate, per-instance ceiling

The `state` workload, rewriting an incompressible 1 MB / 8 MB payload at every step, sustained
**46 MB/s / 60 MB/s per database instance** — bound by the instance's disk/WAL path. (Compressible
payloads measured 3× higher because PostgreSQL's TOAST compression shrank them to almost nothing on disk
— beware benchmarks with repetitive payloads.) Byte throughput scales out with shards: two instances gave
×1.77 / ×1.82. It also scales with disk size, sublinearly: the same 8 MB probe on a 500 GB disk sustained
**87.5 MB/s** (+46% for 5× the disk) — Cloud SQL raises disk throughput with size, but not in proportion,
so byte-bound deployments should measure before buying disk. The `dwarf_state_write_bytes` metric
(labelled by workflow and by the column written) is the operational gauge against this ceiling.

## Fan-out: the engine host's own ceiling

![Fan-out throughput ceiling on a 4 vCPU engine](benchmark-cloud-fanout.png)

The sections above measure what a *database* supplies. This one measures what one **4 vCPU engine host**
can drive, using fan-out to raise the number of runnable steps far above the flow count (a linear flow
has one runnable step at a time, so it cannot build a deep queue; a `forEach` of width `W` makes the
queue roughly `flows × W`). Three 8-vCPU shards, so the database is not the constraint.

**The highest sustained rates measured were ~9,400 steps/s, and the shape of the work did not change
that:**

| workload | in-flight flows | steps/s | engine CPU | steps/core |
|---|---|---|---|---|
| linear, 3 shards | 4096 | 9,392 | 3.16 / 4 (79%) | 2,972 |
| fan-out width 16 | 1024 | 9,402 | 3.16 / 4 (79%) | 2,976 |

The two agree to 0.1%, at the same CPU. So **fan-out costs the engine nothing extra per step** — the
cohort bookkeeping is not a throughput tax on the engine host.

**It is not free on the database, though.** Against a *single* saturated shard, where the database rather
than the engine is the constraint, a width-16 fan-out ran **3,484 steps/s against linear's 5,048** on the
same instance: the extra rows and the fan-in convergence write are database work. So fan-out buys queue
depth and parallel task execution, not cheaper steps.

**It is a planning limit, not a reproducible constant.** The same configuration measured anywhere from
~3,000 to ~9,400 steps/s depending on how much data the database had accumulated (see
the [caveat](#benchmark-against-a-fresh-database-and-size-disk-iops-to-the-write-rate)). **Do not plan a
4 vCPU engine above ~10,000 steps/s**, expect less on a mature database, and size from `steps/core`
measured on your own host class.

**Nothing is saturated at that rate**: engine 79%, databases ~70% CPU, connection pool at ~60% of what
its per-step database time would support. Pushing more load past it *lowers* throughput — engine CPU
falls to as little as 0.86 of 4 cores while median latency climbs into the tens of seconds. Treat
~10,000 steps/s per 4 vCPU host as a ceiling to stay under, not a target to approach.

### What the ceiling is not

Every resource that could plausibly bind was measured and eliminated:

| candidate | evidence it is not binding |
|---|---|
| engine CPU | 79% at the ceiling; *falls* to 20–40% in the degraded regime |
| database CPU | 51–89%, versus the 91–100% a saturated instance shows |
| connection pool | ~60% of what the measured per-step database time would support |
| worker goroutines | flat at 1,167 across every load point |
| the engine's candidate cache | doubling the connection budget (`M` 48→96) doubles the cache but bought only ×1.09 |
| fairness-key layout | flat over a 1000× range of key cardinality, at two widths |
| the shared fan-in arrival row | per-sibling rows in its place cut its lock waits 4.4×; throughput did not rise |

Two of those rows answer knobs an operator would otherwise reach for:

- **Spreading fairness keys does not help.** At width 16, medians across 1, 8, 32, 256 and 1024 keys span
  6,622–7,056 steps/s — a 6.5% range, inside the run-to-run spread at a single cardinality. Keys
  interleave different flows; a cohort's siblings share one flow and therefore one key.
- **The shared fan-in arrival row is the largest single wait (~15% of active backends) and still not the
  ceiling.** Giving each sibling its own row instead cuts those waits 4.4× and leaves throughput flat to
  29% *worse* — the extra round trip costs more than the lock time it saves.

**Narrower fan-outs are cheaper.** Width 16 was the best measured; width 64 peaked lower and degraded at
lower load. Per-task time does not buy throughput either: 5 ms plus 10 ms jitter lowered it 7–15% at
fixed in-flight flows, while keeping latency well behaved (p99 ≈ 1.1 s).

### Benchmark against a fresh database, and size disk IOPS to the write rate

Two methodology facts shape every number here.

**Accumulated volume costs a one-time settling, so each configuration is benchmarked against a fresh
database.** A fill matures ~15–20% off its empty baseline and then plateaus
([below](#volume-accumulated-rows-and-bytes-cost-a-one-time-settling)); comparing a fresh-config run
against a long-lived database folds that settling into the result. A fresh database is reproducible to
about ±10%, so only interleaved A/B results — never a cross-time comparison — support a conclusion.

**Write-heavy fan-out is disk-IOPS-bound.** A fan-out fill drives far more IOPS than a linear one
(inserts, a churned index, autovacuum). A disk whose provisioned IOPS are undersized for that rate — a
100 GB PD-SSD gives only ~3,000 (~30/GB) — throttles it, and throughput scatters run to run. Size the
disk's IOPS to the workload's write rate (`dwarf_state_write_bytes`); on an adequately provisioned disk
fan-out throughput is [flat through 20M rows](#volume-accumulated-rows-and-bytes-cost-a-one-time-settling),
a non-event exactly as row count is for linear load. An undersized disk mimics engine defects convincingly
— rule it out first on any new rig.

## Scaling out: more shards

![Two shards vs one: measured scaling factors](benchmark-cloud-scaleout.png)

Two 8-vCPU shards (each its own database instance) at saturation scaled to **×1.81** over a single shard,
and byte throughput scaled ×1.77 / ×1.82 — shards scale both steps and bytes near-linearly at the small
end, for the reason the [write ceiling](#the-single-shard-write-ceiling-is-a-stack-of-locks) section
gives: each instance adds an independent WAL.

![Steps throughput by shard count](benchmark-cloud-shardladder.png)

Further up the ladder, six 8-vCPU shards on a 16-vCPU engine host (n=3 interleaved, fresh database per
run, 256 fairness keys, 5 ms task delay):

| shards | steps/s | replicate range | vs 1 shard |
|---|---|---|---|
| 1 | 2,826 | 2,416–3,043 | — |
| 2 | 5,204 | 5,093–5,409 | ×1.84 |
| 3 | 7,224 | 7,015–7,379 | ×2.56 |
| 4 | 7,750 | 5,836–9,553 | ×2.74 |
| 5 | 10,811 | 8,856–12,450 | ×3.83 |
| 6 | 13,900 | 12,414–14,685 | **×4.92** |

**Sharding keeps paying to at least six shards**, and the gain from five to six (+29%) is the one
arm-to-arm step in that ladder that clears its own noise floor. The single-shard arm's run-to-run spread
is 22% of its mean, which is wider than several of the differences between adjacent arms — so read the
endpoint, not the individual steps.

**⚠️ Multi-shard runs are bimodal.** On a larger rig (six 16-vCPU shards, 32-vCPU engine host) a single
shard reproduces to **1%** while every multi-shard arm swings **20–110%** between otherwise identical
runs — a two-shard arm ranged from ×1.94 down to ×0.92, i.e. worse than one shard.

The mode is readable from the engine's own metrics. Within a run, count each shard's candidate-selection
passes (`dwarf_refill_duration_seconds` count, one series per shard) and take busiest ÷ quietest. Runs
where the passes stay in one band (ratio ~1.5–3×) land in the fast mode; runs where one shard's pass time
diverges (~400 ms against its peers' ~70 ms, ratio 5–10×) land 30–50% lower, with a rank correlation
between imbalance and throughput as strong as −1.0 over five repeats. **Plan multi-shard capacity with
margin, and watch that spread** rather than only the totals.

## Scaling out: more engine replicas

Replicas exist for **availability and engine CPU, not for database throughput** — several engines sharing
one database still share its fixed capacity. So the correct outcome for a replica ladder against one
shard is *flat*, and that is what it measures. R ∈ {1, 2, 4, 8} against one 16-vCPU shard, in-flight
flows held constant, n=3 interleaved:

| replicas | steps/s | replicate range | vs R=1 | p99 |
|---|---|---|---|---|
| 1 | 7,166 | 7,096–7,267 | — | 13.9 s |
| 2 | 6,936 | 6,672–7,168 | ×0.97 | 13.3 s |
| 4 | 6,898 | 6,665–7,068 | ×0.96 | 14.6 s |
| 8 | 6,782 | 6,623–7,095 | ×0.95 | **20.9 s** |

**Eight replicas sharing one database deliver 95% of what one delivers**, against a single-replica
run-to-run spread of 2%. Two replicas across two shards measured 12,829 steps/s, i.e. replicas and shards
compose.

Two things an operator should take from this:

- **The connection budget really is divided.** Each replica opens its share (`M ÷ R`), discovered from
  the database rather than configured, so adding replicas does not over-connect the instance. Connection
  wait time stayed at 0.00 ms at every rung, and the engine's wasted-selection counters *fell* as
  replicas were added rather than rising.
- **The tail degrades before throughput does.** p99 rises ~44% between four and eight replicas while
  throughput moves 1%. If you run many replicas against one shard, alarm on latency, not on steps/s.

**One failure mode to know about: a stale replica registration.** An engine that dies without
deregistering leaves survivors sizing their pools for a peer that no longer exists. The engine ages those
records out, but until it does the fleet under-connects — a benchmark rig that kills engines hard
measures that hole rather than the change it meant to measure.

## Volume: accumulated rows and bytes cost a one-time settling

![Per-step DB time vs accumulated rows and accumulated bytes](benchmark-cloud-volume.png)

Two fills on identical 8-vCPU / 32 GB / 500 GB instances isolate the two ways a database grows, with a
connection-bound probe (`M=8`, the `linear` workload) between fill stages so per-step database time is
read directly as `M ÷ throughput` at every checkpoint:

- **Rows** — the `linear` workload to **100M step rows** (53 GB of small rows: row count grows, bytes stay
  far below RAM per byte of index). Per-step database time rose from 7.3 ms to a ~8.7 ms plateau by ~10M
  rows and **never worsened again** — the 100M checkpoint (8.0 ms) sits within 10% of the empty baseline.
- **Bytes** — the `state` workload with 64 KB payloads to **405 GB** (only 3.7M rows: bytes grow, row
  count stays in a range the rows fill proved cheap). Per-step database time rose from 8.9 ms to a
  ~10.7 ms plateau by ~32 GB and stayed in a 10–11.3 ms band to 405 GB.

Both axes tell one story: a one-time ~15–20% settling as the database matures from empty, then a plateau
— no cliff, no compounding decay. **Fan-out churn behaves the same**: a probe held flat across a
0 / 1M / 5M / 10M / 20M-row ladder on an adequately provisioned disk, its higher IOPS demand the only
thing separating it from linear volume. Three mechanisms were checked directly at every checkpoint:

- **Cache pressure barely materializes.** Buffer-cache hit ratios stayed ≥ 99.7% (index) and ≥ 99.2%
  (heap/TOAST) even at 13× RAM — the access pattern is recency-dominated (active flows touch recent
  rows), so cold volume is evicted harmlessly. The bytes curve shows no break at the `shared_buffers`
  (~11 GB) or RAM (32 GB) boundaries.
- **B-tree depth is a non-event.** Four of the five step-table indexes gained a level during the rows
  fill (verified with `pageinspect`); the probe curve did not move when they did — an extra *cached* page
  per descent is unmeasurable.
- **Bloat is held by autovacuum.** Dead tuples oscillated between 4% and 30% with vacuum cycles, with no
  upward trend at either fill's write rate; probe scatter (±5–7%) tracks the vacuum cycle.

The saturated ceiling settles by the same factor: filling at `M=48` started at ~3,400 steps/s on the
empty database and plateaued at ~2,750 ± 150 from ~20M rows onward. **For capacity planning, derate a
fresh-database ceiling by ~20% for a mature deployment.** Retention (`Purge` / `DeleteOnCompletion`) caps
volume, but these fills show the penalty for carrying history is bounded and flat, not a slow death.

## Self-tuning, measured against hand-tuning

The engine derives its own connection pool and worker count from one declared fact per shard
(`ShardSpec.VirtualCPUs`). Two questions: is the derivation as good as tuning by hand, and does it hold
when tasks are long?

**Short tasks: the derived configuration matches a hand-tuned one.** Three repeat runs of each
configuration, alternating, on a fresh database each (`linear`, 512 in-flight flows, 8-vCPU instance):

| Configuration | steps/s (3 runs) | mean | worker goroutines |
|---|---|---|---|
| Hand-tuned (`SetWorkers(512)`, `SetMaxOpenConns(48)`) | 3454, 3439, 3623 | **3,505** | 512 |
| Derived (`ShardSpec.VirtualCPUs: 8`, nothing else) | 3408, 3379, 3536 | **3,441** | 384 |

Within 2% — and with fewer goroutines, because the connection budget is what actually bounds dispatch.
One declared integer replaces two tuned ones.

**Long tasks: the worker set grows to fit the work in flight.** A single-step workflow whose task sleeps
60 seconds (an LLM call, a human-latency integration), same derived configuration — the resident worker
set is 384, so anything beyond that requires growth:

| in-flight flows | flows/s | `flows ÷ 60 s` | worker goroutines | errors / lease recoveries |
|---|---|---|---|---|
| 500 | 8.3 | 8.3 | 514 | 0 |
| 2,000 | 33.3 | 33.3 | 2,011 | 0 |
| 10,000 | 166.6 | 166.7 | 10,011 | 0 |

Throughput tracks `flows ÷ task duration` exactly at every level: the engine holds 10,000 minutes-long
tasks in flight against a 48-connection pool, and median flow latency is 60.0 s — the task duration
itself, with no queueing.

A worker inside a task holds no database connection, so worker count and database load are separate
quantities. The engine adds a worker when no existing worker is free, work is waiting, **and** the shard
still has capacity to admit database work. The last condition is what keeps growth from becoming
connection contention: a database-bound backlog has no spare admission capacity, so it triggers no
growth. No part of the rule refers to task duration, which is what lets one engine serve 5 ms lookups and
30 s model calls on the same worker set.

The worker *maximum* is derived, not a round number: it is the largest count that keeps a synchronized
completion storm — every in-flight task unblocking at once when a downstream recovers — inside the
crash-recovery lease margin, computed from the pool size and the measured round-trip time. On the
reference configuration it is in the tens of thousands. The cost of a parked worker is a goroutine, a
stack and its in-flight state map, so **memory is the reason to cap it**, via `SetWorkers(n)`.

## The sizing formula

Inputs: `V` = the shard database's vCPU count, `L` = round-trip time to the shard, `exec` = mean task
time, `Y` = the database's `max_connections` setting, `R` = the number of engine replicas. `headroom` is a
small reserve of connections left for monitoring and other clients.

```
ratio = 6 if V < 32 else 12         # conservative below 32 vCPUs, at the knee above
M     = min(Y − headroom, ratio·V) ÷ R    # connections per replica (R is read from the shared databases)
db    = k·L + s ≈ 11·L + 4.4ms      # per-step database time
T     = db + exec                   # per-step worker time
N     = M × T/db                    # workers actually doing database work
ceiling ≈ min(N/T, M/db, C_db)      # steps/s, where C_db is the instance's own ceiling
```

Worked example — an 8-vCPU shard, same-zone (`L` = 0.5 ms), 50 ms tasks: `M` = 6×8 = 48 connections;
`db` ≈ 11×0.5 + 4.4 ≈ 9.9 ms; `T` ≈ 60 ms; `N` = 48 × 6.1 ≈ 290 workers. Predicted ceiling ≈ 3,700
steps/s, which is the 8-vCPU instance's own ceiling — the database binds, as designed.

### How many engine hosts, and how much database behind them

The formula above sizes one shard. Two further rules size the *engine* fleet:

```
engine steps/s   ≈ steps/core × engine vCPUs × utilisation
plan limit       ≈ 10,000 steps/s per 4 vCPU engine     # do not exceed; add replicas past this
database vCPUs   ≈ 6 × engine vCPUs                     # at a ~70% database utilisation target
```

**`steps/core` must be measured on your own host class, with the engine host saturated.** It held within
2,600–2,980 across shard counts, pool sizes, fan-out widths, key layouts and volumes on the 4-vCPU host
that produced the fan-out numbers above, and is identical there for linear and fan-out work — that
stability is why the rule is expressed in these terms. But it is a property of the CPU, and a reading
taken while the *database* is the constraint is not a capacity number at all: the same metric reads
~950–1,250 on runs where the engine host was only lightly loaded.

**The database ratio is only meaningful together with the utilisation it assumes.** The measured point was
9,400 steps/s from a 4 vCPU engine (79% busy) against three 8-vCPU shards running ~70% CPU. Sizing so the
database *stays* near 70% — the sane target, since a database at 100% has nothing left for autovacuum,
checkpoints or a traffic spike — gives **1 engine vCPU : 6 database vCPUs**. If you were instead willing
to drive the database to saturation the same measurement reads as ~1:4. Prefer 1:6: an under-provisioned
engine merely queues, while an over-driven database degrades non-linearly.

**The engine applies most of this automatically**: provide `ShardSpec.VirtualCPUs` and it derives each
shard's connection pool (divided by the replica count it reads live from the shared databases — nothing
to declare), its capacity-proportional share of new-flow placement, and the worker set it grows on
demand. `SetWorkers` and `SetMaxOpenConns` survive as expert overrides for tests, benchmark sweeps,
memory-bounded hosts, and externally-constrained connection budgets.

## Known gaps

- **The top of vertical scaling is not found.** The 32 and 64-vCPU peaks were reached with the database
  at 49% and 24% CPU — they are lower bounds set by the load generator, and raising them needs an engine
  fleet, not a bigger database.
- **Multi-shard throughput is bimodal.** Selection-cycle imbalance predicts the mode; what makes one
  shard's cycle diverge is unresolved. Quote multi-shard numbers with their replicate range.
- **Replica scaling is measured against one shard only, with replicas as engines inside one process.** No
  per-replica memory figure, and no hard-kill test of the stale-registration path.
- **Worker growth is not measured on cloud hardware in the 0.5–10 s task band.** The 60 s end and the
  no-op end both are.
- **Volume was measured under accumulation, not steady-state retention.** A create-and-purge equilibrium
  exercises autovacuum differently, and slow drift (memory, goroutines, connection recycling) needs a
  soak test rather than a 60 s window.
- **The ~9,400 steps/s fan-out ceiling on a 4-vCPU engine is bounded but not attributed** — every
  candidate in the table above is eliminated.
- **The 1→2 vCPU database step scales poorly** (suspected WAL-insert-lock serialization) and **1-vCPU
  variance is ±40%**; treat that tier as indicative.
- Absolute numbers are one cloud, one dialect (PostgreSQL — the fastest per the
  [in-repo benchmarks](benchmark.md)), one region; ratios and shapes are the durable findings.

## Reproducing

```sh
# Provision on GCP (knobs documented in bench/gcp/provision.sh), then:
GOOS=linux GOARCH=arm64 go build -o dwarf-bench ./bench

# Capacity: open-loop at a commanded arrival rate, the method every ceiling here uses.
./dwarf-bench -dsn 'postgres://USER:PASS@PRIVATE_IP:5432/dwarf?sslmode=disable' \
  -workload linear -vcpus 16 -open-loop -arrival-rate 600 \
  -window 60s -warmup 20s -label ladder-r600

# Sweep the commanded rate to find where achieved throughput leaves the diagonal; that is the ceiling.
# Pin the pool explicitly only when the pool itself is the variable:
#   -max-open-conns 192
# Fresh database per configuration; never reuse tables. Tear down with bench/gcp/teardown.sh.
```
