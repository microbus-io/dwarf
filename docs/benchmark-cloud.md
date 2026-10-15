# Cloud benchmarks

> **For operators** sizing a deployment. This page answers how much one database serves, what adding
> shards and engine replicas does, and what the engine itself costs. Every number was measured, and the
> configuration that produced it is stated beside it. The database volume section quotes measurements from
> July 2026 while it is being remeasured, and says so.

## The short version

- **One 32 vCPU PostgreSQL database runs ~19,000 steps/s**, about 1.6 billion steps a day, at the
  engine's shipped defaults.
- **Plan at ~90% of that and latency stays low.** Offered loads up to 99% of saturation all drained, with a
  p99 flow latency of 0.6-1.3 s for a 10-step flow.
- **Shards add throughput linearly.** Four shards ran 69,400–74,900 steps/s, 3.9–4.2 times one shard.
- **Replicas add availability, not cost.** One, two or four replicas on a shard deliver the same
  throughput within 6%. Each shard is served by two of them, and any others stand by to take over.
- **Pair each database with two engines at half its vCPU.** 8 vCPU with 2 × 2 vCPU engines runs ~4,000
  steps/s, 16 vCPU with 2 × 4 runs ~7,700, 32 vCPU with 2 × 8 runs ~18,700. The 32 vCPU pairing costs
  the least per step: about $0.05 per million steps at full load.
- **The engine is cheap.** A shard's work takes about 5.5 engine cores. Two 8 vCPU engine hosts per
  32 vCPU database serve it at about a third of their CPU each, and either can carry it alone.
- **Long tasks cost no throughput.** With 8-second tasks the engine delivered the full offered rate, and
  each step's hand-off added under 10 ms. 10,000 concurrent 60-second tasks ran on under one engine core.
- **Failover takes about 40 seconds and loses nothing.** With every dispatcher killed at once, dispatch
  paused for ~40 s, standby replicas took over, and every flow completed.
- **Keep the engine near its database.** A step makes about five round trips, so distance costs throughput;
  the engine opens more connections as distance grows, which recovers most of it out to 1 ms.
- **One large engine host can serve several shards.** One 48 vCPU engine ran four 32 vCPU shards at their
  databases' pace on 20 of its cores. Size its CPU from what the shards can deliver, and on a large host cap
  `GOMAXPROCS` to that: garbage collection takes about twice the share of CPU on 48 vCPU as on 2–16.

[Sizing a deployment](#sizing-a-deployment) turns these into a recipe.

## How these numbers were measured

| | |
|---|---|
| Cloud | Google Cloud, one zone; engine-to-database round trip ~0.06 ms |
| Database | Cloud SQL for PostgreSQL 16, **32 vCPU**, 128 GB RAM, 1 TB SSD, default `max_connections` (1,000), unless a section says otherwise |
| Engine host | c4a-standard-16 (16 vCPU Arm), one replica per host, unless a section says otherwise |
| Engine configuration | Shipped defaults: no pool size, no `MaxConnections`, no `Weight` |
| Workload | `linear`: a 10-step chain of no-op tasks, no payload. It isolates the engine and database cost per step. |
| Load | Saturated: flows are created as fast as the backlog allows, up to **2,000 in flight per shard** |
| Window | 2 minutes of warmup, then a **16-minute measurement window**. Throughput is the window mean. |
| Database | A fresh database for every run |
| Outcome | No flow errors in any run |

**Steps/s** counts task executions. A 10-step flow at 1,000 steps/s is 100 flows/s.

**A shard is a separate database server**, not a separate database on a shared server: shards share
nothing, so each brings its own write capacity ([measured below](#what-limits-one-database)).

**Absolute numbers belong to the instance that produced them; ratios travel.** Cloud instances of the
same spec differ by up to ~20%. The runs below span several sessions, engine builds and rig setups, each
session on its own set of instances. A comparison is made within one session, on one build and one setup,
unless a section says otherwise, and each section dates its runs.

## One database

On one 32 vCPU database, one engine replica ran **19,154–19,868 steps/s** over the 16-minute window: three runs
on two instances, about 1,950 flows/s and 1.6 billion steps a day. Each run starts near 21,000 steps/s and
settles to 18,400–19,200 within the window ([minute by minute](#minute-by-minute)). *(Measured 2026-10-14.)*

Earlier engines measured 17,507–19,234 on other instances (2026-10-08/09). The spread between instances of one
size is larger than the difference between engines.

## Database size, with its engine pairing

![Database size and price-performance](benchmark-cloud-tiers.png)

Each database size paired with two engine replicas at half its vCPU, one shard, saturated:

| Database | Engines | steps/s | Engine CPU per replica | Monthly cost | Steps a month | $ per million steps |
|---|---|---|---|---|---|---|
| 8 vCPU, 32 GB | 2 × 2 vCPU | 4,041 | 0.7 of 2 cores | ~$700 | 10.6 billion | $0.0661 |
| 16 vCPU, 64 GB | 2 × 4 vCPU | 7,675 | 1.3 of 4 cores | ~$1,240 | 20.2 billion | $0.0613 |
| 32 vCPU, 128 GB | 2 × 8 vCPU | 18,720 | 2.3–3.0 of 8 cores | ~$2,310 | 49.2 billion | **$0.0469** |

The 32 vCPU row is the per-shard rate of the four-shard, eight-replica run below. Monthly cost is the
on-demand list price in one US region: the database (without high availability), its 1 TB SSD, and both
engine hosts. Every database ran on a 1 TB disk so storage IOPS would not limit it; a smaller disk costs less
and can bind first. The cost per million steps assumes the deployment runs at its measured rate around the
clock (730 hours a month); one that averages half that rate pays twice as much per step.

- **Throughput roughly doubles with each doubling of the database**, and more than doubles from 16 to 32
  vCPU, which makes 32 vCPU the lowest cost per step of the three.
- **The pairing holds at every size.** Each engine runs at about a third of its CPU, so either replica can
  carry its shard alone if the other fails.
- **The engine's connection budget is the same at every size.** Cloud SQL sets `max_connections` to 600, 800
  and 1,000 for these sizes, and the engine opens the same 250 connections per shard on each.

**Past 32 vCPU, add shards rather than a bigger database.** A 64 vCPU database ran **25,366 steps/s**
against 20,863 for a 32 vCPU database in the same session: +22% for 1.9 times the price. It needed
`MaxConnections: 500` to get there (+14% over the default 250), because Cloud SQL's `max_connections` stops
growing at 1,000. Its CPU sat at 33-59%: it was bound by writing the write-ahead log, not by compute. A
larger disk did not help either: the same 64 vCPU instance ran 28,491 steps/s on 1 TB and 28,162 on 4 TB.
*(Measured 2026-10-08, one engine host, 250 or 500 connections.)*

## What limits one database

A saturated database is limited by its **write-ahead log (WAL)**, not by CPU or disk. On a saturated 32 vCPU
shard, ~140 backends wait on the WAL write lock at once. A deeper look at a 16 vCPU instance (July 2026)
showed why buying around it does not work: the ceiling is a stack of locks.

| connections | steps/s | `LWLock/WALWrite` waiters | `LWLock/BufferContent` | on CPU | `IO/WALSync` |
|---|---|---|---|---|---|
| 96 | 5,355 | 33 | 0.1 | 1.5 | 0.8 |
| 192 | 7,016 | 70 | 2.6 | 7.2 | ~0 |
| 384 | ~6,400 | 99 | 76 | 24 | ~0 |

- **The wall is the WAL write lock**, which serializes writes into the log. Every step's writes funnel
  through it. Not CPU: at the peak only 7.2 of 16 cores were doing work. Not disk: WAL sync waits were ~0.
- **Removing the top of the stack exposes the next one about 5% higher.** Turning off `synchronous_commit`
  eliminated WAL-write waits and bought **+4%**, because buffer-page contention more than doubled.
  `wal_compression` cost 5%: it reduces WAL bytes, and bytes are not what is scarce.

**So a shard must be its own server.** A PostgreSQL server has one WAL, shared by every database on it.
Same total connections, measured directly:

| topology | steps/s | vs one database on one server |
|---|---|---|
| 2 databases, **one** server | 6,916 | ×1.03 |
| 2 databases, **two** servers | 13,995 | **×2.08** |

*(Measured July 2026 on 16 vCPU instances; these are properties of PostgreSQL, not of the engine.)*

## Shards: throughput adds up

![Throughput by shard count and fleet shape](benchmark-cloud-shards.png)

Each shard is its own 32 vCPU database.

| Shards | Replicas | Engine hosts | steps/s | vs 1 shard | Per shard |
|---|---|---|---|---|---|
| 1 | 1 | 16 vCPU | 17,938 | ×1.00 | 17,938 |
| 2 | 2 | 16 vCPU | 34,115 | ×1.90 | 17,058 |
| 4 | 4 | 16 vCPU | 69,382 | **×3.87** | 17,346 |
| 4 | 8 | 8 vCPU | 74,879 | **×4.17** | 18,720 |

- **Every shard carries what one shard carries alone.** At four shards, each held 17,300–18,700 steps/s,
  within the spread of a single shard. The two four-shard runs used different instances, so the 8%
  between them is instance variance, not the fleet's shape.
- **New flows spread evenly.** Each shard received an even share of new flows (1,680–1,790 flows/s per
  shard with four replicas).

## One engine for several shards

The same shard counts, served by a single replica:

| Shards | Engine host | steps/s | Engine CPU | Databases |
|---|---|---|---|---|
| 2 | 16 vCPU | 32,564 | 10.3 of 16 cores | saturated |
| 2 | 48 vCPU | 34,049 | 14.8 of 48 cores | saturated |
| 4 | 48 vCPU | **66,843 (×3.73)** | 20.0 of 48 cores | slowest saturated |

- **One engine serves two shards at full speed**, even on a 16 vCPU host, with a third of its CPU to
  spare.
- **One engine runs four shards at their databases' pace.** It delivered 66,843 steps/s, steady from
  minute to minute, with 28 of its 48 cores idle. Four replicas on four shards ran 69,382 and eight ran 74,879,
  each on its own set of instances; the gap is within the spread between instances of one size.
- **The slowest database set every shard's pace in this run.** All four ran at 16,700 steps/s, because
  the load generator places flows evenly and caps flows in flight across all shards: the slowest shard's
  flows stay in flight longest and fill that shared cap. Shards whose databases ran faster sat with free
  capacity. The same holds for any caller that limits its own concurrency across shards.

*(The four-shard row measured 2026-10-12; the two-shard rows 2026-10-10, in a separate session.)*

## Replicas: availability without a throughput cost

![Replicas on one shard](benchmark-cloud-replicas.png)

One 32 vCPU shard, served by one, two and four replicas on separate hosts:

| Replicas | steps/s | vs 1 replica | Executing replicas | Engine CPU per executing replica |
|---|---|---|---|---|
| 1 | 17,938 | — | 1 | 5.4 cores |
| 2 | 16,805 | −6.3% | 2 (8,404 + 8,402) | 2.7 cores |
| 4 | 17,042 | −5.0% | 2 (8,526 + 8,516) | 2.6 cores |

- **Throughput does not depend on the replica count.** Two and four replicas deliver the same
  throughput, within 6% of one replica. That is inside the spread between runs on one instance.
- **Each shard has two dispatchers.** As soon as there are two replicas, two of them split each
  shard's work evenly (to within 0.1% minute by minute). Other replicas create flows and answer reads,
  but run none of that shard's steps. They hold a few connections each, and step in if a dispatcher stops.
  [Deployment](deployment.md) describes how the fleet chooses dispatchers and what happens when one fails.
- **Across shards, every replica gets work.** Different shards pick different dispatchers. At four
  shards and four replicas, every replica dispatched exactly two shards; at four shards and eight, every
  replica dispatched exactly one.

## The recommended shape

**Two 8 vCPU engine replicas for each 32 vCPU database.** With `S` shards, that is `2S` replicas, and each
replica dispatches exactly one shard.

Measured at four shards and eight c4a-standard-8 replicas: **74,879 steps/s**. Each replica carried
8,400–9,800 steps/s on 2.3–3.0 of its 8 cores. If one replica of a shard fails, the other takes over its
share: about 5.5 cores, which still fits an 8 vCPU host.

| Replicas | Shards each replica dispatches | Engine host | Engine CPU used |
|---|---|---|---|
| `2S` | 1 | 8 vCPU | 2.3–3.0 cores |
| `S` | 2 | 16 vCPU | 5.3–5.6 cores |

Choose `S` or `2S` replicas so every replica carries the same share. Replicas beyond `2S` add standby
capacity and capacity for your own traffic, not workflow throughput.

The same rule scales down: two engines at half the database's vCPU (see
[Database size](#database-size-with-its-engine-pairing)).

## The engine is cheap

![Engine host size](benchmark-cloud-engine-hosts.png)

| Engine host | Runs | steps/s per engine core | Garbage collection, share of engine CPU |
|---|---|---|---|
| 2 vCPU | 2 replicas on an 8 vCPU database | 2,850–2,950 | 10–11% |
| 4 vCPU | 2 replicas on a 16 vCPU database | 2,950–3,000 | 10–11% |
| 8 vCPU | 8 replicas on 4 shards | 3,250–3,600 | 11–15% |
| 16 vCPU | 1 or 2 shards per replica | 3,150–3,300 | 11–12% |
| 48 vCPU | 1 replica on 4 shards | 3,350 | 21% |

- **The database is the limit.** An engine host from 2 to 16 vCPU runs at about a third of its CPU while
  its database is saturated, and a 48 vCPU host serving four shards at under half.
- **Steps per core hardly depend on the host's size; garbage collection's share does.** On a 48 vCPU host
  the engine's garbage collection takes about twice the share of CPU it takes on 2 to 16 vCPU. Go sizes
  its runtime to every core it may use, and the collector's cost grows with that count, so on a large
  host set `GOMAXPROCS` to the cores the engine needs rather than the whole host (see
  [Sizing a deployment](#sizing-a-deployment)). Raising `GOGC` above its default of 100 makes the
  collector run less often at the price of memory: in July 2026, `GOGC=400` on a 48 vCPU host cut
  garbage collection from about a fifth of the engine's CPU to about a twentieth. It has not been
  remeasured since.

These figures are the engine's own cost. Your tasks run on the same hosts and need their own headroom.

## Minute by minute

![Minute by minute, per shard](benchmark-cloud-minutes.png)

*The vertical axis starts at 14,000 steps/s.*

Every run starts near 19,000 steps/s per shard and settles **8–13% lower** within its window. The
settling is the same per shard with one, two or four shards and with one, two, four or eight replicas,
so it belongs to the database, not to the engine or the fleet's shape. Plan capacity on the settled rate.

On the current engine (2026-10-14, three runs on two instances) a run starts near 21,000 and settles 7–9% lower,
to ~19,000, between minutes 3 and 7. The step does not come from the write-ahead log's size: with Cloud SQL's
`max_wal_size` raised from its 1.5 GB default to 16 GB, the run settled the same way, and its mean was within
run-to-run noise (20,269 against 19,579 and 19,868 at the default).

## Planning headroom and latency

Saturated runs hold a backlog of 2,000 flows per shard, so their latency is mostly time spent waiting in that
backlog: on 32 vCPU databases a 10-step flow completed in a median of **~1.2 s**, with a p99 of
**1.8–2.3 s**. A smaller database works through the same backlog more slowly: the p99 was ~3.5 s at
16 vCPU and ~8.2 s at 8 vCPU. That is the latency of a database driven flat out.

A deployment planned below saturation sees much less. Offered at fixed rates up to 99% of the same
instance's saturated mean (18,900 steps/s), every rate drained and the series stayed flat for 16 minutes:

| offered steps/s | % of saturated | delivered | flow p50 | flow p99 |
|---|---|---|---|---|
| 16,000 | 85% | 15,843 | 57 ms | 0.9 s |
| 17,000 | 90% | 16,808 | 70 ms | 0.6 s |
| 17,900 | 95% | 17,627 | 69 ms | 1.3 s |
| 18,800 | 99% | 18,694 | 90 ms | 0.7 s |

- **The sustainable rate is the saturated rate.** There is no stall zone below saturation, so planning does
  not need a large safety factor for stability.
- **Leave headroom for catch-up, not for stability.** A backlog left by a failover or a traffic spike
  clears only as fast as the spare capacity allows ([Failover](#failover)). Planning at ~90% of the
  settled rate leaves room for that.

*(Measured 2026-10-08 on one 32 vCPU instance, with the same connection budget.)*

## Long tasks

A task that waits on something slow, such as a model call or a human, ties up the flow but not the
database. These runs check that the engine keeps the offered rate as tasks lengthen, and that a waiting
task costs little.

**Seconds-long tasks at a fixed rate.** 1,000 flows/s of the 10-step `linear` workflow, about 10,000
steps/s or half of what the database runs saturated, with every task sleeping 0, 1 or 8 seconds. One
32 vCPU database, one 16 vCPU engine host, a 6-minute window after 2 minutes of warmup:

| Task duration | flows/s | steps/s | Flow p50 | Flow p99 | Hand-off per step | Flows in flight | Engine CPU | `Create` p99 |
|---|---|---|---|---|---|---|---|---|
| 0 s | 999 | 9,988 | 0.045 s | 0.37 s | ~5 ms | ~750 | 2.7 cores | 29 ms |
| 1 s | 999 | 9,987 | 10.06 s | 10.47 s | ~6 ms | ~10,500 | 3.0 cores | 68 ms |
| 8 s | 997 | 9,970 | 80.09 s | 80.42 s | ~9 ms | ~80,250 | 3.2 cores | 49 ms |

*Hand-off per step is the flow p50, less ten task durations, divided by the ten steps.*

- **Task duration drops out of throughput.** Every run delivered the offered rate, and a flow took ten
  times its task duration plus a few milliseconds per step.
- **The database does the same work at every duration.** It committed ~31,000 transactions/s in all
  three runs. A task in progress holds no database connection.
- **Creating a flow stays fast.** `Create` p99 stayed between 29 and 68 ms at every duration, while the
  engine held up to 80,000 flows in flight.
- **Memory follows flows in flight.** Holding ~80,000 flows, the engine's heap peaked at ~1.2 GB, about
  15 KB per flow. `SetWorkers` caps the number of tasks in progress on a replica if memory must be bounded.

**Minute-long tasks.** One 60-second task per flow, the shape of a workflow that waits on a model call,
with a fixed number of flows in flight:

| Flows in flight | flows/s | In flight ÷ 60 s | Flow p50 | Flow p99 | Worker goroutines | Engine CPU |
|---|---|---|---|---|---|---|
| 500 | 8.3 | 8.3 | 60.008 s | 60.044 s | 502 | 0.04 cores |
| 2,000 | 33.3 | 33.3 | 60.011 s | 60.110 s | 2,002 | 0.14 cores |
| 10,000 | 166.7 | 166.7 | 60.013 s | 60.276 s | 10,004 | 0.74 cores |

- **Throughput is the work in flight divided by the task duration,** exactly, at every level, and a
  flow takes as long as its task and no longer.
- **The engine grows its workers to fit the work.** It ran one worker per task in progress and needed
  under one core to hold 10,000 of them.

No flow errors in any of the six runs.

## Failover

![Failover: dispatchers killed 1, 2, 3, then 4 at a time](benchmark-cloud-failover.png)

Two shards, each a 32 vCPU database, served by eight 8 vCPU engine replicas: four dispatchers (two per
shard) and four standbys. The offered load is fixed at 2,800 flows/s, 28,000 steps/s, about 80% of what
the two shards run saturated. At window minutes 2, 6, 10 and 14, 1, 2, 3 and then 4 of the current
dispatchers are killed at once (`SIGKILL`), and each killed engine is restarted ~8 s later, as an
orchestrator would. Admission stops at minute 18.

| Kill | Dispatch lost | Throughput during the outage | Back to full dispatch |
|---|---|---|---|
| 1 dispatcher | half of shard 1 | 22,000–26,500 steps/s | ~30 s |
| 2 dispatchers | all of shard 1 | 14,000–16,000 steps/s (shard 2 unaffected) | ~40 s |
| 3 dispatchers | all of shard 1, half of shard 2 | 8,500–13,000 steps/s | ~40 s |
| 4 dispatchers | every shard | 0 | ~40 s |

- **No flow is lost.** All 3,334,099 flows completed. None failed, none was left running, and no
  replica records were left behind.
- **A dead engine is detected in about 40 seconds**, and a standby takes its place. A shard that keeps
  one dispatcher keeps running at that dispatcher's share; a shard that loses both pauses until the
  standbys take over.
- **A restarted engine rejoins as a standby.** Dispatch roles go to the longest-running replicas, so a
  loss on one shard can move roles on the other. Those moves cost no throughput.
- **Flows keep arriving during an outage, and headroom sets the catch-up.** After each takeover the
  fleet ran 30,000–33,000 steps/s, above the offered rate, until the backlog cleared: about 2 minutes
  after losing one shard's dispatch. After losing all of them, ~107,000 flows were still waiting when
  admission stopped 4 minutes later, and they completed within 3 minutes. A fleet with more headroom
  catches up faster.

## Network distance

A step makes **about five database round trips** on PostgreSQL, and each holds a connection while it
travels, so the time to the database is paid five times per step. Measured with a deliberately small pool
of 8 connections, the time a step holds a connection is **5.19 × round trip + 7.88 ms**.

The engine compensates by opening more connections as distance grows: **250 per shard up to 0.25 ms,
rising linearly to 500 at 1 ms** and beyond (clamped to half the server's `max_connections`). Measured at
0.25 and 1 ms, steps/s, with the connections per shard in parentheses:

| Database | 0.25 ms | 1 ms, same connections | 1 ms, twice the connections |
|---|---|---|---|
| 16 vCPU | 8,484 (200) | 8,006 (200), −6% | 8,373 (400), −1% |
| 32 vCPU | 20,863 (250) | 19,363 (250), −7% | — |
| 64 vCPU | 22,301 (250) | 19,567 (250), −12% | 24,254 (500), +9% |

- **Out to 1 ms, twice the connections recover what distance costs**, which is what the ramp to 500 is for.
  (A 64 vCPU database gains from 500 connections even nearby.)
- **Past 1 ms, co-locate.** Each further millisecond costs about five per step, and the budget does not keep
  growing. A cross-zone hop on Google Cloud is ~1.1 ms; keep the engine in its database's zone.

*(Measured 2026-10-07/08 with network delay added on the engine host, with the same connection budget.)*

## How the connection budget was chosen

The budget of 250 connections per shard is measured, not guessed.

- **More connections keep helping, and do not collapse.** On a 16 vCPU database at 0.25 ms, pinned pools of
  59, 88, 192 and 384 connections ran 5,478, 6,647, 7,802 and **8,185** steps/s. The gain comes from
  group commit: more committers per WAL flush (22 → 70). The run at 384 was the steadiest of the set.
- **A budget of 125-250 connections beat a smaller pool sized per vCPU at every size:**
  +42% at 4 vCPU, +7% at 8, +15% at 16, +10% at 32 and +10% at 64 vCPU. A 4 vCPU database ran stable at
  250 connections, so one constant serves every size.
- **A constant, clamped to the server.** Half of `max_connections` is the ceiling, so a small or tightly
  configured server is never handed all its connections. Two dispatchers per shard split the budget.
- **`MaxConnections` is the lever.** A 64 vCPU database wants ~500 even nearby (+14%); a database shared
  with other work wants less. See [deployment](deployment.md#connection-pool).

*(Measured 2026-10-07/08, saturated, 16-minute windows, one run per setting.)*

## Sizing a deployment

1. **Shards.** Plan each 32 vCPU database at **~16,000 steps/s**: its settled rate (~17,000) less room to
   catch up after a failover or a spike. `shards = peak steps/s ÷ 16,000`, rounded up. Below one shard's
   worth, a 16 vCPU database (~7,000 settled) or an 8 vCPU database (~3,600) costs less, at a higher price
   per step.
2. **Engine replicas.** Two per shard at half the database's vCPU: `2S` replicas of 8 vCPU for 32 vCPU
   shards. Each runs at about a third of its CPU and can carry its shard alone. A larger host can serve
   several shards instead: give its engine about one core per 3,300 steps/s its shards can deliver at
   most - four 32 vCPU shards, ~20–24 cores - and set `GOMAXPROCS` to that plus whatever your tasks use
   in the same process, rather than leaving it at the host's full count. Run at least two such hosts, so
   one can fail.
3. **Your tasks.** Add the CPU and memory your tasks use on the engine hosts. The engine itself holds ~15 KB
   per flow in flight, plus the state of each step it is running - large payloads need far more
   ([below](#workload-shape-and-payload)); with long tasks, flows in flight = flows/s × flow duration.
4. **Distance.** Put the engine in its database's zone. Up to 1 ms the budget compensates; past it, plan for
   less per shard.
5. **Disk.** Size each database's disk for IOPS, not only capacity (see [method notes](#method-notes)).

**Worked example.** 50,000 steps/s at peak (5,000 flows/s of 10-step flows): 4 × 32 vCPU shards and
8 × 8 vCPU engine replicas, about $9,200 a month at on-demand list price before your tasks' own cost.

## Workload shape and payload

Three workflow shapes on one 32 vCPU database, at payloads from 0 to 1 MB. Every flow has 10 steps:

- **Linear:** a chain of 10 tasks, each writing a fresh payload of the given size to the flow's state.
- **Fan-out:** a task splits into 8 parallel branches that join again. Every step, branches included, writes a
  fresh payload: the same per-step write as linear, so the two differ only in shape.
- **Scatter:** the first task writes an array of 8 elements of the given size. Each of the 8 branches reads its
  own element and writes nothing, and the join does nothing. This is the shape of a document split into pages.

Payloads are incompressible. One run per cell, saturated, 16-minute window.

| Payload | Linear, steps/s | Fan-out, steps/s | Scatter, steps/s |
|---|---|---|---|
| none | 20,352 | 20,961 | — |
| 1 KB | 15,901 | 16,093 | 19,443 |
| 4 KB | 15,635 | 15,945 | 14,990 |
| 64 KB | 3,445 | 2,958 | 2,392 |
| 256 KB | — | — | 655 |
| 1 MB | 271 | 249 | 176 |

*These runs come from one session of their own (see [how these numbers were measured](#how-these-numbers-were-measured)),
except scatter at 1 MB, measured 2026-10-14 in a later session. The no-payload rows run ~13% above the
[headline](#one-database) for the same workload, so compare rows within this table.*

**Small payloads, on the current engine.** The 1 KB and 4 KB rows above were measured on an earlier engine, which
wrote small fields more often than it needed to. Remeasured `linear` in one session (2026-10-14):

| Payload | steps/s | vs no payload | Last 5 minutes |
|---|---|---|---|
| none | 19,154 | — | 18,400 |
| 1 KB | 17,543 | −8% | 17,100 |
| 2 KB | 17,021 | −11% | 16,500 |
| 4 KB | 15,387 | −20% | 14,800 |

**Throughput falls with payload in two regimes:**

- **Up to a few KB per step, rows are the limit.** 1 KB costs 8%, and each further KB about 4%.
- **From ~64 KB up, bytes are the limit:** about 230–280 MB/s of payload per 32 vCPU database. Plan large-payload
  throughput as steps/s ≈ 250 MB/s ÷ payload per step: 64 KB gives ~3,900 (3,445 measured), 1 MB gives ~240
  (271 measured).

Across the shapes:

- **The byte ceiling by shape.** Linear wrote ~226 MB/s of payload at 64 KB and ~284 MB/s at
  1 MB, and fan-out ~194 and ~261 MB/s. Scatter writes each array once, so its rate is lower: ~125 MB/s at
  64 KB and ~137 MB/s at 256 KB.
- **Little is written beyond the payload.** At 64 KB and 1 MB, the write-ahead log grew by 1.2–1.3 times the
  payload, and stored data by about 1.15 times.
- **Shape matters little.** Fan-out ran within 2% of linear up to 4 KB, and 8–14% below it at 64 KB and 1 MB.
- **Scatter branches read only their own element.** At 1 KB per element, the engine read about the same
  ~2.2 KB per branch with 64 branches as with 8. Widening the fan-out from 8 to 64 branches cost 20% of
  steps/s (15,592 vs 19,443).
- **Deeper scatter branches cost nothing extra.** 4 branches two steps deep ran 19,033 steps/s at 1 KB.
- **Many fairness keys cost nothing on fan-out.** Spreading flows over 512 fairness keys instead of one ran
  22,454 steps/s against 20,961.
- **The database is the limit at every payload.** The engine host stayed at 25–41% of its CPU in every run.

**Engine memory with large payloads.** The engine keeps recently written and read large state fields in memory,
so the steps that read them next do not go back to the database. That cache, not the work in progress, is most
of the engine's memory with large payloads. State inside running tasks never exceeded 25 MB.

The cache is sized in bytes: by default a tenth of the memory the engine's process may use, `GOMEMLIMIT` when it
is set ([`SetStateCacheSize`](deployment.md) chooses another size). Scatter on one 32 vCPU database and a 64 GB
engine host, with the cache at several sizes:

| Workload | Cache | steps/s | Engine heap, mean | Engine heap, peak |
|---|---|---|---|---|
| 8 × 1 MB | ~15.5 GB (a quarter of memory) | 176 | 32.8 GB | 50.1 GB |
| 8 × 1 MB, `GOMEMLIMIT=48GiB` | 12 GB (a quarter of the limit) | 178 | 27.0 GB | 44.1 GB |
| 8 × 256 KB | ~15.5 GB (a quarter of memory) | 670 | 26.6 GB | 36.9 GB |
| 8 × 256 KB | 1 GB | 649 | 3.7 GB | 7.0 GB |

- **A cache smaller than the work in flight costs little.** At 256 KB, a 1 GB cache instead of 15.5 GB cost 3%
  of throughput and cut the heap about seven-fold. A miss reads the field from the database again, which is
  cheap next to the payload writes that set the pace. The default is a tenth of memory for that reason.
- **Set `GOMEMLIMIT` on engine hosts that carry large payloads.** Go's garbage collector lets the heap reach about
  twice what it holds; the limit bounds the whole process, and the cache is sized from it.

*(Measured 2026-10-14.)*

## Database volume

*Being remeasured: throughput as a 32 vCPU database grows to ~200 million step rows and to 512 GB. Until
then, measurements on 8 vCPU databases with 500 GB disks:*

- **Growth costs a one-time settling of ~15–20%, then nothing more.** Filling to **100 million step rows**,
  the time a step holds a connection rose 7.3 → ~8.7 ms by ~10 million rows and then stayed flat. Filling to
  **405 GB** with 64 KB payloads, it rose 8.9 → ~10.7 ms by ~32 GB and stayed in a 10–11.3 ms band.
- **Why it flattens.** Buffer-cache hit ratios stayed ≥ 99.2% even at 13 times the RAM, because active flows
  touch recent rows; extra index levels made no measurable difference; autovacuum held dead rows between 4%
  and 30% with no upward trend.
- **For capacity planning, derate a fresh database's ceiling by ~20%** for a mature deployment.
  Retention (`Purge`, `DeleteOnCompletion`) caps volume.

*(Measured July 2026.)*

## Method notes

- **Use a fresh database for every configuration.** Accumulated volume settles a database by ~15–20%, so
  comparing a fresh run with a long-lived one folds that settling into the result.
- **Size disk IOPS to the write rate.** Cloud SQL provisions ~30 IOPS per GB, so a 100 GB disk gives ~3,000
  IOPS (~23 MB/s of random writes) and a write-heavy workload measures the disk. Every run here used 1 TB.
- **Use incompressible payloads.** PostgreSQL compresses large values, and repetitive test payloads measured
  ~3 times higher than real ones.
- **Record the round trip beside every throughput number**, and compare runs only within one instance:
  same-spec instances differ by up to ~20%, and placement within a zone varies the round trip.

## Known gaps

- **A capped `GOMAXPROCS` on a large host** is not measured: how much engine CPU it saves, and where the cap
  starts to cost throughput.
- **Only PostgreSQL on Google Cloud is measured here.** MySQL and SQL Server ceilings at the default budget
  are not, nor other clouds or regions.
- **Mixed task durations on one engine** (fast lookups beside minute-long calls) are not measured; each run
  used one duration.
- **State size** beyond 1 MB per task, up to the database column's own limit, is not measured.
- **Payload results are single runs**, and the byte ceiling across shards is not remeasured. In July 2026,
  two shards scaled it ×1.77–1.82.
- **Many fairness keys on a linear workflow** are not yet measured cleanly. Both runs at 512 keys hit a slowdown
  of several minutes in one of the engine's own background queries; the fix is in progress.
- **Replicas spread across zones**, with unequal distance to the database, are not measured.
- **Sustained create-and-purge**: hours of retention churn and recovery after a long-task burst are not
  measured.

## Reproducing

```sh
GOOS=linux GOARCH=arm64 go build -o dwarf-bench ./bench

# Saturated: flows created as fast as a backlog of 2,000 per shard allows.
./dwarf-bench -dsn '1=postgres://USER:PASS@PRIVATE_IP:5432/dwarf?sslmode=disable' \
  -workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000 \
  -warmup 2m -window 16m -stats-interval 10s -label saturated

# A fixed offered rate instead (flows/s): -arrival-rate 1000
# More shards: repeat -dsn with 2=..., 3=...; replicas are separate processes on separate hosts.
# Long tasks: -task-delay 8s, or -workload llm -task-delay 60s -concurrency 10000 for closed-loop in-flight.
```

Use a fresh database for every run (`DROP DATABASE ... WITH (FORCE)` after killing engines), and wait ~60 s
after a hard kill before reusing a database so the dead engine's replica record expires.
