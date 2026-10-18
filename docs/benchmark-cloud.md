# Cloud benchmarks

> **For operators** sizing a deployment. This page answers how much one database serves, what adding
> shards and engine replicas does, and what the engine itself costs. Every number was measured, and the
> configuration that produced it is stated beside it. The database volume section quotes measurements from
> July 2026, on 8 vCPU databases, and says so.

## The short version

- **One 32 vCPU PostgreSQL database runs ~21,900 steps/s**, about 1.9 billion steps a day, at the engine's
  shipped defaults. Instances of the same size differ: others ran 20,000–24,100 steps/s on the same engine.
- **It sustains ~95% of that with low latency.** Offered at 95% of its saturated rate, a database delivered the
  full load with a median flow latency of 83 ms; at 100% its backlog grew. Plan from your load's average, its
  peaks and your tolerance for waiting during them ([Planning capacity](#planning-capacity)).
- **32 vCPU is the most economical database size**, about $0.04 per million steps at full load. Smaller
  databases run out of CPU, larger ones are held back by the write-ahead log, so past 32 vCPU you add shards.
- **Shards add throughput linearly.** Four shards ran 3.9–4.2 times one shard.
- **Replicas add availability, not cost.** One, two or four replicas on a shard deliver the same
  throughput within 6%. Each shard is served by two of them, and any others stand by to take over.
- **Pair each database with two engines at half its vCPU.** 8 vCPU with 2 × 2 vCPU engines runs ~4,200
  steps/s, 16 vCPU with 2 × 4 runs ~7,100, 32 vCPU with 2 × 8 runs ~21,900.
- **The engine is cheap.** A shard's work takes about 5 engine cores. Two 8 vCPU engine hosts per
  32 vCPU database serve it at about a third of their CPU each, and either can carry it alone.
- **Long tasks cost no throughput, and mixed durations share one engine.** With 8-second tasks the engine
  delivered the full offered rate, and a mix of 5 ms, 200 ms and 8 s tasks ran at the latency each class
  predicts. 10,000 concurrent 60-second tasks ran on under one engine core.
- **Large state works up to the database column's limit.** 250 MB per step completed cleanly, at ~45 MB/s for
  one flow and ~135 MB/s for four.
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
nothing, so each brings its own write capacity ([measured below](#the-write-ahead-log-and-why-a-shard-is-its-own-server)).

**Absolute numbers belong to the instance that produced them; ratios travel.** Cloud instances of the
same spec differ: on one engine, 32 vCPU instances ran from ~16,000 to 24,100 steps/s ([method
notes](#method-notes)). The runs below span several sessions, engine builds and rig setups, each session on its
own set of instances. A comparison is made within one session, on one build and one setup, unless a section
says otherwise, and each section dates its runs.

## One database

On one 32 vCPU database, one engine replica ran **21,952, 21,931 and 21,878 steps/s** in three runs, about
2,190 flows/s and 1.9 billion steps a day, steady from the first minute of each window to the last
([minute by minute](#minute-by-minute)). A saturated flow waited in the 2,000-flow backlog, so it completed in
a median of ~0.94 s, with a p99 of ~2.0 s. The engine used 5.1–5.3 cores of its 16 vCPU host.
*(Measured 2026-10-17.)*

Other instances of the same size, on the same engine, ran 19,960 and 24,107 steps/s, and two ran near 16,000.
Those two spent ~40% more database CPU per step, so the difference is in the instance, not the load
([method notes](#method-notes) describe how to check an instance).

## Database size: what limits each

![Database size: throughput, cost per million steps, and where 32 vCPU wins](benchmark-cloud-tiers.png)

One shard at each database size, saturated:

| Database | Engines | steps/s | Database CPU | Database core-ms per step | Monthly cost | $ per million steps |
|---|---|---|---|---|---|---|
| 8 vCPU, 32 GB | 2 × 2 vCPU | 4,186 | 93% | 1.74 | ~$700 | $0.064 |
| 16 vCPU, 64 GB | 2 × 4 vCPU | 7,015–7,122 | 82–86% | 1.85–1.95 | ~$1,240 | $0.067 |
| 32 vCPU, 128 GB | 1 × 16 vCPU, priced as 2 × 8 | 21,900 | 74% | 1.08 | ~$2,310 | **$0.040** |
| 64 vCPU, 256 GB | 1 × 16 vCPU, priced as 2 × 16 | 29,415 | 63% | 1.36 | ~$4,440 | $0.057 |

The 64 vCPU database ran with `MaxConnections: 500`; at the default 250 it ran 24,162. The 32 and 64 vCPU rows
each ran on one 16 vCPU engine host, which used 5.1–5.3 and 6.8 of its cores: the engine was not the limit at
either size. Each row is priced with the recommended pairing, two engine hosts totalling half the database's
vCPU, so either host can carry the shard alone: 2 × 8 vCPU for 32, 2 × 16 for 64. Monthly cost is
the on-demand list price in one US region: the database (without high availability), its 1 TB SSD, and both
engine hosts. The cost per million steps assumes the deployment runs at its measured rate around the clock
(730 hours a month); one that averages half that rate pays twice as much per step.

- **Below 32 vCPU the database runs out of CPU.** The 8 and 16 vCPU databases ran at 82–93% CPU, and each
  step cost them 1.7–1.9 milliseconds of database CPU: the same connections share fewer cores, and the
  database spends more of its time coordinating them. More connections still help these sizes (see
  [How the connection budget was chosen](#how-the-connection-budget-was-chosen)), so this is a cost of the
  small instance, not a sign of too many connections.
- **From 32 vCPU up, the write-ahead log is the limit.** Each step costs about 1.1–1.4 milliseconds of database
  CPU, and a quarter to a third of the cores sit idle: every commit goes through the database's single
  write-ahead log ([below](#the-write-ahead-log-and-why-a-shard-is-its-own-server)), and more cores do not
  widen it.
- **So 32 vCPU is the most economical size.** Each of its vCPUs delivers ~685 steps/s, against 442–523 for the
  other sizes. From 16 to 32 vCPU, throughput triples for 1.8 times the price, because the step also gets
  cheaper. From 32 to 64 vCPU it grows 1.34 times for 1.9 times the price, and only with
  `MaxConnections: 500`. Past 32 vCPU, add shards, which scale ~1.9 times per doubling.
- **The pairing holds at every size.** Each engine runs at about a third of its CPU, so either replica can
  carry its shard alone if the other fails.

*(Measured 2026-10-17/18. Same-size instances differ; see [method notes](#method-notes).)*

## The write-ahead log, and why a shard is its own server

On a saturated 32 vCPU shard, ~140 backends wait on the write-ahead log's write lock at once. A deeper look at
a 16 vCPU instance (July 2026) showed that the ceiling is a stack of locks, so buying around one of them does
not raise it far:

| connections | steps/s | `LWLock/WALWrite` waiters | `LWLock/BufferContent` | on CPU | `IO/WALSync` |
|---|---|---|---|---|---|
| 96 | 5,355 | 33 | 0.1 | 1.5 | 0.8 |
| 192 | 7,016 | 70 | 2.6 | 7.2 | ~0 |
| 384 | ~6,400 | 99 | 76 | 24 | ~0 |

*Counts are database backends sampled in each state. They show where backends wait, not how busy the CPU is.*

- **The write lock serializes every commit into the log.** Writes waited on it, not on the disk: sync
  waits were ~0.
- **Removing the top of the stack exposes the next one about 5% higher.** Turning off `synchronous_commit`
  eliminated write-lock waits and bought **+4%**, because buffer-page contention more than doubled.
  `wal_compression` cost 5%: it reduces the log's bytes, and bytes are not what is scarce.

**So a shard must be its own server.** A PostgreSQL server has one write-ahead log, shared by every database on
it. Same total connections, measured directly:

| topology | steps/s | vs one database on one server |
|---|---|---|
| 2 databases, **one** server | 6,916 | ×1.03 |
| 2 databases, **two** servers | 13,995 | **×2.08** |

*(Measured July 2026 on 16 vCPU instances; these are properties of PostgreSQL, not of the engine.)*

## Shards: throughput adds up

![Throughput by shard count and fleet shape](benchmark-cloud-shards.png)

Each shard is its own 32 vCPU database. *These runs used an earlier engine (2026-10-09/12), which ran
~15–20% below the current one per database; the ratios between shard counts are what carry over.*

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

*(Measured 2026-10-09 on an earlier engine; the ratios between replica counts are what carry over.)*

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
[Database size](#database-size-what-limits-each)).

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

These figures are the engine's own cost, measured 2026-10-09/12. On the current engine, one 16 vCPU host
driving a 32 vCPU database ran 4,100–4,300 steps/s per engine core (2026-10-17). Your tasks run on the same
hosts and need their own headroom.

## Minute by minute

![Minute by minute: three saturated runs on one 32 vCPU database](benchmark-cloud-minutes.png)

*The vertical axis starts at 14,000 steps/s.*

A saturated run holds its rate from the first minute of the window to the last: every minute of the three runs
above fell between 21,200 and 22,800 steps/s, and no run drifted down over its 16 minutes. The rate a run
starts at is the rate to plan on. Over hours and a growing database, [Database volume](#database-volume) applies.

*(Measured 2026-10-17.)*

## Planning capacity

![Commanded at 100% and at 95% of a database's saturated rate](benchmark-cloud-headroom.png)

**How much a database sustains.** One 32 vCPU database was first run saturated (19,960 steps/s on this
instance), then offered fixed rates at 100% and at 95% of that, 16 minutes each:

| offered steps/s | % of saturated | delivered | flows waiting, first → last minutes | flow p50 | flow p99 |
|---|---|---|---|---|---|
| 19,960 (saturated) | — | 19,960 | held at 2,000 | 1.04 s | 2.1 s |
| 19,960 | 100% | 19,684 | 322 → 19,881, still growing | 4.4 s | 14.7 s |
| 18,962 | 95% | 18,871 | 129 → 173, flat | **83 ms** | **1.4 s** |

- **At 95% the database keeps up, and flows barely wait.** The backlog stayed near 150 flows, and the median
  10-step flow completed in 83 ms against ~1 s when saturated.
- **At 100% it does not.** Small dips in the database's rate leave work that the offered rate never gives back
  time to clear, so the backlog grew for the whole window. The saturated rate is a ceiling, not a target.

*(Measured 2026-10-18.)*

**Plan from the shape of your load, not one percentage.** Call the rate a database sustains `C` (about 95% of
its saturated rate):

- **The average must stay at or below `C`.** Above it the backlog grows without end.
- **Peaks above `C` cost waiting, not work.** A peak at `P` steps/s lasting `T` seconds leaves a backlog of
  `(P − C) × T` steps. It drains at `C − A`, where `A` is the load after the peak, and while it lasts each new
  flow waits about `backlog ÷ C` longer.
- **Your tolerance for that wait decides the headroom.** If waiting during peaks is unacceptable, size so that
  peaks stay under `C`. If it is acceptable, size for the average, with quiet periods long enough to drain each
  peak before the next.

For example, a shard sustaining 19,000 steps/s that sees two minutes at 24,000 builds a backlog of 600,000 steps.
With the load back at 17,000 that clears in five minutes, and at its largest it adds about 30 seconds to a flow.

**Priority bands let urgent work skip that wait.** The engine always dispatches a higher band's steps ahead of
any lower band's, so during a peak the backlog forms in the lowest bands. Flows that cannot wait run in a higher
band and keep their latency; best-effort flows absorb the peak and catch up in the quiet that follows. What has
to stay under `C` is then the urgent work alone. Three things to know:

- A flow's priority is set when it is created and does not change.
- Dispatch is not preemptive, but the only wait a high-priority step has is for a database connection to free
  up, typically well under a second; a step whose task is still running holds no connection.
- A higher band that never goes quiet keeps the lower bands from running at all.

## Long tasks

A task that waits on something slow, such as a model call or a human, ties up the flow but not the
database. These runs check that the engine keeps the offered rate as tasks lengthen, and that a waiting
task costs little.

**Seconds-long tasks at a fixed rate.** 1,000 flows/s of the 10-step `linear` workflow, about 10,000
steps/s or half of what the database runs saturated, with every task sleeping 0, 1 or 8 seconds. One
32 vCPU database, one 16 vCPU engine host, a 6-minute window after 2 minutes of warmup (2026-10-10):

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
  engine held up to 80,000 flows in flight. On the current engine it is ~6 ms at this rate (below).
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

**Mixed durations on one engine.** The same rate and setup, with each task's duration drawn at random from
weighted classes, so fast and slow tasks share one engine (2026-10-18):

| Task durations | steps/s | Flow p50 | Flow p99 | `Create` p99 | Workers | Database connections in use |
|---|---|---|---|---|---|---|
| none | 9,994 | 39 ms | 0.17 s | 6.1 ms | 500 | 43 mean |
| 80% 5 ms, 15% 200 ms, 5% 8 s | 9,998 | 671 ms | 24.1 s | 6.3 ms | ~4,700 | 43 mean |
| 85% 2 ms, 12% 100 ms, 3% 5 s | 9,997 | 252 ms | 10.2 s | 5.5 ms | ~2,050 | 43 mean |

- **Each mix runs at the latency its durations predict.** Ten draws per flow from the mix, plus the few
  milliseconds per step of the no-delay run, predicted a p50 of ~680 ms and ~260 ms and a p99 of ~24 s and
  ~10 s. Slow tasks do not hold up fast ones.
- **The engine grows workers for the slow tasks, not connections.** Workers rose to ~4,700 for the mix with
  8-second tasks, while the database connections in use stayed the same as with no delay.

No flow errors in any of these runs.

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
- **`MaxConnections` is the lever.** A 64 vCPU database wants ~500 even nearby (+14–22%); a database shared
  with other work wants less. See [deployment](deployment.md#connection-pool).

*(Measured 2026-10-07/08, saturated, 16-minute windows, one run per setting.)*

## Sizing a deployment

1. **Shards.** Plan each 32 vCPU database at **~19,000 steps/s** sustained, 95% of the slower full-speed
   instances measured, and size from your load's shape ([Planning capacity](#planning-capacity)):
   `shards = average steps/s ÷ 19,000`, rounded up, then check that each peak's backlog drains in time and adds
   a wait you accept, or put the work that cannot wait in a higher priority band. Below one shard's worth,
   a 16 vCPU database (~6,700 sustained) or an 8 vCPU database (~4,000) costs less in total, at a higher
   price per step.
2. **Engine replicas.** Two per shard at half the database's vCPU: `2S` replicas of 8 vCPU for 32 vCPU
   shards. Each runs at about a third of its CPU and can carry its shard alone. A larger host can serve
   several shards instead: give its engine about one core per 3,300 steps/s its shards can deliver at
   most - four 32 vCPU shards, ~20–24 cores - and set `GOMAXPROCS` to that plus whatever your tasks use
   in the same process, rather than leaving it at the host's full count. Run at least two such hosts, so
   one can fail.
3. **Your tasks.** Add the CPU and memory your tasks use on the engine hosts. The engine itself holds ~15 KB
   per flow in flight, plus the state of each step it is running - large payloads need far more
   ([State size](#state-size)); with long tasks, flows in flight = flows/s × flow duration.
4. **Distance.** Put the engine in its database's zone. Up to 1 ms the budget compensates; past it, plan for
   less per shard.
5. **Disk.** Size each database's disk for IOPS, not only capacity (see [method notes](#method-notes)).

**Worked example.** 50,000 steps/s on average (5,000 flows/s of 10-step flows), with a daily peak of 65,000
for 10 minutes: 3 × 32 vCPU shards sustain ~57,000, so the average fits. The peak leaves a backlog of
(65,000 − 57,000) × 600 s = 4.8 million steps, which clears in ~11 minutes once the load falls back to 50,000
and adds up to ~84 seconds to a flow meanwhile. If that wait is acceptable, the deployment is 3 shards and
6 × 8 vCPU engine replicas, about $6,900 a month at on-demand list price before your tasks' own cost. If it is
not, put the latency-sensitive flows in a higher priority band, or plan a fourth shard.

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

*These runs come from one session of their own on an earlier engine (2026-10-13/14), except scatter at 1 MB,
measured in a later session. Its no-payload rows ran ~4–7% below the current [headline](#one-database), so
compare rows within this table.*

**Small payloads, measured 2026-10-14.** For 1 KB and 4 KB, read this table rather than the rows above, which
come from an earlier build. `linear`, one session:

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

## State size

How large a payload one step can carry, on one 32 vCPU database and one 16 vCPU engine host. Each flow is 5 steps,
and every step writes a fresh, incompressible value of the given size; one or four flows run at a time:

| Payload per step | MB/s, 1 / 4 flows | Flow time p50, 1 / 4 flows | Engine heap mean / peak, 4 flows |
|---|---|---|---|
| 8 MB | 46 / 139 | 0.8 / 0.9 s | 9.6 / 13.3 GB |
| 32 MB | 48 / 141 | 3.0 / 4.3 s | 10.4 / 14.6 GB |
| 64 MB | 49 / 137 | 6.2 / 8.7 s | 11.3 / 15.8 GB |
| 128 MB | 47 / 132 | 12.8 / 17.8 s | 12.9 / 19.4 GB |
| 250 MB | 44 / 134 | 26.1 / 34.9 s | 16.2 / 24.6 GB |

- **The engine adds no limit of its own.** Every run completed without an error or a re-run step. A step's
  value can be as large as the database column allows, which on PostgreSQL is just under 256 MB.
- **Large payloads move at a byte rate:** ~45–49 MB/s for one flow and ~135 MB/s for four, whatever the size.
  A 250 MB step takes about 5 seconds alone.
- **Engine memory is the state cache plus about ten times each payload in flight.** The cache stayed within its
  budget (a tenth of the host's memory, ~6 GB here) at every size; with four 250 MB steps in flight the heap
  held ~10 GB beyond it. Peaks ran 1.5–2 times the mean. Size engine hosts for the cache, plus ten times the
  largest payloads you expect in flight at once, plus that headroom.

*(Measured 2026-10-18.)*

## Database volume

*Measured in July 2026 on 8 vCPU databases with 500 GB disks:*

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
  placement within a zone varies the round trip.
- **Check each instance before trusting its numbers.** On one engine and one rig setup, 32 vCPU instances ran
  from ~16,000 to 24,100 steps/s. The slow ones did not look busier: at the same ~77% CPU, they spent ~1.5
  milliseconds of database CPU per step against ~1.1. Divide the database's CPU (cores busy × 1,000) by the
  steps/s a few minutes into a run; if it is well above the size's usual figure, use another instance.

## Known gaps

- **A capped `GOMAXPROCS` on a large host** is not measured: how much engine CPU it saves, and where the cap
  starts to cost throughput.
- **Only PostgreSQL on Google Cloud is measured here.** MySQL and SQL Server ceilings at the default budget
  are not, nor other clouds or regions.
- **Payload results are single runs**, and the byte ceiling across shards is not remeasured. In July 2026,
  two shards scaled it ×1.77–1.82.
- **Shards, replicas, failover and the engine's host sizes** were measured on an earlier engine, which ran
  ~15–20% below the current one per database. The ratios hold; the absolute figures are low.
- **Priority bands under peaks past capacity** are not measured on a cloud database: the engine's ordering by band
  is tested, the latency it keeps for a higher band through a peak is not.
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

Use a fresh database for every run (`DROP DATABASE ... WITH (FORCE)` after killing engines). A killed engine
leaves its replica record behind: a reused database counts it as a live replica for 40 seconds, shrinking the
other replicas' connection pools, and until the record is pruned, minutes later, it can delay each hand-off of a
shard's dispatch by about a second.
