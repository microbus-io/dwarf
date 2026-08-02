# Dwarf `bench` — the load harness, and how not to fool yourself with it

> Load when: running a benchmark campaign, adding a workload or a flag, or reading a results artifact.
> Coupled with: `engine/CLAUDE.md` (the derivations these runs measure) and `docs/benchmark-cloud.md`
> (the cloud campaign's findings).

The harness drives the engine through its **public API** against a real database and writes one
self-contained JSON artifact per run. What follows is not style advice: every item below is a mistake that
was actually made here, cost a run, and produced a number someone nearly believed.

## The rig lies more often than the engine does

**RTT out-predicts every variable under test.** On a local Docker Postgres, measured Spearman
ρ(rtt, steps/s) = **−0.90** across a replica ladder: a run at 0.5ms did 1431 steps/s and one at 1.5ms did
404, in the *same* arm. Before attributing anything to a code change, check the RTT sampler.

**The model, and the only form of it worth quoting.** A SATURATED shard obeys

    occupancy_ms = k·RTT_ms + s          throughput = M / occupancy

where `M` is the pool, `k` the round trips one step makes, and `s` everything per-step that is not a
round trip (server execution, WAL, queueing at the pool). Both terms are MEASURED, and the two behave
completely differently:

- **`k` is roughly portable** — a property of the code path. Measured **12.1** (netem sweep, M=8,
  2026-07-11) and **9.11** (netem sweep, M=96, `linear`, 2026-08-01, R² high, residuals uncurved).
  Quote it as **~9-12**, never a single digit.
- **`s` is NOT portable and is the bigger trap.** 4.4 ms at M=8 against **9.49 ms** at M=96 — it absorbs
  pool contention, so it grows with the pool. Carrying M=8 constants to an M=96 rig over-predicts
  throughput by **21-44%**. Re-derive `s` per configuration or do not use the model.

  How steeply it grows, measured at FIXED RTT (~4.2 ms, 16 vCPU, 2026-08-01): `s` = 6.6 / 8.1 / 12.1 /
  18.4 / 32.9 / **97.7** ms at M = 100 / 200 / 300 / 400 / 500 / 600. It is also inflated by RTT
  INDEPENDENTLY of the pool — a transaction spans `BEGIN…COMMIT`, so distance lengthens how long it holds
  rows and locks. Two arms at the same server-side concurrency disagree because of it: B=72 at 4.19 ms
  gives `s`=12.08 and 5,970 steps/s, B=73 at 0.33 ms gives `s`=**9.54** and **7,658**. Slope at the
  knee: **+1.45 ms of `s` per ms of RTT at 16 vCPU, ~2.2 at 32 vCPU**.

- 🔑 **NEVER FIT `k` FROM A LADDER THAT MOVES THE POOL WITH THE RTT.** Doing so makes M a linear function
  of `(k·RTT+s)`, so `s`-growth is absorbed into the apparent slope: such a ladder fit **11.40**, which a
  lightly-loaded fixed-RTT arm then refuted arithmetically (M=100 at 4.13 ms occupies 44.14 ms, so
  `k < 10.70` or `s` is negative). Fit `k` at FIXED pool, or bound it from a low-M fixed-RTT arm.
- **Workload shape was EXPECTED to move `k` and MEASURED NOT TO — do not re-assert it without new data.**
  The intuition is that a fan-out does more database work, so its `k` should be larger. Measured
  (2026-08-01, same rig, same M=96): `linear` **9.11**, `fanout` width 16 **8.67** - slightly LOWER, and
  the 5% gap sits well inside the fanout arms' own spread (up to 34% between reps, against 0.3-4% for
  linear). **Treat `k ~= 9` as shape-independent until someone measures otherwise.**

  The reason the intuition misleads is worth keeping: a fan-out does more work per FLOW but throughput is
  quoted per STEP, and most of its steps are *branches*. A branch inserts no successor, writes no
  `successor_id` and advances no `step_id` (a fan-out flow sits at `step_id=0`), so it is CHEAPER in
  round trips than a linear step; the expensive spawn and fan-in are 2 steps in 18.

**Use it to NORMALIZE, which is what makes campaigns on different rigs comparable at all:**

    T(ref) = T(measured) · (k·RTT_measured + s) / (k·RTT_ref + s)

Placement is a lottery you do not control (Cloud SQL exposes only the zone), and two same-zone instances
measured **0.32 ms and 0.82 ms — a 29% throughput difference on identical hardware and an identical
build**. That single fact reconciles the two conflicting 16-vCPU/pool-96 numbers in
`docs/benchmark-cloud.md` (7,491 and 5,355): one equation, two RTTs. **Always record RTT beside any
throughput number, and normalize before comparing across sessions.**

Validation that the netem arms are a faithful stand-in for real distance: genuine-placement runs land
within **2-6%** of the injected curve.

**`sequel.SimulateRTT` IS THE WRONG INSTRUMENT for anything about connection occupancy** — use netem. It
pauses *before* the operation reaches the database, and for a DB-level statement `database/sql` acquires
the pooled connection *inside* that operation, so the pause holds no connection at all. It is faithful
only inside a transaction (the connection is held from `BeginTx`). Since RTT's whole effect on throughput
is that it occupies connections, the bias runs the wrong way: the pool looks less binding than it is.

**CONNECTIONS BUY BACK DISTANCE — partially, and with a hard ceiling.** Raising the pool as RTT grows
held throughput at **93-100%** of base across 0.14 → 5.07 ms (16 vCPU), a **5.05x** recovery at 5 ms
against what the shipped pool would have served. But that compares to base at the shipped 6x; knee to
knee, a 4.3 ms path still delivers only **79%**. The useful invariant is `B = T·s/1000`, the backends
actually inside Postgres (Little's law), which gives

    M*(RTT) = B* · (1 + k·RTT / s)

⚠️ **This does NOT separate into `B*(vCPU) · f(RTT)` — the test was run and it failed.** Predicting the
32-vCPU knee at 4 ms from the 16-vCPU RTT correction gave 573 (separable) or 662 (tier-specific `s*`);
the measurement was **>900 with the knee never reached** and DB CPU at only 65%. Bigger tiers are hurt
disproportionately: they start from a smaller `s` (8.73 ms at 32 vCPU vs 12.73 at 16), so `k·RTT`
dominates sooner, *and* their `s` inflates faster with distance. **At distance, add shards rather than
grow one** — full compensation at 4 ms would have needed ~1,163 connections on one instance.

Two scaling results that contradict the shipped ratio's shape (all at short RTT, n=1):
**`B* = 15.0·vCPU^0.72`** — sublinear, so per-vCPU sizing over-connects large instances — and the optimal
multiplier therefore **FALLS** with instance size (**11.2x / 8.8x / 6.9x** at 8 / 16 / 32 vCPU) where the
shipped rule rises. 6x nonetheless captures 90-94% of peak at every tier; the last 6-10% costs roughly a
doubling of connections.

**DB CPU at the knee is RTT-invariant but TIER-dependent**: 61.6 / 65.1 / 66.3% across 0.4 / 1.2 / 4.3 ms
at 16 vCPU, against **76.9 / 66.3 / 51.6%** at 8 / 16 / 32 vCPU. One "size to N% CPU" target cannot serve
all tiers, and on 16 vCPU the 70-90% band straddles the collapse edge rather than the optimum.

**Throwaway databases degrade the server.** 65 accumulated bench databases (3.5GB) pushed real RTT from
0.76ms to 2.26ms and R=1 throughput from 885 to 345 — a 2.5x drift that biases LATE phases against EARLY
ones, silently, across a campaign. **Drop each database immediately after its arm**, not just before.
Cross-phase comparisons of absolute throughput are unsafe without this; within-phase comparisons survive.

**Stale `dwarf_peers` rows crater a run.** A killed engine leaves a row that inflates R, gutting every
pool (measured ~180 vs ~7500 steps/s). `dwarf_peer_changes` reads zero in a settled fleet by construction,
so check it in the artifact: nonzero means the fleet churned mid-run and the numbers are suspect.

**Lower-case the database name.** Postgres folds unquoted identifiers, so `CREATE DATABASE dwarfFoo` makes
`dwarffoo` while a DSN asking for `dwarfFoo` gets `does not exist`. Cost one 22-minute run, every arm.

**RTT DEGRADES ACROSS A SESSION, so back-to-back arms are ordered by server fatigue, not by treatment.** On
local Docker Postgres, six interleaved arms run without pause measured RTT 0.44 → 5.63 → 6.37 → 2.24 → 0.64
→ 6.69 ms, and throughput tracked it: every arm that started degraded landed at 291-427 steps/s regardless
of which build it was running, while the one arm that started fresh hit the full commanded 800. Interleaving
does **not** save you here — it spreads the damage evenly rather than removing it, and with ρ(rtt, steps/s) ≈
−0.90 the residual noise is larger than most effects worth measuring. Two controls, both cheap: an idle
**cooldown** between arms, and an **RTT gate** that probes host→database latency *while idle* and waits for
it to come back under a threshold before starting. An arm that starts degraded is not a measurement.

**The RTT to gate on is the HOST's, and it must be one long-lived connection.** A `docker exec psql` probe
measures the unix socket *inside* the container (~0.1 ms) and is blind to the port-forward the engine
actually traverses; a fresh connection per sample measures the handshake instead of the path. Take the
**minimum** over a dozen `SELECT 1`s on one connection, discarding the first.

**A flat-out probe measures a different machine than a commanded-rate one — do not size a rig with the
wrong one.** Choosing the pool by saturating open-loop (`-arrival-rate 0`, deep backlog) said pool 12 beat
24 and 48 (1041 vs 424 vs 446 steps/s) and looked like a textbook over-connection collapse. At a *commanded*
rate with a shallow backlog the ordering **reverses**: 357 / 743 / 800, with the largest pool serving the
full command at 116 flows outstanding and the smallest falling half short. The flat-out arms were
backlog-bound (a deep same-band queue inflates the band scan and with it RTT), so they measured the scan,
not the pool. Size the rig under the regime the experiment will actually run in.

## Design the comparison, not just the run

**Rotate arm order (Latin square).** A fixed order inside each rep ties every arm to a sequence position:
the first arm always runs on the freshest server. That produced a clean-looking replica ladder whose entire
signal was position, and it was only caught because RTT tracked the arms.

**Interleave reps; never run all of arm A then all of arm B.** Host drift then loads onto one arm.

**Change one variable, and re-derive what depended on it.** Injecting 2ms of RTT cut the DB ceiling from
~1330 to ~493 steps/s, which invalidated load levels chosen against the old ceiling — two of three cells
saturated both arms and stopped discriminating. Same class of error as holding the *step* rate constant
across a chain-depth sweep, which varied the flow-CREATION rate 6.7x and made the shallow cells
creation-limited rather than dispatch-limited.

**A commanded rate must sit well under the ceiling.** Open-loop at a fixed arrival rate is the sensitive
instrument — the signal is whether completions keep up and whether the backlog diverges — but only with
real headroom. At 75% of ceiling an RTT blip pushes the ceiling below the command and the control arm stops
being a control.

**Closed-loop hides things open-loop shows.** Closed-loop makes starts ≡ completions by construction, so
`dwarf_flows_started` cannot distinguish a saturated CALLER from a saturated engine; only a commanded
arrival rate can. Closed-loop also *amplifies* per-shard heterogeneity through Little's law: blocked
submitters stop generating load, so a slow subset costs its share of the MEAN latency, not its share of the
work.

**Doubling concurrency does not exonerate the load generator.** A saturated generator absorbs concurrency
without producing throughput, exactly as a saturated database does. Measure host CPU (`host.cpuCores`)
instead — there is precedent for a "shards add nothing" finding that was a 4-vCPU engine host all along.

## What the workload has to exercise

**A closed-loop linear workload dispatches ~100% through the DOORBELL, not the refiller.** Measured:
`dwarf_steps_offered` 42,070 against `dwarf_steps_executed` ~42,060, with the refiller supplying ~3%. Every
completion offers its own successor into an empty local partition, so a chain never leaves the replica that
created it and the **piston, planner, global band, slice rule and residue partition are all inert**. A
replica ladder run that way measures none of them.

To put load on the scan path, work must be created by a replica that cannot dispatch it: `-replica-workers
"0,64,64"` makes replica 0 await-only, and its creations are reachable only by a peer SCANNING. That is the
`peerFleet`/`stealFleet` shape, and it is why those fixtures create through a `SetWorkers(0)` engine.

**Deep backlog needs `-fanout-width` or open-loop.** A linear flow holds one pending step at a time, so a
closed-loop generator's backlog can never exceed its concurrency.

**`state` and `carry` measure opposite things, and using the wrong one answers the wrong question.**
`state` REWRITES its payload every step, so each step's payload is genuinely NEW data that has to be stored
somewhere - it is the MB/s instrument, and what it measures is irreducible write volume. `carry`/
`carryfanout` write the payload once (as the flow's initial state, so the ENTRY step anchors it) and
thereafter only carry it, which is the multiplier refs exist to remove.

**Do NOT say the `state` workload "cannot mint refs" - it mints them fine, and the counters say so.** A
field the task rewrote is re-anchored at the step that wrote it (its `changes` column holds the bytes) and
the successor's `state` refs that, so `state`'s STATE column measures **0.0 K/flow** while its CHANGES
column carries the full 322.5 K/flow (5 rewriting steps x 64K). Measured, local Postgres 18.1, 64K payload,
6 steps:

| | write `state` | write `changes` | total stored | read `state_ref` |
|---|---|---|---|---|
| `state` (5 rewrites) | 0.0 K/flow | 322.5 K/flow | **322.5 K/flow** | 259.1 K/flow |
| `carry` (0 rewrites) | 64.1 K/flow | 0.1 K/flow | **64.1 K/flow** | 64.0 K/flow |

So the 5x gap between the two is **not** refs working in one and failing in the other; it is that `state`
genuinely produces five payloads and `carry` produces one. Refs remove the CARRY multiplier, never write
volume - no storage mechanism can, because the data really is new. Quote the table, not a claim about
minting.

**Read the carry workloads off `dwarf_state_write_bytes{column=state}`, NOT off `MB/s`.** Their tasks
declare almost no write volume of their own - that is the entire point, and `bytesWritten` is deliberately
left untouched, so the reported MB/s is 0.00 and means nothing. The gap between what the task wrote and what
the engine wrote *is* the measurement. `dwarf_state_read_bytes{column=state_ref}` reading nonzero is the
confirmation that refs are actually being resolved rather than the payload having quietly gone missing.

**The carry tasks self-validate, and a run that loses the document fails LOUDLY.** Every carry task asserts
the document is still present (and, under `-carry-read`, still the right size). This is not decoration: a
carried field silently vanishing past a fan-in is a real failure mode the ref carry-forward and the anchor
pin exist to prevent, and it would otherwise present as an *excellent* byte number - the harness would be
measuring data loss and reporting it as a saving. Verified by removing the document from the initial state:
the run comes back `valid: false` with 1,425 errors rather than a flattering number.

**`-carry-read` switches how state is HELD, not how much is stored.** It makes every task read the carried
document instead of passing it along. Storage is identical in both arms, so a byte number that moves with
this flag means the workload is not carrying what it thinks it is; what it does move is the in-flight
held-state gauges (`dwarf_state_in_flight_bytes` / `_steps`).

## Measuring a crew that only shrinks when load falls

**A constant `-task-delay` cannot see it, and that is a property of the arm rather than of the engine.** The
crew grows while tasks are long and comes back down once they are not, so an arm that holds the exec term
fixed for its whole window grows to a plateau and reports the plateau — whatever would have happened next.
Both questions need load to change *within* one run: does the crew come back down, and does throughput
recover to what the same rig does with no delay at all.

`-task-burst D -task-quiet D` alternates `-task-delay` with zero on that cycle. The quiet half is **not
idle** — flows keep arriving at the same rate, tasks simply stop sleeping — so only the exec term moves and
the comparison stays controlled. Both flags are needed; half a cycle is not a cycle, and either alone leaves
the delay constant. The schedule is a pure function of elapsed time (`taskProfile.delayAt`), so there is no
driver goroutine whose phase would drift under the load being measured, and one profile is anchored once and
shared by every replica so a multi-replica fleet is in the same phase.

Size the window to span **several** cycles, and set `-stats-interval` to a fraction of the burst: the answer
is a shape over time, and both the window-edge snapshots and the window mean report a burst arm as something
in between. `bench/gcp/burst.sh` is the runner — three arms (`steady0` the recovery target, `steadyN` the
plateau, `burst` the one under test), and it tees the readout per arm because **the artifact records a gauge
mean and peak but no trough, and the trough is the entire question**.

**The retirement window is a shipped 2-minute constant with no knob**, so the quiet half has to outlast
several of them. The crew decays about a quarter per round, so ~3 rounds (6 min) to lose half and ~7 (14
min) to lose seven eighths; a quiet half under ~6 min measures the onset only, and one that shows a flat
crew is as likely to be too short as to be a finding.

**`-stats-interval` prints the readout, and `crew` is the column it exists for.** It rides the gauge sampler
that already ticks at 50 ms rather than collecting on its own, so the line and the artifact's means come from
the same readings. `delayMs` beside it says which half of the cycle a line belongs to. It reads
`dwarf_workers_resident`, not `runtime.NumGoroutine` — the process also holds the load generator, which is a
rounding error against 36,000 workers and most of the count against 800 (both columns are printed).

⚠️ **GAUGES HAVE NO BARE-NAME TOTAL — sum across the attribute sets.** Counters emit `name` alongside
`name|k=v`; gauges emit only the attributed series (`dwarf_turnstile_available|shard=1`,
`dwarf_steps_pending|priority=100`). Reading the bare name gets a confident **0.0**, which is what three of
five gauge columns in this readout printed on a run executing hundreds of steps a second, and it would read
identically on a rig. `gaugeTotal` is the helper; use it for anything new here.

## Profiling a run

`-pprof DIR` writes a CPU profile for the whole run and a heap profile at the end, named from `-label`.

It exists because the headline cost — microseconds of engine CPU per step rising as the crew grows — is a
whole-**process** number and this process also contains the load generator. No counter in the artifact
separates them; a profile does.

**Pass ONE `-concurrency` value when attributing.** The CPU profile spans every step of a sweep, so profiling
one averages arms that differ in the quantity under test and produces a profile of nothing in particular.
Profiling starts before engine startup and migrations deliberately — they are process CPU like anything else,
and a profile that began after them would under-report. The heap profile is taken after a GC, so it is live
bytes; read it against the goroutine count rather than expecting it to explain RSS, since **stacks are not
heap** and the crew is mostly stacks.

## Reading an artifact

- **Counters carry attributes** as `name|k=v` alongside the bare-name total. Both are emitted: the total is
  what a throughput number reads off, the split is the only thing that shows one shard falling behind.
- **Gauges every replica reports identically are MAXed, not summed** (`agreementGauges`) — cluster-wide
  reads (`dwarf_steps_pending`, `oldest_pending_age`) and per-replica readings
  of a shared fact (`dwarf_peer_replicas`, `dwarf_peer_blind_seconds`). Summing them multiplied by the
  replica count; that bug inflated those in **every multi-replica artifact produced before it was
  fixed**, so distrust older cloud numbers that lean on backlog depth or oldest-age.
- **`dwarf_refill_query_duration_seconds{shard,phase=band_keys}`'s COUNT is cycles per window** — i.e.
  piston RPS, since a cycle scans exactly once — and the four phases summed are the duty cycle. There is
  no revolutions instrument and no end-to-end cycle histogram; the phases are what reconstruct one.
- **Per-shard cycle-duration spread is mostly POOL WAIT, not a slow shard.** Measured 28ms vs 125ms on
  hardware whose RTT differed by 0.036ms. Decompose against `pg_stat_statements` and the pool-wait gauges
  before calling a shard slow. Note the pgss/dbstats/RTT samplers watch `dsns[0]` only, so a multi-shard run
  has no server-side counterpart for shards 2+.
- **`valid: false`** means a recovery/unwedge counter fired; the run is not comparable.

## Flags worth knowing

`-open-loop -arrival-rate N -max-outstanding M` (commanded rate + backpressure bound), `-replica-workers`
(per-replica worker counts; `0` = await-only, `1` = capacity-crippled and grow-on-demand disabled),
`-linear-steps` (chain depth — under partitioning, depth changes the share of FLOWS a slow replica blocks
without changing the share of STEPS), `-refill-interval-ms` (pin the cycle period; **mandatory** in any
ladder that varies replica count, since the derived interval is a function of R and would otherwise
masquerade as a partitioning cost), `-fairness-keys`, `-vcpus`, `-max-open-conns`, `-task-burst`/`-task-quiet` (alternate the exec term within a
run — the only way to see the crew shrink), `-stats-interval` (periodic readout), `-pprof` (CPU + heap
profiles).

Test-only knobs the bench CANNOT reach: `Engine.Seams()` and `Engine.DB()` panic outside a test binary, so
fault injection and `sequel`'s `SimulateRTT` are available to fixtures but not here. Reaching them from a
benchmark needs real API, not a back door.
