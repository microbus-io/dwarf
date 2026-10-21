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

- **`k` is portable** — a property of the code path, and COUNTED rather than fitted: `completionRoundTrips`
  (`engine/poolsize.go`) plus the claim and the flow read. The per-driver totals are stated once, in
  `engine/CLAUDE.md` ("THE CEILING EQUATION"); take them from there rather than keeping a copy here. The fits quoted in this file - **12.1** (netem sweep, M=8, 2026-07-11) and **9.11** (netem sweep,
  M=96, `linear`, 2026-08-01, R² high, residuals uncurved) - were taken on PostgreSQL before the transition
  was pipelined; they are not the current PostgreSQL `k`.
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
  (2026-08-01, same rig, same M=96, PostgreSQL before the transition was pipelined): `linear` **9.11**,
  `fanout` width 16 **8.67** - slightly LOWER, and the 5% gap sits well inside the fanout arms' own spread
  (up to 34% between reps, against 0.3-4% for linear). **Treat `k` as shape-independent until someone
  measures otherwise.**

  The reason the intuition misleads is worth keeping: a fan-out does more work per FLOW but throughput is
  quoted per STEP, and most of its steps are *branches*. A branch's transition is a cohort bump: it inserts no
  successor and writes no `successor_id`, so it is CHEAPER in round trips than a linear step; the expensive
  spawn and fan-in are 2 steps in 18.

**Use it to NORMALIZE, which is what makes campaigns on different rigs comparable at all:**

    T(ref) = T(measured) · (k·RTT_measured + s) / (k·RTT_ref + s)

Placement is a lottery you do not control (Cloud SQL exposes only the zone), and two same-zone instances
measured **0.32 ms and 0.82 ms — a 29% throughput difference on identical hardware and an identical
build**. That single fact reconciled two conflicting 16-vCPU/pool-96 measurements (7,491 and 5,355 steps/s):
one equation, two RTTs. **Always record RTT beside any
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
against what a fixed 96-connection pool would have served. But that compares to base at 96; knee to
knee, a 4.3 ms path still delivers only **79%**. The useful invariant is `B = T·s/1000`, the backends
actually inside Postgres (Little's law), which gives

    M*(RTT) = B* · (1 + k·RTT / s)

⚠️ **This does NOT separate into `B*(vCPU) · f(RTT)` — the test was run and it failed.** Predicting the
32-vCPU knee at 4 ms from the 16-vCPU RTT correction gave 573 (separable) or 662 (tier-specific `s*`);
the measurement was **>900 with the knee never reached** and DB CPU at only 65%. Bigger tiers are hurt
disproportionately: they start from a smaller `s` (8.73 ms at 32 vCPU vs 12.73 at 16), so `k·RTT`
dominates sooner, *and* their `s` inflates faster with distance. **At distance, add shards rather than
grow one** — full compensation at 4 ms would have needed ~1,163 connections on one instance.

Two scaling results against sizing the pool per vCPU (all at short RTT, n=1): **`B* = 15.0·vCPU^0.72`** —
sublinear, so per-vCPU sizing over-connects large instances — and the optimal multiplier therefore **FALLS**
with instance size (**11.2x / 8.8x / 6.9x** at 8 / 16 / 32 vCPU). A constant budget follows that shape
better than any per-vCPU ratio (see `engine/CLAUDE.md`, "Connection pool sizing").

**DB CPU at the knee is RTT-invariant but TIER-dependent**: 61.6 / 65.1 / 66.3% across 0.4 / 1.2 / 4.3 ms
at 16 vCPU, against **76.9 / 66.3 / 51.6%** at 8 / 16 / 32 vCPU. One "size to N% CPU" target cannot serve
all tiers, and on 16 vCPU the 70-90% band straddles the collapse edge rather than the optimum.

**Throwaway databases degrade the server.** 65 accumulated bench databases (3.5GB) pushed real RTT from
0.76ms to 2.26ms and R=1 throughput from 885 to 345 — a 2.5x drift that biases LATE phases against EARLY
ones, silently, across a campaign. **Drop each database immediately after its arm**, not just before.
Cross-phase comparisons of absolute throughput are unsafe without this; within-phase comparisons survive.
It does not take 65 of them: **four** left on an 8-vCPU instance (3.9GB) roughly HALVED the next arm's
throughput at identical settings. Autovacuum is the mechanism worth knowing — its workers are per
INSTANCE (3 by default), so an abandoned database full of dead tuples spends workers that the live one
needs, on top of the disk it holds.

**A 10-minute arm is the shortest one worth trusting, because this system stalls for 30–60s at a time.**
The same 8-vCPU configuration reports 99.9% of command over 600s and something close to catastrophic over
a 120s window that happens to land on a stall — both true, same run. Two consequences for arm design:
**state the window length beside every throughput number**, and never read a single short arm as a
property of the configuration. The stalls are invisible in throughput anyway (every arm of a six-rate
sweep delivered ~100% of command, including one whose p99 was 62 SECONDS) — **watch p99/p50**, which ran
1.2x when clean and 6.5x at the onset. The stall recurs and clears itself with the offered rate unchanged
(one pool and rate on 64 vCPU: 9,183 steps/s with a 75s p50 in one 120s window, 19,685 in another), and a
collapsed arm has been bracketed by working pools on both sides - **never place a stability boundary from a
collapsed arm**.

**The run-to-run noise floor is ~5-12% and rig-dependent - establish it before reading any curve.** Two
same-spec 64-vCPU instances measured 25,366 and 28,491 steps/s at one pool; two 16-vCPU rigs 7,296 and 8,342.
Only within-rig comparisons resolve anything finer.

**Count backends, not connections, before calling a far-RTT budget an over-connection.** At 1 ms a
connection is inside the database well under half the time: 384 connections on 16 vCPU ran ~338 active.

**To reproduce a stall, command BELOW what the database can sustain, not above it.** Pushing harder looks
in the wrong place. In a 3x3 Latin square (PostgreSQL 16, 16 vCPU, one shard, linear, open loop, 120s
windows, each rate run once in each sequence position), the spread across repeats was **1.83x at 700
flows/s**, 1.26x at 800, and **1.04x at 900**. 900 was the only rate above the sustainable rate. A
saturated arm keeps a standing backlog, so it has no healthy state to fall out of. That is the same property
that makes over-saturating the right way to size a pool (below), and the reason it can never show you a
stall. Rotate sequence position when chasing one: position means were 6,536 / 6,498 / 5,910, so order was
not the signal here, but that has to be measured each time, not assumed.

**To size a pool, command ABOVE saturation and read the 600s mean — do not sweep for the smallest pool
that "sustains rate R".** The second form has to choose R, and the choice decides the answer; it also
reduces to a binary that a short window decides by where a stall landed. Over-saturating removes both
problems: the machine's maximum needs no target, and a mean absorbs transients rather than being flipped
by them. Two rules for such a sweep, both learned by getting them wrong:

- **Pin `-max-connections`.** The engine derives its pool from a Startup RTT probe, which varies run to run
  (idle 0.157-0.182 ms on one rig) and moves the budget along its ramp. An unpinned sweep confounds pool size
  with probe noise.
- **Set `-max-outstanding` LOW, not high, so every arm is closed-loop at the SAME backlog depth.** With a
  high cap the starved arms pin at the cap while the healthy ones sit lower, and since the band scan is
  O(backlog) the starved arms pay a second penalty — biasing the result toward the larger pool. (`main.go`
  will also mark those arms invalid, which is correct for a commanded-rate arm and a false alarm here.)

**`-arrival-stop` measures recovery, which no other flag can.** It halts admission partway through the
window while the engine keeps running, so the remainder is a drain under load that has ABATED rather than
one still being offered — the difference between "the backlog is self-sustaining" and "it was being
refilled faster than it emptied". Measured: a 42,134-step backlog drained to zero in 76s. The window mean
spans both phases and is meaningless by construction, so read `-stats-interval`, not the artifact.

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

**The same degraded state slows the WAL flush, comes and goes mid-arm, and outlives the database.** On Docker
Desktop a saturated local arm can run at roughly half speed for a minute or more and then recover by itself,
with nothing changed. Its signature: both phases wait on `LWLock:WALWrite` at the same commits per WAL sync,
and the slow phase completes about half the syncs per second - each flush takes twice as long - while RTT is up
with it. Autovacuum and checkpoints do not line up with its edges. It is not state the engine or the database
holds: it carries across separate bench processes and freshly created databases, landing on whichever build
happens to run. Nothing in dwarf moves it, so design around it: the RTT gate catches an arm that STARTS degraded
but not one that degrades or recovers inside the window, so read `-stats-interval` lines rather than the window
mean, compare arms by database time per step rather than steps/s, and never read a local throughput difference
between two single arms as a property of either build.

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

## Running the cloud rig

**The kit is `bench/gcp/rig/`.** Each orchestrator creates its rig, sources a step script, pulls the artifacts
into `bench/results/redo-<name>-<date>/`, and tears everything down on exit, failures included.
- **`x-rig.sh <name> <disk-gb> <step>`:** one Cloud SQL instance (`TIER`, default 32 vCPU) and one engine/admin VM
  (`VMTYPE`, default c4a-16).
- **`xp-rig.sh <name> <tier> <vm-type> <tag>`:** the tier table's pairing, two engine hosts sharing one database,
  plus an admin VM.
- **`s2-rig.sh`:** the shard ladder on one set of four instances: a parallel solo draw check (redraws a slow one),
  then 4 shards on one c4a-48, 4 x 8 c4a-8, 2 x 4 and 1 x 2, deleting instances and VMs as the count falls. Gates
  every arm on `rtt.sh` (one `psql` session per DSN, min of a dozen `SELECT 1`) from every engine host.
- **`real-rig.sh`:** REAL, a day on one database: a draw check, the mix's calibration, then three 90-minute periods
  on two c4a-8 replicas with a rolling restart, verified against the database at the end.
- **Steps:** `one_run` from `a-step-lib.sh` runs one arm on a fresh database. `vrows-step.sh` and `vbytes-step.sh`
  are V's two axes, on `x-rig.sh` with a 1 TB and a 2 TB disk. `*-step.sh` are the arms each
  campaign used, with their pass criteria in the header.
- **What stays out of git** lives in `$RIG_HOME` (default `tmp/rig/`, ignored): `dbpass` and the pinned
  `bench-<sha>` binaries (`CGO_ENABLED=0 GOOS=linux GOARCH=arm64`, built from a clean clone).
- **Gate every quoted rig on its draw:** run `bench/gcp/dbcpu.sh -live 3` a few minutes into the first window.

Operational traps on Cloud SQL + GCE, each of which has cost a run.

- **Same-spec instances are not the same machine, and a slow draw looks ordinary.** On one build and one rig
  shape, 32 vCPU instances ran 15.9k-24.1k steps/s. The slow ones burned ~1.5 DB core-ms per step against
  ~1.1, at the same ~77% CPU, so neither steps/s nor CPU alone gives them away. `dbcpu.sh` prints core-ms per
  step; read it early, and redraw rather than quote a slow instance. Nothing in how an instance is created picks
  the draw: slow ones came up with and without `--database-flags`, alone and beside other saturated instances.

- **Cloud SQL provisions IOPS by disk size (~30 IOPS/GB).** A 100 GB disk gives ~3,000 IOPS (~23 MB/s of
  random 8 KB I/O), and a write-heavy or volume run then measures the disk: the fill rate falls off a cliff
  rather than a slope, with requested checkpoints far outnumbering timed ones. Use 1 TB (~30,000 IOPS) unless
  disk size is the variable.
- **Read the database's own log when a run dips.** Cloud SQL Postgres logs checkpoints and autovacuum to Cloud
  Logging, and the log outlives a deleted instance. At ~70-110 MB/s of WAL the default `max_wal_size` made a
  32 vCPU instance checkpoint every 3-16 s for a whole run ("checkpoints are occurring too frequently"), and a
  checkpoint that REMOVED old segments instead of recycling them forced new ones to be created: one backend in
  `IO/WALInitSync`, ~250 queued on `WALWrite`, dispatch down ~65% for ~10 s.
- **A wait-event profile is not a utilization figure.** `pg_stat_activity` samples show the queue, not the
  cores: an instance at 80% CPU read 77% `LWLock:WALWrite` against 7% `CPU:running`. Pair it with the
  instance's CPU utilization metric before calling anything spare.
- **Open-loop p50/p99 come from COMPLETED flows only**, so a long-task arm mid-backlog reads a spurious ~0.
  Judge such an arm on steps/s, flows/s against the commanded rate, and `maxOutstandingObserved`.
- **`INTERNAL_ERROR` on `gcloud sql instances create` can be partial:** the instance is created and reaches
  `RUNNABLE` while the command fails. Wait for `RUNNABLE` and carry on; do not create a second instance.
- **Never rewrite a rig script in place while a rig is running it.** Bash reads a script as it executes, so
  truncating and rewriting the same file (an editor's save, `open(p, 'w')`) hands every running copy garbage at
  its current offset: three rigs died at once mid-provision that way, each tearing itself down. Write a new file
  and rename it over the old one (`sed -i` does), or copy the kit before changing it.
- **`pkill -f`/`pgrep -f` over ssh match their own command line**, so a poll for a finished driver reads it as
  running forever. Match the binary's exact name (`-x`), or poll a marker file the driver writes.
- **`DROP DATABASE` needs `WITH (FORCE)` after engines are killed** - their connections outlive the process
  long enough to block a plain drop. Wait out the peer freshness window (~40 s) before reusing a database, or
  the dead engine's row inflates the next run's replica count.
- **gcloud auth expires mid-session.** An orchestrator must check `gcloud auth print-access-token` before each
  step and wait rather than fail, or an unattended rig strands resources. Runs started with `setsid nohup` on
  the VM survive an expiry; the workstation's monitoring does not.
- **macOS deletes files in `/private/tmp` that have not been touched for a few days**, including a session
  scratchpad. A rig whose password file or chart virtualenv lives there breaks silently the next day (an empty
  root password on new instances; a matplotlib without `__init__.py`). Check them before launching, and keep
  anything long-lived outside `/private/tmp`.

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

**SIZE THE GENERATOR BEFORE BOOKING THE RIG — the harness costs ~2,755–2,881 steps/s per host core.**
Measured on `linear` across a 2× load range (11,953 → 23,178 steps/s), and flat over it, so it multiplies
out honestly: a 16-vCPU host ceilings near **46,000 steps/s**, all in, since `cpuCores` covers the load
generator *and* the engine in one process. That carried a 64-vCPU database (ceiling ~25,500 steps/s) at
51–60% host CPU with room to spare, and it is why those arms measure the tier. Against the next tiers up
it does not: ~35–40k steps/s would sit at 76–87%, and a 128-vCPU database's rate is simply out of reach.
Two constraints make this awkward to solve by reflex. The generator must share the database's zone (a
cross-zone hop adds ~1.1 ms and moves the very axis under test), and large instance types stock out
per-zone — `c4a-standard-32` has been unavailable in the zone a run needed. So check `steps/core`
against the target tier's expected ceiling first, and if one VM cannot cover it, shard the generator
rather than accepting the number.

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
`steps/s` is the executed-steps counter's delta since the previous line, and each line is also kept in the
artifact as `results[].gauges.series` (`t`, `stepsPerSec`, `outstanding`) - the only record of a window's shape,
since `stepsPerSec` is its mean. `cell.sh` reports each run's slowest and fastest interval from it.
On Postgres each point also carries the interval's WAL pressure - `walMBps`, `commitsPerSync`, `fpiPerSec`,
`walBuffersFull`, `walWriteWaiters` and `activeBackends` (means per 50 ms wait poll) - and `results[].db` the
window totals (`walFpi`, `walBuffersFull`, `walSyncs`, `commits`). A single Postgres instance is WAL-write-bound
at its ceiling, so these are where to look for the signal that a run is past its sustainable rate. The flush
count is `pg_stat_wal.wal_sync` through PostgreSQL 17 and `pg_stat_io` (`object='wal'`) from 18; `-1` means the
server exposes neither. Flush LATENCY (`wal_sync_time`) needs `track_wal_io_timing`, which Cloud SQL does not
allow on PostgreSQL 16.

⚠️ **GAUGES HAVE NO BARE-NAME TOTAL — sum across the attribute sets.** Counters emit `name` alongside
`name|k=v`; gauges emit only the attributed series (`dwarf_turnstile_available|shard=1`,
`dwarf_steps_pending|priority=100|shard=1`). Reading the bare name gets a confident **0.0**, which is what three of
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

- **`results[].db` is what the database stored and logged over the window** - table + index + TOAST growth
  and `pg_stat_wal` bytes (PostgreSQL 14+, cluster-wide, so only meaningful on an instance nothing else
  writes to). Divided by the task-declared bytes behind MB/s it is the amplification. Expect `final_state`
  to hold a second copy of any large field that survives to the end of the flow: a flow-boundary value is
  always flattened.
- **An open-loop window's `flowsPerSec` is the ADMISSION rate, not what was served.** Flat out, admission
  runs ahead of completion by up to `-max-outstanding` flows: a SQLite smoke admitted ~160 flows/s while
  executing 532 steps/s of a 10-step chain (~53 flows/s), so "2/3 of its flowsPerSec" commanded ~2x the
  ceiling and the "operating point" window had a 2.5 s p99. Take a served rate from `stepsPerSec` divided by
  the workload's steps per flow - as `cell.sh` and `-volume-probe` do.
- **Open-loop latency is sampled across all creators**, ~16 flows/s at a commanded rate. Do not count samples
  per creator: on a slow (payload-heavy) arm with many creators, each makes fewer flows than the stride and
  nothing is sampled at all.
- **Counters carry attributes** as `name|k=v` alongside the bare-name total. Both are emitted: the total is
  what a throughput number reads off, the split is the only thing that shows one shard falling behind.
- **Gauges several replicas can report for one fact are MAXed, not summed** (`agreementGauges`) — per-shard
  readings (`dwarf_steps_dispatch_lag_seconds`, `dwarf_steps_pending`, `oldest_pending_age`) and per-replica
  readings of a shared fact (`dwarf_peer_replicas`, `dwarf_peer_blind_seconds`). Summing them multiplies by the
  replica count, so before leaning on a multi-replica backlog-depth or oldest-age figure, check the artifact
  MAXed it.
- **`dwarf_refill_query_duration_seconds`'s COUNT is cycles per window — of WHICHEVER LOOP emits that
  phase, and there are two.** `band_keys` comes from the piston's Tallier, `planning`/`fetch_steps`/
  `pushing` from its Supplier, and the two are paced independently, so the counts are unrelated: measured
  76 against 8,215 in one 60s window, a 108x gap. **DO NOT SUM THE FOUR PHASES** — the total is two
  unrelated clocks added together and reconstructs no single object; there is no "a cycle" left to
  reconstruct, and no end-to-end cycle histogram (`engine/CLAUDE.md` records why there is none).
  Read `fetch_steps`' count as the SUPPLY rate (what reaches the workers) and `band_keys`' as how often
  the fairness tally refreshes.
- **Per-shard cycle-duration spread is mostly POOL WAIT, not a slow shard.** Measured 28ms vs 125ms on
  hardware whose RTT differed by 0.036ms. Decompose against `pg_stat_statements` and the pool-wait gauges
  before calling a shard slow. Note the pgss/dbstats/RTT samplers watch `dsns[0]` only, so a multi-shard run
  has no server-side counterpart for shards 2+.
- **`valid: false`** means the run is not comparable, and `invalidity` names the first reason: a recovery or
  unwedge counter fired, a flow errored, a window stalled, the drain left a flow unfinished, failed or
  missing, a metric collection failed on any replica, or `-max-outstanding` bound under a commanded rate.
  Each sweep window and volume probe also carries its own `invalidity`, so a chart can drop one window
  without dropping the run.

## Run guards: the watchdog and the drain

**Every mode runs the stall watchdog** (`watchProgress`): sweep windows open- and closed-loop, the volume fill
and soak. It fires when no task has started and no flow has ended for `-stall-timeout` while work is in
flight, and that ends the RUN, not the window: the artifact is written, and the process exits 3 without the
engines' shutdown, which would wait on whatever hung. **A phase waits on its submitters through the watchdog
(`progressWatch.await`), and stops the watchdog only after that wait.** A submitter stuck in `Create` or `Run`
on a hung engine never returns, and one wedged flow among healthy ones is masked while the others keep tasks
starting: it shows only once they have exited, so a wait taken after `stop()` hangs on exactly that flow
(probed: 18s past a 2s stall timeout and still waiting).

**Silence is measured between task starts as well as flow ends**, by the watchdog and the drain alike. Flow
ends alone go quiet for a whole flow's remaining length - 10-step flows of 60s tasks in lockstep end nothing
for 10 minutes while their tasks keep starting - so a guard reading only flow ends invalidates healthy
long-task runs. Counted this way, the longest gap a healthy run shows is one task, which is the bound the flag
checks hold `-stall-timeout` and `-drain` to.

**The drain gives up on silence, never on a wall-clock bound**, and giving up is a stall. A bound invalidates
every slow cell: 20,000 outstanding 10-step flows need 300s even at ~667 steps/s. And since its silence is
the watchdog's own definition of a hang, met after the watchdog stopped, a drain that gives up ends the run
too - the next window would start on the hung engines.

**The drain's accounting is per PROCESS, so a fleet of bench processes on separate hosts fails it falsely.**
Each process counts flows created by its own load generator but flows completed by its own engine, and a
replica completes its peers' flows too. The first process to reach zero exits and finishes more of its peers'
flows during its shutdown, after its counters were read, so a peer is left reporting them "unfinished" until it
gives up. Measured: 110 "unfinished" on one host, which was exactly 16 counted by the other plus 94 finished in
its shutdown, while the database held every flow `completed`. In a multi-process run, verify with status
counts in the database before dropping it, and read the per-process drain verdict as advisory.
`-replicas N` (in-process) is not affected, since one process sees every engine's counters.
A process whose engines dispatch NO shard trips the watchdog the same way: peers run its flows, so nothing it
counts moves. Its verdict says "dispatched no shard" - read that as a fleet-shape fact, not a hang.

**A collection that failed on any replica is a failed collection**, never the other replicas' sum. Counters
and gauges come back nil, histograms report `histogramsMissing`, a gauge-sampler tick is left out of the means
(`gaugeSamplesFailed`), and a soak sample says `collectionFailed`: its alarms are differences against the
soak's opening collection, so a missing reading at either end reads as zero alarms.

**The volume fill tracks its progress from the executed-steps counter**, and takes the exact `COUNT(*)` only
at a checkpoint. Polled, the count is a full scan of both tables that holds a snapshot for its whole run, and
at a size where it outlasts its timeout the fill never reaches a checkpoint at all.

## Flags worth knowing

`-payload P` (fresh incompressible bytes per `linear`/`fanout` task, the element size of `scatter`; 0 = none
for those three, 64 KB for `state`/`carry`), `-workload scatter -branch-depth D` (the doc -> pages shape that
item refs change; each branch checks its element arrived whole), `-volume-probe D` (a saturated and a 2/3
window at each `-volume` checkpoint, the backlog drained between them), `-print-pool RTT -server-max-connections N` (the
budget the engine derives at that RTT against that server limit, for a script to pin), `-open-loop -arrival-rate N -max-outstanding M` (commanded rate + backpressure bound), `-replica-workers`
(per-replica worker counts; `0` = await-only, `1` = capacity-crippled and grow-on-demand disabled),
`-linear-steps` (chain depth — under partitioning, depth changes the share of FLOWS a slow replica blocks
without changing the share of STEPS), `-refill-interval-ms` (pin the cycle period; **mandatory** in any
ladder that varies replica count, since the derived interval is a function of R and would otherwise
masquerade as a partitioning cost), `-fairness-keys`, `-max-connections` (the `ShardSpec.MaxConnections`
lever: the fleet-wide budget, so with `-replicas` above 1 it is split across the shard's dispatchers rather
than given to each replica, and a stale `dwarf_peers` row halves it), `-task-burst`/`-task-quiet` (alternate
the exec term within a run — the only way to see the crew shrink), `-task-delay-dist "0.80:5ms,0.15:200ms,0.05:8s"`
(task durations drawn per invocation from weighted classes, so several durations share one crew; replaces
`-task-delay`), `-state-cache-mb` (`SetStateCacheSize` for
every replica; 0 derives it from `GOMEMLIMIT` or the host's memory - the artifact's `environment` records
`goMemLimit` and `GOGC`, which a payload arm's heap depends on as much as on the flag), `-stats-interval`
(periodic readout), `-pprof` (CPU + heap profiles), `-log-level` (the engines' own log on stderr, `warn` by
default; the engine's default logger discards everything, so without it a role change or a stale peer row
leaves no trace in a rig's log), `-mix "0.85:linear:1K,0.15:fanout:4K:8"` (per-flow classes, each its own
payload and shape under its own graph URLs; usable in any mode), `-volume-checkpoints` (the step-row counts a
`-volume` fill probes at), `-volume-checkpoints-mb` (the stored sizes it probes at instead, PostgreSQL only - the
axis for a bytes fill: two 0 B probes on an empty database wrote ~15M small step rows, twice a 512 GB fill of
64 KB rows, so a row-keyed ladder for bytes is consumed by the probes before the fill starts),
`-volume-probe-two-thirds=false` (one saturated probe per checkpoint instead of two windows).

**`-real D` runs a schedule, not a window.** `-arrival-profile` (levels and ramps in fleet steps/s) and
`-arrival-bursts` repeat every period, with seeded per-minute `-arrival-noise` and Poisson arrivals, plus a
`-high-priority-share` band, Zipf tenants (`-fairness-skew`) and a `-retention` Purge. Four things about it:
- **The schedule is a function of the time since `-arrival-epoch`.** Give every process of a fleet the same
  epoch and its `-arrival-share`, and a process restarted mid-run rejoins in phase and stops at the shared end.
- **The record is a JSONL series beside the artifact**, one line per `-stats-interval`, written as it goes, with
  commanded, offered and admitted flows/s side by side - a `Create` that slows in a burst shows as admission
  falling short, not as a smaller overload - and p50/p99 of the flows that completed in the interval, by class,
  band and top tenant against the tail.
- **SIGTERM is a graceful stop** (stop creating, write the artifact, let the engines drain), which is what a
  rolling restart sends. At the schedule's end a process instead waits until no flow is running on the shard,
  asking the database rather than its own counters, so every flow can be checked for an end afterwards.
- **A process's counts are its own.** Admitted flows are the ones it created and ended flows the ones its engines
  ended, so a fleet's totals are the sum of its processes' series.

**Collecting the engine's gauges costs the database nothing, however often this harness collects.** The per-band
backlog (`dwarf_steps_pending`, `oldest_pending_age`) is an O(backlog) aggregate the shard's leader runs in the
background every 30 s once a collection has asked for it, and the 50 ms collection reads its cache - so a window
shorter than 30 s may have no per-band reading at all, and none is fresher than 30 s. The live per-window
signal is `dwarf_steps_dispatch_lag_seconds`, read from the band scan; the series' backlog depth is the load
generator's own `outstanding` (flows created, not yet ended; open loop only).

`bench/gcp/cell.sh` drives one cell of a tier x RTT grid end to end - peak at the derived pool, a pool sweep
at 2/3 of it, a confirming peak when the sweep moves the pool - and writes `cell.json` beside the artifacts.
`RATES` adds a rate ladder (fractions of the measured peak), `STEP_RATES` one in absolute offered steps/s that
needs no peak, `PIN_POOL` replaces the derived pool and `NEAR=1`
runs at the rig's own RTT without netem (with `RTT_SLACK_MS=0`, every run starts at or under the target), which
together with `SWEEP=""` and `MAX_OUTSTANDING` cover the method pilot: backlog-cap independence, the rate
ladder, and the k/s fit at a small fixed pool. Every artifact's `environment` carries `vcs.revision` and
`vcs.modified` when the binary came from `go build` in the work tree - build every run of a campaign from one
clean commit;
`bench/charts_redo.py` charts a tree of cells and `-volume` artifacts. Run `dbcpu.sh` with `WRITE=1` first so
the artifacts carry database CPU.

`bench/charts_cloud.py` draws `docs/benchmark-cloud.md`'s charts and `bench/charts.py` the local dialect charts in
`docs/benchmark.md`. A cloud chart drawn from many runs or from CPU profiles keeps its numbers as a table in the
script, because no single artifact holds them; the rest read their artifacts under `bench/results/`.
**`bench/results/` is gitignored**, so those charts can be redrawn only on the machine that holds the run's
local results - a clean checkout cannot rebuild them (`charts_cloud.py` skips a chart whose artifacts are
missing, with a note).

Test-only knobs the bench CANNOT reach: the engine's seams are enabled only under `testing.Testing()`, so
arming one from `internal/engineseams` in a bench binary does nothing, and fault injection and `sequel`'s
`SimulateRTT` are available to fixtures but not here. Reaching them from a
benchmark needs real API, not a back door.
