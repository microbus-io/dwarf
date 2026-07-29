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
404, in the *same* arm. Before attributing anything to a code change, check the RTT sampler. The cost model
that explains it is `db_time = k·L + s` with **k ≈ 9.6 round trips, s ≈ 3.3ms** re-derived locally (the
cloud campaign's k ≈ 11-12, s ≈ 4.4ms), and throughput ≈ `pool / db_time`. A ceiling can therefore be
predicted from RTT alone, and a run at 78% of it is a *database* result, not an engine one.

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

## Reading an artifact

- **Counters carry attributes** as `name|k=v` alongside the bare-name total. Both are emitted: the total is
  what a throughput number reads off, the split is the only thing that shows one shard falling behind.
- **Gauges every replica reports identically are MAXed, not summed** (`agreementGauges`) — cluster-wide
  reads (`dwarf_steps_pending`, `oldest_pending_age`, `task_concurrency_running`) and per-replica readings
  of a shared fact (`dwarf_peer_replicas`, `dwarf_peer_blind_seconds`). Summing them multiplied by the
  replica count; that bug inflated those three in **every multi-replica artifact produced before it was
  fixed**, so distrust older cloud numbers that lean on backlog depth or oldest-age.
- **`dwarf_refill_duration_seconds{shard}`'s COUNT is cycles per window** — i.e. piston RPS — and its sum
  is the duty cycle. There is no separate revolutions instrument.
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
masquerade as a partitioning cost), `-fairness-keys`, `-vcpus`, `-max-open-conns`.

Test-only knobs the bench CANNOT reach: `Engine.Seams()` and `Engine.DB()` panic outside a test binary, so
fault injection and `sequel`'s `SimulateRTT` are available to fixtures but not here. Reaching them from a
benchmark needs real API, not a back door.
