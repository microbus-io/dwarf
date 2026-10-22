# Dwarf `internal/piston` — the engine's per-shard cylinder

> Load when: changing `Run`, `Liveness`, either source query, the steal, the idle mode, or the
> instruments.
> Coupled with: `internal/pipeline/CLAUDE.md` (the cycle this drives and its error policy),
> `internal/planner/CLAUDE.md` (`Tally`/`Clear`), `internal/migrations/CLAUDE.md` (the `dwarf_steps`
> columns these queries touch), and `internal/peers/CLAUDE.md` (what consumes `Liveness`, and where the
> partition pair it is handed comes from).

One piston works one shard: it fires the same supply cycle over and over against its own database, on its
own clock, with no barrier against its peers. A fleet of shards is a fleet of pistons, and an engine with
N shards runs N of them.

**It is a CONSUMER of its database, never the owner.** The handle arrives already open and is closed by
whoever opened it, so there is no `Open`, no `Close`, and no pool control here. That is also why it is not
called a *shard*: the shard is the database partition; this is the thing that works it. A type named
`Shard` holding a `*sequel.DB` it does not own, next to a `ShardSet` that does, would be ambiguous at
every call site.

It **owns** the pipeline, the two queries behind it, and its instruments. It **borrows** the planner and
the candidate cache, both shared with every other piston on the replica. It publishes nothing about this
replica anywhere — `Liveness` is a pure read for whoever does.

## `Run` — TWO loops, one goroutine each

```
tally  (paced by the pipeline) -> record -> repeat
supply (paced by the pipeline) -> record -> repeat
```

`Run` spawns both and blocks until both return; the caller puts `Run` itself in a goroutine and waits on
its own WaitGroup. There is no `Stop` — a cancelled context ends both loops, and that is safe with no
second signal because **both queries are read-only**: nothing to commit, nothing to strand, so abandoning
mid-flight is free. Cadence lives entirely in the pipeline, so neither loop holds timing policy of its own,
and `SetIdle` parks both.

**Why two.** The band scan costs O(due rows at the band) and grows with the backlog, so fused it set the
whole cycle's period and therefore the rate candidates reached the workers — the refiller supplied least
exactly when the backlog was deepest. The measurement, the ~10x it is worth, and what goes stale instead
are all in `internal/pipeline/CLAUDE.md`. What belongs *here* are the three consequences for this package:

- **Two concurrent connections per shard** while a scan and a fetch overlap. Structural and visible at this
  level, where a goroutine hidden inside `ScanBand` would have concealed it.
- **CADENCE IS PER LOOP, and nothing here may key off "the" period.** The only setters are
  `SetTallyCadence`/`SetSupplyCadence`, one per loop, and there is deliberately no combined knob over both:
  the two are derived from different quantities (the supply loop paces against the drain it fills for, the
  tally loop against a scan outrunning its own interval), so one number over both expresses a cadence no
  owner means. Consequently every threshold here asks the LOOP: `Liveness` compares each loop's `WorkingFor`
  against **its own** `Period()`, and `stealGrace` takes the **longer** of the two (reaching one's own work
  is a full round trip: a scan to tally it, then a supply cycle to fetch it — the slower loop is what bounds
  how fast a healthy owner gets there, and keying off the faster one under-states the grace, which admits a
  healthy peer's work). Pinned by `TestPiston_CadenceIsPerLoop`, which sets the two loops apart deliberately.
- **`Liveness`'s turn count is the MINIMUM of the two loops'** — see below.
- **The steal must not acquire a flag one loop writes and the other reads** — see below. Anything shared
  between them is sampled at an arbitrary phase of the other's cadence, and the phases are not independent:
  both loops start together and the tally's interval is derived from the supply's - the owner multiplies it
  by the number of scanning pistons and clamps the product at 1s, so the two are equal with one scanner, a
  whole multiple below the clamp, and a fixed ratio at it - so a "race" of that shape lands the same way
  every time rather than half the time.

## `Liveness` — a counter, and why publishing is somebody else's job

**How often this replica's liveness is published must not depend on how long a cycle takes**, and that was
a real bug when the two shared a goroutine. The band scan is O(due rows at the band) on **every** dialect
(see below), measured in the **tens of seconds** at a few million due rows. A signal gated on a cycle *returning* lets one such scan drop a
**healthy** replica out of its own fleet — shrinking every peer's pool divisor and reshuffling every
ordinal — exactly the outcome that should mean "the process is stuck, nothing less." Reporting rather than
publishing removes the coupling by construction: the reader samples on its own clock.

Four things the shape has to get right:

- **A counter, never a flag the reader clears.** A consuming getter would create a contract — call it once
  per publication, from one caller — and any second caller (a metric, a test) would silently swallow the
  evidence. Holding "since I last looked" belongs to the reader.
- **The count is the MINIMUM of each loop's own count, and EITHER loop alone reports a broken piston as
  fully alive.** A turn has to mean this piston can both LOOK at its shard and SERVE it, and the two failures
  are exactly symmetric:

  | every… fails | what the piston still does | which counter keeps moving |
  |---|---|---|
  | SCAN | clears itself from planning, selects nothing | the SUPPLY loop's — it plans from an empty planner, pushes nothing, reports no error |
  | FETCH | tallies honestly, claims its band, takes zero candidates | the TALLY loop's — the scan is perfectly healthy |

  Either way the replica holds a residue class nobody else will select, which is the whole stranding this
  evidence exists to prevent. The minimum stalls when either loop does. Each loop writes only its own
  counter, so this adds no cross-loop state. Pinned from both sides by
  `TestPiston_FailingCyclesReportNoLiveness` and `TestPiston_FailingFetchesReportNoLiveness`, each of which
  fails against a build that takes the count from the other loop.

  **The fetch side is the one with no other signal.** A failing scan at least logs and clears the shard; a
  failing fetch leaves an honest tally, a claimed band, and a silent partition — no error counter names it,
  and the metric signature is a normal `fetch_steps` duration with `selected` at zero on one shard. That is
  also the shape a bind-count overflow produces, which is why `FaultFetchErr` exists at all.

  **A residual blind spot: a HUNG loop reads busy forever.** `busy` covers a scan that is merely slow (it
  reads either loop's work window), but one that never returns is indistinguishable from a legitimately long
  one — the same false-alive trade the section below records. `dwarf_refill_tally_age_seconds` (the
  planner's `TallyAge`) is the signal that distinguishes a rotting tally from a fresh one.
- **A turn in flight counts, but only once it has outrun the cycle period.** Same O(backlog) argument — a
  piston in the middle of a long scan is plainly still serving — but "in flight" alone is the wrong
  predicate. A cycle whose scan fails *instantly* is also briefly in its queries
  (building the error, recording the phase, logging it), measured at **~1.2% of samples**, and a reader
  sampling every 50ms catches that within seconds. It then keeps a piston that serves nothing looking alive
  indefinitely — the precise stranding the evidence exists to prevent. A scan shorter than the period will
  have completed and advanced the turn count before any reader looks, so nothing is lost by requiring the
  duration. Pinned by `TestPiston_FailingCyclesReportNoLiveness`, which samples ~1ms apart for half a second
  and requires *zero* busy readings; against the bool it fails with 16 of 438, and 333 of 440 at the zero
  period a bench sweep or a hand-driven caller can legitimately set — which is why the threshold is floored
  at `MinGap`, the package's existing fuse for that same degenerate regime.

  The threshold bounds the **false-alive** direction only. A query that hangs forever reads busy forever and
  keeps its residue class — indistinguishable from a legitimate long scan by construction, and the same
  trade the bool made. That is accepted: a piston stuck inside a query genuinely is a piston nobody should
  be redistributing work away from until its lease-recovery-shaped problems surface elsewhere.
- **A cycle that found nothing due still counts.** It proves the piston looked and could have served;
  gating on candidates instead would make a quiet fleet read as having no dispatchers at all, disable
  partitioning, and then thrash when work arrived.

## Idle

An idle piston runs no cycle and claims no work, and says so through `Liveness` — so its owner can keep
the replica counted for the connections it still holds while excluding it from anything that divides work.
That is the await-only replica.

**Going idle WITHDRAWS the shard, and skipping that wedges the replica.** The planner's contract is that
every shard either tallies or clears each cycle; an idle piston runs no cycle, so it does neither, and its
last tally would stand forever. The planner is **shared** across the replica's pistons, so a stale claim
on a better band that fills the batch is the documented wedge — every live piston finds no capacity left to
spill into and dispatches nothing, indefinitely. It is benign only when every piston is idle, and `SetIdle` is per-piston,
so the API permits the bad case. `SetIdle(true)` therefore does the same two things an empty plan does:
`planner.Clear(shard)` and `cache.Refill(shard, nil, NoBand)` — the partition for the same reason, since a
dead hint costs a worker a claim round-trip. Pinned by `TestPiston_IdleWithdrawsTheShard`, which asserts the
release from a *peer's* point of view rather than just the local state.

**`SetIdle` ALONE IS NOT SUFFICIENT, and this is the whole of why each loop withdraws again.** The setter can
only clear what is there when it runs. A loop already inside a cycle — past its own idle check — finishes
*afterwards* and re-enters the shard it just withdrew, or re-pushes the partition it just emptied; both loops
then park and the stale tally stands **forever**, which is precisely the wedge above. The window is not a
sliver: the band scan runs for seconds on a deep backlog. So each loop repeats its own half on the way into
idle — the Tallier `Clear`s, the Supplier empties the partition, the same seam the error policy splits on.

**The flag that makes it once-per-transition starts TRUE.** It exists to undo what a COMPLETED cycle
re-inserted, and a loop that has not run one has nothing to undo. Starting it false makes an
already-idle piston wipe state on its first iteration, racing whatever its owner set up between `Startup`
and then — caught by `TestFault_RefillScanErrPreservesCache`, which seeds an await-only engine's partition
by hand. Pinned by `TestPiston_IdleWithdrawsAgainstARunningLoop`, which blocks both loops inside a cycle
through the context func and only then goes idle; without the loop-side withdrawal the idle shard's band
claim outranks a live peer's forever (measured: peer sees band 5, not the 9 it holds).

The default is *not* idle, deliberately: a fresh piston dispatches, which is the common case, and a zero
value that silently did nothing would be the worse default.

Note the word is overloaded against the refill vocabulary, where *idle* means **nothing is due** — a
circumstance a dispatching piston meets constantly. Here it is a configured **mode**.

## The two queries

They implement `pipeline.BandSource` and `pipeline.StepSource` — one per loop, so neither loop can reach
the other's query — which is the whole reason the piston owns both. Nothing else has the handle. Their per-query rationale lives in their doc comments; the cross-cutting rules are:

**The band scan is TWO statements, whatever the number of bands.** Priorities run `1..MaxPriority` (5 by
default, at most 100 - the engine's `SetMaxPriority`), so the bands are known in advance:

1. **Counts** (`bandCountsQuery`): every band, each counted up to the cache capacity by an equality seek with a
   `LIMIT` inside. From them the scan knows exactly which bands a fill can reach - every band up to the one where
   the counts reach capacity (`internal/planner/CLAUDE.md`: no fill reaches past it).
2. **Tally** (`bandTallyQuery`): one `GROUP BY` per reached band, each with its band inlined, and nothing beyond
   them. A deep band under one that fills the cache is never read.

Nothing due costs the first statement only. Both are `UNION ALL`s of per-band branches, and that shape is what
keeps every band a seek:

- **Do not count or tally across a priority range.** The due index leads `(parked, priority, not_before)`, so
  only an equality on `priority` makes the `not_before` range a seek; across a range the scan reads every pending
  row, sleepers included. A sleeper-only band is an empty range under the equality - its sleepers are past the
  seek's upper bound on `not_before` and are never read.
- **Do not count with one `GROUP BY priority`.** It has no early stop, so a deep band below one that fills the
  cache is read in full on every scan; the per-band `LIMIT` caps each band's cost at the capacity.
- **Each branch inlines its band**, so a planner sizes it from that band's statistics. **Do not make the band a
  subquery value**: the planner then sizes it from the AVERAGE band, and on PostgreSQL 18 an under-estimated deep
  band chose the due index plus a whole-band sort.
- **The counts statement's cost scales with the configured maximum, not with the bands in use**: one index probe
  per empty band, at most the capacity per band with due work, and - on MySQL and SQLite, which keep no plan across
  executions - parsing and planning one branch per band on every tally. That is why the maximum is capped at 100.

**Work at a priority beyond `MaxPriority` is an OVERFLOW band, never stranded.** `Create` refuses such a priority,
but a step can still carry one: a flow from before the range existed, from a replica configured with a larger
maximum, or inherited from either by `Continue`, `Fork` or a subgraph. The counts statement has one extra branch
counting every priority past the maximum together, and when the fill reaches it the tally groups those rows by
their actual priority, so they are served after every band in the range, in their own order, and the fetch
matches them exactly. **Do not drop the overflow to keep the statement to the range**: a step outside it would
then sit pending forever with no error anywhere. In normal operation it is empty and costs one index probe; its
range on `priority` reads the overflow's sleepers, which only out-of-range work has.

The statements' texts change with the reach and with the cache capacity they inline, so each distinct text is its
own prepared statement. **The tally covers every band up to the reach, empty ones included, and that is what bounds
the set**: at most `MaxPriority+1` tally texts per capacity, so the plan caches settle. Tallying only the non-empty
bands would make the text a function of which sparse bands happen to hold work - up to `2^(MaxPriority+1)` texts,
each a fresh prepare on pgx and a fresh compile on SQL Server as bands come and go - to save an index probe per
empty band.

**The two statements are not one snapshot.** Steps can be claimed between them, so a band the counts saw filling
the cache can tally short; that cycle's plan then reads the fill as fitting (`Reach` is `math.MaxInt`), leaves the
shortfall idle, and lets the doorbell admit any band until the next tally. It needs a band hovering at the
capacity and lasts one tally period, so it is accepted rather than carried into the planner. Pinned by `TestPiston_ScanBandTalliesBandsUntilCapacity`, `TestPiston_ScanBandSkipsASleepingBand`,
`TestPiston_ScanBandServesTheOverflowLast`, `TestScanQueries_PlaceholdersMatchArgs` and, for both statements,
`TestScanBand_CanSeekTheDueIndex`.

**Each band's statement costs O(due rows at the band), and no cap inside the query changes that — do not
re-derive one as a fix.** The `GROUP BY` reads every due row of the band to count it; the count is capped in Go,
which bounds the reported demand, not the scan. A per-key cut is worse than useless: ranking for it needs the rows
in key order, which is what put PostgreSQL 18 into a spilled sort of a whole deep band (see `bandTallyQuery`). The
cost grows super-linearly with the backlog, because a slower scan deepens the backlog it scans, and a single
fairness key - one group over the whole due backlog - puts a fan-out workload into a **bistable** regime, healthy
or collapsed and never between. Sharding divides the variable term; nothing inside the query does.

**Do not bound the scan by fetching the oldest N due rows instead of tallying the band.** It is the
obvious way to cap the O(backlog) term, and it breaks fairness in the one regime fairness exists for. A
tenant holding 1M old due steps against one holding 10 newer ones at equal weight fills the whole window,
so the second tenant wins zero slots every cycle until the first drains, and never if the first keeps
producing. The planner's guarantee is that queue depth does not buy share (`internal/planner/CLAUDE.md`).
The tally can only honour that if every key with due work appears in it. "It only starves under a burst" is
no defence: fairness does nothing except under backlog, so "only at peak" means "only when it matters".

**The partition filters the counts as well as the tally.** Every branch of both statements carries the same
predicate, so this replica's reach is computed from the bands its own class holds, plus foreign steps past the
steal grace. **Do not count unpartitioned.** A band only a peer's class holds would then fill this replica's
counts, tally nothing of its own, and idle it while its own class waits - and the peer is serving that band from
its own plan. Cross-replica priority is therefore strict within the steal grace: a peer's better-band work that
its owner cannot reach enters this replica's counts, ahead of its own worse bands, once it has waited out a grace.
Pinned by `TestPiston_ReachIsCountedInThisReplicasOwnClass`, which fills this replica's counts with a peer's
band; `TestPiston_PartitionNarrowsTheBands` pins the tally side.

The residue class is a **residency, not a lock**: the claim CAS remains the only thing that grants a step,
so a stale `(replicas, ordinal)` pair costs a lost claim, never correctness. `step_id % R` is not sargable,
so this reduces claim collisions rather than scan cost — the intended trade, since the scan was never the
contended resource.

**The pair is VALIDATED, not trusted** (`replicas > 1 && 0 <= ordinal < replicas`, else select everything).
Both bad shapes are strictly worse than not partitioning: `replicas == 0` emits `step_id % 0`, which errors
every query — so the scan fails, the Tallier clears this shard, and it stays out of planning for as long as
the func keeps saying so. An ordinal at or past `replicas` is quieter and worse: the predicate matches
nothing, so the piston reports `NoBand` while genuinely holding work, with no error anywhere. Today's
intended caller guards all of this itself, but fail-open is *this package's* advertised posture, so it is
enforced here rather than assumed of the caller.

**Validated in exactly ONE place (`resolvePartition`), and read ONCE per query — a half-validated second
read is worse than no validation at all.** The pair now feeds three things: the predicate that selects rows,
the multiple that over-fetches, and the ranking that orders what comes back. A consumer that loads it again
gets two hazards for the price of one. It can see a *different* pair, if the fleet changed between the two
loads, and then filter by one and rank by the other. And if it validates more weakly — `ok && r > 1`, say,
without the ordinal range — an out-of-range ordinal correctly disables the SQL predicate while the ranking
built from that same bad pair puts **nothing** in tier 0 and counts **every kept step** as taken from a
peer: `dwarf_steps_stolen` floods and `CheckpointStole` fires for steals that never happened, which is
exactly the dishonesty counting-after-the-trim exists to prevent. Pinned for both queries by
`TestPiston_PartitionPairIsValidated`.

**The band scan also publishes the dispatch lag (`DispatchLag`), and it must stay a passenger.** It is one more
aggregate of the same `GROUP BY` - `DATE_DIFF_MILLIS(NOW_UTC(), MIN(not_before))` per key, maxed across keys - one
reading per band tallied, so the engine's lag gauge costs no query. Four constraints keep it honest:

- **`not_before`, not `created_at`.** The lag is how long a step has been DUE; `created_at` stays the fairness
  ordering. `not_before` is carried in the selection index, so the scan stays index-only. It means "due since"
  only because every write that returns a step to `pending` stamps it (`internal/migrations/CLAUDE.md`).
- **Exact, over every due row.** `MIN(not_before)` per key is one more aggregate of the same `GROUP BY`, so the
  reading is the band's genuinely most overdue step at no extra cost.
- **Grows between scans, cleared on idle.** Each band's reading carries when its statement was sent, and `DispatchLag`
  adds the time since, so a tally loop that stops coming round shows as rising lag rather than a frozen value.
  `SetIdle` and the tally loop's own withdrawal clear it, for the same reason they clear the tally: a scan in
  flight when the piston went idle would otherwise leave a reading that resumes stale.
- **Leaves out the bands strict priority is holding back.** When better bands fill the batch, the planner
  gives the worse ones no slots and their due steps age by design. Reported as lag, every fleet with mixed
  priorities would page on it. So each reading carries its band and `DispatchLag` takes the max over the bands
  no worse than `planner.Reach(capacity)` - the standing tallies, not the last plan, which a stalled supply loop
  would hold at a reach that has since moved; the held-back work shows in the per-band backlog age instead. Because the reading grows between scans, a healthy shard reads up to one tally period, not zero.

## Stealing — the answer to a peer that is SLOW rather than dead

A dead replica stops advancing `dispatched_at`, drops out of the dispatcher divisor within the dispatch
window, and its class is redistributed. A **slow** one keeps beating, keeps its class, and cannot serve it —
and nothing else in the fleet will look at those steps. `SetStealAfter` relaxes the predicate to close that.

**Measured, because the size of it is the whole justification.** Three replicas, one crippled, work created
by an await-only replica so the residue class is genuinely in the path:

| | steps/s (commanded 500) | claim lost |
|---|---|---|
| no steal, capacity-crippled peer (`1 worker`) | **137–153** | ~0 |
| no steal, latency-crippled peer (`+10ms` RTT) | **269** | ~0 |
| no steal, that peer REMOVED from the divisor | 494–505 | ~0 |
| steal, single tier | 501–558 | 8–27% |
| steal, two tiers | **458–501** | **0.4–5.4%** |

⚠️ **These rows were measured under a coarser relaxation than the one below** — one that ran an entire scan
relaxed once triggered, where the fill order takes foreign work only for the slots its own class could not
fill. They therefore **bound** this mechanism rather than describing it, and the claim-lost column in
particular should be re-measured before being quoted of it.

Two facts to take from the first three rows. Keeping a slow replica cost **more than deleting it** — the
fleet capped near `S·R` regardless of offered load, its class aging past 30s while healthy peers sat at a
third of a core. And two unrelated cripplings produced the same cap, so this is a property of the
PARTITION, not of how a replica goes slow.

### The steal is a FILL ORDER, not a gate

The relaxed predicate runs on **every** query. Nothing arms it. What decides whether a foreign step is
actually taken is where it lands in the ranking: `rankByResidue` sorts each key's admitted steps by residue
distance — own class, then the designated neighbour's, then anyone's — and trims to the plan's per-key cap.
**So a replica reaches outside its class only for the slots its own class could not fill.**

Two conditions, and each covers the other's blind spot:

- **The GRACE** admits. A healthy owner dispatches its own class within a cycle or two, so its work is never
  eligible; a stalled owner's ages without bound (measured: **23–41s** against a ~67ms cycle, while a
  healthy fleet's oldest due step sits at **0–1s**). It is the only thing covering **moderate load**,
  because a batch is sized to cache capacity rather than to what is due — so every replica in an
  under-saturated fleet has spare slots the fill order alone would let it take from healthy peers. Measured:
  a healthy fleet at ~30% of the database's capacity stole **0–23 steps across five arms**.
- **The FILL ORDER** ranks. It is the only thing covering **saturation**, where normal queueing delay is
  seconds so the grace admits everything — and there every class is deep, so tier 0 fills the batch and
  nothing foreign survives the trim.

**DO NOT gate the relaxation on a flag armed from the previous scan's tally** — arm when the last scan's
own-class count came in under `cache.Capacity()`, run the next scan relaxed. It is the obvious alternative
to the fill order, it answers the same question, and it fails three independent ways:

- **The input is self-referential.** A tally counts whichever population the scan that produced it was
  looking at — which the flag itself chose. `tally < capacity` then means "my own class is short" after a
  strict scan and "my class *plus* my neighbours' long-due work is short" after a relaxed one: two different
  questions read through one comparison.
- **It alternates rather than settling.** A relaxed scan that fills the batch disarms the flag, so the steal
  fires on every other cycle at best — and the cycles in between plan from a thin strict tally and supply a
  correspondingly short batch.
- **The two loops read it at different phases, and the phases are not independent.** Both start together and
  run at a fixed ratio of intervals (the same interval when one piston scans the shard), so the fetch reliably
  catches the *disarmed* half: it goes strict against a plan
  built from the relaxed tally, asks for the foreign steps that tally promised, and selects none of them.
  Measured: `TestPeerStalledDispatcherflow` fails most runs that way, at ~6.4s against ~0.9s, because the
  work falls through to the 5s dispatch-window eviction instead of being taken. A latch (record the scan's
  decision, have the fetch replay it) closes that third failure and neither of the first two.

The fill order answers the same question **exactly, currently, and per slot**, with no flag to keep in step.

**Do not weaken the grace to "any foreign step".** A gate armed on `Band == NoBand` — nothing due in this
replica's own class at all — **passes every fixture while doing nothing in production shape**: a burst workload drains a class to zero, so the fixtures armed, but a workload with
continuous arrivals leaves a keeping-up replica a step or two due on almost every scan. Measured against a
50 flows/s open-loop bench with one crippled peer: **zero steals, throughput unchanged at 177.** The general
lesson holds for the fill order too — the fixtures cannot distinguish a mechanism that fires from one that
does not, so a change here is measured on the bench or it is not measured.

### The OVER-FETCH and its clamp — `max(1, min(R, 4))`

The fetch asks for **`perKey × max(1, min(R, 4))`**, and **only while the grace can admit a foreign row at
all** — with the class strict, every admitted row is already this replica's own, so a multiple would walk
and ship four times the plan's demand purely to discard three quarters of it. In practice that guard costs
nothing: the grace is zero only under `SetStealAfter(0)`, which nothing outside tests calls, so a
partitioned production fleet always takes the multiple. It is there so the two cannot drift apart, not for a
regime anyone is in. Over-fetching otherwise is not an optimisation, and
removing it breaks the fill order outright: the query returns rows **oldest first**, and the oldest admitted
rows are precisely the stalled peer's, so asking for exactly `perKey` comes back **entirely foreign** and
leaves the ranking nothing of this replica's own to prefer — failing in exactly the case the mechanism
exists for.

Both bounds of the clamp are load-bearing, for unrelated reasons.

**Floored at 1, because `replicas` reads ZERO when partitioning does not apply.** A multiple of zero binds
`LIMIT 0` and the fetch returns **nothing** — silently starving every solo deployment while the query, the
plan, the phase histogram and the `selected` counter all still look correct. Nothing names that failure, so
the floor is the only thing between a solo replica and a refiller that supplies nothing at all.

**Capped at R, because a fleet of R has only R residue classes to rank.** Fetching more than R times the cap
can never surface an own-class row that R times would have missed, so anything above it is pure wire.

**And capped at 4, because the multiple has to cover how many classes are simultaneously LONG-DUE, not how
large the fleet is.** With k stalled peers this replica's share of the admitted rows is ~1/(1+k), so 4
covers three stalled peers at once — well past the design point. Scaling with R instead is actively harmful
on the healthy path, where the admitted fraction is 1/R and the index walk is `perKey × m × R`, i.e.
**quadratic in R**.

Measured, PostgreSQL 18.1, **1,392,636 pending rows**, one key, real lateral shape, `jit=off`
(`walked` = rows returned + `Rows Removed by Filter`):

| predicate | LIMIT 128 | 512 | 2048 |
|---|---|---|---|
| solo (no partition) | 128 | 512 | 2048 |
| strict `% R = ordinal` | **505** | **2050** | **8191** |
| 3 tiers, nothing aged | 505 | 2050 | 8191 |
| 3 tiers, everything aged | **128** | **512** | **2048** |

So at R=16 the 4× cap walks 8,191 entries (~1.7–3 ms) where an uncapped `m = R` walks ~32,700 (~12 ms),
**every cycle**. Below R=4 the cap is inert and the multiple is simply R.

Three facts from that table worth keeping:

- **Every arm is `Index Only Scan using idx_dwarf_steps_selection` with `Heap Fetches: 0`, and the `LIMIT`
  stops the scan.** The 3-term `OR` does **not** flip the plan — it lands in the `Filter` while
  `(status, parked, priority, fairness_key)` stay equality-matched in the `Index Cond`, so the index still
  supplies `(created_at, step_id)` order. That is what makes an always-relaxed predicate affordable.
- **The relaxed predicate is never more expensive than the strict one at the same LIMIT**, and is *cheaper*
  when it admits — the scan stops at n instead of skipping R−1 of every R rows.
- **The walk is `LIMIT ÷ admitted fraction`**, not a constant.

⚠️ **`EXPLAIN` this query with `jit = off`.** The first pass measured 100–280 ms and it was almost entirely
JIT emission on a one-shot statement; the scan underneath was 0.3 ms. Nothing above is readable through it.

**A two-phase fetch was designed and NOT built.** Fetch own+neighbour at `perKey × 2` first, then the
remaining tiers only if short: it walks half as much on the healthy path (`perKey × 2 × R` against
`perKey × 4 × R`) and wins whenever the multiple exceeds 2. It is not built because at R=4 the saving is
~0.3 ms per key per cycle, against a band scan measured at **1.6–2.1 s in the same database** and an RTT of
~0.6 ms that the extra round trip costs whenever it fires. Revisit it for a large-R deployment; the table
above is the measurement, so that decision needs no re-run.

**Known exposure: fairness-key CARDINALITY.** The tally is relaxed too (it must be — see below), so the R
replicas do not have disjoint plans by construction. This replica owns a row for a planned key with
probability `1 − ((R−1)/R)^n`, where n is that key's due depth: at R=4 that is **94% at n=10** but only
**25% at n=1**. So with many keys holding ~1 due step each, tier 0 is empty for most planned keys and every
non-owner ranks the same single row first. **Unmeasured — the entire cloud archive ran at
`fairness-keys=1`.** If it bites, the escalation is to keep the tally strict (plans disjoint at any
cardinality) and top up *unfilled* capacity with a second key-unrestricted query, which trades away the
fairness allocation for the top-up portion.

**BOTH queries run the identical predicate, and this is an invariant rather than a tidiness.** The plan is
built from the tally, so a fetch selecting from a different population than the scan counted asks for rows
it cannot see. A fully-global tally against an age-gated fetch would allocate slots for fresh foreign rows
the fetch must reject — and at moderate load most foreign work *is* fresh, so most of the batch would be
unfillable. Nothing gates either query, so the two cannot drift apart.

### Two tiers, and why the second one is not optional

```
own class            always
neighbour's class    after 1 grace   ((ordinal+1) mod replicas)
anyone's class       after 2 graces
```

Tier one gives each class exactly ONE designated stealer, so that window is contention-free — no two
replicas are ever eligible for the same step. Measured: it took the worst single-tier arm from **26.8% claim
loss to 0.4%** with throughput held.

Tier two exists because tier one alone can STRAND work. With two consecutive degraded replicas the far
one's class has no working stealer — its designated one is itself broken — so it gets *zero* service rather
than slow service, which is qualitatively worse than contention. Past two graces the class opens to
everyone. Measured on the bench at `1,1,64`, the fleet still met the full commanded rate with
`oldest_pending` at 0s.

**At the engine's two dispatchers per shard the two tiers coincide**: with two classes the neighbour is
everyone else. The tier still earns its one `OR` term, because the predicate is general in the class count
and the divisor reaches three for a few seconds during a dispatcher handoff. It is pinned here, by
`TestPiston_StealReachesPastTheNeighbourAfterTwoGraces` (three classes, real SQL), not by an engine fixture:
the engine cannot hold three dispatchers on a shard on demand, so a fixture would need a seam no deployment
has.
Why the engine does not raise its dispatcher count to make the tier matter is in `engine/CLAUDE.md`
(`maxDispatchers`).

The honest limit: tier one helps in proportion to the neighbour's SPARE capacity. In a saturated fleet most
work falls through to tier two and contention returns to the single-tier level. This is a win for one bad
apple in a fleet with headroom — the common deployment — not a general contention fix.

### The grace, and the three things it must not be

- **Measured from `not_before`, never `created_at`.** `not_before` is stamped `NOW_UTC()` at creation and
  pushed forward by `flow.Sleep` and every retry backoff, so the predicate reads "has been DUE for at least
  the grace". Against `created_at` an hour-long sleep would be stolen the instant it came due, on a fleet
  with nothing wrong with it.
- **Floored at the `pipeline.DefaultMinGap` CONSTANT, not the configured `MinGap`.** A caller may pin both
  interval and gap to zero (a bench sweep, a hand-driven cycle); deriving the floor from a configurable that
  can itself be zeroed yields a zero grace, which steals every foreign step the instant it comes due with no
  fuse at all.
- **NOT an absolute age threshold — it is a multiple of the CYCLE PERIOD.** Normal queueing delay under load
  is *seconds*, so any wall-clock constant either never fires or admits everything under exactly the load it
  exists for. Deriving it from the period keeps it proportionate to how fast a healthy owner reaches its own
  work, and the FILL ORDER is what covers the regime where it admits everything anyway: under saturation
  every class is deep, so tier 0 fills the batch and nothing foreign survives the trim.

`defaultStealAfter = 4` is not delicate: a healthy fleet's oldest due step sits at 0–1s while a stalled
owner's class ages to 23–41s, against a ~67ms derived period. Anything from ~2 to ~10 periods separates
those cleanly.

### What a healthy fleet does: nothing

At healthy RTT the bench fleet ran at ~30% of the database's capacity with spare everywhere and stole **0–23
steps** across five arms. Every replica had spare slots the fill order alone would have let it fill from its
peers — nothing aged past the grace, so nothing was admitted. It is not that idle replicas steal harmlessly;
they do not steal at all. **This arm is the direct evidence that the grace, not the fill order, is what
covers moderate load**, and it is why the grace cannot be dropped alongside the gate.

**A debounce was therefore considered and shelved.** The case for it was a uniformly slow fleet
stealing pointlessly, and it does happen (a `-race` fixture run: 6 of 40 steps; a bench cell at rtt 2.4ms:
79% of *claims*, both measured under the gate build). But the second figure is a ratio over claims, and a failed claim is one round trip where a
completed step is ~9.6 — in real terms 7.6% of round trips, on a run already at **78% of the ceiling its RTT
allowed**. The missing throughput was the slow database, not the steal. Do not build the debounce without
evidence that names a cost the grace does not already bound.

**`FetchSteps` orders each key oldest-first, never on a recomputed age.** Both shapes get it by
construction — `(created_at, step_id)` is the lateral's `ORDER BY` and the window's ranking alike. Do not
select `DATE_DIFF_MILLIS(NOW_UTC(), created_at)` and re-sort each key in Go by age descending: that is the same
ordering read backwards through millisecond-truncated arithmetic, agreeing only incidentally (anything created
inside one millisecond falls through to the `step_id` tiebreak, which is also all a test of it exercises). An
age would only serve a cross-shard merge, and none exists here: the planner has already assigned the slots. Note the ordering is per key only —
rows are grouped by key, not sorted across them, which is all `assemble` reads.

## `FetchSteps` is ONE statement over the plan's (band, key) pairs, passed as a fixed number of binds - a correctness bound

**One statement serves every band the plan reached**, so a plan spanning several bands costs one round trip on
the supply loop, which is the supply ceiling's path. Each pair is a lateral lookup on pgx and mssql, a row-value
`IN` on mysql and sqlite.

**One bind per key overruns SQL Server's hard ceiling of 2,100 parameters per statement at ~2,095 keys.**
The pair count is bounded only by the candidate cache's capacity — twice the worker count — so a
192-connection pool at `workersPerConnBudget = 8` plans at **3,072**, and a per-tenant fairness key with a
few thousand active tenants puts the plan's pair count right there. SQLite's own ceiling
(`SQLITE_MAX_VARIABLE_NUMBER`) is 32,766 on a current build but **999** before 3.32.

The failure is quiet, which is what makes it worth a doc section. A failed fetch is the pipeline's "push
nothing, clear nothing" path — the tally already succeeded and is still true — so the shard goes on
publishing an honest band claim while supplying zero candidates, every cycle, for as long as the workload's
cardinality holds. It logs through `Run`, but there is no error counter; the metric signature is
`fetch_steps` recording a normal duration with `selected` at zero on one shard. Pinned by
`TestFetchQuery_BindCountIsFixed`.

### The lateral's early stop is worth 256x, measured

The `rn <= perKey` cut runs **after** the window function has ranked every row, so it bounds the rows
returned and not the rows read. PostgreSQL 18.1, 400k due rows over 4 fairness keys at one band, `perKey=8`,
`EXPLAIN (ANALYZE, BUFFERS)`:

| shape | rows read | buffers | time |
|---|---|---|---|
| window + `rn<=?` | **400,000** | 7,408 | 64.4 ms |
| `LIMIT` in `CROSS JOIN LATERAL` | **32** | 13 | **0.252 ms** |

**PostgreSQL 15's WindowAgg run condition does not rescue the first shape, and the plan says so out loud** —
it prints `Run Condition: (row_number() OVER w1 <= 8)` and *still* reports `rows=400000` out of the index
scan. With `PARTITION BY` present the node stops evaluating past the cut but must keep pulling tuples to
find the partition boundary; only the single-partition-at-plan-time case can stop the node outright. Do not
re-derive a per-key cap as a fix for scan cost on any dialect: the cap is what the *planner* needs, and on
the ranking shape it buys nothing on the scan.

The gap widens with the backlog, since the window shape is proportional to it and the lateral is flat.

### On pgx the pairs are native arrays (`int[]`, `text[]`), so the planner can count them

A custom plan sizes `unnest`'s output from the bound arrays. A JSON set-returning function
(`jsonb_array_elements_text`) is estimated at 100 rows whatever it holds, which multiplies
the fetch's estimated cost by 100/keys - at one key a flipped plan priced ~500k, past Postgres's JIT thresholds
(100k / 500k by default), so it was compiled with full optimization on every execution: measured locally, JIT
in 439-633 of the slow statements a saturated run logged, and none once the keys were an array. **Do not bind a
text literal and cast it to `text[]`** to keep the argument a plain string: the cast is not folded at plan time,
so the estimate stays at 100. Pinned by `TestFetchQuery_PostgresPlan`.

### The lateral fetch cannot use the due index (`dueIndexGuard`)

`idx_dwarf_steps_due`'s partial predicate is `status = 'pending' AND step_id > 0`. The second conjunct is always
true (ids start at 1); it exists so that only a query stating it can use the index, because a database uses a
partial index only for a query whose own predicate implies the index's. The band scan's counts and tally,
the window-shaped fetches (`dueSteps`) and the engine's backlog aggregate state it. The pgx and mssql lateral
fetch (`dueNow`) does not, so no plan - generic or custom, under any statistics - can put the fetch on the due
index.

Why it has to be the index and not the plan: the lateral's early stop needs `idx_dwarf_steps_selection`, which
seeks `(parked, priority, fairness_key)` and returns each key's run already in `(created_at, step_id)` order. The
due index seeks `(parked, priority)` too, and when its estimates are small the planner takes it, filters each key
row by row and sorts: O(backlog) per key. Measured on a rig (PG 16, 512 keys, saturated): a CUSTOM plan made
1 m 50 s into a fresh database's warmup, `rows=1` at cost 8.4, ran **1,224 s** and held the vacuum horizon the
whole time, and the held horizon slowed every later read (`engine/CLAUDE.md`, "A long read holds back vacuum"):
a later good-plan fetch took 69 s, the lease-recovery sweep 57 s. The plan
could not be reproduced locally on PG 16 or 18 from any table contents, so a fix that steers the plan cannot be
tested against the failure; this one can. The rules that keep it working:

- **A new query that wants the due index states `step_id>0`.** Without it the query silently falls back to the
  selection index or a scan - correct results, the sleeper-heavy cost the due index exists to avoid.
- **Never state the guard in the lateral fetch.** That reopens the 1,224 s plan.
- **Never add the guard to the selection index's predicate** to make the band scan cheaper: the fetch, which does
  not state it, would lose the selection index too.
- **Do not steer the plan instead.** A CASE around `parked` or `priority` blinds the selection index as well
  (both lead both indexes: PG 18's skip scan steps over the wrapped column and puts a Sort under the `LIMIT`;
  PG 16 seeks on `status` alone). A plan-cache mode or `enable_sort = off` only removes the cases it names, and
  neither could be shown to matter in a local test. Nor can the statistics be steered: rows that moved the
  fetch's estimates would have to be pending rows at the band, which get dispatched, and the 1,224 s plan was made
  before the table's first ANALYZE anyway.

**The cost, measured** (PG 18, 400k due rows at one band, 32 keys, warm, six runs each): the dense band scan
reads its rows off the selection index, where the guard is checked per row - **~71 -> ~80 ms (+12%)**. The
sleeper shape (396k asleep, 4k due), where the band scan seeks the due index, and the backlog aggregate in both
shapes are unchanged, and so is the fetch's cost. The band scan is a
resource tax rather than a supply bound (`engine/CLAUDE.md`, Metrics), so the 12% is database CPU, not throughput.
Pinned by `TestFetchQuery_PostgresPlan` (with the selection index dropped the fetch must not touch the due index;
the same statement with the guard must), `TestScanBand_CanSeekTheDueIndex` (pgx, sqlite) and
`engine/TestBacklogSQL_UsesTheDueIndex`; SQL Server was checked once by hand through its plan cache.

### The lateral join is NOT available on two of the four dialects

| dialect | pair list | per-key cap | cost |
|---|---|---|---|
| pgx | `unnest` of an `int[]` and a `text[]` (2 binds) | `LIMIT` in `CROSS JOIN LATERAL` | O(pairs × perKey) |
| mssql | `OPENJSON` of `[band, key]` pairs (1 bind) | `TOP` in `CROSS APPLY` | O(pairs × perKey) |
| sqlite | row-value `IN` over `json_each` pairs (1 bind) | `WHERE rn<=?` after the window | O(due rows at the planned keys) |
| mysql | row-value `IN`-list (**two binds per pair**) | `WHERE rn<=?` after the window | O(due rows at the planned keys) |

**mysql is the one branch that still binds per pair, and both halves of that are load-bearing.**

*No lateral*, because the driver name covers MySQL **and** MariaDB and MariaDB has no lateral derived tables
at any version — `MDEV-19078` is open, and 10.11 rejects `CROSS JOIN LATERAL` outright. (MariaDB's "lateral
derived optimization" is an internal split-materialization strategy, not the keyword; the name invites exactly
this mistake.) CI runs `mysql:8`, so a lateral would pass every pipeline and fail on half of a supported
production dialect — the ambiguity `internal/staterefs` names when it says a value keyed on this driver name
cannot tell the two engines apart, except here it is not moot.

*No `JSON_TABLE` either*, and this one is a bug, not a preference. MySQL 8 gives the extracted
column the **server's** default collation while `dwarf_steps.fairness_key` carries the **database's**, so the
join predicate raises `Error 1267: Illegal mix of collations` — measured on MySQL 8.4 with a
`utf8mb4_0900_ai_ci` server against a `utf8mb4_general_ci` database. **MariaDB cannot reproduce it** (it has
no `utf8mb4_0900` family), so it passes every MariaDB run and fails on the engine CI uses; a local MariaDB is
not a proxy for MySQL here. A bind parameter is immune: its collation is COERCIBLE, so the column's wins.
That makes the row-value `IN`-list the *safe* shape on this dialect, not merely the simpler one — and it keeps the
supported-version floor down, since nothing needs `JSON_TABLE`.

Two binds per pair are sound *here specifically*: MySQL's ceiling is 65,535 placeholders against a pair count
bounded by the cache capacity (twice the worker count, a few thousand on a large pool) — better than 10x of
margin. The overflow the one-bind shape exists to avoid is SQL Server's hard 2,100, which no other dialect
approaches.
Pinned by `TestFetchQuery_BindCountIsFixed`, which asserts the mysql exception explicitly rather than skipping
it.

**A `LATERAL` for MySQL-only was measured and is not currently worth its machinery.** MySQL 8.0.14+ does have
one, and at 400k due rows over 4 keys it took the fetch from **581ms to 200ms (2.9x)** — real, but two orders
off what the same shape gives Postgres, because MySQL plans the lateral's `ORDER BY created_at, step_id` as
**`Using filesort`** rather than reading it off the index, so it sorts each key's whole run and the `LIMIT`
bounds only the output. Taking it would also require distinguishing MySQL from MariaDB at runtime — a
server-version probe this codebase has deliberately avoided. Revisit if the filesort can be eliminated, which
is where the large remaining factor is.

So mysql and sqlite keep `ScanBand`'s cost shape, and closing that for them needs a different mechanism than
the one that closed it for pgx and mssql.

**The mssql argument order is not copyable.** `CROSS APPLY` puts `TOP (?)`'s cap *before* the band, where
every other dialect binds the cap last. `TestFetchQuery_PlaceholdersMatchArgs` counts `?` against `len(args)`
for every dialect crossed with every partition shape (none / strict / stealing, which contribute 0, 2 and 6
binds), because a hand-ordered arg slice is exactly the thing review does not catch.

## Metrics take a `Meter`, not a `MeterProvider`

So the **owner** picks the instrumentation scope once for every module it assembles. Each package deriving
its own scope from a provider would split one engine's metrics across several scopes in the export the
moment a name drifted, and nothing here needs provider-level capability. Instrument names are a public
surface that dashboards bind to — `TestPiston_RecordsItsInstruments` asserts them by name so a rename
fails loudly.

**The `shard` attribute must be a STRING, matching the engine's.** The owner emits `shard` on its own
instruments too, and an OTLP-native backend distinguishes attribute *types*, so the same key at two types
is two different attributes there — which breaks any query grouping this package's instruments against the
engine's. That join is a real one: the refiller's cost read against the turnstile's queue. An
`attribute.Int` here beside the engine's `attribute.String` goes unnoticed on Prometheus, which renders both as
the same label string and hides the split entirely. A test cannot catch it from inside this
package — the conflict only exists across the two — so it is a rule rather than an assertion.

**`refillBuckets`' LOW end is load-bearing and must not be raised.** A warm same-zone band scan measures
~0.29ms; the *same* query on the *same* data measures ~100ms once its Postgres statistics go stale and the
plan flips to a sequential scan — the flip the `phase` label exists to expose. Boundaries starting at
0.0005 file the healthy case in the first bucket and hide exactly that. The values are a public surface in their own right: a histogram whose boundaries changed
under a dashboard is a silent regression, so treat them as fixed rather than as a tuning knob.

## Two faults and three checkpoints, and why each is the exception

`SetSeams` takes the **owner's** `*seamster.Seamster` — one catalogue of fault names per application,
armed in one place, however many modules consult it — for the same reason `SetMeter` takes a `Meter`. Nil
restores an inert one, which is the default, so an unwired piston consults nothing and a disabled Seamster
makes every consult a bool read.

Two faults are consulted, **`FaultScanErr`** in `ScanBand` and **`FaultFetchErr`** in `FetchSteps`.
`FaultScanErr` earns its place because it perturbs
the **database query**, which is the boundary a test cannot otherwise reach — the pipeline's scan-error
policy (clear this shard from planning, leave its cache partition *alone*) is only reachable by making a
real scan fail, and the two halves are asymmetric, so neither can be inferred from the other. The name is
exported so the owner's catalogue aliases it rather than re-spelling the string. Pinned by
`TestPiston_ScanErrSeamDrivesThePipelineErrorPolicy` and `TestPiston_SeamsDefaultInert`.

`FaultFetchErr` earns its place on a sharper version of the same rule: a piston whose every fetch fails is
**invisible from every angle a test can otherwise reach** — it scans honestly, publishes a real tally,
claims its band, logs nothing, and simply supplies no candidates. Without the seam there is no way to drive
`TestPiston_FailingFetchesReportNoLiveness`, and therefore no way to pin that the turn count demotes it.

Three checkpoints are fired. **`CheckpointStole`** reports a fetch that took steps from outside this
replica's residue class, and it earns its place on the same boundary rule: a test proving a slow peer's work
is picked up cannot wait out a duration, because the steal fires on the first cycle where the grace has
elapsed and this replica's own class has run short — a function of the cadence, the peer's degradation and the backlog, none of which a test
controls. Without it the only available assertion is "the flows eventually finished", which passes equally
against a build where stealing does nothing and the dispatch-window eviction did the work seconds later.

**`CheckpointTallyDone`** is there because **a push does not imply a scan**. The supply loop plans from
whatever tally the planner already holds and touches the database only to fetch, so any number of pushes can
resolve a tally that predates the work under test. A caller needing "a scan has SEEN what I just committed"
waits on this first and only then on `CheckpointCycleDone` — which is what `enginetest.AwaitShardCycles`
does. Gated on the scan succeeding, since a failed one clears the shard rather than reporting it.

The third is **`CheckpointCycleDone`**, in `Run`, once per cycle that PUSHED. It earns
its place on the same boundary rule read from the other side — it publishes the one fact about a cycle that
is invisible from outside the process and cannot be waited out on a clock. **Each piston turns on its own
cadence, so "every shard has reconciled its partition against the plan" is not a function of elapsed time**:
a shard whose goroutine is starved, or whose cycle is blocked on a slow round trip, can hold an unreconciled
partition arbitrarily long while its peers turn normally. A caller that needs that state — a test asserting
strict cross-shard priority must, because `Cache.Pop` ranks partitions by the bands they hold and cannot tell a
doorbell-admitted candidate from a plan-selected one — has no other way to know.

**It is gated on `SupplyResult.Reconciled`, NOT on `Err == nil`, and the difference is not cosmetic.** Two
paths hold everything and leave the partition exactly as they found it — a failed fetch, and an untallied
shard — and **only one of them is an error**. Gating on the error would fire a visit on the untallied path,
telling a waiter the partition reflects the plan at the one moment it demonstrably does not. `Reconciled` is
set where the push happens, so a visit means the push happened.

The gate is what keeps this from being the forbidden kind of seam. A counting checkpoint over pure logic
would be a signal to inject a dependency instead; this one reports a **cycle's effect on shared state** the
package borrows (the cache partition), which no injection into `pipeline` would surface, since the effect is
the point rather than the call.

**`pipeline` gets none, and that is not an oversight.** Every fault a test could want of a cycle is
reachable through its two source interfaces — which are this type — or through the per-loop cadence setters,
and `TallyResult`/`SupplyResult` give a caller everything a counting checkpoint would. The rule that separates the two
cases: a seam inside **pure logic** is a signal a dependency should have been injected instead; a seam at
an **I/O boundary** is reaching the one thing that cannot be injected away. Do not add one to `planner`
either, for the same reason.

## Tests run against real SQL

Each test stands up its own isolated, migrated database through `internal/database.ShardSet` — a
**test-only** import, since the piston itself never opens a handle — and inserts real `dwarf_steps` rows.
That is what lets them cover things a fake could not: the parked-step predicate, the capacity cap actually
capping, the fetch's oldest-first ordering, and the residue class splitting a real row set disjointly across
ordinals.
