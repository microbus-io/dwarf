# Dwarf `internal/peers` — one shard's replica registry

> Load when: changing the Sonar's lifecycle, its seven statements (register's UPDATE and INSERT, the beat, the
> read, `Withdraw`, `Leave`, the prune), the two windows, or the blindness rules.
> Coupled with: `internal/migrations/CLAUDE.md` (the `dwarf_peers` columns these statements touch) and
> `engine/CLAUDE.md` §"Peer discovery" and §"Connection pool sizing" (what the published values are used
> for).

One Sonar works one shard. It owns this replica's row in that shard's registry — creates it, refreshes it,
deletes it — reads the whole registry on its own cadence, and publishes what the reading implies: how many
replicas are there, this replica's rank among them, the work partition, and how stale the answer is.

**Nothing here spans shards, and it must not start to.** Every timestamp in the registry is stamped by the
shard that holds it (`NOW_UTC()`), so ages are comparable *within* a shard and timestamps are comparable
nowhere. A fleet view assembled from two shards would be ranking two clocks, and a clock-fast shard would
win every comparison regardless of what it actually knew. Keeping the type single-shard is what makes that
error unexpressible rather than merely avoided — the whole package holds one `*sequel.DB` and has no
concept of a second.

The per-shard scope is also the *right* answer rather than a simplification. Evidence and decision share a
scope: a peer whose dispatcher wedges on shard 3 drops out of **shard 3's** work divisor and stays in every
other shard's, and a peer whose beats to shard 3 fail stops dispatching only shard 3. A process-global
divisor derived from one shard's rows can express neither.

## The row's whole lifecycle lives here, and that is what makes deletion final

Registration creates, the beat refreshes, `Leave` deletes, the prune deletes other replicas' stale rows. All
four in one owner, because splitting create/delete from refresh reintroduces a hazard that has no local fix:
**the beat must never create a row.** If it did, a straggler beat could resurrect a replica that had
just deleted itself on the way down, and the shutdown delete would then be correct only under an ordering
constraint spanning two packages. Instead the beat is an UPDATE and nothing else, so a late beat matches
zero rows.

The consequence to hold onto: **a row that goes missing is refreshed by nobody.** That is why the
registration repair exists (below) and why it cannot be left to the beat.

`Leave` takes a context and is the *owner's* call, made after `Run` has returned. `Run` cannot do it on the
way out — its context is cancelled by then, so the DELETE would fail — and `Run` having returned is itself
the guarantee that no beat follows.

**The API is twenty calls, and keeping it that small is a standing constraint.** Seven inputs (`New`,
`SetProfile`, `SetCut`, `SetEvidence`, `SetTurnFunc`, `SetLogger`, `SetSeams`), five lifecycle calls (`Join`,
`Run`, `Withdraw`, `Leave`, `SetDraining`), seven published facts (`Replicas`, `Rank`, `Standing`, `InCut`,
`Draining`, `Partition`, `BlindFor`), and one knob (`SetCadence`). `SetCut` earns its place because the cut needs the
shard's index and the slot rule, which are the owner's, while applying it has to happen here: the hold that
makes entering the cut wait a reading needs the previous raw reading, which only the Sonar has.

The windows and the beat are **constants with plain fields behind them**, not setters, because they are
this package's policy — derived from which errors are safe in which direction rather than from anything an
owner knows better. Do not export a setter/getter pair for them: it doubles the surface for the benefit of
tests that live *in this package* and can assign the fields directly.

**`SetCadence` is the one exception, and the test it passed is worth stating** because the next proposed
setter will not pass it: the read cadence is the only number here that prices something **only the owner
can see** — its own startup latency, since every replica waits two of these inside `Join`. A suite standing
an engine up per test would spend most of its time there. That is categorically different from a window,
which prices a risk this package owns. Any further setter has to clear the same bar.

## Two timestamps, because they answer different questions

`seen_at` moves on every beat: *this replica is alive and holding connections*, which is what the pool
divisor counts. `dispatched_at` moves only with a turn behind it: *this replica is genuinely serving this
shard*, which is what the work partition divides across.

**The distinction is evidence versus claim, and getting it wrong strands work.** A replica handed a residue
class of `step_id` it never selects leaves those steps unpicked by anyone. If the divisor trusted a flag, a
replica that published "I dispatch" and then wedged would keep its class forever. With a timestamp, a
wedged dispatcher simply stops advancing it and drops out on its own while `seen_at` keeps it in the pool
divisor where it belongs.

Registration therefore does **not** stamp `dispatched_at`. Registration is intent; the column is evidence. A
replica earns it on its first turn, and the cost is a brief window in the fail-open direction.

## The evidence seam: a counter, not a flag

`SetEvidence` supplies `func() (turns uint64, busy, idle bool)` — the one fact a Sonar cannot observe for
itself. Three rules, each load-bearing:

- **It is a pure read.** A consuming getter (a sticky bool cleared by whoever looks) creates an API
  contract — "call this exactly once per beat, from exactly one caller" — and any second caller (a metric,
  a test, a debug endpoint) then silently clears the evidence and leaves a healthy dispatcher reading as
  stalled. The counter moves the "since the last beat" part *here*, held against `lastTurns`, which only a
  beat that actually published advances. A pass that merely looks cannot swallow a turn.
  Pinned by `TestPeers_EvidenceIsNotConsumedByLooking`.
- **`busy` is evidence too, and it must mean a LONG turn rather than one in flight.** A scan can
  legitimately outlast the dispatch window on a deep backlog, so without the term every healthy replica in a
  loaded fleet would fall out of the divisor at once — precisely when overlapping selection costs the most.
  But a turn that fails *instantly* is also briefly in flight, and this Sonar samples every cadence: at a
  measured ~1.2% duty that is caught within seconds, and a dispatcher that serves nothing then keeps its
  residue class forever. The producer owes a duration-qualified answer, not a liveness bit — see
  `internal/piston/CLAUDE.md`.
- **The `idle` term stays explicit** even though an idling dispatcher stops advancing its counter anyway. A
  turn that completed just before it went idle would otherwise make the next beat claim service for a whole
  window, and over-counting dispatchers is the direction that strands work.

Without any evidence func, a Sonar keeps its row alive and never claims to dispatch — exactly right for a
replica that holds connections but claims no work. **Absence of evidence must never read as evidence.**

## Three consumers of one reading, three postures — matched to reversibility

This is the core of the design. All three are derived from the same snapshot, and they disagree
deliberately:

| | direction of error that hurts | posture |
|---|---|---|
| `Replicas` + `InCut` (who dispatches) | under-count / over-promote → over-size pools → collapse a database | **hold, and withhold a fall or an entry into the cut across a gap** |
| `Partition` (work divisor) | over-count → a residue class nobody selects → stranded work | **fail open** |
| prune (hygiene) | a wrong delete is the only irreversible act here | **wait 5 minutes** |

- **`Replicas` rises instantly, and a fall is withheld on the one reading that ended a blind spell.** A
  rise shrinks pools, which is safe; a fall grows every pool derived from it. The case it exists for is not
  one peer dying but a **correlated stall**: a few seconds of database trouble stalls every replica's write
  *and* every replica's read at once; it clears, everyone reads a roster where every row is stale, everyone
  computes a tiny count, and everyone grows to the full per-database budget simultaneously against a
  database that is already sick. Skipping one reading covers the count for a gap that aged no row out. It does
  NOT cover a stall past the fresh window: the beat (1s) is longer than the read cadence (250ms), so peers'
  beats land spread over several readings once it clears, and those readings show a partial roster with no gap
  to hold anything. What keeps that from becoming a storm of dispatchers is the staggered entry into the cut
  (below), not the count: the replicas that dispatch while the roster is partial are the ones already in a
  cut and the few at the top of the order with no stale row ahead, so a shard runs a dispatcher share or so
  over its slots for those readings at most, never a share per replica. The fall is held against the previous
  *raw* reading, as the cut and the candidate count are (below), and
  `Replicas` is published before the standing, so the replica count and the pool divisor fall on the same
  reading and no reader sees the divisor fall ahead of the count. Pinned by
  `TestPeers_ReplicaFallThroughSustainedGaps`.

  **A general K-consecutive debounce was built here and removed.** It is the obvious next reach, and it
  buys nothing this does not: the only failure it additionally covers is a peer whose row *straddles* the
  fresh window with no gap on the reader's side, which takes ~40 consecutive failed beats and is a real
  fault rather than flapping. It cost a type, a knob, a run-maximum rule for alternating readings, and a
  test — to protect against nothing nameable. Reuse the `gapped` flag the prune already needs instead.
- **`Partition` fails open on every doubt** — solo dispatcher, self absent, ordinal out of range, or blind.
  Not partitioning costs a lost claim round trip, which the claim CAS absorbs; partitioning on a stale pair
  costs work that nobody runs. Blindness is evaluated **at the getter**, not published by the loop, so a
  shard that stops answering stops being partitioned within a round trip rather than within a cadence.
- **The prune waits** (below).

- **`InCut` obeys the same asymmetry as `Replicas`, for the same storm.** After a correlated stall every
  row reads stale at once, so every replica would see a handful of candidates, land inside some shard's cut,
  and grow its pool to a dispatcher's share simultaneously. Leaving the cut shrinks a pool and is believed at
  once; entering it waits one reading - `InCut` is true only if the previous raw reading also put the replica
  in the cut. That rule holds on every reading, not only after a gap, because it also orders an ordinary
  handoff (see "Who dispatches"). Alone it does not cover a stall past the fresh window; the two rules below do.

  Two more rules decide the cut, and together they are the whole defence against a fleet promoting itself at once:

  - **A REPLICA WHOSE OWN ROW IS NOT FRESH DOES NOT DISPATCH, entering or staying.** Its beats are not landing,
    so its peers have already dropped it and promoted around it; a slot it held or took would be one nobody
    counts. While beats fail fleet-wide (a full disk, a read-only failover, which end no blind spell at all)
    every replica reads its own row stale, so NOBODY dispatches - which is what those failures allow anyway.
    Without the rule, every replica's own row goes stale with the rest, each sees a fleet of one, and every one
    enters every shard; guarding only entry is not enough either, because a shard's dispatchers slide out of
    its window as rows age out, and which shards keep one becomes a lottery of row ages, sizing for a fleet of
    one. The rule is part of the raw verdict, so coming back takes the two agreeing readings any entry does, and
    the step-down is logged once ("Own peer row stale"). The cost: a failure confined to `dwarf_peers` writes (a
    revoked grant, a table-level write error) stops dispatch on the shards it hits - a lone replica's included,
    since it cannot tell itself from a fleet whose beats all fail. A write that BLOCKS (a lock held on the table)
    does not: the beat and the read share a pass, so the replica goes blind and holds its last standing.
  - **ENTRY IS STAGGERED BY THE STALE ROWS AHEAD, a beat's worth of readings each.** A replica sees itself
    in a cut it does not belong to whenever rows ahead of it in the order read stale - after a stall, until
    every peer's beat lands again, spread over a beat or more. The stale rows are still in the registry, because
    the prune waits a healthy run, so their count (`staleAhead`) is large exactly when the verdict is suspect,
    and zero in the steady state. As peers beat again the rows ahead turn fresh and the replica falls out of the
    cut before its wait runs out. **The wait is counted in READINGS, never in time.** A failed read earns
    nothing, so blind time does not count (a database that flaps would otherwise admit an entry on the reading
    that ends a gap), while a database merely slow to read - every reading gapped - still progresses a reading at
    a time. Restarting the wait at every gap freezes the cut there, and the shard behind a crashed dispatcher
    runs nothing (`TestPeers_StaggerProgressesThroughSustainedGaps`). Leaving is never delayed. The first reading
    after `New` is the one exception: it keeps the cut `New` seeds, as the two-reading rule does. The stale rows
    ahead of a replica on a cold start belong to processes that are gone, and `Join`'s announce-then-wait
    already orders that handoff.

  **Why the wait is shaped this way.** Each alternative weighed here loses for a reason that recurs:

  - **It scales with the stale rows ahead, not a fixed delay.** The wait does not admit the right replica so
    much as give the wrong ones time to see the real roster and drop out. Where peers' beats land later than a
    fixed window (a sick database), every replica that looked eligible reaches the end of the same window
    together and enters at once: the storm, delayed. Scaled, the margin grows with depth in the order, where a
    verdict is likeliest to be an artifact, and replicas that do come online come one depth at a time, so
    connections grow in steps rather than a burst. The cost is the handoff delay below, accepted for that.
  - **It counts stale rows ahead, not a replica's place in the order** (or its distance from the shard's window
    start). A place-based delay slows every ordinary entry, a replica deep in the order on every shard whose
    window it sits in. The stale count is zero in the steady state, and it is local to one reading - the
    window's start depends on the owner's cut, which the Sonar does not see.
  - **Every stale row ahead counts, however old.** A row past the straggler age is as much a sign of a partial
    picture as a fresh-stale one: a stall longer than the straggler age ages every row past it, and counting
    only the younger ones would zero the count exactly then. The prune bounds the cost.

  **The cost is on ordinary handoffs: a beat per stale row ahead, until the prune removes those rows.** One
  crashed dispatcher delays the promotion it causes by about a beat. Losing a zone of k replicas, or a rolling
  restart that kills processes (each restart a new engine id), leaves k stale rows ahead of the replicas that
  must take over, and each promotion waits k beats until the prune removes them. That takes the straggler age
  AND five minutes of healthy run, which starts at a Sonar's first reading and which a read gap or a non-fresh
  own row restarts: after a full-fleet crash restart, or a kill-style roll that replaced every older replica, the
  stale rows ahead persist about five minutes, and every entry meanwhile waits for them.

  **Do not replace these with a hold that keys on read gaps** (a settle window after a gap, a hold against the
  published value): every such shape needs a timer tuned to the beat spread, misses the replica whose last
  good beat landed last (its peers age out one by one with no gap), and ratchets the cut on a database that is
  merely slow to read. Pinned by `TestPeers_StallRecoveryStaggersEntry`, `TestPeers_BlindTimeEarnsNoEntry`,
  `TestPeers_StaggerProgressesThroughSustainedGaps`,
  `TestPeers_LateBeaterCannotEnterAsPeersAgeOut`, `TestPeers_StaleOwnRowStepsDown` and
  `TestPeers_CrashedLeaderHandsOverAfterABeat`.

  The rank itself is published as read: a lower place in the order is not
  "closer to dispatching" once the cut is a window, so holding the larger rank protects nothing. Pinned by
  `TestPeers_CutEntryIsWithheldAcrossAGap` and `TestPeers_CutEntryTakesTwoReadings`.

  **HOLD A GAPPED CUT AGAINST THE PREVIOUS RAW READING, NEVER AGAINST THE PUBLISHED VALUE.** The published
  value is itself held, so holding against it ratchets: across consecutive gapped readings a replica could
  only leave the cut - and a slow enough pass makes every reading gapped (below). A frozen cut is not
  fail-safe: a reader whose dispatchers left is never promoted, and the shard runs nothing. Against the raw reading, two consecutive readings that agree are
  believed whether or not either was gapped. Pinned by `TestPeers_CutEntryThroughSustainedGaps`.
- **`Withdraw` takes a replica out of the ranking without taking it out of the fleet**: it sets the row's
  `working=0` and makes a later repair write the same value. An owner calls it when it stops taking on work
  but still holds connections - a drain long enough to be worth it - so another replica takes over its
  dispatcher slots for the length of the drain rather than after it. It states `draining` in the same
  write, and `rowMu` serializes it against the beat, which carries the same column - unserialized, a beat
  that loaded the statement before `Withdraw` and wrote after it puts the old value back. Pinned by
  `TestPeers_WithdrawDropsTheReplicaFromEveryRanking`, `TestPeers_WithdrawStatesDraining` and
  `TestPeers_BeatCannotUndoAWithdrawal`.
- **`Draining` counts the OTHER fresh rows that state they hold a dispatcher-sized share from outside the
  cut** - a demoted replica finishing its claimed steps, or a withdrawn one finishing its drain. The owner
  adds it to its dispatcher count when dividing the budget, so the share being drained is divided around
  rather than grown into. It is a separate column, not a value of `working`, because the two are
  independent: a demoted replica is still working and must stay ranked (dropping it would move every
  peer's rank and the slot count), while a withdrawn one is not working and is still draining. Folding both
  into one column makes it a four-valued enum that every reader of `working` would have to decode.

  **This is not the "I dispatch" flag `dispatched_at` forbids.** That trap is a claim the PARTITION reads: a
  replica that set it and wedged would own a residue class forever. `draining` feeds only the pool divisor,
  so a replica stuck stating it costs every pool on the shard one share of headroom - the safe direction -
  and strands nothing. It is a statement that MOVES during the life of the process, so the beat carries it
  (and a change is published by the next pass, like a change in dispatch evidence), and a registration
  repair rewrites it. Its count is held across a gapped reading like `Replicas`: a rise shrinks pools and is
  believed at once, a fall waits a reading. Pinned by `TestPeers_ClassifyCountsDraining`,
  `TestPeers_DrainingRidesTheReadCadenceWhenItFlips` and `TestPeers_DrainingFallIsWithheldAcrossAGap`.

  **The divisor is `dispatcher slots + draining`, and only replicas OUTSIDE the cut state anything.** The
  cut already says who the dispatchers are, so a dispatcher states nothing, and the flag flips only when a
  replica leaves the cut with work still in flight. With `B` the shard's budget, `X` its dispatcher slots and
  `D` the other replicas draining, everyone dividing by the same count keeps the shard on budget during a
  DEMOTION drain: two dispatchers and one draining replica each hold a third. A SHUTDOWN
  drain is narrower: the owner's reconcile loop has stopped by then, so the withdrawn replica keeps the
  `B/X` share it had while its peers divide by `X+1`, and the shard is over by `B/(X(X+1))` for the drain.

  **Known residual, accepted: a demotion with work in flight runs over budget for up to about two scan
  intervals.** The replica taking the slot sizes its pool from its own reading of the fleet. The replica it
  displaced shrinks to the held share `B/(X+D+1)` on its own reading and raises `draining` on its next pass;
  each peer then needs a reading and a reconcile tick to divide by it. Until then the shard is over by about
  that held share - at `X=2`, `4B/3` at the peak, `B` once settled; at `X=4`, `6B/5`. Both stay within the
  budget-plus-one-share bound a join already accepts. A displaced replica with nothing in flight is over only
  until its own reading, and then by the two connections of the reader pool.

  **Do not close it with a flag every share-holder raises, dispatchers included** (a "holds a pool" column
  peers count instead of the cut). It closes the window only by registering every working newcomer as a
  holder before its rank is known - shrinking the fleet for every replica that then ranks out - and it
  makes every role change, on every replica, order a flag write against a pool push, where `draining` asks
  that only of a replica leaving the cut. Several moving parts on the common path to shorten a sub-second
  window on an event that happens a few times per deploy. Do not fold `draining` into `working` either (see above).

**A failed read publishes nothing at all.** A read that did not happen is not an observation that anybody
left. Pinned by `TestPeers_FailedReadHoldsTheLastGoodFleet` and
`TestPeers_ReplicaFallIsWithheldAcrossAGap`.

## The read is unfiltered, and that buys three things

No `WHERE` clause. The table holds one row per live replica plus a few stale ones, has no secondary index by
design, and is scanned whole either way — so a freshness predicate saves nothing and costs:

1. every window becomes a value this package can change without touching SQL;
2. the prune gets its candidate list from the *same* reading everything else is derived from, instead of a
   second aggregate query;
3. a row that is **absent** becomes distinguishable from one that is merely **stale** — which the
   registration repair needs.

`ORDER BY engine_id` is the database's, not Go's, so every replica orders the same rows identically. That
is what lets each derive a **distinct** ordinal with no coordination between them. Ages are `float64`
because `DATE_DIFF_MILLIS` is fractional on SQLite, where scanning it into an `int64` fails outright.

**A tempting refinement to reject:** because absence is now distinguishable, one could act on a clean
departure (a `Leave` DELETE — definitive evidence) instantly instead of withholding it, regrowing pools
faster on a rolling deploy. Do not. A truncated or swept registry then reads as the entire fleet departing
at once, and every replica grows to the full budget simultaneously — exactly the storm the debounce exists
to prevent, re-entered through a new door. Regrowth at three readings is already sub-second.

## Blindness, and why the prune anchors on it

`blind` is "the last successful read is older than **two** scan intervals," derived rather than configured:
one interval is a single missed reading, which a healthy Sonar meets on any transient error, while two
means the readings have actually stopped. **One threshold serves all three consumers** — it disables
partitioning, it interrupts the prune's healthy run, and it withholds a fall in the replica count — so a
gap counts against all of them or none. An own row that is not fresh (stale or missing) also interrupts the
prune's healthy run, and must: beats
failing fleet-wide while reads succeed age every peer past the straggler age with no gap at all, and a patience
that ran on through that would have every replica delete every other's row (pinned by
`TestPeers_StaleOwnRowRestartsThePrunePatience`).

**The prune is the most conservative of the three thresholds on purpose.** Every other decision here is
reversible — a wrongly dropped peer returns on the next reading — while a deleted row is refreshed by
nobody afterward. The scenario is a stall longer than the straggler age: it clears, every row in the table
is stale at once *including the pruner's own*, and a delete anchored on the clock rather than on what was
**observed** empties the registry for the whole fleet. Every replica then reads an empty roster, and
without the repair below nothing would ever re-register.

Waiting costs **nothing**: the freshness windows already exclude those rows from every count, so the delete
is pure table hygiene and hygiene deferred is hygiene. That asymmetry is the whole argument — for the two
counts, delay costs accuracy; for the delete, delay costs literally nothing.

Two structural guards on top of the patience, so a wipe is not merely unlikely:

- **Delete by an explicit id list, never a range predicate on the timestamp.** The server never
  re-evaluates a clock this process did not observe, there are no gap locks to contend over (the one
  MySQL-deadlock-shaped operation on this table), and a fleet-wide wipe is not expressible.
- **Never self** (`classify` excludes it). A replica that deleted its own row is refreshed by nobody.

Pinned by `TestPeers_PruneWaitsForAHealthyRun` and `TestPeers_PruneStandsDownAfterAGap` — the second
verified sensitive: removing the gap's reset of the healthy run makes it delete the stale row immediately.

## Self-absence: counted in one divisor, never in the other

- **`Replicas` counts self whether its row is MISSING or merely STALE.** This process exists and holds
  connections; leaving itself out under-counts, which over-sizes every pool. A stale own row is also
  exactly what a heartbeat starved of a connection produces — the moment when growing pools is most
  harmful.
- **`dispatchers` never counts self.** That divisor has to agree with what peers compute from the same
  table, so a replica whose row is absent must decline to partition (`ordinal = -1`) rather than claim a
  class its peers have handed to someone else.

Both pinned by `TestPeers_ClassifySelfAbsenceCutsBothWays`.

- **`Rank` follows `Replicas`, not the partition: self is ranked even when its row is missing.** That keeps
  the candidate count, and the dispatcher slots derived from it, from falling with one missing row. It does
  not put the replica in the cut: a replica whose own row is not fresh dispatches nothing (below), so a
  replica whose row went missing, a lone one included, waits for its repaired row to be read back. Pinned by
  `TestPeers_ClassifyRanksAnAbsentSelf`.

**The registration repair fires promptly, not on a hygiene cadence**, because being missing makes *peers*
under-count and over-size — the dangerous direction. It triggers on any successful read that does not
contain self, **including an empty one**: an empty registry is always wrong, since this process is in it by
definition, and reading it as "no peers" and stopping there would leave the whole fleet unregistered until a
restart. Pinned by `TestPeers_EmptyRegistryStillRepairsItself`.

`register`'s `RowsAffected` test is safe on MySQL (which counts *changed*, not matched, rows) because
neither caller can hit the one tripping case — an existing row whose `seen_at` already holds this
millisecond. At startup nothing has beaten yet; the repair path has just *observed* the row absent, and this
replica is the only writer of its own row, so that observation cannot be raced.

## Who dispatches: one shared order, a staggered cut per shard

A shard's dispatchers are chosen in two steps, and the split between this package and its owner is part of
the design.

**The order (here).** Every fresh row with `working=1` is a candidate; candidates are ordered by when they
joined - the row's `joined_at`, oldest first, ties to the lower id - and `Rank` is how many candidates come
ahead of this replica, published with the candidate count as one value - a rank only means anything against
the count it came from.
Rows with `working=0` (the owner's `Profile.Working` false) are counted in `Replicas` but take no place in the
order. Pinned by `TestPeers_ClassifyNeverRanksAnIdlePeer` and `TestPeers_ClassifyRanksAgreeAcrossReplicas`.

**Join time is the same on every shard, give or take a millisecond**, because every replica registers on every
shard as it starts, so every shard's registry yields the same order. That sameness is what lets the owner
stagger the cut. Two replicas joining within the same millisecond may order differently on two shards; that
dents the tiling, never agreement, because each shard's cut is computed from that shard's rows alone.

- `joined_at` is written when the row is INSERTed and never by the beat or the registration's UPDATE, so a
  replica restarting under a pinned id while its old row still exists keeps its place. A row that is deleted
  and re-created - a graceful leave, a registration repair - joins again, at the end.
- Ages are computed by the shard's database within one statement, so they compare exactly within a reading,
  and like every timestamp here are never compared across shards.
- A replica that cannot see its own row places itself as the newest, behind every peer it can see.
- **Why join time and not a hash of the id:** a newcomer always lands at the END of the order, so once there
  are as many candidates as roles (`C >= S*X`) adding a replica, or removing the newest, moves no live
  dispatcher at all - simulated at 4 shards and 8 replicas: 0 demotions per scale-up against 2.2 for a hash
  order, 0 per newest-removed against 1.5. A leave from the middle and a rolling-restart step (oldest
  replaced) cost the same either way. It is also the order that explains itself, and there is no hash
  function that must stay frozen across releases.

**The cut (the owner's, applied here).** The owner gives each Sonar a `CutFunc(rank, candidates) bool` with
the shard's place and slot count baked in (`SetCut`); the Sonar applies it to every reading and publishes
the answer as `InCut`. The engine's cut is a window: the shard at place `p` among the sorted shard indices
takes the `X = min(2, C)` candidates at positions `[p*X, p*X+X) mod C` (`engine/poolsize.go`, `inWindow`).
**Anchored on the place, not the index, because the steady state is what runs**: index-anchored windows
collide when indices have gaps (1, 3, 4) and leave replicas idle. The cost is a transient - while a shard is
added or retired below others, replicas briefly hold different shard sets and disagree on the windows of the
shards above it (between none and twice the dispatchers there) until the roll completes; a shard added above
every other moves nothing. The shards tile the order, so:

- **Every replica carries the floor or the ceiling of `S*X/C` roles** whenever the shards' rosters agree,
  which is the steady state. 4 shards x 8 replicas: one role each; 4 shards x 2 replicas: both dispatch
  everything. Pinned by `TestDispatchers_WindowsBalanceTheRoles`.
- **A join or leave moves at most one dispatcher per shard** when `C >= S*X`. A newcomer lands at the end of
  the order, past every window, so a join moves nobody; a leave from the middle shifts every later place down
  by one, so each shard whose window lies past the leaver swaps one dispatcher (one demotion, one promotion).
  Fewer candidates than slots make the windows wrap and a change in `C` moves more.
  `TestDispatchers_WindowsMoveOneDispatcherPerShard` pins the bound for a JOIN at every place in the order
  (`C` from `2S` to 16, up to 6 shards); a leave is the same transition in reverse and is covered only by
  that symmetry.
- **The price of the balance is churn, and it is accepted.** Unlike an independent draw, a fleet change can
  move the cut for replicas other than the one that joined or left - the mid-order leave above, or any change
  while the windows wrap. Fleet changes are deploys and scaling, and each move is made safe rather than
  avoided: **entering a cut takes two agreeing readings while leaving takes one**, so a handoff between two
  existing replicas goes leave-then-enter and errs a dispatcher short for a cadence, and the demotion hold
  keeps a demoted replica's in-flight steps on a share until they finish. **The one exception is a Sonar's
  first reading:** `New` seeds both the published standing and `lastRaw` in the cut (rank 0 of 1, so a Sonar
  dispatches before it has read anything), and a first reading that places it in the cut is therefore
  believed at once. The roles settle even again once the fleet does - pinned by
  `TestPeerRollingRestart_ShardsStayOnBudgetAndRebalance` (three replicas over two shards, the wrapping case:
  every sample stays within the budget plus one dispatcher share plus the reader pools, and after the roll
  every replica carries one or two of the four roles), and the handoff order by
  `TestPeers_CutEntryTakesTwoReadings`.
- **Each shard still decides from its own registry.** A replica blind on shard 3 can only be wrong about
  shard 3; the other shards' cuts never read its rows.

**Two shapes that lose, for reasons that recur:**

- **An independent draw per shard** (a per-shard rendezvous score, top `X` per shard). It agrees fleet-wide
  and churns least, but it does not balance: at 8 replicas and 4 shards a replica misses a shard's top two
  with probability 6/8 and all four with 0.75^4 ~ 0.32, so about a third of the fleet dispatched nothing while
  others carried three shards - uneven engine CPU, and the multi-shard-process case where GC cost was
  measured at ~28% of a replica's CPU. A consistent-hash ring is the same draw in another form and balances
  no better: simulated at 4 shards x 8 replicas, 34% of replicas held no role, against 32% for the draw and 0%
  for the window.
- **A fleet-wide assignment over every shard's roster** (bounded-load rendezvous: shards in order, each
  taking its top replicas below a role cap). It balances and churns less than the window, but every shard's
  cut then depends on every other shard's roster: one blind or lagging read moves dispatchers everywhere, and
  divergent views need a reconciliation rule. The window keeps the per-shard isolation this package is built
  around.

**A replica that is not working is never in the cut**, whatever its rank, and neither is one whose own row is
not fresh. **A missing own row still ranks** (self-absence follows `Replicas`, below), which keeps the
candidate count whole.

**The order, the window rule and its anchor are part of the wire.** Every process in a fleet must apply the
same ones, and a fleet is upgraded one replica at a time: during a rolling upgrade that changed any of them,
old and new replicas would cut the same rows differently, so a shard could be claimed by more dispatchers
than its budget feeds, or by none.

**A zero `Profile` states a replica that is NOT working.** `New` seeds `Working: true`, so a Sonar never given
a profile ranks normally, but `SetProfile` replaces the whole value: an owner that sets only the RTT would
take itself out of every cut. Always set `Working` alongside it. The `Profile` is written at registration and
at a repair, **never by the beat**: it is a fact about the process, not a liveness signal.

## Two cadences, because only one of them is detection

`scanInterval` (250ms) is how often the registry is **read**, and it is the fleet's entire detection
latency. `beatInterval` (1s) is how often this replica's own row is **written**, and it only has to sit well
inside the tightest window any reader applies — five beats fit in the dispatch window. Writing four times
more often buys no detection at all and costs a round trip on the connection pool the owner's dispatcher is
competing for, which is where that pool's contention is already the binding cost.

**Do not re-derive the two windows from one cadence.** They point in opposite directions (the table above),
so a shared multiplier silently ties the conservative one to the aggressive one.

**The read is the expensive one, and it carries three loads, only one of which is detection.** At four reads
a second per shard it takes the pool-contention argument the beat is held to, four times over, and no
consumer needs 250ms *detection* — both `Replicas` and `Partition` respond to deployment events. What sizes
it is the other two:

- **`Join`.** Every replica waits two of these before it may open a connection, so a one-second interval
  adds two seconds to every start and to every step of a rolling deploy.
- **Convergence.** There is **no nudge entry point**, deliberately: polling this often converges faster than
  a fleet-membership broadcast, which is what lets an owner drop the broadcast entirely rather than keep a
  delivery path correctness would then rest on. That argument is a function of this constant. Lengthen it
  and the trade re-opens — either accept slower convergence or add a nudge call to the API.

Lengthening it is legitimate (a 1s interval cuts the load 4x and widens every derived grace), but decide it
against those two, not against detection.

**The loop paces start to start** (`untilNextPass`), which is what makes the period actually be the scan
interval. Sleeping the interval *after* each pass instead makes the real period `scan + pass`, leaving the
blindness grace of two intervals with only one pass of margin — and a pass is one or two round trips on the
contended pool. In that regime **every** reading looks like it ended a gap, so the replica count can never
fall, the prune's patience never accumulates, and partitioning switches off, all at once. Each of those
fails in the safe direction, which is exactly why it would go unnoticed: the only symptom is a `BlindFor`
that occasionally crosses the grace, indistinguishable from a registry that genuinely stopped answering. A
floor of a quarter interval keeps a pass slower than the whole interval from turning the loop into
back-to-back readings against a database that is evidently already struggling. Pinned by
`TestPeers_PacingMeasuresFromThePassStart`, which makes each clock reading cost time so a pass has a
measurable duration — a wall-clock test cannot reach the regime, since a pass costs a fraction of a
millisecond against a 250ms interval.

Residual, and it is not fixable by pacing — a slow enough pass still reads as blind, at two different
thresholds. A **lone** slow pass among fast ones trips at `pass > scan`: the previous reading is a full
interval old when this one starts, so its age at publication is `scan + pass`. A **sustained** slow pass
settles at `pass > 1.75 × scan`, because the floor takes over the pacing and the period becomes
`pass + scan/4`. Both are a genuinely sick database at a 250ms interval, and all three consequences remain
fail-safe. `InCut` stays live through it only because a gapped cut is held against the previous raw reading
(see above); held against the published value it would freeze, and a frozen cut strands work.

The beat rides the *read's* cadence when the evidence bit **flips**. A starting replica's first turn lands
milliseconds after it starts, long before its next beat is due, and until it is published the replica is
absent from its own fleet's dispatcher count; the flip bounds that to a scan interval instead of a beat
interval. The same early publish applies when a replica *stops* serving — the direction that strands work.
Pinned by `TestPeers_BeatRidesTheReadCadenceWhenEvidenceFlips`.

A beat that FAILED rides the read's cadence too: it stays due, so the pass that first reaches the database after
an outage refreshes this replica's row before reading it. Do not schedule the next beat from a failed one: the
replica then reads its own row stale - and steps down from its cut - for up to a beat after every peer is back.
A write that keeps failing is therefore attempted once per read, never retried in a loop. Pinned by
`TestPeers_FailedBeatIsDueOnTheNextPass`.

## `Join` — announce before you consume

A joining replica sizes its own pool for the fleet it is joining, but its peers go on holding pools sized
for the fleet *without* it until they read again, so consuming immediately puts the shard's server over
budget by roughly one replica's share. `Join` inverts that — announce, wait, read — so peers shrink first
and the budget is respected at every instant rather than eventually.

**The wait is inside `Join` on purpose.** As a separate `JoinDelay()` an owner could read it and forget to
honour it, or honour it in the wrong place; folded in, the ordering is not something a caller can get
wrong, and the whole startup sequence is one call that returns with every getter seeded.

It waits **two** scan intervals, not one: a peer's read may have begun just before this replica's row was
committed, so that read proves nothing and only the one after it must see the row. It also gives a
simultaneously starting fleet's rows time to land, so the reading that follows is of a settled roster
rather than a partial one — and a partial one *under*-counts, which over-sizes pools. Derived from the scan
interval, so lengthening that cannot silently break either guarantee.

What it buys is peers having **stopped acquiring** beyond their new cap — lowering a pool's limit closes
nothing, so any surplus drains as connections are returned. Under load that is milliseconds; the guarantee
is about the decision, not the socket count.

## What is deliberately absent

- **No metrics.** The owner builds its own async gauges by pulling `Replicas`, `Rank`, `Partition` and `BlindFor` at
  collection time, which is one fewer scope to keep in step and needs no meter here.
- **Exactly two fault seams, both at an I/O boundary, and that is the whole rule for adding a third.**
  Everything reachable from *inside* the package needs none: `observe` takes an error directly, and every
  time-dependent decision takes the injected clock. A seam over pure logic would be a signal that a
  dependency should have been injected instead — and here it already is. What the two reach is what
  injection cannot: `FaultReadErr` is the only way an owner's assembled engine can be made blind, and
  `FaultBeatErr` the only way a test can occupy a *peer's* view of a replica that has stopped proving
  itself. Both consult unscoped OR shard-scoped, because per-shard blindness is the property this package
  exists to provide and would otherwise be untestable.

  `FaultBeatErr` gates **both** writes that prove liveness — the beat and the registration repair. Gating
  only the beat leaves the hole its own premise forbids: a peer eventually prunes the row this replica
  stopped refreshing, the repair then re-creates it with a *fresh* timestamp, and a replica that cannot
  prove its liveness proves it anyway through the other write. Pinned inside
  `TestPeers_BeatFaultStopsProvingLiveness`, including that lifting the fault restores the repair — so the
  gate is the fault and not a broken repair path.
- **No checkpoint seams.** Nothing here has a moment a test needs to hold.
- **No config setters beyond `SetCadence`.** See the API note above for the bar it cleared.
- **No callback to the owner.** The Sonar publishes into atomics and the owner *pulls*. An edge
  notification would have to be fired from every path that can move a published value — a confirmed fall, a
  recovery from blindness, a repair, a prune — and missing one leaves the owner's derived sizes stale
  forever with no backstop, whereas a reconcile loop covers every path by construction. It would also put
  the owner's pool policy on this Sonar's goroutine, coupling one shard's beat to another shard's slow
  push.

## Tests

Real SQL through `internal/database.ShardSet` (a **test-only** import — a Sonar never opens a handle) plus
an injected clock, so every time-dependent rule is driven by advancing time rather than sleeping and the
whole suite stays parallel and sub-second. The clock is an unexported field with no setter: it is a test
seam, not API.

Note the rig replaces `now` **and** re-seeds `lastGood`. Swapping only the clock leaves a real timestamp
being compared against a synthetic one, which reads as either decades of blindness or none at all. The two
tests that exercise real pacing — `Join`'s wait and `Run`'s loop — put the real clock back and shorten the
scan interval instead.

**`Run`'s loop test also drops `t.Parallel`, and it is the only test here that does.** It asserts an UPPER
BOUND on lateness - the loop is never blind, never two cadences without a reading - and an upper bound is the
one shape co-running tests break, because they can only make the loop turn slower, never faster. CPU
oversubscription then pushes a healthy loop past the grace and the failure reports the machine. The same
starvation resets `healthySince`, so both of that test's real properties ride on its goroutine being
scheduled. An OUTCOME assertion tolerates parallelism (it just waits longer); this does not - the same rule
`fixtures/CLAUDE.md` applies to its reaction-latency fixtures. The rule rests on the shape of the assertion -
an upper bound against two dozen parallel siblings, several driving their own real-clock loops - more than on a
reproduction: the test passes `-count=10 -race` alone, and a failure under the full parallel `-race` suite is
rare.

**Those two are the only tests a slow database can race, and both rules below were learned by it happening.**
Every threshold in this package is a multiple of the scan interval, so shortening that interval shortens the
grace the assertions are measured against:

- **Read the getters BEFORE any assertion queries anything.** `Partition` is gated on the blindness grace,
  so a round trip of the test's own between `Join` and the assertion spends the exact margin under test.
  Measured on an idle Postgres: one `SELECT engine_id FROM dwarf_peers` moved `BlindFor` from **2µs to
  2-8ms** — already a third of a 20ms budget, and past it on a loaded box. The same query is microseconds
  on SQLite, which is why this only ever fails on a real database.
- **Do not shorten the cadence below a round trip plus scheduler jitter** (the test cadences sit at
  100-250ms). At a 20ms cadence the grace is 40ms, which a co-running suite eats outright — and the test then
  correctly reports a cadence the machine, not the code, failed to keep.

Prefer stating a fact as an **order** rather than a duration where it can be: "`lastGood` is newer than the
call" is the same fact as "the read landed" with no wall-clock budget attached.

When advancing the clock across several passes, keep each step inside the blindness grace (two scan
intervals) or the pass will correctly treat the jump as a gap and reset the healthy run — which is what
makes a naive "advance five minutes, then prune" test silently prove the opposite of what it intends.
