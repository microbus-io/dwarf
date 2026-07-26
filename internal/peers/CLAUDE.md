# Dwarf `internal/peers` — one shard's replica registry

> Load when: changing the Sonar's lifecycle, the four statements, the two windows, the blindness rules, or
> the prune.
> Coupled with: `internal/migrations/CLAUDE.md` (the `dwarf_peers` columns these statements touch) and
> `engine/CLAUDE.md` §"Peer discovery" and §"Connection pool sizing" (what the published values are used
> for).

One Sonar works one shard. It owns this replica's row in that shard's registry — creates it, refreshes it,
deletes it — reads the whole registry on its own cadence, and publishes what the reading implies: the pool
divisor, the work partition, and how stale the answer is.

**Nothing here spans shards, and it must not start to.** Every timestamp in the registry is stamped by the
shard that holds it (`NOW_UTC()`), so ages are comparable *within* a shard and timestamps are comparable
nowhere. A fleet view assembled from two shards would be ranking two clocks, and a clock-fast shard would
win every comparison regardless of what it actually knew. Keeping the type single-shard is what makes that
error unexpressible rather than merely avoided — the whole package holds one `*sequel.DB` and has no
concept of a second.

The per-shard scope is also the *right* answer rather than a simplification. Evidence and decision share a
scope: a peer whose dispatcher wedges on shard 3 drops out of **shard 3's** work divisor and stays in every
other shard's, and a peer whose beats to shard 3 fail mis-sizes only shard 3's pool. A process-global
divisor derived from one shard's rows can express neither.

## The row's whole lifecycle lives here, and that is what makes deletion final

Registration creates, the beat refreshes, `Leave` deletes, the prune deletes other replicas' corpses. All
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

**The API is ten calls, and keeping it that small is a standing constraint.** Three inputs (`New`,
`SetEvidence`, `SetLogger`), three lifecycle calls (`Join`, `Run`, `Leave`), three published facts
(`Replicas`, `Partition`, `BlindFor`), and one knob (`SetCadence`).

The windows and the beat are **constants with plain fields behind them**, not setters, because they are
this package's policy — derived from which errors are safe in which direction rather than from anything an
owner knows better. An earlier cut exported a setter/getter pair for each, which doubled the surface for
the benefit of tests that live *in this package* and can assign the fields directly.

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
- **`busy` is evidence too.** A scan can legitimately outlast the dispatch window on a deep backlog, so
  without it every healthy replica in a loaded fleet would fall out of the divisor at once — precisely when
  overlapping selection costs the most.
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
| `Replicas` (pool divisor) | under-count → over-size pools → collapse a database | **hold, and withhold a fall across a gap** |
| `Partition` (work divisor) | over-count → a residue class nobody selects → stranded work | **fail open** |
| prune (hygiene) | a wrong delete is the only irreversible act here | **wait 5 minutes** |

- **`Replicas` rises instantly, and a fall is withheld on the one reading that ended a blind spell.** A
  rise shrinks pools, which is safe; a fall grows every pool derived from it. The case it exists for is not
  one peer dying but a **correlated stall**: a few seconds of database trouble stalls every replica's write
  *and* every replica's read at once; it clears, everyone reads a roster where every row is stale, everyone
  computes a tiny count, and everyone grows to the full per-database budget simultaneously against a
  database that is already sick. Skipping exactly one reading closes it, because the beat is far shorter
  than the read cadence — by the next reading every live peer has refreshed its row and the count is real.

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

**A failed read publishes nothing at all.** A read that did not happen is not an observation that anybody
left. Pinned by `TestPeers_FailedReadHoldsTheLastGoodFleet` and
`TestPeers_ReplicaFallIsWithheldAcrossAGap`.

## The read is unfiltered, and that buys three things

No `WHERE` clause. The table holds one row per live replica plus a few corpses, has no secondary index by
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
gap counts against all of them or none, and there is no second number to keep in step.

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
verified sensitive: removing the gap's reset of the healthy run makes it delete the corpse immediately.

## Self-absence: counted in one divisor, never in the other

- **`Replicas` counts self whether its row is MISSING or merely STALE.** This process exists and holds
  connections; leaving itself out under-counts, which over-sizes every pool. A stale own row is also
  exactly what a heartbeat starved of a connection produces — the moment when growing pools is most
  harmful.
- **`dispatchers` never counts self.** That divisor has to agree with what peers compute from the same
  table, so a replica whose row is absent must decline to partition (`ordinal = -1`) rather than claim a
  class its peers have handed to someone else.

Both pinned by `TestPeers_ClassifySelfAbsenceCutsBothWays`.

**The registration repair fires promptly, not on a hygiene cadence**, because being missing makes *peers*
under-count and over-size — the dangerous direction. It triggers on any successful read that does not
contain self, **including an empty one**: an empty registry is always wrong, since this process is in it by
definition, and reading it as "no peers" and stopping there would leave the whole fleet unregistered until a
restart. Pinned by `TestPeers_EmptyRegistryStillRepairsItself`.

`register`'s `RowsAffected` test is safe on MySQL (which counts *changed*, not matched, rows) because
neither caller can hit the one tripping case — an existing row whose `seen_at` already holds this
millisecond. At startup nothing has beaten yet; the repair path has just *observed* the row absent, and this
replica is the only writer of its own row, so that observation cannot be raced.

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
  and the trade re-opens — either accept slower convergence or add a tenth call to the API.

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
fail-safe.

The beat rides the *read's* cadence when the evidence bit **flips**. A starting replica's first turn lands
milliseconds after it starts, long before its next beat is due, and until it is published the replica is
absent from its own fleet's dispatcher count; the flip bounds that to a scan interval instead of a beat
interval. The same early publish applies when a replica *stops* serving — the direction that strands work.
Pinned by `TestPeers_BeatRidesTheReadCadenceWhenEvidenceFlips`.

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

- **No metrics.** The owner builds its own async gauges by pulling `Replicas`, `Partition` and `BlindFor` at
  collection time, which is one fewer scope to keep in step and needs no meter here.
- **No fault or checkpoint seams.** Every interesting failure is reachable by calling `observe` with an
  error, and every time-dependent decision by advancing the injected clock. A seam over pure logic is a
  signal that a dependency should have been injected instead — and here it already is.
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

When advancing the clock across several passes, keep each step inside the blindness grace (two scan
intervals) or the pass will correctly treat the jump as a gap and reset the healthy run — which is what
makes a naive "advance five minutes, then prune" test silently prove the opposite of what it intends.
