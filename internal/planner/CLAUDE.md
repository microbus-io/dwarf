# Dwarf `internal/planner` — cross-shard band + fairness planning from per-shard tallies

> Load when: changing `Tally`/`Clear`/`Plan`/`LastBand` semantics, the merge, the fairness lottery, or
> the slice rule.
> Coupled with: `engine/CLAUDE.md` §"Execution Model" — the *scan* that produces a `Tally`, the *fetch*
> that resolves a `Plan` back to steps, the cadence the caller runs this at, and the candidate cache the
> result lands in all live there. This package is only the decision in the middle.

## Why the package exists

Two scheduling rules are **global** while the thing that enforces them is **per-shard**. Strict priority
says no worse band is served while a better one has due work anywhere in the fleet; weighted fairness says
the distinct keys at that band split the batch by weight. Neither is evaluable from one shard's view, so
one shard cannot decide what it may dispatch on its own.

The tally map is what a cross-shard **barrier** used to be. A barrier made every shard wait for the
slowest before anyone could plan — an order-statistics tax measured at 2.02x by 6 shards, on the path that
is the throughput ceiling. Retaining each shard's last tally costs one struct and zero extra queries, and
lets every shard plan on its own clock. The trade: a shard sees its peers up to one of their cycles stale.
Fairness (slowly varying) absorbs that; dispatch never sees it, because a shard's own tally is always fresh
by the time it plans.

## The load-bearing properties

**A `Tally` is O(distinct keys), never O(backlog), and that is the whole point.** One row per fairness key,
carrying a `Count` — not one row per step. The producer is expected to cap `Count` at the planning
capacity; the cap is lossless because no key is ever assigned more than the whole batch, so a count above
capacity is indistinguishable from capacity. A caller that reports per-step defeats the entire three-phase
shape and reintroduces the scan cost the design exists to avoid.

**A key's weight comes from its OLDEST due step, never its newest.** Both locally (one shard's tally) and
in the merge (`max` age wins across shards). Keying off the newest would let a tenant self-promote by
queueing fresh high-weight work in front of its own backlog.

**The lottery re-rolls per SLOT, not per key** (`pick`, Efraimidis-Spirakis over the keys). That is what
makes a key's expected share proportional to its weight and **independent of backlog depth** — a tenant
with a million queued steps and one with ten get the same share at the same weight. Rolling once per key
and handing out runs would reintroduce depth-proportionality through the back door.

**Every caller rolls its own plan independently.** The rolls are uncoordinated, which changes nothing in
expectation and needs no cross-shard agreement.

**`pick` is O(capacity x distinct keys), and it is the one cost in the whole design that scales with key
CARDINALITY.** The three-phase split bounds wire cost and (on Postgres) server scan cost against cardinality;
this is not covered by either. The inner loop calls `randFloat` and `math.Pow` once per key per slot, so at
the reference capacity of 768 against a few thousand keys at the band it is millions of `Pow` calls, per
shard, per cycle.

There is a known faster shape with **identical semantics**: an Efraimidis-Spirakis top-1 draw with exponent
`1/w` selects key *i* with probability `w_i / Σw`, so each slot is just a weighted draw, and the loop as a
whole is weighted sampling with replacement plus exhaustion. A Fenwick tree over the weights gives
O(K + C·log K).

**It is deliberately NOT built yet, because it cannot honestly be justified until it is measured** - the same
bar every other refiller tuning change in this codebase was held to, and the one three adaptive designs
failed. What was missing was the measurement itself: `Result.Planning` was computed by the pipeline and then
dropped, so planning cost was observable only as `cycleDuration` minus the two query phases. The piston now
records it (`phase="planning"`). Get a real cardinality profile off that before touching the lottery.

**`slice` must be deterministic, and this is the subtle one.** Each shard runs it independently and they
must agree on who owns which slot *without exchanging a word*. Same plan + same snapshot must produce the
same assignment on every caller, so nothing here may depend on map iteration order — hence shard-ordinal
iteration, insertion-ordered keys out of `merge`, and largest-remainder rounding with an explicit tie-break.

**The first slot of each key goes to the shard holding that key's oldest step.** Not an optimization: a key
with one old step on a quiet shard and a constantly-replenished backlog on a busy one would see a purely
proportional split round the quiet shard to **zero, cycle after cycle, forever**. The head slot is the
starvation guard. Below the head the interleave is approximate by design — each shard's own fetch keeps its
steps oldest-first, and the head carries the globally-oldest.

## Shape decisions

**A `Plan` carries no verdict, and does not say *why* it is empty.** Nothing due anywhere, nothing due
here, above the global band, out-slotted at the band — all four take the same action, hold no candidates,
so `len(Slots)==0` is the entire test. This needs no special case in the code either: a shard above the
band, or with nothing due, holds none of the planned keys and falls out of `slice` with zero slots on its
own. A caller that wants the distinction for a log line compares `Plan.GlobalBand` against the band it just
tallied. Do not add an enum back for the three-way distinction — no caller can act on it differently.

**Participation is DECLARED, never inferred: there is no timeout, and no shard is ever dropped for being
quiet.** Each cycle a shard either `Tally`s what it saw or `Clear`s because it could not look.

The hazard being handled is real and severe. A shard whose scan fails still holds the best band in the
map; that claim then makes every peer compute the same global minimum, find none of *its* keys there, and
dispatch nothing — forever, waiting on a shard that will never report again. Strict priority turns into a
suicide pact. So a failed shard genuinely must leave the map.

The question is only how the planner learns of it, and a clock is the wrong answer — not because the
constants are hard to tune, but because it is **inference where a fact is available**. The reports come
from the caller's own per-shard workers over a shard set fixed at startup; the caller *knows* its scan
failed, and a timeout throws that away and reconstructs it from silence. Worse, silence is genuinely
ambiguous: a shard reporting slowly (a deep backlog can make a scan take seconds) is indistinguishable
from a dead one by elapsed time alone, and guessing wrong in that direction is itself harmful — dropping
a live shard erases its band from the global minimum, so peers dispatch worse work, and erases its keys
from the plan, so it wins no slots when it does return. Every timeout-shaped design spends its complexity
budget on that ambiguity and still gets it wrong at the tails.

`Clear` dissolves the ambiguity rather than managing it. A shard that could not look is excluded because
it *told* the planner, and it would not have been dispatchable this cycle in any case — the global band
means the best band with due work that someone can actually serve. A shard that is merely slow says
nothing, keeps its tally, and stays counted, which is correct: it is alive and its last report is still
the best information anyone has. Detection is also immediate on the failing cycle instead of a cutoff
later, and recovery costs exactly one cycle with no cooldown.

**Do not add a timeout back as a "backstop."** The residual case it would target — a worker alive but
wedged forever, so it neither tallies nor clears — belongs upstream: put a deadline on the scan and it
cannot hang, so it returns rows or an error and both are signals. A planner-side timer would instead hide
a liveness bug behind a plausible-looking recovery, while reintroducing the slow-versus-dead question in
full. Note too that a wedged shard is not dispatching either, so the fleet is already degraded; papering
over the symptom costs the diagnosis.

**Weight normalization lives in `Tally`.** A non-positive weight makes the lottery's `1/weight` exponent
infinite and the key **permanently unpickable** — silent starvation, no error anywhere. Normalizing at the
boundary means no producer can reintroduce it.

**`LastBand` reports a negative band for "nothing to report."** Its consumer labels a metric series with
the value, so returning an idle fleet's `math.MaxInt` would publish a series named for a priority no caller
can ever set.

## Contracts a caller must hold

**`Tally` before `Plan`, every cycle.** Planning first means planning against your own previous report —
at best a wasted cycle, at worst claiming a band you no longer hold. The two are deliberately *not* folded
into one call: tests need to seed a *peer's* tally to fabricate a competing shard, so both entry points
have to exist anyway and the fold would buy less than it costs in clarity.

**Every cycle, every shard either `Tally`s or `Clear`s — staying quiet is not an option a caller has.**
A shard with nothing due still reports (band `math.MaxInt`, no tallies), because "nothing due here" is
information its peers need: it may raise the global band and release them. A shard whose scan failed
clears. Silence means "my previous tally still stands," which is only true while the shard is genuinely
mid-cycle — a caller that goes quiet on failure instead of clearing is what wedges the fleet.

**The `tallies` slice is HANDED OVER — retained *and* normalized in place.** Entries are *replaced*, never
mutated, which is what lets `snapshot` hand out pointers and hold the lock for the map copy only — never
across the planning itself, which would re-couple the very cycles this design decouples. So a caller must
neither mutate nor reuse a slice it has passed. The sharp case is passing the **same backing array twice**:
the second call's weight normalization writes into an array the first call's snapshot may still be handing
to an unlocked `Plan`. Latent today — every producer allocates fresh per cycle — but it is the contract, not
a coincidence, and the godoc says so rather than only forbidding caller-side mutation (which read as though
the planner did none of its own).

## What this package deliberately does not do

No I/O, no database, no cache, no steps — it deals in fairness keys and counts, and resolving a key to
actual step ids is the caller's job. It does not decide *when* to plan (cadence, pacing, and rate limiting
are the caller's), and it does not own the candidate cache the result lands in.

## The guarantee it cannot provide on its own

A shard learns of newly-arrived better-band work from its own scan; peers learn of it **only** through that
shard's next `Tally`. So a new band becomes globally visible one caller-cycle later (this shard reports)
plus up to one more (a peer plans on its own next cycle). Inside that window a peer computes a stale global
minimum, finds itself holding it, and legitimately dispatches worse-band work. **Cross-shard priority is
therefore strict within one or two caller cycles, not instantly** — an inversion of observable dispatch
*order*, not merely of latency. This is a known, accepted property; the engine-side statement of it, and
the public one, are cross-referenced from `engine/CLAUDE.md`.

If it is ever worth closing, the shape is an `Announce(shard, band)` that writes an arriving band straight
into the map without waiting for a scan. Note the trap before trying: `merge` skips row-less entries when
computing the minimum, so a bare band announcement is invisible and would need a synthetic entry — which
then raises how long it lives, what happens when the real tally contradicts it, and whether a peer can be
wedged by an announcement from a shard that then goes quiet. Not designed; recorded so the next attempt
starts past the first wrong turn.
