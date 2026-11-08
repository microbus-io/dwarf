# Dwarf `internal/planner` — cross-shard band + fairness planning from per-shard tallies

> Load when: changing `Tally`/`Clear`/`Plan`/`Reach`/`LastBand` semantics, the merge, the band fill, the
> fairness lottery, or the slice rule.
> Coupled with: `engine/CLAUDE.md` §"Execution Model" — the *scan* that produces a `Tally`, the *fetch*
> that resolves a `Plan` back to steps, the cadence the caller runs this at, and the candidate cache the
> result lands in all live there. This package is only the decision in the middle.

## Why the package exists

Two scheduling rules are **global** while the thing that enforces them is **per-shard**. Strict priority
says a worse band gets only the capacity every better band leaves unused; weighted fairness says the
distinct keys at each band split that band's share of the batch by weight. Neither is evaluable from one
shard's view, so one shard cannot decide what it may dispatch on its own.

The tally map stands where a cross-shard **barrier** would. Do not reintroduce one: a barrier makes every
shard wait for the slowest before anyone can plan — an order-statistics tax measured at 2.02x by 6 shards,
on the path that is the throughput ceiling. Retaining each shard's last tally costs one struct and zero extra queries, and
lets every shard plan on its own clock. The trade: a shard sees its peers up to one of their cycles stale.
Fairness (slowly varying) absorbs that; dispatch never sees it, because a shard's own tally is always fresh
by the time it plans.

## The load-bearing properties

**The fill is work-conserving: best band first, then spill.** `fill` runs the best band's lottery for the whole
capacity, and the next band's for whatever is left, until capacity or the bands run out. **Do not draw a plan
from the best band alone.** It reads as the strict rule and is not: a sparse but continuous better band - due at
nearly every scan, never enough to fill the batch - leaves the rest of the batch idle every cycle while the worse
band never moves, with the workers idle. Pinned by `TestPlanner_FillSpillsToWorseBands`, and end to end by
`fixtures/bandspillflow_test.go`.

**Fairness is decided WITHIN a band, never across.** A key with due work at two bands is two independent
entries (the merge keys on `Slot{Band, Key}`); its share at one band says nothing about its share at the other,
and a band's lottery never sees another band's keys.

**`Plan.Reach` is the band where the fill ran out of capacity, and it needs no lottery.** `pick` draws
`min(capacity left, the band's total count)` whatever it rolls, so `reach` is a cumulative sum. It is the cache's
bar for admitting a doorbell (work at the reach is still waiting uncached, so a worse band admitted outside the
plan would run first) and the dispatch-lag gauge's bar for what strict priority is holding back. `math.MaxInt`
when everything due fit - then nothing is held back at all.

**A `Tally` is O(distinct keys), never O(backlog), and that is the whole point.** One row per fairness key per
band, carrying a `Count` — not one row per step. The producer is expected to cap `Count` at the planning
capacity; the cap is lossless because no key is ever assigned more than the whole batch, so a count above
capacity is indistinguishable from capacity. A caller that reports per-step defeats the entire three-phase
shape and reintroduces the scan cost the design exists to avoid.

**A producer may stop at the band where its own counts reach capacity, and that is lossless too.** The global
cumulative count at any band is at least one shard's, so no fill reaches past the band where a single shard
fills it. The piston's tally stops there, which is what keeps a deep band below the reach from being read.

**A key's weight is the LOWEST among its due steps.** Both locally (one shard's tally) and in the merge (`min`
across shards). Any reading that can RISE when a tenant queues more work - the newest step's weight, the
highest, an average - lets a tenant self-promote by queueing heavier work; the lowest can only fall, and a
light flow then lowers only its own key. The oldest step's weight would be safe too, but an aggregate cannot
read it: it takes a lookup per key, and a fan-out puts thousands of a key's steps in one millisecond.

**The lottery re-rolls per SLOT, not per key** (`pick`, a weighted draw over the keys). That is what
makes a key's expected share proportional to its weight and **independent of backlog depth** — a tenant
with a million queued steps and one with ten get the same share at the same weight. Rolling once per key
and handing out runs would reintroduce depth-proportionality through the back door.

**Every caller rolls its own plan independently.** The rolls are uncoordinated, which changes nothing in
expectation and needs no cross-shard agreement.

**Only the weights' ratios may reach the draw.** `pick` divides the weights of the keys still holding steps
by the largest among them, again whenever a key runs out, and takes one cumulative draw per slot (key *i* with
probability `w_i / Σw`). Rescaling once is not enough: against a dominating key the others can round to zero,
and re-summing those rounded values after it runs out cannot restore their ratios. **Do not draw by comparing `U^(1/w)` per
key** (Efraimidis-Spirakis top-1): it is the same distribution in exact arithmetic, but at a large weight
every score rounds to 1 and at a small one underflows to 0, and the strict comparison then hands the first
key every slot - two keys at weight 1e20 split 100/0. `Tally` rewrites a NaN or infinite weight to 1 beside
a non-positive one, since either poisons the sum for every key at the band. Pinned by
`TestPlanner_PickIsScaleInvariant`, `TestPlanner_PickRebasesWhenTheLargestKeyRunsOut` and
`TestPlanner_TallyNormalizesWeight`.

**`pick` is O(capacity x distinct keys), and it is the one cost in the whole design that scales with key
CARDINALITY.** The three-phase split bounds wire cost and (on Postgres) server scan cost against cardinality;
this is not covered by either. Each slot walks the live keys' weights once, so at the reference capacity of
768 against a few thousand keys at the band it is millions of additions, per shard, per cycle. A Fenwick
tree over the weights would make it O(K + C·log K) with identical semantics.

**The tree is deliberately NOT built, because it cannot honestly be justified until it is measured** - the same
bar every other refiller tuning change in this codebase was held to, and the one three adaptive designs
failed. The piston records planning cost directly (`dwarf_refill_query_duration_seconds{phase="planning"}`);
get a real cardinality profile off that before touching the lottery.

**`slice` must be deterministic, and this is the subtle one.** Each shard runs it independently and they
must agree on who owns which slot *without exchanging a word*. (`fill` is deterministic in its LENGTH per band,
which is all `reach` and the band boundaries depend on; the lottery's order within a band is the only random
part, and every shard rolls its own.) Same plan + same snapshot must produce the
same assignment on every caller, so nothing here may depend on map iteration order — hence shard-ordinal
iteration, insertion-ordered keys out of `merge`, and largest-remainder rounding with an explicit tie-break.

**The first slot of each key goes to the shard holding that key's oldest step.** Not an optimization: a key
with one old step on a quiet shard and a constantly-replenished backlog on a busy one would see a purely
proportional split round the quiet shard to **zero, cycle after cycle, forever**. The head slot is the
starvation guard. Below the head the interleave is approximate by design — each shard's own fetch keeps its
steps oldest-first, and the head carries the globally-oldest.

## Shape decisions

**A `Plan` carries no verdict, and does not say *why* it is empty.** Nothing due anywhere, nothing due
here, only work past the reach, out-slotted — all four take the same action, hold no candidates, so
`len(Slots)==0` is the entire test. This needs no special case in the code either: a shard holding only work
past the reach, or nothing due, holds none of the planned slots and falls out of `slice` with zero slots on
its own. Do not add an enum back for the distinction — no caller can act on it differently.

**`Plan.Tallied` is NOT that enum, and the difference is that a caller genuinely acts on it.** All four
cases above are *positive statements made by a shard that reported*; `Tallied=false` says the shard is not
in the map at all — it cleared, or has not reported yet — which is **unknown**, a fifth thing entirely. The
consumer is `internal/pipeline`'s Supplier: an empty plan from a reporting shard clears that shard's cache
partition (nothing is dispatchable, so every hint is dead), while an untallied shard must leave it exactly
as it found it, because throwing away live candidates on a database blip idles that shard's workers for a
cycle. It has to be explicit because the caller's scanning and pushing loops turn independently: the pushing
one keeps going while a scan is failing, so it must be able to tell *absent* from *empty-handed* without
asking.

**Participation is DECLARED, never inferred: there is no timeout, and no shard is ever dropped for being
quiet.** Each cycle a shard either `Tally`s what it saw or `Clear`s because it could not look.

The hazard being handled is real and severe. A shard whose scan fails still holds its last tally in the
map; when that tally claims a better band deep enough to fill the batch, every peer finds no capacity left to
spill into and dispatches nothing — forever, waiting on a shard that will never report again. Strict priority
turns into a suicide pact. So a failed shard genuinely must leave the map.

The question is only how the planner learns of it, and a clock is the wrong answer — not because the
constants are hard to tune, but because it is **inference where a fact is available**. The reports come
from the caller's own per-shard workers over a shard set fixed at startup; the caller *knows* its scan
failed, and a timeout throws that away and reconstructs it from silence. Worse, silence is genuinely
ambiguous: a shard reporting slowly (a deep backlog can make a scan take seconds) is indistinguishable
from a dead one by elapsed time alone, and guessing wrong in that direction is itself harmful — dropping
a live shard erases its bands from the fill, so peers dispatch worse work, and erases its keys
from the plan, so it wins no slots when it does return. Every timeout-shaped design spends its complexity
budget on that ambiguity and still gets it wrong at the tails.

`Clear` dissolves the ambiguity rather than managing it. A shard that could not look is excluded because
it *told* the planner, and it would not have been dispatchable this cycle in any case — the fill means
the due work that someone can actually serve. A shard that is merely slow says
nothing, keeps its tally, and stays counted, which is correct: it is alive and its last report is still
the best information anyone has. Detection is also immediate on the failing cycle instead of a cutoff
later, and recovery costs exactly one cycle with no cooldown.

**Do not add a timeout back as a "backstop."** The residual case it would target — a worker alive but
wedged forever, so it neither tallies nor clears — is not the planner's to detect, and it cannot be
removed upstream either: the scan deliberately carries no deadline (`engine/CLAUDE.md`, "Why no deadline
on background reads"), since any bound is a guess at backlog depth and a scan that can never finish inside
it clears its shard every cycle. A hung scan therefore shows as a rotting tally, and
`dwarf_refill_tally_age_seconds` (`TallyAge`) is the signal that names it. A planner-side timer would
instead hide a liveness bug behind a plausible-looking recovery, while reintroducing the slow-versus-dead
question in full. Note too that a wedged shard is not dispatching either, so the fleet is already
degraded; papering over the symptom costs the diagnosis.

**Weight normalization lives in `Tally`.** The lottery draws in proportion to weight, so a zero or negative
weight leaves its key **permanently unpickable** — silent starvation, no error anywhere — and NaN or an
infinity poisons the draw for every key at the band. Normalizing at the boundary means no producer can
reintroduce it.

**`LastBand` reports the last plan's best band, and a negative one for "nothing to report."** An idle fleet's real value is
`math.MaxInt`, which reads as a genuine band — wrong in the direction of looking valid — so it is reported
as a sentinel a caller can test for instead. Its readers are the piston's tests and this package's own;
no metric reads it, so treat the key count as a test observable rather than a shipped signal.

**`Reach` is the production read of what is being served, and it reads the TALLIES, not the last plan.** The
piston's dispatch lag leaves out every band worse than it, because strict priority, not dispatch, is holding
that work. The last plan's reach would be wrong there: it moves only when something plans, so a stalled supply
loop would keep a reach that has since moved and hide the very lag the gauge exists to show.

## Contracts a caller must hold

**A caller that BOTH tallies and plans must tally first.** Planning first means planning against your own
previous report — at best a wasted cycle, at worst claiming a band you no longer hold. A caller whose
planning runs on a *separate* cadence from its tallying does not violate this and cannot honour it: it plans
from whatever report stands, many times per tally, and `Plan.Tallied` is what tells it whether one stands at
all. That is the shape `internal/pipeline` uses. The two are deliberately *not* folded
into one call: tests need to seed a *peer's* tally to fabricate a competing shard, so both entry points
have to exist anyway and the fold would buy less than it costs in clarity.

**Every cycle, every shard either `Tally`s or `Clear`s — staying quiet is not an option a caller has.**
A shard with nothing due still reports (no tallies), because "nothing due here" is information its peers
need: it may free capacity for them to spill into. A shard whose scan failed
clears. Silence means "my previous tally still stands," which is only true while the shard is genuinely
mid-cycle — a caller that goes quiet on failure instead of clearing is what wedges the fleet.

**The `tallies` slice is HANDED OVER — retained *and* normalized in place.** Entries are *replaced*, never
mutated, which is what lets `snapshot` hand out pointers and hold the lock for the map copy only — never
across the planning itself, which would re-couple the very cycles this design decouples. So a caller must
neither mutate nor reuse a slice it has passed. The sharp case is passing the **same backing array twice**:
the second call's weight normalization writes into an array the first call's snapshot may still be handing
to an unlocked `Plan`. Every producer allocates fresh per cycle, so nothing hits it - but it is the contract,
not a coincidence, and the godoc says so rather than only forbidding caller-side mutation (which would read
as though the planner did none of its own).

## What this package deliberately does not do

No I/O, no database, no cache, no steps — it deals in fairness keys and counts, and resolving a key to
actual step ids is the caller's job. It does not decide *when* to plan (cadence, pacing, and rate limiting
are the caller's), and it does not own the candidate cache the result lands in.

## The guarantee it cannot provide on its own

A shard learns of newly-arrived better-band work from its own scan; peers learn of it **only** through that
shard's next `Tally`. So a new band becomes globally visible one caller-cycle later (this shard reports)
plus up to one more (a peer plans on its own next cycle). Inside that window a peer fills from a stale
picture and legitimately dispatches worse-band work. **Cross-shard priority is
therefore strict within one or two caller cycles, not instantly** — an inversion of observable dispatch
*order*, not merely of latency. This is a known, accepted property; the engine-side statement of it, and
the public one, are cross-referenced from `engine/CLAUDE.md`.

If it is ever worth closing, the shape is an `Announce(shard, band)` that writes an arriving band straight
into the map without waiting for a scan. Note the trap before trying: `merge` builds bands only from tally
rows, so a bare band announcement is invisible and would need a synthetic entry — which
then raises how long it lives, what happens when the real tally contradicts it, and whether a peer can be
wedged by an announcement from a shard that then goes quiet. Not designed; recorded so the next attempt
starts past the first wrong turn.
