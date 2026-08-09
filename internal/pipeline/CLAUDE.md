# Dwarf `internal/pipeline` — one shard's supply, in two independently paced loops

> Load when: changing either `Cycle`'s phases or its error policy, the pacer (`SetInterval`/`SetMinGap`),
> the `BandSource`/`StepSource` contracts, or the plan-to-batch assembly.
> Coupled with: `internal/planner/CLAUDE.md` (what a `Tally` is worth, why `Clear` exists, and what
> `Plan.Tallied` means), `internal/candidates/CLAUDE.md` (why a push is a wholesale replace, and
> hints-not-ownership), `internal/piston/CLAUDE.md` (the two goroutines that drive these, and the residue
> steal whose predicate both loops must agree on), and `engine/CLAUDE.md` §"Execution Model" (the SQL behind
> the sources, where the interval is derived).

This is the supply side of dispatch: it looks at what is due on one shard, asks the planner what that shard
may serve, fetches it, and pushes it to the cache the workers drain. Workers are the demand side and live
elsewhere.

## Two loops, not one

```
Tallier    tallying                       -> planner.Tally
Supplier   planning -> fetching -> pushing -> the candidate cache
```

**The band scan costs O(due rows at the band) on every dialect and nothing in the query early-stops, so it
grows with the backlog.** A single loop would let it set the cycle's period and therefore the rate at which
candidates reach the workers: supply is `capacity / period`, so the refiller would supply *least* exactly
when the backlog was deepest. Measured on local PostgreSQL 18.1 (fan-out width 32, open-loop, with
`idx_dwarf_steps_due`), the scan was **~90% of the cycle** across every key cardinality tried:

| fairness keys | pending | period | band scan | fetch | scan share |
|---|---|---|---|---|---|
| 1 | 496,034 | 634 ms | 569 ms | 56 ms | 90% |
| 64 | 351,045 | 495 ms | 442 ms | 36 ms | 89% |
| 1024 | 304,307 | 570 ms | 507 ms | 43 ms | 89% |

`selected` per cycle pinned at exactly `capacity` (224) in all three, with 1–10% discarded: the refiller
delivered one full cache per cycle and it was consumed. So the period *was* the supply ceiling — **353
steps/s at a 634 ms period**, against **~3,340** for a Supplier alone at a 67 ms interval over a 56 ms
fetch. **That ~10x is arithmetic on the supply CEILING, not a throughput result** — it is what the ceiling
moves to, not what a workload delivers, and the two coincide only where the database can absorb the
difference. The one end-to-end measurement of the split is **+12%** (local PostgreSQL, one crippled peer
reachable only by scanning, a 0.67-2.1s band scan, the split arm carrying a *deeper* backlog than the
control); on that rig the database bound first. Quote the +12%. A fixed fetch depth and a duty-cycle
cooldown both address the same symptom and add nothing on top; neither is worth building.

**Do not read the doorbell as a second supply channel.** `Offer` is rejected when the partition has no
room, and under a deep backlog the refiller fills it to capacity every cycle; worse, a doorbelled step not
*popped* within the cycle is thrown away by the next wholesale `Refill` and returns to the backlog — which
is what `dwarf_refill_candidates_discarded` counts. It is opportunistic latency relief on the fast path,
never throughput. `capacity / period` is the supply ceiling, full stop.

**They coordinate ONLY through the planner**, which is already the asynchronous handoff and is built for
it: `Tally` retains under the mutex, `Plan` snapshots under it and computes outside. Two loops meeting there
introduce no new concurrency concept. **Do not add a second channel between them.** Anything one loop writes
and the other reads is sampled at an arbitrary phase of the other's cadence — and the phases are not
independent, since both start together and run at the same interval, so a mistake of that shape lands the
same way every time rather than half the time.

**What goes stale is the FAIRNESS TALLY, and that is the cheap thing to be stale about.** Under
a deep backlog the counts are pinned at `capacity` anyway, so they barely move; a key that drained but is
still in the tally simply comes up short for one cycle, which the assembly already handles. Strict
*priority* is different in kind — a peer-created better band is seen up to one Tallier iteration late — and
that is a deliberate concession, not a free simplification.

## The four phases name everything here

```
tallying   |  planning -> fetching -> pushing
```

Two of those names were chosen against more obvious ones, and the reasons are worth keeping.

**Not "dispatching."** In this codebase dispatch already means a worker claiming and executing a step —
the dispatch-sized resident worker set, dispatch latency, `workersDispatch`. The final phase hands a batch
to a cache; workers dispatch from it later, and possibly never. This is also why the second loop is
`Supplier` and not `DispatchLoop`.

**Not "queuing."** That implies FIFO accumulation and possession, and both are wrong. `Cache.Refill` is a
wholesale **replace** — it discards whatever the partition held, which is what the `discarded` count
reports — and cache entries are *hints, not ownership*: a pushed candidate may be gone before anyone pops
it, and a worker still has to win the claim CAS. Naming the phase after a queue would enshrine exactly the
mental model the cache's own doc warns against.

## The pacer is shared as a TYPE, never as an instance

Each loop embeds its own `pacer`. Duplicating the logic instead would invite drift, because the
self-correcting part is subtle: **the wait is computed from *elapsed time*, not from the previous cycle's
duration**, so any delay the caller introduces between calls is absorbed rather than added. A trailing
sleep cannot see that, which is why `Cycle` sleeps at the **front**:

```go
for {
    select {
    case <-ctx.Done():
        return
    default:
    }
    observe(t.Cycle(ctx))
}
```

It takes **two** timestamps, and they measure different things:

- `lastStart` anchors the **interval**, start-to-start. A cycle that took time waits only the remainder
  rather than stacking its duration on top.
- `lastEnd` anchors the **min gap**, end-to-start.

The wait is the larger of the two, so whichever constraint binds, binds. Both are zero on the first cycle,
so a starting loop looks immediately rather than paying a cadence delay at startup.

**`MinGap` is a fuse for the case the interval alone cannot cover.** When a cycle outruns its own interval
an interval-only rule computes a non-positive wait and the next cycle starts immediately — a 100% duty
cycle in the one regime the rate limit exists for. The gap makes it unrepresentable.

**A CONSTANT gap on the Supplier is not a fuse at all but the BINDING supply rate, wherever the derived
interval falls under it.** **A caller that pins this loop's gap above its interval is setting the supply
rate, whether or not it means to** — and it does so silently, since nothing here can tell a gap meant as a
fuse from one meant as a rate. So the Supplier's gap is derived from the Supplier's own period and clamped
to never exceed the constant, which keeps it a fuse at both ends: it cannot become the rate at a short
period, and it cannot throttle a shard whose period is long. Measured at the derived cadence with a flat
constant governing instead: **85% discard and +42% host CPU**, against a 17.8ms period held at the 20ms
floor. The arithmetic and the clamp live in `engine/CLAUDE.md`.

The gap is *additionally* load-bearing on mysql and sqlite, where `FetchSteps` keeps
the ranking shape: it is flat only on pgx and mssql, where the per-key cap is a `LIMIT` inside a lateral
join. mysql and sqlite kept the ranking shape (MariaDB has no lateral at any
version, and MySQL 8's `JSON_TABLE` trips a cross-collation comparison — see `internal/piston/CLAUDE.md`),
so there the fetch is still O(due rows at the band) and *will* cross the interval at depth.

Both knobs are read through their accessors **once per cycle**, never captured. A caller that re-derives
the interval (from a pool size, a fleet count, anything) takes effect on the very next cycle. Capturing
either would leave the loop on yesterday's cadence forever — silent, and invisible to any test that does
not change it mid-run. They are atomic because they are set from wherever the caller derives them while a
cycle may be mid-sleep.

## `WorkingFor` is a duration, not a bool

Each loop is driven by an owner that publishes its shard's liveness somewhere the fleet can see it, and the
ordinary evidence is a cycle COMPLETING. `WorkingFor` covers the case that evidence cannot: one scan can
outrun any sane publishing cadence on a deep backlog.

**A bool cannot serve that.** A cycle whose query fails instantly is also
briefly inside its work — building the error, recording the phase, logging it — so a "working right now"
flag reads true a small but nonzero fraction of the time (**measured ~1.2%** with an injected scan
failure). A reader sampling every 50ms catches that within seconds, and a shard that serves nothing then
looks alive indefinitely. Returning how LONG lets the reader require more than a blip, which is the only
version of the question that distinguishes a slow scan from a broken one.

**It spans the work and not the pace.** The pace is most of a healthy cycle's wall clock, so including it
would make any predicate over this permanently true.

## Neither `Cycle` returns an error — this is a hazard defence, not a style choice

Every failure a `Cycle` can hit is already handled inside it, so the caller has no decision to make. A
returned `error` would say the opposite, and the reflex it invites is:

```go
r, err := t.Cycle(ctx)
if err != nil {
    return          // <- removes this shard from the fleet, permanently
}
```

That shard stops tallying forever. It has already cleared itself, so it is excluded from planning and
never comes back — a fleet-degrading bug written by someone following Go convention correctly. `Err` on
the result makes the natural caller `observe(t.Cycle(ctx))` and routes the failure to the log line where it
belongs.

The error returns that *do* exist are on `NewTallier`/`NewSupplier`: a wiring mistake is a construction
concern, which is precisely why `Cycle` has none to spend on one.

## The error policy, and its asymmetry

This is the part most likely to be "simplified" into a bug. The split put the two halves in different
types, so neither can be read without the other.

| Outcome | Owner | Planner | Cache |
|---|---|---|---|
| Scan succeeded | Tallier | `Tally` | untouched (not its cache) |
| **Scan failed** | Tallier | **`Clear`** | untouched (not its cache) |
| **Shard untallied (`!Plan.Tallied`)** | Supplier | untouched | **untouched** |
| Plan gave slots | Supplier | (already tallied) | `Refill(batch, band)` |
| Plan gave no slots | Supplier | (already tallied) | **`Refill(nil, NoBand)`** — cleared |
| **Fetch failed** | Supplier | **untouched** | **untouched** |

**A failed scan clears the shard but spares the cache — and after the split those are two different
loops.** Clearing is necessary because a stale claim on the best band makes every peer find none of its own
keys there and dispatch nothing. Sparing the cache is necessary because the failure means *unknown*, not
"nothing is due" — wholesale-replacing a healthy partition with nothing because the database blipped would
idle this shard's workers for a cycle, throwing away the last good information anyone had.

**`Plan.Tallied` carries the second half across the seam, and without it the Supplier clears a healthy
partition.** The Supplier turns at its own cadence while a scan is failing, so *something* has to
tell it the shard is absent from the planner rather than merely empty-handed. `Tallied=false` is the only outcome
that means UNKNOWN; all four ways `Slots` can be empty are positive statements made by a shard that
reported. Pinned by `TestSupplier_UntalliedShardSparesTheCache`, which turns the Supplier three times
against a failing scan.

**A failed fetch clears nothing.** The tally already succeeded and is *still true*: the shard looked, saw
its band, and reported it honestly. Clearing would drop a valid band claim from the global minimum and let
peers serve worse work for no reason. It simply pushes nothing this cycle.

**An empty plan from a TALLIED shard is not a failure, and is the one case that DOES empty the partition.**
It is a positive statement — nothing here is dispatchable — so every cached candidate is a dead hint a
worker would pop and burn a claim round-trip on. This is the distinction the cache's doc calls its sharp
edge; here it is an invariant with a name.

Being outranked (this shard's band worse than `GlobalBand`, a peer holds a better one) travels this same
empty-plan path and returns `Err == nil`. It is ordinary strict priority, never a fault — and it costs no
fetch at all.

**`Reconciled` is not `Err == nil`, and a caller waiting on the partition must use it.** Two paths hold
everything and only one of them is an error, so a waiter gated on the error would be told the partition
reflects the plan when it does not. `internal/piston` fires `CheckpointCycleDone` on `Reconciled` for
exactly this reason.

## One context, and why there is no second stop signal

Each `Cycle` takes a single `ctx`, sleeps on `ctx.Done()`, and the caller cancels it to stop. The engine's
other loops need two signals — a stop channel plus a longer-lived context — so that in-flight database work
commits during drain. **Both queries here are read-only**, so there is no write to commit and no partial
state to strand; abandoning them mid-flight is free. That is what buys prompt shutdown here with half the
ceremony.

## What stays outside

**The SQL.** The two source methods are implemented by the caller because they are dialect-specific, bound
to the step schema, and carry the replica partition predicate — peer-discovery state this module has no
business knowing.

**`BandSource` and `StepSource` are the only interfaces here, and the rule is I/O — not "collaborator."**
They are abstracted so each loop tests with no database, and they are TWO narrow interfaces rather than one
fat one each loop half-uses, so neither loop can reach the other's query. The planner and the cache are
in-memory, cheap to construct, and taken as concrete types on purpose: faking them buys no testability and
costs correctness, because a fake can drift from the real semantics. That risk is not hypothetical for the
cache — `Refill`'s wholesale-replace is precisely what the error policy above turns on, and a fake that
appended instead would let "empty plan clears, error does not" pass while broken. Do not add an interface
for either.

**Where the interval comes from.** This module applies a cadence; it does not derive one.

**The goroutines, and metrics emission.** Each result is returned, not observed, so no telemetry API
reaches in here and the caller keeps owning its instrument names. `Total` excludes `Slept` — it is the
cycle's cost, not its period; the period is `Slept + Total`.

## Contracts a source must hold

**`ScanBand` returns O(distinct keys), never O(backlog)** — one tally per fairness key, with a `Count`,
capped at the planning capacity. A source that returns per-step defeats the entire three-phase shape.
(Note the cost is a separate question: the scan *returns* O(keys) while it *costs* O(due rows at the band),
which is the whole reason for the split above.)

**`FetchSteps` returns each key's steps OLDEST FIRST.** The assembly consumes the lists in order and never
re-sorts them, so the ordering is the source's to get right. A key may come back short or missing.

**A source whose two methods must agree owns that agreement itself.** The Supplier fetches against a plan
built from the Tallier's scan, and the two calls happen on different goroutines at different times — so a
source whose fetch predicate can disagree with its scan predicate must make them agree by construction, not
by re-reading shared state at each call. `internal/piston`'s residue-class steal is the live instance, and
the failure it costs is recorded there.

## Assembly preserves the interleave

The batch is built by walking `Plan.Slots` **in order** and taking each key's next fetched step — not by
grouping per key. Grouping would hand one tenant a contiguous run and undo the fairness interleave the
lottery produced. A key that comes up short (its steps claimed between the fetch and now) simply
contributes fewer candidates; the batch runs short for one cycle and the next re-selects. There is nothing
to reconcile.

## Concurrency

**A `Cycle` is not safe for concurrent use.** One `Tallier` and one `Supplier` per shard, each driven by
one goroutine, is the whole intended shape, and each pacer's timestamps are unsynchronized on that basis.
The cadence setters are the deliberate exception and may be called from anywhere. Do not add shared state
between the two loops — the planner and the cache are both already concurrency-safe and are where
cross-shard state belongs.

**Planning sits on the hot loop, and there is a threshold to watch.** `Plan()` runs once per Supplier
cycle — an order of magnitude more often than a band scan at the reference cadence — and it is `O(capacity x keys)`, the one cost `internal/planner/CLAUDE.md` says scales with
fairness-key cardinality. At the measured **2.1 ms for 1024 keys** that is ~3% of a 67 ms cycle; ~10k keys
would be ~20 ms, i.e. 30% of the loop, which is when the Fenwick tree that doc holds in reserve stops being
theoretical. Read `dwarf_refill_query_duration_seconds{phase="planning"}` before assuming it is still free.
