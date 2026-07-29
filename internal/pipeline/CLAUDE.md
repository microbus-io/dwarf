# Dwarf `internal/pipeline` — one shard's supply cycle

> Load when: changing `Cycle`'s phases or its error policy, the cadence (`SetInterval`/`SetMinGap`), the
> `Source` contract, or the plan-to-batch assembly.
> Coupled with: `internal/planner/CLAUDE.md` (what a `Tally` is worth and why `Clear` exists),
> `internal/candidates/CLAUDE.md` (why a push is a wholesale replace, and hints-not-ownership), and
> `engine/CLAUDE.md` §"Execution Model" (the SQL behind `Source`, where the interval is derived, and the
> goroutine that drives this).

This is the supply side of dispatch: it looks at what is due on one shard, asks the planner what that
shard may serve, fetches it, and pushes it to the cache the workers drain. Workers are the demand side and
live elsewhere.

## `WorkingFor` is a duration, not a bool

The pipeline is driven by an owner that publishes its shard's liveness somewhere the fleet can see it, and
the ordinary evidence is a cycle COMPLETING. `WorkingFor` covers the case that evidence cannot: one scan can
outrun any sane publishing cadence on a deep backlog, since phase one is O(backlog) on every dialect without
the run-condition early-stop.

**A bool cannot serve that, and shipping one was a bug.** A cycle whose scan fails instantly is also briefly
inside its queries — building the error, recording the phase, logging it — so a "working right now" flag
reads true a small but nonzero fraction of the time (**measured ~1.2%** with an injected scan failure). A
reader sampling every 50ms catches that within seconds, and a shard that serves nothing then looks alive
indefinitely. Returning how LONG lets the reader require more than a blip, which is the only version of the
question that distinguishes a slow scan from a broken one.

**It spans the queries and not the pace.** The pace is most of a healthy cycle's wall clock, so including it
would make any predicate over this permanently true.

## The five phases name everything here

```
sleep -> tallying -> planning -> fetching -> pushing
```

Two of those names were chosen against more obvious ones, and the reasons are worth keeping.

**Not "dispatching."** In this codebase dispatch already means a worker claiming and executing a step —
the dispatch-sized resident worker set, dispatch latency, `workersDispatch`. The final phase hands a batch
to a cache; workers dispatch from it later, and possibly never.

**Not "queuing."** That implies FIFO accumulation and possession, and both are wrong. `Cache.Refill` is a
wholesale **replace** — it discards whatever the partition held, which is what the `discarded` count
reports — and cache entries are *hints, not ownership*: a pushed candidate may be gone before anyone pops
it, and a worker still has to win the claim CAS. Naming the phase after a queue would enshrine exactly the
mental model the cache's own doc warns against.

## No `Run`, no goroutine — `Cycle` paces itself

`Cycle` sleeps at the **front**, for whatever the cadence still owes, so the caller's loop holds no policy:

```go
for {
    select {
    case <-ctx.Done():
        return
    default:
    }
    observe(p.Cycle(ctx))
}
```

**Front-loading is what makes the pacing self-correcting.** The wait is computed from *elapsed time*, not
from the previous cycle's duration, so any delay the caller introduces between calls is absorbed rather
than added. A trailing sleep cannot see that.

It takes **two** timestamps, and they measure different things:

- `lastTallyStart` anchors the **interval**, start-to-start. A cycle that took time waits only the
  remainder rather than stacking its duration on top.
- `lastCycleEnd` anchors the **min gap**, end-to-start.

The wait is the larger of the two, so whichever constraint binds, binds. Both are zero on the first cycle,
so a starting shard looks immediately rather than paying a cadence delay at startup.

**`MinGap` is a fuse for the case the interval alone cannot cover.** When a cycle outruns its own interval
— a deep backlog makes the scan expensive, which is exactly when scanning is dearest — an interval-only
rule computes a non-positive wait and the next cycle starts immediately. That is a 100% duty cycle in the
one regime the rate limit exists for. The gap makes it unrepresentable.

Both are read through their accessors **once per cycle**, never captured. A caller that re-derives the
interval (from a pool size, a fleet count, anything) takes effect on the very next cycle. Capturing either
would leave the shard on yesterday's cadence forever — silent, and invisible to any test that does not
change it mid-run. They are atomic because they are set from wherever the caller derives them while a
cycle may be mid-sleep.

## `Cycle` never returns an error — this is a hazard defence, not a style choice

Every failure `Cycle` can hit is already handled inside it, so the caller has no decision to make. A
returned `error` would say the opposite, and the reflex it invites is:

```go
r, err := p.Cycle(ctx)
if err != nil {
    return          // <- removes this shard from the fleet, permanently
}
```

That shard stops tallying forever. It has already cleared itself, so it is excluded from planning and
never comes back — a fleet-degrading bug written by someone following Go convention correctly. `Result.Err`
makes the natural caller `observe(p.Cycle(ctx))` and routes the failure to the log line where it belongs.

The error return that *does* exist is on `New`: a wiring mistake is a construction concern, which is
precisely why `Cycle` has none to spend on one.

## The error policy, and its asymmetry

This is the part most likely to be "simplified" into a bug.

| Outcome | Planner | Cache |
|---|---|---|
| Scan succeeded | `Tally` | per the plan, below |
| **Scan failed** | **`Clear`** | **untouched** |
| Plan gave slots | (already tallied) | `Refill(batch, band)` |
| Plan gave no slots | (already tallied) | **`Refill(nil, NoBand)`** — cleared |
| **Fetch failed** | **untouched** | **untouched** |

**A failed scan clears the shard but spares the cache.** Clearing is necessary because a stale claim on
the best band makes every peer find none of its own keys there and dispatch nothing. Sparing the cache is
necessary because the failure means *unknown*, not "nothing is due" — wholesale-replacing a healthy
partition with nothing because the database blipped would idle every worker for a cycle.

**A failed fetch clears nothing.** The tally already succeeded and is *still true*: the shard looked, saw
its band, and reported it honestly. Clearing would drop a valid band claim from the global minimum and let
peers serve worse work for no reason. It simply pushes nothing this cycle.

**An empty plan is not a failure, and is the one case that DOES empty the partition.** It is a positive
statement — nothing here is dispatchable — so every cached candidate is a dead hint a worker would pop and
burn a claim round-trip on. This is the distinction the cache's doc calls its sharp edge; here it is an
invariant with a name.

Being outranked (`Band > GlobalBand`, a peer holds a better band) travels this same empty-plan path and
returns `Err == nil`. It is ordinary strict priority, never a fault — and it costs no fetch at all.

## One context, and why there is no second stop signal

`Cycle` takes a single `ctx`, sleeps on `ctx.Done()`, and the caller cancels it to stop. The engine's other
loops need two signals — a stop channel plus a longer-lived context — so that in-flight database work
commits during drain. **Both of this cycle's queries are read-only**, so there is no write to commit and no
partial state to strand; abandoning them mid-flight is free. That is what buys prompt shutdown here with
half the ceremony.

## What stays outside

**The SQL.** `Source`'s two methods are implemented by the caller because they are dialect-specific, bound
to the step schema, and carry the replica partition predicate — peer-discovery state this module has no
business knowing.

**`Source` is the only interface here, and the rule is I/O — not "collaborator."** It is abstracted so the
whole cycle tests with no database. The planner and the cache are in-memory, cheap to construct, and taken
as concrete types on purpose: faking them buys no testability and costs correctness, because a fake can
drift from the real semantics. That risk is not hypothetical for the cache — `Refill`'s wholesale-replace
is precisely what the error policy above turns on, and a fake that appended instead would let "empty plan
clears, error does not" pass while broken. Do not add an interface for either.

**Where the interval comes from.** This module applies a cadence; it does not derive one.

**The goroutine, and metrics emission.** `Result` is returned, not observed, so no telemetry API reaches in
here and the caller keeps owning its instrument names. `Total` spans tallying through pushing and
**excludes** `Slept` — it is the cycle's cost, not its period; the period is `Slept + Total`.

## Contracts the `Source` must hold

**`ScanBand` returns O(distinct keys), never O(backlog)** — one tally per fairness key, with a `Count`,
capped at the planning capacity. A source that returns per-step defeats the entire three-phase shape.

**`FetchSteps` returns each key's steps OLDEST FIRST.** The assembly consumes the lists in order and never
re-sorts them, so the ordering is the source's to get right. A key may come back short or missing.

## Assembly preserves the interleave

The batch is built by walking `Plan.Slots` **in order** and taking each key's next fetched step — not by
grouping per key. Grouping would hand one tenant a contiguous run and undo the fairness interleave the
lottery produced. A key that comes up short (its steps claimed between the fetch and now) simply
contributes fewer candidates; the batch runs short for one cycle and the next re-selects. There is nothing
to reconcile.

## Concurrency

**`Cycle` is not safe for concurrent use.** One `Pipeline` per shard, driven by one goroutine, is the whole
intended shape, and the cadence timestamps are unsynchronized on that basis. `SetInterval` and `SetMinGap`
are the deliberate exceptions and may be called from anywhere. Do not add shared state that assumes
otherwise — the planner and the cache are both already concurrency-safe and are where cross-shard state
belongs.
