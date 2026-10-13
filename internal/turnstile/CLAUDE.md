# Dwarf `internal/turnstile` — a priority-and-age ordered admission gate

> Load when: changing the ordering, the bands, how a claim is stamped, the handoff, the give-up path, or
> what `Close` promises.
> Coupled with: `engine/CLAUDE.md` §"Turn-taking on the database" (what this gates, the band assignment,
> and why the turn taken before a candidate is picked up is also the worker crew's growth brake).

A bounded counter, a priority queue, and a direct handoff. What a pass *means* — what it gates, how many
exist, which band each caller sits in — is the engine's, deliberately. What lives here is the ordering, and
the properties that make it safe to block on.

## The whole point is WHERE the waiting happens, and that fixes the sizing

A connection pool has a queue of its own, and that queue has no ordering: it hands connections out in
whatever order the runtime wakes waiters. `engine/CLAUDE.md` records the consequence — the band scan's
apparent "fixed ~46ms floor" is *pool wait*, not query work, and no query-shaped change ever moved it.

The turnstile does not remove that wait, and it does not empty that queue. **A caller holding a pass still
competes for the resource, and still waits when every unit of it is busy.** What changes is *who gets to
compete, and in what order*: the population contending for the resource is bounded by the pass count, and
admission into it is by band and then by age. Pool wait at saturation is therefore the expected steady
state, not a sign the mechanism is failing.

**DO NOT SIZE THE PASSES TO THE RESOURCE.** One pass per unit is the intuitive reading of "bound the
population" and it is measured catastrophic — the engine's arm collapsed **6x** on local Postgres: 281
steps/s, against 1,687 for a gate sized above the pool on the same rig. With as many passes as units there are exactly as many candidates for one, so
every gap between a holder finishing and the next waiter being woken, scheduled and asking for the resource
is idle resource time. **A queue at the resource is not waste; it is what keeps the resource busy.** The
engine sizes at 2 passes per connection; the turn count is also what a saturated worker crew grows to.

What the count still buys is that the contending population is BOUNDED AT ALL rather than growing with the
caller pool, and that admission into it is ordered. Both survive a generous multiple; neither survives no
bound at all.

That is also why a double `Return` is a no-op rather than being left to the caller to avoid. An inflated
bound is not merely a bound that is too large: it is the ordering quietly switching itself off, since the
surplus goes back to queueing where nothing is ordered. The entry is already allocated, so the flag costs
nothing. **The leak direction is
still unguarded and still fatal** — admission decays to nothing — so callers must return unconditionally on
the way out; the no-op cases exist to make that safe.

## A pass must ENCLOSE the resource, or the two deadlock

Never acquire a pass while already holding a connection. With `P` passes and `M == P` connections:

- `M` callers hold passes and are blocked waiting for a connection.
- The `M` connections are held by callers who released their pass between statements and now want another.

Neither side can move. This is not hypothetical for anything transactional: a transaction holds one
connection across many statements, so **one pass wraps the whole transaction**, never one per statement
inside it. The rule that makes it checkable: a pass is taken before the resource and returned after it,
never the other way round.

## Handoff, not a condition variable — and that IS the ordering

A plain semaphore wakes a waiter with `Signal` and has it re-check a count. That is correct there and
**wrong here**: between the wake and the re-check, a caller arriving fresh can take the
count, so the queue's decision is advisory and the caller with the best claim loses at random. The whole
design would be a suggestion.

So a release **transfers** the pass: the queue is popped under the lock, the winner is marked granted, and
its channel is closed. The waiter that wakes already holds the pass; nothing it can lose a race to remains.
The fast path (`avail > 0` with an empty queue) is the same rule stated for the empty case — the queue is
non-empty only while `avail <= 0`, since a grant always drains what it can, so taking a free pass directly
cannot barge past anyone.

`granted` is read *without* the mutex after receiving on the channel. That is sound — the write precedes the
close, and the receive follows it — and it matters, because that lock is the one every caller contends for.

## The claim rides the CONTEXT, and the composition is left at the call site

Nothing here wraps `sequel.DB`. A wrapper was designed and dropped, and the reason is a fact about the
driver rather than a matter of taste: **a query returns while still holding its connection.**
`sequel.Rows` embeds `*sql.Rows` and `sequel.Row` embeds `*sql.Row`, so the connection goes back to the
pool at `Close`, or at the `Scan` of a `QueryRow` — both of which happen in the *caller*, after the
wrapped call has returned. A wrapper therefore cannot hold the pass for as long as the connection is
held, which is the one invariant this must not break. `Transact` is the only one of the three that a
wrapper could have got right, because its closure ends where the connection is released.

So the call site composes, and holds its pass until it has finished reading. What the context carries is
the claim, so it does not have to be threaded through every signature to get there.

**The context key is private and there is no exported setter for a claim.** The ordering trusts `Since`
completely — a caller able to write its own would be choosing its own place in line, and the oldest claim
wins everything.

**`ContextWithPriority` is a `Set` method rather than a package function, and that is not a style
preference.** A package-level `ContextWithPriority(ctx, shard, priority)` needs a package-level registry
to resolve the shard, which makes it process-global — and dwarf runs several engines in one process
(`fixtures/crossreplicaawait_test.go`), which would then share one set of turnstiles across engines that
share nothing else. `WaitTurn(ctx)` *is* package-level, because it needs no registry: the context already
names the turnstile, and that is the function called deep in the call chain where threading a set would be
the whole problem.

## The `Gate` is not about the database at all — it is the caller's growth brake

`Gate.Acquire` takes a job's first turn *before* the job is picked up, and both halves of that ordering are
load-bearing for a reason that has nothing to do with bounding anything:

- **A caller blocked here is holding no work**, so a pool that sizes itself by watching whether its
  goroutines are busy reads it as available and declines to grow. Blocking IS the signal; nothing has to
  ask this package how full it is. Reverse the order and a goroutine that popped first blocks while
  *holding* work, reads as busy, and every take spawns another — measured against a gate that does not
  wait: the crew ran to **398 goroutines where 64 sufficed**. Pinned by
  `TestCrew_SaturationDoesNotGrowThePool` and `TestPoolSizing_SaturationDoesNotGrowThePool`.
- **Work not yet taken is still visible to every other goroutine.** Taking it first and blocking afterwards
  strands it inside one that cannot proceed with it.

The turn it takes is the job's **first**, not a separate reservation: it covers that job's first call and
is handed straight back, and the context it returns carries the claim the job's later turns are taken on. Holding
it longer would meter *phases* rather than calls, which makes the count mean something different again — a
reason to hand it back early, not a reason to size the count differently (the sizing argument is above and
is independent of this).

`Acquire` is also the one place where a closed set means **stop** rather than **proceed unordered** — a
caller whose whole purpose is to shut down on a drain. It therefore asks `Set.Closed()` on both sides of
the wait rather than reading the pass, since a pass cannot express the difference.

## Re-pointing a job at another shard keeps its age

`ContextWithPriority` stamps `Since` and `Seq` only if the context does not already carry them. A
cross-shard operation re-points one job at each shard in turn, and re-stamping there would make a job that
has been running for a while look **newly arrived at every hop** — so it would keep losing to work that
had only just started, which is the exact inversion the age ordering exists to prevent. Pinned by
`TestSet_ContextKeepsTheJobsAgeAcrossShards`.

Re-pointing preserves, always. Moving the age is a separate call, `Away`, for the one case where part of
the age a job carries is time it spent somewhere else (next section).

## Time away from the resource does not age a claim (`Away`)

A job that leaves to wait on something else - the engine's host call - comes back with its stamp moved
forward by the time it was gone. It then ranks by the time it has spent AT the resource: ahead of callers
that arrived after its own work began, behind callers that have waited longer than it has worked. Pinned by
`TestAway_CountsOnlyTimeAtTheResource`, which fails against both alternatives below.

**Not moving it turns age into a strict band.** A job still carrying the stamp it began with after a long
call elsewhere is older than every caller that arrived while it was away. Once those calls outlast the
queue that is *every* caller, so it wins every turn - and a band SERIALISES A PIPELINE: the returning callers
take every pass while the arriving ones queue, the arriving ones rush in once that queue drains, and the two
stages alternate instead of overlapping. Measured on the engine, where the returning caller is a step
recording its outcome: traced at 50 ms on local Postgres (one shard, pool 8, 1 s tasks, 10-step linear flows
at 60 flows/s), ~1 s exit bursts held every turn while ~1,300 callers queued and nothing was dispatched,
~0.5 s entry bursts then started ~1,250 tasks at once, then ~0.4 s passed with turns free and the pool idle,
the whole crew inside tasks of one length - which kept the bursts together into the next round. Flow p50
ran 11-25 s for 10 s of tasks, and `Create` p99 1.2-2.7 s. On a 32 vCPU shard at 1,000 flows/s the same
ordering cost `Create` a p99 of 485 ms at 1 s tasks and 2.9 s at 8 s, lost 11-27% of admissions, and added
~0.35 s / ~2 s per hand-off.

**Re-stamping it fresh fails the other way.** A fresh stamp is younger than every caller already queued,
so a finished step waits out the whole queue behind work that has not started - under saturation the
engine keeps starting tasks while the ones that finished wait to be recorded. Measured saturated on a
pool of 16, exit wait against entry wait: fresh 334 against 345 ms (1 s tasks) and 8.3 against 8.4 ms (0 s
tasks) - no preference left.

**The moved stamp finishes work SOONER than the unmoved one, not later.** Saturated with 1 s tasks on a pool
of 16, two interleaved pairs at matched RTT (0.47-0.56 ms): moved 2,298 / 2,303 steps/s against 1,501 /
1,642 (+45%), exit wait 88 / 84 ms against 194 / 91 ms, entry wait 230 / 235 ms against 347 / 260 ms, and
flow p50 13.9 s against 19.5-21.2 s. The pool sat at 4 or fewer of 16 connections in 3-4% of 50 ms samples
against 24-29%. A demoted exit does not linger: it is the unmoved stamp's bursts that make exits queue behind
each other. With 0 s tasks the moved stamp keeps the unmoved one's preference (exit 0.3-0.5 ms against entry
13-22 ms, unmoved 0.8 against 34.5 ms).

**Nor give returning callers a band.** That is strict precedence chosen on purpose: completions given
precedence collapsed short-task throughput 3x (4,416 against 7,964 steps/s), and an exit band measured
2,837 against 2,825 steps/s on a mixed 8-60 s workload - nothing. Nor leave them UNORDERED: served with no
ordering at all, completions lost at random and 286 of them waited out a full second.

The preference this leaves is self-scaling and needs no knob: a returning job is credited with the time it
spent queueing and working before it left, which under saturation is about one queue's worth - near the
front, but never ahead of a caller that has waited longer than that.

## Fail OPEN, unlike a gate whose bound is the point

An unstamped context, a shard with no turnstile, an expired ctx and a closed set all let the caller
straight through with a zero pass. **Do not "harden" this into failing closed.** A gate that IS the bound
must fail closed, because unbounded is the collapse it exists to prevent. This one only ORDERS access to
something already bounded, so the cost of it being unreachable is that its callers go unordered — while
failing closed would wedge a resource over a sizing mistake, and would make converting call sites one at a
time impossible.

The consequence to keep in view: a sizing mistake is **silent**, visible only as ordering that is not
happening. Pinned by `TestWaitTurn_PassesThroughWhenUnordered`.

## `Claim.Seq` is for the clock, not for fairness

It orders two jobs a clock could not separate. With nanosecond resolution that is nearly never; with a
coarse one — Windows sits around a millisecond, which SQL Server deployments make reachable — many jobs
share a timestamp, and without the tiebreak they interleave their turns instead of one finishing and
leaving. It is assigned **once per job** for that reason, and a per-call number would be useless: it
would re-randomise a job's position on every turn it took.

**It is a counter, not a random value.** When the clock cannot resolve two arrivals, the counter still
can, so ties fall back to true arrival order rather than to a coin flip.

It wraps at a million. Wrapping only ever misorders two jobs that share a *timestamp* AND sit a million
apart in creation order, which no clock coarse enough to collide can produce within one of its own ticks.
A saturating counter would be the wrong choice instead of merely a bounded one: every job past the
ceiling would compare equal, permanently.

## Ordering: band, then age, then job, then arrival — and the last two are not decoration

Lower band first, then earlier `Since`, then lower `Seq`, then arrival. The last term catches two
*identical* claims — the same job taking two turns at once — which the heap would otherwise order
arbitrarily, making the order untestable and letting one of them be passed over repeatedly. Pinned by
`TestTurnstile_EqualClaimsServedInArrivalOrder` and `TestSet_JobSeqOrdersClaimsTheClockCannotSeparate`,
each of which fails with its own term removed.

**`since` is the age of the JOB at the resource, not of the call, and that is what replaces the permits
split.** A gate with no age ordering needs two separate reservations, because a caller part-way through its
work must not queue behind a flood of callers just starting theirs, and both single-pool answers were
measured failing (served at random, 286 completions waited a full second; completions given precedence,
short-task throughput collapsed 3x to 4,416 from 7,964 steps/s). Ordering by age gets the guarantee without a
rule: a caller coming back for its next turn keeps its place, so it outranks everything that arrived after
its work began, and nothing can starve, because every claim eventually becomes the oldest.

**That property dies the moment a band is used for anything that can arrive faster than it is served.**
Bands are strict: a band is exhausted before the next is looked at, so a flooding band starves everything
below it — which is the collapse quoted above, reached through a different door. A band is for a source
that is *bounded by construction* (a paced, cadenced one), or for one whose precedence has been measured
to cost less than it buys. Everything left over shares the last band and is separated by age alone.

The engine numbers its bands with gaps (0 refiller, 10 everything else) so one can be inserted between two
already in use without renumbering either — the values are ordinals, and nothing reads the distance between
them. The gap is not an invitation: see the section above for the band that keeps being proposed for it.

## A pass must enclose the resource — and NOTHING ELSE

The mirror of the enclosure rule, and the one that is easy to get wrong in the other direction: a pass
handed out for the duration of a *phase* ends up spanning waits that are not the resource at all. The
engine hit all three shapes — a host call, a retry backoff, and a mutex — and each parks a pass on
something whose duration the resource cannot bound. The mutex case is the sharpest, because that lock's own
reason for existing was that its loser waits holding no connection; a pass held across it re-creates the
occupancy one level up.

So a caller holding a pass across such a wait must hand it back and take a fresh one after (the engine's
`yieldTurn`). The cost is one re-queue; the alternative is a bound that measures resource occupancy while
actually metering sleep.

## Giving up must remove the caller from the QUEUE, not just from the wait

Two halves, and the second is the one that bites. Leaving an abandoned caller in the heap means the next
pass is handed to somebody who is no longer there to take it, and every waiter behind it stalls on a claim
that can never be satisfied. Pinned by
`TestTurnstile_ContextDeadlineReleasesTheCallerAndItsPlace`, which hangs to the timeout without the removal.

**There are TWO ways a waiter can leave the queue while it is on its way to the lock, and both must be
handled — one was a process-killing panic.** `Close` pops every entry, which sets its heap index to -1 and
leaves it UNGRANTED; a waiter whose ctx expired in the same instant then takes the give-up branch, sees no
grant, and removes itself from a heap it is no longer in — `heap.Remove(-1)` indexes the slice at -1 and
takes the process down, on the caller's goroutine, which nothing wraps. It is reachable in production:
shutdown closes the set while operations with request deadlines are in flight. The index test is what
separates the cases — a grant pops *and* sets `granted`, so an index below zero without a grant means
exactly "Close got here first". Pinned by `TestTurnstile_CloseRacingAnExpiredContextDoesNotPanic`, which
forces the interleaving rather than racing for it.

The other window: the pass can be granted between `ctx.Done()` firing and the caller taking the lock. The caller then holds a pass **nobody else knows about**, so walking away retires it — the
ceiling shrinks by one, silently, permanently, and again on every recurrence. It must be passed on instead.
`TestTurnstile_AbandonedGrantIsNotLost` **forces** that interleaving with the turnstile's own lock rather
than racing for it. Do not make it stochastic: a racing version of that test passes against an
implementation that drops the pass outright.

## `Resize` moves `avail` by the DELTA, never assigns it

Assigning the new ceiling re-issues passes in-flight holders are still using. A shrink below what is held drives the count negative, which admits nobody until
enough return, and needs no special case. A grow hands its new passes straight to the head of the queue —
raising the count alone would leave the waiters it was meant for asleep on passes that are already free.
Pinned by `TestTurnstile_ResizeMovesByDelta` and `TestTurnstile_ResizeGrowAdmitsWaitersInOrder`.

**A closed set stays closed**, so `Set.Resize` on one is a no-op rather than a fresh turnstile. Without
that, an owner whose pool recompute races its own shutdown mints an OPEN turnstile inside a closed set, and
callers park in something nothing will ever close — a drain that hangs on a shard nobody is using.

## `Close` is a latch, and it is the only stop signal

It permanently releases every waiter and `WaitTurn` reports `!ok` forever after; there is no reopen. A
caller parked here is reachable by nothing else, so a drain that closes only the other things callers park
on hangs. `!ok` is reported for a close *and* a ctx expiry, told apart by the caller's own `ctx.Err()`
rather than by a second return value — the caller already holds the context that answers it.

## One instance per resource, which also buys per-resource locks

One counter with a map keyed by shard needs per-shard condition variables to avoid lost wakeups, and
funnels every shard's hot path through one mutex. A turnstile is one object per resource instead, so the
separation is structural: no key, no shared lock, and no way for a release on one
to wake a waiter on another. A single instance spanning several resources would also be wrong on its own
terms — a caller queued for a busy resource would hold up one bound for an idle one.

## Cost

The hot path is a mutex, one small allocation, and — only if the caller queues — a heap operation: measured
**28ns/op, 1 B/op** with no contention and **~285ns/op** with every caller queueing across 10 cores
(`BenchmarkTurnstile_Uncontended` / `_Contended`, Apple M1 Pro). This sits under every acquisition of the
resource it gates, several per step for a database phase, so re-measure rather than assuming a change here
is free. Two things that were not obvious and are worth not undoing:

- **The clock dominated.** Timing the wait from function entry meant two `time.Now()` calls on a path that
  discards the result, which was **nearly half** the uncontended cost (65ns → 36ns to move it past the
  fast path). A caller that never queues waited zero by definition, so there is nothing to time. Note the
  same trap upstream: `ContextWithPriority` reads the clock too, so it belongs once per job and not per
  call — which is where the ordering wants it anyway.
- **The pass token is split out of the queue entry** so a caller that never queues does not allocate a
  whole place in line: 64 B/op → 1 B/op, and 73ns → 65ns before the clock change.

## `TryWaitTurn` takes a pass only where `WaitTurn` would without queueing

It issues a pass only when one is free AND nobody is queued - the fast path's exact condition - so it can
never barge past a waiter, which is the whole ordering. It presents no claim because it never joins the
queue. Its one caller is `Gate.TryAcquire`, which lets the engine's crew move a worker to another shard with
work instead of queueing it behind a shard whose turns are all taken (`engine/CLAUDE.md` §"Turn-taking on
the database").

## Deliberately absent

- **No aging, weighting or anti-starvation term.** The age ordering already makes the common band
  starvation-free, and a band that starves is a band that was given to the wrong caller. A control loop
  here would have to clear the bar every control loop in this engine has had to.
- **No metrics.** The free-pass count is an instant, so a turnstile saturated all window without being
  sampled empty reads as idle; the durable measure is `Pass.Waited`, reported per acquisition for the
  caller to record.
