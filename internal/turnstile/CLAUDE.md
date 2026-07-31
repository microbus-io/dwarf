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
population" and it is measured catastrophic — the engine's arm collapsed **6x** (281 steps/s against 1,687)
against the gate it replaced. With as many passes as units there are exactly as many candidates for one, so
every gap between a holder finishing and the next waiter being woken, scheduled and asking for the resource
is idle resource time. **A queue at the resource is not waste; it is what keeps the resource busy.** The
engine sizes at 8 passes per connection, matching the margin it already keeps for its worker set.

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
is handed straight back, and the context it returns carries the claim every later turn is taken on. Holding
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

Preserving is the ONLY behaviour, and there is deliberately no way to force a fresh stamp. The argument for
one - a caller whose work has "restarted" rather than continued, so the age it carries describes a wait
that is over - applies to exactly one caller, the engine's exit side, and the next section is why that
caller does not get it. Adding the API back means adopting that case with it.

## DO NOT give exiting callers their own band, and do not re-stamp their claims

Both keep being proposed, together, and the case for them is genuine — which is why the reasoning against
is written out rather than left as a preference.

**The problem they would fix is real.** A claim is stamped when the caller picks its work up, so age at
exit is a proxy for how long the caller's WORK took, which says nothing about the few round trips it now
needs to record it. On a mixed-duration workload a caller leaving a 60s task carries a 60s-old claim and
beats one leaving an 8s task that exited before it, systematically. Re-stamping at exit would order exits
by when they exited; a band would keep them ahead of entering callers, which re-stamping alone destroys.
Neither half works without the other: a fresh claim is the YOUNGEST in the system, so re-stamping without a
band puts exits BEHIND entries — the "served evenly" arrangement measured at 286 completions waiting out a
full second.

**They are not worth it, for three reasons in increasing order of force:**

1. **The harm they fix is bounded and rare.** A long exit jumping a short one costs the short one one
   persist - a few round trips - not the length of the long task. And long tasks produce exits in inverse
   proportion to their length: an 8s caller exits every 8s, a 60s caller every 60s, so the queue-jumpers
   are the rarer population.
2. **It buys nothing measurable.** On the mixed workload built for it (durations spread 8-60s, 600 flows/s,
   16-vCPU shard), the exit band measured **2,837 steps/s against the control's 2,825** - 0.4%, inside the
   noise of the rig it ran on.
3. **A band is strictly ordered, so it SERIALISES A PIPELINE.** Exits block entries; the exit queue drains;
   entries rush in; those become exits and refill it. The two stages alternate instead of overlapping, and a
   pipeline needs both live at once to keep the resource busy. This is the same failure as "completions
   given precedence", which collapsed short-task throughput 3x (4,416 against 7,964 steps/s) - and it is
   worst exactly where the band looks safest to add, because the alternation is fastest when tasks are
   short and exits arrive as fast as entries.

**What the pickup stamp gives instead is a GRADUATED preference, and that is the property to protect.** An
exiting caller wins by however long its task ran, so the advantage is proportional to the work it
represents and never becomes a rule: measured at 12.7x on wait under 8s tasks (159.1 ms entry against
12.6 ms exit), decaying toward parity as tasks get shorter - which is precisely where a strict rule would
do the most damage. It sits between the two measured failures rather than choosing one.

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

**`since` is the age of the JOB, not of the call, and that is what replaces the permits split.**
A gate with no age ordering needs two separate reservations, because a caller finishing work must never
queue behind callers starting it, and both single-pool answers were measured failing (served evenly, 286 completions
waited a full second; completions given precedence, short-task throughput collapsed 3x to 4,416 from 7,964
steps/s). Ordering by job age gets the same guarantee for free: a caller coming back for its next turn keeps
its original timestamp, so it outranks everything that started later. Work in progress finishes before new
work begins, with no precedence rule to starve either side, and no starvation either — every claim
eventually becomes the oldest, so the order is first-in-first-out over jobs.

**Measured, because the property is easy to assume and easy to get backwards.** On a 16-vCPU shard at 600
flows/s with 8s tasks, the entry turn (taken before a candidate is picked up) waited a mean **159.1 ms**
across 1,280,424 acquisitions, while the exit turn waited **12.6 ms** across 300,814. Exits win by ~12.7x
without any rule saying so, purely because an exiting worker's claim is older by however long its task ran.
The corollary is the cost: entry and `Create` are the ones queueing, and `Create`'s p99 tracked task
duration (28 ms at 0s, 570 ms at 1s, 1,417 ms at 8s) for exactly that reason.

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
than racing for it; an earlier stochastic version of that test passed against an implementation that
dropped the pass outright, which is the standing warning about how to test this window.

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

## Deliberately absent

- **No `TryWaitTurn`.** It is the missing piece of a cross-resource head-of-line mitigation (see
  `engine/CLAUDE.md` §"Turn-taking on the database"), and the same rule applies: add it with the caller that
  uses it, or it is untested surface on a type whose whole job is a bound.
- **No aging, weighting or anti-starvation term.** The age ordering already makes the common band
  starvation-free, and a band that starves is a band that was given to the wrong caller. A control loop
  here would have to clear the bar every control loop in this engine has had to.
- **No metrics.** The free-pass count is an instant, so a turnstile saturated all window without being
  sampled empty reads as idle; the durable measure is `Pass.Waited`, reported per acquisition for the
  caller to record.
