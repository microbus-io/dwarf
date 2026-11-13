# Dwarf `internal/diagnostics` — what the engine's components report through

> Load when: changing what a component logs, traces or meters through, or the seams it consults.
> Coupled with: `engine/CLAUDE.md` §"The Host interface" (the observability providers a host injects),
> `internal/accessors/CLAUDE.md` (how a test outside package engine reaches the engine's seams) and
> `fixtures/CLAUDE.md` (the waiting helpers and their arm-first idiom).

`Diags` holds the logger, the meter and tracer and the providers they come from, and the seamster. The engine owns
one, fills it before Startup returns - the seams at construction, the logger from `SetLogger`, the providers,
meter and tracer resolved at Startup - and hands the same one to every component it builds (the janitor, the
piston, the Sonar, the shard set's sequel layer).

- **One per engine, never process-wide.** A test process runs many engines at once, each with its own seams armed
  and often its own metric reader; a singleton would make parallel tests interfere.
- **Every getter works on a nil `*Diags`**, returning what reports nowhere: a discard logger, inert seams, a no-op
  meter, tracer and providers. A component's tests pass nil rather than building one, and get the silent defaults
  a component had before anything was set.
- **Fields are set, then read, and fixed once a run starts.** The setters run before the `Diags` is handed out
  (construction and Startup); the getters are then read from any goroutine without a lock, which is safe because
  handing it out - starting the goroutines that read it - orders the writes before the reads. Components hold the
  `*Diags` and call its getters rather than copying a field. **Do not make the fields swappable mid-run (atomic
  pointers):** a new value would reach almost nothing - sequel took its logger and providers at `Open`, every
  instrument was made on the meter when its component was built - so the engine's setters reject a change after
  Startup, and a restart rebuilds every component from the `Diags` as it then stands.

## Seams: the faults and checkpoints a test arms

Recovery and race paths are hard to trigger on demand (a lost revive, a commit that loses to a contention storm, a
Delete landing in a one-statement window inside another operation's transaction). Rather than forge DB rows or
hammer timing, the engine carries **two white-box seams** a same-package test uses to drive those paths
deterministically:

- **Fault injection** makes a site *misbehave* - a test arms a named fault, the engine consults it at a strategic
  site and simulates the failure (a synthetic error, a dropped signal/doorbell, a stale `lease_seq`, a reap aborted
  mid-tree).
- **Execution checkpoints** make a site *observable and pausable* - `Waiter`/`Wait`/`WaitTimeout` rendezvous with
  the engine reaching a named point; `Break`/`Resume` freeze the engine there until released. They compose to
  drive a concurrent op into a precise window with no timing hammer.

**Both are inert in production by construction:** every consult short-circuits on the `enabled` bool the engine
passes to `seamster.New` (cached from `testing.Testing()` in `NewEngine`), so a production binary pays a single
bool read per site and neither seam can arm or fire. The mechanism lives in the `seamster` package; the
names and `Join` for their targeted forms live here, every component's in one catalogue, so the valid set stays
discoverable, a test cannot arm a fault or checkpoint no site consumes, and none of it is on the engine's exported
surface. A test outside package engine reaches the engine's seams through `internal/accessors` (`Seams`).

**This package must stay production-safe and import no component:** every component imports it for the names
and its `Diags`, so it must not import `testing`, `testarossa` or anything test-only - which is why the names
cannot live in `internal/enginetest`, the shared test helpers - nor any component, or the two are an import
cycle.

**Consults are written inline at the site they affect, never wrapped in a helper.** A wrapper puts the fault's
effect a jump away from the code it perturbs, which is exactly backwards for a seam whose whole purpose is to be
readable at the point of perturbation. Two consults that are easy to wrap are inline: the lost-delivery consult in
`deliverFlowFailureToParent` (`subgraph.go`) and the flow-row-write counter in `queueFlowGrab` (`dispatch.go`). Both
guard on `e.diags.Seams().Enabled()` first - the lost-delivery one because building its scope needs a **DB read** (a live
binary must run neither the query nor the consult), the counter because formatting the flow id would otherwise
allocate a throwaway scope string on the hot path.

**A rendezvous is two steps - *arm*, then *receive* - and which spelling to use is a question about the CALLER,
not about taste.** Fusing the steps means a test can only arm AFTER the operation it wants to observe is already
running, so every checkpoint reached in that gap is lost. Both workarounds for that cost something real: a goroutine
calling a blocking wait merely *moves* the race (it may not have registered
before the checkpoint fires) and leaks on timeout; a `Break` closes the race honestly but **freezes the engine**,
perturbing the timing under test and obliging a `Resume`. So:

- **`e.diags.Seams().Waiter(name)`** - arms and hands back a channel; receive after the trigger. The only form that works
  when the test's *own next statement* drives the engine to the checkpoint.
- **`e.diags.Seams().WaitTimeout(ctx, name, timeout)`** - arms and blocks, reporting arrival. Use it where the
  engine is *already* running into the checkpoint: after a `Break` froze it there, or with the trigger in flight.
  Spelled `assert.True(e.diags.Seams().WaitTimeout(ctx, cp, 10*time.Second), "engine never reached checkpoint cp")` so a
  miss fails with a diagnostic rather than hanging to the suite deadline. (`Wait(ctx, name)` is the same thing
  bounded only by ctx; `WaitTimeout` is sugar over it for the usual "wait this long" shape. A targeted name is
  built with `diagnostics.Join` first.)

Because `Waiter` makes a rendezvous race-free on its own, **`Break` is reserved for what it is actually for** -
genuinely *holding* the engine while a racing operation runs - and is never armed merely to make an observation
race-free.

**Prefer a checkpoint rendezvous over a status poll.** `enginetest.AwaitFlowStatus` is the one wait that still
needs a helper, because a flow key is not known until `Create` returns; its arm-FIRST-then-read-`Visits` order is
load-bearing (`fixtures/CLAUDE.md`).

**A targeted seam name is built by `diagnostics.Join`**, which joins a base name with the entity it
targets. A consult site and the test that arms it both call it, which is what keeps the two from spelling the
join differently - the seam itself keys on a plain string and knows nothing about the parts. `signalStop` fires
the stop checkpoint **both bare and targeted at `(flowKey, status)`**: `Checkpoint(ctx, diagnostics.CheckpointFlowStopped)`
and `Checkpoint(ctx, diagnostics.Join(diagnostics.CheckpointFlowStopped, flowKey, status))`. Both are needed because **the two are
different names, and firing one does not wake a waiter on the other** (the same contract faults carry) - the
bare name serves "any flow stopped", the targeted one lets a test wait for one flow while a subgraph child, a
fan-out sibling, or a peer replica's flows stop concurrently. The targeted form is meaningful only because
`signalStop` runs **post-commit**: when it fires the status is durable, so a test reading the row immediately
after sees it - the exact fact a status poll was spinning to sample.

**Every join outside a test is wrapped in `e.diags.Seams().Enabled()`, without exception.** The join
assembles a string, and a caller assembles it *before* the seam's own gate can short-circuit - so an unguarded
site allocates on every pass in production, for a seam that can never fire there. A consult reads
`if e.diags.Seams().Enabled() && e.diags.Seams().IsFault(diagnostics.Join(...))`, whose `&&` short-circuits before the join; a
checkpoint pair puts both fires inside one `if e.diags.Seams().Enabled()` block, since the bare fire is a no-op when
disabled anyway. A **bare constant** name costs nothing and needs no guard - that is most sites.

Every component and every test shares `diagnostics.Join`, so no two can spell the separator differently.

Two limits: only stops routed through `signalStop` have a rendezvous (completed/failed/terminated/cancelled/interrupted),
and because `Visits` is monotonic a **repeated** status (interrupt → resume → interrupt) is satisfied by the
earlier occurrence, so wait on those with a `Waiter` armed around the specific trigger.
