# Dwarf `internal/engineseams` — the engine's test-only seams

> Load when: adding or consulting a fault or checkpoint in engine code, writing a test that arms one, or changing
> `engineundertest.go`'s accessors.
> Coupled with: `fixtures/CLAUDE.md` (the waiting helpers and their arm-first idiom) and `engine/CLAUDE.md`.

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
names, `Join` for their targeted forms, and the accessors (`DB`, `Seams`, and the test/bench overrides) live in
`internal/engineseams`, so the
valid set stays discoverable, a test cannot arm a fault or checkpoint no site consumes, and none of it is on
the engine's exported surface. `engineundertest.go`'s `init` fills the accessors (function variables taking
`any`, since `engineseams` cannot import `engine`).

**`engineseams` must stay production-safe:** engine production code imports it for the names, so it must not
import `testing`, `testarossa` or anything test-only - which is why the names cannot live in
`internal/enginetest`, the shared test helpers. And it must not import `engine`, or the pair is an import
cycle.

**Consults are written inline at the site they affect, never wrapped in a helper.** A wrapper puts the fault's
effect a jump away from the code it perturbs, which is exactly backwards for a seam whose whole purpose is to be
readable at the point of perturbation. Two consults that are easy to wrap are inline: the lost-delivery consult in
`deliverFlowFailureToParent` (`completion.go`) and the flow-row-write counter's two sites in `execution.go`. Both
guard on `e.seams.Enabled()` first - the lost-delivery one because building its scope needs a **DB read** (a live
binary must run neither the query nor the consult), the counter because formatting the flow id would otherwise
allocate a throwaway scope string on the hot path.

**A rendezvous is two steps - *arm*, then *receive* - and which spelling to use is a question about the CALLER,
not about taste.** Fusing the steps means a test can only arm AFTER the operation it wants to observe is already
running, so every checkpoint reached in that gap is lost. Both workarounds for that cost something real: a goroutine
calling a blocking wait merely *moves* the race (it may not have registered
before the checkpoint fires) and leaks on timeout; a `Break` closes the race honestly but **freezes the engine**,
perturbing the timing under test and obliging a `Resume`. So:

- **`e.seams.Waiter(name)`** - arms and hands back a channel; receive after the trigger. The only form that works
  when the test's *own next statement* drives the engine to the checkpoint.
- **`e.seams.WaitTimeout(ctx, name, timeout)`** - arms and blocks, reporting arrival. Use it where the
  engine is *already* running into the checkpoint: after a `Break` froze it there, or with the trigger in flight.
  Spelled `assert.True(e.seams.WaitTimeout(ctx, cp, 10*time.Second), "engine never reached checkpoint cp")` so a
  miss fails with a diagnostic rather than hanging to the suite deadline. (`Wait(ctx, name)` is the same thing
  bounded only by ctx; `WaitTimeout` is sugar over it for the usual "wait this long" shape. A targeted name is
  built with `engineseams.Join` first.)

Because `Waiter` makes a rendezvous race-free on its own, **`Break` is reserved for what it is actually for** -
genuinely *holding* the engine while a racing operation runs - and is never armed merely to make an observation
race-free.

**Prefer a checkpoint rendezvous over a status poll.** `enginetest.AwaitFlowStatus` is the one wait that still
needs a helper, because a flow key is not known until `Create` returns; its arm-FIRST-then-read-`Visits` order is
load-bearing (`fixtures/CLAUDE.md`).

**A targeted seam name is built by `engineseams.Join`**, which joins a base name with the entity it
targets. A consult site and the test that arms it both call it, which is what keeps the two from spelling the
join differently - the seam itself keys on a plain string and knows nothing about the parts. `signalStop` fires
the stop checkpoint **both bare and targeted at `(flowKey, status)`**: `Checkpoint(ctx, engineseams.CheckpointFlowStopped)`
and `Checkpoint(ctx, engineseams.Join(engineseams.CheckpointFlowStopped, flowKey, status))`. Both are needed because **the two are
different names, and firing one does not wake a waiter on the other** (the same contract faults carry) - the
bare name serves "any flow stopped", the targeted one lets a test wait for one flow while a subgraph child, a
fan-out sibling, or a peer replica's flows stop concurrently. The targeted form is meaningful only because
`signalStop` runs **post-commit**: when it fires the status is durable, so a test reading the row immediately
after sees it - the exact fact a status poll was spinning to sample.

**Every join outside a test is wrapped in `e.seams.Enabled()`, without exception.** The join
assembles a string, and a caller assembles it *before* the seam's own gate can short-circuit - so an unguarded
site allocates on every pass in production, for a seam that can never fire there. A consult reads
`if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(...))`, whose `&&` short-circuits before the join; a
checkpoint pair puts both fires inside one `if e.seams.Enabled()` block, since the bare fire is a no-op when
disabled anyway. A **bare constant** name costs nothing and needs no guard - that is most sites.

`engine`, `fixtures` and `internal/enginetest` share `engineseams.Join`; `internal/piston` and
`internal/peers` keep unexported copies, since they sit below the engine. The copies must agree on the
separator; a drift makes a wait target a name nothing fires, so it times out and fails loudly rather than going
quietly wrong.

Two limits: only stops routed through `signalStop` have a rendezvous (completed/failed/terminated/cancelled/interrupted),
and because `Visits` is monotonic a **repeated** status (interrupt → resume → interrupt) is satisfied by the
earlier occurrence, so wait on those with a `Waiter` armed around the specific trigger.

