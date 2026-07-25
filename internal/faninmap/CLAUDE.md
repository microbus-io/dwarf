# Dwarf `internal/faninmap` — the fan-out → fan-in convergence map

> Load when: changing the traversal, what counts as a fan-out source, or where the map is built and
> cached.
> Coupled with: `workflow/CLAUDE.md` (`Graph.Validate`, which rejects the shapes this traversal is
> allowed to assume away) and `engine/CLAUDE.md` §"Fan-Out and Fan-In" (the empty-`forEach` shortcut that
> is this map's only consumer).

## It is a derived VIEW, not part of the graph

`workflow.Graph` carries only the author's definition — nodes, transitions, fan-in flags. This mapping is
an engine-side lookup computed from that definition, and keeping it out of the graph is what stops it
from leaking into the public API and into every flow's frozen graph JSON.

**It is never persisted.** The map is a pure, deterministic `O(V+E)` function of the graph structure, so
freezing it onto the flow row would store something rederivable, and — worse — would pin a stale copy for
the life of every flow already running when the traversal changes. The engine computes it once per flow
at dispatch and caches it beside the parsed graph, which is the same lifetime the parse already has.

**`Graph.Validate` computes the same mapping and throws it away.** That is deliberate, not waste:
`Validate` is *pure* (it needs the mapping only to reject branches of one fan-out converging on different
fan-in nodes) and storing its result on the graph would make the author's definition carry engine state.
Two cheap traversals of a small structure, on two paths that run at completely different times, beat one
shared mutable field.

## The traversal assumes a VALIDATED graph, and that assumption is load-bearing

`Validate` runs at `Create` and rejects the malformed shapes — a fan-out whose branches converge on
different fan-in nodes, a fan-in reached from outside any cohort, a graph with no explicit `END` edge —
so this package only *builds* the mapping and guards none of them.

The consequence to hold in mind: **a graph frozen onto a flow before a validator fix is replayed from the
flow row on every dispatch and is never re-validated.** So a shape the current validator would reject can
still reach this code. The engine carries its own runtime guard for the case that matters (a fan-in
arrival with no cohort), and that guard is in `processStep`, not here. If you find yourself wanting to
harden this traversal against a malformed graph, the question to ask first is whether the engine already
fails that flow loudly — it does for the known case.

## What consumes it

One thing: routing an **empty** `forEach` cohort straight to its convergence node. A non-empty cohort
reaches its fan-in through ordinary cohort accounting and never asks. So `For` returning `""` means "this
source converges nowhere", and the engine's empty-cohort path treats that as complete-the-flow — an arm
that is effectively unreachable for any graph `Validate` accepted, and is a defensive fallback rather
than documented behavior.
