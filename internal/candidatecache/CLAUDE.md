# Dwarf `internal/candidatecache` — bounded step-candidate hint cache

> Load when: changing the cache's `Offer`/`Refill`/`Pop`/`floor`/`lowWater` semantics.
> Coupled with: `engine/CLAUDE.md` §"Execution Model" — the refiller's two-level priority+fairness selection and
> the doorbell/single-slot-refiller protocol that *drive* this cache live there, not here.

This package is only the **mechanism**: a small, per-replica bounded set of step *candidates*. The invariant an
editor must not break is that entries are **hints, not ownership** — a worker pops a candidate and then
CAS-acquires the underlying step before executing, so a stale, duplicated, or already-claimed candidate is
harmless (the CAS loser just pops the next one). Nothing here may assume a popped candidate is still runnable.

The `floor`/`lowWater`/`Offer`-vs-`Refill` behavior is a protocol with the engine's refiller: any change to their
semantics must stay consistent with the selection algorithm described in the engine doc. The godoc on each method
states the local contract; the *why* (priority bands, urgent-arrival head-insert, liveness after `processStep`)
is the engine's.
