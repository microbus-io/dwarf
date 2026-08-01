# Dwarf Engine Design Notes

## Overview

Dwarf is a standalone workflow-orchestration engine (`github.com/microbus-io/dwarf`). It executes workflow graphs by
dispatching tasks through an injected executor, managing state between steps, and handling fan-out/fan-in, interrupts,
retries, subgraphs, and failure recovery. It depends only on `sequel` (SQL), plus the pure-types sub-package
`dwarf/workflow`.

This document captures the engine's internal design rationale - the *why* behind the mechanics, which godoc does not
record. The engine is library code: it reaches tasks, fetches graphs, signals peers, and reports stops through a
single **injected `Host` interface** (plus separately injected observability providers) rather than a built-in
transport. A host application (for example a microservice) wires that interface to its own transport, identity, and
observability. Where this doc refers to "the host" or "the adapter," it means that wrapping layer.

## Documentation conventions

**Host-agnostic prose.** Keep all prose and code comments host-agnostic. Dwarf is standalone; though it sits
downstream of a specific framework in this repo, it must never name or assume that host (not the Fabric, the Foreman,
or their types like `sub.TimeBudget` or a "plane"). Use **"host"** for the upstream layer and describe what it *does*
(mints a token, enforces a per-call deadline, shares a per-test isolation key), not which product does it.

**Audience — every piece of prose is written for exactly one of two readers; know which before you write.**

- **Module users** (people and agents *using* dwarf as a dependency) read the **public-facing** docs:
  **`docs/`, `README.md`, and godoc comments** on exported types, functions, and packages. Write these about the
  **public API only**: how to use it, what to pass, what comes back. They must **not** name or describe internal
  implementation - unexported functions, internal packages, private struct fields, source-file names (`state.go`),
  or internal mechanics - because that reader cannot see any of it. Godoc especially stays at the how-to-use-it
  altitude, not the how-it-works one.
- **The dwarf coding agent** (you, working *on* the engine) reads the **internal** prose: **`CLAUDE.md` files and
  private/unexported code comments**. These *may* freely reference internals - private identifiers, file names,
  internal packages, the schema - and are where design rationale and pitfall warnings belong.

**The same split applies WITHIN `internal/`, one level down.** An internal package has its own two readers, and
they divide exactly as above:

- Its **godoc** - the package comment and every exported symbol - is written for the **package's consumer**
  (the engine, or a sibling internal package): what the type is for, how to drive it, what the callbacks
  must hold, what is safe to call concurrently. A short usage sketch belongs here. Rationale does not.
- Its **`CLAUDE.md`** is written for an agent working **on that package**: why the mechanism is shaped this
  way, which alternatives were built and lost, which invariants are load-bearing, and which tests pin them.

So **every internal package with non-obvious mechanics carries a `CLAUDE.md`**, and design rationale that
drifted into a package comment moves there rather than being duplicated. (A package whose mechanics are
genuinely textbook - `internal/lru` - needs none.) The pull to explain *why* in the package doc is strong
and constant, because the author has just finished deciding it; resist it, or the rationale ends up in two
places and one of them goes stale.

Both audiences share one rule from above: **stay host-agnostic** (say "host", never name the upstream framework),
in public and internal prose alike.

Further guidance within each:
- **Private comments** should stay concise: don't restate what the code plainly does; do capture design rationale
  or a pitfall specific to that location.
- **Cross-cutting design rationale** belongs in the appropriate `CLAUDE.md`, not scattered across code comments.
- **Describe what IS, never what changed.** Every comment and every `CLAUDE.md` line states the design as it
  stands now. Do not write "X used to be Y", "this replaced Z", "there is no longer a...", or a numbered account
  of what was deleted in what order. Git holds the change history; prose that re-tells it ages into a lie at the
  next edit, and it costs the reader the one thing they came for - the current picture.
- **Record the footgun, not the change.** A rejected or removed alternative earns its place only when it encodes
  a trap the next person would otherwise walk into, and then it is written as a **standing constraint with its
  evidence**, not as a chronology:
  - Yes: *"Do not bind the status in a `WHERE` clause - it defeats the SQL Server / SQLite filtered index."*
  - Yes: *"Do not give this its own timer; derive the ticker from the configured budget. Measured: a derived
    interval oscillated at ~1,000x the discard and a 2.4x worse p99."*
  - No: *"This used to have its own timer, which we deleted in favour of a ticker."*

  The test is whether a reader who had never seen the old code could **act** on the sentence. If they could not,
  it is history, and it belongs in the commit message. Keep the number, the measurement, or the failure mode that
  makes the constraint checkable - that is the part with value, and it survives the code being rewritten again.
- **No prose refers to anything not checked in.** Working documents ("finding A2", "_PLAN.md"), review
  threads, and **numbered benchmark campaigns** ("campaign 11", "run 14") are all invisible to every future
  reader, so a citation of one is a dead end that also implies the claim cannot be checked. Cite the
  measurement itself - the number, the configuration, the failure mode - or a checked-in doc such as
  `docs/benchmark-cloud.md`. "Measured at ~120 steps/s per connection" travels; "campaign 11 measured it"
  does not.
- **Code comments do not refer to `CLAUDE.md`.** The agent reads `CLAUDE.md` implicitly.

## Where the design docs live

This root file is always loaded and holds only the conventions, core vocabulary, and orientation below. The design
rationale lives in package `CLAUDE.md` files that load automatically when you read a file in that package - **read the
matching one before working there:**

- **`engine/CLAUDE.md`** - the orchestrator internals (the bulk): the Host interface & configuration, every Engine
  operation, the execution/scheduling model, fan-out/fan-in, subgraphs, crash recovery, metrics/tracing, and sharding.
- **`workflow/CLAUDE.md`** - the `workflow.Flow` carrier and pure types: state model, control signals
  (`flow.Retry`/`Sleep`/`Goto`/`Interrupt`/`Subgraph`), task self-identity, and the `FlowRenderer`.
- **`internal/migrations/CLAUDE.md`** - schema: the `dwarf_flows`/`dwarf_steps` column catalog, indexing strategy, and the
  SQL-authoring gotchas.
- **`fixtures/CLAUDE.md`** - the test harness: `NewEngineUnderTest`/`SetTestName` test mode, the per-test-engine +
  no-`t.Parallel` connection-load rule, and `TestProxy` conventions.
- **`internal/database/CLAUDE.md`** - the sharded SQL connections (`ShardSet`): SQL-dialect guidance, shard-count
  sizing + shard-per-server topology, the connection lifecycle, and the sharding *mechanics* (1-indexed routing,
  parallel `OnEach` fan-out, DSN/test-mode resolution). The pool-sizing *formula* and sharding *semantics* stay in
  `engine/CLAUDE.md`.
- **`internal/keys/CLAUDE.md`** - the flow/step key *format* (`{shard}-{id}-{token}`), token entropy (why 64-bit),
  and the token-free `CorrelationID` derivation. (The engine-side enforcement/posture stays in `engine/CLAUDE.md`.)
- **`internal/workers/CLAUDE.md`** - the demand side: the grow-on-demand goroutine `Crew` (not "pool" -
  that word is the database connection pools), the idleness-plus-gate growth trigger (and why it needs both
  an edge and a cadence), and the two-phase drain.
- **`internal/candidates/CLAUDE.md`** - the bounded hint-cache mechanism; its driving refiller algorithm is in
  `engine/CLAUDE.md`. (`internal/lru` is a textbook LRU+TTL - godoc only, no design doc.)
- **`internal/staterefs/CLAUDE.md`** - storing a large carried state field once: the anchor-cost size policy
  (why fan-out width is the primary axis), the one-hop and both-column invariants, and why the Loader is a
  per-call batched callback rather than a bound connection. (The engine-side integration - flow-boundary
  flattens, `Fork`'s remap - stays in `engine/CLAUDE.md`.)
- **`bench/CLAUDE.md`** - the load harness: how a rig lies (RTT dominance, database litter, stale peer
  rows), how to design an arm so the comparison survives it, and what a workload must do to exercise the
  refiller at all.
- **`internal/planner/CLAUDE.md`** - the cross-shard band + fairness decision: why per-shard tallies replaced a
  barrier, why participation is *declared* (`Clear`) rather than inferred from silence, and the slice rule's
  determinism.
- **`internal/pipeline/CLAUDE.md`** - one shard's supply cycle (`sleep -> tallying -> planning -> fetching ->
  pushing`): its self-pacing, and the asymmetric error policy (a failed scan clears the shard but spares the
  cache; a failed fetch touches neither; an empty plan clears the partition).
- **`internal/piston/CLAUDE.md`** - the per-shard cylinder that drives that cycle: the two SQL queries, idle
  mode, the replica-partition predicate, and `Liveness` (why dispatch evidence is a turn COUNTER plus a
  duration-qualified busy flag, not a bool).
- **`internal/peers/CLAUDE.md`** - one shard's replica registry: the `Sonar` that owns this replica's row
  there, the two timestamps (alive vs serving), and the three consumers of one reading whose postures are
  matched to how reversible each decision is (hold the pool divisor, fail the work divisor open, make the
  hygiene delete wait).
- **`internal/claimstracker/CLAUDE.md`** - the intra-replica in-flight claim set: why the window is bounded
  (1-2s) rather than tied to a step, and the two-generation roll that costs no per-entry work.
- **`internal/faninmap/CLAUDE.md`** - the per-flow fan-out-to-fan-in routing map: why it is derived at dispatch
  rather than persisted or stored on the graph, and why an unreachable fan-out routes nowhere.
- **`internal/latch/CLAUDE.md`** - parking callers on a key until it settles: why polling makes registration
  order irrelevant, why the board holds no notion of what a status means, and why closing travels the same
  one-slot channel as a status.

**Landmines that radiate into engine code - obey these even though the full detail now lives in a package doc:**

- **Timestamps:** never bind a Go `time.Time` into SQL; write with `NOW_UTC()`/`DATE_ADD_MILLIS`. (`internal/migrations/CLAUDE.md`)
- **MySQL JSON compare:** `json_col = '{}'` never matches on MySQL - use a per-driver `CAST(... AS CHAR)`. (`internal/migrations/CLAUDE.md`)
- **Canonical JSON in storage is a PRECONDITION being kept, not a rule anything currently enforces.**
  Nothing in the engine compares stored payload bytes today: `union` dedupes with `reflect.DeepEqual` over
  *materialized* values (order-independent for maps), `Fingerprint` hashes `status|count|max(updated_at)`,
  and no query compares one payload column with another. What canonicalization actually buys is **Go TYPE
  normalization** - `Set` round-trips a value so a struct is stored as a `map[string]any`, because
  `DeepEqual` between a struct and its decoded twin is false. That is the real mechanism behind the
  `Continue`-`additionalState` bug; key ORDER never entered into it.
  Keep writing canonical bytes anyway: it is what a **byte-comparing reducer** would need (comparing raw
  arrays instead of materializing every element), and that is the standing plan for `union`/`append`. Break
  it and that optimization is foreclosed, silently and much later. (`workflow/CLAUDE.md`)
- **State delete:** `flow.Del`/`Set(k,nil)` writes a JSON `null` tombstone, which has **two spellings** - a
  Go nil, and the raw bytes `null` on a field still held as JSON - and `isCleared` must recognize both or a
  delete read back from a column silently stops taking effect. `State.Merge`/`MergeReduce`
  *preserve* it (accumulation); `State.DelNils` *enacts* it (materialization) - the engine accumulates the
  changes delta with the tombstone intact, then `DelNils` when the delta folds onto state. A cleared value on
  a *reducer*-managed field is *ignored* (the reducer's identity), never dropped. (`workflow/CLAUDE.md`; enforced
  at `execution.go`)
- **Two problem values are KNOWN and currently UNGUARDED - a deliberate, backlogged punt.** Nothing checks
  storability on the write side, by decision, not oversight. (1) An integer-shaped value beyond **±2^53**.
  **STORAGE IS EXACT** - raw-field storage means a carried field is never decoded, so it round-trips
  byte-exact into the next step's `state`, `final_state`, a `Fork` and a `Continue`, and reading one does not
  memoize the rounded form. The exposure is **per-READER, not per-field**: a read into an `any`
  (`Get`/`Value`/`All`/`Map`/`Parse` into a map) rounds, while **`GetInt` does not** - it unmarshals straight
  into an `int`, exact at any int64 magnitude. `boolexp` still rounds, because it re-marshals the symbols and
  decodes them itself, so a `when` comparing a >2^53 id compares float64s regardless of storage. Carry a
  large id as a string if it must be branched on. (2) A **NUL** (`U+0000`) in a string - Postgres `JSONB`
  rejects it (`SQLSTATE 22P05`) while the other dialects accept it, so it passes SQLite tests and fails on the
  recommended production DB; base64-encode binary data. Do not add a guard (nor "fix" the integer with
  `UseNumber`) without revisiting the punt - a write-side guard must run on the **raw caller bytes** before
  any decode, and must not re-check the engine's own derived merges. (`workflow/CLAUDE.md`)
- **Write-first transactions:** every flow-terminating transaction must UPDATE first, or the flow strands as a
  `running` orphan. (`engine/CLAUDE.md`)
- **Lease fencing:** every post-execution write to the *dispatched* step must carry `AND lease_seq=?` (the
  generation returned by the claim CAS) and treat a zero-row match as a benign no-op (`return nil`, never an
  error) - otherwise a slow or DB-clock-skewed "zombie" worker corrupts or terminalizes a peer's healthy
  re-execution. Execution is at-least-once; the fence protects *state*, not side effects. (`engine/CLAUDE.md`)
- **Status literals, not binds:** in a `WHERE` clause, inline the status constant
  (`"...WHERE status='"+workflow.StatusRunning+"'..."`), never bind it (`status=?`) - a bound status defeats the
  SQL Server / SQLite filtered index (and, for `List`'s `ORDER BY ... LIMIT`, the cardinality estimate). A
  caller-supplied status (`List`/`Query.Status`) is validated with `workflow.IsValidStatus` then inlined.
  Exception: `SET status=?` assignments stay bound. (`internal/migrations/CLAUDE.md`)

### Core Concepts

**Workflow graph** - A directed graph defining a workflow's structure: which tasks run, in what order, under what
conditions. Built in code with the `workflow.Graph` API via `NewGraph(name)`. Each graph has a human-friendly
display name (surfaced in rendering and denormalized onto the flow row as `workflow_name`), an entry point,
tasks, transitions, and optional reducers for fan-in state merging. The graph does **not** carry its own
resolve URL: the resolve key is a separate opaque `workflowURL` passed to `Create`/`Run`/`LoadGraph` and stored
on the flow (`workflow_url`); the engine never keeps it on the graph. Each node is bound to its dispatch
endpoint with `graph.SetEndpoint(nodeName, url)` (create-or-update); the same endpoint may be bound under
multiple node names.

**Naming convention.** Graph and task (node) names are PascalCase (`Reserve`, `Charge`) - graph-topology
identifiers, kept visually distinct from the lowercased dispatch URLs and the camelCase state fields. The engine
imposes no casing; this is a fixture/example convention only.

**Task** - A named unit of work within a workflow, identified by a task name/URL and executed via the injected
`ExecuteTask`. Tasks receive state via a `workflow.Flow` carrier, read input from state fields, perform work, and
write output back to state fields. Tasks are reusable across workflows.

**Flow** - A single execution of a workflow graph. Each flow has a unique ID, tracks its current position, and
maintains a state map that evolves as tasks execute. Statuses: `created` -> `running` -> `completed`/`failed`/
`cancelled`, with `interrupted` as a parked state for human-in-the-loop scenarios.

**Step** - A single task execution within a flow. Each step captures an immutable input snapshot (`state`), the output
delta (`changes`), and metadata (status, error, timing). Steps are numbered by `step_depth`; parallel fan-out
siblings share a `step_depth`. Once terminal (`completed`/`failed`/`cancelled`), a step is immutable.

**Reducer** - A merge strategy for state fields during fan-in. When parallel branches converge, each branch's changes
are merged using the reducer for that field. Ten are defined (`workflow/reducers.go`): `replace` (last write wins,
default), `append` (concatenate arrays), `union` (concatenate arrays, deduplicated), `add` (sum numbers), `min`/`max`
(smaller/larger number), `and`/`or` (logical fold of booleans), `concat` (concatenate strings), and `merge` (combine
objects, new key wins). A field with no registered reducer uses `replace`; every non-default fan-in field is wired
explicitly with `graph.SetReducer(name, reducer)`. There is **no** inference from a field's name - a `sum*`/`list*`/
`set*` prefix means nothing, so an unwired field silently takes `replace` instead of the reducer its name implies.

**Thread** - A chain of flows linked by `Continue`. Each flow has a `thread_id` grouping it with others in the same
multi-turn conversation; defaults to `flow_id` (each flow its own thread). `Continue` inherits the thread's
`thread_id`. Subgraph flows always start their own thread to avoid contaminating the parent's continuation chain. The
flowKey returned by the initial `Create` doubles as the threadKey.

### Terminal flows are immutable

**A terminal flow (`completed`/`failed`/`cancelled`) is immutable.** Its outcome (`status`, `final_state`,
`error`/`cancel_reason`) is frozen; the only operations on it are **read** (Snapshot/History/Continue-source/
Fork-source) and **removal** (Delete/Purge). There is **no in-place re-run of a terminal flow** - recovery and
exploration happen only via **`Fork`**, which clones a terminal flow up to a chosen step into a *new*
self-contained flow and never mutates the original. This invariant governs which operations may exist: there is
no `Restart`/`RestartFrom`/`Recover`, and no in-place rewind may be added, because any of them would mutate a
frozen outcome.

Two clarifications: *immutable ≠ permanent* - Delete/Purge/DeleteOnCompletion still **remove** terminal flows.
And *outcome*-frozen, not byte-frozen - a straggler fan-out sibling can still settle to terminal after its
flow terminalizes (convergence to the frozen outcome), and `flow.Retry` rewinds a step in place but only while
the flow is `running` (pre-terminal). `interrupted` is **not** terminal - `Resume` still mutates it.

### Flow Lifecycle

```
Create --> running --> completed   (terminal, immutable)
            |  ^
            |  | Resume
            v  |
        interrupted
            |
            v
  failed   (terminal, immutable)

  cancelled (terminal, immutable, via Cancel)

Recovery/exploration: Fork clones a terminal flow up to a chosen step into a NEW flow.
```

1. **Create** inserts a flow and its entry step and starts them in one transaction - the flow is returned
   already `running` (its entry step `pending`), and the doorbell is rung. There is no separate `Start`
   step and no externally-visible `created` resting state (`created` survives only as an internal/transient
   state inside Create's own transaction and Fork's leaf-gate).
2. A worker picks up the step, executes the task, and evaluates transitions to create next steps.
3. Repeats until no transitions match (flow completes), a task errors (flow fails), or the flow is cancelled.
4. Tasks can call `flow.Interrupt()` to pause for external input; `Resume` continues. A flow that should
   *wait* before doing work uses this as **staged start**: an entry task that interrupts, resumed when ready
   (such a flow rests as `interrupted`, not `created`).
5. A terminal flow is never re-run in place. To re-run from a chosen step (optionally with state overrides),
   `Fork` clones it into a new flow. A task can re-run *itself* in place via `flow.Retry` while still running.

