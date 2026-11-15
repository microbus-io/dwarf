# Dwarf Engine Design Notes

## Overview

Dwarf is a standalone workflow-orchestration engine (`github.com/microbus-io/dwarf`). It executes workflow graphs by
dispatching tasks through an injected executor, managing state between steps, and handling fan-out/fan-in, interrupts,
retries, subgraphs, and failure recovery. Its storage is `sequel` (SQL); beside it the module depends on `boolexp`
(transition `when` expressions), `errors`, `seamster` (test-only fault and checkpoint seams, inert in production),
the OpenTelemetry API, and `golang.org/x/sync`. The pure types live in the sub-package `dwarf/workflow`.

This document captures the engine's internal design rationale - the *why* behind the mechanics, which godoc does not
record. The engine is library code: it fetches graphs and reaches tasks through a single **injected `Host`
interface** with exactly two methods, `LoadGraph` and `ExecuteTask` (plus separately injected observability
providers), rather than a built-in transport. Everything between replicas - work discovery, `Await` wakes, fleet
membership - goes through the shared database; the engine sends nothing to its peers. A host application (for
example a microservice) wires that interface to its own transport, identity, and observability. Where this doc
refers to "the host" or "the adapter," it means that wrapping layer.

## Documentation conventions

**Host-agnostic prose.** Keep all prose and code comments host-agnostic. Dwarf is standalone; though it sits
downstream of a specific framework in this repo, it must never name or assume that host (not the Fabric, the Foreman,
or their types like `sub.TimeBudget` or a "plane"). Use **"host"** for the upstream layer and describe what it *does*
(mints a token, enforces a per-call deadline, shares a per-test isolation key), not which product does it.

**Audience — every piece of prose is written for exactly one of two readers; know which before you write.**

- **Module users** (people and agents *using* dwarf as a dependency) read the **public-facing** docs:
  **the docs site ([docs.dwarf.run](https://docs.dwarf.run), whose source is `../cloudflarepages/docs.dwarf.run`), `README.md`, and godoc comments** on exported types, functions, and packages. Write these about the
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
- **One explanation per invariant.** When a change touches an invariant whose rationale appears in more than one
  place, fold the copies into the owning `CLAUDE.md` in the same commit and leave each call site its local rule,
  hazard and pinning test. Copies drift until they contradict, and a reader cannot follow both.
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
  measurement itself - the number, the configuration, the failure mode - or a published doc such as
  [Cloud Benchmarks](https://docs.dwarf.run/performance/benchmark-cloud/). "Measured at ~120 steps/s per connection" travels; "campaign 11 measured it"
  does not.
- **Code comments do not refer to `CLAUDE.md`.** The agent reads `CLAUDE.md` implicitly.
- **IMPORTANT: Do not add Claude as a co-author on git commits.** No `Co-Authored-By: Claude ...` trailer,
  in this repo or any sibling repo (e.g. `boolexp`) touched from a dwarf task.

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
- **`fixtures/CLAUDE.md`** - the test harness: the name-keyed `NewEngineUnderTest` test mode and its
  caller-owned teardown, the per-test-engine rule under a `t.Parallel` suite (and which tests opt out),
  `hosts.Mux` conventions, and the table of every `DWARF_TESTING_*` environment variable a test or the bench reads
  (the engine's production code reads none).
- **`internal/enginetest/CLAUDE.md`** - the invariant checker (`AssertInvariants`, and `CaptureOutcomes` /
  `AssertOutcomesKept` for closed outcomes that must not move), generated operation histories against a reference
  model, and the mutation inventory: each engine transaction's starting state, guard, post-commit backstop and
  lost-reply test. A new transaction or state transition updates it.
- **`internal/database/CLAUDE.md`** - the sharded SQL connections (`ShardSet`) and the `Shard` handle every actor on
  a shard is given (its database, turns, residue class and server measurements): SQL-dialect guidance, shard-count
  sizing + shard-per-server topology, the connection lifecycle, and the sharding *mechanics* (1-indexed routing,
  parallel `OnEach` fan-out, DSN/test-mode resolution). The pool-sizing *formula* and sharding *semantics* stay in
  `engine/CLAUDE.md`.
- **`internal/keys/CLAUDE.md`** - the flow/step key *formats* (`{shard}-{hour}-{flowID}-{token}`, `{shard}-{hour}-{flowID}-{stepID}-{token}`), token entropy (why 64-bit),
  and the token-free `CorrelationID` derivation. (The engine-side enforcement/posture stays in `engine/CLAUDE.md`.)
- **`internal/workers/CLAUDE.md`** - the demand side: the grow-on-demand goroutine `Crew` (not "pool" -
  that word is the database connection pools), the growth trigger (one edge plus a standing reserve of one idle
  worker, and why no timer), the busy-fraction shrink rule, and the two-phase drain.
- **`internal/graphcache/CLAUDE.md`** - the two-layer parsed-graph cache: why a per-flow layer of weak pointers
  sits over a shared layer that owns one parse per graph, why the shared layer has no TTL, and why its key is
  SHA-256.
- **`internal/archiver/CLAUDE.md`** - one shard's archive day tables: why a day is made by its own migration
  sequence, why the catalog and not the migration records says which days exist, retention by days, and the
  one-statement locate with its sparse directory.
- **`internal/janitor/CLAUDE.md`** - one shard's background jobs on a replica that dispatches it: why each job has a
  goroutine of its own, the idle wait after a round without progress, why every batch takes a fresh turn and no
  connection of its own, why the janitors stop before the workers drain, fast archiving and fast deletion, how
  their jobs share out a shard's roots, and the pace of the jobs that run the engine's recovery repairs.
- **`internal/candidates/CLAUDE.md`** - the bounded hint-cache mechanism; its driving refiller algorithm is in
  `engine/CLAUDE.md`. (`internal/lru` is a textbook LRU+TTL, bounded by count or by weight - godoc only, no
  design doc.)
- **`internal/memlimit/CLAUDE.md`** - the process's memory limit, for memory-bound defaults: why `GOMEMLIMIT`
  comes first and why the cgroup read walks the process's own cgroup and every ancestor.
- **`internal/staterefs/CLAUDE.md`** - storing a large carried state field once: the anchor-cost size policy
  (why fan-out width is the primary axis), the one-hop and both-column invariants, and why the Loader is a
  per-call batched callback rather than a bound connection. (The engine-side integration - flow-boundary
  flattens, `Fork`'s remap - stays in `engine/CLAUDE.md`.)
- **`bench/CLAUDE.md`** - the load harness: how a rig lies (RTT dominance, database litter, stale peer
  rows), how to design an arm so the comparison survives it, and what a workload must do to exercise the
  refiller at all.
- **`internal/planner/CLAUDE.md`** - the cross-shard band + fairness decision: why per-shard tallies rather than a
  barrier, why participation is *declared* (`Clear`) rather than inferred from silence, and the slice rule's
  determinism.
- **`internal/pipeline/CLAUDE.md`** - one shard's supply, in two independently paced loops (a Tallier:
  `sleep -> tallying`; a Supplier: `sleep -> planning -> fetching -> pushing`): why they are split, their
  self-pacing, and the asymmetric error policy (a failed scan clears the shard but spares the
  cache; a failed fetch touches neither; an empty plan clears the partition).
- **`internal/piston/CLAUDE.md`** - the per-shard cylinder that drives that cycle: the two SQL queries, idle
  mode, the replica-partition predicate, and `Liveness` (why dispatch evidence is a turn COUNTER plus a
  duration-qualified busy flag, not a bool).
- **`internal/peers/CLAUDE.md`** - one shard's replica registry: the `Sonar` that owns this replica's row
  there, the two timestamps (alive vs serving), the shared order and staggered cut that pick each shard's
  dispatchers (and why the order key is frozen), and the three consumers of one reading whose postures are
  matched to how reversible each decision is (hold the count and the cut, fail the work divisor open, make the hygiene
  delete wait).
- **`internal/claimstracker/CLAUDE.md`** - the intra-replica in-flight claim set: why the window is bounded
  (2-4s) rather than tied to a step, and the two-generation roll that costs no per-entry work.
- **`internal/diagnostics/CLAUDE.md`** - what the engine's components report through (`Diags`: logger, meter and
  tracer and their providers, seams): one per engine, and nil-safe getters; and the seam catalogue - why seams are
  inert in production, why consults are inline, the arm-then-receive rendezvous, and the `Enabled()` guard on every
  join.
- **`internal/accessors/CLAUDE.md`** - how a test or benchmark outside package engine reaches a running engine's
  internals, and why the accessors are function variables over `any`.
- **`internal/faninmap/CLAUDE.md`** - the per-flow fan-out-to-fan-in routing map: why it is derived at dispatch
  rather than persisted or stored on the graph, and why an unreachable fan-out routes nowhere.
- **`internal/latch/CLAUDE.md`** - parking callers on a key until it settles: why polling makes registration
  order irrelevant, why the board holds no notion of what a status means, and why closing travels the same
  one-slot channel as a status.
- **`internal/turnstile/CLAUDE.md`** - the priority-and-age ordered admission gate in front of each shard's
  connections: why the waiting happens there, why passes are NOT sized to the resource, and what `Close`
  promises.

**Landmines that radiate into engine code - obey these; the full detail lives in the named package doc:**

- **Timestamps:** never bind a Go `time.Time` into SQL; write with `NOW_UTC()`/`DATE_ADD_MILLIS`. On PostgreSQL a time
  column is `TIMESTAMP(3)`, never `TIMESTAMPTZ` (a non-UTC session shifts every stored instant) nor a bare `TIMESTAMP`
  (microseconds, finer than the millisecond `NOW_UTC()`). (`internal/migrations/CLAUDE.md`)
- **MySQL JSON compare:** `json_col = '{}'` never matches on MySQL - use a per-driver `CAST(... AS CHAR)`. (`internal/migrations/CLAUDE.md`)
- **Stored JSON bytes are not canonical, and nothing compares them.** Every comparison decodes first (`union`
  dedupes decoded values, numbers by value; `Fingerprint` hashes `status|count|max(updated_at)`). What JSON storage buys is Go
  TYPE normalization, so never hand a reducer a caller's Go value that has not been marshalled.
  (`workflow/CLAUDE.md`)
- **A step's `changes` is `workflow.Changes`; an overwrite is its field name with a `!` suffix, and an addition
  (the elements a list with no reducer gained, recorded by the engine) with a `+` suffix.** Decode a
  `changes` column with `changesOf` / `Changes.UnmarshalJSON`, never as a plain `State`. A null contribution
  means "nothing", so `isCleared` must recognize both null spellings. (`workflow/CLAUDE.md`)
- **Integers beyond ±2^53 and NULs in strings are unguarded, by decision.** An untyped read rounds such an
  integer; Postgres `JSONB` rejects a NUL. Do not add a write-side guard or a read-side `UseNumber` without
  revisiting the punt. A fold's decode is internal - its Go values never leave the reducer and go back out as
  JSON - so it decodes numbers as `json.Number` and stays exact; a new fold site must too. Engine code that passes
  stored values on (a forEach's elements) moves their bytes, never decoded values. (`workflow/CLAUDE.md`)
- **Write-first transactions:** every flow-terminating transaction must take the flow-row lock first
  (`lockFlowRows`: a locking read on PostgreSQL and MySQL, a `touch` flip elsewhere), or the flow strands as a
  `running` orphan. (`engine/CLAUDE.md`)
- **Lease fencing:** every post-execution write to the *dispatched* step must carry `AND lease_seq=?` (the
  generation returned by the claim CAS) and treat a zero-row match as a benign no-op (`return nil`, never an
  error) - otherwise a slow or DB-clock-skewed "zombie" worker corrupts or terminalizes a peer's healthy
  re-execution. Execution is at-least-once; the fence protects *state*, not side effects. (`engine/CLAUDE.md`)
- **Ids are bound, never written into SQL:** every distinct statement text is prepared and cached per connection,
  so an id in the text grows every connection's cache without bound (measured ~3 MB of PostgreSQL backend memory per
  cached statement carrying 8,192 ids). Bind ids; bind a list with `database.IDList` (padded to one of five lengths,
  at most `database.MaxListIDs` = 2,000 a statement). (`internal/migrations/CLAUDE.md`)
- **Status literals, not binds:** in a `WHERE` clause, inline the status constant
  (`"...WHERE status='"+workflow.StatusRunning+"'..."`), never bind it (`status=?`) - a bound status defeats the
  SQL Server / SQLite filtered index (and, for the lists' `ORDER BY ... LIMIT`, the cardinality estimate). A
  caller-supplied status (`Query.Status`) is validated with `workflow.IsValidStatus` then inlined.
  Exception: `SET status=?` assignments stay bound. (`internal/migrations/CLAUDE.md`)

### Core Concepts

**Workflow graph** - A directed graph defining a workflow's structure: which tasks run, in what order, under what
conditions. Built in code with the `workflow.Graph` API via `NewGraph(name)`. Each graph has a human-friendly
display name (surfaced in rendering and denormalized onto the flow row as `workflow_name`), an entry point,
tasks, transitions, and optional per-field reducers. The graph does **not** carry its own
resolve URL: the resolve key is a separate opaque `workflowURL` passed to `Create`/`Run`/`LoadGraph` and stored
on the flow (`workflow_url`); the engine never keeps it on the graph. Each task is bound to its dispatch
URL with `graph.SetURL(taskName, url)` (create-or-update); the same URL may be bound under multiple task
names.

**Naming convention.** Graph and task names are PascalCase (`Reserve`, `Charge`) - graph-topology
identifiers, kept visually distinct from the lowercased dispatch URLs and the camelCase state fields. The engine
imposes no casing; this is a fixture/example convention only.

**Task** - A named unit of work within a workflow, identified by a task name/URL and executed via the injected
`ExecuteTask`. Tasks receive state via a `workflow.Flow` carrier, read input from state fields, perform work, and
write output back to state fields. Tasks are reusable across workflows.

**Flow** - A single execution of a workflow graph. Each flow has a unique ID, tracks its current position, and
maintains a state map that evolves as tasks execute. Statuses: `created` -> `running` -> `completed`/`failed`/
`terminated`/`cancelled`, with `interrupted` as a parked state for human-in-the-loop scenarios.

**Step** - A single task execution within a flow. Each step captures an immutable input snapshot (`state`), the output
delta (`changes`), and metadata (status, error, timing). Steps are numbered by `step_depth`; parallel fan-out
siblings share a `step_depth`. Once terminal (`completed`/`failed`/`terminated`/`cancelled`), a step is immutable.

**Reducer** - How writes to a state field combine: `replace` (the default), `append`, `union`, `add`, `min`, `max`,
`and`, `or`, `concat`, `merge` (`workflow/reducers.go`). On a reduced field every write is a contribution, folded at
every hand-off; `Overwrite`/`Del` replace instead (`workflow/CLAUDE.md`). Each non-default field is wired with
`graph.SetReducer`; nothing is inferred from a field's name, so an unwired `sum*` field silently takes `replace`.

### Closed flows are immutable

**A closed flow (`completed`/`failed`/`terminated`/`cancelled`) is immutable.** Its outcome (`status`, `final_state`,
`error`/`cancel_reason`) is frozen; the only operations on it are **read** (Snapshot/History/Continue-source/
Fork-source) and **removal** (Delete/Purge). There is **no in-place re-run of a closed flow** - recovery and
exploration happen only via **`Fork`**, which clones a closed flow up to a chosen step into a *new*
self-contained flow and never mutates the original. This invariant governs which operations may exist: there is
no `Restart`/`RestartFrom`/`Recover`, and no in-place rewind may be added, because any of them would mutate a
frozen outcome.

Two clarifications: *immutable ≠ permanent* - Delete/Purge/DeleteOnCompletion still **remove** closed flows.
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

  terminated (terminal, immutable, via Terminate or flow expiration)

  cancelled  (terminal, immutable, via Cancel when every unrecovered loss was a cancellation)

Recovery/exploration: Fork clones a closed flow up to a chosen step into a NEW flow.
```

1. **Create** inserts a flow and its entry step and starts them in one transaction - the flow is returned
   already `running` (its entry step `pending`), and the doorbell is rung. There is no separate `Start`
   step and no externally-visible `created` resting state (`created` survives only as a transient status
   inside Fork's clone transaction, where the cloned flows and the leaf step are held `created` until the id
   mapping is complete).
2. A worker picks up the step, executes the task, and evaluates transitions to create next steps.
3. Repeats until no transitions match (flow completes), a task errors (flow fails), or the flow is terminated.
4. Tasks can call `flow.Interrupt()` to pause for external input; `Resume` continues. A flow that should
   *wait* before doing work uses this as **staged start**: an entry task that interrupts, resumed when ready
   (such a flow rests as `interrupted`, not `created`).
5. A closed flow is never re-run in place. To re-run from a chosen step (optionally with state overrides),
   `Fork` clones it into a new flow. A task can re-run *itself* in place via `flow.Retry` while still running.

