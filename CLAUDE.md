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

> **Documentation convention.** Keep all prose and code comments host-agnostic. Dwarf is standalone; though it sits
> downstream of a specific framework in this repo, it must never name or assume that host (not the Fabric, the Foreman,
> or their types like `sub.TimeBudget` or a "plane"). Use **"host"** for the upstream layer and describe what it *does*
> (mints a token, enforces a per-call deadline, shares a per-test isolation key), not which product does it.

> **Audience convention (where each kind of prose lives).**
> - **Godoc on exported identifiers** is the host developer's API doc: what the call does, its contract, args, returns,
>   errors, and caller invariants. Focused on use, not implementation.
> - **Internal rationale - the *why* behind the mechanics - belongs here in CLAUDE.md by default.** Prefer adding to the
>   relevant section here over expanding a godoc comment.
> - **Unexported/inline comments** carry agent-facing notes that must sit *next to the code* - a non-obvious ordering
>   constraint, an invariant a future edit could break. Keep them short.
> - **Avoid lengthy rationale in code.** If a comment grows into paragraphs, move the rationale here and leave a terse note.
> - **Do not refer to CLAUDE.md from code.** Comments stand on their own; CLAUDE.md references code, never the reverse.

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
- **`fixtures/CLAUDE.md`** - the test harness: `RunInTest`/`SetInTest` test mode, the per-test-engine +
  no-`t.Parallel` connection-load rule, and `TestProxy` conventions.
- **`internal/database/CLAUDE.md`** - the sharded SQL connections (`ShardSet`): SQL-dialect guidance, shard-count
  sizing + shard-per-server topology, the connection lifecycle, and the sharding *mechanics* (1-indexed routing,
  parallel `OnEach` fan-out, DSN/test-mode resolution). The pool-sizing *formula* and sharding *semantics* stay in
  `engine/CLAUDE.md`.
- **`internal/keys/CLAUDE.md`** - the flow/step key *format* (`{shard}-{id}-{token}`), token entropy (why 64-bit),
  and the token-free `CorrelationID` derivation. (The engine-side enforcement/posture stays in `engine/CLAUDE.md`.)
- **`internal/candidatecache/CLAUDE.md`** - the bounded hint-cache mechanism; its driving refiller algorithm is in
  `engine/CLAUDE.md`. (`internal/lru` is a textbook LRU+TTL - godoc only, no design doc.)

**Landmines that radiate into engine code - obey these even though the full detail now lives in a package doc:**

- **Timestamps:** never bind a Go `time.Time` into SQL; write with `NOW_UTC()`/`DATE_ADD_MILLIS`. (`internal/migrations/CLAUDE.md`)
- **MySQL JSON compare:** `json_col = '{}'` never matches on MySQL - use a per-driver `CAST(... AS CHAR)`. (`internal/migrations/CLAUDE.md`)
- **State delete:** `flow.Delete`/`Set(k,nil)` writes a JSON `null` that `MergeState` *drops* on materialization but
  *preserves* during changes-accumulation. (`workflow/CLAUDE.md`; enforced at `execution.go`)
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
are merged using the reducer for that field: `replace` (last write wins, default), `append` (concatenate arrays),
`add` (sum numbers), `union` (deduplicate arrays), or `merge` (combine objects, new key wins). A field with no
registered reducer uses `replace`; every non-default fan-in field is wired explicitly with
`graph.SetReducer(name, reducer)` (the older `sum*`/`list*`/`set*` name-prefix inference was removed).

**Thread** - A chain of flows linked by `Continue`. Each flow has a `thread_id` grouping it with others in the same
multi-turn conversation; defaults to `flow_id` (each flow its own thread). `Continue` inherits the thread's
`thread_id`. Subgraph flows always start their own thread to avoid contaminating the parent's continuation chain. The
flowKey returned by the initial `Create` doubles as the threadKey.

### Terminal flows are immutable

**A terminal flow (`completed`/`failed`/`cancelled`) is immutable.** Its outcome (`status`, `final_state`,
`error`/`cancel_reason`) is frozen; the only operations on it are **read** (Snapshot/History/Continue-source/
Fork-source) and **removal** (Delete/Purge). There is **no in-place re-run of a terminal flow** - recovery and
exploration happen only via **`Fork`**, which clones a terminal flow up to a chosen step into a *new*
self-contained flow and never mutates the original. This invariant governs which operations may exist; it is
why `Restart`/`RestartFrom`/`Recover` (in-place rewinds) were removed.

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

