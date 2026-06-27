# Concepts

A small vocabulary runs through all of dwarf. Learn these seven terms and the rest of the docs read easily.

## Graph

A **graph** is the definition of a workflow: which tasks exist, how they connect, and how parallel
branches merge. It's a directed graph of **tasks** (nodes) and **transitions** (edges), with an entry
point and optional per-field reducers. You build one in code with `workflow.NewGraph` and the `Add*`
methods, and it is identified by a name. Graphs are reusable templates — many flows run the same graph.

See [Building graphs](graphs.md).

## Task

A **task** is a named unit of work in a graph. It has a node name (its identity within the graph) and a
URL (the address your host's `ExecuteTask` uses to reach it — a function key, an endpoint, a topic). A task
reads inputs from the flow's state, does its work, and writes outputs back. Tasks are reusable across
graphs; the same task URL can appear at several positions.

See [Writing tasks](tasks.md).

## Flow

A **flow** is a single execution of a graph — one running instance. It has a unique key, tracks its
current position, and carries a state map that evolves as tasks run. A flow moves through a lifecycle:

```
Create ──► running ──► completed
             │  ▲              \
    Interrupt│  │Resume         ► failed   (an unhandled task error)
             ▼  │
        interrupted              ► cancelled (via Cancel)
```

- **running** — `Create` makes a flow and immediately runs it; a task is being dispatched/executed.
- **interrupted** — parked for external input (a task called `Interrupt`).
- **completed** — finished; no transition matched.
- **failed** — a task returned an error with no matching error handler.
- **cancelled** — terminated by `Cancel`.

A flow's key is a composite string of the form `{shard}-{flowID}-{token}`. You pass it to every operation.

## Step

A **step** is one task execution within a flow. Each step captures an immutable **input snapshot**
(`state`), the **output delta** the task produced (`changes`), and metadata (status, error, timings,
attempt count). Steps are numbered by depth; parallel fan-out siblings share a depth. The next step's
input is `merge(state, changes)`. This immutability is what makes checkpointing, forking, and crash
recovery possible — every step is a durable, replayable record.

The full step-by-step record is available with `History`; one step with `Step`.

## Thread

A **thread** groups flows that form a multi-turn conversation, linked by `Continue`. Each flow has a
thread key; by default a flow is its own thread. `Continue` starts a new flow that inherits the previous
flow's final state and identity, so you can model a chat or an iterative process as a chain of flows that
share history.

See [Continue](operations.md#continue-a-thread).

## Reducer

A **reducer** is a merge strategy for a state field during fan-in. When parallel branches converge, each
field's changes are combined with its reducer:

| Reducer | Constant | Effect |
|---|---|---|
| replace | `ReducerReplace` (default) | Last write wins |
| append | `ReducerAppend` | Concatenate arrays |
| add | `ReducerAdd` | Sum numbers |
| min / max | `ReducerMin` / `ReducerMax` | Smaller / larger number |
| union | `ReducerUnion` | Merge arrays, deduplicate |
| merge | `ReducerMerge` | Merge objects, new key wins |
| and / or | `ReducerAnd` / `ReducerOr` | Logical AND / OR of booleans |
| concat | `ReducerConcat` | Concatenate strings |

A field with no registered reducer uses `replace`. You wire non-default fields with
`graph.SetReducer(field, reducer)`. **Important:** a task writing to a reducer-managed field sets only its
*delta* (e.g. the one item to append), not the accumulated value — otherwise fan-in double-counts.

See [Fan-out & subgraphs](fan-out-and-subgraphs.md).

## State model

Every step has three JSON facets:

- **state** — the input snapshot, set when the step is created and (normally) immutable.
- **changes** — the output delta the task wrote.
- **interrupt payload** — what a task passed to `Interrupt`, surfaced to the awaiting caller.

A task receives `merge(state, changes-so-far)` as its working state. It mutates the flow; the engine
records the diff as that step's `changes`. The next step's `state` is the merge of the current step's
state and changes. Because each step's snapshot is frozen, you can fork from any step, replay history,
or recover a crashed flow without ambiguity about "what did this task see?".

## How it fits together

> You define a **graph** of **tasks**. The engine creates a **flow** for each execution, runs one **step**
> per task, persisting **state** between them and merging parallel branches with **reducers**. Related
> flows form a **thread**.

Next: [Building graphs](graphs.md).
