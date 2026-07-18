# Dwarf `workflow` package — carrier, state, and rendering

> Load when: editing the `workflow.Flow` carrier, state/merge semantics, control signals, or the Mermaid renderer.
> Coupled with: root `CLAUDE.md` — the engine dispatch side implements much of what these describe.

### Task self-identity on the carrier

`Flow.FlowKey()` and `Flow.StepKey()` return the task's own flow/step keys (`{shard}-{id}-{token}`), populated by the
orchestrator on every dispatch alongside the timestamps. They let a task correlate logs/traces or call back into the
engine (`History`, `Step`, `Snapshot`) for its own flow without the host threading identity through baggage. `step_token`
is read alongside the claim/read in `processStep` (added to the RETURNING/OUTPUT/SELECT of all three driver branches);
`flow_token` already rides the flow-row load. Both keys also survive the `Flow` JSON round-trip (the `flowJSON` wire
format carries them), so a remote task reached over a transport sees the same identity as an in-process one. Empty when
read outside a dispatched task.

### State Model

Each step has three JSON columns: `state` (input snapshot), `changes` (output delta), `interrupt_payload` (from
`flow.Interrupt`). `state` is set at creation and normally immutable; `changes` is written after execution. The next
step's `state` is `merge(currentState, changes)`. This immutability enables checkpointing, restart, and recovery.

**A task's output is its `changes`, never the state map - the public API must not offer a raw-state write.**
`Flow.SetState` (marshal a struct into `state`, tracking nothing) was **deleted**: the engine reads back only
`changes`, so such a write could never be persisted. It read as the natural write-side twin of `ParseState` -
`ParseState(&order)` / mutate / `SetState(order)` - and the result was **silent, inconsistent data loss**:
`processStep` saw an empty `changes` and persisted the *prior* attempt's, so the write never reached the next step's
`state` nor `final_state`, while `evaluateTransitions` routed off `RawState()`, which *did* contain it. The flow
branched on a value it then threw away. `ParseState`'s godoc now shows the correct pairing (`Snapshot` ->
`ParseState` -> mutate -> `SetChanges(source, snap)`). `RawFlow.SetRawState` remains - it is the engine's own
seeding path, not task-facing.

**State mutation on retry:** on `flow.Retry()`, the engine merges `state + changes` back into `state` so the task
sees its own prior output next attempt; `changes` is preserved. `Resume` does **not** mutate `state`: it writes the
caller's data to `resume_data`, which `flow.Interrupt` returns on re-dispatch.

**Reducer delta convention:** tasks writing to reducer-managed fields (append, add, union, merge) set only the
**delta**, not the accumulated value. E.g. for a field wired to the append reducer via
`graph.SetReducer("messages", workflow.ReducerAppend)`, set `flow.Set("messages", []string{newMessage})`, not the
whole history. Violating this duplicates during fan-in merge.

**forEach element injection:** the current element is injected into `state` only (under `as`), not `changes`, so it is
available to the task but does not participate in fan-in merge.

**Delete is a cleared (JSON null) entry in `changes`, and the merge has two modes for it, split across two
`State` methods.** `flow.Del` (and `Set(k, nil)`) writes JSON `null` into `changes` - `dwarf` conflates null
and absent everywhere (`isCleared`), so there is no way to store a literal null value. `State.Merge`/`MergeReduce`
are the **accumulation** primitive: a cleared key is **preserved** (the replace path stores the tombstone; a
reducer-managed field *ignores* the clear - see below), so a changes delta can be built up across attempts/members
with the pending-delete marker intact. `State.DelNils` is the **materialization** step: it drops every cleared
key, so a delete never survives as a `"k": null` tombstone in materialized state - which is what
`final_state`/`FlowOutcome.State`/`Snapshot` expose to the host, and what the next step's `state` column becomes.
So *materialize* = `Merge`/`MergeReduce` then `DelNils`; *accumulate* = `Merge` alone. `processStep`'s changes
accumulation (folding a task's fresh output onto a prior attempt's `changes` before persisting the `changes`
column) uses `Merge` **without** `DelNils`, because there the null must be **preserved** - it only enacts the
delete when `changes` later folds onto `state` (a don't-materialize-here note is at that site in `execution.go`).

**A cleared incoming for a reducer-managed field is the reducer's IDENTITY - ignored, not dropped.** `MergeReduce`
skips a cleared value on a reduced field (`isCleared` check before the fold), leaving the accumulator untouched, so
a branch that `flow.Del`s a reduced field never wipes the cohort's contributions to it. Deleting a reduced field
is not a supported way to clear it - a delete is a *replace*-field concern. Pinned by
`fixtures/reducerdeleteflow_test.go`.

### Numbers are float64-domain; two unstorable values are a KNOWN, UNGUARDED punt

**Invariant (still holds): every number the engine itself produces round-trips exactly through a `float64`.**
State/baggage/payloads are carried as `map[string]any` across a JSON round trip through the database, and
`encoding/json` decodes a JSON number into a **`float64`** whenever the target is an `any` - exact for integers
only up to **2^53**. This is what lets every reader of a state map - including `boolexp`, which re-marshals its
symbols through JSON and compares in `float64` - treat numbers as `float64` without qualification, and it holds
by construction for every value dwarf derives (a `ReducerAdd`/`min`/`max` sum stores and round-trips fine even
past 2^53). Do **not** "fix" the read side with `UseNumber`: it changes the Go type every state-map reader sees
(`outcome.State["x"].(float64)` stops matching for an integral value; JSON has one number type, so no scheme
preserves the writer's Go type through the database), needs a reflect-walker for caller-supplied targets, and
buys exactness for a value an author can carry losslessly as a string anyway.

**Two *external/authored* values are unstorable and are currently NOT guarded - a deliberate, backlogged punt**
(the former write-side storability guard, and its whole `internal/jsonx` package, were removed;
`Flow.Set`/`set`/`SetChanges`/`Interrupt`/`Subgraph` and the engine's `Create`/`Continue`/`Fork`/`resume`
ingress no longer check). Both fail *silently*, which is why they were guarded and why the punt is a real
exposure to revisit, not a non-issue:

- **Integer beyond ±2^53.** Comes back **rounded** (`1234567890123456789` -> `...768`) and the engine
  re-marshals the rounded value onward into the next step's `state`, `final_state`, a `Fork`, a `Continue`.
  Nothing errors; the workflow charges the wrong order. Only **integer-shaped** literals are affected - a
  fractional/exponent number is float64-domain at any magnitude (`1e300` is fine). **Workaround: carry a large
  id as a string** (the `id_str` pattern 64-bit-id APIs already publish).
- **NUL (`U+0000`) in a string.** Valid UTF-8, legal JSON, but **PostgreSQL's `JSONB` rejects it**
  (`SQLSTATE 22P05`) while MySQL/SQLite/SQL Server accept it - so it passes the SQLite test suite and kills the
  flow on the recommended production database, in the worst way: the write that carries it is the step's own
  **completion UPDATE** (`execution.go`), so unguarded the step is left `running` with `error=""` and lease
  recovery re-executes the task forever (reproduced against real Postgres). **Workaround: base64-encode binary
  data.** (Invalid UTF-8 needs no guard - `encoding/json` coerces it to `U+FFFD` on marshal.) Note that
  `persist.go`'s `sanitizeErrorMessage` still strips control bytes from the *error-text* write independently, and
  its "ask the database" classifier turns a rejected payload write into a clean step failure rather than the old
  eternal loop - so the NUL is degraded from "silent eternal loop" to "clean failure at write time on Postgres,
  silently stored on the others," not fully benign.

If a guard is ever reintroduced, it must run on the RAW marshaled bytes *before* the decode (the decode rounds
a >2^53 integer to `float64`, so checking the decoded value is too late) and must NOT re-check the engine's own
derived sums (a legitimate `ReducerAdd` result past 2^53 marshals integer-shaped and would be falsely rejected).

### The typed getters panic on a type mismatch (and that is the safe option here)

`GetString`/`GetInt`/`GetFloat`/`GetBool`/`GetDuration`/`GetStrings` **panic** when the key holds a value of the
wrong type (the typed getters on `State` in `state.go` delegate to `Get` and `panic(errors.Trace(err))` on its
error; `Flow`'s getters forward to them). They previously discarded that error and returned the
zero value, which made *absent*, *cleared*, and *wrong-typed* indistinguishable - and the zero value is an
actively dangerous answer: `GetInt("retryAfter")` over a `1.5` (a fractional number from an upstream task, a
host's `initialState`, or `ReducerAdd`) read as `0`, and the task built a zero-delay retry loop against its
downstream. `Has` returned `true`, so the usual guard did not help.

Panicking is the right disposition *because of the engine's panic isolation*, not despite it: `ExecuteTask` is
wrapped in `errors.CatchPanic` at the call boundary, so the panic becomes the call's error and takes the normal
disposition - routed to `onError` if the graph has one, else `failStep` - with a stack trace attached. It is a
clean, immediate, correctly-attributed step failure, not a crash. (This is the *boundary* catch, which releases
the lease; the coarse worker-loop catch would have wedged the step. See "Host-call panic isolation" in
`engine/CLAUDE.md`.)

The rejected alternative was two-valued getters (`(T, error)`). `f.GetString("x")` is used inline everywhere
(conditions, arguments, struct literals), so it would force a temp var at every call site and the realistic
outcome is `v, _ := f.GetInt(…)` - the same silent zero with more ceremony. **`Get(key, target) error` already is
the two-valued form**, and is the documented escape hatch for a task that wants to *handle* a mistyped field.

Absent and cleared keys are **not** a mismatch - they still yield the zero value, so the optional-field idiom is
untouched. Only the typed getters panic; the engine's own `when`-expression evaluation does not go through them.
Pinned by `TestFlow_TypedGetterPanicsOnTypeMismatch` (workflow) and
`fixtures/panicflow_test.go` (end-to-end: a mistyped read fails the step and routes to `onError`, and the task is
proven not to have proceeded past the read).

### The `Flow` carrier is single-owner and carries no lock

A task owns its `Flow` exclusively for its execution; `Flow` is a bare struct over two `map[string]any`
with no mutex, and **deliberately so** - a mutex would paper over a misuse pattern (concurrent writes with
no defined ordering) rather than prevent it, and the carrier is single-owner by design.

The failure mode is why this is documented rather than merely implied: two goroutines writing a `Flow`
trip the Go runtime's concurrent-map-write detector, which is a **`throw`, not a panic**. `errors.CatchPanic`
around `ExecuteTask` (the host-call panic isolation) **cannot recover it**, so one task fanning out
internally with an errgroup and writing results straight back to the `Flow` takes down the whole replica and
every unrelated in-flight flow on it. The godoc on `Flow` (and the package doc) now says so, with the
collect-then-write-from-one-goroutine pattern spelled out. Parallelizing across *steps* (a `forEach`
transition, one `Flow` per branch) is the first-class answer.

### Task-Initiated Control Signals

Tasks signal the engine via control methods on the `Flow` carrier (distinct from the operations above):

- **`flow.Retry(initialDelay, delayMultiplier, maxIntervalDelay, giveUpAfter) bool`** - re-execute this task with
  exponential backoff. The bound is wall-clock, not a count: returns `true` (task should return `nil`) while the next
  attempt would still land within `giveUpAfter` of the step's first creation, else `false` (task should return its
  error) - including when the next backoff delay alone would overshoot the horizon, so a doomed wait is never parked
  before failing. The give-up check is made client-side in `Retry` against `flow.StepCreatedAt()`; the engine only
  consumes the backoff shape.
  Pass `giveUpAfter <= 0` for unlimited; to bound by count instead, pass `0` and gate on `flow.Attempt()`. The step row
  is reused. The engine tracks `attempt` and computes the re-dispatch delay `min(initialDelay * delayMultiplier^attempt,
  maxIntervalDelay)`, merging `state + changes` back into `state` so the task sees its prior output. `flow.Retry`
  rewinds the step row in place (it is task-initiated, while the flow is still `running` - distinct from `Fork`, which
  clones a *terminal* flow into a new one). **A retry clears the park
  slot** (`interrupt_done`/`subgraph_done` -> 0, `resume_data`/`subgraph_result` -> `'{}'`, `subgraph_error` -> `''`),
  so a retry after a resolved `flow.Subgraph` re-runs the child and after a resolved `flow.Interrupt` re-interrupts.
  **A retry of a step that launched a subgraph reaps the prior attempt's child flow, recursively, in the same
  transaction as the rewind** (`deleteSubgraphFlowsRootedAt(stepID)`). The child is always *terminal* at retry time
  (the park resolves only on a terminal child), so this is a delete of inert rows, not a cascade-cancel of live work.
  Leaving it would make the execution DAG claim two paths (`X -> iter1 -> iter2 -> Y`) when the model is single-path,
  and let History assembly attach the discarded child's subtree to the caller. The reap is **step-scoped** (only this
  caller's children). **The rewind is guarded on `status='running'`, and the reap runs only if it fires:** a `Cancel`
  landing mid-task terminalizes this running step (cancelled) before the task returns and arms the retry, so an
  unguarded rewind would revive the immutable cancelled step to `pending` (with a far backoff `not_before` - a transient
  zombie the terminal-flow check only clears minutes later) *and* reap the now-terminal tree's children (inert, but
  belonging to a terminal flow - an immutability violation). `processStep` therefore rewinds first under the guard and
  reaps/re-dispatches only when it actually rewound a still-running step; a lost guard leaves the step cancelled (its
  children already cancelled by the `Cancel` cascade) and returns. This is the enforcement behind "`flow.Retry` rewinds
  a step in place but only while the flow is `running`" (see root "Terminal flows are immutable"). Defense in depth:
  History's `loadSubgraphChildren` keeps only the latest child per caller step (`ORDER BY flow_id`, last row wins =
  highest `flow_id`), matching `completeSurgraphFlow`/wedge/`Continue`, so even a stray dangling child never renders. `flow.Retry` carries no condition - the task writes the retryable condition explicitly in the surrounding `if`
  (retry-on-any-error is usually wrong). To retry only on a timeout, gate on
  `errors.StatusCode(err) == http.StatusRequestTimeout`.
- **No jitter on retry backoff:** the worker pool already throttles per-replica concurrency, so simultaneous retries
  queue in the pool rather than overwhelm downstream. Jitter would add latency for no throughput benefit.
- **`flow.Sleep(duration)`** - delay the *next* step's execution by setting its `not_before`. The timer adapts to
  wake when the sleep expires. In fan-out, only the last sibling's sleep affects the fan-in point.
- **`flow.Goto(target)`** - override transition routing: skip normal evaluation and follow the `withGoto` transition
  to `target`, if registered. Goto transitions are never taken during normal evaluation.
- **`flow.Interrupt(payload)`** - pause and park the flow. The payload is stored in `interrupt_payload` and propagated
  up the surgraph chain. The task should return normally after. The engine sets the flow `interrupted`; a caller
  wanting to be told resumes via `Await` or an orchestrating workflow (the engine has no stop callback).

**Single-park guard.** A step parks at most once - interrupt XOR subgraph, never both and never the other kind on
re-entry. `processStep` enforces this after the task returns: a competing-signals check fails the step if more than
one control signal is set in one dispatch, and a second check fails the step when the returned flow arms a park while
the step row's materialized `interrupt_done`/`subgraph_done` shows the *other* kind already resolved. The `workflow`
package's parkers already reject a conflicting second park at the call site; this guard is the trust boundary for an
untrusted returned flow.

## Flow Rendering (`workflow.FlowRenderer`)

`FlowRenderer` produces a Mermaid flowchart from a `History` result. Diagnostic intent: answer "where did the time go
in this flow?" at a glance. Defaults render top-down; `With*Colors` swap palettes; `WithLinks` enables per-node click
directives. `HistoryMermaid` wraps it as an engine method writing to an `io.StringWriter`.

### CSS-variable theming model

Color values flow through `classDef`/`style` directives; the renderer emits no themeVariables init block. Callers pass
either hex literals (static rendering) or CSS custom-property references like `var(--primary-container)` (host pages
that track a page-level theme). With `var()` values, browsers resolve through the SVG's CSS cascade and the diagram
re-colors on light/dark toggle without reinvocation. The CSS-mode pattern: pass `"currentColor"` for fill and `""`
for text - the generated classDef omits `color:`, host CSS sets it, and Mermaid inherits via `currentColor`.

### Color knobs and status groups

| Pair | Statuses |
|---|---|
| primary | `completed`, `running` (running gets a dashed border) |
| secondary | `pending` + chrome (`_start`, `_end`, fan-out cohort wrappers, subgraph block fills) |
| error | `failed`, `cancelled` |
| attention | `interrupted` (distinct from error - "needs human eyes," not hard failure) |

### DAG-edge model

The execution DAG is reconstructed from `PredecessorID`/`SuccessorID`, NOT `step_depth`. Every edge is recorded on at
least one endpoint - fan-out via each child's `PredecessorID`, fan-in via each cohort exit's `SuccessorID`, linear on
both - and the rendered edge set is their deduped union, exact for arbitrary nesting.

### Subgraph caller decomposition

A subgraph caller step renders as **two** visual elements: the caller's task node and a visible Mermaid subgraph
wrapper block containing the recursively-rendered SubHistory. The caller node's duration label is the **net** caller
cost: `net = (caller.UpdatedAt - caller.StartedAt) - subgraph_wall_time`, where `subgraph_wall_time = max(SubHistory*
.UpdatedAt) - min(SubHistory*.CreatedAt)` walked recursively. The net is the caller's own pre-call + post-return body
time; total call cost reconstructs as `net + subgraph_wall_time` without double-counting. Edges thread:

```
predecessor --> caller         (parent DAG, transition gap label)
caller --> innerHead           (the call, queue-wait label)
innerTail --> Y.entries        (the return, transition gap label)
```

`byID[caller].exits` is set to the subgraph's inner tails, so the existing `addEdge(caller, Y)` machinery emits one
edge per inner-tail x parent-DAG-successor combination. A terminal subgraph caller surfaces its inner tails as outer
tails, connected to `_end`.

### Node and edge label semantics

**Node label** = `UpdatedAt - StartedAt` (task body time) for any non-caller step that ran and reached a terminal
status. Pending/created/in-flight steps render with just the task name. Subgraph callers render the *net* cost.

**Edge label** = `to.StartedAt - from.UpdatedAt` (transition gap: DB commit + queue + dispatch). Computed from the
step records.

**Call edge label** = `entry.StartedAt - entry.CreatedAt` (queue wait on the subgraph's entry step). Without it, that
time would be inside `subgraph_wall_time` but invisible on any rendered edge.

### Fan-out cohort wrappers

Two-or-more steps sharing one `PredecessorID` get wrapped in an invisible Mermaid `subgraph` block (empty label, no
fill, no stroke) - purely a layout container so siblings cluster near their parent. Edges always go between actual
task nodes; nothing terminates at the cohort wrapper.

### Truncation in label formatting

`formatDuration` uses integer-millisecond truncation for sub-second values (`%dms`). A diagram with N labeled edges
accumulates up to ~N/2 ms of systematic underestimation in any path sum - diagnostically irrelevant (the goal is to
spot where time went, not reconcile to the millisecond).

### Mermaid escaping (author-controlled strings)

Task names, workflow/subgraph names, graph titles, and `when`-expressions are **author-controlled** and land in a
diagram a host renders in a UI, so every one of them passes through the single `escapeMermaid` helper (defined in
`flowrenderer.go`, shared by both renderers). **Go `%q` is not Mermaid escaping** and must never be used for these:
Mermaid honors no backslash escaping inside a label, so a raw `"` ends the label and lets the rest inject
node/edge/`click` syntax, and `<`/`>`/`` ` `` pass straight through into the label's HTML rendering (an XSS/markup
vector). `escapeMermaid` rewrites `" ' < > `` ` `` [ ] { } ( ) | #` to their Mermaid character references (`#quot;`,
`#lt;`, `#124;`, …), each rendering as the literal glyph; `#` is escaped **first** so an input already shaped like
`#quot;` cannot survive as a live reference (`strings.NewReplacer` is a single non-overlapping pass). Every sink
supplies its own surrounding quotes (`["%s"]`, `-->|"%s"|`, `{{"%s"}}`, `subgraph id ["%s"]`) and escapes only the
content. The lone deliberate `%q` left is `themeCSS` in the graph frontmatter (host-controlled CSS, YAML-quoted); the
frontmatter **title** is `escapeMermaid`'d then wrapped in plain quotes (escaping strips every `"`, so it is a valid
YAML scalar *and* neutralizes markup that `%q` would have passed into the rendered caption). Engine-generated edge
labels (durations) are escaped too, harmlessly, so no sink is a `%q` exception a future edit could cargo-cult. Pinned
by `TestEscapeMermaid`, `TestGraph_MermaidEscapesInjection`, `TestFlow_MermaidEscapesInjection`.
