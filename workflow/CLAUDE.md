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
step's `state` is `merge(currentState, changes)`. This immutability enables checkpointing, `Fork`, `flow.Retry` and
crash recovery.

**A task's output is its `changes`, never the state map - the public API must not offer a raw-state write.**
The engine reads back only `changes`, so a write into `state` alone (marshal a struct into it, tracking nothing)
is never persisted. Such a method reads as the natural write-side twin of `Parse` - `Parse(&order)` / mutate /
write - and the result is **silent, inconsistent data loss**: `processStep` sees an empty `changes` and persists
the *prior* attempt's, so the write never reaches the next step's `state` nor `final_state`, while
`evaluateTransitions` routes off `RawState()`, which *does* contain it. The flow branches on a value it then
throws away. A task writes with `Set` and the typed setters. `RawFlow.SetRawState` is the engine's own seeding
path, not task-facing.

**`SetChanges(source)` is `Set` for each field `json.Marshal(source)` emits, and nothing more.** It exists for a
host's generated task wrappers, which write a typed output struct (decode -> parse the input -> handler ->
`SetChanges(out)`). It marshals the struct once and `Set`s each key, so encoding/json alone decides the names and
which fields are included (`omitempty`/`omitzero`, `,string`, embedded promotion, a custom `MarshalJSON`). **Do not
diff against a snapshot.** A diff dropped a branch's contribution that happened to equal the accumulated value
(`total` 5 under `add`, contribution 5). It also recorded an empty `omitzero` output field as null - a delete - so
a host returning "no new items" wiped the field. Pinned by `TestFlow_SetChangesIsSetPerField`.

**State mutation on retry:** on `flow.Retry()`, the re-dispatched task's view is `state` with `changes` merged onto
it, so it sees its own prior output; `changes` is preserved, and the rewind writes nothing to `state`. `Resume`
does **not** mutate `state`: it writes the caller's data to `resume_data`, which `flow.Interrupt` returns on
re-dispatch.

**Reducers fold only what fan-out BRANCHES contribute, at their fan-in - and `Continue`'s `additionalState`.**
Everywhere else a write replaces: the hand-off to the next step, the fan-out source's own write (the base its
branches fold onto), and the final state. So a branch writing a reduced field sets only its **contribution**
(`flow.Set("messages", []string{newMessage})` under `ReducerAppend`), while a trunk task writes the whole value
(`f.SetStrings("messages", append(f.GetStrings("messages"), m))`). Pinned by
`fixtures/terminalreducerflow_test.go` and `fixtures/emptyforeachreducerflow_test.go`.

- **Folding a step's own write onto its own input is the trap.** It treats the write as a delta, so the outcome
  depends on graph shape: `total` 10 under `add` and a last task setting 15 end at 25 if the final state folds
  and at 15 if a do-nothing step follows, since the hand-off replaces.
- **Do not reduce on every hand-off (LangGraph's model) without answering four costs:**
  - a sentinel overwrite value has to stay out of Fork's overrides, `Create`'s input and `Subgraph`'s `in`;
  - per-field input tracking in the carrier, where a `Set` after an overwrite undoes it;
  - reducers shipped to remote tasks, where an older remote task computes the view `when` routes on
    differently;
  - refs, since `reduce(nil, d)` is not `d` for `append` of a single value, so a ref to the writer's
    `changes` resolves to the bare delta.
- **No `Append`-style helper.** On the trunk it would be a plain read-append-write; in a branch, on a reduced
  field, it would contribute the whole history plus the item, duplicating the history at the fan-in. The carrier
  cannot tell a branch from the trunk.

`Graph.Validate` rejects a reducer name that is not one of the `Reducer` constants, so a misspelled reducer fails
at `Create` rather than at the first fan-in.

**forEach element injection:** the current element is injected into `state` only (under `as`), not `changes`, so it is
available to the task but does not participate in fan-in merge.

**Delete is a cleared entry in `changes`, and the merge has two modes for it, split across two `State`
methods.** `flow.Del` writes a Go-nil tombstone into `changes` and `Set(k, nil)` writes the bytes `null`; a
delete read back from a column is `null` too. `isCleared` treats both spellings as cleared - `dwarf` conflates
null and absent everywhere, so there is no way to store a literal null value. `State.Merge`/`MergeReduce`
are the **accumulation** primitive: a cleared key is **preserved** (the replace path stores the tombstone; a
branch's contribution to a reducer-managed field *ignores* the clear - see below), so a changes delta can be built
up across attempts/members with the pending-delete marker intact. `State.DelNils` is the **materialization** step: it drops every cleared
key, so a delete never survives as a `"k": null` tombstone in materialized state - which is what
`final_state`/`FlowOutcome.State`/`Snapshot` expose to the host, and what the next step's `state` column becomes.
So *materialize* = `Merge`/`MergeReduce` then `DelNils`; *accumulate* = `Merge` alone. `processStep`'s changes
accumulation (folding a task's fresh output onto a prior attempt's `changes` before persisting the `changes`
column) uses `Merge` **without** `DelNils`, because there the null must be **preserved** - it only enacts the
delete when `changes` later folds onto `state` (a don't-materialize-here note is at that site in `execution.go`).

**A branch's cleared contribution to a reducer-managed field is the reducer's IDENTITY - ignored, not dropped.**
`MergeReduce` skips a cleared value on a reduced field (`isCleared` check before the fold), leaving the
accumulator untouched, so a branch that `flow.Del`s a reduced field clears it in its own view only and never wipes
the cohort's contributions - order-independent. The same holds for a cleared field in `Continue`'s
`additionalState`. Anywhere else a delete replaces like any write: a trunk step's or the fan-out source's `Del`
removes the field (the source's delete leaves the fan-in to fold the branches onto nothing). Pinned by
`fixtures/reducerdeleteflow_test.go` and `fixtures/terminalreducerflow_test.go`.

### `State` stores every value as JSON, and decodes only what is read

`State` wraps `map[string]json.RawMessage` in a struct. A field is held as its JSON encoding and decoded
only when something reads it. A step's `state` is a full input snapshot, so most fields on most steps are
carried rather than read, and decoding a carried field costs several times its byte length for the whole
duration of the host call - the phase where a worker holds no connection and the crew may grow to thousands.

**Measured.** Interleaved 3-rep A/B per row, alternating binaries against one database, structured 64KB
payload, 200ms tasks, concurrency 16:

| shape | carriers in flight | heap mean, decoded -> JSON-stored | heap peak |
|---|---|---|---|
| linear, D=6 | 14 | 32.2 -> 26.6 MB (**-18%**) | 52.2 -> 47.0 MB (-10%) |
| fan-out, width 16 | 83 | 98.8 -> 27.8 MB (**-72%**) | 194.7 -> 49.8 MB (-74%) |
| fan-out, width 64 | 131 | 281.2 -> 21.7 MB (**-92%**) | 521.9 -> 36.4 MB (-93%) |

**Read the columns, not the percentages: the stored-as-JSON arm is FLAT (26.6 / 27.8 / 21.7 MB) while the
decoded arm grows with the carrier count.** The N-carriers multiplier is not reduced, it is GONE - which is
why the percentage climbs with width rather than settling. A decoded width-64 fan-out peaked at **522 MB**
for one replica on one workload; that is the ceiling this removes.

**Throughput is not measurable on this rig and must not be quoted.** It tracks RTT (rho ~ -0.90, see
`bench/CLAUDE.md`): across arms, every apparent win or loss lined up with an RTT difference, and comparing
only the RTT-matched reps leaves everything inside noise.

### Why the storage is UNIFORM rather than either-or

Every field is held as JSON; there is no per-field choice between a decoded Go value and JSON. **Do not add a
decoded slot beside the JSON one.** A per-field either-or representation measures the same - the
carried-payload win is captured once carried fields are not decoded, and an A/B between the two came out
neutral on every axis (RTT-matched: heap +3.9%, throughput +2.7%, both noise) - so uniformity is chosen for
what it removes:

- **Every extra tombstone spelling is one more for a cleared-check to miss.** A tombstone already has two: a
  Go nil (`deleteOne` and `Flow.Clear` write `changes.d[k] = nil`) and the bytes `null` (`Set(k, nil)`, and
  any delete read back from a column). `encoding/json` marshals a nil `json.RawMessage` as `null`, so both
  persist as one value, and `isCleared` recognizes both. A decoded slot adds a third (a nil `any`). A
  cleared-check that misses any spelling makes a delete read back from a `changes` column silently stop
  taking effect - `DelNils` leaves it, `Has` reports the field present, nothing logs.
- **No branch to get wrong.** With one representation no reader has to ask "is this one decoded or not",
  so no path can answer it wrongly.
- **Reads and writes each do one codec pass.** `Get` only unmarshals and `Set` only marshals; a decoded slot
  makes `Get` marshal-then-unmarshal and `Set` marshal-then-decode.

The price is stated plainly in the godoc because callers feel it: **the Go type is not preserved by a
store.** JSON has one number type, so a value stored as an `int` and read into an `any` comes back a
`float64`. A typed getter (`GetInt`, ...) or `Get` with a typed target returns the type asked for; only an
untyped read is float64-domain. This is a deliberate trade of that intuition for the memory above, and it
is why some tests assert `float64` where a reader might expect `int`.

### The constraints that hold this up

1. **JSON storage normalizes the Go TYPE - not byte order.** `Set` stores a value as its `json.Marshal`
   output, so a struct contributed by a caller comes back from any untyped read (and from `decodeValue`,
   which feeds the reducers) as `map[string]any`/`float64`, exactly as a column read does. That matters
   because `union` dedupes with `reflect.DeepEqual`, and `DeepEqual` between a struct and its decoded twin is
   **false** - the mechanism behind the `Continue`-`additionalState` bug. Key ORDER is not part of it: a
   deliberately unsorted spelling dedupes correctly against a sorted one. Pinned by
   `TestState_SetNormalizesGoType`. Any value path that hands a reducer a caller's Go value without going
   through stored JSON reopens it. `NewState` holds every field as JSON too: its variadic pairs go through
   `Set`, and a single map/struct argument is marshalled once and split shallowly into fields, each kept as
   the encoder wrote it.
2. **`[]byte` and `json.RawMessage` are distinguished by TYPE, not by method name.** `Set` stores a
   `[]byte` base64-encoded (right for binary) and a `json.RawMessage` as JSON. `SetJSON`/`GetJSON` are the
   explicit pair, typed on `json.RawMessage` so the compiler keeps a call site honest. `SetJSON` does not
   validate and returns nothing - a method with no error return cannot be reporting one; `Set` validates a
   `RawMessage` and can tell you. **`Set` COPIES a `json.RawMessage` (`bytes.Clone`); `SetJSON` keeps the
   caller's slice.** A stored value is shared by `Clone` and every fan-out branch on the strength of being
   immutable, so a caller reusing its buffer after `Set` must not be able to rewrite it (pinned by
   `TestState_SetCopiesRawMessage`); `SetJSON` is for bytes the caller hands over for good.
3. **A combining reducer decodes and re-encodes, so a cohort folds in ONE pass.** `MergeReduceAll`, not
   `MergeReduce` per member: the accumulator grows as it folds, so re-encoding it per member is quadratic in
   accumulated bytes - roughly 500x the element encodes at width 1,000. The replace path (the default, and
   the overwhelming majority of fields) moves bytes and pays none of this; `Merge` is a plain map copy.
   `MergeReduce` is `MergeReduceAll` of one member, so there is one fold to keep correct. Pinned by
   `TestState_MergeReduceAllMatchesPerMember`, which asserts the one-pass and per-member results are
   byte-identical, so the optimization can never quietly change a fan-in outcome. **On a reducer error the
   receiver is left partly merged and must be discarded** - every caller propagates the error and drops the
   state, so nothing restores a consistent partial result.
4. **Reads do not memoize.** Decoding on read and writing the result back would make a READ a map write, and
   a `State` is read concurrently in at least one place (baggage on a context), where that is an
   unrecoverable runtime **throw** `errors.CatchPanic` cannot contain. Two reads of a field therefore yield
   independent values, which is also what makes sharing one stored value across fan-out branches safe.
5. **`Has` and `IsDeleted` are different questions.** `Has` = present and holding a value; `IsDeleted` =
   present as a delete. A delta is the only place the difference is normally observable, and the mint needs
   the union of the two (`mentions` in `internal/staterefs`) - with `Has` alone it silently drops carried
   fields.

**Stored bytes are NOT canonical, and nothing compares them.** `Set` keeps what `json.Marshal` writes (a
map's keys sorted, a struct's in declaration order), a `json.RawMessage` and `SetJSON` keep the caller's
bytes as given, and a column read keeps the database's spelling (JSONB/JSON re-space and reorder). Every
comparison that exists decodes first - `union` over decoded values. A
byte-comparing `union` (the standing plan in the root `CLAUDE.md` landmine) therefore cannot assume canonical
input: it must canonicalize what it compares, or every write path above must be made canonical first.

**A blob payload cannot measure any of this.** A JSON string decodes to a Go string of about the same size,
so an opaque payload expands ~1x and a measurement taken with one reads flat however state is represented.
Structured data is where the expansion lives: 800 small records (50.3KB of JSON) occupy 340.7KB decoded -
**6.8x** - because every element costs an interface, a boxed float64 and a map entry. Element count drives
it, not byte count. See `bench/CLAUDE.md`.

### Numbers are float64-domain; two unstorable values are a KNOWN, UNGUARDED punt

**Invariant: every number the engine itself produces round-trips exactly through a `float64`.** State,
baggage and payloads are `State` values held as JSON, and `encoding/json` decodes a JSON number into a
**`float64`** whenever the target is an `any` - exact for integers only up to **2^53**. This is what lets every
untyped reader - `Get` into an `any`, `Parse` into a map - treat numbers as `float64` without qualification, and it holds by construction for every value
dwarf derives (a `ReducerAdd`/`min`/`max` sum stores and round-trips fine even past 2^53). Do **not** "fix"
the read side with `UseNumber`: it changes the Go type every state-map reader sees
(`outcome.State["x"].(float64)` stops matching for an integral value; JSON has one number type, so no scheme
preserves the writer's Go type through the database), needs a reflect-walker for caller-supplied targets, and
buys exactness for a value an author can carry losslessly as a string anyway.

**Two *external/authored* values are unstorable and are NOT guarded - a deliberate, backlogged punt.**
Nothing on the write side checks them: not `Flow.Set`/`set`/`SetChanges`/`Interrupt`/`Subgraph`, and not the
engine's `Create`/`Continue`/`Fork`/`resume` ingress. Both fail *silently*, which is why the punt is a real
exposure to revisit, not a non-issue:

- **Integer beyond ±2^53.** A read into an `any` comes back **rounded** (`1234567890123456789` ->
  `1234567890123456800`), so a task that reads one acts on the wrong number and nothing errors. Only
  **integer-shaped** literals are affected - a fractional/exponent number is float64-domain at any magnitude
  (`1e300` is fine). **Workaround: carry a large id as a string** (the `id_str` pattern 64-bit-id APIs
  already publish).

  **What is STORED does not round.** Raw field storage means a carried field is never decoded, so it
  round-trips byte-exact into the next step's `state`, `final_state`, a `Fork` and a `Continue`; and because
  a read does not memoize, even reading one leaves storage exact. Verified: carried
  `{"id":1234567890123456789}` marshals back identically before and after a read. **Do not decode carried
  fields at dispatch:** that stores `...800` from the first hop onward. So the damage is confined to the Go
  value a reader receives, and does not propagate.

  Two consequences worth holding. **The rounding is per-READER, not per-field**: a plain `Get` into an `any`
  (and `Parse` into a map) rounds, while `GetInt` does not - it unmarshals
  straight into an `int`, which is exact at any int64 magnitude - and neither does a `when` expression: it
  compares a >2^53 id as an integer rather than through a float64 (`boolexp` v1.2+ normalizes a JSON
  integer literal to an exact `int64` instead of decoding it through `float64`).
- **NUL (`U+0000`) in a string.** Valid UTF-8, legal JSON, but **PostgreSQL's `JSONB` rejects it**
  (`SQLSTATE 22P05`) while MySQL/SQLite/SQL Server accept it - so it passes the SQLite test suite and kills the
  flow on the recommended production database: the write that carries it is the step's own **completion
  UPDATE** (`execution.go`), which, left to lease recovery, would leave the step `running` with `error=""`
  and re-execute the task forever (reproduced against real Postgres). **Workaround: base64-encode binary
  data.** (Invalid UTF-8 needs no guard - `encoding/json` coerces it to `U+FFFD` on marshal.) `persist.go`'s
  "ask the database" classifier turns that rejected payload write into a clean step failure instead, and
  `sanitizeErrorMessage` strips control bytes from the *error-text* write so the failure itself can land - so
  a NUL is a clean failure at write time on Postgres and silently stored on the others: not benign.

If a guard is ever reintroduced, it must run on the RAW marshaled bytes *before* the decode (the decode rounds
a >2^53 integer to `float64`, so checking the decoded value is too late) and must NOT re-check the engine's own
derived sums (a legitimate `ReducerAdd` result past 2^53 marshals integer-shaped and would be falsely rejected).

### The typed getters panic on a type mismatch (and that is the safe option here)

`GetString`/`GetInt`/`GetFloat`/`GetBool`/`GetDuration`/`GetStrings` **panic** when the key holds a value of the
wrong type (the typed getters on `State` in `state.go` delegate to `Get` and `panic(errors.Trace(err))` on its
error; `Flow`'s getters forward to them). **Do not discard that error and return the zero value:** it makes
*absent*, *cleared*, and *wrong-typed* indistinguishable, and the zero value is an actively dangerous answer -
`GetInt("retryAfter")` over a `1.5` (a fractional number from an upstream task, a host's `initialState`, or
`ReducerAdd`) reads as `0`, and the task builds a zero-delay retry loop against its downstream. `Has` returns
`true`, so the usual guard does not help.

Panicking is the right disposition *because of the engine's panic isolation*, not despite it: `ExecuteTask` is
wrapped in `errors.CatchPanic` at the call boundary, so the panic becomes the call's error and takes the normal
disposition - routed to `onError` if the graph has one, else `failStep` - with a stack trace attached. It is a
clean, immediate, correctly-attributed step failure, not a crash. (This is the *boundary* catch, which releases
the lease; the coarse worker-loop catch would have wedged the step. See "Host-call panic isolation" in
`engine/CLAUDE.md`.)

The rejected alternative was two-valued getters (`(T, error)`). `f.GetString("x")` is used inline everywhere
(conditions, arguments, struct literals), so it would force a temp var at every call site and the realistic
outcome is `v, _ := f.GetInt(…)` - the same silent zero with more ceremony. **`Get(key, target) (ok, err)` already is
the two-valued form**, and is the documented escape hatch for a task that wants to *handle* a mistyped field.

Absent and cleared keys are **not** a mismatch - they still yield the zero value, so the optional-field idiom is
untouched. Only the typed getters panic; the engine's own `when`-expression evaluation does not go through them.
Pinned by `TestFlow_TypedGetterPanicsOnTypeMismatch` (workflow) and
`fixtures/panicflow_test.go` (end-to-end: a mistyped read fails the step and routes to `onError`, and the task is
proven not to have proceeded past the read).

### The `Flow` carrier is single-owner and carries no lock

A task owns its `Flow` exclusively for its execution; `Flow` is a bare struct holding two `State` values
(`state` and `changes`, each a reference to a map) with no mutex, and **deliberately so** - a mutex would paper over a misuse pattern (concurrent writes with
no defined ordering) rather than prevent it, and the carrier is single-owner by design.

The failure mode is why this is documented rather than merely implied: two goroutines writing a `Flow`
trip the Go runtime's concurrent-map-write detector, which is a **`throw`, not a panic**. `errors.CatchPanic`
around `ExecuteTask` (the host-call panic isolation) **cannot recover it**, so one task fanning out
internally with an errgroup and writing results straight back to the `Flow` takes down the whole replica and
every unrelated in-flight flow on it. The godoc on `Flow` (and the package doc) says so, with the
collect-then-write-from-one-goroutine pattern spelled out. `Flow.UnmarshalJSON` initializes both `State`s even
when the wire omits them, because the setters write to both and a nil map there panics. Parallelizing across *steps* (a `forEach`
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
  caller's children). **The rewind is guarded on `status='running'`, and the reap runs only if it fires:** a `Terminate`
  landing mid-task terminalizes this running step (`terminated`) before the task returns and arms the retry, so an
  unguarded rewind would revive the immutable terminated step to `pending` (with a far backoff `not_before` - a transient
  zombie the terminal-flow check only clears minutes later) *and* reap the now-terminal tree's children (inert, but
  belonging to a terminal flow - an immutability violation). `processStep` therefore rewinds first under the guard and
  reaps/re-dispatches only when it actually rewound a still-running step; a lost guard leaves the step terminated (its
  children already terminated by the `Terminate` cascade) and returns. (A graceful `Cancel` does not interfere: it only
  marks the step, and the rewound step is preempted at its next claim.) This is the enforcement behind "`flow.Retry` rewinds
  a step in place but only while the flow is `running`" (see root "Terminal flows are immutable"). Defense in depth:
  History's `loadSubgraphChildren` keeps only the latest child per caller step (`ORDER BY flow_id`, last row wins =
  highest `flow_id`), matching `completeSurgraphFlow`/wedge/`Continue`, so even a stray dangling child never renders. `flow.Retry` carries no condition - the task writes the retryable condition explicitly in the surrounding `if`
  (retry-on-any-error is usually wrong). To retry only on a timeout, gate on
  `errors.StatusCode(err) == http.StatusRequestTimeout`.
- **No jitter on retry backoff:** the worker pool already throttles per-replica concurrency, so simultaneous retries
  queue in the pool rather than overwhelm downstream. Jitter would add latency for no throughput benefit.
- **`flow.Sleep(duration)`** - delay the *next* step's execution by setting its `not_before`. There is no timer:
  the step is invisible to selection until `not_before` passes, and the next scheduling cycle after that picks it
  up. In fan-out, only the last sibling's sleep affects the fan-in point.
- **`flow.Goto(target)`** - override transition routing: skip normal evaluation and follow the `goto` transition
  to `target`, if registered. Goto transitions are never taken during normal evaluation.
- **`flow.Interrupt(payload, out) (yield bool, err error)`** - pause and park the flow, or return the resume data
  once it has arrived. On the first call the payload is stored in `interrupt_payload` and propagated up the surgraph
  chain, and `yield` is true: the task returns at once. On re-dispatch after `Resume` it unmarshals the resume data
  into `out` with `yield` false and does not re-arm. The engine sets the flow `interrupted`; a caller wanting to be
  told uses `Await` or an orchestrating workflow (the engine has no stop callback).

**Single-park guard.** A step parks at most once - interrupt XOR subgraph, never both and never the other kind on
re-entry. `processStep` enforces this after the task returns: a competing-signals check fails the step if more than
one control signal is set in one dispatch, and a second check fails the step when the returned flow arms a park while
the step row's materialized `interrupt_done`/`subgraph_done` shows the *other* kind already resolved. The `workflow`
package's parkers already reject a conflicting second park at the call site; this guard is the trust boundary for an
untrusted returned flow.

## Flow Rendering (`workflow.FlowRenderer`)

`FlowRenderer` produces a Mermaid flowchart from a `History` result. Diagnostic intent: answer "where did the time go
in this flow?" at a glance. Defaults render top-down; `With*Colors` swap palettes; `WithLinks` enables per-node click
directives. The engine has no rendering method of its own: a caller renders `History`'s result, choosing the options.

### CSS-variable theming model

Color values flow through `classDef`/`style` directives; the renderer emits no themeVariables init block. Callers pass
either hex literals (static rendering) or CSS custom-property references like `var(--primary-container)` (host pages
that track a page-level theme). With `var()` values, browsers resolve through the SVG's CSS cascade and the diagram
re-colors on light/dark toggle without reinvocation. The CSS-mode pattern: pass `"currentColor"` for fill and `""`
for text - the generated classDef omits `color:`, host CSS sets it, and Mermaid inherits via `currentColor`.

### Color knobs and status groups

| Pair | Statuses |
|---|---|
| primary | `completed`, `running` (running gets a dashed border), every edge's stroke, and the subgraph block fill (at 6% opacity) |
| secondary | `pending` (and a step with no status) + the `_start`/`_end` terminals |
| error | `failed`, `terminated`, `cancelled` |
| attention | `interrupted` (distinct from error - "needs human eyes," not hard failure) |

Fan-out cohort wrappers take no color at all (`fill:none,stroke:none`); they are layout containers only.

### DAG-edge model

The execution DAG is reconstructed from `PredecessorKey`/`SuccessorKey`, NOT `step_depth`. Every edge is recorded on at
least one endpoint - fan-out via each child's `PredecessorKey`, fan-in via each cohort exit's `SuccessorKey`, linear on
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

Two-or-more steps sharing one `PredecessorKey` get wrapped in an invisible Mermaid `subgraph` block (empty label, no
fill, no stroke) - purely a layout container so siblings cluster near their parent. Edges always go between actual
task nodes; nothing terminates at the cohort wrapper.

### Truncation in label formatting

`flowFormatDuration` uses integer-millisecond truncation for sub-second values (`%dms`). A diagram with N labeled edges
accumulates up to ~N/2 ms of systematic underestimation in any path sum - diagnostically irrelevant (the goal is to
spot where time went, not reconcile to the millisecond).

### Mermaid escaping (author-controlled strings)

Task names, workflow/subgraph names, and `when`-expressions are **author-controlled** and land in a
diagram a host renders in a UI, so every one of them passes through the single `escapeMermaid` helper (defined in
`flowrenderer.go`, shared by both renderers). **Go `%q` is not Mermaid escaping** and must never be used for these:
Mermaid honors no backslash escaping inside a label, so a raw `"` ends the label and lets the rest inject
node/edge/`click` syntax, and `<`/`>`/`` ` `` pass straight through into the label's HTML rendering (an XSS/markup
vector). `escapeMermaid` rewrites `" ' < > `` ` `` [ ] { } ( ) | #` to their Mermaid character references (`#quot;`,
`#lt;`, `#124;`, …), each rendering as the literal glyph; `#` is escaped **first** so an input already shaped like
`#quot;` cannot survive as a live reference (`strings.NewReplacer` is a single non-overlapping pass). Every sink
supplies its own surrounding quotes (`["%s"]`, `-->|"%s"|`, `subgraph id ["%s"]`) and escapes only the
content. Neither renderer draws a title: the page that shows a diagram labels it. Engine-generated edge
labels (durations) are escaped too, harmlessly, so no sink is a `%q` exception a future edit could cargo-cult. Pinned
by `TestEscapeMermaid`, `TestGraph_MermaidEscapesInjection`, `TestFlow_MermaidEscapesInjection`.

## Fuzz targets (`fuzz_test.go`)

Six targets here and two in `internal/keys` (`FuzzParseFlowKey`, `FuzzParseStepKey`), all pure - no database.
Plain `go test` runs every seed and every saved corpus entry under `testdata/fuzz/<Target>/`, so a found input
stays a regression test on every push. What `go test` never runs is **discovery**.

**Discovery is run by hand, not on a schedule, and that is a decision.** Its finds concentrate in the first
minutes on new or changed code, and the fuzzed surfaces (key parsing, reducers, state merge, Mermaid escaping,
`when` expressions, the graph and flow wire formats) change rarely - a nightly job would mostly re-fuzz code
nobody touched, and needs someone to triage its failures. Do not add discovery to the push workflow either: a
time-boxed fuzz run fails nondeterministically, on pushes that did not cause it.

Run it when it can find something - after changing a fuzzed surface, or after bumping `boolexp` - one target
per invocation:

```
go test ./workflow -run '^$' -fuzz '^FuzzGraphJSON$' -fuzztime 2m
```

A failure writes the input under `testdata/fuzz/<Target>/`. Commit it with the fix: it becomes a permanent
seed. A target whose failure mode is a hang (`FuzzWhenExpression`, `FuzzGraphJSON`) runs the call under a 3s
watchdog so a hang fails the test instead of stalling it; keep that on any target reaching `boolexp`.
