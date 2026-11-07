# Dwarf `workflow` package — carrier, state, and rendering

> Load when: editing the `workflow.Flow` carrier, state/merge semantics, control signals, or the Mermaid renderer.
> Coupled with: root `CLAUDE.md` — the engine dispatch side implements much of what these describe.

### Task self-identity on the carrier

`Flow.FlowKey()` and `Flow.StepKey()` return the task's own flow/step keys (`{shard}-{hour}-{flowID}-{token}` and
`{shard}-{hour}-{flowID}-{stepID}-{token}`), populated by the
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
the *prior* attempt's, so the write never reaches the next step's `state` nor `final_state`. A task writes with `Set` and the typed setters. `RawFlow.SetRawState` is the engine's own seeding
path, not task-facing.

**A task writes field by field; do not add a typed-struct writer** (the write-side twin of `Parse`). Marshalling
the struct decides which fields are written, so an `omitzero`/`omitempty` field left at zero is never written and
a task cannot reset it to `false`, `0` or nil; a host's generated wrappers write each output with `Set`. Nor may a
writer diff against a snapshot: a diff drops a branch's contribution that happens to equal the accumulated value
(`total` 5 under `add`, contribution 5).

**A step's runs compose:** on `flow.Retry()`, a resumed `Interrupt` or a `Subgraph` re-entry, the re-dispatched
carrier's view is `state` with the earlier runs' `changes` folded in, and its `changes` are seeded with them, so
the run's writes compose onto theirs and the carrier returns the step's WHOLE delta. The engine persists that
delta as it is. **Never fold the earlier runs' changes onto the carrier's returned changes again**: that counts
them twice, which a replay (whose carrier is the recorded changes) makes visible at once - pinned by the
`reduced` case of `TestOrphanSweep_ReplaysSlowWorkersStep`. `Resume` does **not** mutate `state`: it writes the
caller's data to `resume_data`, which `flow.Interrupt` returns on re-dispatch.

**On a reduced field every write is a delta, folded through the field's reducer at every hand-off** - the
successor's state, the fan-out source's base, the fan-in, the final state, `Continue`'s `additionalState`.
A read returns the combined value. So a task writes only its contribution, wherever it runs:
`f.Set("messages", []any{m})` under `ReducerAppend`. A read-modify-write (`Set(k, append(Get(k), m))`) adds the
history again, and that is the contract, not a bug - pinned by `terminalreducerflow`'s
`read_modify_write_counts_the_input_twice`. Because every hand-off folds, the outcome does not depend on graph
shape: a do-nothing step changes nothing (`padding_does_not_change_the_outcome`).

Three alternatives were weighed against this rule and lost; do not re-propose them without new evidence:
- **A branch's whole OUTPUT as its contribution** (instead of its writes). Consistent when nested, but one level
  of `append` over base `[a]`, with branches writing `x` and `y`, gives `[a, x, a, y]` where everyone expects
  `[a, x, y]` - and wrapping a step in a one-item `forEach` would change the result.
- **A branch as ONE contributor** (writes replace inside a branch; its net addition over what it was handed is its
  contribution). Every reducer then needs its own "net addition" (a suffix for `append`, a difference for `add`,
  undefined for `min`/`max`), and a task reads a reduced field differently inside a branch than outside it.
- **A re-run starts from the step's original input, and only its last run's writes count.** Safer against a
  `Set` placed before `Interrupt`/`Subgraph`, but it loses the attempt log (one entry per `Retry`) and a task can
  no longer see what its earlier runs wrote. The ordering rule is documented instead (`docs/tasks.md`).

**The carrier (`flow.go`): every write lands in both `changes` and the view.** A `Set` on a field with no
reducer folds nothing, so its encoded bytes go into both directly (`Changes.setPlain`, the rule `contribute`
applies to such a field); every other write - a reduced field's `Set`, `Overwrite`, `Del` - is a one-field
`Changes`, composed into `changes` and applied to the view (`Changes.Compose`, `State.Apply`) through the
graph's reducers. The engine hands the carrier those
reducers at every dispatch (`RawFlow.SetReducers`, the `reducers` wire field); nothing stores them, and the
reducers a remote host returns are never read back. `Changes` holds each field it writes in one of two maps,
never both: `contributions`, combined through the field's reducer, and `overwrites` - `Overwrite(k, v)`, and
`Del(k)`, which is `Overwrite(k, nil)`. The traps the always-reduce model has to answer, each pinned:

- **Plain input carries no overwrite.** `Create`'s input, `Continue`'s `additionalState` (`ChangesOf`), Fork's
  overrides and a subgraph's `in` are plain values; only a task's `Changes` can overwrite a reduced field.
- **An overwrite is STICKY within the step.** A later `Set` folds onto the overwritten value and the field stays
  an overwrite (`Changes.contribute`; `TestChanges_ComposeTable`, `TestFlow_OverwriteAndDelThenSet`). Treating a
  later `Set` as a fresh contribution undoes the overwrite - "trim to 5, then append" kept everything.
- **Routing reads the ENGINE's fold, never the carrier's returned view** (`evaluateTransitions` takes the
  successor base). A remote task is untrusted and may compute its view differently.
- **A folded value exists in no row**, so a ref must never anchor it at the step that folded it: not its
  `state` (before the fold) nor its `changes` (the contribution). Under `append` the engine stores it as a list
  of the rows holding its elements; otherwise inline in the successor's state (`engine/CLAUDE.md`, "A fold's
  result is in no row"). Never assume `reduce(nil, d) == d` - false for `append` of a single
  value and for `union` with duplicates.

**A delete is an overwrite with a null value, and a materialized state holds no deletes.** `State.Apply` enacts
it. `Changes.Set` turns a nil on a plain field into such a delete and drops a nil on a reduced one. A null
CONTRIBUTION can still arrive - from `ChangesOf` of plain input and from a remote host: it deletes a field with
no reducer and is the reducer's IDENTITY on a reduced field. That
identity is what lets a task that collected nothing into a nil slice write it safely - at the JSON level a nil
slice and a bare nil cannot be told apart. `isCleared` therefore still recognizes both null spellings (a Go-nil
`json.RawMessage` and the bytes `null`).

**Overwriting or deleting a reduced field inside a fan-out branch fails the step**, at any depth: the branches
are parallel, and a reset in one has no defined meaning for the others. The carrier refuses `Overwrite` when
told it is in a branch (`RawFlow.SetInBranch`, carried on the wire), and the engine re-checks every result
(`checkChanges`), since `Del` cannot return an error and a remote task is untrusted. A field with no reducer
may be deleted in a branch; it folds in branch order. Pinned by `fixtures/reducerdeleteflow_test.go` and
`TestChanges_RemoteResultIsChecked`.

### `Changes` is ONE JSON object, an overwrite named with the `!` suffix

`Changes.MarshalJSON` writes a contribution under the field's name and an overwrite under `name!`
(`OverwriteSuffix`); a delete is `"name!": null`. That one object is the `changes` column, the carrier's
`changes` wire field, and a remote host's result. `State`'s JSON stays plain data.

- **The suffix is reserved in the names a task WRITES.** `Changes.Set`/`Overwrite` and `Flow.Set`/`Overwrite`
  return an error for a name ending in `!`, `Flow.Del` panics (the host-call boundary turns it into a step
  failure), and the engine re-checks a returned result (`checkChanges`), because a remote host can send `x!!`.
  Graph-declared names cannot contain `!`. A plain state - input, a step's `state`, `final_state` - may still
  hold such a name as data.
- **A row with no `!` key reads as all contributions.** If both spellings of a field appear, the overwrite
  wins.
- **Every reader of a `changes` column decodes it as `Changes`** (`changesOf` in the engine). A plain `State`
  decode reads `name!` as a field of its own. The state-refs anchor lookup is the one exception, reading the raw
  object and trying `field!` before `field`: an anchored field is one the step replaced, so it is under one of
  the two. `FlowStep.Changes` stays a `State`, showing an overwrite as its value and a delete as a null.
- **Do not move overwrites into a column, wire field or `State` property of their own.** Each is a second
  thing every writer and reader of `changes` must carry, nothing fails to compile when one misses it, and a miss
  silently turns an overwrite into a contribution. In one object, a writer that stores `changes` stores both.

### An addition: `name+`, the elements a list gained

`Changes` holds a third kind, written under `name+` (`AdditionSuffix`): the elements appended to the array the
field held in the step's input. Only the engine records one (`recordAdditions`, before storing a step's changes):
a write to a field with no reducer whose new value begins with the whole input array. So a loop that rewrites a
growing list each step stores each element once. Applying the Changes onto that input gives the value written.

- **It is relative to the step's OWN input**, so it is correct only where a step's changes are applied onto its own
  state: the successor base, a re-run's view, the final state's tails, the spawn's base at a fan-in. The engine
  records none in a fan-out cohort, where the fan-in applies a member's changes onto the spawn's state and a replace
  must win outright; `checkChanges` refuses one there, or on a reduced field.
- **Compose:** a later replace or overwrite drops it; a later addition joins it; an addition onto an earlier write
  extends that write (`Changes.add`). `Apply` appends it to the field's current value.
- **`+` is reserved in the names a task writes**, like `!`, and the anchor lookup probes `field+` only for a list
  entry reading the changes column - a whole-field ref never resolves to the elements a step gained.
- **It reaches a task only on a re-run** (retry, resume, subgraph return), in the earlier runs' changes the carrier
  is seeded with; the task's own `Set` replaces it. Elements compare by bytes, else decoded and re-encoded, since a
  database reorders and respaces what it stores; a number spelled two ways, or a task that does not round-trip
  elements faithfully, gets no addition, and nothing depends on getting one. Pinned by `TestRecordAdditions`
  (engine) and `TestChanges_AdditionsApplyAndCompose`.

`Graph.Validate` rejects a reducer name that is not one of the `Reducer` constants, so a misspelled reducer fails
at `Create` rather than at the first fold.

**forEach element injection:** the current element is injected into `state` only (under `as`), not `changes`, so it is
available to the task but does not participate in fan-in merge.

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
carried-payload win is captured once carried fields are not decoded, and an RTT-matched A/B between the two
came out within noise on heap and throughput - so uniformity is chosen for what it removes:

- **Every extra null spelling is one more for a cleared-check to miss.** A null value already has two: a Go
  nil `json.RawMessage` and the bytes `null` (a `Set(k, nil)`, or a value read back from a column).
  `encoding/json` marshals a nil `json.RawMessage` as `null`, so both persist as one value, and `isCleared`
  recognizes both. A decoded slot adds a third (a nil `any`). A cleared-check that misses any spelling makes a
  null read back from a column stop meaning "nothing" - `Has` reports the field present, nothing logs.
- **No branch to get wrong.** With one representation no reader has to ask "is this one decoded or not",
  so no path can answer it wrongly.
- **Reads and writes each do one codec pass.** `Get` only unmarshals and `Set` only marshals; a decoded slot
  makes `Get` marshal-then-unmarshal and `Set` marshal-then-decode.

The price is stated plainly in the godoc because callers feel it: **the Go type is not preserved by a
store.** JSON has one number type, so a value stored as an `int` and read into an `any` comes back a
`float64`. A typed getter (`GetInt`, ...) or `Get` with a typed target returns the type asked for; only an
untyped read is float64-domain. This is a deliberate trade of that intuition for the memory above, and it
is why some tests assert `float64` where a reader might expect `int`.

**`State.Names` yields names only; do not add a name/value iterator.** Yielding values means materializing
every one, and a field held as JSON is decoded to be yielded, so a loop that reads two fields out of forty pays
the decode for all forty - the cost this type exists to avoid. Iterating names is free and the reads stay
explicit; `Parse` into a `map[string]any` is the escape hatch, visible at the call site.

### The constraints that hold this up

1. **JSON storage normalizes the Go TYPE - not byte order.** `Set` stores a value as its `json.Marshal`
   output, so a struct contributed by a caller comes back from any untyped read as `map[string]any`/`float64`,
   exactly as a column read does (and from `decodeValue`, which feeds the reducers, with numbers as
   `json.Number`). That matters
   because `union` dedupes with `reflect.DeepEqual` (on `unionKey`, which changes only numbers), and `DeepEqual`
   between a struct and its decoded twin is
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
3. **A combining reducer decodes and re-encodes, so a cohort folds in ONE pass.** `ApplyAll`, not
   `Apply` per member: the accumulator grows as it folds, so re-encoding it per member is quadratic in
   accumulated bytes - roughly 500x the element encodes at width 1,000. The replace path (the default, and
   the overwhelming majority of fields) moves bytes and pays none of this.
   **`append` and `concat` are not decoded at all: they splice bytes** (`appendRaw`/`concatRaw`, used by
   `ApplyAll` and `Changes`' fold). They are the reducers a long history or transcript sits under, and every
   step that adds a message pays the fold twice (the carrier's `Set` and the successor base). **Do not fold
   them by decoding:** a decoding fold costs time and allocation in proportion to the accumulated history on
   every fold, while the splice costs a fixed handful of allocations whatever the size (`BenchmarkFold_*`
   measures both). `ApplyAll` collects the pieces and joins them once, so a wide fan-in stays linear. The
   splice keeps every element's bytes as given. Pinned by `TestAppendRaw_EqualsTheDecodingFold` and
   `TestConcatRaw_EqualsTheDecodingFold` (every input shape, escapes and the database's re-spelled whitespace
   included). `concat` splices between the quotes, which is exact because JSON escapes are self-contained -
   except a UTF-16 surrogate pair escaped across two writes, which a decoding fold turns into two U+FFFD and
   the splice reunites; Go's encoder never escapes that way. **The splice trusts its input to be well-formed**
   (`SetJSON`'s contract, stated on `ApplyAll`): a malformed base such as `[1] [2]` splices into malformed JSON
   with no error. Do not add `json.Valid` on this path, and do not settle for a first/last-byte check - `[1] [2]`
   passes it; only a full scan catches it. `union` must compare decoded values, so it
   decodes; `merge` decodes only its top level, into `json.RawMessage` entries.
   `Apply` is `ApplyAll` of one member, so there is one fold to keep correct. Pinned by
   `TestState_ApplyAllMatchesPerMember`, which asserts the one-pass and per-member results are
   byte-identical, so the optimization can never quietly change a fan-in outcome. **On a reducer error the
   receiver is left partly merged and must be discarded** - every caller propagates the error and drops the
   state, so nothing restores a consistent partial result.
4. **Reads do not memoize.** Decoding on read and writing the result back would make a READ a map write, and
   a `State` is read concurrently in at least one place (baggage on a context), where that is an
   unrecoverable runtime **throw** `errors.CatchPanic` cannot contain. Two reads of a field therefore yield
   independent values, which is also what makes sharing one stored value across fan-out branches safe.
5. **`Has` and `IsDeleted` are different questions.** `Has` = present and holding a value; `IsDeleted` =
   present as a null. A null contribution is the only place the difference is normally observable. Whether a
   STEP rewrote a field is `Changes.Mentions` - a contribution or an overwrite, a delete included - which is
   what the state-refs mint needs; `Has` alone would carry a deleted field's ref forward.

**Stored bytes are NOT canonical, and nothing compares them.** `Set` keeps what `json.Marshal` writes (a
map's keys sorted, a struct's in declaration order), a `json.RawMessage` and `SetJSON` keep the caller's
bytes as given, and a column read keeps the database's spelling (JSONB/JSON re-space and reorder). Every
comparison that exists decodes first - `union` over decoded values, with every number compared by value
(`unionKey`), since the database respells numbers too. A byte-comparing `union` (raw elements
compared instead of decoded ones) therefore cannot assume canonical input: it must canonicalize what it
compares, or every write path above must be made canonical first.

**A blob payload cannot measure any of this.** A JSON string decodes to a Go string of about the same size,
so an opaque payload expands ~1x and a measurement taken with one reads flat however state is represented.
Structured data is where the expansion lives: 800 small records (50.3KB of JSON) occupy 340.7KB decoded -
**6.8x** - because every element costs an interface, a boxed float64 and a map entry. Element count drives
it, not byte count. See the carry payload's comment in `bench/workloads.go`.

### Untyped reads are float64-domain; two unstorable values are a KNOWN, UNGUARDED punt

**Invariant: an untyped reader sees every number as a `float64`; storage keeps the literal.** State, baggage
and payloads are `State` values held as JSON, and `encoding/json` decodes a JSON number into a **`float64`**
whenever the target is an `any` - exact for integers only up to **2^53**. So every untyped reader - `Get` into an
`any`, `Parse` into a map - treats numbers as `float64` without qualification, while the stored bytes keep the
literal that was written (see "Storage is exact" below). That includes a value dwarf derives: an `add` fold of
integers is exact at any magnitude, so a sum past 2^53 is stored exactly and reads rounded through an untyped
reader (exactly through `GetInt`, within int64). Do **not** "fix"
the read side with `UseNumber`: it changes the Go type every state-map reader sees
(`outcome.State["x"].(float64)` stops matching for an integral value; JSON has one number type, so no scheme
preserves the writer's Go type through the database), needs a reflect-walker for caller-supplied targets, and
buys exactness for a value an author can carry losslessly as a string anyway.

**Two *external/authored* values are unstorable and are NOT guarded - a deliberate, backlogged punt.**
Nothing on the write side checks them: not `Flow.Set`/`set`/`Interrupt`/`Subgraph`, and not the
engine's `Create`/`Continue`/`Fork`/`resume` ingress. Both fail *silently*, which is why the punt is a real
exposure to revisit, not a non-issue:

- **Integer beyond ±2^53.** A read into an `any` comes back **rounded** (`1234567890123456789` ->
  `1234567890123456800`), so a task that reads one acts on the wrong number and nothing errors. Only
  **integer-shaped** literals are affected - a fractional/exponent number is float64-domain at any magnitude
  (`1e300` is fine). **Workaround: carry a large id as a string** (the `id_str` pattern 64-bit-id APIs
  already publish).

  **Storage is exact, carried and folded alike - except on MySQL beyond 64 bits.** Raw field storage means a
  carried field is never decoded, so it round-trips exactly into the next step's `state`, `final_state`, a
  `Fork` and a `Continue`; and because a read does not memoize, even reading one leaves storage exact. The one
  exception is the database itself: **MySQL's `JSON` stores an integer outside its 64-bit range (above 2^64-1 or
  below -2^63) as a double** (`12345678901234567890123` -> `1.2345678901234568e22`), and an exact `add` fold can
  produce such a sum. Postgres respells a literal but keeps its value; SQLite and SQL Server keep the bytes.
  **Do not decode carried fields at
  dispatch:** that stores `...800` from the first hop onward. A forEach decodes nothing either: it splits its source
  into raw elements (`engine/transitions.go`). A fold is the one place that decodes and writes
  back, so it decodes numbers as `json.Number` (`decodeValue`, and the cohort record in
  `engine/cohortfold.go`) and never reaches a reader: `add`/`min`/`max` fold integers in arbitrary precision
  (`numberApply`; a fraction or exponent folds in float64, as an untyped reader sees it), `min`/`max` keep the
  winning literal, and `merge` keeps each entry's bytes. **`union` compares numbers by value, never by literal**
  (`unionKey`): an integer literal by its exact integer, any other number by its float64 in Go's shortest decimal,
  so `1`, `1.0` and `1e0` are one element while two ids past 2^53 stay two. Do not key a whole float by its exact
  binary integer: past 2^53 that is not the decimal any writer spells (`1e23` is `99999999999999991611392`), so
  `1e+23` from Go and `100000000000000000000000` from Postgres would land twice. Do not compare literals: the database is a non-Go writer -
  Postgres JSONB writes `1e-7` back as `0.0000001` and `1e+21` in full, MySQL `1e+21` as `1e21` - so the same
  value arriving once from a column and once from a task would land twice. Pinned by `TestState_FoldsKeepNumbersExact`
  and the large-integer cases of `TestCohortFold_ProjectionEqualsContributions`.

  Two consequences worth holding. **The rounding is per-READER, not per-field**: a plain `Get` into an `any`
  (and `Parse` into a map) rounds, while `GetInt` does not - it unmarshals
  straight into an `int`, which is exact at any int64 magnitude - and neither does a `when` expression: it
  compares a >2^53 id as an integer rather than through a float64 (`boolexp` v1.2+ normalizes a JSON
  integer literal to an exact `int64` instead of decoding it through `float64`).
- **NUL (`U+0000`) in a string.** Valid UTF-8, legal JSON, but **PostgreSQL's `JSONB` rejects it**
  (`SQLSTATE 22P05`) while MySQL/SQLite/SQL Server accept it - so it passes the SQLite test suite and kills the
  flow on the recommended production database: the write that carries it is the step's own **completion
  UPDATE** (`dispatch.go`), which, left to lease recovery, would leave the step `running` with `error=""`
  and re-execute the task forever (reproduced against real Postgres). **Workaround: base64-encode binary
  data.** (Invalid UTF-8 in a Go string is coerced to `U+FFFD` on marshal; bytes stored as given - a
  `json.RawMessage` passed to `Set`, a remote host's `changes` - keep it, and the `append`/`concat` splice
  carries it forward, so Postgres rejects it the same way.) `persist.go`'s
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

A task owns its `Flow` exclusively for its execution; `Flow` is a bare struct holding a `State` and a `Changes`
(`state` and `changes`, references to maps) with no mutex, and **deliberately so** - a mutex would paper over a misuse pattern (concurrent writes with
no defined ordering) rather than prevent it, and the carrier is single-owner by design.

The failure mode is why this is documented rather than merely implied: two goroutines writing a `Flow`
trip the Go runtime's concurrent-map-write detector, which is a **`throw`, not a panic**. `errors.CatchPanic`
around `ExecuteTask` (the host-call panic isolation) **cannot recover it**, so one task fanning out
internally with an errgroup and writing results straight back to the `Flow` takes down the whole replica and
every unrelated in-flight flow on it. The godoc on `Flow` (and the package doc) says so, with the
collect-then-write-from-one-goroutine pattern spelled out. Parallelizing across *steps* (a `forEach`
transition, one `Flow` per branch) is the first-class answer.

**The zero `Flow` is writable.** Every write path (`write`, `Set`'s plain path) and `Flow.UnmarshalJSON` call
`ready`, which creates both maps, since the setters write to both and a nil map panics. A write path that skips
it panics on the zero value while its siblings work (`TestFlow_ZeroValueTakesEveryWrite`).

### The carrier's wire format (`carriers.go`)

Everything that turns a `Flow` into JSON and back lives in `carriers.go`: the wire struct (`flowJSON`), the version
rules, the duration conversion, the `Flow`'s own `MarshalJSON`/`UnmarshalJSON`, and the two direction views,
`RequestCarrier` and `ResultCarrier`.

- **The `Flow`'s own `MarshalJSON`/`UnmarshalJSON` are the wire format**, every field both ways, and stay valid for
  good: a host may carry the flow with the default `json` package, and one known host does. So
  the version check and the duration encoding live there, on the path nothing can skip.
- **The views are an optimization over the same field names**, each direction's fields and no others, so they add
  nothing to the frozen format. `RequestCarrier`'s decode clears the control signals (a request cannot arm one);
  `ResultCarrier`'s decode sets only the changes and signals, leaving the host's state, reducers, keys and attempt.
  They return the exported `Carrier` interface holding a pointer, because `json.Unmarshal` refuses a non-pointer.
- **The trust boundary is what the engine READS, not how a reply is decoded.** After `ExecuteTask` the engine reads
  only `RawChanges()` and the `...Requested` signals (`d.result` in `engine/dispatch.go`); routing folds onto its own
  input, reducers come from the graph, `attempt` from the step row. A host decoding a reply with the default
  `UnmarshalJSON` puts a forged state on the carrier, and nothing reads it. **Do not start reading anything else off
  the returned carrier** - `fixtures/carrierwireflow_test.go`'s `forged` mode (a reply forging state, reducers,
  attempt, keys and `inBranch`) pins that the outcome cannot tell.

**The version, `v`.** `CarrierVersion` is 1. Every encoding writes `v`; every decode checks it:
- **Missing: refused (400). Do not default a missing `v` to 1.** A body without `v` is an older sender's, whose
  durations are integer nanoseconds under `sleepDuration`, `backoffInitialDelay` and `backoffMaxDelay` - names
  Go's decoder ignores without a word, so read as v1 its sleep and backoff would vanish. Measured: a body carrying
  `"retry": true` and the old `backoffInitialDelay` re-ran its task 4.7 million times in 15 minutes.
- **Not a positive integer: refused (400). Newer than `CarrierVersion`: refused (501), naming both versions.**
  501 matches the engine's own "data in a newer format than this replica reads".
- **Older and still read: accepted.** N reads 1..N, as `payload_ver` does.
- **A Flow encodes in the version it was decoded with, or the newest if never decoded** (`carrierVersion`, never
  persisted). So a task server replies in its request's version and an older engine can read the reply, which
  makes "task servers first, then the engine" a clean rollout. The echo is in `Flow` itself, not only in
  `ResultCarrier`, because a task server may reply with the default encoder. `ResultCarrier`'s decode does not
  record the reply's version: the host's Flow is the engine's and stays in the newest format.
- **Introducing v2:** bump `CarrierVersion`; make every encoder write the old form for a Flow whose
  `carrierVersion` is older, failing the encode, naming the feature, when the task used something the old version
  cannot carry (never dropping it); keep every decoder reading 1..N; add golden files at both versions.

**Durations are whole milliseconds** (`sleepMillis`, `backoffInitialDelayMillis`, `backoffMaxDelayMillis`): the
engine schedules at millisecond resolution (`d.sleep.Milliseconds()`), so a finer unit carries nothing it reads,
and an integer millisecond is exact in every language (JavaScript loses integer precision past 2^53 - ~104 days in
nanoseconds, ~285,000 years in milliseconds). A sleep is truncated, as the engine truncates it. **A backoff delay is
rounded UP (`millisRoundedUp`), never truncated:** the engine truncates the delay it computes for an attempt, not
the base, so a truncated 500µs base would arrive as 0, which no multiplier grows, and the retry would run back to
back where in process it backs off to a minute. Decoding saturates at the `Duration`'s range rather than
overflowing. Timestamps stay RFC 3339 strings. Field names are case-sensitive to any other implementation, though
Go's decoder matches them case-insensitively; the golden files spell them exactly.

**The golden files under `testdata/carrier` are the conformance fixtures** (`carriers_test.go`): each direction for
a task that writes and for each control signal, the bodies every decoder refuses (`refused.json`), and each
reducer's fold (`reducers.json`). Each body is also decoded with its form's decoder and re-encoded, which must give
the same bytes (`TestCarrierGolden_Decode`), so a field a decoder drops fails there; the request bodies hold the
state as the engine sends it, with the earlier runs' changes folded in. Regenerate with
`go test ./workflow -run 'TestCarrierGolden|TestReducerGolden' -update` and review the diff as a wire change: after
v1.0, a change to an existing file is a breaking change and needs a new `CarrierVersion`. A carrier implementation
in another language reads the same files.

### Task-Initiated Control Signals

Tasks signal the engine via control methods on the `Flow` carrier (distinct from the operations above):

- **`flow.Retry(initialDelay, delayMultiplier, maxIntervalDelay, giveUpAfter) bool`** - re-execute this task with
  exponential backoff. The bound is wall-clock, not a count: returns `true` (task should return `nil`) while the next
  attempt would still land within `giveUpAfter` of the step's first creation, else `false` (task should return its
  error) - including when the next backoff delay alone would overshoot the horizon, so a doomed wait is never parked
  before failing. The give-up check is made client-side in `Retry` against `flow.StepCreatedAt()`; the engine only
  consumes the backoff shape. `Retry` refuses a negative delay or cap and a negative, NaN or infinite multiplier;
  a remote host's carrier is not bound by that, so the engine fails a step whose retry delay comes out negative
  (`TestRetry_NegativeDelayFailsTheStep`), and `backoffDelay` saturates at the largest `Duration`.
  Pass `giveUpAfter <= 0` for unlimited; to bound by count instead, pass `0` and gate on `flow.Attempt()`. The step row
  is reused. The engine tracks `attempt` and computes the re-dispatch delay `min(initialDelay * delayMultiplier^attempt,
  maxIntervalDelay)`, merging `state + changes` back into `state` so the task sees its prior output. `flow.Retry`
  rewinds the step row in place (it is task-initiated, while the flow is still `running` - distinct from `Fork`, which
  clones a *terminal* flow into a new one). **A retry clears the park
  slot** (`interrupt_done`/`subgraph_done` -> 0, `resume_data`/`subgraph_result`/`subgraph_error` -> `'{}'`),
  so a retry after a resolved `flow.Subgraph` re-runs the child and after a resolved `flow.Interrupt` re-interrupts.
  **A retry of a step that launched a subgraph reaps the prior attempt's child flow, recursively, in the same
  transaction as the rewind** (`deleteSubgraphFlowsRootedAt(stepID)`). The child is always *terminal* at retry time
  (the park resolves only on a terminal child), so this is a delete of inert rows, not a cascade-cancel of live work.
  Leaving it would make the execution DAG claim two paths (`X -> iter1 -> iter2 -> Y`) when the model is single-path,
  and let History assembly attach the discarded child's subtree to the caller. The reap is **step-scoped** (only this
  caller's children). **The rewind is guarded on `status='running'`, and the reap runs only if it fires:** a `Terminate`
  landing mid-task terminalizes this running step (`terminated`) before the task returns and arms the retry, so an
  unguarded rewind would revive the immutable terminated step to `pending` (with a far backoff `not_before` - a transient
  zombie the closed-flow check only clears minutes later) *and* reap the now-closed tree's children (inert, but
  belonging to a closed flow - an immutability violation). `processStep` therefore rewinds first under the guard and
  reaps/re-dispatches only when it actually rewound a still-running step; a lost guard leaves the step terminated (its
  children already terminated by the `Terminate` cascade) and returns. (A graceful `Cancel` does not interfere: it only
  marks the step, and the rewound step is preempted at its next claim.) This is the enforcement behind "`flow.Retry` rewinds
  a step in place but only while the flow is `running`" (see root "Closed flows are immutable"). Defense in depth:
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
