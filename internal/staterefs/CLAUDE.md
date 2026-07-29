# `internal/staterefs` — storing a large carried state field once

> Load when: editing the ref size policy, the mint, or the resolve/flatten path.
> The engine's side of this — which Linker, how an anchor is read, the byte metric — is `engine/staterefs.go`,
> and the integration rationale (where refs are flattened, how `Fork` remaps them) is in `engine/CLAUDE.md`.

## Why this exists

A step's `state` is a full input snapshot, so a field that is *carried* but not *changed* is re-serialized
into every row it survives into: D copies down a chain of depth D, and **N·D across a fan-out of N branches**.
Bytes are the binding resource for large-state workflows (a shard sustaining ~4,600 steps/s on small state
caps near ~700-900 steps/s rewriting 64KB per step, bound by the instance's disk/WAL path —
`docs/benchmark-cloud.md`). The doc-extraction shape (a document fanned out over pages, then chunks) measures
**~29x** fewer stored state bytes with refs than without.

So a field above a size bar is not written into the successor's `state` at all: it is omitted, and the refs
map records `{"<field>": <anchor step_id>}` — the step that physically holds the bytes.

## The shape: resolve once at dispatch, mint once at the successor write

Everything in between works on literals — `when` evaluation, `forEach` expansion, the task carrier, the
transport to a remote task — so the ref encoding never leaks into transition evaluation or task-facing code.
`Mint` needs **no database read** (state was already resolved, so "inlining" is just declining to omit a
field), which is what makes the write-side win free of round-trips.

## Policy prices the ANCHOR, not the field

`Resolve` fetches whole payload columns, so the cost is one row per *distinct anchor* (and, on a cache miss,
one round-trip for all of them together) while any number of fields in an already-fetched row are free. From
the measured constants (`db = k·L + s`, k ≈ 11-12, s ≈ 4.4ms; byte ceiling 46-60 MB/s), same-zone **one
round-trip costs about what ~23KB of avoided writes buys** (`budget`). Three tiers follow:

- **Free** — a field whose anchor is already in the fetch set. Every field a step could newly anchor lives in
  that *one* step's row, so all candidates share a single prospective anchor: once any one opens it, the rest
  ride along at zero extra read cost, subject only to `floor` (~1KB).
- **Fan-out** — the bar scales as `budget/N`, because the cache collapses N resolves into one miss while the
  write saving is paid N times. Linear break-even sits near 23KB (the depth cancels: cost ~D resolves, saving
  S·D), but at N=100 even a few hundred bytes pays. **Fan-out width is the primary axis, not a refinement** —
  a ~100x swing in the correct threshold, on a quantity the caller knows exactly at mint time.
- **Linear** — one field clears `threshold`, **or the per-anchor sum does**. Summing is legitimate *only*
  because candidates are co-located in one row; four 1KB fields at four *different* anchors would mean four
  whole-row overfetches for the same bytes, and the sum is never taken across anchors.

**Do not clamp `openThreshold` at `floor`.** Clamping there made the `1/N` term dead code for every fan-out
wider than `budget/floor` = 23: the bar sat at a flat 1024 at width 24 and at width 10,000 alike, so a forEach
source array stayed inline in all N branches — N copies of an N-element array, quadratic and worse under
nesting. The clamp is `minField`, which is the break-even against a refs entry's own ~26 stored bytes.

**The Postgres inflation factor is an approximation with a known failure direction, and that is why it is
gated to `n>1`.** Marshalled-JSON-text length understates what `jsonb` stores: measured 862 bytes for a
183-byte text array of 64 small ints (4.7x), against MySQL 8.4 at 197 (1.08x) and MariaDB 11.8 verbatim
(1.00x); SQL Server `VARBINARY(MAX)` and SQLite `TEXT` cannot inflate at all. String-heavy state therefore
refs ~5x more eagerly than its true cost warrants. A structural estimator was considered and rejected because
at a fan-out the bar is already tiny (73 bytes at N=64, 18 at N=256), so a 5x error moves nothing across it —
whereas the linear bar of 4096 is exactly where estimate accuracy would decide cases, so the factor never
applies there. MySQL and MariaDB share the driver name `mysql` and have genuinely different JSON engines, but
both measure ~1x, so the ambiguity is moot and no server-version probe is needed.

## Invariants that cost bugs to find

1. **Refs live only in `state`, never in `changes`.** `changes` is always a task's literal output or a
   reducer's literal result. That is what makes one refs column sufficient and the changes-shadows-state rule
   safe — there is no case where both sides are indirections.
2. **Refs are ONE HOP.** This mostly holds by construction (a ref is a few bytes, so it never trips the size
   bar and is never re-minted) — but that is a side effect of a SIZE comparison, and the fan-out policy is
   deliberately size-*independent*. So `Mint` carries an inherited ref forward unconditionally and never
   re-mints it, and `Resolve` **asserts** the invariant rather than walking a chain (walking would be the
   rejected delta design wearing a hat). Pinned by `TestLinker_ResolveAssertsOneHop`.
3. **An anchor's bytes can be in EITHER payload column**, `changes` shadowing `state`. Three places they sit,
   and only the first is the obvious one: a task produced the value → the anchor's `changes`; the flow's
   **initial input** → the **entry step's `state`** (no task produced it, so it is in no `changes` anywhere —
   and it is the headline case); a fan-in **reducer's output** → the **fan-in step's `state`** (the merged
   value exists nowhere else). A changes-only resolver silently misses the last two. Pinned by
   `TestLinker_ResolveReadsBothColumns`.
4. **A carried ref absent from `merged` must still be re-emitted.** The fan-in resolves only what its reducers
   fold, so a merely-carried ref's key is absent from `merged` and `Mint`'s main candidate loop never sees it.
   Omitting the trailing carry-forward silently dropped the field from the fan-in step onward and from
   `final_state` — a permanent loss on the common `preprocess → fan-out → fan-in` shape with a large carried
   field.
5. **`inlineExcessAnchors` must never drop an un-inlineable carried ref.** Its literal is not in `merged` to
   inline back, so dropping it would delete the field outright. Such an anchor is *pinned* past the
   `maxAnchors` cap — correctness over the perf bound.
6. **The cache holds BYTES, never decoded values — and `Resolve` now splices those bytes STRAIGHT INTO
   state rather than decoding them.** `State` holds a field as raw JSON until something reads it
   (`SetCanonicalJSON`), so resolving a ref costs the field's byte length instead of the several times that
   its decoded Go form occupies. This is where the two designs meet: refs already collapse N branches onto
   one cached copy of the bytes, and not decoding at the splice is what stops those N branches from
   re-inflating it N times anyway — the read multiplier the ref design deliberately left alone.

   Sharing one immutable byte slice across branches is safe in a way sharing a decoded value is not: bytes
   cannot be mutated in place by a task, and each branch's own read decodes into its own copy. So the
   anti-aliasing guarantee is unchanged and now rests on immutability rather than on copying.
   `TestLinker_ResolveCachesBytesNotValues` still pins it (two resolves must not share a mutable value) and
   still mutation-fails against a decoded-value cache.

## `inlineOnly` is NOT "put a nil in changes"

The mint reads two independent signals about a field and needs both:

- `changes[k]` present ⇒ **the task rewrote it**, so do not carry the inherited ref forward.
- `inlineOnly[k]` ⇒ **the bytes are in no step row**, so never ref it — carried or new.

A nil tombstone only delivers the first. The second is candidacy suppression, and it is the correctness half:
a synthesized `forEach` element (`<as>`/`<as>Index`/`<as>Count`) or a member-contributed fan-in field would
otherwise be ref'd against an anchor whose row does not hold it, and dangle. The two sets genuinely differ in
both directions — a forEach element is `inlineOnly` and *not* in `changes`; an ordinary task write is in
`changes` and *not* `inlineOnly`. There is a second reason at the forEach site: the engine's
`accumulatedChanges` map is shared across the linear mint and every branch mint, and a JSON null in a
persisted `changes` column is a tombstone `DelNils` enacts as a delete — so synthesizing nils there would leak
across branches and overload a value that already means something else.

## The Loader is per call, and batched, on purpose

`Resolve` takes a `Loader` rather than owning a connection because **two of the four resolve sites run inside
an open transaction** (the fan-in paths in `engine/execution.go`), and their read must land on that
transaction's connection. Binding a shard's `*sequel.DB` at construction would silently move those reads onto
a different connection, where they cannot see the transaction's own uncommitted writes. Anchors never cross a
flow and a flow never crosses a shard, which is what makes a same-connection read always sufficient.

It takes **every anchor at once** because the free tier is priced on exactly that: "an already-fetched row's
other fields are free" is only true if k anchors cost one round trip rather than k. A per-id loader would make
the per-anchor SUM test in `Mint` price something the reader does not do. Pinned by
`TestLinker_ResolveOneRoundTripPerBatch`.

## Two byte counts, and they are not interchangeable

`Resolve` reports the bytes it **spliced into state**; the `Loader` reports the raw **columns it scanned**.
They disagree in both directions and answer different questions, so a caller must pick deliberately:

| | Loader's count | `Resolve`'s return |
|---|---|---|
| includes the anchor's *other* fields | yes (whole rows) | no |
| on a resolve-cache hit | **zero** (nothing was read) | unchanged (the field is materialized either way) |
| answers | the database's byte throughput | what the caller now HOLDS |

`dwarf_state_read_bytes` wants the first - it tracks a throughput ceiling, so it must count whole columns and
must read zero when no read happened. The engine's held-state gauge wants the second, and the cache-hit row
is why: across a fan-out every branch resolves the same anchor set, so one miss and N-1 hits means the
Loader's count reports a large carried document as **nothing on all but the first branch** - in precisely the
measurement that exists to catch a large carried document. That asymmetry is the whole reason `Resolve`
returns a count at all; every caller but the dispatch path discards it.

## One Linker per shard

A `step_id` is a per-shard auto-increment, so the resolve cache's key is only unique within a shard — that is
why the key carries no shard number: the Linker *is* the shard scope. It also isolates cache pressure, which
matters because in a fan-out every branch resolves the SAME anchor set, so an eviction between sibling
dispatches turns one miss plus N-1 hits into many misses. Whether that eviction actually binds is an open
empirical question; the per-shard split removes one way to provoke it.

The driver is bound at construction for the same reason — it is a property of the shard's handle, and it is
the only dialect-dependent term in the policy.

## Rejected alternatives, so they are not re-proposed

- **A content-addressed side table** (`(hash, bytes)`, flow-scoped). It buys field-granular fetches (read exactly
  the value, not the anchor's whole JSON row) and cross-flow dedup. It costs a new table, a new GC path, a new
  orphan class, and a `Fork`/`Continue` story for blobs whose owning flow may be deleted. Step rows are *already*
  immutable, flow-scoped, deleted with the flow, and remapped by `Fork`'s id map — a ref into one reuses a lifecycle
  that is entirely paid for. It stays the escape hatch if **overfetch** ever binds (resolving pulls the anchor's
  whole `state + changes`, so a fat anchor row makes the reader pay for bulk nobody wanted).
- **Delta-encoded step state** (a step's `state` as a delta over its predecessor's, with a bounded lookback). It
  **cuts writes, not reads** — the task still receives fully materialized state at dispatch, so the large field is
  still read and decoded at every one of the N branch dispatches; refs plus the cache improve the read side too. And
  the cost lands on the multi-step-in-one-transaction paths (`computeFinalState`, `Cancel`'s cascade, `Fork`'s
  DB-side clone, `History`), where a trivial row read becomes a chain resolution. If it is ever revived, do **not**
  revive the bounded-lookback walk: store an anchor id plus the *cumulative* delta, so materialization is always two
  rows. Note that this is the same skeleton as refs — refs are that idea applied per *field* rather than per *state
  map*, which is what makes them one-hop and cache-friendly.
- **An inline `"pdf$ref": 1234` key instead of a separate refs column.** Rejected because `Fork` remaps ref targets
  through its clone id map while cloning rows DB-side, so an inline ref would force every large `state` blob through
  the engine to remap. Plus: unresolved state would be the same Go type as resolved state, so a missed resolve is
  silent (and its symptom — a missing field — reads as an absent optional); and a user field literally named
  `pdf$ref` becomes a collision.
- **`JSON_FIELD` extraction for resolution** (available in sequel). Deferred, not adopted: SQL Server's `JSON_VALUE`
  is `NVARCHAR(4000)` and returns NULL past that, so a large *scalar* — our exact case — reads back NULL and would
  need a whole-row fallback anyway; extraction saves wire bytes and Go-side decode but not the server-side read (a
  TOASTed `jsonb` is de-TOASTed whole before slicing); and multi-anchor gets awkward (per-row paths force a
  `UNION ALL` or a `CASE`). **It is also coupled to the free tier**: tier 1 is free *because* resolution fetches
  whole columns, so adopting per-field extraction would require re-pricing it.

## Open follow-ups

- ~~A carry workload for `bench/`.~~ **Built** — `-workload carry` (writes the payload once as the flow's
  initial state, then carries it through `-linear-steps` hops) and `-workload carryfanout` (the N·D case:
  `-fanout-width` branches, two steps deep, all carrying one document anchored at the entry/spawn step).
  Local SQLite at 32KB: **1.01x payload stored per flow against a naive 6x** linear, **1.07x against 17x** at
  width 8. Read these off `dwarf_state_write_bytes{column=state}`; their tasks declare almost no write
  volume, so MB/s is meaningless for them by design.

  **The `state` workload does NOT fail to mint** — a claim worth killing before it spreads. A rewritten
  field is re-anchored at the step that wrote it (its `changes` holds the bytes) and the successor's
  `state` refs that, so measured on Postgres at 64K/6 steps its `state` column is **0.0 K/flow** against
  **322.5 K/flow** of `changes`, versus `carry`'s 64.1 K/flow of `state` and 0.1 K/flow of `changes`. The
  5x between them is that `state` genuinely produces five payloads and `carry` produces one. Refs remove
  the CARRY multiplier; write volume for actually-new data is irreducible.
- **Whether `maxAnchors` ever binds** is unmeasured (how many large fields are concurrently live in a
  realistic workflow). The engine's byte counters are the instrument.
- **forEach item refs** — a branch pointing at its element inside the array's anchor instead of storing it
  inline, which would take a fan-out from ~2x back to ~1x. Designed, not implemented; it retypes `Refs`'
  value from `int` to an opaque form, so the three places that *inspect* an anchor id (the one-hop assertion,
  `Mint`'s carry-forward, and `Fork`'s remap) are the ones that change.
