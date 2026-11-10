# Dwarf `internal/keys` — key format, token entropy, correlation id

> Load when: changing the flow/step key format, the token width, or the correlation-id derivation.
> Coupled with: `engine/CLAUDE.md` §"Keys are capabilities, not authorization" (the engine-side *enforcement* of
> what this package *encodes*) and §"Tracing" (which consumes `CorrelationID`).

This package owns the *encoding*: how a key is shaped, how much entropy its token carries, and how the token-free
correlation id is derived from it. The engine owns the *enforcement* (the `WHERE flow_id=? AND flow_token=?` gates,
uniform not-found, host-side authz) — see the engine doc.

### Format

A flow key is **`{shard}-{hour}-{flowID}-{token}`** and a step key **`{shard}-{hour}-{flowID}-{stepID}-{token}`**,
with a **1-based** shard:

- `{shard}` routes the request to a database shard (1-based; `ParseFlowKey`/`ParseStepKey` reject `< 1`).
- `{hour}` is the flow's creation hour, in whole hours since 2000-01-01 00:00 UTC (`dwarf_flows.created_hour`,
  written by the database clock). A step key carries its flow's hour (`dwarf_steps.flow_created_hour`), not a time
  of its own. It is part of the address, checked with the token (below). The parsers accept 0 through 2^31-1, the
  range of the `INT` columns: a larger hour would fail inside a driver as an encoding error rather than as the
  invalid key it is.
- `{flowID}` and `{stepID}` are the per-shard sequential `flow_id`/`step_id` — unique only within a shard, which
  is why the shard is carried in the key and why the graph cache is keyed by `(shard, flowID)`, with the flow token
  beside them because a restore or a lossy failover reissues ids. **A step key names its flow**, and the step gates
  check it beside the token and the hour (`step_id=? AND flow_id=? AND step_token=? AND flow_created_hour=?`), so a
  step key is only ever found as a step of the flow it names.
- `{token}` is an unguessable random capability (`RandomIdentifier(16)`).

**Every segment is accepted only as the constructors spell it** (no `+`, no leading zeros), and **every gate checks
the hour with the token**: `WHERE flow_id=? AND flow_token=? AND created_hour=?`, and for a step
`step_id=? AND flow_id=? AND step_token=? AND flow_created_hour=?`. Either rule alone
leaves a flow with two spellings, and a parked `Await` keyed on one is never released by a stop signalled under the
other: it re-reads the flow on every latch sweep until its budget runs out. A key with the wrong hour is therefore
not found, exactly as one with the wrong token is.

`NewFlowKey` and `NewStepKey` are the **only** places the formats are spelled — nothing outside this package composes
a key by hand, so each constructor and its parser stay inverses by construction rather than by convention (pinned by
the fuzzers' round-trips). A flow key does not parse as a step key, nor the reverse: their segment counts differ.

`CorrelationID(shard, hour, id)` returns `"{shard}-{hour}-{id}"` — the flow key with only its token omitted. The
token is the key's one secret, so the id grants nothing: it is deliberately **not** a valid engine key (three
segments where a flow key has four; no operation accepts it), so a trace/log reader cannot escalate it into the
flow's write capability. It is unique, since ids are per-shard sequences.

It is an **exact prefix of the flow key**, so an operator holding a correlation id from a log finds the flow
through the lists: `Query.Search` matches substrings of the key, and searching `{correlationID}-` with
`Query.Shard` set matches that flow's key and no other's (the trailing `-` keeps flow 123 from matching 1234,
`Query.Shard` keeps shard 1 from matching inside shard 21's keys). That is not a correlation-id→key oracle: the
lists already return keys wholesale to whoever may call them, so the search reveals nothing that caller could not
list anyway, and a log reader without list access still holds no key. Keep it a prefix — an id that is not a
substring of the key (dropping the hour, reordering segments) leaves the operator with no way to find the flow.
Pinned by `TestCorrelationID_IsTheKeyWithoutItsToken` and the engine's `TestList_SearchFindsTheFlowOfACorrelationID`.

### Why 64-bit, and why not widen

`RandomIdentifier(16)` is 16 hex chars = **64 bits** of `crypto/rand`. That is adequate *because the token is never
a standalone lookup*: every gate is `WHERE flow_id=? AND flow_token=?` (and `step_id=? AND step_token=?`), with the
hour beside it — verified across the codebase; the only token-only `WHERE`s are in tests. So the token has no birthday/collision surface (the
unique id disambiguates rows) — it only has to resist *targeted* guessing of one specific row's token. Targeted
online guessing of 64 bits of `crypto/rand`, even at an implausible sustained 1M attempts/sec against one `flow_id`,
is ~300,000 years expected; offline is impossible (no oracle). Widening to 128-bit (periodically
suggested) buys no security against any realistic threat — the capability-URL leak channels (email, logs, referrer) are
unaffected by token width and want short-TTL/single-use, a host concern — while doubling the visible random tail on
every printed key. So keep 64-bit; revisit only if a concrete driver appears (a future token-only lookup, or an
offline-exposed capability URL).

**If it is ever widened, the 16-character token columns (`VARCHAR(16)`, `NVARCHAR(16)` on SQL Server; SQLite's
`TEXT` is unbounded) must grow in lockstep and `RandomIdentifier`'s argument with them.**
