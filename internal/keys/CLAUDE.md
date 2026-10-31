# Dwarf `internal/keys` — key format, token entropy, correlation id

> Load when: changing the flow/step key format, the token width, or the correlation-id derivation.
> Coupled with: `engine/CLAUDE.md` §"Keys are capabilities, not authorization" (the engine-side *enforcement* of
> what this package *encodes*) and §"Tracing" (which consumes `CorrelationID`).

This package owns the *encoding*: how a key is shaped, how much entropy its token carries, and how the token-free
correlation id is derived from it. The engine owns the *enforcement* (the `WHERE flow_id=? AND flow_token=?` gates,
uniform not-found, host-side authz) — see the engine doc.

### Format

A flow/step key is **`{shard}-{hour}-{id}-{token}`** with a **1-based** shard:

- `{shard}` routes the request to a database shard (1-based; `ParseFlowKey`/`ParseStepKey` reject `< 1`).
- `{hour}` is the flow's creation hour, in whole hours since 2000-01-01 00:00 UTC (`dwarf_flows.created_hour`,
  written by the database clock). A step key carries its flow's hour (`dwarf_steps.flow_created_hour`), not a time
  of its own. It is part of the address, checked with the token (below). The parsers accept 0 through 2^31-1, the
  range of the `INT` columns: a larger hour would fail inside a driver as an encoding error rather than as the
  invalid key it is.
- `{id}` is the per-shard sequential `flow_id`/`step_id` — unique only within a shard, which is why the shard is
  carried in the key and why the graph cache is keyed by `(shard, flowID)`, with the flow token beside them
  because a restore or a lossy failover reissues ids.
- `{token}` is an unguessable random capability (`RandomIdentifier(16)`).

**Every segment is accepted only as `New` spells it** (no `+`, no leading zeros), and **every gate checks the hour
with the token**: `WHERE flow_id=? AND flow_token=? AND created_hour=?`, and for a step
`step_token=? AND flow_created_hour=?`. Either rule alone
leaves a flow with two spellings, and a parked `Await` keyed on one is never released by a stop signalled under the
other: it re-reads the flow on every latch sweep until its budget runs out. A key with the wrong hour is therefore
not found, exactly as one with the wrong token is.

`New(shard, hour, id, token)` is the **only** place the format is spelled — nothing outside this package composes
a key by hand, so the constructor and the two parsers stay inverses by construction rather than by convention
(pinned by `FuzzParseFlowKey`'s round-trip). It serves flow and step keys alike, since the two formats are
identical.

`CorrelationID(shard, id)` returns `"{shard}-{id}"` — the key with the hour and token omitted. It is deliberately
**not** a valid engine key (no operation accepts it) so a trace/log reader cannot escalate it into the flow's write
capability, and the engine offers no correlation-id→key lookup. It stays unique without the hour, since ids are
per-shard sequences.

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
