# Dwarf `internal/keys` — key format, token entropy, correlation id

> Load when: changing the flow/step key format, the token width, or the correlation-id derivation.
> Coupled with: `engine/CLAUDE.md` §"Keys are capabilities, not authorization" (the engine-side *enforcement* of
> what this package *encodes*) and §"Tracing" (which consumes `CorrelationID`).

This package owns the *encoding*: how a key is shaped, how much entropy its token carries, and how the token-free
correlation id is derived from it. The engine owns the *enforcement* (the `WHERE flow_id=? AND flow_token=?` gates,
uniform not-found, host-side authz) — see the engine doc.

### Format

A flow/step key is **`{shard}-{id}-{token}`** with a **1-based** shard:

- `{shard}` routes the request to a database shard (1-based; `ParseFlowKey`/`ParseStepKey` reject `< 1`).
- `{id}` is the per-shard sequential `flow_id`/`step_id` — unique only within a shard, which is why the shard is
  carried in the key and why the graph cache is keyed by `(shard, flowID)`.
- `{token}` is an unguessable random capability (`RandomIdentifier(16)`).

`New(shard, id, token)` is the **only** place the format is spelled — nothing outside this package composes a key
by hand, so the constructor and the two parsers stay inverses by construction rather than by convention (pinned by
`FuzzParseFlowKey`'s round-trip). It serves flow and step keys alike, since the two formats are identical.

`CorrelationID(shard, id)` returns `"{shard}-{id}"` — the key with the token segment omitted. It is deliberately
**not** a valid engine key (no operation accepts it) so a trace/log reader cannot escalate it into the flow's write
capability, and the engine offers no correlation-id→key lookup.

### Why 64-bit, and why not widen

`RandomIdentifier(16)` is 16 hex chars = **64 bits** of `crypto/rand`. That is adequate *because the token is never
a standalone lookup*: every gate is `WHERE flow_id=? AND flow_token=?` (and `step_id=? AND step_token=?`) — verified
across the codebase; the only token-only `WHERE`s are in tests. So the token has no birthday/collision surface (the
unique id disambiguates rows) — it only has to resist *targeted* guessing of one specific row's token. Targeted
online guessing of 64 bits of `crypto/rand`, even at an implausible sustained 1M attempts/sec against one `flow_id`,
is ~300,000 years expected; offline is impossible (no oracle). Widening to 128-bit (a periodically-suggested review
item) buys no security against any realistic threat — the capability-URL leak channels (email, logs, referrer) are
unaffected by token width and want short-TTL/single-use, a host concern — while doubling the visible random tail on
every printed key. So keep 64-bit; revisit only if a concrete driver appears (a future token-only lookup, or an
offline-exposed capability URL).

**If it is ever widened, the `CHAR(16)` columns must grow in lockstep and `RandomIdentifier`'s argument with them.**
