# Dwarf `internal/graphcache` — the two-layer parsed-graph cache

> Load when: changing how a flow's graph is found, parsed or kept in memory.
> Coupled with: `engine/CLAUDE.md` §"Round-trip minimization in `processStep`" (where the engine looks a graph
> up, seeds it at `Create`, and wraps a parse failure as permanent) and `internal/migrations/CLAUDE.md`
> (`graph_hash`).

A flow's graph JSON is frozen at `Create` and is the largest payload on the flow row. Reading and parsing it per
dispatch would cost a round trip and a parse per step, so a replica keeps parses in memory. Nothing ever
invalidates an entry, because nothing about a stored graph ever changes.

### How it works

**Where a flow's graph comes from.** `Create` (and a subgraph spawn) asks the host for the graph by its workflow
URL (`Host.LoadGraph`), serializes it, and stores the bytes on the new flow row with `graph_hash`, the SHA-256 of
those bytes. From then on the flow runs on its own stored bytes and the URL is never asked again for it. The URL
is an opaque name to the engine; the hash is the graph's identity.

**The two layers.**

- The **shared layer** holds one parsed `Entry` per distinct graph, keyed by `(workflowURL, graph_hash)`. Every
  flow of that graph shares the one parse - hence "shared". Bounded at 4,096 parses, which bounds memory.
- The **per-flow layer** maps a flow, `(shard, flow_id, flow_token)`, to a weak pointer at its graph's shared
  entry. Bounded at 65,536 flows, ~150 bytes each.

**A dispatch's lookup**, in order:

1. `Flow(key)` - this replica has run the flow before. Nothing is read.
2. `Shared(url, hash)` - the claim's flow read already returned the row's `graph_hash`, so a flow this replica has
   never seen, of a graph it has, is found with no graph read. The common case for a flow created on another
   replica: `Create` seeds the cache only on a replica that dispatches the shard, so where frontends create and
   do not dispatch (an HA deployment) every flow's first dispatch misses step 1. Without the hash that is one
   graph read per FLOW (1.0 per step for a one-step flow); with it, one per graph per replica. Pinned by the
   engine's `TestGraphCache_HashHitReadsNoGraph`.
3. Read the `graph` column and `Intern` it - once per graph per replica.

After 2 or 3 the engine calls `Point(key, entry)`, so the flow's next step stops at 1. `Create` on a replica that
dispatches the shard interns the bytes it just stored and points the new flow at once, so its entry step stops at
1 too. `flowGraph` (final state, at a flow's end) has the flow's key but not its hash, so it tries 1, then reads
the row and interns, without pointing.

**When the host's graph changes.** Changing what a URL returns (a new workflow version) does not touch any flow
already created: those keep their bytes and hash, and keep finding the old parse. New flows store new bytes, so
they get a new hash and a new shared entry under the same URL. The two versions coexist until no flow of the old
one is dispatched; its parse then stops being touched and the LRU evicts it. A host that returns a different
graph on every call (per actor) gives every flow its own hash: correct, with no sharing.

**Why the URL is in the shared key at all.** The hash alone identifies the bytes. The URL is there because an
entry carries it - `flowGraph` returns it on a per-flow hit without reading the row, and it labels the
`workflow` metric - and byte-identical graphs stored under two URLs must not hand one flow the other's URL.

**Eviction.** The shared layer is a plain LRU with no TTL; the per-flow layer expires an entry 15 minutes after it
was stored. An evicted parse is released once no dispatch holds it, and every flow pointing at it then reads as a
miss at step 1 and finds or re-parses the graph at step 2 or 3.

### Two layers: the per-flow layer is sized for flows IN FLIGHT, the shared layer for memory

- **The shared layer** owns the parses, one per distinct graph, keyed by `(workflowURL, graph_hash)`. The flows
  of one graph share one parse, which is the whole point: a fleet runs many flows of few graphs.
- **The per-flow layer** maps `(shard, flow_id, flow_token)` to a weak pointer into the shared layer, so a flow
  this replica has seen is found by its key, with no hash in hand. An entry is ~150 bytes.

The per-flow layer alone thrashed at a 4,096-entry bound: one process holding ~8,200 flows (4 shards x 2,000 in
flight) paid 0.98 graph reads per executed step. With the shared layer behind it, a per-flow miss looks the hash
up there before reading anything, and the `graph` column is read only for a graph this replica has never parsed.

**The token is in the per-flow key** because a restore or a lossy failover reissues flow ids: without it a flow
given an old id runs on the lost flow's graph. Pinned by `TestCache_FlowIsKeyedByToken` and the engine's
`TestCreate_SeedsGraphCache`.

### Keep the per-flow pointer weak

A strong pointer lets the per-flow layer pin a parse the shared layer evicted, so a workload whose flows rarely
share graph bytes (per-actor graphs) holds up to 65,536 parses - measured 2.4-14 KB each (5-50 tasks),
~0.5-0.9 GB. Weak, memory is bounded by the shared layer's 4,096 parses, and such a workload still gets 4,096
flows of cover. An evicted parse is released once no dispatch holds it, and its flows then read as a miss.
Pinned by `TestCache_SharedEvictionReleasesTheParse`.

### The shared layer has no TTL

A graph's bytes never change, and expiring a hot graph would turn every one of its flows into a miss at once. Its
recency is refreshed by each new flow of the graph (`Create`'s seed, or a first dispatch), which is what keeps a
graph in use from being evicted.

### Never key the shared layer by a fast or truncated hash

`graph_hash` is SHA-256 of the bytes `Create` stored (`HashOf`). A collision would run a flow on another flow's
graph. The workflow URL is in the key too, because an entry carries it.

### Entries are frozen, and parsed from stored bytes only

An `Entry` is shared by every flow of its graph and by concurrent dispatches, so nothing may mutate one after the
parse. `Intern` takes the stored JSON, never a caller's live `*workflow.Graph`, which could change under the
dispatches that share the entry; `Create`'s seed therefore interns the bytes it just wrote.

The fan-in map is a pure function of the graph and is not persisted, so it is computed once per parse and kept
in the entry, amortizing its graph walk across every step.

### A parse failure is a plain error here, a permanent one in the engine

`Intern` returns an error for unparseable bytes and caches nothing. The engine's `internGraph` wraps it as a
`permanentError`, which is load-bearing: a flow whose graph cannot be read must still be stoppable, and
`terminalFinalState` takes a permanent error as an empty final state. Every engine intern goes through that
wrapper.
