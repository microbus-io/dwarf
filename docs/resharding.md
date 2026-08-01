# Resharding

> **For operators** changing the set of database shards behind a running fleet. Read
> [Deployment → Sharding](deployment.md#sharding) first for what shards are and how many to run; this page is
> about changing the set after flows already exist.

## The constraint

**A flow's shard is part of its identity and never changes.** Every flow key is `{shard}-{id}-{token}`, so
the shard index is encoded in the handle your callers hold, and it is what routes every subsequent operation.
Everything derived from a flow stays on its shard for life: subgraph children, thread continuations, and
forks are all shard-pinned.

Three rules follow, and they are the whole of this page:

1. **The shard set is fixed for a process's lifetime.** `SetShard` is construction-time only and is rejected
   on a running engine. Changing the set means restarting replicas.
2. **The index-to-connection-string map must be identical on every replica.** A flow created on a shard a
   peer does not know about is *unroutable* there — that replica answers "not found," and a caller may
   legitimately act on that.
3. **Nothing rebalances.** There is no mechanism to move an existing flow to a different shard, and none is
   planned. Growth changes where *new* flows land, never where existing ones live.

**Indexes are sparse.** They must be unique and at least 1, but need not be contiguous — shards 1, 2 and 99
are a valid set. This exists precisely so a retired shard's index can be dropped without renumbering the
others, which would otherwise invalidate every flow key on them.

## Adding a shard

Adding capacity is the common case and it can be done without a maintenance window, in **two rolling
restarts**. The trick is to make every replica able to *route* to the new shard before any replica starts
*placing* flows on it.

**Phase 1 — introduce it, cordoned.**

```go
eng.SetShard(engine.ShardSpec{
    Index:       4,
    DSN:         "...",
    VirtualCPUs: 8,
    Cordoned:    true,   // opened, migrated, routable — but takes no new flows
})
```

Roll the fleet. Each replica opens and migrates shard 4 at startup and can route flow keys to it, but the
capacity-weighted placement skips it entirely, so nothing is created there yet. At the end of this phase the
fleet is uniform and no flow exists that any replica cannot reach.

**Phase 2 — activate it.** Flip `Cordoned` to `false` and roll again. New flows now distribute across all
four shards in proportion to their declared `VirtualCPUs`.

**Do not collapse this into one step.** A single rolling restart leaves a window in which upgraded replicas
place flows on shard 4 while replicas still on the old configuration cannot route to them. Work still
executes — the replicas that know shard 4 dispatch it — but callers hitting an old replica get "not found"
for flows that exist. That is precisely the failure the two-phase order avoids.

**Declare `VirtualCPUs` honestly.** It drives both the shard's connection budget and its share of new
placement. An undeclared shard is assumed to be a 2-vCPU machine, which means a large new server will be sent
a trickle of work and run at a fraction of its capacity. Nothing detects an over-declared value either — the
facts you declare are trusted.

**Expect the new shard to fill slowly.** It only receives *new* flows. The existing imbalance persists until
the old shards' flows complete and age out under your retention policy.

## Retiring a shard

**Phase 1 — cordon it.** Set `Cordoned: true` on the shard and roll the fleet. It stops receiving new flows
immediately. Everything resident continues normally: flows keep executing, and their subgraph children,
`Continue` turns and forks are still created on it, because those are shard-pinned.

**Phase 2 — drain.** Nothing drains automatically. The shard is empty when it holds no flow you still need,
which means both:

```go
// no live work
flows, _, _ := eng.List(ctx, workflow.Query{Shard: 3, Status: workflow.StatusRunning})
flows, _, _ := eng.List(ctx, workflow.Query{Shard: 3, Status: workflow.StatusInterrupted})

// and no terminal flow you still want readable
count, _ := eng.Purge(ctx, workflow.Query{Shard: 3, OlderThan: 30 * 24 * time.Hour})
```

`Query.Shard` scopes both to one shard. Watch `interrupted` flows especially — they wait indefinitely and are
the ones that will still be there when you assumed the shard was empty. Either resume them, cancel them, or
wait.

`Purge` marks at most 4,096 roots per call, so draining a large shard is a loop, and a background reaper does
the actual deletion within about a minute of marking.

**Phase 3 — remove it.** Drop the `SetShard` call and roll the fleet. Leave the index unused; do not
renumber the survivors, and do not reuse the retired index for a different database — old flow keys naming
it would resolve against unrelated data.

**Deciding when it is safe to remove:** any flow key on the retired shard becomes permanently unresolvable.
If callers hold keys durably (in their own database, in emails, in webhooks), they will get "not found"
rather than an error explaining why. Retire the index only once you are content for those keys to be gone.

## Moving a shard to a different server

**This is not a reshard.** If the shard index stays the same, the engine does not care where the database
lives: replace the connection string, keep the index, and do an ordinary database migration by whatever means
your dialect supports. Restart the fleet to pick up the new connection string.

This is the supported way to grow a shard's *capacity* — move it to a bigger server and update
`VirtualCPUs` — as opposed to adding shards, which grows the *number* of servers. Vertical growth of one
shard needs no coordination beyond the restart; horizontal growth needs the two-phase dance above.

## Changing the shard count is a capacity decision, not a scaling reflex

Before resharding at all, check that the shard count is what actually binds. Shards exist to add *database
servers*, and the measured ladder keeps paying well past the first one: 2 shards ×1.84, 3 ×2.56, 6 ×4.92
against a single shard. Scaling is sub-linear but does not flatten in the measured range, so do not stop at
two on the assumption that later shards add nothing.
→ [Cloud benchmarks](benchmark-cloud.md#scaling-out-more-shards)

**Co-locating several shards on one database server buys nothing.** The point of a shard is a separate write
path; two shards on one server contend for the same one and add overhead for no capacity. If you are not
adding a server, you are not adding capacity.

See [Deployment → Sharding](deployment.md#sharding) for the sizing table, and
[Cloud benchmarks](benchmark-cloud.md) for the measurements behind it.

## What is not supported

- **Live shard growth.** Cross-replica agreement on a changing shard set, plus rollout sequencing, was
  designed and deliberately deferred. The two-phase cordoned restart above is the supported substitute.
- **Rebalancing existing flows.** There is no move, no dual-write, and no re-keying tool. A flow key encodes
  its shard, so moving a flow would invalidate a handle callers already hold.
- **Fault tolerance across shards.** Dwarf is not shard-fault-tolerant: `List` and `Purge` fail as a whole
  if any shard errors. Shards multiply capacity, not availability — each shard's own availability is its
  database's. (`ShardInfo` is the exception and is deliberately built to survive this: it returns a row per
  shard with that shard's error recorded on it, so it still answers when a shard is down. It is the
  diagnostic to reach for in exactly that situation.)
