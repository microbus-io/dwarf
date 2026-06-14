# Testing

Dwarf ships an in-process test harness so you can exercise real workflows — scheduling, fan-out, subgraphs,
interrupts, retries — with no database to set up and no transport to stand up.

## The two pieces

- **`Engine.RunInTest(t)`** replaces `Startup`/`Shutdown`. It opens an isolated SQLite in-memory database
  (per test, and per shard for multi-shard tests), runs migrations, and registers cleanup via `t.Cleanup`.
  No DSN, no teardown code.
- **`engine.TestProxy`** is an in-memory implementation of the `Host` interface. Register graphs and task
  functions on it, then register it with `WithHost(proxy)` — its `LoadGraph`/`ExecuteTask` back the
  required methods, and its peer methods are no-ops.

```go
func TestCheckout(t *testing.T) {
    ctx := context.Background()
    proxy := engine.NewTestProxy()

    g := workflow.NewGraph("Checkout", "checkout")
    g.AddTask("Reserve", "inventory.reserve")
    g.AddTask("Charge", "billing.charge")
    g.AddTransition("Reserve", "Charge")
    g.AddTransition("Charge", workflow.END)
    proxy.HandleGraph("checkout", g)

    proxy.HandleTask("inventory.reserve", func(ctx context.Context, f *workflow.Flow) error {
        f.SetBool("reserved", true)
        return nil
    })
    proxy.HandleTask("billing.charge", func(ctx context.Context, f *workflow.Flow) error {
        f.SetString("receipt", "r-123")
        return nil
    })

    eng := dwarf.NewEngine().
        WithHost(proxy)
    eng.RunInTest(t)

    out, err := eng.Run(ctx, "checkout", map[string]any{"sku": "ABC"}, nil)
    testarossa.NoError(t, err)
    testarossa.Equal(t, workflow.StatusCompleted, out.Status)
    testarossa.Equal(t, "r-123", out.State["receipt"])
}
```

`TestProxy.HandleGraph(name, graph)` registers a graph; `HandleTask(url, handler)` registers a task by its
URL (the address used in `AddTask`). The handler signature is the same `func(ctx, *workflow.Flow) error`
you write in production.

## Configuring the test engine

`RunInTest` honors any `With*` settings you apply first — workers, shards, time budget, default priority:

```go
eng := dwarf.NewEngine().
    WithHost(proxy).
    WithWorkers(1).        // serialize dispatch to assert ordering
    WithNumShards(4)       // each shard gets its own in-memory database
eng.RunInTest(t)
```

`WithWorkers(1)` is a common trick for deterministically asserting dispatch order (e.g. fairness or
priority tests).

## Testing the harder paths

The harness drives every engine feature. A few patterns:

**Interrupts / human-in-the-loop.** A task calls `Interrupt`; assert the flow parks, then `Resume`:

```go
flowKey, _ := eng.Create(ctx, "approval", state, nil)
eng.Start(ctx, flowKey)
out, _ := eng.Await(ctx, flowKey)
testarossa.Equal(t, workflow.StatusInterrupted, out.Status)
// out.InterruptPayload holds what the task asked for

eng.Resume(ctx, flowKey, map[string]any{"approved": true})
out, _ = eng.Await(ctx, flowKey)
testarossa.Equal(t, workflow.StatusCompleted, out.Status)
```

**Backpressure and breakers.** Return the disposition wrappers from a task to drive the valve or breaker:

```go
proxy.HandleTask("flaky", func(ctx context.Context, f *workflow.Flow) error {
    if firstFewCalls() {
        return workflow.ErrBreakerTrip(errors.New("unreachable"), "ack_timeout")
    }
    return nil
})
```

Because `TestProxy` returns synchronously, it produces a far tighter, more adversarial timing environment
than a real network — which makes it excellent at surfacing concurrency bugs in retries, fan-in, and
breaker recovery.

**Inspecting execution.** `eng.History(ctx, flowKey)` returns the full step record (including nested
subgraph history); `eng.HistoryMermaid(ctx, flowKey, w)` renders the execution DAG as a Mermaid diagram for
eyeballing what ran in what order.

## Multi-replica tests

To test cross-replica behavior, give each engine its own `TestProxy` and register the other engines with
`proxy.AddPeer(otherEngine)`. The proxy's `SignalPeers` then relays each signal to every peer's
`DeliverSignal`, standing in for the bus:

```go
proxy1, proxy2 := engine.NewTestProxy(), engine.NewTestProxy()
// register the same graphs/tasks on both...
eng1 := engine.NewEngine().WithHost(proxy1).WithDSN(sharedDSN)
eng2 := engine.NewEngine().WithHost(proxy2).WithDSN(sharedDSN)
proxy1.AddPeer(eng2)
proxy2.AddPeer(eng1)
```

Use a shared in-memory DSN (e.g. `"file:x%d?mode=memory&cache=shared"`) so both engines see the same
databases. This is how the engine's own cross-replica `Await` and distributed-backpressure tests are
written — see the `fixtures` package in the repository for worked examples.

## Where examples live

The repository's `fixtures` package contains ~60 end-to-end workflow tests built exactly this way — from
`basicflow` up through `subgraphflow`, `dynamicfanoutflow`, and `fairnessflow`. They're the canonical,
runnable reference for every feature.
