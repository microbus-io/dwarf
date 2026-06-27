# Testing

Dwarf ships an in-process test harness so you can exercise real workflows — scheduling, fan-out, subgraphs,
interrupts, retries — with no database to set up and no transport to stand up.

## The two pieces

- **`Engine.RunInTest(t)`** replaces `Startup`/`Shutdown`. It opens an isolated SQLite in-memory database
  (per test, and per shard for multi-shard tests), runs migrations, and registers cleanup via `t.Cleanup`.
  No DSN, no teardown code.
- **`engine.TestProxy`** is an in-memory implementation of the `Host` interface. Register graphs and task
  functions on it, then register it with `SetHost(proxy)` — its `LoadGraph`/`ExecuteTask` back the
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

    eng := dwarf.NewEngine()
    eng.SetHost(proxy)
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

`RunInTest` honors any `Set*` settings you apply first — workers, shards, time budget, default priority:

```go
eng := dwarf.NewEngine()
eng.SetHost(proxy)
eng.SetWorkers(1)   // serialize dispatch to assert ordering
eng.SetNumShards(4) // each shard gets its own in-memory database
eng.RunInTest(t)
```

`SetWorkers(1)` is a common trick for deterministically asserting dispatch order (e.g. fairness or
priority tests).

## Testing the harder paths

The harness drives every engine feature. A few patterns:

**Interrupts / human-in-the-loop.** A task calls `Interrupt`; assert the flow parks, then `Resume`:

```go
flowKey, _ := eng.Create(ctx, "approval", state, nil)
out, _ := eng.Await(ctx, flowKey)
testarossa.Equal(t, workflow.StatusInterrupted, out.Status)
// out.InterruptPayload holds what the task asked for

eng.Resume(ctx, flowKey, map[string]any{"approved": true})
out, _ = eng.Await(ctx, flowKey)
testarossa.Equal(t, workflow.StatusCompleted, out.Status)
```

**Transient failures.** A task that returns `nil` after arming `flow.Retry` re-runs after backoff; assert
the eventual outcome once the task stops failing:

```go
proxy.HandleTask("flaky", func(ctx context.Context, f *workflow.Flow) error {
    if firstFewCalls() {
        if f.Retry(time.Millisecond, 2.0, 10*time.Millisecond, time.Minute) {
            return nil
        }
    }
    return nil
})
```

Because `TestProxy` returns synchronously, it produces a far tighter, more adversarial timing environment
than a real network — which makes it excellent at surfacing concurrency bugs in retries, fan-in, and
crash recovery.

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
eng1 := engine.NewEngine()
eng1.SetHost(proxy1)
eng1.SetDSN(sharedDSN)
eng2 := engine.NewEngine()
eng2.SetHost(proxy2)
eng2.SetDSN(sharedDSN)
proxy1.AddPeer(eng2)
proxy2.AddPeer(eng1)
```

Use a shared in-memory DSN (e.g. `"file:x%d?mode=memory&cache=shared"`) so both engines see the same
databases. This is how the engine's own cross-replica `Await` and step-recovery tests are
written — see the `fixtures` package in the repository for worked examples.

## Where examples live

The repository's `fixtures` package contains ~60 end-to-end workflow tests built exactly this way — from
`basicflow` up through `subgraphflow`, `dynamicfanoutflow`, and `fairnessflow`. They're the canonical,
runnable reference for every feature.
