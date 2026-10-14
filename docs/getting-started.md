# Getting started

> **For developers** meeting dwarf for the first time. It installs the module, explains the two callbacks you
> must implement, and gets a flow running against the in-process test harness — no database required.

## Install

```bash
go get github.com/microbus-io/dwarf
```

Dwarf requires Go 1.26+ and a SQL database for production use (PostgreSQL, MySQL/MariaDB, or SQL Server).
For tests and local experiments it uses SQLite in-memory automatically — no database to set up.

## The three things you provide

The engine handles scheduling, state, durability, and recovery. You provide three things:

1. **A graph** — the shape of the workflow, built with `workflow.NewGraph`.
2. **A `Host` implementing `LoadGraph`** — returns a graph by workflow URL. The engine calls it when a flow
   is created (including a subgraph's child flow), then freezes the graph onto the flow.
3. **The same `Host` implementing `ExecuteTask`** — runs one task. It receives a `*workflow.Flow` with the
   step's input state already populated; it does the work and writes outputs back onto the flow.

That's the whole contract. The engine never learns *how* a task is reached — whether your host's
`ExecuteTask` calls a local function, makes an RPC, or publishes to a bus is entirely up to you.

## Your first flow (test harness)

The fastest way to see dwarf run is the in-process test harness. `hosts.Mux` implements the `Host`
interface against in-memory registries, and `engine.NewEngineUnderTest(name)` builds an engine against an
isolated, auto-dropped SQLite database.

```go
package example

import (
	"context"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestGreeting(t *testing.T) {
	ctx := context.Background()
	mux := hosts.NewMux()

	// 1. Define a two-task graph: Greet -> Shout -> END.
	g := workflow.NewGraph("Welcome")
	g.SetURL("Greet", "greet") // task "Greet" dispatches to URL "greet"; the first task bound is the entry point
	g.SetURL("Shout", "shout")
	g.AddTransition("Greet", "Shout")
	g.AddTransition("Shout", workflow.End)
	mux.HandleGraph("welcome", g)

	// 2. Register the tasks.
	mux.HandleTask("greet", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("message", "hello "+f.GetString("name"))
		return nil
	})
	mux.HandleTask("shout", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("message", strings.ToUpper(f.GetString("message")))
		return nil
	})

	// 3. Wire and start the engine.
	eng := dwarf.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	eng.Startup(ctx)

	// 4. Run a flow to completion.
	_, out, err := eng.Run(ctx, "welcome", map[string]any{"name": "ada"}, nil)
	testarossa.NoError(t, err)
	testarossa.Equal(t, workflow.StatusCompleted, out.Status)
	testarossa.Equal(t, "HELLO ADA", out.State.GetString("message"))
}
```

`Run` is `Create` + `Await` in one call. The `nil` last argument is `*workflow.FlowOptions`
(scheduling and baggage) — see [Driving flows](flows.md).

## Wiring a real engine

In production you point the engine at a real database with `SetShard` and manage its lifecycle explicitly.
If your tasks are Go functions in the same process, `hosts.Mux` can stay as the host; if they run elsewhere,
behind HTTP, a message bus or an RPC framework, replace it with your own `Host`, which need only implement the
two required methods:

```go
type myHost struct{}

func (h *myHost) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	return fetch(ctx, workflowURL) // your registry / file / database / RPC
}

func (h *myHost) ExecuteTask(ctx context.Context, taskURL string, f *workflow.Flow) error {
	return dispatch(ctx, taskURL, f) // your local table / RPC / bus
}
```

That is the whole interface. Wire it up and start the engine:

```go
eng := dwarf.NewEngine()
eng.SetShard(engine.ShardSpec{Index: 1, DSN: "postgres://user:pass@db:5432/dwarf"})
eng.SetHost(&myHost{})

err := eng.Startup(ctx)
if err != nil {
	log.Fatal(err)
}
defer eng.Shutdown(ctx)
```

- `Startup` opens the database connections, runs schema migrations, and starts the worker pool.
- `Shutdown` drains the workers and closes the connections cleanly.
- The database must already exist; the engine migrates the schema but does not `CREATE DATABASE`.

Not registering a host (`SetHost`) makes `Startup` return an error.

## Where to go next

- The [Concepts](concepts.md) guide explains flows, steps, threads, and reducers — the vocabulary the rest
  of the docs assume.
- [Building graphs](graphs.md) and [Writing tasks](tasks.md) are the two day-to-day authoring guides.
