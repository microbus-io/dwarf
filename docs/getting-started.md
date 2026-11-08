# Getting started

> **For developers** meeting dwarf for the first time. It installs the module, explains the two callbacks you
> must implement, and gets a flow running against the in-process test harness — no database required.

## Install

```bash
go get github.com/microbus-io/dwarf
```

Dwarf requires Go 1.27.3+ and a SQL database for production use (PostgreSQL, MySQL/MariaDB, or SQL Server).
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
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != workflow.StatusCompleted {
		t.Fatalf("status %s", out.Status)
	}
	if msg := out.State.GetString("message"); msg != "HELLO ADA" {
		t.Errorf("message %q", msg)
	}
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

## Tasks in another process

A host that reaches its tasks over a network sends the `*workflow.Flow` it is given as JSON, and the task server
replies with the task's changes and control signals, which the host reads back onto the same `Flow`. Wrap the Flow
in `workflow.RequestCarrier` going out and `workflow.ResultCarrier` coming back, and use the same pair on the task
server:

```go
// host
func (h *myHost) ExecuteTask(ctx context.Context, taskURL string, f *workflow.Flow) error {
	body, err := json.Marshal(workflow.RequestCarrier(f))
	if err != nil {
		return err
	}
	reply, err := post(ctx, taskURL, body) // your transport; a task's error comes back as an error
	if err != nil {
		return err
	}
	return json.Unmarshal(reply, workflow.ResultCarrier(f))
}

// task server
func handle(w http.ResponseWriter, r *http.Request) {
	var f workflow.Flow
	if err := json.NewDecoder(r.Body).Decode(workflow.RequestCarrier(&f)); err != nil {
		status := http.StatusBadRequest // malformed JSON, or no version
		if errors.StatusCode(err) == http.StatusNotImplemented {
			status = http.StatusNotImplemented // a newer version than this build reads
		}
		http.Error(w, err.Error(), status)
		return
	}
	if err := runTask(r.Context(), &f); err != nil {
		http.Error(w, err.Error(), errors.StatusCode(err))
		return
	}
	json.NewEncoder(w).Encode(workflow.ResultCarrier(&f))
}
```

- The engine reads back only the task's changes and control signals. The result carrier leaves out the state and
  the graph's reducers, so a large state crosses the wire once rather than twice. Plain `json.Marshal(f)` and
  `json.Unmarshal(reply, f)` also work, sending every field both ways.
- Every body carries the format's version in `"v"`. Decoding fails when it is missing (status 400) or newer than
  this build reads (status 501, naming both versions): upgrade that side, or roll out task servers before the
  engine. A task server replies in the version its request came in, so an older host can always read the reply.
- The JSON carries the flow, not the call's context. Forward the flow's baggage (`workflow.BaggageFrom(ctx)`) and the
  context's deadline over your transport, and give the task server's handler a context carrying them
  (`workflow.ContextWithBaggage`).
- A result's `changes` are the step's whole delta. On a re-run - after a `Retry`, a resumed `Interrupt` or a returning
  `Subgraph` - the request already holds what the step's earlier runs wrote, and the result must hand it back with the
  new writes, as a `workflow.Flow` does on its own. A task server in another language that returns only the new
  writes loses the earlier ones.
- Durations go out as whole milliseconds: `sleepMillis`, `backoffInitialDelayMillis`, `backoffMaxDelayMillis`.

## Where to go next

- The [Concepts](concepts.md) guide explains flows, steps, and reducers — the vocabulary the rest
  of the docs assume.
- [Building graphs](graphs.md) and [Writing tasks](tasks.md) are the two day-to-day authoring guides.
