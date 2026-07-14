# Dwarf `fixtures` — test harness and test mode

> Load when: writing or changing fixtures / engine tests.
> Coupled with: root `CLAUDE.md`.

### SQLite Testing Support

`engine.RunInTest(t)` hashes `t.Name()` into the engine's `testHashedID`, then runs the normal `Startup`/`Shutdown`
(via `t.Cleanup`) against per-test isolated databases. It is sugar over **`SetInTest(name)`** — the
construction-time, `*testing.T`-free hook that does the hashing and flips on test mode — plus `Startup` and a
`t.Cleanup` shutdown. A host with no `*testing.T` (e.g. one running under an external test harness) calls
`SetInTest(key)` itself with a stable isolation key — one shared across its replicas so they resolve to the same
isolated set. The `testHashedID` switches the open path into test mode: `openDatabaseShard`
resolves the base DSN in three tiers - an explicitly-set DSN wins; else `SEQUEL_TESTING_DSN` (the same variable
sequel reads, so one knob redirects the whole suite at a real server); else the SQLite in-memory default
**`file:dwarf_%d?mode=memory&cache=shared`** - substitutes `%d` with the shard index, then routes through
`sequel.CreateTestingDatabase`, which keys an isolated, auto-dropped database on `(driver, baseDSN, testID)`.

**Per-shard isolation comes from the DSN's `%d`, not from the testID.** The default DSN carries `%d`, so shard *i*
opens base `file:dwarf_i?…` - a distinct base per shard, so `CreateTestingDatabase`'s key already differs per shard
and a multi-shard engine never collapses onto one in-memory DB. (An earlier design used a no-`%d` default and folded
the shard index into the testID to manufacture that difference; with a `%d`-bearing default that fold is redundant
and was removed.) The consequence is a deliberately-honest sharp edge: a multi-shard run against a `SEQUEL_TESTING_DSN`
that **lacks `%d`** collapses its shards onto one database - caught loudly at `Open`, which rejects two shards
resolving to the same DSN rather than silently folding them.

The testID is hashed to a bounded 16 hex chars (SQL identifier limits: Postgres 63 / MySQL 64) before it reaches
`CreateTestingDatabase`, so an arbitrarily long Go subtest name still yields a valid database name. The hash is
deterministic, so two engines in the **same** test (both keyed by `t.Name()`) resolve to the *same* isolated
databases - which is how an in-test multi-replica fixture gives its peer engines shared state.

### Running the suite against a real server

`SEQUEL_TESTING_DSN` redirects the whole suite at a real database (the `%d` is required - it is what gives each
shard its own isolated test database; see the DSN resolution above). All four dialects pass:

```sh
SEQUEL_TESTING_DSN='postgres://root:secret1234@127.0.0.1:5432/dwarftest_%d?sslmode=disable' go test ./engine/ ./fixtures/
SEQUEL_TESTING_DSN='root:secret1234@tcp(127.0.0.1:3306)/dwarftest_%d'                        go test ./engine/ ./fixtures/
SEQUEL_TESTING_DSN='sqlserver://sa:PASS@127.0.0.1:1433?database=dwarftest_%d&encrypt=disable' go test ./engine/ ./fixtures/
```

**SQL Server needs `&encrypt=disable` against a container's self-signed certificate.** The stock image's cert has a
*negative serial number*, which Go's `crypto/x509` refuses to parse, so every connection dies with
`TLS Handshake failed: x509: negative serial number` before a single query runs. It looks like a dwarf failure (the
tests fail in `t.Cleanup`, dropping the test database) and is not one.

Worth doing before shipping anything dialect-sensitive: SQLite serializes writes and has the weakest planner, so it
hides exactly the bugs the others catch - MySQL's `RowsAffected` counting *changed* rather than *matched* rows, its
gap locks under `REPEATABLE READ`, SQL Server's filtered-index/parameterization rules and its `OUTPUT INSERTED`
claim path, and Postgres's real MVCC lock ordering. Expect the run to be slower there: SQL Server takes ~3x SQLite.

#### Per-test engine + sequential execution (connection-load control)

**Each `RunInTest` engine opens its own connection pool** - up to `maxOpenConns` (default 8) per
shard. **Each fixture stands up its own engine** (`engine.NewEngine()` + `SetHost(proxy)` + `RunInTest(t)`, with a
fresh `proxy := engine.NewTestProxy()` per test), and `RunInTest` registers a `t.Cleanup` that shuts it down - so a
fixture's pool is open only for the duration of that one test. There is **no** shared/`TestMain`-built engine; the
suite was deliberately moved off one.

What keeps the pools from summing past a server's connection cap is that **the fixtures do not run with
`t.Parallel()`** - they run **sequentially**, so at most ~one engine (plus the one being torn down by `t.Cleanup`) is
live at a time. With `t.Parallel()`, `go test` runs up to `-parallel` (defaults to `GOMAXPROCS`) tests at once, and
each per-test engine would multiply into *N parallel engines × pool × shards* connections to the **same** server - a
sum that overruns the cap (**PostgreSQL defaults to `max_connections = 100`**; MySQL 151, SQL Server ~32k, SQLite
none, so Postgres trips first). A pool opening a connection past the cap is *rejected* with an error (not blocked); the
engine then fails the operation or, on a `pollPendingSteps` sizing query, briefly re-polls (see the poll-error clamp
below). Retrying does not fix *structural* oversubscription (3 replicas each wanting 60 against a 100-cap server) - the
only fix is to keep the **sum of live pools under the cap**, which serial execution does. So a fixture must **not** add
`t.Parallel()`; if the suite is ever parallelized, connection load must be re-controlled (a shared engine, or a
per-test `SetMaxOpenConns` low enough that `parallel × pool × shards` stays under the cap).

Each fixture owns its own engine *and* its own `TestProxy`, so there is no cross-test sharing to coordinate. `TestProxy`
still guards its handler maps with a `RWMutex` because the engine's own worker goroutines dispatch concurrently while
the test registers handlers; fixture task/graph URLs are namespaced per file (`<fixture>.verify:428/<task>`) by
convention. A fixture asserting over the **full** flow set (`List`/`Purge`/`ShardInfo`) gets a clean database for free
- its engine sees only its own flows.

A fixture needing a **non-default topology** (multiple `SetShard`s, `SetTimeBudget`, a specific `SetWorkers` count) or
**host singletons** (multi-replica `AddPeer`/peers, a custom host wrapping `TestProxy`) configures
them on its own engine/proxy before `RunInTest` - the same per-test ownership, just with non-default knobs.
