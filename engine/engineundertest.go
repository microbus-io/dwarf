/*
Copyright (c) 2026 Microbus LLC and various contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"context"
	"log/slog"
	"math"
	"os"
	"slices"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/seamster"
)

// This file is the engine's test-support surface. NewEngineUnderTest wires an engine for isolated testing,
// and init registers the internal/engineseams accessors that reach the internals a black-box test (one
// in another package, e.g. fixtures) legitimately needs - the shard set for row inspection, the
// instrumentation seams for deterministic race control, and the live tables in every status and an archiving
// pass with no grace, for a test that lists or purges what it just closed. The seam names live in internal/engineseams too, so
// none of this is on the engine's exported surface. A test that needs to drive an unexported method or forge
// internal columns directly (failStep, the refiller, pool sizing, ...) stays a white-box test in package
// engine, where it reaches those directly.

// NewEngineUnderTest constructs an engine wired for testing: isolated, auto-dropped databases keyed by the
// given test name. It takes a name rather than a testing.TB, so a host driving the engine under its
// own harness (one with no *testing.T to hand over) reaches it the same way a Go test does. The caller
// configures the engine (SetHost, SetShard, ...), drives Startup, and owns teardown:
//
//	e := NewEngineUnderTest(t.Name())
//	defer e.Shutdown(ctx)
//	e.SetHost(h)
//	e.Startup(ctx)
//
// The name is the isolation key: engines given the SAME name share one set of isolated databases (which is
// how a multi-replica test gives its peer engines shared state), and engines given DIFFERENT names are
// independent deployments. So a test standing up several INDEPENDENT engines gives each its own name
// (t.Name()+"#"+suffix), and a benchmark reused across warmup/measure passes gives each pass a fresh one.
//
// Logging default: stderr at Error, so a CI failure surfaces the engine's error-level alarms without the
// Info-level play-by-play noise (stderr, not t.Log, because a `go test` timeout
// panic drops buffered t.Log output but not stderr). DWARF_TEST_LOG_LEVEL overrides the level (e.g. "info"
// or "debug" for the flow-status play-by-play; "silent" or "off" for the discard logger). A benchmark or
// fuzz target silences the engine itself with SetLogger, since per-iteration logging would dominate the
// measurement / flood the fuzz output; SetLogger before Startup takes over entirely either way.
func NewEngineUnderTest(testName string) *Engine {
	e := NewEngine()
	level := slog.LevelError
	silent := false
	if s := os.Getenv("DWARF_TEST_LOG_LEVEL"); s != "" {
		switch s {
		case "silent", "off":
			silent = true
		default:
			_ = level.UnmarshalText([]byte(s))
		}
	}
	if silent {
		_ = e.SetLogger(slog.New(slog.DiscardHandler))
	} else {
		_ = e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	}
	// database.TestID hashes the name to a short, bounded id (the SQL identifier limit) and salts it with a
	// per-PROCESS nonce, so concurrent `go test` runs of one package do not derive the same database name and
	// drop it out from under each other. A non-empty testHashedID becomes Config.TestID at Open, which
	// switches the ShardSet's open path onto the isolated-test path; it is set on a fresh engine, so it is in
	// place long before any shard opens.
	e.testHashedID = database.TestID(testName)
	// A pass in a day's last moments archives as at any other time: the quiet margin is pinned by its own test, and
	// left on it would hold back every archiving test that runs then.
	e.noReapQuiet = true
	return e
}

func init() {
	engineseams.DB = func(eng any) *database.ShardSet { return &eng.(*Engine).db }
	engineseams.Seams = func(eng any) *seamster.Seamster { return eng.(*Engine).seams }
	engineseams.SetRefillInterval = func(eng any, d time.Duration) error { return eng.(*Engine).setRefillInterval(d) }
	engineseams.ListLive = func(eng any, ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, error) {
		e := eng.(*Engine)
		if err := e.ensureStarted(); err != nil {
			return nil, err
		}
		return e.listLive(ctx, query, slices.Concat(openStatuses, closedStatuses), "list_open")
	}
	engineseams.ArchiveNow = func(eng any, ctx context.Context) { eng.(*Engine).reapPass(ctx, 0) }
	engineseams.FlowsClosed = func(eng any) int64 { return eng.(*Engine).flowsClosedCount.Load() }
	engineseams.ArchiveDays = func(eng any, ctx context.Context, shard int) ([]int, error) {
		set := eng.(*Engine).archives.Load()
		if set == nil || set.byShard[shard] == nil {
			return nil, nil
		}
		a := set.byShard[shard]
		if err := a.Refresh(ctx); err != nil {
			return nil, err
		}
		return a.DaysIn(math.MinInt, math.MaxInt), nil
	}
	engineseams.ShardBudget = func(rttMs float64, serverMax int) int { return shardBudget(ShardSpec{}, rttMs, serverMax, "pgx") }
}
