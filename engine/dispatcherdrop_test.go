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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// How TestDispatcherDrop_TheJanitorsKeepEveryFlow works. Two engines dispatch shard 1, each running its janitor's jobs
// in the background with the graces, the sweeps' window and the recovery interval cut short, while both create a stream
// of flows: linear, a small fan-out, a failing one, a subgraph caller, and some with DeleteOnCompletion. Every tree
// over four rows is staged two rows a chunk, so a staged copy takes several transactions. The survivor's large-tree
// archiving is held before its first batch and the other engine's after a chunk that leaves a copy part written, so
// when that engine drops out a staged copy only it wrote is part written. It shuts down ("shutdown", every engine in
// test mode, on any dialect), or it is a child process killed with SIGKILL while it holds a task ("killed", on a
// database the test makes, SQLite or PostgreSQL), so its lease lapses and its dispatch evidence goes stale until the
// survivor takes its residue class. A fresh engine then starts under the dropped engine's id and joins, and both stream
// again. Once every flow has closed and the janitors have emptied the live tables, every flow is where its shape puts
// it - archived once with the outcome its shape computes, or deleted - no archive day holds a flow twice, and no
// janitor job failed on any engine.
//
// It does not catch the move's root lock removed on PostgreSQL: the second of two moves of one root waits on the
// first's row locks in its DELETE ... RETURNING, then matches nothing, and nothing here races the move's recheck. On
// SQLite the killed arm fails, as a SQLITE_BUSY job failure. Catching it on PostgreSQL takes a Fork or a Delete racing
// archiving.

const (
	dropEngineID    = 7_900_001                                // the dropped engine's id, and its replacement's
	dropBudget      = 2 * time.Second                          // every flow's time budget: with the lease margin, how long a dead claim holds
	dropPace        = 20 * time.Millisecond                    // between one engine's creates
	dropAfterRejoin = 30                                       // the flows each engine creates once the replacement has joined
	dropCeiling     = 60 * time.Second                         // the "did it hang" ceiling on each wait
	dropHeldChunk   = diagnostics.CheckpointReapStageChunkDone // where the dropped engine's large-tree archiving is held
	dropBaseA       = 0                                        // the survivor's flows are numbered from here,
	dropBaseB       = 7 * 100_000                              // the dropped engine's from here,
	dropBaseRejoin  = 7 * 200_000                              // its replacement's from here,
	dropBaseHeld    = 7 * 300_000                              // and the killed process's held flows from here, each a multiple of 7: a line
)

// dropShape is one shape of the stream's flows, chosen by the flow's number.
type dropShape struct {
	url          string
	deleteOnDone bool
	flows, steps int // the tree's rows, once archived
	fails        bool
}

// dropShapes are the stream's shapes, the flow numbered i taking dropShapes[i%7].
var dropShapes = []dropShape{
	{url: "drop/line", flows: 1, steps: 3},
	{url: "drop/fan", flows: 1, steps: 5},
	{url: "drop/fail", flows: 1, steps: 2, fails: true},
	{url: "drop/sub", flows: 2, steps: 3},
	{url: "drop/line", deleteOnDone: true, flows: 1, steps: 3},
	{url: "drop/fan", deleteOnDone: true, flows: 1, steps: 5},
	{url: "drop/fail", deleteOnDone: true, flows: 1, steps: 2, fails: true},
}

func dropShapeOf(i int) dropShape { return dropShapes[i%len(dropShapes)] }

// gone reports whether a flow of the shape is deleted rather than archived: DeleteOnCompletion keeps a flow that fails.
func (s dropShape) gone() bool { return s.deleteOnDone && !s.fails }

// dropMux serves the stream's flows. Every task takes a moment, so each engine has work in flight when one drops out.
// In the killed process, the entry task of a flow created with "hold" reports on held and keeps its worker until the
// process dies; anywhere else it runs as any other.
func dropMux(child bool, held chan<- struct{}) *hosts.Mux {
	mux := hosts.NewMux()
	graph := func(name, url string, tasks ...string) *workflow.Graph {
		g := workflow.NewGraph(name)
		for _, task := range tasks {
			g.SetURL(task, "drop/"+strings.ToLower(task))
		}
		mux.HandleGraph(url, g)
		return g
	}
	graph("DropLine", "drop/line", "Line1", "Line2", "Line3").AddTransitionChain("Line1", "Line2", "Line3", workflow.End)
	fan := graph("DropFan", "drop/fan", "Seed", "X", "Y", "Z", "Join")
	fan.SetFanIn("Join")
	fan.SetReducer("sum", workflow.ReducerAdd)
	for _, branch := range []string{"X", "Y", "Z"} {
		fan.AddTransition("Seed", branch)
		fan.AddTransition(branch, "Join")
	}
	fan.AddTransition("Join", workflow.End)
	graph("DropFail", "drop/fail", "Start", "Boom").AddTransitionChain("Start", "Boom", workflow.End)
	graph("DropSub", "drop/sub", "Call", "After").AddTransitionChain("Call", "After", workflow.End)
	graph("DropChild", "drop/child", "Work").AddTransition("Work", workflow.End)

	task := func(name string, fn func(f *workflow.Flow, n int) error) {
		mux.HandleTask("drop/"+strings.ToLower(name), func(ctx context.Context, f *workflow.Flow) error {
			time.Sleep(2 * time.Millisecond) // the task's work
			return fn(f, f.GetInt("n"))
		})
	}
	task("Line1", func(f *workflow.Flow, n int) error {
		if child && f.GetBool("hold") {
			held <- struct{}{}
			select {} // the worker is this process's until it is killed
		}
		f.SetInt("a", n+1)
		return nil
	})
	task("Line2", func(f *workflow.Flow, n int) error { f.SetInt("b", 2*f.GetInt("a")); return nil })
	task("Line3", func(f *workflow.Flow, n int) error { f.SetInt("c", f.GetInt("b")+3); return nil })
	task("Seed", func(f *workflow.Flow, n int) error { return nil })
	for k, branch := range []string{"X", "Y", "Z"} {
		task(branch, func(f *workflow.Flow, n int) error { f.SetInt("sum", n+k+1); return nil })
	}
	task("Join", func(f *workflow.Flow, n int) error { f.SetInt("total", f.GetInt("sum")); return nil })
	task("Start", func(f *workflow.Flow, n int) error { f.SetInt("s", n); return nil })
	task("Boom", func(f *workflow.Flow, n int) error { return errors.New("boom") })
	task("Call", func(f *workflow.Flow, n int) error {
		var out struct {
			W int `json:"w"`
		}
		yield, err := f.Subgraph("drop/child", map[string]any{"n": n}, &out)
		if yield || err != nil {
			return err
		}
		f.SetInt("w", out.W)
		return nil
	})
	task("After", func(f *workflow.Flow, n int) error { f.SetInt("done", f.GetInt("w")+1); return nil })
	task("Work", func(f *workflow.Flow, n int) error { f.SetInt("w", 10*n); return nil })
	return mux
}

// assertDropOutcome asserts the outcome the shape of the flow numbered i computes.
func assertDropOutcome(t *testing.T, i int, key string, out *workflow.FlowOutcome) {
	t.Helper()
	assert := testarossa.For(t)
	s, n := dropShapeOf(i), i
	if s.fails {
		assert.Equal(workflow.StatusFailed, out.Status, "flow %d (%s)", i, key)
		assert.Contains(out.Error, "boom", "flow %d (%s)", i, key)
		assert.Equal(n, out.State.GetInt("s"), "flow %d (%s)", i, key)
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status, "flow %d (%s): %s", i, key, out.Error)
	want := map[string]map[string]int{
		"drop/line": {"a": n + 1, "b": 2 * (n + 1), "c": 2*(n+1) + 3},
		"drop/fan":  {"sum": 3*n + 6, "total": 3*n + 6},
		"drop/sub":  {"w": 10 * n, "done": 10*n + 1},
	}[s.url]
	for field, v := range want {
		assert.Equal(v, out.State.GetInt(field), "flow %d (%s): %s", i, key, field)
	}
}

// dropEngine is an engine for TestDispatcherDrop: in test mode under name when dsn is empty, or on dsn outside test
// mode, where another process reaches the database too. Its janitors run in the background, with the graces, the
// sweeps' window and the recovery interval cut short, and every tree over four rows staged two rows a chunk.
func dropEngine(t *testing.T, name, dsn string) (*Engine, *sdkmetric.ManualReader) {
	t.Helper()
	var e *Engine
	if dsn == "" {
		e = NewEngineUnderTest(name)
	} else {
		e = NewEngine()
		for _, err := range []error{
			e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))),
			e.SetShard(ShardSpec{Index: 1, DSN: dsn, MaxConnections: 8}),
		} {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	reader := sdkmetric.NewManualReader()
	if err := e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))); err != nil {
		t.Fatal(err)
	}
	e.janitorManual = false
	e.janitorIdle = 20 * time.Millisecond
	e.noReapQuiet = true
	e.archiveGrace, e.deletionGrace, e.sweepOverdue = 200*time.Millisecond, 200*time.Millisecond, 100*time.Millisecond
	e.recoveryInterval = 500 * time.Millisecond
	e.leaseMargin = 500 * time.Millisecond
	e.reapBatchRows, e.reapStageRows = 2, 4
	return e, reader
}

// dropFlows is every flow the test knows was created, its key to its number.
type dropFlows struct {
	mu   sync.Mutex
	byID map[string]int
}

func (d *dropFlows) record(i int, key string) {
	d.mu.Lock()
	d.byID[key] = i
	d.mu.Unlock()
}

// dropStream creates the stream's flows on one engine, one every dropPace, numbered from its base, until halted.
type dropStream struct {
	e       *Engine
	next    atomic.Int64
	count   atomic.Int64
	record  func(i int, key string)
	stop    chan struct{}
	done    chan struct{}
	mu      sync.Mutex
	failure error
}

func startDropStream(e *Engine, base int, record func(i int, key string)) *dropStream {
	s := &dropStream{e: e, record: record, stop: make(chan struct{}), done: make(chan struct{})}
	s.next.Store(int64(base))
	go func() {
		defer close(s.done)
		for {
			select {
			case <-s.stop:
				return
			case <-time.After(dropPace):
			}
			i := int(s.next.Add(1) - 1)
			shape := dropShapeOf(i)
			key, err := e.Create(context.Background(), shape.url, map[string]any{"n": i},
				&workflow.FlowOptions{TimeBudget: dropBudget, DeleteOnCompletion: shape.deleteOnDone})
			if err != nil {
				s.mu.Lock()
				s.failure = errors.Join(s.failure, err)
				s.mu.Unlock()
				continue
			}
			s.record(i, key)
			s.count.Add(1)
		}
	}()
	return s
}

// halt stops the stream and returns once it has, with the errors its creates returned.
func (s *dropStream) halt() error {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}

// awaitDividing waits until e's residue class on shard 1 divides the shard between two dispatchers or, with want false,
// divides nothing while e reads the registry: e dispatches it alone.
func awaitDividing(t *testing.T, e *Engine, want bool) {
	t.Helper()
	assert := testarossa.For(t)
	ok := enginetest.WaitUntil(t, dropCeiling, func() bool {
		n, _, divides := e.sonarFor(1).Partition()
		return divides == want && (want && n == 2 || !want && !e.blindOn(1))
	})
	assert.Must(assert.True(ok, "the residue class never came to divide=%v", want))
}

// dropDays returns the archive days of shard 1.
func dropDays(t *testing.T, e *Engine) []int {
	t.Helper()
	a := e.archiveOn(1)
	if err := a.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	return a.DaysIn(math.MinInt, math.MaxInt)
}

// halfCopied counts the live trees whose staged copy is under way and part written: the root has an archive attempt
// and is not yet published, and an archive day already holds some of the tree's steps.
func halfCopied(t *testing.T, e *Engine) int {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	n := 0
	for _, day := range dropDays(t, e) {
		var trees int
		err := db.QueryRowContext(t.Context(), "SELECT COUNT(DISTINCT f.root_flow_id) FROM dwarf_flows f JOIN "+
			migrations.StepsTable(day)+" s ON s.flow_id=f.flow_id WHERE f.root_flow_id IN (SELECT flow_id FROM dwarf_flows"+
			" WHERE "+janitor.AttemptedSQL(db.DriverName())+" AND delete_after_ms=0)").Scan(&trees)
		if err != nil {
			t.Fatal(err)
		}
		n += trees
	}
	return n
}

// holdLargeArchiving holds e's large-tree archiving jobs at their first batch's turn, before they read or copy anything,
// until release.
func holdLargeArchiving(e *Engine) (release func()) {
	var names []string
	for k := 1; k <= largeArchivingJobs; k++ {
		name := diagnostics.Join(diagnostics.CheckpointJanitorBatchTurn, janitor.ShareName(janitor.JobLargeArchiving, k))
		e.diags.Seams().Break(name)
		names = append(names, name)
	}
	return func() {
		for _, name := range names {
			e.diags.Seams().Resume(name)
		}
	}
}

// dropCounts reads what an engine's janitor has archived and deleted: the flows of the trees moved into the archive
// or published there, and of the trees deleted without archiving.
func dropCounts(t *testing.T, reader *sdkmetric.ManualReader) (archived, deleted int64) {
	t.Helper()
	rm := collect(t, reader)
	return sumAt(rm, "dwarf_flows_archived", nil), sumAt(rm, "dwarf_flows_reaped", map[string]string{"reason": janitor.ReasonDeleted})
}

// awaitDropReady waits until e may drop out: its janitor has archived and deleted trees of its own, and its large-tree
// archiving is held after a chunk while a staged copy is part written - e's, since the survivor's copies are held before
// they begin. The hold is dropHeldChunk's breakpoint, armed before e started; a chunk it held that left no copy part
// written - a publication, or a root another transaction held - is let go, and the next held.
func awaitDropReady(t *testing.T, e *Engine, reader *sdkmetric.ManualReader) (archived, deleted int64) {
	t.Helper()
	assert := testarossa.For(t)
	ok := enginetest.WaitUntil(t, dropCeiling, func() bool {
		archived, deleted = dropCounts(t, reader)
		return archived > 0 && deleted > 0
	})
	assert.Must(assert.True(ok, "the janitor archived %d and deleted %d flows", archived, deleted))
	seams := e.diags.Seams()
	for range 50 {
		assert.Must(assert.True(seams.WaitTimeout(t.Context(), dropHeldChunk, dropCeiling*enginetest.TimeoutScale()),
			"large-tree archiving never committed a chunk"))
		if halfCopied(t, e) > 0 {
			return archived, deleted
		}
		seams.Resume(dropHeldChunk)
		seams.Break(dropHeldChunk)
	}
	t.Fatal("no staged copy was part written at a held chunk")
	return 0, 0
}

// TestDispatcherDrop_TheJanitorsKeepEveryFlow runs two dispatchers of one shard, each with its janitor, under a stream
// of flows; drops one mid-stream - a clean shutdown, or a process killed - and brings a replacement back under its
// engine id. Every flow must end where its shape puts it, once.
func TestDispatcherDrop_TheJanitorsKeepEveryFlow(t *testing.T) {
	t.Parallel()
	t.Run("shutdown", func(t *testing.T) {
		t.Parallel()
		dropByShutdown(t)
	})
	t.Run("killed", func(t *testing.T) {
		t.Parallel()
		dropByKill(t)
	})
}

// dropByShutdown drops an engine of the pair with a clean Shutdown, every engine in test mode.
func dropByShutdown(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	flows := &dropFlows{byID: map[string]int{}}

	a, aReader := dropEngine(t, t.Name(), "")
	defer a.Shutdown(ctx)
	assert.NoError(a.SetHost(dropMux(false, nil)))
	release := holdLargeArchiving(a)
	assert.Must(assert.NoError(a.Startup(ctx)))
	b, bReader := dropEngine(t, t.Name(), "")
	defer b.Shutdown(ctx)
	assert.NoError(b.SetHost(dropMux(false, nil)))
	assert.NoError(b.SetEngineID(dropEngineID))
	b.diags.Seams().Break(dropHeldChunk)
	assert.Must(assert.NoError(b.Startup(ctx)))
	awaitDividing(t, a, true)
	awaitDividing(t, b, true)

	streamA := startDropStream(a, dropBaseA, flows.record)
	defer streamA.halt()
	streamB := startDropStream(b, dropBaseB, flows.record)
	defer streamB.halt()
	archived, deleted := awaitDropReady(t, b, bReader)
	assert.NoError(streamB.halt())
	assert.Zero(b.diags.Seams().Visits(diagnostics.CheckpointJanitorFailed), "the dropped engine's janitor failed")
	assert.NoError(b.Shutdown(ctx))
	t.Logf("dropped after archiving %d and deleting %d flows", archived, deleted)

	rejoined, rejoinedReader := dropEngine(t, t.Name(), "")
	defer rejoined.Shutdown(ctx)
	assert.NoError(rejoined.SetHost(dropMux(false, nil)))
	assert.NoError(rejoined.SetEngineID(dropEngineID))
	dropAndRejoin(t, a, aReader, release, rejoined, rejoinedReader, streamA, flows)
}

// dropByKill drops an engine of the pair by killing its process, outside test mode, on a database the test makes.
func dropByKill(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	flows := &dropFlows{byID: map[string]int{}}
	dsn := killDatabase(t)
	keysPath := filepath.Join(t.TempDir(), "keys")

	a, aReader := dropEngine(t, "", dsn)
	defer a.Shutdown(ctx)
	assert.NoError(a.SetHost(dropMux(false, nil)))
	release := holdLargeArchiving(a)
	assert.Must(assert.NoError(a.Startup(ctx)))
	streamA := startDropStream(a, dropBaseA, flows.record)
	defer streamA.halt()

	archived, deleted, failed := dropChild(t, dsn, keysPath)
	assert.Zero(failed, "the killed engine's janitor failed")
	t.Logf("killed after archiving %d and deleting %d flows", archived, deleted)
	data, err := os.ReadFile(keysPath)
	assert.Must(assert.NoError(err))
	for line := range strings.Lines(string(data)) {
		i, key, ok := strings.Cut(strings.TrimSuffix(line, "\n"), " ")
		n, err := strconv.Atoi(i)
		if ok && err == nil && strings.HasSuffix(line, "\n") {
			flows.record(n, key)
		}
	}

	rejoined, rejoinedReader := dropEngine(t, "", dsn)
	defer rejoined.Shutdown(ctx)
	assert.NoError(rejoined.SetHost(dropMux(false, nil)))
	assert.NoError(rejoined.SetEngineID(dropEngineID))
	dropAndRejoin(t, a, aReader, release, rejoined, rejoinedReader, streamA, flows)
}

// dropAndRejoin goes on from the moment the dropped engine is gone: the survivor a, still streaming on streamA, its
// large-tree archiving held until release, takes the shard alone; the replacement, built and not yet started, joins
// under the dropped engine's id and streams beside it. Then every flow is held to its shape.
func dropAndRejoin(t *testing.T, a *Engine, aReader *sdkmetric.ManualReader, release func(), rejoined *Engine,
	rejoinedReader *sdkmetric.ManualReader, streamA *dropStream, flows *dropFlows) {
	assert := testarossa.For(t)
	ctx := context.Background()

	assert.True(halfCopied(t, a) > 0, "a staged copy the dropped engine part wrote")
	release()
	awaitDividing(t, a, false)

	assert.Must(assert.NoError(rejoined.Startup(ctx)))
	awaitDividing(t, a, true)
	awaitDividing(t, rejoined, true)
	since := streamA.count.Load()
	streamRejoined := startDropStream(rejoined, dropBaseRejoin, flows.record)
	ok := enginetest.WaitUntil(t, dropCeiling, func() bool {
		return streamA.count.Load() >= since+dropAfterRejoin && streamRejoined.count.Load() >= dropAfterRejoin
	})
	assert.True(ok, "the engines created %d and %d flows after the rejoin", streamA.count.Load()-since, streamRejoined.count.Load())
	assert.NoError(streamA.halt())
	assert.NoError(streamRejoined.halt())

	// Every flow stops - one the killed process created and never reported among them - and the live tables are then
	// at rest.
	flows.mu.Lock()
	byID := maps.Clone(flows.byID)
	flows.mu.Unlock()
	for _, key := range slices.Sorted(maps.Keys(byID)) {
		awaitCtx, cancel := context.WithTimeout(ctx, dropCeiling*enginetest.TimeoutScale())
		_, err := a.Await(awaitCtx, key)
		cancel()
		if dropShapeOf(byID[key]).gone() && errors.StatusCode(err) == http.StatusNotFound {
			continue
		}
		assert.NoError(err, "flow %d (%s)", byID[key], key)
	}
	closed := "'" + strings.Join([]string{workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled}, "','") + "'"
	ok = enginetest.WaitUntil(t, dropCeiling, func() bool {
		return enginetest.CountRows(t, a, 1, "SELECT COUNT(*) FROM dwarf_flows WHERE status NOT IN ("+closed+")") == 0
	})
	assert.Must(assert.True(ok, "flows still open"))
	enginetest.AssertInvariants(t, a)

	// The janitors archive or delete every tree.
	ok = enginetest.WaitUntil(t, dropCeiling, func() bool {
		return enginetest.CountRows(t, a, 1, "SELECT COUNT(*) FROM dwarf_flows") == 0 &&
			enginetest.CountRows(t, a, 1, "SELECT COUNT(*) FROM dwarf_steps") == 0
	})
	assert.Must(assert.True(ok, "%d flows and %d steps still live", enginetest.CountRows(t, a, 1, "SELECT COUNT(*) FROM dwarf_flows"),
		enginetest.CountRows(t, a, 1, "SELECT COUNT(*) FROM dwarf_steps")))

	// What the archive days hold of each tree, by root, and every flow or step held twice across them.
	sh, err := a.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	type tree struct{ flows, steps int }
	trees := map[int]*tree{}
	at := func(root int) *tree {
		if trees[root] == nil {
			trees[root] = &tree{}
		}
		return trees[root]
	}
	flowsSeen, stepsSeen := map[int]int{}, map[int]int{}
	for _, day := range dropDays(t, a) {
		scan := func(query string, each func(id, root int)) {
			rows, err := db.QueryContext(ctx, query)
			assert.Must(assert.NoError(err))
			defer rows.Close()
			for rows.Next() {
				var id, root int
				assert.Must(assert.NoError(rows.Scan(&id, &root)))
				each(id, root)
			}
			assert.NoError(rows.Err())
		}
		scan("SELECT flow_id, root_flow_id FROM "+migrations.FlowsTable(day), func(id, root int) {
			flowsSeen[id]++
			at(root).flows++
		})
		scan("SELECT step_id, flow_id FROM "+migrations.StepsTable(day), func(id, _ int) { stepsSeen[id]++ })
		scan("SELECT s.step_id, f.root_flow_id FROM "+migrations.StepsTable(day)+" s JOIN "+migrations.FlowsTable(day)+
			" f ON f.flow_id=s.flow_id", func(_, root int) { at(root).steps++ })
		var orphans int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+migrations.StepsTable(day)+" s WHERE NOT EXISTS"+
			" (SELECT 1 FROM "+migrations.FlowsTable(day)+" f WHERE f.flow_id=s.flow_id)").Scan(&orphans))
		assert.Zero(orphans, "day %d holds steps without their flow", day)
	}
	for id, n := range flowsSeen {
		assert.Equal(1, n, "flow %d archived %d times", id, n)
	}
	for id, n := range stepsSeen {
		assert.Equal(1, n, "step %d archived %d times", id, n)
	}

	// Each flow, by key and in the archive.
	for _, key := range slices.Sorted(maps.Keys(byID)) {
		i := byID[key]
		shape := dropShapeOf(i)
		_, _, root, _, err := keys.ParseFlowKey(key)
		assert.Must(assert.NoError(err))
		got := at(root)
		out, err := a.Snapshot(ctx, key)
		if shape.gone() {
			assert.Equal(http.StatusNotFound, errors.StatusCode(err), "flow %d (%s) deleted", i, key)
			assert.Equal(tree{}, *got, "flow %d (%s) deleted, not archived", i, key)
			continue
		}
		if assert.NoError(err, "flow %d (%s)", i, key) {
			assertDropOutcome(t, i, key, out)
		}
		assert.Equal(tree{shape.flows, shape.steps}, *got, "flow %d (%s) archived once, whole", i, key)
	}

	// Both engines' janitors ran, each fast archiving job reporting its lag, and none failed.
	driver := db.DriverName()
	for _, r := range []struct {
		name   string
		e      *Engine
		reader *sdkmetric.ManualReader
	}{{"survivor", a, aReader}, {"replacement", rejoined, rejoinedReader}} {
		archived, deleted := dropCounts(t, r.reader)
		assert.True(archived > 0 && deleted > 0, "the %s's janitor archived %d and deleted %d flows", r.name, archived, deleted)
		ok := enginetest.WaitUntil(t, dropCeiling, func() bool {
			rm := collect(t, r.reader)
			for k := 1; k <= fastJobs(fastArchivingJobs, driver); k++ {
				lag, _, found := gaugeOver(rm, "dwarf_archive_lag_seconds", map[string]string{"job": janitor.ShareName(janitor.JobFastArchiving, k)})
				if !found || lag != 0 {
					return false
				}
			}
			return true
		})
		assert.True(ok, "the %s's fast archiving jobs report no lag once nothing is due", r.name)
		assert.Zero(r.e.diags.Seams().Visits(diagnostics.CheckpointJanitorFailed), "the %s's janitor failed", r.name)
	}
}

// dropChild runs TestDispatcherDrop_Child in a child process on dsn until it reports it may drop out, then kills it
// with SIGKILL. It returns what the child's janitor archived and deleted, and how often a job of it failed.
func dropChild(t *testing.T, dsn, keysPath string) (archived, deleted int64, failed int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*dropCeiling*enginetest.TimeoutScale())
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDispatcherDrop_Child$", "-test.count=1", "-test.timeout=10m")
	cmd.Env = append(os.Environ(), "DWARF_TESTING_DROP_DSN="+dsn, "DWARF_TESTING_DROP_KEYS="+keysPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var output []string
	ready := false
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if line, ok := strings.CutPrefix(scanner.Text(), "READY "); ok {
			_, err := fmt.Sscanf(line, "archived=%d deleted=%d failed=%d", &archived, &deleted, &failed)
			ready = err == nil
			break
		}
		output = append(output, scanner.Text())
	}
	cmd.Process.Kill() // an error is a child that already exited, told below
	cmd.Wait()
	if !ready || cmd.ProcessState.ExitCode() != -1 {
		t.Fatalf("the child was not killed ready (exit code %d):\n%s\n%s", cmd.ProcessState.ExitCode(),
			strings.Join(output, "\n"), stderr.String())
	}
	return archived, deleted, failed
}

// TestDispatcherDrop_Child is the process the killed arm of TestDispatcherDrop_TheJanitorsKeepEveryFlow kills: one of the
// pair, streaming beside the parent's engine, its large-tree archiving held. Once it may drop out, it creates a flow
// whose entry task holds its worker, reports READY with what its janitor did, and waits to be killed. Every flow it
// creates goes to the keys file, a line each as Create returns.
func TestDispatcherDrop_Child(t *testing.T) {
	dsn := os.Getenv("DWARF_TESTING_DROP_DSN")
	if dsn == "" {
		t.Skip("the process TestDispatcherDrop_TheJanitorsKeepEveryFlow starts and kills")
	}
	ctx := context.Background()
	file, err := os.OpenFile(os.Getenv("DWARF_TESTING_DROP_KEYS"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	record := func(i int, key string) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(file, "%d %s\n", i, key)
	}
	held := make(chan struct{}, 1)
	e, reader := dropEngine(t, "", dsn)
	for _, err := range []error{e.SetHost(dropMux(true, held)), e.SetEngineID(dropEngineID)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	e.diags.Seams().Break(dropHeldChunk)
	if err := e.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	awaitDividing(t, e, true)
	stream := startDropStream(e, dropBaseB, record)
	archived, deleted := awaitDropReady(t, e, reader)

	// The flow holding a worker is created last, so its time budget has not run out at the kill. The parent's engine may
	// claim its task first and run it through; then another is created, until one holds here.
	for k := 0; ; k++ {
		if k == 20 {
			t.Fatal("the parent's engine claimed every held flow's task")
		}
		i := dropBaseHeld + 7*k
		key, err := e.Create(ctx, "drop/line", map[string]any{"n": i, "hold": true}, &workflow.FlowOptions{TimeBudget: dropBudget})
		if err != nil {
			t.Fatal(err)
		}
		record(i, key)
		awaitCtx, cancel := context.WithTimeout(ctx, dropCeiling*enginetest.TimeoutScale())
		ran := make(chan error, 1)
		go func() {
			_, err := e.Await(awaitCtx, key)
			ran <- err
		}()
		select {
		case <-held:
			cancel()
		case err := <-ran:
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		break
	}
	if err := stream.failureSoFar(); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("READY archived=%d deleted=%d failed=%d\n", archived, deleted, e.diags.Seams().Visits(diagnostics.CheckpointJanitorFailed))
	time.Sleep(2 * time.Minute)
	t.Fatal("never killed")
}

// failureSoFar returns the errors the stream's creates have returned, while it runs on.
func (s *dropStream) failureSoFar() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}
