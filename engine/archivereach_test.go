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
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	merrors "github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// How TestArchiveReach works. Each tree is a root that seeds a large carried document, fans out over a list into a
// fan-in, and calls a one-task subgraph child (reachMux); the child's outcome decides how the tree closes -
// completed, failed, terminated or cancelled - and a child can wait at a named gate (reachGates), so the test decides
// when that tree closes. Every tree is closed and read once before anything moves, by every read on every key it
// has, and the answers are kept (reachTree.want). Readers then loop over every tree and every read while the test
// drains the janitor in waves (reapDueFlows), with faults armed at the moves' steps and two trees still closing; each
// read must answer what it answered before the move, compared by value. Three trees are deleted at different moments,
// and only theirs may go unfound, and once unfound never answer again. The janitor runs only when driven in test
// mode, so the waves are the test's own.

// reachGates holds the gates a tree's child waits on: a child whose gate is named reports the name on started, then
// waits for the gate to open.
type reachGates struct {
	mu      sync.Mutex
	gates   map[string]chan struct{}
	started chan string
}

func newReachGates() *reachGates {
	return &reachGates{gates: map[string]chan struct{}{}, started: make(chan string, 64)}
}

func (g *reachGates) gate(name string) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	ch, ok := g.gates[name]
	if !ok {
		ch = make(chan struct{})
		g.gates[name] = ch
	}
	return ch
}

func (g *reachGates) open(name string) {
	ch := g.gate(name)
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (g *reachGates) openAll() {
	g.mu.Lock()
	names := make([]string, 0, len(g.gates))
	for name := range g.gates {
		names = append(names, name)
	}
	g.mu.Unlock()
	for _, name := range names {
		g.open(name)
	}
}

// awaitStarted waits for the child behind gate name to reach it.
func (g *reachGates) awaitStarted(t *testing.T, name string) {
	t.Helper()
	timeout := time.After(10 * time.Second * enginetest.TimeoutScale())
	for {
		select {
		case got := <-g.started:
			if got == name {
				return
			}
		case <-timeout:
			t.Fatalf("the child behind gate %s never started", name)
		}
	}
}

// reachMux serves the trees TestArchiveReach moves: a root that seeds a document carried by ref, fans out over its
// width into a fan-in that reads the document, then calls a one-task child. The child completes, fails, or first
// waits on the tree's gate, as the root's initial state says.
func reachMux(gates *reachGates) *hosts.Mux {
	mux := hosts.NewMux()
	root := workflow.NewGraph("Reach")
	root.SetURL("Seed", "reach/seed")
	root.SetURL("Branch", "reach/branch")
	root.SetURL("Join", "reach/join")
	root.SetURL("Call", "reach/call")
	root.SetFanIn("Join")
	root.SetReducer("seen", workflow.ReducerAppend)
	root.AddTransitionForEach("Seed", "Branch", "items", "item")
	root.AddTransitionChain("Branch", "Join", "Call", workflow.End)
	mux.HandleGraph("reach/root", root)
	child := workflow.NewGraph("ReachChild")
	child.SetURL("Work", "reach/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("reach/child", child)

	mux.HandleTask("reach/seed", func(ctx context.Context, f *workflow.Flow) error {
		items := make([]int, f.GetInt("width"))
		for i := range items {
			items[i] = i
		}
		if err := f.Set("items", items); err != nil {
			return err
		}
		return f.Set("doc", blob(200_000))
	})
	mux.HandleTask("reach/branch", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("seen", []int{f.GetInt("item")})
	})
	mux.HandleTask("reach/join", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("docLen", len(f.GetString("doc")))
	})
	mux.HandleTask("reach/call", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("reach/child", map[string]any{"mode": f.GetString("mode"), "gate": f.GetString("gate")}, &out)
		if yield || err != nil {
			return err
		}
		return f.Set("got", out["done"])
	})
	mux.HandleTask("reach/work", func(ctx context.Context, f *workflow.Flow) error {
		if name := f.GetString("gate"); name != "" {
			gate := gates.gate(name)
			gates.started <- name
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if f.GetString("mode") == "fail" {
			return errors.New("no luck")
		}
		return f.Set("done", true)
	})
	return mux
}

// reachTree is one tree TestArchiveReach moves, and what its reads returned before it moved.
type reachTree struct {
	mode     string
	width    int
	gate     string
	key      string
	rootID   int
	flowKeys []string       // the root's and its child's
	stepKeys []string       // every step of the tree
	want     map[string]any // by read: op|key
	doomed   atomic.Int64   // the read clock when the test began to Delete the tree; 0 while it has not
}

// readOps are the reads TestArchiveReach runs, by their names in reachTree.want.
var readOps = []string{"snapshot", "fingerprint", "history", "step", "await"}

// read runs op on key.
func (tr *reachTree) read(ctx context.Context, e *Engine, op, key string) (any, error) {
	switch op {
	case "snapshot":
		return e.Snapshot(ctx, key)
	case "fingerprint":
		fp, status, err := e.Fingerprint(ctx, key)
		return [2]string{fp, status}, err
	case "history":
		return e.History(ctx, key)
	case "step":
		return e.Step(ctx, key)
	case "await":
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
		defer cancel()
		return e.Await(awaitCtx, key)
	}
	return nil, fmt.Errorf("no read %q", op)
}

// keysFor returns the keys op reads.
func (tr *reachTree) keysFor(op string) []string {
	switch op {
	case "step":
		return tr.stepKeys
	case "await":
		return tr.flowKeys[:1]
	}
	return tr.flowKeys
}

// capture records what every read of the tree returns.
func (tr *reachTree) capture(t *testing.T, e *Engine) {
	t.Helper()
	ctx := t.Context()
	tr.want = map[string]any{}
	history, err := e.History(ctx, tr.key)
	if err != nil {
		t.Fatal(err)
	}
	tr.stepKeys = stepKeysOf(history)
	for _, op := range readOps {
		for _, k := range tr.keysFor(op) {
			v, err := tr.read(ctx, e, op, k)
			if err != nil {
				t.Fatalf("%s of %s: %v", op, k, err)
			}
			tr.want[op+"|"+k] = v
		}
	}
}

// reachLog is what the readers of TestArchiveReach observed.
type reachLog struct {
	t        *testing.T
	clock    atomic.Int64 // ticks at the start and end of every read, and as a Delete begins
	drains   atomic.Int64 // odd while the test drains the janitors
	reads    [5]atomic.Int64
	during   atomic.Int64 // reads that began and ended inside one drain
	notFound atomic.Int64
	failures atomic.Int64
	goneMu   sync.Mutex
	gone     map[string]int64 // by op|key: the clock at the end of the earliest read that found nothing
}

func (l *reachLog) fail(format string, args ...any) {
	if l.failures.Add(1) <= 10 {
		l.t.Errorf(format, args...)
	}
}

// short is v printed, cut to a length a failure message can carry: a step's state holds a document.
func short(v any) string {
	s := fmt.Sprintf("%+v", v)
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}

// readLoop reads the trees at random until ctx ends, checking each read against what the tree's reads returned before
// it moved. A tree not found is a failure unless the test began to Delete it before the read ended, and a read that
// found nothing is never followed by one, begun after it, that finds the flow again.
func (l *reachLog) readLoop(ctx context.Context, e *Engine, trees *atomic.Pointer[[]*reachTree]) {
	for ctx.Err() == nil {
		all := *trees.Load()
		tr := all[rand.IntN(len(all))]
		opIdx := rand.IntN(len(readOps))
		op := readOps[opIdx]
		ks := tr.keysFor(op)
		key := ks[rand.IntN(len(ks))]
		drain := l.drains.Load()
		start := l.clock.Add(1)
		got, err := tr.read(ctx, e, op, key)
		end := l.clock.Add(1)
		if ctx.Err() != nil {
			return
		}
		l.reads[opIdx].Add(1)
		if drain%2 == 1 && l.drains.Load() == drain {
			l.during.Add(1)
		}
		id := op + "|" + key
		if err != nil {
			if merrors.StatusCode(err) != http.StatusNotFound {
				l.fail("%s of %s: %v", op, key, err)
				continue
			}
			l.notFound.Add(1)
			if d := tr.doomed.Load(); d == 0 || d > end {
				l.fail("%s of %s: not found, and the tree was not deleted", op, key)
				continue
			}
			l.goneMu.Lock()
			if was, ok := l.gone[id]; !ok || end < was {
				l.gone[id] = end
			}
			l.goneMu.Unlock()
			continue
		}
		l.goneMu.Lock()
		was, ok := l.gone[id]
		l.goneMu.Unlock()
		if ok && start > was {
			l.fail("%s of %s: found again after a read found nothing", op, key)
		}
		if want := tr.want[id]; !reflect.DeepEqual(got, want) {
			l.fail("%s of %s: read %s, before the move %s", op, key, short(got), short(want))
		}
	}
}

// delete deletes the tree, having first recorded that every read from now on may find nothing.
func (l *reachLog) delete(ctx context.Context, e *Engine, tr *reachTree) {
	tr.doomed.Store(l.clock.Add(1))
	if err := e.Delete(ctx, tr.key); err != nil {
		l.fail("Delete of %s: %v", tr.key, err)
	}
}

// TestArchiveReach_ReadsHoldWhileTreesMove pins that a closed tree stays reachable, with its outcome unchanged, while
// the janitor moves it out of the live tables: readers run every read of every tree - Snapshot, Fingerprint, History,
// Step and Await, on the root's key and its child's - continuously while the trees move one at a time, and each read
// must return what it returned before the move. The moves take every path a tree leaves by: whole, or staged in
// chunks and published (a tree past the staging threshold), with faults before a move's commit (a batch failing mid
// tree, contention, an archive error, a staging chunk failing) and after it (a move's, a staging chunk's and a
// published live copy's deletion reply lost). Trees close while others move. Three trees are deleted - one live, one
// while the moves run, one archived - and only theirs may go unfound, and never come back once a read found nothing.
// At the end every other tree is in the archive and nowhere live, every outcome recorded before the moves is either
// readable unchanged or deleted, and the live tables hold the invariants. Run with UTC days, and with two-second
// days, so that most trees are archived a day or more after their creation and found through the directory.
func TestArchiveReach_ReadsHoldWhileTreesMove(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		day  time.Duration
	}{{"utc_days", 0}, {"short_days", 2 * time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			readsWhileTreesMove(t, tc.day)
		})
	}
}

func readsWhileTreesMove(t *testing.T, day time.Duration) {
	assert := testarossa.For(t)
	ctx := t.Context()
	gates := newReachGates()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	t.Cleanup(gates.openAll) // runs first, so a failed test frees the workers
	e.SetHost(reachMux(gates))
	e.archiveGrace = 0
	e.sweepOverdue = 0         // a root a failure left behind is retried by the next sweep
	e.reapBatchRows = 10       // a batch holds one small tree (8 rows), so the small trees move one at a time
	e.reapStageRows = 12       // a tree of width 16 (22 rows) is staged, in chunks of the batch
	e.archiveDayOverride = day // 0 is a UTC day
	assert.Must(assert.NoError(e.SetRetention(100)))
	assert.Must(assert.NoError(e.Startup(ctx)))
	enginetest.AwaitVisitCount(t, e.diags.Seams(), 1, 10*time.Second, diagnostics.CheckpointArchivePassDone)

	start := func(tr *reachTree) {
		var err error
		tr.key, err = e.Create(ctx, "reach/root", map[string]any{"width": tr.width, "mode": tr.mode, "gate": tr.gate}, nil)
		assert.Must(assert.NoError(err))
		_, _, tr.rootID, _, err = keys.ParseFlowKey(tr.key)
		assert.Must(assert.NoError(err))
		if tr.gate != "" {
			gates.awaitStarted(t, tr.gate)
		}
	}
	var trees []*reachTree
	for range 2 {
		for _, width := range []int{2, 16} {
			for _, mode := range []string{"ok", "fail", "terminate", "cancel"} {
				tr := &reachTree{mode: mode, width: width}
				if mode == "terminate" || mode == "cancel" {
					tr.gate = "g" + strconv.Itoa(len(trees))
				}
				trees = append(trees, tr)
			}
		}
	}
	for _, tr := range trees {
		start(tr)
		status := workflow.StatusCompleted
		switch tr.mode {
		case "fail":
			status = workflow.StatusFailed
		case "terminate":
			status = workflow.StatusTerminated
			assert.NoError(e.Terminate(ctx, tr.key, "stopped"))
		case "cancel":
			status = workflow.StatusCancelled
			assert.NoError(e.Cancel(ctx, tr.key, "called off"))
			gates.open(tr.gate)
		}
		enginetest.AwaitFlowStatus(t, e, tr.key, status, time.Minute)
		tr.flowKeys = []string{tr.key, childKey(t, e, tr.key)}
		tr.capture(t, e)
	}
	// Two trees close while the others move.
	late := []*reachTree{{mode: "ok", width: 2, gate: "late0"}, {mode: "ok", width: 16, gate: "late1"}}
	for _, tr := range late {
		start(tr)
		tr.flowKeys = []string{tr.key, childKey(t, e, tr.key)}
	}
	before := enginetest.CaptureOutcomes(t, e)

	seams := e.diags.Seams()
	armed := []string{
		diagnostics.Join(diagnostics.FaultJanitorLostResponse, strconv.Itoa(trees[0].rootID)), // a move commits, its reply lost
		diagnostics.Join(diagnostics.FaultJanitorLostResponse, strconv.Itoa(trees[5].rootID)), // a published live copy's deletion
		diagnostics.Join(diagnostics.FaultReapMidTree, strconv.Itoa(trees[2].rootID)),         // a move fails before it commits
		diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(trees[4].rootID)),      // a staging chunk fails before it writes
		diagnostics.Join(diagnostics.FaultReapStageLost, strconv.Itoa(trees[7].rootID)),       // a staging chunk commits, its reply lost
	}
	for _, f := range armed {
		seams.Inject(f)
	}
	seams.InjectN(diagnostics.FaultReapContention, 2)
	seams.InjectN(diagnostics.FaultReapArchiveErr, 2)
	armed = append(armed, diagnostics.FaultReapContention, diagnostics.FaultReapArchiveErr)
	deletedLive, deletedMoving, deletedArchived := trees[8], trees[13], trees[11]

	log := &reachLog{t: t, gone: map[string]int64{}}
	var reading atomic.Pointer[[]*reachTree]
	closed := slices.Clone(trees)
	reading.Store(&closed)
	readCtx, stopReads := context.WithCancel(context.Background())
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() { log.readLoop(readCtx, e, &reading) })
	}
	stop := sync.OnceFunc(func() {
		stopReads()
		readers.Wait()
	})
	t.Cleanup(stop)

	log.delete(ctx, e, deletedLive)
	liveTrees := func() (n int) {
		for _, tr := range append(slices.Clone(trees), late...) {
			flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", tr.rootID)
			n += flows
		}
		return n
	}
	deadline := time.Now().Add(time.Minute * enginetest.TimeoutScale())
	lateRead := false
	for wave := 0; ; wave++ {
		log.drains.Add(1)
		drained := make(chan struct{})
		go func() {
			e.reapDueFlows(ctx)
			close(drained)
		}()
		var deleting sync.WaitGroup
		switch wave {
		case 1:
			for _, tr := range late {
				gates.open(tr.gate)
			}
		case 2:
			deleting.Go(func() { log.delete(ctx, e, deletedMoving) })
		}
		<-drained
		deleting.Wait()
		log.drains.Add(1)
		if wave >= 1 && !lateRead {
			for _, tr := range late {
				enginetest.AwaitFlowStatus(t, e, tr.key, workflow.StatusCompleted, time.Minute)
				tr.capture(t, e)
			}
			all := append(slices.Clone(*reading.Load()), late...)
			reading.Store(&all)
			lateRead = true
		}
		if wave >= 3 && liveTrees() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d flows of the trees still live after %d drains", liveTrees(), wave+1)
		}
	}
	log.delete(ctx, e, deletedArchived)
	log.drains.Add(1)
	e.reapDueFlows(ctx)
	log.drains.Add(1)
	stop()

	// What the readers saw.
	for i, op := range readOps {
		assert.True(log.reads[i].Load() > 0, "no %s read ran", op)
	}
	assert.True(log.during.Load() > 0, "no read ran inside a drain")
	t.Logf("reads: %d snapshot, %d fingerprint, %d history, %d step, %d await; %d inside a drain, %d not found",
		log.reads[0].Load(), log.reads[1].Load(), log.reads[2].Load(), log.reads[3].Load(), log.reads[4].Load(),
		log.during.Load(), log.notFound.Load())
	for _, f := range armed {
		assert.False(seams.IsFault(f), "fault %s never fired", f)
	}
	assert.True(seams.Visits(diagnostics.CheckpointReapStageChunkDone) > 0, "no tree was staged")

	// Every tree at rest: a deleted one not found, every other in the archive and read as it was before it moved.
	all := append(slices.Clone(trees), late...)
	for _, tr := range all {
		if tr.doomed.Load() != 0 {
			for _, op := range []string{"snapshot", "history"} {
				_, err := tr.read(ctx, e, op, tr.key)
				assert.Equal(http.StatusNotFound, merrors.StatusCode(err), "%s of deleted %s", op, tr.key)
			}
			continue
		}
		archivedDay(t, e, tr.key)
		for id, want := range tr.want {
			op, key, _ := strings.Cut(id, "|")
			got, err := tr.read(ctx, e, op, key)
			if assert.NoError(err, id) {
				assert.True(reflect.DeepEqual(want, got), "%s: read %s, before the move %s", id, short(got), short(want))
			}
		}
	}
	if day > 0 {
		sh, err := e.db.Shard(1)
		assert.Must(assert.NoError(err))
		var named int
		assert.NoError(sh.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable).Scan(&named))
		assert.True(named > 0, "no tree was archived far enough from its creation to be named in the directory")
	}

	// Every outcome recorded before the moves left the live tables, each with a tree whose reads were checked above.
	var wantRemoved []string
	for _, tr := range trees {
		wantRemoved = append(wantRemoved, tr.flowKeys...)
	}
	removed := enginetest.AssertOutcomesKept(t, e, before)
	slices.Sort(removed)
	slices.Sort(wantRemoved)
	assert.Equal(wantRemoved, removed)
	enginetest.AssertInvariants(t, e)
}
