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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// The points TestProcessKill_DurableBoundaries kills its child process at.
const (
	killExecuting        = "executing"         // the victim's task is running: its claim is all that is durable
	killBeforeTransition = "before_transition" // the victim's completion is written, its transition not begun
	killAfterCommit      = "after_commit"      // the victim's arrival has committed; everything after it dies with the process
	killSpawnParked      = "spawn_parked"      // inside the caller's spawn transaction: the caller parked, no child inserted
	killBeforeRevive     = "before_revive"     // the child flow's completion has committed, its caller not revived
)

const (
	killBudget      = 2 * time.Second        // every flow's time budget: with the lease margin, how long a dead claim holds
	killLeaseMargin = 500 * time.Millisecond // both processes' lease margin
	killWait        = 3 * time.Second        // the Sleeper's sleep and the Retrier's backoff
)

// killHost serves kill/root: Seed fans out to five branches - Victim, Sleeper then Napper, Retrier, Asker and Caller -
// each step appending its name to "done" once it is through, into Join. Sleeper sleeps, so Napper waits its turn;
// Retrier retries; Asker interrupts; Caller calls a one-task child. At the kill each branch rests in a different state,
// and recovery must leave each to its own rule. Every run of a task is logged to a file both processes append to.
// In the child process, the task the kill point names waits at the gate until its siblings have settled, then arms
// the kill point, and a task that must still be running at the kill holds its worker forever.
type killHost struct {
	mux    *hosts.Mux
	e      *Engine
	child  bool
	point  string
	runLog string
	logMu  sync.Mutex
	atGate chan string   // a task waiting at the gate reports its name
	gate   chan struct{} // closed once the siblings have settled
	held   chan string   // a task holding its worker forever reports its name
}

func newKillHost(child bool, point, runLog string) *killHost {
	k := &killHost{child: child, point: point, runLog: runLog,
		atGate: make(chan string, 8), gate: make(chan struct{}), held: make(chan string, 8)}
	mux := hosts.NewMux()
	root := workflow.NewGraph("Kill")
	branches := []string{"Victim", "Sleeper", "Retrier", "Asker", "Caller"}
	for _, task := range append([]string{"Seed", "Napper", "Join"}, branches...) {
		root.SetURL(task, "kill/"+strings.ToLower(task))
	}
	root.SetFanIn("Join")
	root.SetReducer("done", workflow.ReducerAppend)
	for _, b := range branches {
		root.AddTransition("Seed", b)
	}
	for _, b := range []string{"Victim", "Napper", "Retrier", "Asker", "Caller"} {
		root.AddTransition(b, "Join")
	}
	root.AddTransition("Sleeper", "Napper")
	root.AddTransition("Join", workflow.End)
	mux.HandleGraph("kill/root", root)
	childGraph := workflow.NewGraph("KillChild")
	childGraph.SetURL("Work", "kill/work")
	childGraph.AddTransition("Work", workflow.End)
	mux.HandleGraph("kill/child", childGraph)

	done := func(f *workflow.Flow, task string) error { return f.Set("done", []string{task}) }
	mux.HandleTask("kill/seed", func(ctx context.Context, f *workflow.Flow) error {
		k.log("Seed", f, "run", 0)
		return nil
	})
	mux.HandleTask("kill/victim", func(ctx context.Context, f *workflow.Flow) error {
		k.log("Victim", f, "run", 0)
		if k.child && slices.Contains([]string{killExecuting, killBeforeTransition, killAfterCommit}, k.point) {
			k.waitAtGate("Victim")
			switch k.point {
			case killExecuting:
				k.holdForever("Victim")
			case killBeforeTransition:
				k.e.diags.Seams().Break(diagnostics.CheckpointBeforeTransitionTx)
			case killAfterCommit:
				k.e.diags.Seams().Break(diagnostics.CheckpointClaimedDispatchDone)
			}
		}
		return done(f, "Victim")
	})
	mux.HandleTask("kill/sleeper", func(ctx context.Context, f *workflow.Flow) error {
		k.log("Sleeper", f, "sleep-until", time.Now().Add(killWait).UnixMilli())
		f.Sleep(killWait)
		return done(f, "Sleeper")
	})
	mux.HandleTask("kill/napper", func(ctx context.Context, f *workflow.Flow) error {
		k.log("Napper", f, "run", 0)
		return done(f, "Napper")
	})
	mux.HandleTask("kill/retrier", func(ctx context.Context, f *workflow.Flow) error {
		if f.Attempt() == 0 {
			k.log("Retrier", f, "retry-at", time.Now().Add(killWait).UnixMilli())
			f.Retry(killWait, 1, 0, 0)
			return nil
		}
		k.log("Retrier", f, "run", 0)
		return done(f, "Retrier")
	})
	mux.HandleTask("kill/asker", func(ctx context.Context, f *workflow.Flow) error {
		k.log("Asker", f, "run", 0)
		var answer map[string]any
		yield, err := f.Interrupt(map[string]any{"question": "go on?"}, &answer)
		if yield || err != nil {
			return err
		}
		return done(f, "Asker")
	})
	mux.HandleTask("kill/caller", func(ctx context.Context, f *workflow.Flow) error {
		k.log("Caller", f, "run", 0)
		if k.child && k.point == killSpawnParked {
			k.waitAtGate("Caller")
			k.e.diags.Seams().Break(diagnostics.CheckpointSubgraphSpawnParked)
		}
		var out map[string]any
		yield, err := f.Subgraph("kill/child", nil, &out)
		if yield || err != nil {
			return err
		}
		return done(f, "Caller")
	})
	mux.HandleTask("kill/work", func(ctx context.Context, f *workflow.Flow) error {
		k.log("Work", f, "run", 0)
		if k.child {
			if k.point != killBeforeRevive {
				k.holdForever("Work") // the caller stays parked on a running child
			}
			k.waitAtGate("Work")
			k.e.diags.Seams().Break(diagnostics.CheckpointBeforeReviveWrite)
		}
		return f.Set("worked", true)
	})
	mux.HandleTask("kill/join", func(ctx context.Context, f *workflow.Flow) error {
		k.log("Join", f, "run", 0)
		return nil
	})
	k.mux = mux
	return k
}

// log appends a line for one run of a task to the run log: the task, the event, the process, the attempt, the time
// in Unix milliseconds and the event's argument.
func (k *killHost) log(task string, f *workflow.Flow, event string, arg int64) {
	k.logMu.Lock()
	defer k.logMu.Unlock()
	role := "parent"
	if k.child {
		role = "child"
	}
	file, err := os.OpenFile(k.runLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	fmt.Fprintf(file, "%s %s %s %d %d %d\n", task, event, role, f.Attempt(), time.Now().UnixMilli(), arg)
}

func (k *killHost) waitAtGate(task string) {
	k.atGate <- task
	<-k.gate
}

// holdForever keeps the worker until the process is killed, deaf to the task's deadline: a task that returned at its
// deadline would fail its step before the kill.
func (k *killHost) holdForever(task string) {
	k.held <- task
	select {}
}

// killRun is one line of the run log.
type killRun struct {
	task, event, role string
	attempt           int
	at, arg           int64
}

func readKillRuns(t *testing.T, path string) []killRun {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var runs []killRun
	for line := range strings.Lines(string(data)) {
		var r killRun
		if _, err := fmt.Sscanf(line, "%s %s %s %d %d %d", &r.task, &r.event, &r.role, &r.attempt, &r.at, &r.arg); err != nil {
			t.Fatalf("run log line %q: %v", line, err)
		}
		runs = append(runs, r)
	}
	return runs
}

// killEngine is an engine on dsn outside test mode - its database is the one the test made, shared by the process it
// kills and its replacement - with the host's tasks and a short lease margin.
func killEngine(t *testing.T, dsn string, k *killHost) *Engine {
	t.Helper()
	e := NewEngine()
	k.e = e
	for _, err := range []error{
		e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))),
		e.SetShard(ShardSpec{Index: 1, DSN: dsn, MaxConnections: 8}),
		e.SetHost(k.mux),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	e.leaseMargin = killLeaseMargin
	return e
}

// killDatabase makes a database the child process and its replacement share: a SQLite file, or with SEQUEL_TESTING_DSN
// naming a PostgreSQL server, a database of its own there, dropped when the test ends.
func killDatabase(t *testing.T) (dsn string) {
	t.Helper()
	base := os.Getenv("SEQUEL_TESTING_DSN")
	if base == "" || strings.HasPrefix(base, "file:") {
		return "file:" + filepath.Join(t.TempDir(), "kill.sqlite") + "?_pragma=busy_timeout(5000)"
	}
	if !strings.HasPrefix(base, "postgres") {
		t.Skip("process kills run on SQLite and PostgreSQL")
	}
	return ownPostgresDatabase(t, "kill").String()
}

// ownPostgresDatabase makes a database of its own on the PostgreSQL server SEQUEL_TESTING_DSN names, outside test
// mode - whose database names are salted per process, so no other process could open them - and drops it when the
// test ends. It returns the database's URL.
func ownPostgresDatabase(t *testing.T, prefix string) *url.URL {
	t.Helper()
	u, err := url.Parse(strings.ReplaceAll(os.Getenv("SEQUEL_TESTING_DSN"), "%d", "1"))
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("dwarf_%s_%d_%d", prefix, os.Getpid(), killDatabases.Add(1))
	admin := *u
	admin.Path = "/postgres"
	db, err := sequel.Open("", admin.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "CREATE DATABASE "+name); err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		db.Close()
	})
	u.Path = "/" + name
	return u
}

var killDatabases atomic.Int64

// killChild runs TestProcessKill_Child in a child process on dsn until it reports that it is held at point, then
// kills it with SIGKILL - no deferred function, no Shutdown, no rollback but the database's own - and returns the key
// of the flow it created.
func killChild(t *testing.T, dsn, point, runLog string) (flowKey string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*enginetest.TimeoutScale())
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessKill_Child$", "-test.count=1", "-test.timeout=10m")
	cmd.Env = append(os.Environ(), "DWARF_KILL_POINT="+point, "DWARF_KILL_DSN="+dsn, "DWARF_KILL_LOG="+runLog)
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
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if key, ok := strings.CutPrefix(scanner.Text(), "READY "); ok {
			flowKey = key
			break
		}
		output = append(output, scanner.Text())
	}
	cmd.Process.Kill() // an error is a child that already exited, told below
	cmd.Wait()
	if flowKey == "" || cmd.ProcessState.ExitCode() != -1 {
		t.Fatalf("the child was not killed at %s (exit code %d):\n%s\n%s", point, cmd.ProcessState.ExitCode(),
			strings.Join(output, "\n"), stderr.String())
	}
	return flowKey
}

// TestProcessKill_Child is the process TestProcessKill_DurableBoundaries kills. It creates the flow, waits for its
// branches to settle, lets the victim reach the kill point, reports READY with the flow's key, and waits to be killed.
func TestProcessKill_Child(t *testing.T) {
	point := os.Getenv("DWARF_KILL_POINT")
	if point == "" {
		t.Skip("the process TestProcessKill_DurableBoundaries starts and kills")
	}
	ctx := context.Background()
	k := newKillHost(true, point, os.Getenv("DWARF_KILL_LOG"))
	e := killEngine(t, os.Getenv("DWARF_KILL_DSN"), k)
	e.janitorManual = true // the child recovers nothing; its replacement does
	if err := e.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	flowKey, err := e.Create(ctx, "kill/root", nil, &workflow.FlowOptions{TimeBudget: killBudget})
	if err != nil {
		t.Fatal(err)
	}
	_, _, rootID, _, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}

	// Settled: Napper asleep, Retrier backing off, Asker interrupted, Caller parked on a running child (unless the
	// caller is the victim), and the victim at the gate.
	gated := map[string]string{killExecuting: "Victim", killBeforeTransition: "Victim", killAfterCommit: "Victim",
		killSpawnParked: "Caller", killBeforeRevive: "Work"}[point]
	atGate, held := map[string]bool{}, map[string]bool{}
	deadline := time.Now().Add(30 * time.Second)
	for {
		for drained := false; !drained; {
			select {
			case task := <-k.atGate:
				atGate[task] = true
			case task := <-k.held:
				held[task] = true
			default:
				drained = true
			}
		}
		rows := killRows(t, sh.DB(), rootID)
		settled := rows["Napper"].resting == "pending future" &&
			rows["Retrier"].resting == "pending future" &&
			rows["Asker"].status == workflow.StatusInterrupted &&
			atGate[gated]
		if point != killSpawnParked {
			settled = settled && rows["Caller"].parked == 1
		}
		if point != killSpawnParked && point != killBeforeRevive {
			settled = settled && held["Work"]
		}
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the flow never settled: %v, at the gate %v, held %v", rows, atGate, held)
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(k.gate)

	if point == killExecuting {
		select {
		case task := <-k.held:
			if task != "Victim" {
				t.Fatalf("%s held, not the victim", task)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the victim never held its worker")
		}
	} else {
		cp := map[string]string{
			killBeforeTransition: diagnostics.CheckpointBeforeTransitionTx,
			killAfterCommit:      diagnostics.CheckpointClaimedDispatchDone,
			killSpawnParked:      diagnostics.CheckpointSubgraphSpawnParked,
			killBeforeRevive:     diagnostics.CheckpointBeforeReviveWrite,
		}[point]
		if !e.diags.Seams().WaitTimeout(ctx, cp, 30*time.Second) {
			t.Fatalf("the victim never reached %s", cp)
		}
	}
	fmt.Printf("READY %s\n", flowKey)
	time.Sleep(2 * time.Minute)
	t.Fatal("never killed")
}

// killRow is one step's state, by task name.
type killRow struct {
	status   string
	parked   int
	advance  int
	leaseSeq int
	resting  string // "pending future" for a pending step not due yet
}

// killRows reads the state of every step of the tree rooted at rootID, by task name.
func killRows(t *testing.T, db *sequel.DB, rootID int) map[string]killRow {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		"SELECT s.task_name, s.status, s.parked, s.advancing, s.lease_seq, CASE WHEN s.not_before>NOW_UTC() THEN 1 ELSE 0 END"+
			" FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE f.root_flow_id=? ORDER BY s.step_id", rootID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]killRow{}
	for rows.Next() {
		var task string
		var r killRow
		var future int
		if err := rows.Scan(&task, &r.status, &r.parked, &r.advance, &r.leaseSeq, &future); err != nil {
			t.Fatal(err)
		}
		r.resting = r.status
		if r.status == workflow.StatusPending && future == 1 {
			r.resting = "pending future"
		}
		out[task] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestProcessKill_DurableBoundaries kills a real process at a durable boundary and has a replacement replica on the
// same database finish its work, with no repair but the engine's own. The process runs a flow whose branches rest
// in different states at the kill - asleep, backing off a retry, interrupted, parked on a running child - beside the
// victim, held at the kill point: its task running, its completion written before its transition, its arrival
// committed, inside its spawn's transaction, or its child completed before the delivery. It is killed with SIGKILL.
//
// The replacement must bring every obligation home once its lease and recovery conditions are met: each branch
// arrives once (a duplicate durable contribution would fold its name twice), the sleeping and retrying steps run no
// earlier than their deadlines, the interrupted step does not run again until Resume, the caller has one child, every
// outcome recorded at the kill is unchanged, and the database holds the invariants. Repeated task runs are allowed:
// execution is at-least-once.
func TestProcessKill_DurableBoundaries(t *testing.T) {
	t.Parallel()
	for _, point := range []string{killExecuting, killBeforeTransition, killAfterCommit, killSpawnParked, killBeforeRevive} {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			killAndRecover(t, point)
		})
	}
}

func killAndRecover(t *testing.T, point string) {
	assert := testarossa.For(t)
	ctx := context.Background()
	dsn := killDatabase(t)
	runLog := filepath.Join(t.TempDir(), "runs.log")
	flowKey := killChild(t, dsn, point, runLog)
	killed := time.Now()
	_, _, rootID, _, err := keys.ParseFlowKey(flowKey)
	assert.Must(assert.NoError(err))

	// The observer: a connection of the test's own, which reads the database and writes nothing.
	observer, err := sequel.Open("", dsn)
	assert.Must(assert.NoError(err))
	defer observer.Close()
	children := func() (n int) {
		assert.NoError(observer.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id=?", rootID).Scan(&n))
		return n
	}

	// What the kill left: the victim where its kill point holds it, every sibling resting.
	rows := killRows(t, observer, rootID)
	t.Logf("at the kill: %+v, %d child flows", rows, children())
	assert.Equal("pending future", rows["Napper"].resting, "Napper sleeps")
	assert.Equal("pending future", rows["Retrier"].resting, "Retrier backs off")
	assert.Equal(workflow.StatusInterrupted, rows["Asker"].status)
	switch point {
	case killExecuting:
		assert.Equal(workflow.StatusRunning, rows["Victim"].status)
	case killBeforeTransition:
		assert.Equal(workflow.StatusCompleted, rows["Victim"].status)
		assert.Equal(1, rows["Victim"].advance, "the victim's transition has not committed")
	case killAfterCommit:
		assert.Equal(workflow.StatusCompleted, rows["Victim"].status)
		assert.Equal(0, rows["Victim"].advance, "the victim's arrival has committed")
	case killSpawnParked:
		assert.Equal(workflow.StatusRunning, rows["Caller"].status)
		assert.Equal(0, rows["Caller"].parked, "the spawn's transaction rolled back with the process")
		assert.Zero(children())
	case killBeforeRevive:
		assert.Equal(workflow.StatusCompleted, rows["Work"].status)
		assert.Equal(1, rows["Caller"].parked, "the caller is not revived")
	}
	if point != killSpawnParked {
		assert.Equal(1, rows["Caller"].parked)
		assert.Equal(1, children())
	}
	atKill := enginetest.CaptureOutcomesIn(t, map[int]*sequel.DB{1: observer})

	// The replacement: a fresh engine on the database, its recovery jobs running in the background on a short interval,
	// and a grace that keeps the closed tree live until the test has read it.
	k := newKillHost(false, point, runLog)
	e := killEngine(t, dsn, k)
	e.recoveryInterval = 200 * time.Millisecond
	e.parkWedgeThreshold = time.Second
	e.archiveGrace = time.Hour
	assert.Must(assert.NoError(e.Startup(ctx)))
	defer e.Shutdown(ctx)

	// Every branch but Asker's arrives once its lease, its sleep or its backoff has run out.
	arrived := func() (n int) {
		assert.NoError(observer.QueryRowContext(ctx, "SELECT cohort_arrivals FROM dwarf_steps WHERE flow_id=? AND task_name='Seed'", rootID).Scan(&n))
		return n
	}
	assert.Must(assert.True(enginetest.WaitUntil(t, time.Minute, func() bool { return arrived() == 4 }),
		"the branches never arrived: %d of 4, %+v", arrived(), killRows(t, observer, rootID)))
	t.Logf("four branches arrived %s after the kill", time.Since(killed).Round(time.Millisecond))
	askedBefore := 0
	for _, r := range readKillRuns(t, runLog) {
		if r.task == "Asker" {
			askedBefore++
		}
	}
	assert.Equal(1, askedBefore, "the interrupted step ran again before its Resume")

	assert.NoError(e.Resume(ctx, flowKey, map[string]any{"answer": "yes"}))
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, flowKey)
	assert.Must(assert.NoError(err))
	assert.Equal(workflow.StatusCompleted, out.Status)
	got := out.State.GetStrings("done")
	slices.Sort(got)
	assert.Equal([]string{"Asker", "Caller", "Napper", "Retrier", "Sleeper", "Victim"}, got, "a branch's contribution landed twice or not at all")
	assert.Equal(1, children(), "the caller has one child")

	// Neither waiting step ran before its deadline. The deadline is logged before the step's write, which stamps it on
	// the database clock a moment later, so a run is never before it.
	var sleepUntil, retryAt int64
	runs := map[string]int{}
	for _, r := range readKillRuns(t, runLog) {
		runs[r.task+" "+r.role]++
		switch {
		case r.event == "sleep-until":
			sleepUntil = max(sleepUntil, r.arg)
		case r.event == "retry-at":
			retryAt = max(retryAt, r.arg)
		case r.task == "Napper":
			assert.True(sleepUntil > 0 && r.at >= sleepUntil, "Napper ran at %d, before the sleep it follows ended at %d", r.at, sleepUntil)
		case r.task == "Retrier":
			assert.True(retryAt > 0 && r.at >= retryAt, "Retrier ran again at %d, before its backoff ended at %d", r.at, retryAt)
		}
	}
	t.Logf("task runs: %v", runs)

	removed := enginetest.AssertOutcomesKept(t, e, atKill)
	assert.Len(removed, 0, "nothing removes a flow here")
	enginetest.AssertInvariants(t, e)
}
