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

package janitor

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/metric"
)

// rig is a Manual, active Janitor with every job the engine adds - fast, sweep and large-tree archiving, fast and sweep
// deletion, live retention - over one shard's own isolated,
// migrated database and its archive. Its janitor is made at its first use, so a test sets what it needs before; the
// settings and the residue class are set on the jobs and the shard before every pass, so a test changes them between
// passes.
type rig struct {
	t         *testing.T
	db        *sequel.DB
	shard     *database.Shard
	archive   *archiver.Archive
	seams     *seamster.Seamster
	logs      *logCounter
	retention int
	settings  rigSettings
	count     int
	ordinal   int
	inClass   bool
	meter     metric.Meter
	j         *Janitor
	fa        *FastArchiving
	sa        *SweepArchiving
	la        *LargeArchiving
	fd        *FastDeletion
	sd        *SweepDeletion
	lr        *LiveRetention
}

// rigSettings are the jobs' settings a test sets on the rig.
type rigSettings struct {
	grace, overdue                    time.Duration
	batchRoots, batchRows, batchBytes int
	stageRows, stageBytes             int
	noQuietMargin                     bool
}

// newRig stands up a rig whose closed roots are due at once - and a sweep's once overdue by its default window - with an
// archive of days of dayLength (24h when 0) and
// today's tables made, and a retention of retention days.
func newRig(t *testing.T, retention int, dayLength ...time.Duration) *rig {
	t.Helper()
	ctx := t.Context()
	var set database.ShardSet
	err := set.Open(context.Background(), database.Config{
		Shards:      map[int]database.ShardConfig{1: {MaxIdleConns: 2, MaxOpenConns: 8}},
		TestID:      database.TestID(t.Name()),
		TestConnCap: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(set.Close)
	sh, err := set.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	length := archiver.DefaultDayLength
	if len(dayLength) > 0 && dayLength[0] > 0 {
		length = dayLength[0]
	}
	a := archiver.New(db, length, migrations.Archive())
	if err := a.EnsureToday(ctx); err != nil {
		t.Fatal(err)
	}
	return &rig{
		t:         t,
		db:        db,
		shard:     database.NewShard(1, db),
		archive:   a,
		seams:     seamster.New(true),
		logs:      &logCounter{seen: map[string]int{}},
		retention: retention,
		settings: rigSettings{
			overdue:       DefaultSweepOverdue,
			batchRoots:    512,
			batchRows:     4096,
			noQuietMargin: true,
		},
	}
}

// janitor is the rig's janitor, made with its jobs, started and resumed at its first call, with the rig's settings
// set on the jobs and its residue class on the shard. A Manual janitor runs no goroutine, so the settings are set
// between rounds.
func (r *rig) janitor() *Janitor {
	if r.j == nil {
		d := diagnostics.New()
		d.SetLogger(slog.New(r.logs))
		d.SetSeams(r.seams)
		d.SetMeter(r.meter)
		r.j = New(r.shard, d)
		r.fa = NewFastArchiving(r.archive, r.retention)
		r.sa = NewSweepArchiving(r.fa, r.archive, r.retention)
		r.la = NewLargeArchiving(r.archive, r.retention)
		r.fd = NewFastDeletion()
		r.sd = NewSweepDeletion(r.fd)
		r.lr = NewLiveRetention(r.archive, r.retention)
		r.j.AddJob(JobFastArchiving, r.fa)
		r.j.AddJob(JobSweepArchiving, r.sa)
		r.j.AddJob(JobLargeArchiving, r.la)
		r.j.AddJob(JobFastDeletion, r.fd)
		r.j.AddJob(JobSweepDeletion, r.sd)
		r.j.AddJob(JobLiveRetention, r.lr)
		r.j.SetManual(true)
		r.j.Start(r.t.Context())
		r.j.Resume()
		r.t.Cleanup(r.j.Stop)
	}
	s := r.settings
	r.fa.SetGrace(s.grace)
	r.fa.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	r.fa.SetStaging(s.stageRows, s.stageBytes)
	r.fa.SetQuietMargin(!s.noQuietMargin)
	r.sa.SetGrace(s.grace)
	r.sa.SetOverdue(s.overdue)
	r.sa.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	r.sa.SetStaging(s.stageRows, s.stageBytes)
	r.sa.SetQuietMargin(!s.noQuietMargin)
	r.la.SetBatch(s.batchRows, s.batchBytes)
	r.la.SetQuietMargin(!s.noQuietMargin)
	r.fd.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	r.sd.SetOverdue(s.overdue)
	r.sd.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	r.lr.SetGrace(s.grace)
	r.lr.SetBatch(s.batchRoots)
	r.shard.SetResidency(r.count, r.ordinal, r.inClass, time.Now().Add(time.Hour))
	return r.j
}

// setResidency sets the residue class the shard reports from the next pass on.
func (r *rig) setResidency(count, ordinal int, ok bool) {
	r.count, r.ordinal, r.inClass = count, ordinal, ok
}

// restart replaces the rig's janitor with a new one, which knows nothing: as a restart or a handoff does.
func (r *rig) restart() {
	if r.j != nil {
		r.j.Stop()
		r.j = nil
	}
}

// fast is the rig's fast archiving job.
func (r *rig) fast() *FastArchiving {
	r.janitor()
	return r.fa
}

// sweep is the rig's sweep archiving job.
func (r *rig) sweep() *SweepArchiving {
	r.janitor()
	return r.sa
}

// large is the rig's large-tree archiving job.
func (r *rig) large() *LargeArchiving {
	r.janitor()
	return r.la
}

// roundOf runs one round of the rig's job named name, and reports whether it made progress.
func (r *rig) roundOf(ctx context.Context, name string) bool {
	j := r.janitor()
	return j.round(ctx, j.jobs[slices.IndexFunc(j.jobs, func(rn *runningJob) bool { return rn.name == name })])
}

// chunk runs one round of large-tree archiving: one chunk of the next tree with an attempt. It reports progress.
func (r *rig) chunk(ctx context.Context) bool {
	return r.roundOf(ctx, JobLargeArchiving)
}

// copier returns the chunk round of a second large-tree archiving job on the rig's shard, in a janitor of its own - a
// second replica - with the rig's batch.
func (r *rig) copier() func(ctx context.Context) bool {
	d := diagnostics.New()
	d.SetLogger(slog.New(r.logs))
	d.SetSeams(r.seams)
	j := New(r.shard, d)
	la := NewLargeArchiving(r.archive, r.retention)
	la.SetBatch(r.settings.batchRows, r.settings.batchBytes)
	la.SetQuietMargin(!r.settings.noQuietMargin)
	j.AddJob(JobLargeArchiving, la)
	j.SetManual(true)
	j.Start(r.t.Context())
	j.Resume()
	r.t.Cleanup(j.Stop)
	return func(ctx context.Context) bool { return j.round(ctx, j.jobs[0]) }
}

// sweepDel is the rig's sweep deletion job.
func (r *rig) sweepDel() *SweepDeletion {
	r.janitor()
	return r.sd
}

// liveRetention is the rig's live retention job.
func (r *rig) liveRetention() *LiveRetention {
	r.janitor()
	return r.lr
}

// del is the rig's fast deletion job.
func (r *rig) del() *FastDeletion {
	r.janitor()
	return r.fd
}

// pass runs the rig's jobs until they make no progress.
func (r *rig) pass(ctx context.Context) {
	r.janitor().Drain(ctx)
}

// failing is how long archiving has been failing, as fast archiving reports it at now.
func (r *rig) failing(now time.Time) time.Duration {
	return r.fast().failingFor(now)
}

// moveInto archives the tree rooted at rootID into day.
func moveInto(t *testing.T, r *rig, rootID, day int) {
	t.Helper()
	due, dueArgs := closedDueSQL(0)
	moved, err := move(t.Context(), r.db, r.seams, []int{rootID}, due, dueArgs, &filingDay{day: day, archive: r.archive})
	if err != nil {
		t.Fatal(err)
	}
	if moved.flows != 2 {
		t.Fatalf("moved %d flows of tree %d, want the root and its child", moved.flows, rootID)
	}
}

// located returns the day LocateFlow finds a flow key's flow in.
func located(t *testing.T, r *rig, flowKey string) (int, error) {
	t.Helper()
	_, hour, id, _, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		t.Fatal(err)
	}
	return r.archive.LocateFlow(t.Context(), id, hour)
}

// makeArchiveDay makes day's archive tables.
func makeArchiveDay(t *testing.T, r *rig, day int) error {
	t.Helper()
	dayFS, err := migrations.ArchiveDay(migrations.Archive(), day)
	if err != nil {
		return err
	}
	return r.db.Migrate(migrations.SequenceName(day), dayFS)
}

// treeCount returns how many flows and steps of the tree rooted at rootID the two tables hold.
func treeCount(t *testing.T, r *rig, flowsTable, stepsTable string, rootID int) (flows, steps int) {
	t.Helper()
	ctx := t.Context()
	flows, steps = -1, -1
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+flowsTable+" WHERE root_flow_id=?", rootID).Scan(&flows); err != nil {
		t.Fatal(err)
	}
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+stepsTable+" WHERE flow_id IN (SELECT flow_id FROM "+
		flowsTable+" WHERE root_flow_id=?)", rootID).Scan(&steps); err != nil {
		t.Fatal(err)
	}
	return flows, steps
}

// columnNames returns a table's columns in order.
func columnNames(t *testing.T, db *sequel.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT * FROM "+table+" WHERE 1=0")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	return cols
}

// backdate moves a root's updated_at ago into the past.
func backdate(t *testing.T, r *rig, rootID int, ago time.Duration) {
	t.Helper()
	_, err := r.db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		-ago.Milliseconds(), rootID)
	if err != nil {
		t.Fatal(err)
	}
}

// ascendingCloses gives roots closes in their order, a minute apart, all past any grace, so a walk meets them in order.
func ascendingCloses(t *testing.T, r *rig, roots []int) {
	t.Helper()
	for i, id := range roots {
		backdate(t, r, id, time.Duration(len(roots)+10-i)*time.Minute)
	}
}

// logCounter is a slog.Handler that counts the records carrying each message.
type logCounter struct {
	mu   sync.Mutex
	seen map[string]int
}

func (l *logCounter) Enabled(context.Context, slog.Level) bool { return true }
func (l *logCounter) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logCounter) WithGroup(string) slog.Handler            { return l }
func (l *logCounter) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	l.seen[r.Message]++
	l.mu.Unlock()
	return nil
}

// count returns how many records carried a message starting with prefix.
func (l *logCounter) count(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for msg, c := range l.seen {
		if strings.HasPrefix(msg, prefix) {
			n += c
		}
	}
	return n
}

// treeShape is a closed tree to seed: a root of rootSteps steps whose last step called a child flow of childSteps
// steps, or no child when childSteps is negative. A child of no steps is a flow the move counts as one row.
type treeShape struct {
	rootSteps, childSteps int
}

var (
	// callShape is a root of one step that called a child of one step: two flows, two steps.
	callShape = treeShape{rootSteps: 1, childSteps: 1}
	// chunkShape is a root of five steps whose last called a child of two: two flows, seven steps.
	chunkShape = treeShape{rootSteps: 5, childSteps: 2}
	// soloShape is a tree of one flow of one step, so consecutive roots take consecutive ids.
	soloShape = treeShape{rootSteps: 1, childSteps: -1}
)

// seedTrees inserts n closed trees of shape on db, one after another, and returns their roots in that order. Every flow
// is completed now by the database clock and every step completed, as a tree the engine ran to its end would be;
// created_hour takes its column default, the database clock's hour.
func seedTrees(t *testing.T, db *sequel.DB, n int, shape treeShape) []int {
	t.Helper()
	ctx := t.Context()
	const token = "0123456789abcdef"
	flow := func(tx *sequel.Tx, root, surgraphFlow, surgraphStep int) (int, int, error) {
		id, err := tx.InsertReturnID(ctx, "flow_id",
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status,"+
				" surgraph_flow_id, surgraph_step_id, root_flow_id) VALUES (?,?,?,?,?,'"+workflow.StatusCompleted+"',?,?,?)",
			token, "seed/tree", "Seed", []byte("{}"), "", surgraphFlow, surgraphStep, root) // graph bound: binary on SQL Server
		if err != nil {
			return 0, 0, err
		}
		var hour int
		err = tx.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", id).Scan(&hour)
		return int(id), hour, err
	}
	steps := func(tx *sequel.Tx, flowID, hour, count int) (last int, err error) {
		for i := range count {
			id, err := tx.InsertReturnID(ctx, "step_id",
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms,"+
					" flow_created_hour) VALUES (?,?,?,?,?,'"+workflow.StatusCompleted+"',60000,?)",
				flowID, i+1, token, "T"+strconv.Itoa(i+1), "seed/t", hour)
			if err != nil {
				return 0, err
			}
			last = int(id)
		}
		return last, nil
	}
	roots := make([]int, 0, n)
	for range n {
		var root int
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			id, hour, err := flow(tx, 0, 0, 0)
			if err != nil {
				return err
			}
			root = id
			if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET root_flow_id=? WHERE flow_id=?", root, root); err != nil {
				return err
			}
			caller, err := steps(tx, root, hour, shape.rootSteps)
			if err != nil || shape.childSteps < 0 {
				return err
			}
			child, childHour, err := flow(tx, root, root, caller)
			if err != nil {
				return err
			}
			_, err = steps(tx, child, childHour, shape.childSteps)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}
	return roots
}

// closeTreesAgo moves the close of every flow of the trees rooted at roots to ago before now.
func closeTreesAgo(t *testing.T, db *sequel.DB, ago time.Duration, roots ...int) {
	t.Helper()
	for _, root := range roots {
		if _, err := db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE root_flow_id=?",
			-ago.Milliseconds(), root); err != nil {
			t.Fatal(err)
		}
	}
}

// chunkTree seeds a closed tree of two flows and seven steps on db, closed a minute ago, and returns its root.
func chunkTree(t *testing.T, db *sequel.DB) int {
	t.Helper()
	root := seedTrees(t, db, 1, chunkShape)[0]
	closeTreesAgo(t, db, time.Minute, root)
	return root
}

// seedWideTree inserts a closed tree on db of a root of rootSteps steps with children flows of steps steps each, and
// returns its root. The steps go in by multi-row inserts, so a tree of tens of thousands of rows seeds quickly.
func seedWideTree(t *testing.T, db *sequel.DB, rootSteps, children, steps int) int {
	t.Helper()
	ctx := t.Context()
	const token = "0123456789abcdef"
	var root int
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		insertFlow := func(rootID, surgraph int) (int, int, error) {
			id, err := tx.InsertReturnID(ctx, "flow_id",
				"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status,"+
					" surgraph_flow_id, surgraph_step_id, root_flow_id) VALUES (?,?,?,?,?,'"+workflow.StatusCompleted+"',?,0,?)",
				token, "seed/wide", "Wide", []byte("{}"), "", surgraph, rootID)
			if err != nil {
				return 0, 0, err
			}
			var hour int
			err = tx.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", id).Scan(&hour)
			return int(id), hour, err
		}
		insertSteps := func(flowID, hour, n int) error {
			for n > 0 {
				k := min(n, 200)
				row := "(" + strconv.Itoa(flowID) + ",1,'" + token + "','T','seed/t','" + workflow.StatusCompleted + "',60000," +
					strconv.Itoa(hour) + ")"
				if _, err := tx.ExecContext(ctx, "INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name,"+
					" task_url, status, time_budget_ms, flow_created_hour) VALUES "+strings.TrimSuffix(strings.Repeat(row+",", k), ",")); err != nil {
					return err
				}
				n -= k
			}
			return nil
		}
		id, hour, err := insertFlow(0, 0)
		if err != nil {
			return err
		}
		root = id
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET root_flow_id=? WHERE flow_id=?", root, root); err != nil {
			return err
		}
		if err := insertSteps(root, hour, rootSteps); err != nil {
			return err
		}
		for range children {
			child, childHour, err := insertFlow(root, root)
			if err != nil {
				return err
			}
			if err := insertSteps(child, childHour, steps); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// setStepBytes gives every step of root's tree a payload of n bytes, as its byte counts say, and its flows one of
// flowBytes.
func setStepBytes(t *testing.T, r *rig, root, n, flowBytes int) {
	t.Helper()
	ctx := t.Context()
	if _, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET state_bytes=? WHERE flow_id IN"+
		" (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?)", n, root); err != nil {
		t.Fatal(err)
	}
	if _, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET graph_bytes=? WHERE root_flow_id=?", flowBytes, root); err != nil {
		t.Fatal(err)
	}
}

// rowsOf returns how many flows and steps of the tree rooted at root are live, and whether the root is among them.
func rowsOf(t *testing.T, r *rig, root int) (flows, steps int, rootThere bool) {
	t.Helper()
	flows, steps = treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	var n int
	if err := r.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", root).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return flows, steps, n == 1
}

// stampTree marks the tree rooted at root for deletion, as Delete does, and backdates it a second: a stamp in the
// millisecond the tree was seeded is not due yet on the database clock.
func stampTree(t *testing.T, db *sequel.DB, root int) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), "UPDATE dwarf_flows SET delete_after_ms=1, updated_at=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", root); err != nil {
		t.Fatal(err)
	}
}
