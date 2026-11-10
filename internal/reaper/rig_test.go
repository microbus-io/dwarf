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

package reaper

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
)

// rig is a Reaper over one shard's own isolated, migrated database and its archive, with the settings a pass takes.
type rig struct {
	reaper  *Reaper
	db      *sequel.DB
	archive *archiver.Archive
	seams   *seamster.Seamster
	logs    *logCounter
	cfg     Config
}

// newRig stands up a rig whose closed roots are due at once, with an archive of days of dayLength (24h when 0) and
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
	db, err := set.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	length := archiver.DefaultDayLength
	if len(dayLength) > 0 && dayLength[0] > 0 {
		length = dayLength[0]
	}
	a := archiver.New(db, length, migrations.Archive())
	if err := a.EnsureToday(ctx); err != nil {
		t.Fatal(err)
	}
	r := &rig{
		reaper:  New(),
		db:      db,
		archive: a,
		seams:   seamster.New(true),
		logs:    &logCounter{seen: map[string]int{}},
		cfg: Config{
			Batch:         512,
			BatchRows:     4096,
			Attempt:       30 * time.Second,
			Slice:         5 * time.Second,
			Budget:        30 * time.Second,
			StealGrace:    2 * time.Hour,
			RetentionDays: retention,
			NoQuietMargin: true,
		},
	}
	r.reaper.SetLogger(slog.New(r.logs))
	r.reaper.SetSeams(r.seams)
	return r
}

// shard is the rig's shard, as a pass takes it.
func (r *rig) shard() Shard {
	return Shard{Index: 1, DB: r.db, Archive: r.archive}
}

// p is one pass over the rig's shard with its settings as they stand.
func (r *rig) p() *pass {
	return &pass{r: r.reaper, cfg: r.cfg, shard: r.shard()}
}

// pass runs a Pass over the rig's shard.
func (r *rig) pass(ctx context.Context) {
	r.reaper.Pass(ctx, r.cfg, r.shard())
}

// moveInto archives the tree rooted at rootID into day.
func moveInto(t *testing.T, r *rig, rootID, day int) {
	t.Helper()
	moved, err := r.reaper.MoveTrees(t.Context(), r.db, r.archive, day, []int{rootID})
	if err != nil {
		t.Fatal(err)
	}
	if moved != 2 {
		t.Fatalf("moved %d flows of tree %d, want the root and its child", moved, rootID)
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

// stampTree marks the tree rooted at root for deletion, due at once, as Delete does.
func stampTree(t *testing.T, db *sequel.DB, root int) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), "UPDATE dwarf_flows SET delete_after_ms=1 WHERE flow_id=?", root); err != nil {
		t.Fatal(err)
	}
}

// rowsOf returns how many flows of the tree rooted at root are live, and whether the root is among them.
func rowsOf(t *testing.T, r *rig, root int) (flows, steps int, rootThere bool) {
	t.Helper()
	flows, steps = treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	var n int
	if err := r.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", root).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return flows, steps, n == 1
}
