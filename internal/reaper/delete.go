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
	"database/sql"
	"slices"
	"sort"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// DeleteArchivedRoots deletes the archived trees rooted at roots in day's tables, on the caller's context, in
// transactions of at most chunk roots whose trees fit in about rows rows and bytes payload bytes (0, no byte limit). A
// tree larger than that alone is marked,
// which hides it from every read at once, and deleted in chunks until until; what is left is deleted in the background
// (DeleteMarked). It returns how many trees it deleted or marked, those of the transactions that committed before an
// error included, and 0 for a tree that was not there or was marked already. A day whose tables are gone deletes
// nothing more and is no error: its trees are gone with it. A chunk below 1 is taken as 1. Every transaction locks
// the root first, FOR UPDATE or by flipping its touch column, so a caller's conflicting lock on the root holds it
// back.
func (r *Reaper) DeleteArchivedRoots(ctx context.Context, db *sequel.DB, day int, roots []int, chunk, rows, bytes int, until time.Time) (deleted int, err error) {
	chunk = max(chunk, 1)
	batch := capOf(rows, bytes)
	t := tables{flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day)}
	ids := slices.Sorted(slices.Values(roots))
	for len(ids) > 0 {
		cut, first, err := cutWithin(ids[:min(chunk, len(ids))], batch, batch.rows+1, nil, func(group, limits []int) ([]size, error) {
			return treesSizesUpTo(ctx, db, t, group, limits)
		})
		if sequel.IsUndefinedTableError(err) {
			return deleted, nil
		}
		if err != nil {
			return deleted, errors.Trace(err)
		}
		var n int
		if len(cut) == 1 && batch.over(first) {
			n, err = r.markThenDelete(ctx, db, day, cut[0], batch, until)
		} else {
			n, err = r.deleteArchived(ctx, db, day, cut)
		}
		deleted += n
		if sequel.IsUndefinedTableError(err) {
			return deleted, nil
		}
		if err != nil {
			return deleted, errors.Trace(err)
		}
		ids = ids[len(cut):]
	}
	return deleted, nil
}

// markThenDelete marks the archived root for deletion in day, if it is a root not marked yet, and deletes its tree in
// chunks of about batch until until. It returns 1 when it marked the tree. A chunk that fails after the mark is no
// failure of the call: the tree is deleted already as far as every reader is concerned, and DeleteMarked finishes it.
func (r *Reaper) markThenDelete(ctx context.Context, db *sequel.DB, day, root int, batch limit, until time.Time) (int, error) {
	res, err := db.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(day)+" SET delete_after_ms=1 WHERE flow_id=? AND"+
		" surgraph_flow_id=0 AND delete_after_ms=0", root)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, nil
	}
	// The chunks run under until itself, not only between them: a chunk waiting on a lock would otherwise hold the call
	// past it.
	inCall, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	var pos chunkPos
	for inCall.Err() == nil {
		_, done, err := r.chunk(inCall, db, archiveTarget(day), root, batch, &pos)
		if err != nil || done {
			break
		}
	}
	return 1, nil
}

// DeleteMarked deletes, on one shard, the archived trees marked for deletion whose deletion did not finish in the call
// that marked them. It goes day by day through the retained days, finding each day's marked trees by a probe and
// deleting them in chunks of about a batch, until the pass's budget is spent. It starts where the last call
// left off and wraps around, so neither a day whose probe is slow nor a tree whose deletion keeps failing holds back
// the rest, and it gives the work it finds at least one chunk however the budget went. Observe reports marked trees
// left and days not examined separately. It needs the shard's Archive, and does nothing without one. The caller holds
// its turn on the shard.
func (r *Reaper) DeleteMarked(ctx context.Context, cfg Config, s Shard) {
	if s.Archive == nil {
		return
	}
	p := &pass{r: r, cfg: cfg, shard: s}
	start := time.Now()
	spent := func() bool { return time.Since(start) >= cfg.Budget }
	attemptCtx, cancel := p.attemptCtx(ctx)
	today, err := s.Archive.Today(attemptCtx)
	cancel()
	if err != nil {
		p.logError(ctx, "Reaper: reading today for the marked archive trees", "shard", s.Index, "error", err)
		return
	}
	days := s.Archive.DaysIn(today-max(cfg.RetentionDays, 0), today+1)
	state := r.cleanup.get(s.Index)
	defer func() { r.cleanup.put(s.Index, state) }()
	from := state.from
	first := sort.SearchInts(days, from.day)
	left, unknown, worked := 0, 0, false
	// try deletes the marked tree of root in day for what is left of the budget, and at least a chunk when it is the
	// first work of the call.
	try := func(day, root int) {
		if ctx.Err() != nil || (worked && spent()) {
			left++
			return
		}
		state.from = markedTree{day: day, root: root}
		worked = true
		done := false
		var pos chunkPos
		for !done && ctx.Err() == nil {
			chunkCtx, cancel := p.attemptCtx(ctx)
			var err error
			_, done, err = r.chunk(chunkCtx, s.DB, archiveTarget(day), root, capOf(cfg.BatchRows, cfg.BatchBytes), &pos)
			cancel()
			if sequel.IsUndefinedTableError(err) {
				break // the day dropped since its probe: its trees with it
			}
			if err != nil {
				p.logError(ctx, "Reaper: deleting a marked archive tree", "shard", s.Index, "day", day, "root", root, "error", err)
				break
			}
			if spent() {
				break
			}
		}
		if !done || p.stillThere(ctx, day, root) {
			left++ // a chunk is done too when another transaction holds the root
		}
	}
	// The start day's trees at or before where the last call stopped come last, at the end of the cycle.
	var wrapDay int
	var wrap []int
	for n := range days {
		day := days[(first+n)%len(days)]
		if ctx.Err() != nil || (n > 0 && spent()) {
			unknown++ // not reached
			continue
		}
		// The next call starts at the day after this one, unless it stops inside this day's trees.
		state.from = markedTree{day: day + 1}
		roots, err := p.markedIn(ctx, day)
		if sequel.IsUndefinedTableError(err) {
			continue // dropped since the archive's record: its trees with it
		}
		if err != nil {
			p.logError(ctx, "Reaper: finding marked archive trees", "shard", s.Index, "day", day, "error", err)
			unknown++
			continue
		}
		for i, root := range roots {
			if n == 0 && day == from.day && root <= from.root {
				wrapDay, wrap = day, append(wrap, root)
				continue
			}
			try(day, root)
			if i == len(roots)-1 && state.from == (markedTree{day: day, root: root}) {
				state.from = markedTree{day: day + 1}
			}
		}
	}
	// Do not revisit the starting prefix before reaching the other days. With a spent discovery budget that
	// would put the position back on the same held tree every pass.
	if unknown > 0 {
		left += len(wrap)
	} else {
		for _, root := range wrap {
			try(wrapDay, root)
		}
	}
	if ctx.Err() == nil {
		state.pending, state.unknown, state.observed = left, unknown, true
	}
}

// stillThere reports whether day's flows table still holds root, or might: a read that fails is taken for yes.
func (p *pass) stillThere(ctx context.Context, day, root int) bool {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	var one int
	err := p.shard.DB.QueryRowContext(ctx, "SELECT 1 FROM "+migrations.FlowsTable(day)+" WHERE flow_id=?", root).Scan(&one)
	return err != sql.ErrNoRows && !sequel.IsUndefinedTableError(err)
}

// markedTree is a place among the marked archive trees: a day, and a root within it (0 for its start).
type markedTree struct{ day, root int }

// markedIn returns the roots marked for deletion in day, ascending: a probe of the day's deletion index, which holds an
// entry only for a marked root on PostgreSQL, SQLite and SQL Server, and is named on MySQL and MariaDB, where it is
// full and the planner may otherwise walk the primary key. A day made before the index has none, and a hint naming a
// missing index is an error there, so the probe without it - a read of the day's flows table - follows.
func (p *pass) markedIn(ctx context.Context, day int) ([]int, error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	flows := migrations.FlowsTable(day)
	const where = " WHERE delete_after_ms>0 AND surgraph_flow_id=0 ORDER BY flow_id"
	var rows *sequel.Rows
	var err error
	if d := p.shard.DB.DriverName(); d == "mysql" || d == "mariadb" {
		rows, err = p.shard.DB.QueryContext(ctx, "SELECT flow_id FROM "+flows+" FORCE INDEX (idx_"+flows+"_delete_after)"+where)
		if err != nil && !sequel.IsUndefinedTableError(err) && ctx.Err() == nil {
			rows, err = p.shard.DB.QueryContext(ctx, "SELECT flow_id FROM "+flows+where)
		}
	} else {
		rows, err = p.shard.DB.QueryContext(ctx, "SELECT flow_id FROM "+flows+where)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	var roots []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Trace(err)
		}
		roots = append(roots, id)
	}
	return roots, errors.Trace(rows.Err())
}

// deleteArchived deletes, in one transaction, the trees in day's tables whose roots are among roots and still there:
// the roots locked first, then the directory's rows for their flows, their steps and their flows. It returns how many
// roots it deleted.
func (r *Reaper) deleteArchived(ctx context.Context, db *sequel.DB, day int, roots []int) (int, error) {
	flows, steps := migrations.FlowsTable(day), migrations.StepsTable(day)
	var locked []int
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		var err error
		// A marked root is the background's: a selection made before another call marked it, its tree shrunk below a
		// batch since, would otherwise be deleted and counted again.
		locked, err = lockRoots(ctx, tx, flows, false, "flow_id IN ("+intCSV(roots)+") AND surgraph_flow_id=0 AND delete_after_ms=0")
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		// ids are trusted integers embedded as literals to dodge the per-driver bind-param ceiling.
		ids := intCSV(locked)
		trees := "SELECT flow_id FROM " + flows + " WHERE root_flow_id IN (" + ids + ")"
		// The directory and the steps before the flows: their subqueries read the flow rows.
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+archiver.DirectoryTable+" WHERE flow_id IN ("+trees+")"); err != nil {
			return errors.Trace(err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+steps+" WHERE flow_id IN ("+trees+")"); err != nil {
			return errors.Trace(err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+flows+" WHERE root_flow_id IN ("+ids+")"); err != nil {
			return errors.Trace(err)
		}
		// FaultArchiveDelete, targeted at a root, fails the transaction that would delete it, after its statements ran.
		return r.faultOn(FaultArchiveDelete, locked...)
	})
	if err != nil {
		return 0, errors.Trace(err) // rolled back: nothing deleted
	}
	r.seams.Checkpoint(ctx, CheckpointArchiveDeleted)
	return len(locked), nil
}
