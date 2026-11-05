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
	"net/http"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// purge removes the ended roots matching query, oldest first, at most query.Limit of them: the archived ones first,
// from the internal ascending ListArchived, each deleted at once in its day; then, with what the limit leaves, the ended
// roots still in the live tables, the oldest ended first, each marked for the reaper. Each side windows on its own
// column, archived_at and updated_at. done is true when neither side had more than it took and every root it took
// was removed: a root archived between the live selection and its mark is not, and is found in the archive next time.
func (e *Engine) purge(ctx context.Context, query workflow.Query) (removed int, done bool, err error) {
	// A subgraph child cannot be purged independently - it is removed as part of its root's subtree. Reject
	// IncludeSubgraphs rather than silently ignoring it, so a caller is not surprised the flag did nothing.
	if query.IncludeSubgraphs {
		return 0, false, errors.New("purge cannot include subgraphs; a subgraph child is purged with its root", http.StatusBadRequest)
	}
	// Purge removes only ended flows, so a status filter naming any other status could only ever match nothing.
	if query.Status != "" && workflow.IsValidStatus(query.Status) && !workflow.IsTerminalStatus(query.Status) {
		return 0, false, errors.New("purge removes only ended flows; status %q has not ended", query.Status, http.StatusBadRequest)
	}
	limit, err := listLimit(query)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if _, err := listWindowOf(query, true); err != nil {
		return 0, false, errors.Trace(err)
	}

	archived, err := e.listArchived(ctx, query, limit, true, "purge")
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	took, gone := len(archived), 0
	n, err := e.deleteArchivedRoots(ctx, archived)
	gone += n
	if err != nil {
		return gone, false, errors.Trace(err)
	}
	if took == limit {
		return gone, false, nil // the archive may hold more
	}

	live, err := e.oldestLiveEndedRoots(ctx, query, limit-took)
	if err != nil {
		return gone, false, errors.Trace(err)
	}
	n, err = e.markLiveRoots(ctx, live)
	gone += n
	if err != nil {
		return gone, false, errors.Trace(err)
	}
	return gone, len(live) < limit-took && gone == took+len(live), nil
}

// oldestLiveEndedRoots returns the ended roots in the live tables matching query, not marked for deletion, the oldest
// ended first across shards, at most limit of them.
func (e *Engine) oldestLiveEndedRoots(ctx context.Context, query workflow.Query, limit int) ([]listRow, error) {
	indices, pos := e.shardOrdinals()
	perShard := make([][]listRow, len(indices))
	err := e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if query.Shard != 0 && shardIdx != query.Shard {
			return nil
		}
		ctx, doneTurn := e.opTurn(ctx, shardIdx, "purge")
		defer doneTurn()
		conds, args := e.listFilters(db.DriverName(), shardIdx, query, terminalStatusesSQL)
		timeConds, timeArgs := timeConditions(db.DriverName(), "f.updated_at", query)
		conds, args = append(conds, timeConds...), append(append(args, timeArgs...), limit)
		// Keep the terminal filter in the selection, not only in the mark: the selection is limited, so unended
		// candidates selected ahead of ended ones would use up the limit and mark nothing.
		rows, err := db.QueryContext(ctx, "SELECT f.flow_id, f.updated_at FROM dwarf_flows f"+stepsJoin("dwarf_steps")+
			" WHERE "+strings.Join(conds, " AND ")+" ORDER BY f.updated_at ASC, f.flow_id ASC LIMIT_OFFSET(?, 0)", args...)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		for rows.Next() {
			lr := listRow{shard: shardIdx}
			if err := rows.Scan(&lr.flowID, &lr.at); err != nil {
				return errors.Trace(err)
			}
			perShard[pos[shardIdx]] = append(perShard[pos[shardIdx]], lr)
		}
		return errors.Trace(rows.Err())
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	merged := mergeByTime(slices.Concat(perShard...), true)
	return merged[:min(len(merged), limit)], nil
}

// markLiveRoots stamps the ended live roots for the reaper (delete_after_ms=1, due at once), a chunk of purgeChunk at
// a time, and returns how many it stamped. A root that has been archived since, or stamped by a racing Delete or
// Purge, is not counted.
func (e *Engine) markLiveRoots(ctx context.Context, roots []listRow) (int, error) {
	var marked atomic.Int64
	err := e.onShardsOf(ctx, roots, func(ctx context.Context, db *sequel.DB, rows []listRow) error {
		ids := make([]int, len(rows))
		for i, lr := range rows {
			ids[i] = lr.flowID
		}
		for chunk := range slices.Chunk(ids, e.purgeChunk) {
			// ids are trusted integers embedded as literals to dodge the per-driver bind-param ceiling.
			res, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1"+
				" WHERE flow_id IN ("+intCSV(chunk)+") AND status IN ("+terminalStatusesSQL+") AND delete_after_ms=0")
			if err != nil {
				return errors.Trace(err)
			}
			n, _ := res.RowsAffected()
			marked.Add(n)
		}
		return nil
	})
	return int(marked.Load()), errors.Trace(err)
}

// deleteArchivedRoots deletes the trees of the archived roots, each in the day its archived_at falls in, a transaction
// per run of trees that fits in about reapBatchRows rows (at most purgeChunk roots), and returns how many roots it
// deleted.
func (e *Engine) deleteArchivedRoots(ctx context.Context, roots []listRow) (int, error) {
	set := e.archives.Load()
	if set == nil || len(roots) == 0 {
		return 0, nil
	}
	var deleted atomic.Int64
	err := e.onShardsOf(ctx, roots, func(ctx context.Context, db *sequel.DB, rows []listRow) error {
		// The day a tree is in is the day its archived_at falls in: the reaper stamps no time past the end of the day
		// it files in.
		byDay := map[int][]int{}
		for _, lr := range rows {
			d := archiver.DayOf(lr.at, set.dayLength)
			byDay[d] = append(byDay[d], lr.flowID)
		}
		for day, dayIDs := range byDay {
			slices.Sort(dayIDs)
			t := flowTables{flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day)}
			// A transaction takes the leading trees that fit in about reapBatchRows rows, and at least one, as the
			// reaper's does: 4,096 roots of wide fan-outs would otherwise be millions of rows deleted in one
			// transaction, holding the vacuum horizon and the shard's turn for its length.
			for len(dayIDs) > 0 {
				cut, err := e.rootsWithinRowsIn(ctx, db, t, dayIDs[:min(e.purgeChunk, len(dayIDs))])
				if sequel.IsUndefinedTableError(err) {
					break // the day was dropped since the list: its trees are gone already
				}
				if err != nil {
					return errors.Trace(err)
				}
				n, err := deleteArchivedTrees(ctx, db, day, cut)
				deleted.Add(int64(n))
				if sequel.IsUndefinedTableError(err) {
					break
				}
				if err != nil {
					return errors.Trace(err)
				}
				dayIDs = dayIDs[len(cut):]
			}
		}
		return nil
	})
	return int(deleted.Load()), errors.Trace(err)
}

// onShardsOf runs fn on each shard that holds any of rows, with the rows on it, in parallel, each on a turn of its own.
func (e *Engine) onShardsOf(ctx context.Context, rows []listRow, fn func(ctx context.Context, db *sequel.DB, rows []listRow) error) error {
	byShard := map[int][]listRow{}
	for _, lr := range rows {
		byShard[lr.shard] = append(byShard[lr.shard], lr)
	}
	if len(byShard) == 0 {
		return nil
	}
	return errors.Trace(e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		mine := byShard[shardIdx]
		if len(mine) == 0 {
			return nil
		}
		ctx, doneTurn := e.opTurn(ctx, shardIdx, "purge")
		defer doneTurn()
		return fn(ctx, db, mine)
	}))
}

// deleteArchivedTrees deletes, in one transaction, the trees in day's archive tables whose roots are among roots and
// still there: the roots locked first - the lock Fork takes on an archived source, so a Fork and a deletion of one tree
// serialize - then the directory's rows for their flows, their steps and their flows. It returns how many roots it
// deleted.
func deleteArchivedTrees(ctx context.Context, db *sequel.DB, day int, roots []int) (int, error) {
	flows, steps := migrations.FlowsTable(day), migrations.StepsTable(day)
	var locked []int
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		var err error
		locked, err = lockRootsIn(ctx, tx, flows, "flow_id IN ("+intCSV(roots)+") AND surgraph_flow_id=0")
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
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err) // rolled back: nothing deleted
	}
	return len(locked), nil
}
