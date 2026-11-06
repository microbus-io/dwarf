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
	"sync/atomic"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// purge removes the closed roots matching query from the archive, oldest archived first, at most query.Limit of them:
// the internal ascending ListClosed, each tree deleted at once in its day. It reads only the archive, as ListClosed
// does, so a call removes what the same ListClosed would list; a root closed in the last minute, not archived yet, is
// left for a later call, and Delete removes one by its key. done is true when the archive had fewer than it took and
// every root it took was removed.
func (e *Engine) purge(ctx context.Context, query workflow.Query) (removed int, done bool, err error) {
	// A subgraph child cannot be purged independently - it is removed as part of its root's subtree. Reject
	// IncludeSubgraphs rather than silently ignoring it, so a caller is not surprised the flag did nothing.
	if query.IncludeSubgraphs {
		return 0, false, errors.New("purge cannot include subgraphs; a subgraph child is purged with its root", http.StatusBadRequest)
	}
	// Purge removes only closed flows, so a status filter naming any other status could only ever match nothing.
	if query.Status != "" && workflow.IsValidStatus(query.Status) && !workflow.IsTerminalStatus(query.Status) {
		return 0, false, errors.New("purge removes only closed flows; status %q is not closed", query.Status, http.StatusBadRequest)
	}
	limit, err := listLimit(query)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	archived, err := e.listArchived(ctx, query, limit, true, "purge")
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	gone, err := e.deleteArchivedRoots(ctx, archived)
	if err != nil {
		return gone, false, errors.Trace(err)
	}
	return gone, purgeDone(limit, len(archived), gone), nil
}

// purgeDone reports whether a Purge call left nothing archived that matches, given its limit, the roots it took from
// the archive and the roots it removed of them: it took fewer than the limit, and removed every one - a root a racing
// Delete took first was not, and a call after finds whether anything is left.
func purgeDone(limit, took, gone int) bool {
	return took < limit && gone == took
}

// deleteArchivedRoots deletes the trees of the archived roots, each in the day the list read it from, a transaction
// per run of trees that fits in about reapBatchRows rows (at most purgeChunk roots), and returns how many roots it
// deleted.
func (e *Engine) deleteArchivedRoots(ctx context.Context, roots []listRow) (int, error) {
	set := e.archives.Load()
	if set == nil || len(roots) == 0 {
		return 0, nil
	}
	var deleted atomic.Int64
	err := e.onShardsOf(ctx, roots, func(ctx context.Context, db *sequel.DB, rows []listRow) error {
		// Each row carries the day its list branch read it from.
		byDay := map[int][]int{}
		for _, lr := range rows {
			byDay[lr.day] = append(byDay[lr.day], lr.flowID)
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
				e.metricFlowsRemoved(ctx, rows[0].shard, removedFromArchive, "purge", n)
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
