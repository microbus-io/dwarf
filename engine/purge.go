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
	"sync/atomic"
	"time"

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

// deleteArchivedRoots deletes the trees of the archived roots, each in the day the list read it from, and returns how
// many roots it deleted. Each shard's day goes to the reaper in transactions of about a reaper batch (reapBatchRows and
// reapBatchBytes) and at most purgeChunk roots: 4,096 roots of wide fan-outs would otherwise be millions of rows deleted in one transaction,
// holding the vacuum horizon and the shard's turn for its length. dwarf_flows_removed counts each shard's total once,
// what committed before a failure included.
func (e *Engine) deleteArchivedRoots(ctx context.Context, roots []listRow) (int, error) {
	set := e.archives.Load()
	if set == nil || len(roots) == 0 {
		return 0, nil
	}
	r := e.reaper.Load()
	until := time.Now().Add(e.archiveDeleteInCall)
	var deleted atomic.Int64
	err := e.onShardsOf(ctx, roots, func(ctx context.Context, db *sequel.DB, rows []listRow) error {
		shardDeleted := 0
		defer func() {
			deleted.Add(int64(shardDeleted))
			e.metricFlowsRemoved(ctx, rows[0].shard, removedFromArchive, "purge", shardDeleted)
		}()
		// Each row carries the day its list branch read it from.
		byDay := map[int][]int{}
		for _, lr := range rows {
			byDay[lr.day] = append(byDay[lr.day], lr.flowID)
		}
		for day, dayIDs := range byDay {
			n, err := r.DeleteArchivedRoots(ctx, db, day, dayIDs, e.purgeChunk, e.reapBatchRows, e.reapBatchBytes, until)
			shardDeleted += n
			if err != nil {
				return errors.Trace(err)
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
