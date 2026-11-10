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
	"slices"
	"strconv"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// DeleteArchivedTree deletes, in one transaction on the caller's context, the archived tree rooted at root in day's
// tables: the directory's rows for its flows, its steps and its flows. It returns 1 when it deleted the tree and 0 when
// the tree was not there; err is an undefined-table error when the day's tables are gone. It locks the root first, FOR
// UPDATE or by flipping its touch column, so a caller's conflicting lock on the root holds the deletion back.
func (r *Reaper) DeleteArchivedTree(ctx context.Context, db *sequel.DB, day, root int) (deleted int, err error) {
	return r.deleteArchived(ctx, db, day, []int{root})
}

// DeleteArchivedRoots deletes the archived trees rooted at roots in day's tables, on the caller's context, in
// transactions of at most chunk roots whose trees fit in about rows rows, and at least one tree each. It returns how
// many trees it deleted, those of the transactions that committed before an error included. A day whose tables are
// gone deletes nothing more and is no error: its trees are gone with it. A chunk below 1 is taken as 1. It locks each
// root as DeleteArchivedTree does.
func (r *Reaper) DeleteArchivedRoots(ctx context.Context, db *sequel.DB, day int, roots []int, chunk, rows int) (deleted int, err error) {
	chunk = max(chunk, 1)
	t := tables{flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day)}
	ids := slices.Sorted(slices.Values(roots))
	for len(ids) > 0 {
		cut, _, err := rootsWithinRowsIn(ctx, db, t, ids[:min(chunk, len(ids))], rows)
		if sequel.IsUndefinedTableError(err) {
			return deleted, nil
		}
		if err != nil {
			return deleted, errors.Trace(err)
		}
		n, err := r.deleteArchived(ctx, db, day, cut)
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

// deleteArchived deletes, in one transaction, the trees in day's tables whose roots are among roots and still there:
// the roots locked first, then the directory's rows for their flows, their steps and their flows. It returns how many
// roots it deleted.
func (r *Reaper) deleteArchived(ctx context.Context, db *sequel.DB, day int, roots []int) (int, error) {
	flows, steps := migrations.FlowsTable(day), migrations.StepsTable(day)
	var locked []int
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		var err error
		locked, err = lockRoots(ctx, tx, flows, false, "flow_id IN ("+intCSV(roots)+") AND surgraph_flow_id=0")
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
		if r.seams.Enabled() {
			for _, root := range locked {
				if r.seams.IsFault(join(FaultArchiveDelete, strconv.Itoa(root))) {
					return errors.New("injected fault: " + FaultArchiveDelete)
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err) // rolled back: nothing deleted
	}
	return len(locked), nil
}
