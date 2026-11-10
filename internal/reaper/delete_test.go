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
	"slices"
	"strconv"
	"testing"

	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestSQL_TerminalStatusesAreTheClosedOnes pins the literal the reaper's rechecks inline against the workflow
// package's notion of a closed status.
func TestSQL_TerminalStatusesAreTheClosedOnes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, status := range []string{
		workflow.StatusCreated, workflow.StatusRunning, workflow.StatusInterrupted,
		workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled,
	} {
		assert.Equal(workflow.IsTerminalStatus(status), slices.Contains(terminalStatuses, status), status)
	}
}

// TestDelete_ADroppedDayIsNoError pins the archive-side deletion of a day whose tables are gone: nothing deleted and
// no error, since its trees went with it, while a day still there is deleted and counted.
func TestDelete_ADroppedDayIsNoError(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 2, callShape)
	moveInto(t, r, roots[0], today)
	moveInto(t, r, roots[1], today)

	n, err := r.reaper.DeleteArchivedRoots(ctx, r.db, today-5, roots, 4096, 4096) // never made, as a dropped day
	assert.NoError(err)
	assert.Zero(n)
	n, err = r.reaper.DeleteArchivedRoots(ctx, r.db, today, roots[:1], 4096, 4096)
	assert.NoError(err)
	assert.Equal(1, n)
	flows, steps := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), roots[0])
	assert.Zero(flows + steps)
	flows, _ = treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), roots[1])
	assert.Equal(2, flows)
}

// TestDelete_APartialRemovalCountsWhatCommitted pins the archive-side deletion that fails part-way: the trees of the
// transactions that committed are counted with the error, and the failed transaction's trees stay whole.
func TestDelete_APartialRemovalCountsWhatCommitted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 4, callShape)
	for _, root := range roots {
		moveInto(t, r, root, today)
	}
	fault := join(FaultArchiveDelete, strconv.Itoa(roots[2]))
	r.seams.InjectN(fault, 1000)
	defer r.seams.Withdraw(fault)

	n, err := r.reaper.DeleteArchivedRoots(ctx, r.db, today, roots, 4096, 4) // two trees of two rows a transaction
	assert.Error(err)
	assert.Equal(2, n, "the first transaction's trees")
	for i, root := range roots {
		flows, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
		if i < 2 {
			assert.Zero(flows, "tree %d", i)
		} else {
			assert.Equal(2, flows, "tree %d rolled back whole", i)
		}
	}

	n, err = r.reaper.DeleteArchivedTree(ctx, r.db, today, roots[2])
	assert.Error(err, "the fault applies to one tree too")
	assert.Zero(n)
	r.seams.Withdraw(fault)
	n, err = r.reaper.DeleteArchivedTree(ctx, r.db, today, roots[2])
	assert.NoError(err)
	assert.Equal(1, n)
}
