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
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestChunkedDeletion_APartlyDeletedTreeReadsAsDeleted pins the readers of a live tree the reaper deletes in chunks:
// a child of a stamped tree is neither summarized nor listed; once its root's stamp is due, a multi-statement read
// (Step, History) of what is left is a 404 rather than a partial result; Snapshot and Fingerprint of the root still
// serve it, the root going last; a repeated Delete is success and a Fork a 409.
func TestChunkedDeletion_APartlyDeletedTreeReadsAsDeleted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.Must(assert.NoError(err))
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	hist, err := e.History(ctx, fk)
	assert.Must(assert.NoError(err))
	var rootStep, childKey string
	for _, s := range hist {
		if rootStep == "" {
			rootStep = s.StepKey
		}
		if s.SubgraphKey != "" {
			childKey = s.SubgraphKey
		}
	}
	assert.Must(assert.True(rootStep != "" && childKey != ""))
	_, err = e.Step(ctx, rootStep)
	assert.NoError(err, "read whole before the delete")
	list := func() int {
		got, err := e.ListRecentlyClosed(ctx, enginetest.Recent(workflow.Query{IncludeSubgraphs: true}))
		assert.NoError(err)
		return len(got)
	}
	assert.Equal(2, list(), "the root and its child")

	assert.NoError(e.Delete(ctx, fk))
	_, err = e.Summary(ctx, childKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "a child of a stamped tree")
	assert.Zero(list(), "neither the root nor its child is listed")

	_, _, rootID, _, _ := keys.ParseFlowKey(fk)
	e.reapBatchRows = 1
	e.reapSlice, e.reapBudget = 0, 0
	e.reapDueFlows(ctx)
	flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Must(assert.True(flows >= 1 && flows+steps < 4, "part of the tree is gone: %d flows, %d steps", flows, steps))
	_, err = e.Step(ctx, rootStep)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "a read of a tree in deletion is not a partial result")
	_, err = e.History(ctx, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	out, err := e.Snapshot(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, out.Status, "the root's outcome is served until the root goes")
	_, _, err = e.Fingerprint(ctx, fk)
	assert.NoError(err, "Fingerprint serves what Snapshot does")
	assert.NoError(e.Delete(ctx, fk), "a repeated Delete of a tree in deletion is success")
	_, err = e.Fork(ctx, rootStep, nil)
	assert.Equal(http.StatusConflict, errors.StatusCode(err), "a tree marked for deletion is not forked")

	e.reapSlice, e.reapBudget = time.Hour, time.Hour
	e.reapDueFlows(ctx)
	flows, steps = treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Zero(flows + steps)
	_, err = e.Snapshot(ctx, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}

// TestChunkedDeletion_StepServesThroughTheGrace pins that a DeleteOnCompletion tree is read whole for as long as its
// stamp is not due: Step serves its steps through the grace, while History, as ever, does not.
func TestChunkedDeletion_StepServesThroughTheGrace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	e.deletionGrace = time.Hour
	fk, err := e.Create(ctx, "arch/solo", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.Must(assert.NoError(err))
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	_, hour, id, _, _ := keys.ParseFlowKey(fk)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	var stepID int
	var token string
	assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT step_id, step_token FROM dwarf_steps WHERE flow_id=?", id).Scan(&stepID, &token)))
	step, err := e.Step(ctx, keys.NewStepKey(1, hour, id, stepID, token))
	assert.NoError(err)
	if assert.NotNil(step) {
		assert.Equal(workflow.StatusCompleted, step.Status)
	}
	_, err = e.History(ctx, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}
