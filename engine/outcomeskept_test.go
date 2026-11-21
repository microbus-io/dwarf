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
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestOutcomesKept_AStampedTreeLosesStepsAChunkAtATime pins that AssertOutcomesKept lets a tree stamped for deletion
// lose recorded steps as chunked deletion takes them - a child's steps, then its flow row, then the root's steps - and
// returns the child as removed. Without the stamp's exemption the root's missing step fails as gone.
func TestOutcomesKept_AStampedTreeLosesStepsAChunkAtATime(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	root := workflow.NewGraph("StampedRoot")
	root.SetURL("Call", "stamped/call")
	root.SetURL("After", "stamped/after")
	root.AddTransitionChain("Call", "After", workflow.End)
	mux.HandleGraph("stamped/root", root)
	child := workflow.NewGraph("StampedChild")
	child.SetURL("Work", "stamped/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("stamped/child", child)
	mux.HandleTask("stamped/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("stamped/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("stamped/after", func(ctx context.Context, f *workflow.Flow) error { return f.Set("after", true) })
	mux.HandleTask("stamped/work", func(ctx context.Context, f *workflow.Flow) error { return f.Set("work", true) })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.Must(assert.NoError(e.SetHost(mux)))
	assert.Must(assert.NoError(e.Startup(ctx)))
	rootKey, out, err := e.Run(ctx, "stamped/root", nil, nil)
	assert.Must(assert.NoError(err))
	assert.Must(assert.Equal(workflow.StatusCompleted, out.Status))
	before := enginetest.CaptureOutcomes(t, e)

	// Delete stamps the tree; test mode's janitor is manual, so the rows stay until the test takes them as a first
	// chunk would.
	assert.Must(assert.NoError(e.Delete(ctx, rootKey)))
	shard, _, rootID, _, err := keys.ParseFlowKey(rootKey)
	assert.Must(assert.NoError(err))
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var childID, childHour int
	var childToken string
	assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id, created_hour, flow_token FROM dwarf_flows"+
		" WHERE root_flow_id=? AND flow_id<>?", rootID, rootID).Scan(&childID, &childHour, &childToken)))
	stepsOf := func(flowID int) (ids []int) {
		rows, err := db.QueryContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? ORDER BY step_id", flowID)
		assert.Must(assert.NoError(err))
		defer rows.Close()
		for rows.Next() {
			var id int
			assert.Must(assert.NoError(rows.Scan(&id)))
			ids = append(ids, id)
		}
		assert.Must(assert.NoError(rows.Err()))
		return ids
	}
	rootSteps := stepsOf(rootID)
	assert.Must(assert.Len(rootSteps, 2))
	deleteRow := func(query string, id int) {
		_, err := db.ExecContext(ctx, query, id)
		assert.Must(assert.NoError(err))
	}
	for _, id := range stepsOf(childID) {
		deleteRow("DELETE FROM dwarf_steps WHERE step_id=?", id)
	}
	deleteRow("DELETE FROM dwarf_flows WHERE flow_id=?", childID)
	deleteRow("DELETE FROM dwarf_steps WHERE step_id=?", rootSteps[0])

	removed := enginetest.AssertOutcomesKept(t, e, before)
	assert.Equal([]string{keys.NewFlowKey(shard, childHour, childID, childToken)}, removed)
}
