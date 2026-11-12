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

package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestReviveVsTerminate_Deterministic pins completeSurgraphFlow's revive guard: when a Terminate terminalizes
// the parked caller step in the window between the child's completion and the parent's revive, the revive's
// running+parkedSubgraph guard must match zero rows so it does NOT resurrect the just-terminated caller to
// pending. The checkpoint freezes the worker at the revive (holding no lock - completeFlow's own transaction
// already committed the child's completion), making the Terminate-wins ordering deterministic.
func TestReviveVsTerminate_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var callRuns int
	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "rvc/call")
	parent.AddTransition("Call", workflow.End)
	assert.NoError(parent.Validate())
	mux.HandleGraph("rvc/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("X", "rvc/x")
	child.AddTransition("X", workflow.End)
	assert.NoError(child.Validate())
	mux.HandleGraph("rvc/child", child)
	mux.HandleTask("rvc/call", func(ctx context.Context, f *workflow.Flow) error {
		callRuns++
		yield, err := f.Subgraph("rvc/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("rvc/x", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// Freeze the worker at the caller revive, after the child completed (its completion transaction committed).
	accessors.Seams(e).Break(diagnostics.CheckpointBeforeReviveWrite)
	fk, err := e.Create(ctx, "rvc/parent", nil, nil)
	assert.NoError(err)
	assert.True(accessors.Seams(e).WaitTimeout(ctx, diagnostics.CheckpointBeforeReviveWrite, 10*time.Second), "engine never reached checkpoint diagnostics.CheckpointBeforeReviveWrite")

	// Terminate wins: the parked caller step is flipped terminated under the terminate transaction.
	assert.NoError(e.Terminate(ctx, fk, "test"))

	// Release the revive: its running+parkedSubgraph guard matches zero rows, so the terminated caller is not
	// resurrected to pending and not re-dispatched.
	accessors.Seams(e).Resume(diagnostics.CheckpointBeforeReviveWrite)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusTerminated, 10*time.Second)

	shardNum, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	sh, err := accessors.DB(e).Shard(shardNum)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var callStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='Call'", flowID).Scan(&callStatus))
	assert.Equal(workflow.StatusTerminated, callStatus) // not revived to pending

	// callRuns == 1: the caller ran once (its park), the revive was fenced, so no re-dispatch.
	//
	// A negative assertion needs the window it denies to be CLOSED, and two pushing cycles close it: the only
	// way the caller could run again is as a pending candidate, and a cycle is the dispatcher looking for
	// exactly those. A sleep denies the same thing over an interval nobody sized against the dispatcher's
	// own cadence, and gets weaker the busier the machine - here it gets STRONGER, since a slow piston makes
	// the two cycles later rather than fewer.
	enginetest.AwaitShardCycles(t, e, 2)
	assert.Equal(1, callRuns)
	enginetest.AssertInvariants(t, e)
}
