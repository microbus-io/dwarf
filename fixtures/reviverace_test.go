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
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestReviveVsCancel_Deterministic pins completeSurgraphFlow's revive guard: when a Cancel terminalizes the
// parked caller step in the window between the child's completion and the parent's revive, the revive's
// running+parkedSubgraph guard must match zero rows so it does NOT resurrect the just-cancelled caller to
// pending. The checkpoint freezes the worker at the revive (holding no lock - completeFlow's own transaction
// already committed the child's completion), making the Cancel-wins ordering deterministic.
func TestReviveVsCancel_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var callRuns int
	proxy := engine.NewTestProxy()
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "rvc/call")
	parent.AddTransition("Call", workflow.END)
	assert.NoError(parent.Validate())
	proxy.HandleGraph("rvc/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "rvc/x")
	child.AddTransition("X", workflow.END)
	assert.NoError(child.Validate())
	proxy.HandleGraph("rvc/child", child)
	proxy.HandleTask("rvc/call", func(ctx context.Context, f *workflow.Flow) error {
		callRuns++
		yield, err := f.Subgraph("rvc/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("rvc/x", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := engine.NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	// Freeze the worker at the caller revive, after the child completed (its completion transaction committed).
	e.Seams().Break(engine.CheckpointBeforeReviveWrite)
	fk, err := e.Create(ctx, "rvc/parent", nil, nil)
	assert.NoError(err)
	assert.True(e.Seams().WaitTimeout(ctx, 10*time.Second, engine.CheckpointBeforeReviveWrite), "engine never reached checkpoint engine.CheckpointBeforeReviveWrite")

	// Cancel wins: the parked caller step is flipped cancelled under the cancel transaction.
	assert.NoError(e.Cancel(ctx, fk, "test"))

	// Release the revive: its running+parkedSubgraph guard matches zero rows, so the cancelled caller is not
	// resurrected to pending and not re-dispatched.
	e.Seams().Resume(engine.CheckpointBeforeReviveWrite)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCancelled, 10*time.Second)

	shardNum, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.DB().Shard(shardNum)
	assert.NoError(err)
	var callStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='Call'", flowID).Scan(&callStatus))
	assert.Equal(workflow.StatusCancelled, callStatus) // not revived to pending

	// callRuns == 1: the caller ran once (its park), the revive was fenced, so no re-dispatch.
	//
	// A negative assertion needs the window it denies to be CLOSED, and two pushing cycles close it: the only
	// way the caller could run again is as a pending candidate, and a cycle is the dispatcher looking for
	// exactly those. A sleep denies the same thing over an interval nobody sized against the dispatcher's
	// own cadence, and gets weaker the busier the machine - here it gets STRONGER, since a slow piston makes
	// the two cycles later rather than fewer.
	enginetest.AwaitShardCycles(t, e, 1, 2)
	assert.Equal(1, callRuns)
	enginetest.AssertInvariants(t, e)
}
