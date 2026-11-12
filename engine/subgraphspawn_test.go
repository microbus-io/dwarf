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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// spawnFailEngine starts an engine whose graph "<prefix>/g" runs one task, Call, that subgraphs a child URL no
// graph is registered under, so every spawn fails with the host's 404. firstRuns counts the runs that yielded to
// spawn; the re-run records the code flow.Subgraph returned.
func spawnFailEngine(t *testing.T, prefix string, firstRuns *atomic.Int32) *Engine {
	t.Helper()
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	g := workflow.NewGraph("Spawn")
	g.SetURL("Call", prefix+"/call")
	g.AddTransition("Call", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph(prefix+"/g", g)
	mux.HandleTask(prefix+"/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph(prefix+"/absent", nil, nil)
		if yield {
			firstRuns.Add(1)
			return nil
		}
		f.SetInt("code", errors.StatusCode(err))
		return nil
	})
	e := NewEngineUnderTest(t.Name())
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	return e
}

// TestSpawnResolve_ContentionIsAbsorbedWithoutReExecution pins that the write delivering a failed spawn retries
// its own lock contention. It is not a transaction, so an escaping deadlock would reach processStep's recovery
// defer, which rewinds the running step and runs the task - and its LoadGraph - again.
func TestSpawnResolve_ContentionIsAbsorbedWithoutReExecution(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var firstRuns atomic.Int32
	e := spawnFailEngine(t, "srcont", &firstRuns)
	defer e.Shutdown(ctx)
	e.diags.Seams().InjectN(diagnostics.Join(diagnostics.FaultSpawnResolveContention, "Call"), 3)

	if out := enginetest.BoundedRun(t, e, "srcont/g"); assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(float64(http.StatusNotFound), stateVal(out.State, "code"))
	}
	assert.Equal(int32(1), firstRuns.Load(), "contention on the resolve write must retry the write, never the task")
}

// TestSpawnResolveVsTerminate_Deterministic pins the resolve write's status='running' guard: a Terminate that
// terminalizes the caller step first must leave it terminated, not re-pended for a re-run on a closed flow.
func TestSpawnResolveVsTerminate_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var firstRuns atomic.Int32
	e := spawnFailEngine(t, "srterm", &firstRuns)
	defer e.Shutdown(ctx)

	e.diags.Seams().Break(diagnostics.CheckpointBeforeSpawnResolve)
	fk, err := e.Create(ctx, "srterm/g", nil, nil)
	assert.Must(assert.NoError(err))
	assert.True(e.diags.Seams().WaitTimeout(ctx, diagnostics.CheckpointBeforeSpawnResolve, 10*time.Second), "engine never reached checkpoint diagnostics.CheckpointBeforeSpawnResolve")

	assert.NoError(e.Terminate(ctx, fk, "test"))
	e.diags.Seams().Resume(diagnostics.CheckpointBeforeSpawnResolve)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusTerminated, 10*time.Second)

	shardNum, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	sh, err := e.db.Shard(shardNum)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var stepStatus string
	var subgraphDone int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, subgraph_done FROM dwarf_steps WHERE flow_id=? AND task_name='Call'", flowID).Scan(&stepStatus, &subgraphDone))
	assert.Equal(workflow.StatusTerminated, stepStatus)
	assert.Equal(0, subgraphDone)
	assert.Equal(int32(1), firstRuns.Load())
	enginetest.AssertInvariants(t, e)
}
