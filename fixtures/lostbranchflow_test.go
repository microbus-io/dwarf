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

/*
A fan-out that never converges - one branch failed, was cancelled, or the flow was terminated - closes with the
fan-out's state followed by every branch's stored changes, the lost branch's included, as a lost step on the trunk
keeps its own. A failing run stores none of its writes; what the lost branch contributes is what its runs left
before it was lost: an earlier attempt that retried, a run a Cancel covered, the run that interrupted.
*/
package fixtures

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestLostbranchflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	// Each workflow fans E out to L and R, which converge on J. R contributes and completes; L's handler is the
	// case's.
	inside := make(chan struct{}, 1) // L reached its hold
	release := make(chan struct{})   // lets L return
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) }) // below the Shutdown defer, so it unwinds first
	cases := map[string]func(ctx context.Context, f *workflow.Flow) error{
		// L contributes, retries, then fails: the failing run's write is dropped, the attempt before it stands.
		"retried": func(ctx context.Context, f *workflow.Flow) error {
			if f.Attempt() == 0 {
				f.Set("trail", []string{"L attempt 0"})
				f.Retry(0, 1, 0, 0)
				return nil
			}
			f.Set("trail", []string{"L attempt 1"})
			return errors.New("no luck")
		},
		// L contributes and holds; a Cancel lands while it runs, and its completion is covered.
		"covered": func(ctx context.Context, f *workflow.Flow) error {
			f.Set("trail", []string{"L ran"})
			inside <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
		// L contributes and interrupts; the flow is terminated while it waits.
		"interrupted": func(ctx context.Context, f *workflow.Flow) error {
			f.Set("trail", []string{"L asked"})
			yield, err := f.Interrupt(map[string]any{"question": "?"}, nil)
			if yield || err != nil {
				return err
			}
			return nil
		},
	}
	for name, l := range cases {
		g := workflow.NewGraph("LostBranch")
		g.SetURL("E", "lostbranchflow.verify:428/e")
		g.SetURL("L", "lostbranchflow.verify:428/"+name)
		g.SetURL("R", "lostbranchflow.verify:428/r")
		g.SetURL("J", "lostbranchflow.verify:428/j")
		g.SetReducer("trail", workflow.ReducerAppend)
		g.AddTransitionFanOut("E", "L", "R")
		g.AddTransition("L", "J")
		g.AddTransition("R", "J")
		g.SetFanIn("J")
		g.AddTransition("J", workflow.End)
		mux.HandleGraph("lostbranchflow.verify:428/"+name+"/flow", g)
		mux.HandleTask("lostbranchflow.verify:428/"+name, l)
	}
	noop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	mux.HandleTask("lostbranchflow.verify:428/e", noop)
	mux.HandleTask("lostbranchflow.verify:428/j", noop)
	rDone := make(chan struct{}, 3)
	mux.HandleTask("lostbranchflow.verify:428/r", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("trail", []string{"R ran"})
		rDone <- struct{}{}
		return nil
	})

	t.Run("retried_then_failed", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, err := eng.Create(ctx, "lostbranchflow.verify:428/retried/flow", nil, nil)
		assert.Must(assert.NoError(err))
		outcome, err := eng.Await(ctx, flowKey)
		assert.Must(assert.NoError(err))
		<-rDone
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Equal([]string{"L attempt 0", "R ran"}, outcome.State.GetStrings("trail"))
	})
	t.Run("covered_by_a_cancel", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, err := eng.Create(ctx, "lostbranchflow.verify:428/covered/flow", nil, nil)
		assert.Must(assert.NoError(err))
		<-inside
		<-rDone // R's run is in either way: completed, or covered by the Cancel too
		assert.Must(assert.NoError(eng.Cancel(ctx, flowKey, "stop")))
		releaseOnce.Do(func() { close(release) })
		outcome, err := eng.Await(ctx, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal([]string{"L ran", "R ran"}, outcome.State.GetStrings("trail"))
	})
	t.Run("interrupted_then_terminated", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, err := eng.Create(ctx, "lostbranchflow.verify:428/interrupted/flow", nil, nil)
		assert.Must(assert.NoError(err))
		<-rDone
		// R's run is stored before the Terminate lands, which would fence its completion write.
		shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
		assert.Must(assert.NoError(err))
		assert.Must(assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
			return enginetest.CountRows(t, eng, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='R' AND status='"+workflow.StatusCompleted+"'", flowID) == 1
		}), "R never completed"))
		outcome, err := eng.Await(ctx, flowKey)
		assert.Must(assert.NoError(err))
		assert.Must(assert.Equal(workflow.StatusInterrupted, outcome.Status))
		assert.Must(assert.NoError(eng.Terminate(ctx, flowKey, "stop")))
		outcome, err = eng.Snapshot(ctx, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusTerminated, outcome.Status)
		assert.Equal([]string{"L asked", "R ran"}, outcome.State.GetStrings("trail"))
	})
}
