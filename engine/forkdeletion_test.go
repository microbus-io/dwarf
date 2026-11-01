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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestFork_DeletedBeforeItsLock pins that Fork reads its origin under the lock that opens its transaction. A Delete
// that lands after Fork was called and before the lock makes the Fork a 409; and an origin the reaper removed in that
// window is the 404 of an unknown key, not a missing row surfacing from deep in the clone. A fork at a step inside a
// subgraph sees the Delete too: only the root carries the mark, so the lock is the root's.
func TestFork_DeletedBeforeItsLock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("ForkDeletion")
	g.SetURL("A", "forkdel/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("forkdel/g", g)
	mux.HandleTask("forkdel/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	parent := workflow.NewGraph("ForkDeletionParent")
	parent.SetURL("Call", "forkdel/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("forkdel/parent", parent)
	mux.HandleTask("forkdel/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("forkdel/g", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, time.Hour) // due at once, but only the test's own reap fires
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name     string
		subgraph bool
		reaped   bool
		status   int
	}{
		{"scheduled_for_deletion", false, false, http.StatusConflict},
		{"scheduled_for_deletion_at_a_subgraph_step", true, false, http.StatusConflict},
		{"reaped", false, true, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)
			workflowURL := "forkdel/g"
			if tc.subgraph {
				workflowURL = "forkdel/parent"
			}
			fk, outcome, err := e.Run(ctx, workflowURL, nil, nil)
			if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
				return
			}
			steps, err := e.History(ctx, fk)
			if !assert.NoError(err) || !assert.Len(steps, 1) {
				return
			}
			if tc.subgraph {
				steps = steps[0].SubHistory
			}

			e.seams.Break(engineseams.CheckpointForkBeforeClone)
			forked := make(chan error, 1)
			go func() {
				_, err := e.Fork(ctx, steps[0].StepKey, nil)
				forked <- err
			}()
			assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointForkBeforeClone, 10*time.Second*enginetest.TimeoutScale()),
				"Fork never reached its clone")
			assert.NoError(e.Delete(ctx, fk))
			if tc.reaped {
				time.Sleep(5 * time.Millisecond) // let the 1ms delete_after_ms elapse on the database clock
				e.reapDueFlows(ctx)
			}
			e.seams.Resume(engineseams.CheckpointForkBeforeClone)
			err = <-forked
			assert.Error(err)
			assert.Equal(tc.status, errors.StatusCode(err))
		})
	}
}
