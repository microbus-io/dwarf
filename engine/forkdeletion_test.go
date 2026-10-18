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

// TestFork_DeletionAfterThePreCheck pins that Fork re-checks the origin under its lock. A Delete that lands after
// Fork's pre-transaction read and before its clone must still make the Fork a 409, as it would have been had it
// landed first; and an origin the reaper removed in that window is the 404 of an unknown key, not a missing row
// surfacing from deep in the clone.
func TestFork_DeletionAfterThePreCheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("ForkDeletion")
	g.SetURL("A", "forkdel/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("forkdel/g", g)
	mux.HandleTask("forkdel/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, time.Hour) // due at once, but only the test's own reap fires
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		reaped bool
		status int
	}{
		{"scheduled_for_deletion", false, http.StatusConflict},
		{"reaped", true, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)
			fk, outcome, err := e.Run(ctx, "forkdel/g", nil, nil)
			if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
				return
			}
			steps, err := e.History(ctx, fk)
			if !assert.NoError(err) || !assert.Len(steps, 1) {
				return
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
