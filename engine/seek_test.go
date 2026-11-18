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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestSeek_AWriteReachesOnlyItsOwnFlowsRows pins that the engine's writes by a list of ids, or by a column other than
// the primary key, reach only the rows they name: with another flow's row and steps held by an open transaction, a
// flow's cohort bump, Terminate's pass over its steps and a lock of its row by a list each go through. SQLite has one
// writer.
func TestSeek_AWriteReachesOnlyItsOwnFlowsRows(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Seek")
	g.SetURL("A", "seek/a")
	g.SetURL("B1", "seek/b1")
	g.SetURL("B2", "seek/b2")
	g.SetURL("J", "seek/j")
	g.AddTransitionFanOut("A", "B1", "B2")
	g.AddTransition("B1", "J")
	g.AddTransition("B2", "J")
	g.SetFanIn("J")
	g.AddTransition("J", workflow.End)
	mux.HandleGraph("seek/g", g)
	release := make(chan struct{})
	var started atomic.Int32
	branch := func(ctx context.Context, f *workflow.Flow) error { started.Add(1); <-release; return nil }
	mux.HandleTask("seek/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("seek/b1", branch)
	mux.HandleTask("seek/b2", branch)
	mux.HandleTask("seek/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	releaseBranches := sync.OnceFunc(func() { close(release) })
	defer releaseBranches()
	e.SetHost(mux)
	if !assert.NoError(e.Startup(t.Context())) {
		return
	}

	// The held flow is created first, so its rows sort before the other's: a scan of either table passes them.
	held, err := e.Create(ctx, "seek/g", nil, nil)
	assert.NoError(err)
	other, err := e.Create(ctx, "seek/g", nil, nil)
	assert.NoError(err)
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return started.Load() == 4 }), "branches never started")
	shard, _, heldFlow, _, err := keys.ParseFlowKey(held)
	assert.NoError(err)
	_, _, otherFlow, _, err := keys.ParseFlowKey(other)
	assert.NoError(err)
	sh, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()
	if db.DriverName() == "sqlite" {
		t.Skip("SQLite has one writer")
	}
	var spawnID, b1ID, b1Seq int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", otherFlow).Scan(&spawnID))
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id, lease_seq FROM dwarf_steps WHERE flow_id=? AND task_name='B1'", otherFlow).Scan(&b1ID, &b1Seq))
	// Flag B1 as a completion write would, so the cohort bump's fence matches it.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET advancing=1 WHERE step_id=?", b1ID)
	assert.NoError(err)

	errRollback := errors.New("roll back")
	locked := make(chan error, 1)
	letGo := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- db.Transact(ctx, func(tx *sequel.Tx) error {
			if _, err := lockFlowRows(ctx, tx, "flow_id=?", heldFlow); err != nil {
				locked <- err
				return err
			}
			_, err := tx.ExecContext(ctx, "UPDATE dwarf_steps SET attempt=attempt+1 WHERE flow_id=?", heldFlow)
			locked <- err
			if err != nil {
				return err
			}
			<-letGo
			return errRollback
		})
	}()
	assert.NoError(<-locked)

	attempt := func(name string, fn func(ctx context.Context, tx *sequel.Tx) error) {
		bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err := db.Transact(bounded, func(tx *sequel.Tx) error {
			if err := fn(bounded, tx); err != nil {
				return err
			}
			return errRollback
		})
		assert.True(errors.Is(err, errRollback), "%s waited on the other flow's rows: %v", name, err)
	}
	attempt("the cohort bump", func(ctx context.Context, tx *sequel.Tx) error {
		_, _, _, _, _, err := bumpCohortArrivals(ctx, tx, spawnID, 1, b1ID, b1Seq)
		return err
	})
	attempt("Terminate's step pass over a list of flows", func(ctx context.Context, tx *sequel.Tx) error {
		return updateFlowSteps(ctx, tx, "status=?, parked=?, updated_at=NOW_UTC()", []any{workflow.StatusTerminated, parkedNone},
			[]any{otherFlow}, "status IN ('"+workflow.StatusPending+"', '"+workflow.StatusRunning+"')")
	})
	attempt("the flow-row lock by a list", func(ctx context.Context, tx *sequel.Tx) error {
		ph, ids := database.IDList([]int{otherFlow})
		n, err := lockFlowRows(ctx, tx, "flow_id IN ("+ph+") AND status NOT IN ("+database.TerminalStatusesSQL+")", ids...)
		if err == nil && n != 1 {
			return errors.New("the flow row was not locked")
		}
		return err
	})
	close(letGo)
	assert.True(errors.Is(<-holderDone, errRollback))

	// Every transaction rolled back. B1 is given back the flag it had, and both flows run to the end.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET advancing=0 WHERE step_id=?", b1ID)
	assert.NoError(err)
	releaseBranches()
	for _, fk := range []string{held, other} {
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
		out, err := e.Await(awaitCtx, fk)
		cancel()
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, out.Status)
		}
	}
	enginetest.AssertInvariants(t, e)
}
