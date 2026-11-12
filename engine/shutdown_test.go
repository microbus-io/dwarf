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
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// sweepErrorLog records the Error-level lines a background sweep logs, and passes every Error-level line on to
// stderr as the test-mode default logger would.
type sweepErrorLog struct {
	next  slog.Handler
	mu    sync.Mutex
	lines []string
}

func (l *sweepErrorLog) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError
}
func (l *sweepErrorLog) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *sweepErrorLog) WithGroup(string) slog.Handler      { return l }
func (l *sweepErrorLog) Handle(ctx context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, "Lease recovery sweep") {
		l.mu.Lock()
		l.lines = append(l.lines, r.Message)
		l.mu.Unlock()
	}
	return l.next.Handle(ctx, r)
}

func (l *sweepErrorLog) logged() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// TestShutdown_AbandonsASweepInFlight pins that Shutdown does not wait out a background sweep's statement. The
// background lease-recovery pass is held mid-pass, and the drain must get past it while the hold is still in place:
// a sweep left on the lifetime ctx keeps the drain waiting for as long as the hold lasts. The abandoned pass must not
// log its cancellation as a sweep failure either.
func TestShutdown_AbandonsASweepInFlight(t *testing.T) {
	t.Parallel()

	// The hold is released only once the recovery loop has returned or this ceiling has passed, so a drain that
	// waits for the sweep fails here instead of hanging.
	ceiling := 15 * time.Second * enginetest.TimeoutScale()

	// run holds the background sweep with hold once its SELECT has found the forged step, calls Shutdown, and
	// asserts the recovery loop returned before the hold was released. hold returns what ends the hold.
	run := func(t *testing.T, rowLocks bool, hold func(t *testing.T, e *Engine, db *sequel.DB, stepID int) (release func())) {
		assert := testarossa.For(t)
		ctx := context.Background()

		mux := hosts.NewMux()
		g := workflow.NewGraph("SweepStop")
		g.SetURL("A", "sweepstop/a")
		g.AddTransition("A", workflow.End)
		mux.HandleGraph("sweepstop/g", g)
		mux.HandleTask("sweepstop/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
		log := &sweepErrorLog{next: slog.NewTextHandler(os.Stderr, nil)}

		e := NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		e.SetHost(mux)
		assert.NoError(e.SetLogger(slog.New(log)))
		e.recoveryInterval = 100 * time.Millisecond
		assert.Must(assert.NoError(e.Startup(t.Context())))

		fk, out, err := e.Run(ctx, "sweepstop/g", nil, nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
			return
		}
		shard, _, flowID, _, err := keys.ParseFlowKey(fk)
		assert.NoError(err)
		sh, err := e.db.Shard(shard)
		if !assert.NoError(err) {
			return
		}
		db := sh.DB()
		if rowLocks && db.DriverName() == "sqlite" {
			// A held write lock fails the reset at the busy timeout rather than holding it.
			t.Skip("SQLite has no row lock to hold the reset on")
		}
		var stepID int
		assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&stepID))

		// Forge the shape lease recovery resets - a running step whose lease has lapsed - under a breakpoint, so
		// the next background pass stops between its SELECT and its reset.
		e.seams.Break(engineseams.CheckpointLeaseRecoverySelected)
		_, err = db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=0, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE step_id=?",
			workflow.StatusRunning, stepID)
		assert.NoError(err)
		if !assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointLeaseRecoverySelected, ceiling), "the sweep never selected the forged step") {
			e.seams.Resume(engineseams.CheckpointLeaseRecoverySelected)
			return
		}
		release := hold(t, e, db, stepID)

		// The hold ends once the recovery loop has returned, or at the ceiling. Waiting for Shutdown itself instead
		// would also wait for its teardown of the shard's test database, which a session the hold keeps open there
		// delays.
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			e.Shutdown(ctx)
		}()
		abandoned := make(chan struct{})
		go func() {
			e.recoveryWorker.Wait()
			close(abandoned)
		}()
		select {
		case <-abandoned:
		case <-time.After(ceiling):
			t.Error("Shutdown waited for the sweep's statement")
		}
		release()
		select {
		case <-stopped:
		case <-time.After(ceiling):
			t.Error("Shutdown did not return")
		}
		assert.Len(log.logged(), 0, "an abandoned pass logged its cancellation as a failure")
	}

	t.Run("held_between_statements", func(t *testing.T) {
		t.Parallel()
		// The breakpoint stays armed: the sweep is frozen at it for as long as the hold lasts, as it would be in a
		// slow statement, and returns only when its ctx is cancelled or the breakpoint is resumed.
		run(t, false, func(t *testing.T, e *Engine, _ *sequel.DB, _ int) func() {
			return func() { e.seams.Resume(engineseams.CheckpointLeaseRecoverySelected) }
		})
	})

	t.Run("blocked_on_a_row_lock", func(t *testing.T) {
		t.Parallel()
		// A real statement in flight: the sweep's reset waits on a row lock the test holds.
		run(t, true, func(t *testing.T, e *Engine, db *sequel.DB, stepID int) func() {
			assert := testarossa.For(t)
			tx, err := db.BeginTx(context.Background(), nil)
			if !assert.NoError(err) {
				e.seams.Resume(engineseams.CheckpointLeaseRecoverySelected)
				return func() {}
			}
			// The value moves, so every dialect counts the row as written and holds its lock.
			_, err = tx.ExecContext(context.Background(), "UPDATE dwarf_steps SET lease_seq=lease_seq+1 WHERE step_id=?", stepID)
			assert.NoError(err)
			// Let the sweep go on into its reset, which now waits on the lock.
			e.seams.Resume(engineseams.CheckpointLeaseRecoverySelected)
			return func() { tx.Rollback() }
		})
	})
}
