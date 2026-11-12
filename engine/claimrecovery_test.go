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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// A read error after a successful claim must retain that claim's generation: recovery can re-offer its own
// step, but cannot reset a replacement owner that took the step while the original worker was paused.
func TestMySQLClaim_ReadErrorRecovery(t *testing.T) {
	t.Parallel()
	for _, replaced := range []bool{false, true} {
		name := "current_claim"
		if replaced {
			name = "replaced_claim"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var runs atomic.Int32
			mux := hosts.NewMux()
			g := workflow.NewGraph("ClaimReadError")
			g.SetURL("A", "a")
			g.AddTransition("A", workflow.End)
			mux.HandleGraph("g", g)
			mux.HandleTask("a", func(context.Context, *workflow.Flow) error {
				runs.Add(1)
				return nil
			})
			e := NewEngineUnderTest(t.Name())
			defer e.Shutdown(ctx)
			if err := e.SetWorkers(0); err != nil {
				t.Fatal(err)
			}
			e.SetHost(mux)
			if err := e.Startup(t.Context()); err != nil {
				t.Fatal(err)
			}
			sh, err := e.db.Shard(1)
			if err != nil {
				t.Fatal(err)
			}
			db := sh.DB()
			if driver := db.DriverName(); driver != "mysql" && driver != "mariadb" {
				t.Skip("the two-statement claim is MySQL and MariaDB only")
			}
			awaitStartupRecoverySweep(t, e)
			flowKey, err := e.Create(ctx, "g", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
			if err != nil {
				t.Fatal(err)
			}
			var stepID, seq int
			if err := db.QueryRowContext(ctx, "SELECT step_id, lease_seq FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&stepID, &seq); err != nil {
				t.Fatal(err)
			}
			held := engineseams.Join(engineseams.CheckpointClaimBeforeRead, strconv.Itoa(shard), strconv.Itoa(stepID), strconv.Itoa(seq+1))
			e.seams.Break(held)
			defer e.seams.Resume(held)
			reached := e.seams.Waiter(held)
			done := make(chan error, 1)
			go func() { done <- e.processStep(ctx, shard, stepID, func() {}) }()
			select {
			case <-reached:
			case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
				t.Fatal("the first worker never reached its claim's read")
			}

			wantStatus, wantSeq := workflow.StatusPending, seq+1
			if replaced {
				// Model lease recovery, then take a real second claim and leave its fresh lease running.
				if _, err := db.ExecContext(ctx, "UPDATE dwarf_steps SET status='"+workflow.StatusPending+"', not_before=NOW_UTC(), lease_expires=NOW_UTC() WHERE step_id=?", stepID); err != nil {
					t.Fatal(err)
				}
				replacement := &dispatch{shard: shard, stepID: stepID, returnGateTurn: func() {}}
				if stopped, err := e.claim(ctx, replacement); err != nil || stopped {
					t.Fatalf("replacement claim: stopped=%v, err=%v", stopped, err)
				}
				wantStatus, wantSeq = workflow.StatusRunning, seq+2
			}
			// Fail before Scan can populate any field, including leaseSeq. Arm after the replacement read.
			e.seams.InjectN(engineseams.FaultClaimReadContention, 1)
			e.seams.Resume(held)
			select {
			case err := <-done:
				if !sequel.IsLockContentionError(err) {
					t.Fatalf("expected claim-read contention, got %v", err)
				}
			case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
				t.Fatal("the first worker never finished")
			}
			var status string
			var gotSeq int
			if err := db.QueryRowContext(ctx, "SELECT status, lease_seq FROM dwarf_steps WHERE step_id=?", stepID).Scan(&status, &gotSeq); err != nil {
				t.Fatal(err)
			}
			if status != wantStatus || gotSeq != wantSeq {
				t.Errorf("after read-error recovery: %s generation %d, want %s generation %d", status, gotSeq, wantStatus, wantSeq)
			}
			if runs.Load() != 0 {
				t.Errorf("task ran %d times despite the failed claim read", runs.Load())
			}
		})
	}
}

// An error whose claim response never supplied a generation has no authority to reset the row. Exercise
// both recovery arms and retain a positive control: the current owner still gets immediate recovery.
func TestLeaseFence_RecoveryRequiresKnownGeneration(t *testing.T) {
	t.Parallel()
	for _, status := range []string{workflow.StatusRunning, workflow.StatusCompleted} {
		for _, owner := range []struct {
			name string
			seq  int
		}{{"unknown", 0}, {"stale", 1}, {"current", 2}} {
			t.Run(status+"/"+owner.name, func(t *testing.T) {
				ctx := context.Background()
				mux := hosts.NewMux()
				g := workflow.NewGraph("RecoveryOwnership")
				g.SetURL("A", "a")
				g.AddTransition("A", workflow.End)
				mux.HandleGraph("g", g)
				e := NewEngineUnderTest(t.Name())
				defer e.Shutdown(ctx)
				if err := e.SetWorkers(0); err != nil {
					t.Fatal(err)
				}
				e.SetHost(mux)
				if err := e.Startup(t.Context()); err != nil {
					t.Fatal(err)
				}
				awaitStartupRecoverySweep(t, e)
				flowKey, err := e.Create(ctx, "g", nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
				if err != nil {
					t.Fatal(err)
				}
				sh, err := e.db.Shard(shard)
				if err != nil {
					t.Fatal(err)
				}
				db := sh.DB()
				var stepID int
				if err := db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&stepID); err != nil {
					t.Fatal(err)
				}
				completed := status == workflow.StatusCompleted
				advancing := 0
				if completed {
					advancing = 1
				}
				if _, err := db.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, lease_seq=2, advancing=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), 600000) WHERE step_id=?", status, advancing, stepID); err != nil {
					t.Fatal(err)
				}
				d := &dispatch{shard: shard, stepID: stepID, markedComplete: completed}
				d.row.leaseSeq = owner.seq
				e.recoverStep(ctx, d, errors.New("database is locked"))
				wantStatus := status
				if owner.seq == 2 {
					wantStatus = workflow.StatusPending
				}
				var gotStatus string
				var gotSeq int
				if err := db.QueryRowContext(ctx, "SELECT status, lease_seq FROM dwarf_steps WHERE step_id=?", stepID).Scan(&gotStatus, &gotSeq); err != nil {
					t.Fatal(err)
				}
				if gotStatus != wantStatus || gotSeq != 2 {
					t.Errorf("recovery produced %s generation %d, want %s generation 2", gotStatus, gotSeq, wantStatus)
				}
			})
		}
	}
}
