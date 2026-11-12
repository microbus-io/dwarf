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
)

// TestMySQLClaim_ReadKeepsItsOwnGeneration pins that the MySQL claim's read-back is fenced on the generation its
// own CAS took. The claim and the read are two statements there: a worker held between them while its lease
// expires, recovery returns the step to pending and a second claim runs the task must find no row at its
// generation and abandon - not read the second claim's generation and row, run the task again and insert a
// second successor. Both a second worker of the same engine and one of a peer replica take the step.
func TestMySQLClaim_ReadKeepsItsOwnGeneration(t *testing.T) {
	for _, peer := range []bool{false, true} {
		name := "same_engine"
		if peer {
			name = "peer_replica"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var runs atomic.Int32
			mux := hosts.NewMux()
			g := workflow.NewGraph("ClaimRace")
			g.SetURL("A", "a")
			g.SetURL("B", "b")
			g.AddTransition("A", "B")
			g.AddTransition("B", workflow.End)
			mux.HandleGraph("g", g)
			mux.HandleTask("a", func(_ context.Context, f *workflow.Flow) error {
				runs.Add(1)
				return nil
			})
			mux.HandleTask("b", func(context.Context, *workflow.Flow) error { return nil })

			e := NewEngineUnderTest(t.Name())
			e.SetWorkers(0)
			e.SetHost(mux)
			if err := e.Startup(t.Context()); err != nil {
				t.Fatal(err)
			}
			defer e.Shutdown(ctx)
			sh, err := e.db.Shard(1)
			if err != nil {
				t.Fatal(err)
			}
			db := sh.DB()
			if d := db.DriverName(); d != "mysql" && d != "mariadb" {
				t.Skip("the two-statement claim is MySQL and MariaDB only")
			}
			awaitStartupRecoverySweep(t, e)
			second := e
			if peer {
				second = NewEngineUnderTest(t.Name())
				second.SetWorkers(0)
				second.SetHost(mux)
				if err := second.Startup(t.Context()); err != nil {
					t.Fatal(err)
				}
				defer second.Shutdown(ctx)
				awaitStartupRecoverySweep(t, second)
			}

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

			// Hold the first worker between its CAS and its read, at the generation it is about to take.
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

			// Its lease runs out and recovery returns the step to pending, as recoverExpiredLeases does (no bump).
			if _, err := db.ExecContext(ctx, "UPDATE dwarf_steps SET status='"+workflow.StatusPending+"', not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"'", stepID); err != nil {
				t.Fatal(err)
			}
			// The second claim takes the step at the next generation, runs it and transitions it.
			if err := second.processStep(ctx, shard, stepID, func() {}); err != nil {
				t.Fatal(err)
			}
			if runs.Load() != 1 {
				t.Fatalf("the second claim ran the task %d times, want 1", runs.Load())
			}

			e.seams.Resume(held)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
				t.Fatal("the first worker never finished")
			}
			var successors int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE predecessor_id=?", stepID).Scan(&successors); err != nil {
				t.Fatal(err)
			}
			if runs.Load() != 1 || successors != 1 {
				t.Errorf("the held worker adopted the later claim: task ran %d times, %d successors; want 1 and 1", runs.Load(), successors)
			}
		})
	}
}
