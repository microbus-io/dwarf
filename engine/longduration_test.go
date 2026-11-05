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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// month is past 2^31 milliseconds (~24.8 days), the most SQL Server's DATEADD(MILLISECOND, ...) takes as an int.
// Every DATE_ADD_MILLIS the engine writes must survive it: run these against SQL Server.
const month = 30 * 24 * time.Hour

// TestLongDuration_SleepAndRetryBackoff pins a step whose wake time is past 2^31 ms away: a flow.Sleep, which
// sets its successor's not_before, and a flow.Retry backoff, which sets its own. Each write must land, leaving the
// step pending for a month rather than failing the flow.
func TestLongDuration_SleepAndRetryBackoff(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	sleep := workflow.NewGraph("LongSleep")
	sleep.SetURL("A", "longsleep/a")
	sleep.SetURL("B", "longsleep/b")
	sleep.AddTransition("A", "B")
	sleep.AddTransition("B", workflow.End)
	mux.HandleGraph("longsleep/g", sleep)
	mux.HandleTask("longsleep/a", func(ctx context.Context, f *workflow.Flow) error {
		f.Sleep(month)
		return nil
	})
	mux.HandleTask("longsleep/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	retry := workflow.NewGraph("LongRetry")
	retry.SetURL("A", "longretry/a")
	retry.AddTransition("A", workflow.End)
	mux.HandleGraph("longretry/g", retry)
	mux.HandleTask("longretry/a", func(ctx context.Context, f *workflow.Flow) error {
		f.Retry(month, 1, 0, 0) // no giveUpAfter, so it always arms
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	for _, tc := range []struct {
		name, workflowURL, task, status string
	}{
		{"sleep", "longsleep/g", "B", workflow.StatusPending},
		{"retry", "longretry/g", "A", workflow.StatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)
			flowKey, err := e.Create(ctx, tc.workflowURL, nil, nil)
			if !assert.NoError(err) {
				return
			}
			shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
			if !assert.NoError(err) {
				return
			}
			db, err := e.db.Shard(shard)
			if !assert.NoError(err) {
				return
			}
			var entryStepID int
			assert.NoError(db.QueryRowContext(ctx,
				"SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&entryStepID))
			enginetest.AwaitVisitCount(t, e.seams, 1, 30*time.Second,
				engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(entryStepID)))

			var flowStatus string
			assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&flowStatus))
			assert.Equal(workflow.StatusRunning, flowStatus)
			// The wake time is at least 29 days out, compared on the database clock.
			var waiting int
			assert.NoError(db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name=? AND status='"+tc.status+"'"+
					" AND not_before>DATE_ADD_MILLIS(NOW_UTC(), ?)",
				flowID, tc.task, (29 * 24 * time.Hour).Milliseconds()).Scan(&waiting))
			assert.Equal(1, waiting)
		})
	}
}
