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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestLeaseRecovery_EndToEnd is the end-to-end proof of lease-based crash recovery:
// a worker "crashes" mid-task (its first execution never returns), its lease expires, pollPendingSteps
// resets the step, and a free worker re-executes it to completion. It asserts the whole path heals - the
// flow completes on the second execution - and pins two properties: the dwarf_steps_recovered metric fires,
// and lease recovery is NOT a retry (the step's attempt counter stays 0, unlike flow.Retry which bumps it).
func TestLeaseRecovery_EndToEnd(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	var aCalls atomic.Int64
	aStarted := make(chan struct{}, 1)
	aRelease := make(chan struct{})

	proxy := NewTestProxy()
	g := workflow.NewGraph("LR")
	g.SetEndpoint("A", "lr/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("lr/g", g)
	proxy.HandleTask("lr/a", func(ctx context.Context, f *workflow.Flow) error {
		if aCalls.Add(1) == 1 {
			aStarted <- struct{}{}
			<-aRelease // the crashed worker: its first execution is leaked, released only at teardown
		}
		f.SetString("ran", "yes")
		return nil
	})

	eng := NewEngine()
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(proxy)
	eng.SetMeterProvider(mp)
	eng.RunInTest(t)
	// Release the leaked "crashed" execution at teardown, BEFORE RunInTest's Shutdown drains workers (LIFO
	// cleanup: this runs first), so the blocked worker returns and Shutdown does not wait out its lease.
	t.Cleanup(func() { close(aRelease) })

	flowKey, outcome := zombieDispatch(t, eng, "lr/g", "A", &aCalls, aStarted, aRelease)
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("yes", outcome.State["ran"]) // the recovery re-execution wrote the state

	// The recovery metric fired: pollPendingSteps reset the lost-lease step to pending for re-execution.
	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	recovered, ok := sumCounter(rm, "dwarf_steps_recovered", "", "")
	assert.True(ok, "dwarf_steps_recovered should be present")
	assert.True(recovered >= 1, "at least one step recovered by lease expiry, got %d", recovered)

	// Lease recovery is NOT a retry: the re-executed step's attempt stays 0 (flow.Retry would have bumped it).
	_, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	var attempt int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT attempt FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND status='"+workflow.StatusCompleted+"'",
		flowID).Scan(&attempt))
	assert.Equal(0, attempt)

	// The task ran exactly twice: the crashed (leaked) attempt plus the recovery re-execution - never a third.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(int64(2), aCalls.Load())
}
