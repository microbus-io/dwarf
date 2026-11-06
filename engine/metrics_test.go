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
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// sumCounter returns the total of an int64 Sum metric's data points, optionally filtered to points
// carrying the given attribute key=value (empty key = no filter).
func sumCounter(rm metricdata.ResourceMetrics, name, attrKey, attrVal string) (int64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				return 0, false
			}
			var total int64
			matched := false
			for _, dp := range sum.DataPoints {
				if attrKey != "" {
					v, ok := dp.Attributes.Value(attribute.Key(attrKey))
					if !ok || v.AsString() != attrVal {
						continue
					}
				}
				total += dp.Value
				matched = true
			}
			return total, matched
		}
	}
	return 0, false
}

// histCount returns the total sample count of a float64 Histogram metric, optionally filtered to points
// carrying ALL of the given attributes (nil = no filter).
func histCount(rm metricdata.ResourceMetrics, name string, attrs map[string]string) (uint64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				return 0, false
			}
			var total uint64
			matched := false
		points:
			for _, dp := range hist.DataPoints {
				for k, want := range attrs {
					v, ok := dp.Attributes.Value(attribute.Key(k))
					if !ok || v.String() != want {
						continue points
					}
				}
				total += dp.Count
				matched = true
			}
			return total, matched
		}
	}
	return 0, false
}

// histSum returns the total of a float64 Histogram metric's sample sums, filtered to points carrying ALL of
// the given attributes (nil = no filter).
func histSum(rm metricdata.ResourceMetrics, name string, attrs map[string]string) (float64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				return 0, false
			}
			var total float64
			matched := false
		points:
			for _, dp := range hist.DataPoints {
				for k, want := range attrs {
					v, ok := dp.Attributes.Value(attribute.Key(k))
					if !ok || v.String() != want {
						continue points
					}
				}
				total += dp.Sum
				matched = true
			}
			return total, matched
		}
	}
	return 0, false
}

// histMax returns the largest single sample of a float64 Histogram metric, filtered to points carrying ALL
// of the given attributes (nil = no filter).
func histMax(rm metricdata.ResourceMetrics, name string, attrs map[string]string) (float64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				return 0, false
			}
			var largest float64
			matched := false
		points:
			for _, dp := range hist.DataPoints {
				for k, want := range attrs {
					v, ok := dp.Attributes.Value(attribute.Key(k))
					if !ok || v.String() != want {
						continue points
					}
				}
				if v, ok := dp.Max.Value(); ok {
					largest = max(largest, v)
					matched = true
				}
			}
			return largest, matched
		}
	}
	return 0, false
}

// gaugeValue returns the single data point of an int64 observable gauge. It takes the LAST point rather
// than summing, because an observable gauge reports a level, not an accumulation.
func gaugeValue(rm metricdata.ResourceMetrics, name string) (int64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok || len(g.DataPoints) == 0 {
				return 0, false
			}
			return g.DataPoints[len(g.DataPoints)-1].Value, true
		}
	}
	return 0, false
}

// gaugePresent reports whether an int64 observable gauge with the given name emitted any data point.
func gaugePresent(rm metricdata.ResourceMetrics, name string) bool {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			return ok && len(g.DataPoints) > 0
		}
	}
	return false
}

func TestMetrics_EmittedOnRun(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	mux := hosts.NewMux()
	g := workflow.NewGraph("G")
	g.SetURL("taskA", "metricsflow.verify:428/a")
	g.SetURL("taskB", "metricsflow.verify:428/b")
	g.AddTransition("taskA", "taskB")
	g.AddTransition("taskB", workflow.End)
	mux.HandleGraph("metricsflow.verify:428/g", g)
	mux.HandleTask("metricsflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("metricsflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	_, outcome, err := eng.Run(ctx, "metricsflow.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Run can return before dwarf_flows_closed is recorded: the completing worker counts it after its transaction
	// commits, and the await's database poll can see that commit first. Collect until the counter lands.
	var rm metricdata.ResourceMetrics
	deadline := time.Now().Add(3 * time.Second * enginetest.TimeoutScale())
	for {
		if !assert.NoError(reader.Collect(ctx, &rm)) {
			return
		}
		if _, ok := sumCounter(rm, "dwarf_flows_closed", "status", workflow.StatusCompleted); ok || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Counters. Instrument names carry no _total suffix (the Prometheus exporter appends it); the manual
	// reader here observes the raw instrument names.
	started, ok := sumCounter(rm, "dwarf_flows_started", "", "")
	assert.True(ok, "dwarf_flows_started should be present")
	assert.Equal(int64(1), started)

	ended, ok := sumCounter(rm, "dwarf_flows_closed", "status", workflow.StatusCompleted)
	assert.True(ok, "dwarf_flows_closed{status=completed} should be present")
	assert.Equal(int64(1), ended)

	// Both lifecycle counters carry the flow's priority, so started minus ended subtracts per band.
	startedAt, ok := sumCounter(rm, "dwarf_flows_started", "priority", "3")
	assert.True(ok, "dwarf_flows_started{priority} should be present")
	assert.Equal(int64(1), startedAt)
	endedAt, ok := sumCounter(rm, "dwarf_flows_closed", "priority", "3")
	assert.True(ok, "dwarf_flows_closed{priority} should be present")
	assert.Equal(int64(1), endedAt)

	// Two steps complete (taskA, taskB), each counted under status=completed.
	executed, ok := sumCounter(rm, "dwarf_steps_executed", "status", workflow.StatusCompleted)
	assert.True(ok, "dwarf_steps_executed{status=completed} should be present")
	assert.Equal(int64(2), executed)

	// State byte throughput: both counters fire on the execution path, labelled by workflow and by the
	// column the bytes moved through. Even no-op tasks carry at least the empty-object JSON ("{}"), so the
	// workflow-level sums are strictly positive; per-column, the entry snapshot ("state"), the successor's refs
	// ("state_refs") and the completion delta ("changes") must all appear for this two-task linear flow.
	writeBytes, ok := sumCounter(rm, "dwarf_state_write_bytes", "workflow", "metricsflow.verify:428/g")
	assert.True(ok, "dwarf_state_write_bytes{workflow} should be present")
	assert.True(writeBytes > 0, "state write bytes should be positive, got %d", writeBytes)
	stateW, ok := sumCounter(rm, "dwarf_state_write_bytes", "column", "state")
	assert.True(ok, "dwarf_state_write_bytes{column=state} should be present")
	assert.True(stateW > 0)
	changesW, ok := sumCounter(rm, "dwarf_state_write_bytes", "column", "changes")
	assert.True(ok, "dwarf_state_write_bytes{column=changes} should be present")
	assert.True(changesW > 0)
	// The successor's refs column, written beside its snapshot ("{}" when it holds no ref).
	refsW, ok := sumCounter(rm, "dwarf_state_write_bytes", "column", "state_refs")
	assert.True(ok, "dwarf_state_write_bytes{column=state_refs} should be present")
	assert.True(refsW > 0)
	readBytes, ok := sumCounter(rm, "dwarf_state_read_bytes", "workflow", "metricsflow.verify:428/g")
	assert.True(ok, "dwarf_state_read_bytes{workflow} should be present")
	assert.True(readBytes > 0, "state read bytes should be positive, got %d", readBytes)

	// The queue-depth observable gauge always emits a point at collection time.
	assert.True(gaugePresent(rm, "dwarf_steps_queue_depth"), "dwarf_steps_queue_depth gauge should be present")
	assert.True(gaugePresent(rm, "dwarf_state_cache_bytes"), "dwarf_state_cache_bytes gauge should be present")
}

// gaugeAt returns the data point of an int64 or float64 observable gauge whose attributes include every
// key=value in attrs.
func gaugeAt(rm metricdata.ResourceMetrics, name string, attrs map[string]string) (float64, bool) {
	matches := func(set attribute.Set) bool {
		for k, want := range attrs {
			v, ok := set.Value(attribute.Key(k))
			if !ok || v.AsString() != want {
				return false
			}
		}
		return true
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch g := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, dp := range g.DataPoints {
					if matches(dp.Attributes) {
						return float64(dp.Value), true
					}
				}
			case metricdata.Gauge[float64]:
				for _, dp := range g.DataPoints {
					if matches(dp.Attributes) {
						return dp.Value, true
					}
				}
			}
		}
	}
	return 0, false
}

// TestMetrics_BacklogGauges pins the backlog gauges' three properties: a collection makes no query (the
// per-band pair is read from the cache the shard's leader refreshes, the lag from the band scan), the age is
// measured from when a step came DUE, and a band that empties reads zero rather than vanishing.
func TestMetrics_BacklogGauges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("B")
	g.SetURL("A", "backloggauges/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("backloggauges/g", g)
	mux.HandleTask("backloggauges/a", func(ctx context.Context, f *workflow.Flow) error {
		entered <- struct{}{}
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(mux))
	assert.NoError(e.SetMeterProvider(mp))
	assert.NoError(e.SetWorkers(1)) // one worker, held inside the first task: the rest stay pending
	assert.NoError(e.Startup(t.Context()))
	var flowKeys []string
	for range 4 {
		flowKey, err := e.Create(ctx, "backloggauges/g", nil, nil)
		assert.NoError(err)
		flowKeys = append(flowKeys, flowKey)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the first task never started")
	}
	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	// Made due 30s ago while created now: an age read from created_at stays far below that for the whole test.
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -30000) WHERE status='"+workflow.StatusPending+"'")
	assert.NoError(err)

	shard1 := map[string]string{"shard": "1"}
	band := map[string]string{"shard": "1", "priority": "3"}
	var rm metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &rm))
	assert.True(e.backlogWanted.Load(), "a collection asks for the backlog")
	_, ok := gaugeAt(rm, "dwarf_steps_pending", band)
	assert.False(ok, "nothing is reported before the leader's first refresh: a collection makes no query")

	e.refreshBacklog(ctx)
	deadline := time.Now().Add(5 * time.Second * enginetest.TimeoutScale())
	for {
		assert.NoError(reader.Collect(ctx, &rm))
		if lag, ok := gaugeAt(rm, "dwarf_steps_dispatch_lag_seconds", shard1); ok && lag >= 25 || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	pending, ok := gaugeAt(rm, "dwarf_steps_pending", band)
	assert.True(ok, "the leader reports its shard's backlog")
	assert.Equal(float64(3), pending)
	oldest, ok := gaugeAt(rm, "dwarf_steps_oldest_pending_age_seconds", band)
	assert.True(ok)
	assert.True(oldest >= 25, "the age is measured from not_before, got %v", oldest)
	lag, ok := gaugeAt(rm, "dwarf_steps_dispatch_lag_seconds", shard1)
	assert.True(ok, "the dispatcher reports the lag its band scan saw")
	assert.True(lag >= 25, "the lag is measured from not_before, got %v", lag)

	close(release)
	for _, flowKey := range flowKeys {
		outcome, err := e.Await(ctx, flowKey)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}
	}
	e.refreshBacklog(ctx)
	assert.NoError(reader.Collect(ctx, &rm))
	pending, ok = gaugeAt(rm, "dwarf_steps_pending", band)
	assert.True(ok, "a band that emptied still reports, as zero")
	assert.Equal(float64(0), pending)
}

// TestMetrics_BacklogLeaderOnly pins that the backlog aggregate runs on one replica per shard: a dispatcher
// that is not first in the shard's dispatcher window neither queries nor reports it.
func TestMetrics_BacklogLeaderOnly(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(hosts.NewMux()))
	assert.NoError(e.SetMeterProvider(mp))
	assert.NoError(e.Startup(t.Context()))
	assert.True(e.leadsBacklog(1), "a solo dispatcher leads its shard")
	e.refreshBacklog(ctx)
	e.backlogMu.Lock()
	_, cached := e.backlogByShard[1]
	e.backlogMu.Unlock()
	assert.True(cached)

	// A peer that joined earlier ranks first, so it takes the window's first place and this replica the second.
	before := peerCounts(e)
	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_peers (engine_id, seen_at, ordering_seq, dispatched_at) VALUES (?, NOW_UTC(), ?, NOW_UTC())",
		int64(1), time.Now().UnixMilli()-60000)
	assert.NoError(err)
	awaitPeerChange(t, e, before)
	assert.True(e.dispatchesOn(1), "still a dispatcher")
	assert.False(e.leadsBacklog(1), "but not the leader")

	e.refreshBacklog(ctx)
	e.backlogMu.Lock()
	_, cached = e.backlogByShard[1]
	e.backlogMu.Unlock()
	assert.False(cached, "a dispatcher that does not lead the shard drops its reading, so it stops reporting")
}

// TestBacklog_ResumeStampsNotBefore pins that a step returned to pending is due from NOW, not from whenever it
// first became due. The dispatch lag, the backlog age and the steal grace all read not_before as "due since":
// left alone, a step resumed after days on an interrupt reads as days overdue and is stealable by every peer.
func TestBacklog_ResumeStampsNotBefore(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("R")
	g.SetURL("Ask", "resumestamps/ask")
	g.AddTransition("Ask", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("resumestamps/g", g)
	mux.HandleTask("resumestamps/ask", func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(map[string]any{"q": 1}, nil)
		return err
	})
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(mux))
	assert.NoError(e.Startup(t.Context()))
	flowKey, err := e.Create(ctx, "resumestamps/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err := e.Await(ctx, flowKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status) {
		return
	}
	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	// The interrupt waited an hour.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -3600000)")
	assert.NoError(err)
	assert.NoError(e.Resume(ctx, flowKey, map[string]any{"a": 1}))
	// Nothing after the resume writes not_before, so it still holds the resume's stamp once the flow is done.
	outcome, err = e.Await(ctx, flowKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
		return
	}
	var dueMs float64
	err = db.QueryRowContext(ctx, "SELECT DATE_DIFF_MILLIS(NOW_UTC(), not_before) FROM dwarf_steps").Scan(&dueMs)
	assert.NoError(err)
	assert.True(dueMs < 60000, "a resumed step is due from the resume, got %vms overdue", dueMs)
}

// TestMetrics_RefillInstrumented pins the refiller's four instruments, which exist to answer a question no
// other instrument can: what the refiller actually costs, and how much of that cost is wasted.
//
// The waste is structural rather than a bug. The refiller is triggered after EVERY processStep, coalesced
// into a single slot, and each pass wholesale-REPLACES the cache - so whenever it turns faster than the
// workers drain (the deep-backlog case it exists for), it discards candidates the previous pass selected
// and paid a round-trip to fetch. Those steps stay `pending` and are re-selected, so it is cost, not loss.
// The selected/discarded ratio is the only way to see it.
//
// SetWorkers(0) makes all of this deterministic: nothing dispatches, so the backlog is stable and the
// second pass necessarily discards exactly what the first one cached.
func TestMetrics_RefillInstrumented(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	mux := hosts.NewMux()
	g := workflow.NewGraph("R")
	g.SetURL("A", "refillmetrics/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("refillmetrics/g", g)
	mux.HandleTask("refillmetrics/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(mux))
	assert.NoError(e.SetMeterProvider(mp))
	assert.NoError(e.SetWorkers(0)) // nothing dispatches: the backlog stays put
	assert.NoError(e.Startup(t.Context()))

	for range 8 {
		_, err := e.Create(ctx, "refillmetrics/g", nil, &workflow.FlowOptions{FairnessKey: "tenant"})
		assert.NoError(err)
	}

	// Two cycles over the same stable backlog. The first fills the cache; the second replaces it, so
	// everything the first selected is discarded un-popped. Driven by hand through the engine's own piston,
	// which is also what proves the engine handed it the meter: the instruments are the PISTON's, resolved
	// from the scope the engine resolved once (initRuntime), not built a second time here.
	assert.True(e.cache.Capacity() > 0)
	// The piston STAYS IDLE, which is what makes driving it by hand legal: Piston.Cycle is not safe to call
	// concurrently with Run, and an idle Run never enters a cycle at all (it parks on the idle poll), so the
	// test goroutine is the only one touching the pipeline's cadence timestamps. Cycle itself does not
	// consult the idle flag, so the forced cycles run in full. Clearing the flag here instead released the
	// engine's own Run goroutine to cycle the same pipeline alongside these calls - a real data race, and
	// one that fails whichever OTHER tests happen to be running when the detector trips.
	e.pistons[1].SetTallyCadence(0, 0)
	e.pistons[1].SetSupplyCadence(0, 0)
	e.pistons[1].Cycle(ctx)
	e.pistons[1].Cycle(ctx)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}

	// Every cycle is counted by its band_keys phase - there is no end-to-end cycle histogram, so the phases
	// are what says a cycle happened at all as well as which part of it was slow.
	passes, ok := histCount(rm, "dwarf_refill_query_duration_seconds", map[string]string{"phase": "band_keys", "shard": "1"})
	assert.True(ok, "dwarf_refill_query_duration_seconds{phase=band_keys} should be present")
	assert.True(passes >= 2, "expected at least the 2 forced cycles, got %d", passes)

	// The per-shard query histogram is the one that isolates a straggler, so BOTH scan phases must be
	// labelled and attributed to a shard - a pass that timed only the aggregate would be unable to tell a
	// slow band scan (the plan-instability case) from a slow targeted fetch.
	for _, phase := range []string{"band_keys", "fetch_steps"} {
		n, ok := histCount(rm, "dwarf_refill_query_duration_seconds", map[string]string{"phase": phase, "shard": "1"})
		assert.True(ok, "dwarf_refill_query_duration_seconds{phase=%s,shard=1} should be present", phase)
		assert.True(n > 0, "phase %s recorded no samples", phase)
	}

	// Selected and discarded: the first pass cached a batch, the second threw it away.
	selected, ok := sumCounter(rm, "dwarf_refill_candidates_selected", "", "")
	assert.True(ok, "dwarf_refill_candidates_selected should be present")
	assert.True(selected > 0, "expected candidates to be selected from an 8-step backlog, got %d", selected)

	discarded, ok := sumCounter(rm, "dwarf_refill_candidates_discarded", "", "")
	assert.True(ok, "dwarf_refill_candidates_discarded should be present")
	assert.True(discarded > 0, "the second pass must discard the first pass's un-popped batch, got %d", discarded)
}

// TestMetrics_ForkCountsAsStarted pins that a Fork increments dwarf_flows_started. A fork builds its new
// root flow through its own INSERT...SELECT clone path, which never called metricFlowStarted - but the
// fork's completion runs through the same completeFlow that increments dwarf_flows_closed. So a fork
// was counted on the way out and never on the way in: the standard in-flight panel (started - ended)
// drifts NEGATIVE by one per fork, or permanently understates. Both counters must move together.
func TestMetrics_ForkCountsAsStarted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	mux := hosts.NewMux()
	g := workflow.NewGraph("Forked")
	g.SetURL("A", "forkmetric.verify:428/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("forkmetric.verify:428/g", g)
	mux.HandleTask("forkmetric.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	// One flow, run to completion: started=1, ended=1.
	flowKey, outcome, err := eng.Run(ctx, "forkmetric.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Fork it from its only step, and let the fork run to completion too.
	hist, err := eng.History(ctx, flowKey)
	if !assert.NoError(err) || !assert.True(len(hist) > 0, "the origin has a step to fork from") {
		return
	}
	forkKey, err := eng.Fork(ctx, hist[0].StepKey, nil)
	if !assert.NoError(err) {
		return
	}
	forkOut, err := eng.Await(ctx, forkKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, forkOut.Status)
	// Run and Await can return as soon as the await detector reads the committed row, before the completing
	// worker counts the flow ended; the stop checkpoint fires after the count.
	enginetest.AwaitFlowStatus(t, eng, flowKey, workflow.StatusCompleted, 15*time.Second)
	enginetest.AwaitFlowStatus(t, eng, forkKey, workflow.StatusCompleted, 15*time.Second)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	started, ok := sumCounter(rm, "dwarf_flows_started", "", "")
	assert.True(ok, "dwarf_flows_started should be present")
	ended, ok := sumCounter(rm, "dwarf_flows_closed", "status", workflow.StatusCompleted)
	assert.True(ok, "dwarf_flows_closed{status=completed} should be present")

	// Two flows started (the original and the fork), two ended. The in-flight gauge a dashboard builds
	// from these - started minus ended - must be 0, not -1.
	assert.Equal(int64(2), started, "the fork counts as a started flow")
	assert.Equal(int64(2), ended)
	assert.Equal(int64(0), started-ended, "in-flight (started - ended) must not go negative")
}

// TestOrphanDetection_EmitsMetric pins that the orphan sweep increments dwarf_flows_orphaned (labelled by
// workflow) when it finds a stranded flow it cannot repair - the operability alarm that lets an operator alert on
// such a residual orphan without scraping the error log. It forges the same orphan shape the
// log test uses (a completed flow flipped back to running, its step backdated stale) and asserts the count.
func TestOrphanDetection_EmitsMetric(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	mux := hosts.NewMux()
	g := workflow.NewGraph("Orphan")
	g.SetURL("A", "orphanmetric.verify:428/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("orphanmetric.verify:428/g", g)
	mux.HandleTask("orphanmetric.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	key, err := eng.Create(ctx, "orphanmetric.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	out, err := eng.Await(ctx, key)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
		return
	}
	shard, _, flowID, _, err := keys.ParseFlowKey(key)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}

	// The Startup sweep runs recoverOrphanedFlows too; let it finish before forging, or it counts the orphan
	// a second time and this test's "exactly once" reads two.
	awaitStartupRecoverySweep(t, eng)

	// Forge the orphan shape: running flow, every step terminal and stale past the threshold, and no step flagged
	// as advancing, so the sweep has no lost transition to re-run and counts the flow instead.
	pastMs := -(eng.orphanFlowThreshold + time.Minute).Milliseconds()
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET status=?, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		workflow.StatusRunning, pastMs, flowID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET advancing=0, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?", pastMs, flowID)
	assert.NoError(err)

	eng.recoverOrphanedFlows(ctx, db, shard, true)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	orphaned, ok := sumCounter(rm, "dwarf_flows_orphaned", "workflow", "orphanmetric.verify:428/g")
	assert.True(ok, "dwarf_flows_orphaned{workflow} should be present")
	assert.Equal(int64(1), orphaned, "the forged orphan is counted exactly once")
}

// TestMetrics_InFlightStateGauges pins the one property that makes the in-flight state pair a RESIDENCY
// gauge rather than another byte counter: it rises while a task holds its carrier and falls back to zero
// when the host call returns. dwarf_state_read_bytes already reports bytes-ever-read and cannot answer
// "how much is this replica holding right now", which is the question the crew's unbounded growth for long
// tasks makes load-bearing - a worker inside ExecuteTask holds no connection, so nothing else bounds how
// many carriers are live at once.
//
// The window asserted is the HOST CALL, not processStep: the task blocks, so the reading below is taken
// with the engine parked in exactly the phase the gauge exists to measure.
func TestMetrics_InFlightStateGauges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	// Large enough that the reading cannot be confused with the empty-state floor ("{}") every flow pays.
	payload := strings.Repeat("x", 8192)

	started := make(chan struct{})
	release := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("H")
	g.SetURL("A", "inflightstate.verify:428/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("inflightstate.verify:428/g", g)
	mux.HandleTask("inflightstate.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		close(started)
		<-release
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	done := make(chan *workflow.FlowOutcome, 1)
	go func() {
		_, outcome, err := eng.Run(ctx, "inflightstate.verify:428/g", map[string]any{"doc": payload}, nil)
		if err != nil {
			done <- nil
			return
		}
		done <- outcome
	}()

	select {
	case <-started:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("task never started")
	}

	// DURING the host call: the carrier is held, so both gauges read the work in flight.
	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	steps, ok := gaugeValue(rm, "dwarf_state_in_flight_steps")
	assert.True(ok, "dwarf_state_in_flight_steps should be present")
	assert.Equal(int64(1), steps, "exactly one task is inside a host call")
	bytes, ok := gaugeValue(rm, "dwarf_state_in_flight_bytes")
	assert.True(ok, "dwarf_state_in_flight_bytes should be present")
	assert.True(bytes >= int64(len(payload)),
		"held bytes must cover the carried payload, got %d for an %d-byte field", bytes, len(payload))

	close(release)
	select {
	case outcome := <-done:
		if !assert.NotNil(outcome, "Run should have succeeded") {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("flow never completed")
	}

	// AFTER: the release is what distinguishes a gauge from a counter. Both must return to zero, or the
	// bracket leaks and every reading after the first is cumulative.
	rm = metricdata.ResourceMetrics{}
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	steps, _ = gaugeValue(rm, "dwarf_state_in_flight_steps")
	assert.Equal(int64(0), steps, "no task is in a host call once the flow completed")
	bytes, _ = gaugeValue(rm, "dwarf_state_in_flight_bytes")
	assert.Equal(int64(0), bytes, "held bytes must return to zero - a nonzero reading here is a leaked bracket")
}

// TestMetrics_ClosedCountsEveryTerminalStatus pins that dwarf_flows_closed counts FAILED and
// TERMINATED flows, not only completed ones.
//
// It regressed silently and cost nothing to miss: the instrument carried a `status` attribute and was
// described as counting "flows that have reached a terminal status", while the only call site passed
// StatusCompleted. Two things were wrong at once. `sum by (status)` invited a completed/failed/terminated
// breakdown and answered with completions alone, and the in-flight panel the dwarf_flows_started
// description recommends - started minus ended - drifted upward permanently by every flow that did
// not finish cleanly, never recovering.
//
// Both halves are asserted per status rather than in aggregate, because a single total would pass against
// a build that counted one of them twice.
func TestMetrics_ClosedCountsEveryTerminalStatus(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	mux := hosts.NewMux()

	// A graph whose only task fails, with no onError, so the flow fails.
	failing := workflow.NewGraph("Failing")
	failing.SetURL("boom", "termmetrics.verify:429/boom")
	failing.AddTransition("boom", workflow.End)
	mux.HandleGraph("termmetrics.verify:429/failing", failing)
	mux.HandleTask("termmetrics.verify:429/boom", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("intentional failure")
	})

	// A graph whose only task interrupts, parking the flow so Terminate has something live to terminate.
	parking := workflow.NewGraph("Parking")
	parking.SetURL("wait", "termmetrics.verify:429/wait")
	parking.AddTransition("wait", workflow.End)
	mux.HandleGraph("termmetrics.verify:429/parking", parking)
	mux.HandleTask("termmetrics.verify:429/wait", func(ctx context.Context, f *workflow.Flow) error {
		_, _ = f.Interrupt(nil, nil)
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	_, outcome, err := eng.Run(ctx, "termmetrics.verify:429/failing", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, outcome.Status)

	terminateKey, err := eng.Create(ctx, "termmetrics.verify:429/parking", nil, nil)
	if !assert.NoError(err) {
		return
	}
	// Await returns on any stop, and `interrupted` is one - so this parks until the entry task has actually
	// interrupted, which is what gives Terminate an open flow to terminate rather than a racing one.
	parked, err := eng.Await(ctx, terminateKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusInterrupted, parked.Status)
	assert.NoError(eng.Terminate(ctx, terminateKey, "test"))

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}

	failed, ok := sumCounter(rm, "dwarf_flows_closed", "status", workflow.StatusFailed)
	assert.True(ok, "dwarf_flows_closed{status=failed} should be present")
	assert.Equal(int64(1), failed, "a failed flow must be counted as ended")

	terminated, ok := sumCounter(rm, "dwarf_flows_closed", "status", workflow.StatusTerminated)
	assert.True(ok, "dwarf_flows_closed{status=terminated} should be present")
	assert.Equal(int64(1), terminated, "a terminated flow must be counted as ended")

	// The property the panel actually depends on: over a workload where nothing completed, starts and
	// terminations still balance.
	started, _ := sumCounter(rm, "dwarf_flows_started", "", "")
	allEnded, _ := sumCounter(rm, "dwarf_flows_closed", "", "")
	assert.Equal(started, allEnded, "started minus ended must not drift when flows fail or terminate")
}

// TestMetrics_TerminateCountsOnlyTheFlowsItClosed pins that Terminate counts and signals a flow only when its own
// UPDATE ended it. The root of the tree is kept in the set whatever its status, so a root that terminalized after
// Terminate's scan - here written straight to the row, standing in for the root's own path - matches nothing in
// the batch, and counting it anyway would end it a second time.
func TestMetrics_TerminateCountsOnlyTheFlowsItClosed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "tcoe/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("tcoe/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("X", "tcoe/x")
	child.AddTransition("X", workflow.End)
	mux.HandleGraph("tcoe/child", child)
	mux.HandleTask("tcoe/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("tcoe/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	entered, release := make(chan struct{}, 1), make(chan struct{})
	mux.HandleTask("tcoe/x", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	defer close(release)
	eng.SetHost(mux)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	fk, err := eng.Create(ctx, "tcoe/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	select {
	case <-entered: // the child is live, so Terminate's scan collects it beside the root
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "the child task never ran")
		return
	}

	eng.seams.Break(engineseams.CheckpointTerminateAfterScan)
	terminated := make(chan error, 1)
	go func() { terminated <- eng.Terminate(ctx, fk, "test") }()
	if !assert.True(eng.seams.WaitTimeout(ctx, engineseams.CheckpointTerminateAfterScan, 10*time.Second), "Terminate never finished its scan") {
		return
	}
	shardNum, _, rootID, _, _ := keys.ParseFlowKey(fk)
	db, err := eng.db.Shard(shardNum)
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status=? WHERE flow_id=?", workflow.StatusCompleted, rootID)
	assert.NoError(err)
	eng.seams.Resume(engineseams.CheckpointTerminateAfterScan)
	assert.NoError(<-terminated)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	ended, ok := sumCounter(rm, "dwarf_flows_closed", "status", workflow.StatusTerminated)
	assert.True(ok, "dwarf_flows_closed{status=terminated} should be present")
	assert.Equal(int64(1), ended, "only the child was terminated by this Terminate")
}

// TestMetrics_StartedAndClosedBalanceAcrossTrees pins that every flow counted in dwarf_flows_started is counted
// once in dwarf_flows_closed, for the paths that start or end a whole subgraph tree at once: a Fork at a step
// inside a subgraph (which clones every flow on the path to it running), and a Terminate of an interrupted root,
// which ends its interrupted subgraph child too.
func TestMetrics_StartedAndClosedBalanceAcrossTrees(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "treemetrics.verify:429/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("treemetrics.verify:429/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("Inner", "treemetrics.verify:429/inner")
	child.AddTransition("Inner", workflow.End)
	mux.HandleGraph("treemetrics.verify:429/child", child)
	mux.HandleTask("treemetrics.verify:429/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("treemetrics.verify:429/child", map[string]any{"park": f.GetBool("park")}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("treemetrics.verify:429/inner", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetBool("park") {
			_, err := f.Interrupt(nil, nil)
			return err
		}
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	if !assert.NoError(eng.Startup(t.Context())) {
		return
	}
	balanced := func(t *testing.T) {
		assert := testarossa.For(t)
		assert.Equal(eng.FlowsStarted(), eng.FlowsClosed(), "started and ended disagree")
	}

	t.Run("fork_inside_a_subgraph", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, outcome, err := eng.Run(ctx, "treemetrics.verify:429/parent", nil, nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
			return
		}
		// Run can return as soon as the await detector reads the committed row, before the completing worker
		// counts the flow ended; the stop checkpoint fires after the count.
		enginetest.AwaitFlowStatus(t, eng, flowKey, workflow.StatusCompleted, 15*time.Second)
		balanced(t)
		steps, err := eng.History(ctx, flowKey)
		if !assert.NoError(err) || !assert.Len(steps, 1) || !assert.Len(steps[0].SubHistory, 1) {
			return
		}
		forkKey, err := eng.Fork(ctx, steps[0].SubHistory[0].StepKey, nil)
		if !assert.NoError(err) {
			return
		}
		enginetest.AwaitFlowStatus(t, eng, forkKey, workflow.StatusCompleted, 15*time.Second)
		balanced(t)
	})

	parkedTree := func(t *testing.T) string {
		assert := testarossa.For(t)
		flowKey, err := eng.Create(ctx, "treemetrics.verify:429/parent", map[string]any{"park": true}, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, eng, flowKey, workflow.StatusInterrupted, 15*time.Second)
		return flowKey
	}

	t.Run("terminate_interrupted_tree", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey := parkedTree(t)
		assert.NoError(eng.Terminate(ctx, flowKey, "stop"))
		balanced(t)
		// Deleting the closed tree counts nothing more.
		assert.NoError(eng.Delete(ctx, flowKey))
		balanced(t)
	})
}

func TestMeasureOpts_MemoizesUpToTheCap(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	var memo measureOpts[int]
	build := func(k int) *attrOpts { return newAttrOpts(attribute.Int("k", k)) }

	first := memo.get(1, build)
	assert.True(first == memo.get(1, build), "a memoized tuple must return the same options")

	for k := 2; k <= measureOptsCap; k++ {
		memo.get(k, build)
	}
	assert.Equal(measureOptsCap, len(*memo.known.Load()))

	// Past the cap a new tuple is built per call and never stored, while the tuples already held still hit.
	over := measureOptsCap + 1
	a, b := memo.get(over, build), memo.get(over, build)
	assert.False(a == b, "a tuple past the cap must not be memoized")
	assert.Equal(measureOptsCap, len(*memo.known.Load()))
	assert.True(first == memo.get(1, build))
}

// slowGraphHost delays LoadGraph for one workflow URL, so a test can tell whether Create's graph load is inside
// a timing.
type slowGraphHost struct {
	*hosts.Mux
	url   string
	delay time.Duration
}

func (h slowGraphHost) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	if workflowURL == h.url {
		time.Sleep(h.delay)
	}
	return h.Mux.LoadGraph(ctx, workflowURL)
}

// TestMetrics_OperationSeconds pins dwarf_operation_seconds: every operation records under its own op label and
// the shard it worked on, timed over the shard's share of the call only - a slow graph load stays out of
// create's sample, and Await's park stays out while its reads count as snapshot.
func TestMetrics_OperationSeconds(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	// Each sample is held under the delay rather than their sum: under a loaded suite the database's own share of
	// four creates has summed past 1s, but a leaked delay lands whole in one sample, far above any of them.
	const slow = 2 * time.Second
	mux := hosts.NewMux()
	g := workflow.NewGraph("Timed")
	g.SetURL("A", "opmetric.verify:428/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("opmetric.verify:428/g", g)
	// The task outlasts the samples, so Run's await parks for it between its two reads.
	mux.HandleTask("opmetric.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		time.Sleep(slow)
		return nil
	})
	ask := workflow.NewGraph("Ask")
	ask.SetURL("Ask", "opmetric.verify:428/ask")
	ask.AddTransition("Ask", workflow.End)
	assert.NoError(ask.Validate())
	mux.HandleGraph("opmetric.verify:428/askg", ask)
	mux.HandleTask("opmetric.verify:428/ask", func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(nil, nil)
		return err
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(slowGraphHost{Mux: mux, url: "opmetric.verify:428/g", delay: slow})
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	flowKey, outcome, err := eng.Run(ctx, "opmetric.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	hist, err := eng.History(ctx, flowKey)
	if !assert.NoError(err) || !assert.True(len(hist) > 0) {
		return
	}
	_, err = eng.Step(ctx, hist[0].StepKey)
	assert.NoError(err)
	_, _, err = eng.Fingerprint(ctx, flowKey)
	assert.NoError(err)
	_, err = eng.Fork(ctx, hist[0].StepKey, nil)
	assert.NoError(err)
	_, err = eng.Continue(ctx, flowKey, nil)
	assert.NoError(err)
	_, err = eng.ListOpen(ctx, enginetest.Recent(workflow.Query{}))
	assert.NoError(err)
	_, err = eng.ListClosed(ctx, enginetest.Recent(workflow.Query{}))
	assert.NoError(err)
	_, err = eng.ShardInfo(ctx)
	assert.NoError(err)
	assert.NoError(eng.Delete(ctx, flowKey))
	_, _, err = eng.Purge(ctx, enginetest.Recent(workflow.Query{Status: workflow.StatusTerminated}))
	assert.NoError(err)
	assert.NoError(eng.PurgeBefore(ctx, time.Now().Add(-48*time.Hour)))

	// Three interrupted flows: one each to resume, terminate and cancel.
	var parked []string
	for range 3 {
		k, err := eng.Create(ctx, "opmetric.verify:428/askg", nil, nil)
		if !assert.NoError(err) {
			return
		}
		out, err := eng.Await(ctx, k)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, out.Status) {
			return
		}
		parked = append(parked, k)
	}
	assert.NoError(eng.Resume(ctx, parked[0], nil))
	assert.NoError(eng.Terminate(ctx, parked[1], "test"))
	assert.NoError(eng.Cancel(ctx, parked[2], "test"))

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	once := []string{"history", "step", "fingerprint", "fork", "continue", "list_open", "list_closed", "shard_info", "delete",
		"purge_before", "resume", "terminate", "cancel"}
	for _, op := range once {
		n, ok := histCount(rm, "dwarf_operation_seconds", map[string]string{"op": op, "shard": "1"})
		assert.True(ok, "op=%s should be recorded on shard 1", op)
		assert.Equal(uint64(1), n, "op=%s records one sample per call per shard", op)
	}
	n, _ := histCount(rm, "dwarf_operation_seconds", map[string]string{"op": "purge", "shard": "1"})
	assert.True(n >= 1, "Purge records one sample per stage it runs on a shard")
	n, _ = histCount(rm, "dwarf_operation_seconds", map[string]string{"op": "create", "shard": "1"})
	assert.Equal(uint64(4), n, "Run's create half and the three Creates")
	n, _ = histCount(rm, "dwarf_operation_seconds", map[string]string{"op": "snapshot", "shard": "1"})
	assert.True(n >= 2, "Await reads the flow before and after it parks, and those reads are snapshots")

	// Run's create half paid the graph delay outside its turn, and its await parked for the task outside any
	// turn, so neither shows up in the samples.
	longest, ok := histMax(rm, "dwarf_operation_seconds", map[string]string{"op": "create"})
	assert.True(ok)
	assert.True(longest < slow.Seconds(), "create's samples must exclude the host's graph load, got %fs", longest)
	longest, ok = histMax(rm, "dwarf_operation_seconds", map[string]string{"op": "snapshot"})
	assert.True(ok)
	assert.True(longest < slow.Seconds(), "snapshot's samples must exclude Await's park, got %fs", longest)
}

// TestOperation_HonorsDeadlineOnAFullShard pins that a caller's deadline bounds an operation queued at a shard
// with no free turn: Create returns an error at the deadline rather than waiting for a turn, and the time it
// spent waiting is still recorded against that shard.
func TestOperation_HonorsDeadlineOnAFullShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	mux := hosts.NewMux()
	g := workflow.NewGraph("Blocked")
	g.SetURL("A", "opdeadline.verify:428/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("opdeadline.verify:428/g", g)
	mux.HandleTask("opdeadline.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	eng.SetMeterProvider(mp)
	if !assert.NoError(eng.Startup(t.Context())) {
		return
	}

	// Shrink the shard's turnstile to nothing rather than taking its free turns: a background caller holding a
	// turn at that moment would hand it on to the queued Create when it returns. With no size, every return
	// lands below zero and nothing is granted.
	ts := eng.turnstiles.Turnstile(1)
	size := ts.Size()
	assert.True(size > 0, "the shard has turns")
	ts.Resize(0)
	defer ts.Resize(size)

	const deadline = 200 * time.Millisecond
	dctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	start := time.Now()
	_, err := eng.Create(dctx, "opdeadline.verify:428/g", nil, nil)
	elapsed := time.Since(start)
	assert.Error(err, "a Create that cannot get a turn before its deadline must fail")
	assert.True(errors.Is(err, context.DeadlineExceeded), "the failure is the caller's deadline, got %v", err)
	assert.True(elapsed < 5*deadline, "Create must return at the caller's deadline, took %v", elapsed)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	sum, ok := histSum(rm, "dwarf_operation_seconds", map[string]string{"op": "create", "shard": "1"})
	assert.True(ok, "the queued create is recorded")
	assert.True(sum >= deadline.Seconds()*0.9, "its sample covers the wait at the turnstile, got %fs", sum)
}
