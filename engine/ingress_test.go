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
	"database/sql"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// heldGraphHost holds LoadGraph for one workflow URL: entered closes as the call arrives, and the call returns when
// Release is called or, if honorCtx, when its ctx ends. One that does not honor its ctx closes cancelled when the ctx
// ends, and holds on.
type heldGraphHost struct {
	*hosts.Mux
	url       string
	honorCtx  bool
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	once      sync.Once
}

func newHeldGraphHost(mux *hosts.Mux, url string, honorCtx bool) *heldGraphHost {
	return &heldGraphHost{Mux: mux, url: url, honorCtx: honorCtx,
		entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
}

func (h *heldGraphHost) Release() { h.once.Do(func() { close(h.release) }) }

func (h *heldGraphHost) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	if workflowURL == h.url {
		close(h.entered)
		select {
		case <-h.release:
		case <-ctx.Done():
			if h.honorCtx {
				return nil, ctx.Err()
			}
			close(h.cancelled)
			<-h.release
		}
	}
	return h.Mux.LoadGraph(ctx, workflowURL)
}

// assertNoDrainYet asserts that Shutdown has not begun its drain - the first thing drainRuntime does is close
// drainStop - while a call it admitted is held. A Shutdown that did not wait for the call would begin it at once, so
// the window only has to outlast that.
func assertNoDrainYet(t *testing.T, drainStop chan struct{}, why string) {
	assert := testarossa.For(t)
	select {
	case <-drainStop:
		assert.Must(assert.True(false, why))
	case <-time.After(200 * time.Millisecond):
	}
}

// ingressSoloMux serves one graph of one quick task under prefix/g, and the same graph under prefix/held for a
// heldGraphHost to hold.
func ingressSoloMux(prefix string) *hosts.Mux {
	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", prefix+"/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph(prefix+"/g", g)
	mux.HandleGraph(prefix+"/held", g)
	mux.HandleTask(prefix+"/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	return mux
}

// callsInFlight is how many calls admitIngress has admitted and not yet counted out.
func callsInFlight(e *Engine) int {
	e.ingressMu.Lock()
	defer e.ingressMu.Unlock()
	return len(e.ingress)
}

// assertRestartServes restarts a shut-down engine and runs a flow on it. A caller restarts before it collects what the
// previous run's calls returned, so that under -race nothing those calls did after counting out - a read of e.metrics,
// say - is ordered before the restart's writes of the per-run state.
func assertRestartServes(t *testing.T, e *Engine, workflowURL string) {
	assert := testarossa.For(t)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	_, outcome, err := e.Run(ctx, workflowURL, nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
}

// TestIngress_ShutdownEndsACallInFlight pins that Shutdown cancels a call it admitted and returns only after the call
// has: a Create held in the host's LoadGraph, which honors its ctx, is ended with a 503 before Shutdown returns, so it
// cannot run on into the restarted engine.
func TestIngress_ShutdownEndsACallInFlight(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	host := newHeldGraphHost(ingressSoloMux("ingress.end"), "ingress.end/held", true)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer host.Release() // below the Shutdown defer, so it unwinds first: a held LoadGraph would hold the drain
	e.SetHost(host)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	created := make(chan error, 1)
	go func() {
		_, err := e.Create(ctx, "ingress.end/held", nil, nil)
		created <- err
	}()
	select {
	case <-host.entered:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Create never reached LoadGraph"))
	}

	assert.NoError(e.Shutdown(ctx))
	assert.Must(assert.Zero(callsInFlight(e), "Shutdown returned with a call it admitted still in flight"))
	assertRestartServes(t, e, "ingress.end/g")
	select {
	case err := <-created:
		if assert.Error(err) {
			assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err))
		}
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Create never returned")
	}
}

// TestIngress_ShutdownWaitsForACallThatIgnoresItsContext pins that Shutdown waits for every call it admitted, not
// only for those that heed the cancellation: while the host holds LoadGraph past its ctx's end, Shutdown does not begin
// its drain, and the Create, released, ends with a 503 before Shutdown returns.
func TestIngress_ShutdownWaitsForACallThatIgnoresItsContext(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	host := newHeldGraphHost(ingressSoloMux("ingress.wait"), "ingress.wait/held", false)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer host.Release() // below the Shutdown defer, so it unwinds first: a held LoadGraph would hold the drain
	e.SetHost(host)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	drainStop := e.drainStop

	created := make(chan error, 1)
	go func() {
		_, err := e.Create(ctx, "ingress.wait/held", nil, nil)
		created <- err
	}()
	select {
	case <-host.entered:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Create never reached LoadGraph"))
	}

	stopped := make(chan error, 1)
	go func() {
		stopped <- e.Shutdown(ctx)
	}()
	select {
	case <-host.cancelled:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Shutdown never cancelled the call it admitted"))
	}
	assertNoDrainYet(t, drainStop, "Shutdown began its drain while the host still held a call it admitted")

	host.Release()
	select {
	case err := <-stopped:
		assert.NoError(err)
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Shutdown did not return once the host released the call"))
	}
	assert.Must(assert.Zero(callsInFlight(e), "Shutdown returned before the call it admitted did"))
	assertRestartServes(t, e, "ingress.wait/g")
	select {
	case err := <-created:
		if assert.Error(err) {
			assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err))
		}
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Create never returned")
	}
}

// counterGate holds a call in the counter's Add: arrived closes as the first call arrives, and every call waits for
// release.
type counterGate struct {
	arrived chan struct{}
	release chan struct{}
	once    sync.Once
}

// gatedMeterProvider hands out meters whose dwarf_ingress_operations counter holds in Add at the gate, while one is
// set.
type gatedMeterProvider struct {
	metric.MeterProvider
	gate *atomic.Pointer[counterGate]
}

func (p gatedMeterProvider) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	return gatedMeter{p.MeterProvider.Meter(name, opts...), p.gate}
}

type gatedMeter struct {
	metric.Meter
	gate *atomic.Pointer[counterGate]
}

func (m gatedMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	c, err := m.Meter.Int64Counter(name, opts...)
	if name == "dwarf_ingress_operations" {
		return gatedCounter{c, m.gate}, err
	}
	return c, err
}

type gatedCounter struct {
	metric.Int64Counter
	gate *atomic.Pointer[counterGate]
}

func (c gatedCounter) Add(ctx context.Context, incr int64, opts ...metric.AddOption) {
	if g := c.gate.Load(); g != nil {
		g.once.Do(func() { close(g.arrived) })
		<-g.release
	}
	c.Int64Counter.Add(ctx, incr, opts...)
}

// TestIngress_TheCallIsCountedBeforeItLeaves pins that a call records dwarf_ingress_operations while it is still
// counted in, so Shutdown still waits for it: recorded after the call counts out, the read of e.metrics races the next
// Startup's write of it. Held in the counter's Add, the call must still be counted in.
func TestIngress_TheCallIsCountedBeforeItLeaves(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var gate atomic.Pointer[counterGate]
	host := newHeldGraphHost(ingressSoloMux("ingress.count"), "ingress.count/held", true)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer host.Release() // below the Shutdown defer, so it unwinds first: a held LoadGraph would hold the drain
	e.SetHost(host)
	assert.Must(assert.NoError(e.SetMeterProvider(gatedMeterProvider{noop.NewMeterProvider(), &gate})))
	assert.Must(assert.NoError(e.Startup(t.Context())))

	created := make(chan error, 1)
	go func() {
		_, err := e.Create(ctx, "ingress.count/held", nil, nil)
		created <- err
	}()
	select {
	case <-host.entered:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Create never reached LoadGraph"))
	}

	g := &counterGate{arrived: make(chan struct{}), release: make(chan struct{})}
	gate.Store(g)
	releaseCount := sync.OnceFunc(func() { close(g.release) })
	defer releaseCount()
	stopped := make(chan error, 1)
	go func() {
		stopped <- e.Shutdown(ctx)
	}()
	select {
	case <-g.arrived:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "the call Shutdown ended never recorded its count"))
	}
	assert.Must(assert.Equal(1, callsInFlight(e), "the call counted out before it recorded its count"))

	gate.Store(nil)
	releaseCount()
	select {
	case err := <-stopped:
		assert.NoError(err)
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Shutdown did not return once the count was recorded"))
	}
	assertRestartServes(t, e, "ingress.count/g")
	select {
	case err := <-created:
		assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err))
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Create never returned")
	}
}

// TestIngress_ShutdownEndsTheWaits pins that an Await, a Poll and a Run parked on running flows are ended by Shutdown
// with a 503 - not Await's 408 for a wait that ran out, and not Poll's not-stopped answer, which tells the caller its
// own budget ran out - and that the Run keeps the key of the flow it created.
func TestIngress_ShutdownEndsTheWaits(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := ingressSoloMux("ingress.park")
	g := workflow.NewGraph("Sleeper")
	g.SetURL("Nap", "ingress.park/nap")
	g.SetURL("Wake", "ingress.park/wake")
	g.AddTransitionChain("Nap", "Wake", workflow.End)
	mux.HandleGraph("ingress.park/sleeper", g)
	// The sleep defers Nap's successor an hour, so the flow stays running with no worker in a task.
	mux.HandleTask("ingress.park/nap", func(ctx context.Context, f *workflow.Flow) error {
		f.Sleep(time.Hour)
		return nil
	})
	mux.HandleTask("ingress.park/wake", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.setClaimsTrackerEnabled(false)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	awaitKey, err := e.Create(ctx, "ingress.park/sleeper", nil, nil)
	assert.Must(assert.NoError(err))
	pollKey, err := e.Create(ctx, "ingress.park/sleeper", nil, nil)
	assert.Must(assert.NoError(err))

	type result struct {
		flowKey string
		outcome *workflow.FlowOutcome
		err     error
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	waits := []struct {
		name   string
		parked <-chan struct{}
		result chan result
		call   func() result
	}{
		{"Await", e.diags.Seams().Waiter(diagnostics.Join(diagnostics.CheckpointAwaitParked, awaitKey)), make(chan result, 1),
			func() result {
				outcome, err := e.Await(waitCtx, awaitKey)
				return result{awaitKey, outcome, err}
			}},
		{"Poll", e.diags.Seams().Waiter(diagnostics.Join(diagnostics.CheckpointAwaitParked, pollKey)), make(chan result, 1),
			func() result {
				outcome, err := e.Poll(waitCtx, pollKey)
				return result{pollKey, outcome, err}
			}},
		// Run's key is not known before it returns, so its park is the unscoped one, armed once the others have parked.
		{"Run", nil, make(chan result, 1),
			func() result {
				flowKey, outcome, err := e.Run(waitCtx, "ingress.park/sleeper", nil, nil)
				return result{flowKey, outcome, err}
			}},
	}
	for _, w := range waits {
		parked := w.parked
		if parked == nil {
			parked = e.diags.Seams().Waiter(diagnostics.CheckpointAwaitParked)
		}
		go func() { w.result <- w.call() }()
		select {
		case <-parked:
		case r := <-w.result:
			status := ""
			if r.outcome != nil {
				status = r.outcome.Status
			}
			assert.Must(assert.True(false, "%s returned before it parked: status %q, error %v", w.name, status, r.err))
		case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
			assert.Must(assert.True(false, "%s never parked", w.name))
		}
	}

	assert.NoError(e.Shutdown(ctx))
	assert.Must(assert.Zero(callsInFlight(e), "Shutdown returned with a wait still parked"))
	assertRestartServes(t, e, "ingress.park/g")
	for _, w := range waits {
		select {
		case r := <-w.result:
			assert.Nil(r.outcome, w.name)
			assert.NotEqual("", r.flowKey, w.name)
			if assert.Error(r.err, w.name) {
				assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(r.err), w.name)
			}
		case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
			assert.True(false, "%s never returned", w.name)
		}
	}
}

// TestIngress_ShutdownEndsShardInfo pins that a ShardInfo Shutdown ends returns a 503, not a success whose every row
// reports the cancelled probe as a shard down.
func TestIngress_ShutdownEndsShardInfo(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(ingressSoloMux("ingress.shards"))
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// Every connection of the shard's pool held, so ShardInfo's probe waits for one until Shutdown cancels it.
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB().DB
	var held []*sql.Conn
	releaseConns := sync.OnceFunc(func() {
		for _, c := range held {
			c.Close()
		}
	})
	defer releaseConns() // below the Shutdown defer, so it unwinds first: the drain needs the connections
	connCtx, cancelConn := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancelConn()
	for range db.Stats().MaxOpenConnections {
		c, err := db.Conn(connCtx)
		assert.Must(assert.NoError(err))
		held = append(held, c)
	}

	type result struct {
		shards []ShardSummary
		err    error
	}
	probed := make(chan result, 1)
	go func() {
		shards, err := e.ShardInfo(ctx)
		probed <- result{shards, err}
	}()
	assert.Must(assert.True(enginetest.WaitUntil(t, 30*time.Second, func() bool { return callsInFlight(e) == 1 }),
		"ShardInfo was never admitted"))

	stopped := make(chan error, 1)
	go func() {
		stopped <- e.Shutdown(ctx)
	}()
	select {
	case r := <-probed:
		assert.Nil(r.shards)
		if assert.Error(r.err) {
			assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(r.err))
		}
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "ShardInfo never returned"))
	}
	releaseConns()
	select {
	case err := <-stopped:
		assert.NoError(err)
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Shutdown never returned"))
	}
	assertRestartServes(t, e, "ingress.shards/g")
}
