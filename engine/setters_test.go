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
	"math"
	"net/http"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/memlimit"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSetters_ConstructionTimeOnly asserts the construction-time-only setters reject a call on a running
// engine, while the live setters succeed after Startup. The split is the contract that lets a host change
// safe knobs (time budget, pool size, shard count) hot while refusing the unsafe ones (DSN, workers,
// host, providers) rather than silently no-op'ing.
func TestSetters_ConstructionTimeOnly(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(noopHost{})
	assert.NoError(e.Startup(t.Context()))

	// Construction-time-only: rejected after Startup, as a wrong-state 409.
	for _, err := range []error{
		e.SetShard(ShardSpec{Index: 2, DSN: "file:other.sqlite"}), // shard set is immutable after Startup
		e.SetWorkers(8),
		e.SetEngineID(12345),         // id is registered at Startup and baked into signal-echo suppression
		e.SetStateCacheSize(1 << 20), // the replica's one cache is sized at Startup
		e.SetHost(noopHost{}),
		e.SetLogger(nil),
		e.SetDebugLogger(),
		e.SetMeterProvider(nil),
		e.SetTracerProvider(nil),
		e.Startup(t.Context()),
	} {
		assert.Equal(http.StatusConflict, errors.StatusCode(err), "%v", err)
	}

	// Live: succeed after Startup.
	assert.NoError(e.SetTimeBudget(30 * time.Second))
}

// TestSetShard_RejectsBadSpecs pins the status codes of construction-time configuration mistakes: an invalid
// spec or a missing host is a 400, a shard index set twice is a 409.
func TestSetShard_RejectsBadSpecs(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetShard(ShardSpec{Index: 0})))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetShard(ShardSpec{Index: 1, MaxConnections: -1})))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetShard(ShardSpec{Index: 1, Weight: -1})))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetShard(ShardSpec{Index: 1, Weight: math.NaN()})))
	assert.NoError(e.SetShard(ShardSpec{Index: 1}))
	assert.Equal(http.StatusConflict, errors.StatusCode(e.SetShard(ShardSpec{Index: 1})))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.Startup(t.Context()))) // no host
}

// TestSetWorkers_RejectsOutOfRange pins SetWorkers' bounds. The count is stored as an int32, so a value past it
// wrapped negative: the engine started and never dispatched.
func TestSetWorkers_RejectsOutOfRange(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetWorkers(-1)))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetWorkers(math.MaxInt32+1)))
	assert.NoError(e.SetWorkers(math.MaxInt32))
	assert.Equal(int32(math.MaxInt32), e.workers.Load())
	assert.NoError(e.SetWorkers(0))
}

// TestOperations_UnmarshalableInputIs400 pins that Resume's data and Fork's overrides are rejected as bad
// input before any lookup, like Create's initial state - not dropped, and not a 500.
func TestOperations_UnmarshalableInputIs400(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(noopHost{})
	assert.NoError(e.Startup(t.Context()))

	// The key is well-formed and unknown: with valid input each call is a 404, so a 400 is the input's.
	const key = "1-0-1-0123456789abcdef"
	assert.Equal(http.StatusNotFound, errors.StatusCode(e.Resume(t.Context(), key, nil)))
	_, err := e.Fork(t.Context(), key, nil)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))

	bad := map[string]any{"ch": make(chan int)}
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.Resume(t.Context(), key, bad)))
	_, err = e.Fork(t.Context(), key, bad)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
}

// TestSetEngineID_PinsIdentity pins SetEngineID: a positive id overrides the random default, while 0 and
// negatives are rejected. 0 is the
// pre-column/no-engine sentinel, so accepting it would stamp provenance and heartbeat rows that read as
// "no engine"; the invariant is a positive identifier. The stable-id opt-in exists so a crash-restart reuses
// its one registry row instead of leaving a stale row that transiently over-counts replicas.
func TestSetEngineID_PinsIdentity(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NotEqual(int64(0), e.engineID, "the default id is random and non-zero")

	assert.Error(e.SetEngineID(0), "0 is the pre-column/no-engine sentinel")
	assert.Error(e.SetEngineID(-1), "the id must be a positive identifier")

	assert.NoError(e.SetEngineID(424242))
	assert.Equal(int64(424242), e.engineID)
}

// TestDefaultPriority_IsTheMidpoint pins the priority a flow that sets none gets: the midpoint of 1..max, so it
// moves with the range and can never fall outside it - a flow stamped outside the range would be served only as
// the overflow.
func TestDefaultPriority_IsTheMidpoint(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.Equal(3, e.defaultPriority(), "the midpoint of 1..5")
	assert.NoError(e.SetMaxPriority(100))
	assert.Equal(50, e.defaultPriority())
	assert.NoError(e.SetMaxPriority(1))
	assert.Equal(1, e.defaultPriority())

	mux := hosts.NewMux()
	g := workflow.NewGraph("Prio")
	g.SetURL("A", "prio/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("prio/g", g)
	mux.HandleTask("prio/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e2 := NewEngineUnderTest(t.Name())
	defer e2.Shutdown(t.Context())
	assert.NoError(e2.SetHost(mux))
	assert.NoError(e2.Startup(t.Context()))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*enginetest.TimeoutScale()) // bounds only a hang
	defer cancel()
	_, out, err := e2.Run(ctx, "prio/g", nil, nil)
	if !assert.NoError(err, "a flow at the default priority must dispatch") {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
}

// TestSetStateCacheSize_SharedByShards pins the setter's contract: a non-positive size is rejected, and the
// size is one budget for every shard - a field that fits it is kept whichever shard it is on, and a field on
// one shard can push out another shard's.
func TestSetStateCacheSize_SharedByShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetStateCacheSize(0)))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetStateCacheSize(-1)))

	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2}))
	assert.NoError(e.SetStateCacheSize(8 << 20))
	assert.NoError(e.Startup(t.Context()))

	total, source := e.stateCacheBudget()
	assert.Equal(int64(8<<20), total)
	assert.Equal("SetStateCacheSize", source)

	seed := func(shard int, size int) int64 {
		state, _ := workflow.NewState()
		state.SetJSON("doc", []byte(`"`+strings.Repeat("x", size-2)+`"`))
		e.linkerFor(shard).Seed("ftok", 9, state, workflow.NewChanges(), []byte(`{"doc":9}`))
		return e.stateCache.Bytes()
	}
	assert.True(seed(1, 5<<20) > 5<<20, "5 MiB fits the 8 MiB the shards share")
	held := seed(2, 5<<20)
	assert.True(held > 5<<20 && held < 6<<20, "shard 2's 5 MiB field pushes out shard 1's: one budget, not two")
	assert.Equal(held, seed(1, 9<<20), "a field larger than the whole cache is not kept")
}

// TestStateCacheBudget_Default pins the default without an override: a tenth of the detected memory
// limit. It sets the process's memory limit so a machine with none to detect still checks the arithmetic, and
// so it does not run in parallel: the limit is process-wide.
func TestStateCacheBudget_Default(t *testing.T) {
	assert := testarossa.For(t)

	prev := debug.SetMemoryLimit(40 << 30)
	defer debug.SetMemoryLimit(prev)
	limit, detected := memlimit.Detect()
	assert.Equal(int64(40<<30), limit)
	total, source := NewEngine().stateCacheBudget()
	assert.Equal(limit/10, total)
	assert.Equal(detected, source)
}

// TestCreate_RejectsValuesTheColumnsCannotHold pins the 400s for a priority or a time budget too large for their
// 32-bit columns: on Postgres the insert otherwise fails with a driver error ("unable to encode 2592000000 into
// binary format for int4"), and on SQLite the value is stored. The largest value accepted runs: on Postgres a
// budget at the full 2^31-1 ms is stored, then every claim overflows adding the lease margin and the flow never
// runs.
func TestCreate_RejectsValuesTheColumnsCannotHold(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("Bounds")
	g.SetURL("A", "bounds/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("bounds/wf", g)
	mux.HandleTask("bounds/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	for name, opts := range map[string]*workflow.FlowOptions{
		"priority":    {Priority: math.MaxInt32 + 1},
		"time budget": {TimeBudget: maxTimeBudget + time.Millisecond},
	} {
		_, err := e.Create(ctx, "bounds/wf", nil, opts)
		assert.Error(err, name)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), name)
	}
	_, outcome, err := e.Run(ctx, "bounds/wf", nil, &workflow.FlowOptions{Priority: e.maxPriority, TimeBudget: maxTimeBudget})
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)

	err = e.SetTimeBudget(maxTimeBudget + time.Millisecond)
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	assert.NoError(e.SetTimeBudget(maxTimeBudget))
}

// TestSetMaxPriority pins the range setter: 1..100 and construction-time only. A Create past the maximum is refused rather than stored as
// a step the refiller would never select.
func TestSetMaxPriority(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.Error(e.SetMaxPriority(0))
	assert.Error(e.SetMaxPriority(101), "every band up to the maximum costs the scan a probe each cycle")
	assert.NoError(e.SetMaxPriority(100))

	mux := hosts.NewMux()
	g := workflow.NewGraph("MaxPrio")
	g.SetURL("A", "maxprio/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("maxprio/g", g)
	mux.HandleTask("maxprio/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	e2 := NewEngineUnderTest(t.Name())
	defer e2.Shutdown(t.Context())
	assert.NoError(e2.SetHost(mux))
	assert.NoError(e2.SetMaxPriority(3))
	assert.NoError(e2.Startup(t.Context()))
	assert.Error(e2.SetMaxPriority(4), "construction-time only")

	for _, p := range e2.pistons {
		assert.Equal(3, p.MaxPriority(), "every piston counts the engine's range")
	}

	_, err := e2.Create(t.Context(), "maxprio/g", nil, &workflow.FlowOptions{Priority: 4})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	_, outcome, err := e2.Run(t.Context(), "maxprio/g", nil, &workflow.FlowOptions{Priority: 3})
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, "the worst band is served")
	}
}
