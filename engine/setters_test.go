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
	"testing"
	"time"

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
		e.SetEngineID(12345), // id is registered at Startup and baked into signal-echo suppression
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
	assert.NoError(e.SetDefaultPriority(5))
	assert.NoError(e.SetMaxOpenConns(4))
}

// TestSetShard_RejectsBadSpecs pins the status codes of construction-time configuration mistakes: an invalid
// spec or a missing host is a 400, a shard index set twice is a 409.
func TestSetShard_RejectsBadSpecs(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetShard(ShardSpec{Index: 0})))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: -1})))
	assert.NoError(e.SetShard(ShardSpec{Index: 1}))
	assert.Equal(http.StatusConflict, errors.StatusCode(e.SetShard(ShardSpec{Index: 1})))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.Startup(t.Context()))) // no host
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
	const key = "1-1-abc"
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
// its one registry row instead of leaving a ghost that transiently over-counts replicas.
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

// TestSetDefaultPriority_RejectsNonPositive pins the validation on the engine-level default. A flow stamped
// with a non-positive priority is invisible to the refiller's band selection: it sits `pending` forever
// while the doorbell re-rings it in a loop, and no error is ever raised. `create` already rejected a
// negative FlowOptions.Priority as a caller bug, but the engine-level default reached the same column
// unchecked. The int32 upper bound matters for the same reason - a value that overflows the column's width
// (3_000_000_000, say) wraps NEGATIVE and produces exactly that hang.
func TestSetDefaultPriority_RejectsNonPositive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.Error(e.SetDefaultPriority(0), "0 is not a valid priority (1 is the lowest)")
	assert.Error(e.SetDefaultPriority(-1), "a negative priority hides every flow from the refiller")
	assert.Error(e.SetDefaultPriority(math.MaxInt32+1), "a value beyond int32 wraps negative in the column")
	assert.NoError(e.SetDefaultPriority(1))
	assert.NoError(e.SetDefaultPriority(100))

	// The guard rejects rather than clamps: the accepted value is the one the engine dispatches with.
	proxy := NewTestProxy()
	g := workflow.NewGraph("Prio")
	g.SetURL("A", "prio/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	proxy.HandleGraph("prio/g", g)
	proxy.HandleTask("prio/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e2 := NewEngineUnderTest(t.Name())
	defer e2.Shutdown(t.Context())
	assert.NoError(e2.SetHost(proxy))
	assert.NoError(e2.SetDefaultPriority(7))
	assert.NoError(e2.Startup(t.Context()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, out, err := e2.Run(ctx, "prio/g", nil, nil)
	if !assert.NoError(err, "a flow at the default priority must dispatch") {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
}
