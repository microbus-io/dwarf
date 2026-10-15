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
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestStateRefs_LinearSuccessorIsServedFromTheSeed pins what lets the linear ref bar sit at 1KB: a step's output
// is stored once, as its own changes, with its successor holding a ref - and the successor, dispatched by the
// replica that minted the ref, resolves it from the cache that replica seeded, reading no anchor row.
func TestStateRefs_LinearSuccessorIsServedFromTheSeed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	const size = 2048
	g := workflow.NewGraph("Seeded")
	g.SetURL("A", "seeded/a")
	g.SetURL("B", "seeded/b")
	g.SetURL("C", "seeded/c")
	g.AddTransitionChain("A", "B", "C", workflow.End)
	mux := hosts.NewMux()
	mux.HandleGraph("seeded/wf", g)
	// Each task writes fresh bytes, so every hop anchors a new value at the step that wrote it.
	mux.HandleTask("seeded/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("data", blob(size))
		return nil
	})
	mux.HandleTask("seeded/b", func(ctx context.Context, f *workflow.Flow) error {
		if len(f.GetString("data")) != size {
			return errors.New("B cannot see A's output")
		}
		f.SetString("data", blob(size-1))
		return nil
	})
	mux.HandleTask("seeded/c", func(ctx context.Context, f *workflow.Flow) error {
		if len(f.GetString("data")) != size-1 {
			return errors.New("C cannot see B's output")
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.SetMeterProvider(mp)
	assert.NoError(e.Startup(t.Context()))

	_, outcome, err := e.Run(ctx, "seeded/wf", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal(size-1, len(outcome.State.GetString("data")), "final_state is flattened")

	// The value is stored once: each successor refs its predecessor instead of holding a copy.
	a := stepIDsByTask(t, e, "A")[0]
	b := stepIDsByTask(t, e, "B")[0]
	c := stepIDsByTask(t, e, "C")[0]
	bState, bRefs, _ := stepRefsOf(t, e, b)
	assert.Equal(a, bRefs["data"].Anchor)
	assert.False(bState.Has("data"))
	cState, cRefs, _ := stepRefsOf(t, e, c)
	assert.Equal(b, cRefs["data"].Anchor)
	assert.False(cState.Has("data"))

	// And no dispatch read an anchor row to resolve them.
	var rm metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &rm))
	refRead, _ := sumCounter(rm, "dwarf_state_read_bytes", "column", "state_ref")
	assert.Equal(int64(0), refRead, "a successor dispatched by the minting replica must resolve from the seeded cache")
}
