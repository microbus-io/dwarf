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

package fixtures

import (
	"context"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestBreakerreconstituteflow verifies that a restarting replica re-arms its in-memory breaker map from
// the breaker-parked rows that survive in the database. After a restart, breaker-parked steps are
// invisible to selection and only a probe can release them; without reconstitution they would be
// stranded forever. The second engine boots on the same database and must drain the parked backlog once
// the downstream recovers.
func TestBreakerreconstituteflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()
	graph := workflow.NewGraph("breakerreconstituteflow.verify:428/flow")
	graph.AddTask("work", "breakerreconstituteflow.verify:428/work")
	graph.AddTransition("work", workflow.END)
	proxy.HandleGraph("breakerreconstituteflow.verify:428/flow", graph)

	var broken atomic.Bool
	broken.Store(true)
	proxy.HandleTask("breakerreconstituteflow.verify:428/work", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		if broken.Load() {
			return errors.New("ack timeout: breakerreconstituteflow.verify:428/work", http.StatusNotFound)
		}
		return nil
	})

	// A persistent (file-backed) database so state survives the engine restart.
	dsn := "file:" + filepath.Join(t.TempDir(), "reconstitute.db")

	eng1 := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithDSN(dsn).
		WithWorkers(4)
	if !assert.NoError(eng1.Startup(ctx)) {
		return
	}

	// Create a backlog of flows; they trip the breaker and park.
	var keys []string
	for range 8 {
		k, err := eng1.Create(ctx, "breakerreconstituteflow.verify:428/flow", nil, nil, nil)
		if !assert.NoError(err) {
			eng1.Shutdown(ctx)
			return
		}
		eng1.Start(ctx, k)
		keys = append(keys, k)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if eng1.BreakerTripped("work") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !assert.True(eng1.BreakerTripped("work")) {
		eng1.Shutdown(ctx)
		return
	}

	// Stop the first engine. The parked backlog remains in the database.
	if !assert.NoError(eng1.Shutdown(ctx)) {
		return
	}

	// Downstream recovers, then a fresh engine boots on the same database.
	broken.Store(false)
	eng2 := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithDSN(dsn).
		WithWorkers(4)
	if !assert.NoError(eng2.Startup(ctx)) {
		return
	}
	t.Cleanup(func() { eng2.Shutdown(ctx) })

	// The reconstituted breaker must probe, succeed, unpark, and drain every flow.
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, k := range keys {
		outcome, err := eng2.Await(timeoutCtx, k)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
}
