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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestForkConcurrentflow pins Fork under concurrent writers. Fork's transaction reads the origin before it writes
// the clone; on SQLite a transaction that reads first must upgrade its lock at its first write, and against a
// concurrent writer that upgrade fails at once on every retry - SQLITE_BUSY, or on this suite's shared-cache
// in-memory database SQLITE_LOCKED ("database table is locked"). Fork therefore takes a write first.
// Every Fork here must succeed while other flows keep writing. A read-first Fork fails here only now and then;
// engine's TestFork_TransactionOpensWithAWrite pins the opening write every time.
func TestForkConcurrentflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("ForkConcurrent")
	g.SetURL("A", "forkconcurrent.verify:428/step")
	g.SetURL("B", "forkconcurrent.verify:428/step")
	g.SetURL("C", "forkconcurrent.verify:428/step")
	g.AddTransitionChain("A", "B", "C", workflow.End)
	mux.HandleGraph("forkconcurrent.verify:428/g", g)
	mux.HandleTask("forkconcurrent.verify:428/step", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("n", f.GetInt("n")+1)
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	// Fork sources: completed flows, each forked at its middle step.
	const sources = 8
	var forkAt []string
	for range sources {
		key, out, err := eng.Run(ctx, "forkconcurrent.verify:428/g", nil, nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
			return
		}
		history, err := eng.History(ctx, key)
		if !assert.NoError(err) || !assert.Len(history, 3) {
			return
		}
		forkAt = append(forkAt, history[1].StepKey)
	}

	var stop atomic.Bool
	var writers sync.WaitGroup
	for range 8 {
		writers.Go(func() {
			for !stop.Load() {
				eng.Run(ctx, "forkconcurrent.verify:428/g", nil, nil)
			}
		})
	}
	var forks sync.WaitGroup
	var failed atomic.Int32
	for _, stepKey := range forkAt {
		forks.Go(func() {
			for range 5 {
				forkKey, err := eng.Fork(ctx, stepKey, nil)
				if err != nil {
					failed.Add(1)
					t.Logf("fork: %v", err)
					continue
				}
				out, err := eng.Await(ctx, forkKey)
				if err != nil || out.Status != workflow.StatusCompleted {
					failed.Add(1)
				}
			}
		})
	}
	forks.Wait()
	stop.Store(true)
	writers.Wait()
	assert.Zero(failed.Load(), "every fork under concurrent writers must succeed")
}
