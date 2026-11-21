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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// These tests pin the half of the public idempotency example that is the engine's: every attempt at one step
// carries the same StepKey, so a downstream that checks the key and applies its effect atomically applies it once.
// effectLedger stands in for that downstream, its lock for the one local transaction the example commits.

// effectLedger records each step key it has applied an effect for.
type effectLedger struct {
	mu       sync.Mutex
	applied  map[string]bool
	attempts int
	effects  int
	keys     []string
}

// apply applies the effect unless the key already has one, checking and recording under one lock.
func (l *effectLedger) apply(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attempts++
	l.keys = append(l.keys, key)
	if l.applied[key] {
		return
	}
	if l.applied == nil {
		l.applied = map[string]bool{}
	}
	l.applied[key] = true
	l.effects++
}

func (l *effectLedger) counts() (attempts, effects int, keys []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.attempts, l.effects, append([]string(nil), l.keys...)
}

// TestStepKey_ARetryAfterALostResponseKeepsTheKey runs a task whose downstream applied the effect but whose
// response was lost: the task arms flow.Retry, as it would on a timeout, and its second run carries the same key.
func TestStepKey_ARetryAfterALostResponseKeepsTheKey(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var ledger effectLedger
	mux := hosts.NewMux()
	g := workflow.NewGraph("SK-Retry")
	g.SetURL("Charge", "skretry/charge")
	g.AddTransition("Charge", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("skretry/g", g)
	mux.HandleTask("skretry/charge", func(ctx context.Context, f *workflow.Flow) error {
		ledger.apply(f.StepKey())
		if f.Attempt() == 0 {
			if yield, err := f.Retry(time.Millisecond, 1, time.Millisecond, time.Minute); yield || err != nil {
				return err // the effect applied, the response was lost
			}
		}
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetHost(mux))
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	_, outcome, err := eng.Run(runCtx, "skretry/g", nil, nil)
	assert.Must(assert.NoError(err))
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	attempts, effects, keys := ledger.counts()
	assert.Equal(2, attempts)
	assert.Equal(1, effects)
	if assert.Len(keys, 2) {
		assert.NotEqual("", keys[0])
		assert.Equal(keys[0], keys[1])
	}
}

// TestStepKey_AStaleWorkerAndItsReplacementShareTheKey runs a task twice at once: its first dispatch outlives its
// lease while a peer re-runs the step and applies the effect, then the first finishes and applies it with the same
// key.
func TestStepKey_AStaleWorkerAndItsReplacementShareTheKey(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var ledger effectLedger
	var calls atomic.Int64
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	mux := hosts.NewMux()
	g := workflow.NewGraph("SK-Stale")
	g.SetURL("Charge", "skstale/charge")
	g.AddTransition("Charge", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("skstale/g", g)
	mux.HandleTask("skstale/charge", func(ctx context.Context, f *workflow.Flow) error {
		if calls.Add(1) == 1 {
			started <- struct{}{}
			<-release // the stale worker, still running while its replacement applies the effect
		}
		ledger.apply(f.StepKey())
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.setClaimsTrackerEnabled(false)
	assert.NoError(eng.SetWorkers(3))
	assert.NoError(eng.SetHost(mux))
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, outcome, unblock := zombieDispatch(t, eng, "skstale/g", "Charge", &calls, started, release)
	defer unblock() // below the Shutdown defer, so it runs first
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	unblock()
	awaitDispatchesReturned(t, eng, flowKey, "Charge", 2) // the replacement's and the stale worker's

	attempts, effects, keys := ledger.counts()
	assert.Equal(2, attempts)
	assert.Equal(1, effects)
	if assert.Len(keys, 2) {
		assert.NotEqual("", keys[0])
		assert.Equal(keys[0], keys[1])
	}
}
