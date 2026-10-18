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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestPayloadSize_OversizedChangesFailTheStepOnce pins the check on a task's changes: the step fails at once,
// naming the field, and the task is not run again - neither by the write retries nor by lease recovery.
func TestPayloadSize_OversizedChangesFailTheStepOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs atomic.Int32
	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadSize")
	g.SetURL("A", "ps/changes/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ps/changes/wf", g)
	mux.HandleTask("ps/changes/a", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		f.SetString("small", "x")
		f.SetString("big", strings.Repeat("b", 2000))
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	e.maxPayloadBytes = 1000
	assert.NoError(e.Startup(t.Context()))

	_, outcome, err := e.Run(ctx, "ps/changes/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)
	// The changes check names its payload; the final-state check would catch the same field otherwise.
	assert.Contains(outcome.Error, "changes of task 'A'")
	assert.Contains(outcome.Error, "over the 1000-byte limit")
	assert.Contains(outcome.Error, `"big"`)
	assert.Equal(int32(1), runs.Load(), "the task must run exactly once")
}

// TestPayloadSize_CreateRejectsWithoutInserting pins the check at the door: an oversized initial state or
// baggage is a 413 and no flow is created.
func TestPayloadSize_CreateRejectsWithoutInserting(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadSize")
	g.SetURL("A", "ps/create/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ps/create/wf", g)
	mux.HandleTask("ps/create/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.maxPayloadBytes = 1000
	assert.NoError(e.Startup(t.Context()))

	big := map[string]any{"doc": strings.Repeat("d", 2000)}
	_, err := e.Create(ctx, "ps/create/wf", big, nil)
	assert.Error(err)
	assert.Equal(http.StatusRequestEntityTooLarge, errors.StatusCode(err))
	_, err = e.Create(ctx, "ps/create/wf", nil, &workflow.FlowOptions{Baggage: big})
	assert.Error(err)
	assert.Equal(http.StatusRequestEntityTooLarge, errors.StatusCode(err))

	flows, _, err := e.List(ctx, workflow.Query{})
	assert.NoError(err)
	assert.Len(flows, 0, "a rejected Create must insert nothing")
}

// TestPayloadSize_RefsCarryStateTheFinalStateCannotHold pins what the limit means for a flow. Mid-flow, each
// large field is stored once and carried by a ref, so steps whose combined state is over the limit run fine.
// The final state is flattened, so it holds every field at once: the flow then fails with an empty final state.
func TestPayloadSize_RefsCarryStateTheFinalStateCannotHold(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var lastRan atomic.Bool
	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadSize")
	g.SetURL("A", "ps/final/a")
	g.SetURL("B", "ps/final/b")
	g.SetURL("C", "ps/final/c")
	g.AddTransition("A", "B")
	g.AddTransition("B", "C")
	g.AddTransition("C", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ps/final/wf", g)
	mux.HandleTask("ps/final/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("x", strings.Repeat("x", 2000))
		return nil
	})
	mux.HandleTask("ps/final/b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("y", strings.Repeat("y", 2000))
		return nil
	})
	mux.HandleTask("ps/final/c", func(ctx context.Context, f *workflow.Flow) error {
		// Both fields reach the last step whole, though together they are over the limit.
		if len(f.GetString("x")) == 2000 && len(f.GetString("y")) == 2000 {
			lastRan.Store(true)
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	e.maxPayloadBytes = 3000
	assert.NoError(e.Startup(t.Context()))

	_, outcome, err := e.Run(ctx, "ps/final/wf", nil, nil)
	assert.NoError(err)
	assert.True(lastRan.Load(), "every step must run with the whole state")
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.Contains(outcome.Error, "final state")
	assert.Equal(0, outcome.State.Len(), "a final state too large to store is stored empty")
}
