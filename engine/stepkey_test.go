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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestStepKey_NamesItsFlow pins the step key's flow segment: every step key handed out names the step's own flow,
// (a subgraph child's steps included), and the same step under another flow's id is not found, by Step or by Fork -
// the flow id is part of the address, checked with the token and the hour.
func TestStepKey_NamesItsFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "stepkey/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("stepkey/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("Work", "stepkey/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("stepkey/child", child)
	mux.HandleTask("stepkey/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("stepkey/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("stepkey/work", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(ctx)))

	fk, err := e.Create(ctx, "stepkey/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	history, err := e.History(ctx, fk)
	assert.NoError(err)
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	var check func(h []workflow.FlowStep)
	check = func(h []workflow.FlowStep) {
		for _, s := range h {
			shard, hour, flowID, stepID, token, err := keys.ParseStepKey(s.StepKey)
			if !assert.NoError(err, s.StepKey) {
				continue
			}
			var owner int
			assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id FROM dwarf_steps WHERE step_id=?", stepID).Scan(&owner))
			assert.Equal(owner, flowID, s.StepKey)
			_, err = e.Step(ctx, s.StepKey)
			assert.NoError(err, s.StepKey)
			other := keys.NewStepKey(shard, hour, flowID+1, stepID, token)
			_, err = e.Step(ctx, other)
			assert.Equal(http.StatusNotFound, errors.StatusCode(err), other)
			_, err = e.Fork(context.Background(), other, nil)
			assert.Equal(http.StatusNotFound, errors.StatusCode(err), other)
			check(s.SubHistory)
		}
	}
	check(history)
}
