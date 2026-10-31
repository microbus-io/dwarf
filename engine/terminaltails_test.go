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
	"errors"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestTerminalTails_CohortTailReadsNoRow pins that the final-state read of a failed fan-out reads only its first
// tail's lineage: the state is rebuilt from the cohort, so reading every branch's state and changes - under
// Terminate's or failStep's locks - would be bytes thrown away. A flow whose tail is on the trunk still reads it.
func TestTerminalTails_CohortTailReadsNoRow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	fan := workflow.NewGraph("TailsFan")
	fan.SetURL("Split", "tails/split")
	fan.SetURL("Work", "tails/work")
	fan.SetURL("Join", "tails/join")
	fan.SetFanIn("Join")
	fan.AddTransitionForEach("Split", "Work", "items", "item")
	fan.AddTransitionChain("Work", "Join", workflow.End)
	mux.HandleGraph("tails/fan", fan)
	line := workflow.NewGraph("TailsLine")
	line.SetURL("Split", "tails/split")
	line.AddTransition("Split", workflow.End)
	mux.HandleGraph("tails/line", line)
	nop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	mux.HandleTask("tails/split", nop)
	mux.HandleTask("tails/join", nop)
	mux.HandleTask("tails/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("done", f.GetString("item"))
		if f.GetString("item") == "b" {
			return errors.New("b fails")
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	tailsOf := func(flowKey string) terminalTails {
		shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
		assert.NoError(err)
		db, err := e.db.Shard(shard)
		assert.NoError(err)
		var tails terminalTails
		assert.NoError(readTerminalTails(ctx, db, false, flowID, &tails))
		return tails
	}

	fanKey, out, err := e.Run(ctx, "tails/fan", map[string]any{"items": []string{"a", "b", "c"}}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, out.Status)
	assert.Equal([]string{"a", "b", "c"}, out.State.GetStrings("items"), "the rebuilt final state is intact")
	tails := tailsOf(fanKey)
	assert.True(tails.found)
	assert.NotEqual(0, tails.baseLineage, "the first tail is a cohort member")
	assert.Nil(tails.baseState, "no tail's state was read")
	assert.Len(tails.changes, 0, "no tail's changes were read")

	lineKey, out, err := e.Run(ctx, "tails/line", map[string]any{"x": 1}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	tails = tailsOf(lineKey)
	assert.Equal(0, tails.baseLineage)
	assert.NotNil(tails.baseState, "a trunk tail is read in full")
	assert.Len(tails.changes, 1)
}
