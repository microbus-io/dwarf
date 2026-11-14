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
	"log/slog"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestFinalState_AMissingSpawnIsLoggedAndTheFlowStops pins what a final state's rebuild does when a cohort member's
// spawn step is gone while its flow is present: the flow still stops, and the error names the missing step rather
// than a missing flow. A fan-out rests interrupted with one branch completed - a tail inside the cohort - and its
// spawn's row is deleted, as only a bug or a hand edit would, before Terminate computes the final state.
func TestFinalState_AMissingSpawnIsLoggedAndTheFlowStops(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("MissingSpawn")
	g.SetURL("Split", "finalstate.verify:0/split")
	g.SetURL("Work", "finalstate.verify:0/work-or-wait")
	g.SetURL("Join", "finalstate.verify:0/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Split", "Work", "items", "item")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("finalstate.verify:0/missing-spawn", g)
	mux.HandleTask("finalstate.verify:0/split", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("finalstate.verify:0/work-or-wait", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("item") == "wait" {
			_, err := f.Interrupt(nil, nil)
			return err
		}
		return nil
	})
	mux.HandleTask("finalstate.verify:0/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	logs := &logCounter{seen: map[string]int{}}
	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.SetLogger(slog.New(logs)))
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, outcome, err := eng.Run(ctx, "finalstate.verify:0/missing-spawn", map[string]any{"items": []string{"done", "wait"}}, nil)
	if !assert.NoError(err) || !assert.NotNil(outcome) {
		return
	}
	assert.Must(assert.Equal(workflow.StatusInterrupted, outcome.Status))

	shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.Must(assert.NoError(err))
	sh, err := eng.db.Shard(shard)
	assert.Must(assert.NoError(err))
	res, err := sh.DB().ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id=? AND task_name='Split'", flowID)
	assert.Must(assert.NoError(err))
	n, _ := res.RowsAffected()
	assert.Must(assert.Equal(int64(1), n))

	assert.NoError(eng.Terminate(ctx, flowKey, "stop"))
	outcome, err = eng.Snapshot(ctx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusTerminated, outcome.Status)
	}
	assert.Equal(1, logs.count(missingSpawnLog), "the missing spawn is logged")
	assert.Equal(0, logs.count("Computing final_state while terminalizing a flow found no row"), "the flow's row was there")
}
