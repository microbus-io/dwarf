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
)

// TestFork_KeptChildKeepsItsOutcome pins that a subgraph child Fork keeps closed - off the rewind chain - keeps the
// outcome frozen with its status. The root calls a child that completes and a child that fails (the caller catches
// the error), then Done; the fork at Done clones both children as they were. Nothing recomputes a closed flow's
// outcome, so the clone must copy it: uncopied, Snapshot of the cloned children's keys returned an empty state for
// the completed one and no error for the failed one, though both kept their status.
func TestFork_KeptChildKeepsItsOutcome(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	root := workflow.NewGraph("KeptRoot")
	root.SetURL("Call", "keptchild/call")
	root.SetURL("CallFail", "keptchild/callfail")
	root.SetURL("Done", "keptchild/done")
	root.AddTransitionChain("Call", "CallFail", "Done", workflow.End)
	mux.HandleGraph("keptchild/root", root)
	ok := workflow.NewGraph("KeptOK")
	ok.SetURL("Answer", "keptchild/answer")
	ok.AddTransition("Answer", workflow.End)
	mux.HandleGraph("keptchild/ok", ok)
	bad := workflow.NewGraph("KeptBad")
	bad.SetURL("Boom", "keptchild/boom")
	bad.AddTransition("Boom", workflow.End)
	mux.HandleGraph("keptchild/bad", bad)

	mux.HandleTask("keptchild/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("keptchild/ok", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("keptchild/callfail", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("keptchild/bad", nil, nil)
		if yield {
			return nil
		}
		return f.Set("caught", err != nil)
	})
	mux.HandleTask("keptchild/done", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("keptchild/answer", func(ctx context.Context, f *workflow.Flow) error { return f.Set("answer", 42) })
	mux.HandleTask("keptchild/boom", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("child diagnostic")
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk, out, err := e.Run(ctx, "keptchild/root", nil, nil)
	assert.Must(assert.NoError(err))
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.True(out.State.GetBool("caught"))

	// childOutcomes snapshots the children History reports on a root's Call and CallFail steps.
	childOutcomes := func(rootKey string) (okOut, badOut *workflow.FlowOutcome, doneKey string) {
		steps, err := e.History(ctx, rootKey)
		assert.Must(assert.NoError(err))
		for _, s := range steps {
			switch s.TaskName {
			case "Call":
				okOut, err = e.Snapshot(ctx, s.SubgraphKey)
				assert.NoError(err)
			case "CallFail":
				badOut, err = e.Snapshot(ctx, s.SubgraphKey)
				assert.NoError(err)
			case "Done":
				doneKey = s.StepKey
			}
		}
		if okOut == nil || badOut == nil || doneKey == "" {
			t.Fatal("History is missing a step")
		}
		return okOut, badOut, doneKey
	}
	origOK, origBad, doneKey := childOutcomes(fk)
	assert.Equal(42, origOK.State.GetInt("answer"))
	assert.Equal(workflow.StatusFailed, origBad.Status)
	assert.Contains(origBad.Error, "child diagnostic")

	forkKey, err := e.Fork(ctx, doneKey, nil)
	assert.Must(assert.NoError(err))
	_, err = e.Await(ctx, forkKey)
	assert.NoError(err)

	cloneOK, cloneBad, _ := childOutcomes(forkKey)
	assert.Equal(workflow.StatusCompleted, cloneOK.Status)
	assert.Equal(42, cloneOK.State.GetInt("answer"), "the kept completed child lost its final state")
	assert.Equal(workflow.StatusFailed, cloneBad.Status)
	assert.Equal(origBad.Error, cloneBad.Error, "the kept failed child lost its error")
}
