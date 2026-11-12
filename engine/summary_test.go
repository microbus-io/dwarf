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
	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// deepTreeMux serves a three-level tree: the root's Call runs sum/mid as its subgraph, whose Mid runs sum/leaf,
// whose Ask interrupts until resumed.
func deepTreeMux() *hosts.Mux {
	mux := hosts.NewMux()
	root := workflow.NewGraph("Root")
	root.SetURL("Call", "sum/call")
	root.AddTransition("Call", workflow.End)
	mux.HandleGraph("sum/root", root)
	mid := workflow.NewGraph("Mid")
	mid.SetURL("Mid", "sum/mid-task")
	mid.AddTransition("Mid", workflow.End)
	mux.HandleGraph("sum/mid", mid)
	leaf := workflow.NewGraph("Leaf")
	leaf.SetURL("Ask", "sum/ask")
	leaf.AddTransition("Ask", workflow.End)
	mux.HandleGraph("sum/leaf", leaf)
	call := func(url string) func(ctx context.Context, f *workflow.Flow) error {
		return func(ctx context.Context, f *workflow.Flow) error {
			var out map[string]any
			yield, err := f.Subgraph(url, map[string]any{}, &out)
			if yield || err != nil {
				return err
			}
			return f.Set("got", out["answer"])
		}
	}
	mux.HandleTask("sum/call", call("sum/mid"))
	mux.HandleTask("sum/mid-task", call("sum/leaf"))
	mux.HandleTask("sum/ask", func(ctx context.Context, f *workflow.Flow) error {
		var answer map[string]any
		yield, err := f.Interrupt(map[string]any{"q": "?"}, &answer)
		if yield || err != nil {
			return err
		}
		return f.Set("answer", answer["a"])
	})
	return mux
}

// TestSummary_ReadsEachFlowOfATreeAsTheListsDo pins Summary across a three-level tree's life - open, closed but not
// archived, archived - against the same flow's row from ListOpen, ListRecentlyClosed and ListClosed: each is identical,
// Subgraph tells a child from the root, RootFlowKey names the root on every flow, and ParentStepKey is empty on the
// root and resolves through Step to the step that called the child.
func TestSummary_ReadsEachFlowOfATreeAsTheListsDo(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, deepTreeMux(), 30)
	rootKey, err := e.Create(ctx, "sum/root", nil, nil)
	assert.Must(assert.NoError(err))
	enginetest.AwaitFlowStatus(t, e, rootKey, workflow.StatusInterrupted, time.Minute)

	// byURL lists the tree with list and checks every flow's Summary against its row; it returns the rows by URL.
	byURL := func(stage string, list func(workflow.Query) ([]workflow.FlowSummary, error), q workflow.Query) map[string]workflow.FlowSummary {
		t.Helper()
		q.Limit, q.IncludeSubgraphs = 100, true
		rows, err := list(q)
		assert.NoError(err)
		got := map[string]workflow.FlowSummary{}
		for _, row := range rows {
			got[row.WorkflowURL] = row
			s, err := e.Summary(ctx, row.FlowKey)
			if assert.NoError(err, "%s: Summary of %s", stage, row.WorkflowURL) {
				assert.Equal(row, *s, "%s: Summary of %s matches its listed row", stage, row.WorkflowURL)
			}
		}
		assert.Len(got, 3, "%s: the whole tree is listed", stage)
		root, mid, leaf := got["sum/root"], got["sum/mid"], got["sum/leaf"]
		assert.Equal(rootKey, root.FlowKey)
		assert.False(root.Subgraph, "%s: the root", stage)
		assert.Equal("", root.ParentStepKey, "%s: a root has no caller", stage)
		for _, f := range []workflow.FlowSummary{root, mid, leaf} {
			assert.Equal(rootKey, f.RootFlowKey, "%s: %s names the root", stage, f.WorkflowURL)
		}
		for child, caller := range map[string]string{"sum/mid": "Call", "sum/leaf": "Mid"} {
			f := got[child]
			assert.True(f.Subgraph, "%s: %s is a child", stage, child)
			step, err := e.Step(ctx, f.ParentStepKey)
			if assert.NoError(err, "%s: %s's ParentStepKey is a step key", stage, child) {
				assert.Equal(caller, step.TaskName, "%s: %s was called by %s", stage, child, caller)
			}
		}
		return got
	}

	open := byURL("open", func(q workflow.Query) ([]workflow.FlowSummary, error) { return e.ListOpen(ctx, q) }, workflow.Query{})
	for _, f := range open {
		assert.Equal(workflow.StatusInterrupted, f.Status)
	}

	assert.NoError(e.Resume(ctx, rootKey, map[string]any{"a": 42}))
	enginetest.AwaitFlowStatus(t, e, rootKey, workflow.StatusCompleted, time.Minute)
	recent := byURL("closed", func(q workflow.Query) ([]workflow.FlowSummary, error) { return e.ListRecentlyClosed(ctx, q) },
		workflow.Query{})
	for _, f := range recent {
		assert.True(f.ArchivedAt.IsZero(), "not archived yet")
	}

	accessors.ArchiveNow(e, ctx)
	window := workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: time.Now().Add(time.Hour)}
	archived := byURL("archived", func(q workflow.Query) ([]workflow.FlowSummary, error) { return e.ListClosed(ctx, q) }, window)
	for _, f := range archived {
		assert.False(f.ArchivedAt.IsZero(), "archived")
	}

	assert.NoError(e.Delete(ctx, rootKey))
	for url, f := range archived {
		_, err := e.Summary(ctx, f.FlowKey)
		assert.Equal(http.StatusNotFound, errors.StatusCode(err), "%s, deleted from the archive", url)
	}
}

// TestSummary_NotFoundAndMalformed pins Summary's errors: a flow marked for deletion is a 404 although its rows are still
// in the live tables, as are an unknown key and one whose token is wrong; a malformed key is a 400.
func TestSummary_NotFoundAndMalformed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.Must(assert.NoError(err))
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	s, err := e.Summary(ctx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, s.Status)
		assert.Equal(fk, s.RootFlowKey)
	}

	assert.NoError(e.Delete(ctx, fk)) // not archived yet: marked for deletion, the rows remain
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Equal(2, flows, "the marked tree's rows are still in the live tables")
	_, err = e.Summary(ctx, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "a flow marked for deletion")

	wrong := "0"
	if fk[len(fk)-1] == '0' {
		wrong = "1"
	}
	_, err = e.Summary(ctx, fk[:len(fk)-1]+wrong)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "a wrong token")
	_, err = e.Summary(ctx, "1-1-999999-abcdef0123456789")
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "an unknown flow")
	_, err = e.Summary(ctx, "not-a-key")
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "a malformed key")
}
