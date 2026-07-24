/*
Copyright (c) 2026 Microbus LLC and various contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package fixtures

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphInterruptFailflow pins a subgraph child whose internal cohort has one branch that INTERRUPTS and
// another that FAILS. The cohort can never fully arrive while a branch is parked at an interrupt, so the child
// (and the whole tree up to the root) must rest `interrupted`, NOT failed - a failed branch alongside an
// interrupted one does not terminalize. After the root is resumed, the interrupted branch completes, the cohort
// resolves with a failure, the child fails, and that failure is delivered to the parent's flow.Subgraph call.
func TestSubgraphInterruptFailflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Inner child: fan out over items; "wait" interrupts, "bad" fails, others complete.
	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("Seed", "sgif.verify:900/seed")
	inner.SetEndpoint("Work", "sgif.verify:900/work")
	inner.SetEndpoint("Join", "sgif.verify:900/join")
	inner.SetFanIn("Join")
	inner.AddTransitionForEach("Seed", "Work", "items", "item")
	inner.AddTransitionChain("Work", "Join", workflow.END)
	proxy.HandleGraph("sgif.verify:900/inner", inner)
	proxy.HandleTask("sgif.verify:900/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("sgif.verify:900/work", func(ctx context.Context, f *workflow.Flow) error {
		switch f.GetString("item") {
		case "wait":
			if y, _ := f.Interrupt(map[string]any{"need": "approval"}, nil); y {
				return nil
			}
			return nil // resumed
		case "bad":
			return errors.New("inner branch exploded", http.StatusInternalServerError)
		default:
			return nil
		}
	})
	proxy.HandleTask("sgif.verify:900/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	// Parent: RunInner, no onError -> the delivered child failure fails the parent.
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("RunInner", "sgif.verify:900/run-inner")
	parent.AddTransitionChain("RunInner", workflow.END)
	proxy.HandleGraph("sgif.verify:900/parent", parent)
	proxy.HandleTask("sgif.verify:900/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("sgif.verify:900/inner", map[string]any{"items": []string{"ok", "wait", "bad"}}, nil)
		if yield {
			return nil
		}
		return err
	})

	flowKey, err := eng.Create(ctx, "sgif.verify:900/parent", nil, nil)
	assert.NoError(err)

	// The tree must settle at interrupted (not failed), despite the "bad" branch having already failed.
	var out *workflow.FlowOutcome
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		out, err = eng.Snapshot(ctx, flowKey)
		assert.NoError(err)
		if out.Status == workflow.StatusInterrupted {
			break
		}
		if out.Status == workflow.StatusFailed || out.Status == workflow.StatusCompleted {
			t.Fatalf("tree terminalized as %q before resume; must rest interrupted while a branch is parked", out.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(workflow.StatusInterrupted, out.Status, "a failed branch alongside an interrupted one must rest interrupted")

	// Resume the root: the interrupted inner branch completes, the cohort resolves with a failure, the child
	// fails, and the failure is delivered to the parent, which fails.
	err = eng.Resume(ctx, flowKey, map[string]any{"approved": true})
	assert.NoError(err)
	final, err := eng.Await(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, final.Status, "after resume, the cohort's failure must fail the tree")
	assert.True(strings.Contains(final.Error, "inner branch exploded"), "got %q", final.Error)
}
