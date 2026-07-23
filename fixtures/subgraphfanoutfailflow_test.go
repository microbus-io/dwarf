/*
Copyright (c) 2026 Microbus LLC and various contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package fixtures

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphFanOutFailflow exercises a dynamic fan-out where EVERY branch spawns a subgraph child and one
// child fails - the interaction of cohort accounting (lineage / cohort_arrivals / cohort_failures) with
// subgraph failure delivery (deliverFlowFailureToParent re-arming a parked branch caller step that is itself a
// cohort member). Both dispositions are pinned: the branch propagating the child error (the cohort escalates
// and the flow fails) and the branch swallowing it (every branch converges and the flow completes).
func TestSubgraphFanOutFailflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Inner child: succeeds for most items, fails for the poisoned one.
	newEngine := func(t *testing.T) (*engine.Engine, *engine.TestProxy) {
		proxy := engine.NewTestProxy()
		eng := engine.NewEngine()
		eng.SetHost(proxy)
		eng.RunInTest(t)
		return eng, proxy
	}

	buildInner := func(proxy *engine.TestProxy) {
		inner := workflow.NewGraph("Inner")
		inner.SetEndpoint("Do", "sgfof.verify:900/do")
		inner.AddTransitionChain("Do", workflow.END)
		proxy.HandleGraph("sgfof.verify:900/inner", inner)
		proxy.HandleTask("sgfof.verify:900/do", func(ctx context.Context, f *workflow.Flow) error {
			if f.GetString("item") == "bad" {
				return errors.New("child exploded for %s", f.GetString("item"), http.StatusInternalServerError)
			}
			f.SetString("out", "ok:"+f.GetString("item"))
			return nil
		})
	}

	t.Run("branch_propagates_child_error_cohort_fails_flow", func(t *testing.T) {
		assert := testarossa.For(t)
		eng, proxy := newEngine(t)
		buildInner(proxy)

		g := workflow.NewGraph("Outer")
		g.SetEndpoint("Split", "sgfof.verify:900/split")
		g.SetEndpoint("Branch", "sgfof.verify:900/branch-prop")
		g.SetEndpoint("Join", "sgfof.verify:900/join")
		g.SetFanIn("Join")
		g.SetReducer("outs", workflow.ReducerAppend)
		g.AddTransitionForEach("Split", "Branch", "items", "item")
		g.AddTransitionChain("Branch", "Join", workflow.END)
		proxy.HandleGraph("sgfof.verify:900/outer", g)
		proxy.HandleTask("sgfof.verify:900/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
		proxy.HandleTask("sgfof.verify:900/branch-prop", func(ctx context.Context, f *workflow.Flow) error {
			var out map[string]any
			yield, err := f.Subgraph("sgfof.verify:900/inner", map[string]any{"item": f.GetString("item")}, &out)
			if yield {
				return nil
			}
			if err != nil {
				return err // propagate: no onError, so the branch fails and the cohort escalates
			}
			f.Set("outs", []string{out["out"].(string)})
			return nil
		})
		proxy.HandleTask("sgfof.verify:900/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

		_, outcome, err := eng.Run(ctx, "sgfof.verify:900/outer",
			map[string]any{"items": []string{"a", "bad", "c"}}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.True(strings.Contains(outcome.Error, "child exploded"), "got %q", outcome.Error)
	})

	t.Run("branch_recovers_child_error_flow_completes", func(t *testing.T) {
		assert := testarossa.For(t)
		eng, proxy := newEngine(t)
		buildInner(proxy)

		g := workflow.NewGraph("Outer2")
		g.SetEndpoint("Split", "sgfof.verify:900/split2")
		g.SetEndpoint("Branch", "sgfof.verify:900/branch-recover")
		g.SetEndpoint("Join", "sgfof.verify:900/join2")
		g.SetFanIn("Join")
		g.SetReducer("outs", workflow.ReducerAppend)
		g.AddTransitionForEach("Split", "Branch", "items", "item")
		g.AddTransitionChain("Branch", "Join", workflow.END)
		proxy.HandleGraph("sgfof.verify:900/outer2", g)
		proxy.HandleTask("sgfof.verify:900/split2", func(ctx context.Context, f *workflow.Flow) error { return nil })
		proxy.HandleTask("sgfof.verify:900/branch-recover", func(ctx context.Context, f *workflow.Flow) error {
			var out map[string]any
			yield, err := f.Subgraph("sgfof.verify:900/inner", map[string]any{"item": f.GetString("item")}, &out)
			if yield {
				return nil
			}
			if err != nil {
				// The branch swallows its child's failure and still converges at the fan-in.
				f.Set("outs", []string{"recovered:" + f.GetString("item")})
				return nil
			}
			f.Set("outs", []string{out["out"].(string)})
			return nil
		})
		proxy.HandleTask("sgfof.verify:900/join2", func(ctx context.Context, f *workflow.Flow) error { return nil })

		_, outcome, err := eng.Run(ctx, "sgfof.verify:900/outer2",
			map[string]any{"items": []string{"a", "bad", "c"}}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		var outs []string
		for _, v := range outcome.State["outs"].([]any) {
			outs = append(outs, v.(string))
		}
		sort.Strings(outs)
		assert.Equal([]string{"ok:a", "ok:c", "recovered:bad"}, outs, "every branch converged, the failed child recovered")
	})
}
