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

/*
Fan-in across replicas. Two engines share one database (a shared in-memory SQLite DSN), each with its
own TestProxy and workers, wired as peers both ways. Every flow fans out A -> {B, C, D} and converges
on a reducer'd fan-in E, so the fan-out siblings are claimed by whichever replica wins each step and the
merge runs on whichever replica completes the cohort last. This pins that the reducer merge and exit-step
scan are correct when a cohort's branches execute on different replicas. Distribution is asserted
deterministically off each step's executing engine id (History.EngineID), not a probabilistic counter.
*/
package fixtures

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestFanInReplicaflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const base = "faninreplica.verify:428"

	// buildProxy registers the same fan-out/fan-in graph and handlers on a proxy, so either replica
	// executes any step identically. Each branch writes only its delta; the reducers fold at fan-in.
	buildProxy := func() *engine.TestProxy {
		p := engine.NewTestProxy()
		g := workflow.NewGraph("FanInReplica")
		g.SetEndpoint("A", base+"/a")
		g.SetEndpoint("B", base+"/b")
		g.SetEndpoint("C", base+"/c")
		g.SetEndpoint("D", base+"/d")
		g.SetEndpoint("E", base+"/e")
		g.SetFanIn("E")
		g.SetReducer("total", workflow.ReducerAdd)
		g.SetReducer("tags", workflow.ReducerAppend)
		g.SetReducer("seen", workflow.ReducerUnion)
		g.AddTransition("A", "B")
		g.AddTransition("A", "C")
		g.AddTransition("A", "D")
		g.AddTransition("B", "E")
		g.AddTransition("C", "E")
		g.AddTransitionChain("D", "E", workflow.END)
		assert.NoError(g.Validate())
		p.HandleGraph(base+"/graph", g)

		p.HandleTask(base+"/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
		p.HandleTask(base+"/b", func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("total", 10)
			f.SetStrings("tags", []string{"b"})
			f.SetStrings("seen", []string{"x"})
			return nil
		})
		p.HandleTask(base+"/c", func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("total", 20)
			f.SetStrings("tags", []string{"c"})
			f.SetStrings("seen", []string{"y", "x"})
			return nil
		})
		p.HandleTask(base+"/d", func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("total", 30)
			f.SetStrings("tags", []string{"d"})
			f.SetStrings("seen", []string{"z"})
			return nil
		})
		p.HandleTask(base+"/e", func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("finalSum", f.GetInt("total"))
			f.SetStrings("finalList", f.GetStrings("tags"))
			f.SetStrings("finalSet", f.GetStrings("seen"))
			return nil
		})
		return p
	}

	proxy1 := buildProxy()
	proxy2 := buildProxy()

	// Both engines share one isolated database via a common test-DB key (each built with
	// NewEngineUnderTest(t), which keys by t.Name()), so pointing SEQUEL_TESTING_DSN at a real server
	// exercises this cross-replica cohort on that dialect's actual row-locking/MVCC; the default is a
	// shared in-memory SQLite database.
	eng1 := engine.NewEngineUnderTest(t)
	eng1.SetHost(proxy1)
	assert.NoError(eng1.SetWorkers(4))
	eng2 := engine.NewEngineUnderTest(t)
	eng2.SetHost(proxy2)
	assert.NoError(eng2.SetWorkers(4))
	proxy1.AddPeer(eng2)
	proxy2.AddPeer(eng1)

	assert.NoError(eng1.Startup(ctx))
	t.Cleanup(func() { eng1.Shutdown(ctx) })
	assert.NoError(eng2.Startup(ctx))
	t.Cleanup(func() { eng2.Shutdown(ctx) })

	const flows = 50
	engines := []*engine.Engine{eng1, eng2}
	keys := make([]string, 0, flows)
	creators := make([]*engine.Engine, 0, flows)
	for i := range flows {
		e := engines[i%2] // alternate which replica's Create is called
		k, err := e.Create(ctx, base+"/graph", nil, nil)
		if !assert.NoError(err) {
			return
		}
		keys = append(keys, k)
		creators = append(creators, e)
	}

	// The engine ids that executed a fan-out sibling (B/C/D), and that executed any step, across all
	// flows. Both sets reaching size 2 proves work distributed across the two replicas - the sibling
	// set specifically proves cohorts fanned out across replicas and still converged correctly.
	siblingEngines := map[int64]bool{}
	anyEngines := map[int64]bool{}
	isSibling := map[string]bool{"B": true, "C": true, "D": true}

	for i, k := range keys {
		awaitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		out, err := creators[i].Await(awaitCtx, k)
		cancel()
		if !assert.NoError(err) {
			continue
		}
		if !assert.Equal(workflow.StatusCompleted, out.Status) {
			continue
		}

		// The fan-in produced the reduced result no matter which replica merged the cohort.
		assert.Equal(60.0, out.State["finalSum"])
		assert.Equal([]string{"b", "c", "d"}, sortedStateStrings(out.State["finalList"]))
		assert.Equal([]string{"x", "y", "z"}, sortedStateStrings(out.State["finalSet"]))

		// Every task ran exactly once; record the replica that executed each step.
		hist, err := creators[i].History(ctx, k)
		if !assert.NoError(err) {
			continue
		}
		counts := map[string]int{}
		for _, s := range hist {
			counts[s.TaskName]++
			assert.Equal(workflow.StatusCompleted, s.Status)
			assert.True(s.EngineID != 0, "executed step %s should carry an engine id", s.TaskName)
			anyEngines[s.EngineID] = true
			if isSibling[s.TaskName] {
				siblingEngines[s.EngineID] = true
			}
		}
		for _, name := range []string{"A", "B", "C", "D", "E"} {
			assert.Equal(1, counts[name], "task %s should appear exactly once in history", name)
		}
	}

	assert.Equal(2, len(anyEngines), "both replicas should have executed steps")
	assert.Equal(2, len(siblingEngines), "fan-out siblings should have executed across both replicas")
}

// sortedStateStrings extracts and sorts a state field that decoded as a []any of strings.
func sortedStateStrings(v any) []string {
	var out []string
	for _, e := range v.([]any) {
		out = append(out, e.(string))
	}
	sort.Strings(out)
	return out
}
