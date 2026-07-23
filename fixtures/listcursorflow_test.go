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
Multi-shard List cursor walk. List uses per-shard pagination, not a
cross-shard global order: each shard returns up to ceil(limit/numShards) rows by its own
flow_id DESC and the opaque cursor encodes every shard's smallest-returned flow_id. This pins
that a cursor walk across a 3-shard engine visits exactly the created set once (no duplicates,
no omissions), that a malformed cursor is a 400, and that Query.Shard scopes to a single shard.
The single-shard cursor walk in listflow_test.go does not exercise the per-shard cursor encoding.
*/
package fixtures

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestListCursorflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("ListCursor")
	graph.SetEndpoint("Only", "listcursorflow.verify:428/only")
	graph.AddTransition("Only", workflow.END)
	proxy.HandleGraph("listcursorflow.verify:428/list", graph)
	proxy.HandleTask("listcursorflow.verify:428/only", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	for i := 1; i <= 3; i++ {
		eng.SetShard(engine.ShardSpec{Index: i}) // test mode gives each shard its own in-memory database
	}
	eng.RunInTest(t)

	const total = 25
	created := make(map[string]bool, total)
	for range total {
		flowKey, _, err := eng.Run(ctx, "listcursorflow.verify:428/list", nil, nil)
		testarossa.NoError(t, err)
		created[flowKey] = true
	}

	// shardOf parses the leading segment of a flowKey ({shard}-{id}-{token}).
	shardOf := func(t *testing.T, key string) int {
		t.Helper()
		n, err := strconv.Atoi(strings.SplitN(key, "-", 2)[0])
		testarossa.NoError(t, err)
		return n
	}

	t.Run("cursor_walk_visits_every_key_exactly_once", func(t *testing.T) {
		assert := testarossa.For(t)

		seen := map[string]bool{}
		cursor := ""
		pages := 0
		for {
			flows, next, err := eng.List(ctx, workflow.Query{
				WorkflowURL: "listcursorflow.verify:428/list",
				Limit:       4,
				Cursor:      cursor,
			})
			if !assert.NoError(err) {
				return
			}
			pages++
			for _, fs := range flows {
				assert.False(seen[fs.FlowKey], "flow %s appeared on two pages", fs.FlowKey)
				seen[fs.FlowKey] = true
			}
			// Terminate on an empty page or a cursor that does not advance.
			if next == "" || next == cursor {
				break
			}
			cursor = next
			if !assert.True(pages <= total+3, "pagination did not terminate") {
				return
			}
		}

		// The union of all pages is exactly the created set: no duplicates, no omissions.
		assert.Equal(total, len(seen))
		for fk := range created {
			assert.True(seen[fk], "flow %s missing from paginated results", fk)
		}
	})

	t.Run("query_shard_scopes_to_one_shard", func(t *testing.T) {
		assert := testarossa.For(t)

		// Ground truth: which created keys live on shard 2.
		wantShard2 := map[string]bool{}
		for fk := range created {
			if shardOf(t, fk) == 2 {
				wantShard2[fk] = true
			}
		}

		gotShard2 := map[string]bool{}
		cursor := ""
		for {
			flows, next, err := eng.List(ctx, workflow.Query{
				WorkflowURL: "listcursorflow.verify:428/list",
				Shard:       2,
				Limit:       4,
				Cursor:      cursor,
			})
			if !assert.NoError(err) {
				return
			}
			for _, fs := range flows {
				assert.Equal(2, shardOf(t, fs.FlowKey), "Query.Shard=2 returned a flow from another shard")
				gotShard2[fs.FlowKey] = true
			}
			if next == "" || next == cursor {
				break
			}
			cursor = next
		}

		assert.Equal(len(wantShard2), len(gotShard2))
		for fk := range wantShard2 {
			assert.True(gotShard2[fk], "shard-2 flow %s missing from Query.Shard=2 results", fk)
		}
	})

	t.Run("malformed_cursor_is_400", func(t *testing.T) {
		assert := testarossa.For(t)

		_, _, err := eng.List(ctx, workflow.Query{
			WorkflowURL: "listcursorflow.verify:428/list",
			Cursor:      "not-a-valid-cursor",
		})
		if !assert.Error(err) {
			return
		}
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	})
}
