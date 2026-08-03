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

package fixtures

import (
	"context"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// oneTaskHost serves a single-node graph for any workflow URL and no-ops the task, for tests that need a
// flow to actually run (unlike enginetest.NoopHost, whose nil graph 404s Create).
type oneTaskHost struct{}

func (oneTaskHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) {
	g := workflow.NewGraph("OneTask")
	g.SetEndpoint("Do", "do")
	g.AddTransition("Do", workflow.END)
	return g, nil
}
func (oneTaskHost) ExecuteTask(ctx context.Context, name string, f *workflow.Flow) error { return nil }

func TestDatabase_TestModeCreatesSchema(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(enginetest.NoopHost{})
	assert.NoError(e.Startup(t.Context()))

	// Verify the schema was created by querying the flows table.
	db, err := e.DB().Shard(1)
	assert.NoError(err)
	var count int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_flows").Scan(&count)
	assert.NoError(err)
	assert.Equal(0, count)

	// Verify steps table exists too.
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_steps").Scan(&count)
	assert.NoError(err)
	assert.Equal(0, count)
}

// TestSetShard covers the construction-time-only shard registry: before Startup SetShard records the
// index->DSN pair (applied when the shards open at Startup), indices may be sparse, duplicates and
// invalid indices are rejected, and on a running engine SetShard is rejected - the set is immutable for
// the engine's life (changing it needs a coordinated restart, since flow keys encode the shard).
func TestSetShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	// Before Startup, SetShard just records the pair (shards open at Startup).
	pre := engine.NewEngine()
	pre.SetHost(enginetest.NoopHost{})
	assert.NoError(pre.SetShard(engine.ShardSpec{Index: 1}))
	assert.Equal(0, pre.DB().NumShards())
	assert.Error(pre.SetShard(engine.ShardSpec{Index: 1}))  // duplicate index
	assert.Error(pre.SetShard(engine.ShardSpec{Index: 0}))  // index 0 is the "no shard / all shards" sentinel
	assert.Error(pre.SetShard(engine.ShardSpec{Index: -1})) // negative index

	// Sparse indices: 2 and 99 need not be contiguous.
	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(oneTaskHost{})
	assert.NoError(e.SetShard(engine.ShardSpec{Index: 2}))
	assert.NoError(e.SetShard(engine.ShardSpec{Index: 99}))
	assert.NoError(e.Startup(t.Context()))
	assert.Equal(2, e.DB().NumShards())
	assert.Equal([]int{2, 99}, e.DB().Indices())

	// Both shards are migrated and usable in isolation; unregistered indices route nowhere.
	for _, n := range []int{2, 99} {
		db, err := e.DB().Shard(n)
		assert.NoError(err)
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows").Scan(&count)
		assert.NoError(err, "shard %d has no schema", n)
		assert.Equal(0, count)
	}
	_, err := e.DB().Shard(1)
	assert.Error(err, "index 1 is not registered")

	// Flows are created on the sparse shards and their keys route back to them.
	flowKey, _, err := e.Run(ctx, "bench", nil, nil)
	assert.NoError(err)
	assert.True(strings.HasPrefix(flowKey, "2-") || strings.HasPrefix(flowKey, "99-"))
	_, err = e.Snapshot(ctx, flowKey)
	assert.NoError(err)

	// On a running engine, SetShard is rejected and the set is unchanged.
	assert.Error(e.SetShard(engine.ShardSpec{Index: 3}))
	assert.Equal(2, e.DB().NumShards())
}

func TestDatabase_ShardOutOfRange(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(enginetest.NoopHost{})
	assert.NoError(e.Startup(t.Context()))

	_, err := e.DB().Shard(0)
	assert.Error(err)
	_, err = e.DB().Shard(2)
	assert.Error(err)
	_, err = e.DB().Shard(1)
	assert.NoError(err)
}

func TestDatabase_EachShardSingleShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(enginetest.NoopHost{})
	assert.NoError(e.Startup(t.Context()))

	var visited []int
	err := e.DB().OnEach(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		visited = append(visited, shard)
		return nil
	})
	assert.NoError(err)
	assert.Equal([]int{1}, visited)
}
