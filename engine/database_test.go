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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

func noopGraphLoader(ctx context.Context, name string, metadata map[string]any) (*workflow.Graph, error) {
	return nil, nil
}

func noopTaskExecutor(ctx context.Context, name string, flow *workflow.Flow, metadata map[string]any) error {
	return nil
}

func TestDatabase_RunInTestCreatesSchema(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine().
		WithGraphLoader(noopGraphLoader).
		WithTaskExecutor(noopTaskExecutor)
	e.RunInTest(t)

	// Verify the schema was created by querying the flows table.
	db, err := e.shard(1)
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

func TestDatabase_ShardOutOfRange(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine().
		WithGraphLoader(noopGraphLoader).
		WithTaskExecutor(noopTaskExecutor)
	e.RunInTest(t)

	_, err := e.shard(0)
	assert.Error(err)
	_, err = e.shard(2)
	assert.Error(err)
	_, err = e.shard(1)
	assert.NoError(err)
}

func TestDatabase_EachShardSingleShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine().
		WithGraphLoader(noopGraphLoader).
		WithTaskExecutor(noopTaskExecutor)
	e.RunInTest(t)

	var visited []int
	err := e.eachShard(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		visited = append(visited, shard)
		return nil
	})
	assert.NoError(err)
	assert.Equal([]int{1}, visited)
}
