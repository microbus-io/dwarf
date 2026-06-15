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

	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

func TestDatabase_RunInTestCreatesSchema(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine().
		WithHost(noopHost{})
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

func TestDatabase_StartupInTestCreatesSchema(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine().
		WithHost(noopHost{})
	err := e.StartupInTest(context.Background(), t.Name())
	assert.NoError(err)
	defer e.Shutdown(context.Background())

	db, err := e.shard(1)
	assert.NoError(err)
	var count int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dwarf_flows").Scan(&count)
	assert.NoError(err)
	assert.Equal(0, count)
}

func TestDatabase_StartupInTestRequiresHost(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine() // no host
	err := e.StartupInTest(context.Background(), t.Name())
	assert.Error(err)
}

// TestDatabase_StartupInTestSharesByID is the load-bearing property for multi-replica test apps: engines
// that pass the same testID resolve to the same isolated database (so peer replicas in one app see shared
// state), while a different testID is isolated.
func TestDatabase_StartupInTestSharesByID(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	sharedID := t.Name() + "/shared"

	a := NewEngine().WithHost(noopHost{})
	assert.NoError(a.StartupInTest(ctx, sharedID))
	defer a.Shutdown(ctx)
	b := NewEngine().WithHost(noopHost{})
	assert.NoError(b.StartupInTest(ctx, sharedID))
	defer b.Shutdown(ctx)
	c := NewEngine().WithHost(noopHost{})
	assert.NoError(c.StartupInTest(ctx, t.Name()+"/other"))
	defer c.Shutdown(ctx)

	// A writes a probe table; B (same id) sees it, C (different id) does not.
	dbA, err := a.shard(1)
	assert.NoError(err)
	_, err = dbA.ExecContext(ctx, "CREATE TABLE shared_probe (n INT)")
	assert.NoError(err)
	_, err = dbA.ExecContext(ctx, "INSERT INTO shared_probe (n) VALUES (42)")
	assert.NoError(err)

	dbB, err := b.shard(1)
	assert.NoError(err)
	var n int
	err = dbB.QueryRowContext(ctx, "SELECT n FROM shared_probe").Scan(&n)
	assert.NoError(err)
	assert.Equal(42, n)

	dbC, err := c.shard(1)
	assert.NoError(err)
	err = dbC.QueryRowContext(ctx, "SELECT n FROM shared_probe").Scan(&n)
	assert.Error(err) // isolated database: no such table
}

func TestDatabase_ShardOutOfRange(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine().
		WithHost(noopHost{})
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
		WithHost(noopHost{})
	e.RunInTest(t)

	var visited []int
	err := e.eachShard(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		visited = append(visited, shard)
		return nil
	})
	assert.NoError(err)
	assert.Equal([]int{1}, visited)
}
