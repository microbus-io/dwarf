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
	"testing"

	"github.com/microbus-io/testarossa"
)

// TestPoolFor pins the per-shard pool sizing formula against the documented table and edge cases:
// idle = max(2, ceil(workers/shards/workersPerConn)), open = min(idle*2+2, cap), idle clamped to open.
func TestPoolFor(t *testing.T) {
	assert := testarossa.For(t)

	cases := []struct {
		workers, shards, wpc, cap int
		idle, open                int
	}{
		// Documented default table: workers=64, workersPerConn=8, cap=8. 1 shard == today's flat 8/8.
		{64, 1, 8, 8, 8, 8},
		{64, 2, 8, 8, 4, 8},
		{64, 4, 8, 8, 2, 6},
		{64, 8, 8, 8, 2, 6},
		{64, 16, 8, 8, 2, 6},
		// High ceiling: the formula governs and idle*2+2 shows through (no clamp to the worker count - the
		// ceiling is never reached, demand bounds the actual open count).
		{64, 1, 8, 1000, 8, 18},
		{64, 2, 8, 1000, 4, 10},
		{64, 4, 8, 1000, 2, 6},
		// DB-heavy wpc=1: idle=64, open=130. The ceiling stands; in practice only ~workers ever open.
		{64, 1, 1, 1000, 64, 130},
		// A tight ceiling clamps open, and idle is clamped down to open.
		{64, 1, 8, 1, 1, 1},
		{64, 1, 8, 4, 4, 4},
		// A large workersPerConn (DB-light) floors idle at 2.
		{64, 1, 1000, 8, 2, 6},
		// The floor also covers a tiny worker pool.
		{1, 1, 8, 8, 2, 6},
		// Defensive: shards<1, workersPerConn<1, and cap<1 are all clamped to 1, so the cap dominates -> 1/1.
		{8, 0, 0, 0, 1, 1},
	}
	for _, c := range cases {
		idle, open := calcConnPoolSizes(c.workers, c.shards, c.wpc, c.cap)
		assert.Equal(c.idle, idle, "idle for %+v", c)
		assert.Equal(c.open, open, "open for %+v", c)
	}
}

// TestPoolSizing_LiveResize pins that SetWorkersPerConn re-sizes every live shard pool immediately.
func TestPoolSizing_LiveResize(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetNumShards(4))
	e.RunInTest(t)

	// 4 shards, workers=64, workersPerConn=8, cap=8 -> idle=2, open=min(6,8)=6.
	for i := 1; i <= 4; i++ {
		db, err := e.shard(i)
		assert.NoError(err)
		assert.Equal(6, db.DB.Stats().MaxOpenConnections, "shard %d open at wpc=8", i)
	}

	// Lower workersPerConn (DB-heavier) -> larger target, clamped by the cap of 8.
	// idle=ceil(64/4/2)=8, open=min(18,8)=8.
	assert.NoError(e.SetWorkersPerConn(2))
	for i := 1; i <= 4; i++ {
		db, _ := e.shard(i)
		assert.Equal(8, db.DB.Stats().MaxOpenConnections, "shard %d open after SetWorkersPerConn(2)", i)
	}

	assert.Error(e.SetWorkersPerConn(0)) // must be >= 1
	assert.Error(e.SetMaxOpenConns(0))   // must be >= 1
}

// TestPoolSizing_ShardGrowthResizesExisting pins that growing the shard count shrinks the PRE-EXISTING
// shards' pools too (the shard count is the sizing divisor, so each shard's share drops).
func TestPoolSizing_ShardGrowthResizesExisting(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetNumShards(2))
	e.RunInTest(t)

	// 2 shards: idle=ceil(64/2/8)=4, open=min(10,8)=8.
	for i := 1; i <= 2; i++ {
		db, _ := e.shard(i)
		assert.Equal(8, db.DB.Stats().MaxOpenConnections, "shard %d open at 2 shards", i)
	}

	// Grow to 8 shards: idle=2, open=min(6,8)=6 - the original two shards must resize down to 6.
	assert.NoError(e.SetNumShards(8))
	for i := 1; i <= 8; i++ {
		db, err := e.shard(i)
		assert.NoError(err)
		assert.Equal(6, db.DB.Stats().MaxOpenConnections, "shard %d open at 8 shards", i)
	}
}
