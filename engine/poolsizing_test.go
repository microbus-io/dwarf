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

// TestPoolSizing_ShardPool pins the per-shard pool derivation: the explicit override pins the pool
// exactly; VirtualCPUs derives the open ceiling at the measured knee (~6x CPUs) with a warm idle core;
// unknown CPUs fall back to the measured-safe default (8) rather than a guessed CPU count - the
// over-connection collapse measured on small tiers is why honest ignorance beats a wrong guess.
func TestPoolSizing_ShardPool(t *testing.T) {
	assert := testarossa.For(t)

	cases := []struct {
		vcpus    int
		override int
		idle     int
		open     int
	}{
		{0, 0, 8, 8},    // unknown CPUs, no override: safe default
		{1, 0, 3, 6},    // 1 vCPU: knee at 6
		{2, 0, 6, 12},   // 2 vCPU: knee at 12
		{8, 0, 24, 48},  // 8 vCPU: knee at 48
		{8, 30, 30, 30}, // override wins over derived, pinned exactly
		{0, 5, 5, 5},    // override with unknown CPUs
	}
	for _, c := range cases {
		idle, open := shardPool(ShardSpec{VirtualCPUs: c.vcpus}, c.override)
		assert.Equal(c.idle, idle, "idle for %+v", c)
		assert.Equal(c.open, open, "open for %+v", c)
	}
}

// TestPoolSizing_CapacityWeight pins the placement-weight curve: flat up to 2 vCPUs (the measured
// 1- and 2-vCPU tiers ceiling at the same ~745 steps/s), then ~450 steps/s per vCPU.
func TestPoolSizing_CapacityWeight(t *testing.T) {
	assert := testarossa.For(t)

	assert.Equal(0, capacityWeight(0)) // unknown: resolved by pickShard
	assert.Equal(745, capacityWeight(1))
	assert.Equal(745, capacityWeight(2))
	assert.Equal(1800, capacityWeight(4))
	assert.Equal(3600, capacityWeight(8))
}

// TestPoolSizing_PickShard pins the weighted placement: cordoned shards are never picked, weights
// follow the capacity curve (an 8-vCPU shard drawing ~4.8x a 1-vCPU shard's flows), and a shard with
// unknown CPUs gets the smallest known weight.
func TestPoolSizing_PickShard(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, VirtualCPUs: 8}))
	assert.NoError(e.SetShard(ShardSpec{Index: 3, VirtualCPUs: 4, Cordoned: true}))
	assert.NoError(e.SetShard(ShardSpec{Index: 4})) // unknown CPUs -> smallest known weight (745)

	counts := map[int]int{}
	for range 10000 {
		idx, err := e.pickShard()
		assert.NoError(err)
		counts[idx]++
	}
	assert.Equal(0, counts[3], "cordoned shard must never be picked")
	// Expected proportions: 745 : 3600 : 745 (total 5090). Allow generous slack for randomness.
	assert.True(counts[2] > counts[1]*3, "8-vCPU shard should draw ~4.8x the 1-vCPU shard (got %d vs %d)", counts[2], counts[1])
	assert.True(counts[1] > 800 && counts[4] > 800, "low-weight shards still receive flows (got %d, %d)", counts[1], counts[4])
}

// TestPoolSizing_AllCordoned pins the loud failure: when every shard is cordoned there is nowhere to
// place a new flow, and pickShard errors rather than silently violating the cordon.
func TestPoolSizing_AllCordoned(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetShard(ShardSpec{Index: 1, Cordoned: true}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, Cordoned: true}))
	_, err := e.pickShard()
	assert.Error(err)
}

// TestPoolSizing_LiveOverride pins that SetMaxOpenConns pushes the pinned pool to every live shard
// immediately (the expert/benchmark path).
func TestPoolSizing_LiveOverride(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetHost(noopHost{}))
	for i := 1; i <= 2; i++ {
		assert.NoError(e.SetShard(ShardSpec{Index: i, VirtualCPUs: 1}))
	}
	e.RunInTest(t)

	// Derived from VirtualCPUs=1: open = 6.
	for i := 1; i <= 2; i++ {
		db, err := e.db.Shard(i)
		assert.NoError(err)
		assert.Equal(6, db.DB.Stats().MaxOpenConnections, "shard %d derived pool", i)
	}

	// Live override pins exactly.
	assert.NoError(e.SetMaxOpenConns(11))
	for i := 1; i <= 2; i++ {
		db, _ := e.db.Shard(i)
		assert.Equal(11, db.DB.Stats().MaxOpenConnections, "shard %d after override", i)
	}

	assert.Error(e.SetMaxOpenConns(0)) // must be >= 1
}

// TestPoolSizing_DerivedWorkers pins the derived default worker count: 8x the aggregate connection
// budget across shards (workers are shard-agnostic), floored at the historical 64 so the zero-config
// case is unchanged, and replaced entirely by an explicit SetWorkers.
func TestPoolSizing_DerivedWorkers(t *testing.T) {
	assert := testarossa.For(t)

	// Zero-config: single default shard (pool 8) -> max(64, 8*8) = 64, the historical default.
	e := NewEngine()
	assert.NoError(e.SetHost(noopHost{}))
	e.RunInTest(t)
	assert.Equal(64, int(e.workers.Load()))

	// An 8-vCPU shard (pool 48) + a 2-vCPU shard (pool 12): max(64, 8*60) = 480.
	e2 := NewEngine()
	assert.NoError(e2.SetHost(noopHost{}))
	assert.NoError(e2.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e2.SetShard(ShardSpec{Index: 2, VirtualCPUs: 2}))
	e2.RunInTest(t)
	assert.Equal(480, int(e2.workers.Load()))

	// Explicit SetWorkers pins, regardless of shards.
	e3 := NewEngine()
	assert.NoError(e3.SetHost(noopHost{}))
	assert.NoError(e3.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e3.SetWorkers(2))
	e3.RunInTest(t)
	assert.Equal(2, int(e3.workers.Load()))
}
