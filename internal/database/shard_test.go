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

package database

import (
	"strconv"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// TestShard_ResidencyFailsOpen pins the residue class a shard reports: none until one is set, the class set while it
// is valid, and none - every row taken - once it has expired or was set without a class. An ordinal is reported
// only with a class, so a reader cannot filter by one it was told not to use.
func TestShard_ResidencyFailsOpen(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	sh := NewShard(1, nil)

	count, ordinal, ok := sh.Residency()
	assert.False(ok, "none before one is set")
	assert.Equal(0, count)
	assert.Equal(0, ordinal)

	sh.SetResidency(3, 2, true, time.Now().Add(time.Hour))
	count, ordinal, ok = sh.Residency()
	assert.True(ok)
	assert.Equal(3, count)
	assert.Equal(2, ordinal)

	// Every reading that restricts nothing reads alike, so a reader comparing two sees no change between them: a lone
	// replica's dispatcher count moves from 0 to 1 as its dispatch evidence arrives, and a class lapses at its expiry.
	for name, set := range map[string]func(){
		"set without a class":     func() { sh.SetResidency(3, 2, false, time.Now().Add(time.Hour)) },
		"past its expiry":         func() { sh.SetResidency(3, 2, true, time.Now().Add(-time.Millisecond)) },
		"a single dispatcher":     func() { sh.SetResidency(1, 0, true, time.Now().Add(time.Hour)) },
		"no dispatcher":           func() { sh.SetResidency(0, -1, false, time.Now().Add(time.Hour)) },
		"an ordinal out of range": func() { sh.SetResidency(3, 3, true, time.Now().Add(time.Hour)) },
	} {
		set()
		count, ordinal, ok = sh.Residency()
		assert.Equal([3]any{0, 0, false}, [3]any{count, ordinal, ok}, name)
	}
}

// TestShard_MeasuresItsServerWhenItOpens pins the two measurements Open takes of each shard's server: its
// round-trip time and its connection limit, which SQLite does not have.
func TestShard_MeasuresItsServerWhenItOpens(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	var set ShardSet
	err := set.Open(ctx, Config{
		Shards:      map[int]ShardConfig{1: {MaxIdleConns: 2, MaxOpenConns: 4}},
		TestID:      TestID(t.Name()),
		TestConnCap: 4,
	})
	assert.Must(assert.NoError(err))
	defer set.Close()
	sh, err := set.Shard(1)
	assert.Must(assert.NoError(err))

	switch sh.DB().DriverName() {
	case "sqlite":
		assert.Equal(0, sh.MaxConnections(), "SQLite has no connection limit")
	case "pgx":
		assert.True(sh.RTT() > 0, "the round trip is measured")
		var limit string
		assert.NoError(sh.DB().QueryRowContext(ctx, "SHOW max_connections").Scan(&limit))
		assert.Equal(limit, strconv.Itoa(sh.MaxConnections()))
	default:
		assert.True(sh.RTT() > 0, "the round trip is measured")
		assert.True(sh.MaxConnections() > 0, "the server's connection limit is read")
	}
}
