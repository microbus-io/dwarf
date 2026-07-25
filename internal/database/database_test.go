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
	"context"
	"testing"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestResolveShardDSN_ProductionDSNIsVerbatim pins that a production DSN is never formatted or rewritten.
// It used to go through fmt.Sprintf, which interprets EVERY '%' as a verb - so a percent-encoded
// credential (a password "p@ss" written "p%40ss", entirely routine) was silently corrupted: %40s parsed as
// "width 40, verb s", swallowed the shard index there, and left any real %d with no argument
// (%!d(MISSING)). The driver then failed with an opaque parse error naming neither the shard nor the cause.
func TestResolveShardDSN_ProductionDSNIsVerbatim(t *testing.T) {
	assert := testarossa.For(t)

	prod := Config{} // no TestID: production

	// The password p@ss, percent-encoded. Must survive byte for byte.
	dsn := "postgres://user:p%40ss@host:5432/dwarf?sslmode=disable"
	assert.Equal(dsn, resolveShardDSN(prod, 1, dsn))

	// Other percent-encodings a DSN routinely carries (/ : +), and a literal %d, are all left alone: a
	// production DSN is exact, and shards are declared one by one with their own.
	for _, d := range []string{
		"postgres://user:pa%2Fss@host/db",
		"mysql://user:p%2Bss@host/db?params=a%3Ab",
		"postgres://user:pass@host/dwarf_%d?sslmode=disable",
	} {
		assert.Equal(d, resolveShardDSN(prod, 3, d), "production DSN must be verbatim")
	}
}

// TestResolveShardDSN_TestModeSubstitutesShardIndex pins the other half: in test mode the DSN IS a
// template - one base shared across shards, where %d is what gives each shard its own isolated database
// (the per-shard isolation the fixtures rely on). The substitution is a literal replace, so it still
// cannot interpret any other '%' sequence.
func TestResolveShardDSN_TestModeSubstitutesShardIndex(t *testing.T) {
	assert := testarossa.For(t)

	// Hermetic against the env: the empty-DSN case below falls back to SEQUEL_TESTING_DSN before the SQLite
	// default, so without this the test fails whenever the suite is pointed at a real server - which is the
	// documented way to run it. Not t.Parallel, which t.Setenv forbids.
	t.Setenv("SEQUEL_TESTING_DSN", "")

	test := Config{TestID: "abc123"}

	assert.Equal("file:dwarf_2?mode=memory&cache=shared", resolveShardDSN(test, 2, ""))
	assert.Equal("file:xrepl7?mode=memory&cache=shared", resolveShardDSN(test, 7, "file:xrepl%d?mode=memory&cache=shared"))

	// Even in test mode, only %d is substituted - a percent-encoded credential in a testing DSN survives.
	assert.Equal("postgres://user:p%40ss@host/dwarfbench_4",
		resolveShardDSN(test, 4, "postgres://user:p%40ss@host/dwarfbench_%d"))
}

// TestOnEach_ErrorNamesTheFailingShard pins that a shard's failure is attributable. Without the "shard"
// property an operator sees the reaper or the refiller fail with a bare driver error and no clue WHICH
// database is unhealthy - in a package whose contract is that a persistent outage degrades loudly.
//
// Also pins the deterministic choice among concurrent failures: every shard is attempted, and the LOWEST
// failing index is the one reported (the ops run concurrently, so "whichever failed first" would be a race).
func TestOnEach_ErrorNamesTheFailingShard(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	shardOf := func(err error) any {
		var te *errors.TracedError
		if !errors.As(err, &te) {
			return nil
		}
		return te.Properties["shard"]
	}

	// A ShardSet whose op fails on chosen shards; the DBs are never touched by the op below.
	var s ShardSet
	s.indices = []int{2, 5, 9}
	s.dbs = map[int]*sequel.DB{2: nil, 5: nil, 9: nil}

	// One failing shard: it is named.
	err := s.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if shard == 5 {
			return errors.New("boom on 5")
		}
		return nil
	})
	if assert.Error(err) {
		assert.Equal(5, shardOf(err), "the error must name the failing shard")
	}

	// Two failing shards: the lowest index wins, deterministically.
	err = s.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if shard == 5 || shard == 9 {
			return errors.New("boom on %d", shard)
		}
		return nil
	})
	if assert.Error(err) {
		assert.Equal(5, shardOf(err), "among concurrent failures the lowest shard index is reported")
	}

	// No failure: no error (and Trace(nil) must not manufacture one on the single-shard path either).
	assert.NoError(s.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error { return nil }))

	var one ShardSet
	one.indices = []int{7}
	one.dbs = map[int]*sequel.DB{7: nil}
	assert.NoError(one.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error { return nil }))
	err = one.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		return errors.New("boom on the only shard")
	})
	if assert.Error(err) {
		assert.Equal(7, shardOf(err), "the single-shard path must name its shard too")
	}
}
