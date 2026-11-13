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
	"sync/atomic"
	"time"

	"github.com/microbus-io/sequel"

	"github.com/microbus-io/dwarf/internal/turnstile"
)

// Shard is one open shard: its index, its database, the turns that order access to its connections, this replica's
// residue class among the shard's dispatchers, and what was measured of its server when it opened. A ShardSet makes
// one per shard at Open; it is valid until the set closes.
//
// Every database call takes a turn first and holds it until the call is done with its connection - for a query,
// until its rows are closed; for a transaction, until it commits:
//
//	ctx, pass := sh.WaitTurn(ctx) // or WaitPriorityTurn
//	defer pass.Return()
//	rows, err := sh.DB().QueryContext(ctx, ...)
//
// Never take a turn while holding one on the same shard: with every turn held by a caller waiting for a second,
// nobody proceeds.
type Shard struct {
	index     int
	db        *sequel.DB
	turns     *turnstile.Set
	residency atomic.Pointer[residency]
	// rtt and serverMax are measured when the shard opens, and fixed for its life.
	rtt       time.Duration
	serverMax int
}

// residency is the residue class Residency reports, published as one value so a reader never sees an ordinal
// against the wrong count.
type residency struct {
	count   int
	ordinal int
	ok      bool
	until   time.Time
}

// NewShard returns a shard of index over db that belongs to no ShardSet, for a test: its turns let every caller
// through unordered until SetTurnLimit bounds them, and it has no residue class until one is set.
func NewShard(index int, db *sequel.DB) *Shard {
	return &Shard{index: index, db: db, turns: turnstile.NewSet()}
}

// Index is the shard's index.
func (s *Shard) Index() int {
	return s.index
}

// DB is the shard's database. Take a turn before using it (WaitTurn).
func (s *Shard) DB() *sequel.DB {
	return s.db
}

// RTT is the round-trip time to the shard's server, measured when the shard opened: the least of a few `SELECT 1`s.
// 0 when the measurement failed.
func (s *Shard) RTT() time.Duration {
	return s.rtt
}

// MaxConnections is the connection limit of the shard's server, read when the shard opened. 0 when it has none
// (SQLite) or the read failed.
func (s *Shard) MaxConnections() int {
	return s.serverMax
}

// The two bands. Lower is served first, and STRICTLY: a band is exhausted before the next is looked at, so the
// priority band holds only callers that cannot flood it. Within a band, order is by how long the job asking has
// been at the database.
const (
	bandPriority = 0
	bandCommon   = 10
)

// WaitTurn stamps ctx for a turn on this shard and waits for one. Run the database calls on the returned context,
// and return the pass when they are done with their connection. A context stamped already keeps its job's age, so a
// job that moves from shard to shard is not taken for a newer arrival at each one.
func (s *Shard) WaitTurn(ctx context.Context) (context.Context, Pass) {
	ctx = s.stamp(ctx, bandCommon)
	return ctx, WaitTurn(ctx)
}

// WaitPriorityTurn is WaitTurn in the priority band, served before every common turn. It is for a caller bounded by
// construction - one that turns on a fixed or derived cadence - which therefore cannot starve the common band: a
// caller that can arrive faster than it is served would.
func (s *Shard) WaitPriorityTurn(ctx context.Context) (context.Context, Pass) {
	ctx = s.stamp(ctx, bandPriority)
	return ctx, WaitTurn(ctx)
}

// ContextWithPriority stamps ctx for priority turns on this shard without taking one, for a job that takes several:
// stamp once when the job begins, then take each turn with the package's WaitTurn, so every turn of the job ranks
// at the job's age.
func (s *Shard) ContextWithPriority(ctx context.Context) context.Context {
	return s.stamp(ctx, bandPriority)
}

// stamp stamps ctx for turns on this shard in band.
func (s *Shard) stamp(ctx context.Context, band int) context.Context {
	if s.turns == nil {
		return ctx
	}
	return s.turns.ContextWithPriority(ctx, s.index, band)
}

// TurnLimit is how many turns the shard admits at once: SetShardPool's pool times the turns per connection.
func (s *Shard) TurnLimit() int {
	if s.turns == nil {
		return 0
	}
	if t := s.turns.Turnstile(s.index); t != nil {
		return t.Size()
	}
	return 0
}

// Turns reports the turns free and the callers waiting for one.
func (s *Shard) Turns() (available, waiting int) {
	if s.turns == nil {
		return 0, 0
	}
	if t := s.turns.Turnstile(s.index); t != nil {
		return t.Snapshot()
	}
	return 0, 0
}

// SetTurnLimit sets how many turns the shard admits at once, until the next SetShardPool: for a test that keeps
// every caller waiting.
func (s *Shard) SetTurnLimit(limit int) {
	if s.turns != nil {
		s.turns.Resize(s.index, limit)
	}
}

// Residency is this replica's residue class among the shard's dispatchers: it takes the rows whose id modulo count
// is ordinal. Every reading that restricts nothing is reported alike, as (0, 0, false) - take every row: none set, one
// set without a class or past its expiry, a single dispatcher, an ordinal out of range.
func (s *Shard) Residency() (count, ordinal int, ok bool) {
	r := s.residency.Load()
	if r == nil || !r.ok || r.count < 2 || r.ordinal < 0 || r.ordinal >= r.count || time.Now().After(r.until) {
		return 0, 0, false
	}
	return r.count, r.ordinal, true
}

// SetResidency sets this replica's residue class among the shard's dispatchers, until a time after which Residency
// reports none: the writer's reading of the fleet can no longer be vouched for then, and taking every row only
// overlaps the peers, where a stale class would leave rows nobody takes.
func (s *Shard) SetResidency(count, ordinal int, ok bool, until time.Time) {
	s.residency.Store(&residency{count: count, ordinal: ordinal, ok: ok, until: until})
}

// Pass is one turn on a shard. Return it when the calls it covers are done with their connection. Returning it twice,
// or returning the zero Pass, does nothing.
type Pass struct {
	pass turnstile.Pass
}

// Return hands the turn back.
func (p Pass) Return() {
	p.pass.Return()
}

// WaitTurn waits for a turn on the shard ctx was stamped for, by a Shard's turn or ContextWithPriority, in its band
// and at its job's age. A context stamped for no shard, or for one whose turns have closed, passes at once, unordered.
func WaitTurn(ctx context.Context) Pass {
	return Pass{pass: turnstile.WaitTurn(ctx)}
}

// Away moves the job's age on ctx forward by d, time it spent away from the database on something else, so its
// next turns rank by its time at the database.
func Away(ctx context.Context, d time.Duration) context.Context {
	return turnstile.Away(ctx, d)
}

// HasClaim reports whether ctx was stamped for turns already.
func HasClaim(ctx context.Context) bool {
	_, ok := turnstile.ClaimFrom(ctx)
	return ok
}

// Gate takes a job's first turn on a shard, in the common band, before the job is picked up: a worker blocked on it holds
// no work, which is what keeps a crew that grows when its workers are busy from growing behind full shards.
type Gate struct {
	gate *turnstile.Gate
}

// Acquire stamps ctx for shard and waits for a turn, returning the context the job's later turns
// are taken on and the turn's release. ok is false once the shards' turns have closed: the caller's signal to stop.
func (g *Gate) Acquire(ctx context.Context, shard int) (gated context.Context, release func(), ok bool) {
	return g.gate.Acquire(ctx, shard)
}

// TryAcquire is Acquire without the wait: it takes a turn only if one is free with nobody waiting, and otherwise
// returns at once with acquired false, holding nothing. ok is false once the shards' turns have closed.
func (g *Gate) TryAcquire(ctx context.Context, shard int) (gated context.Context, release func(), acquired, ok bool) {
	return g.gate.TryAcquire(ctx, shard)
}
