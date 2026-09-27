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

package peers

// peer is one registry row as of the read that returned it.
//
// Both ages are computed by the DATABASE - NOW_UTC() minus the column - so each is on that shard's own
// clock and comparable to a window with no reference to this process's clock. They are never comparable to
// another shard's timestamps, which is why nothing here spans shards.
type peer struct {
	engineID int64
	// seenAgeMs is how long ago the peer last proved it is alive and holding connections.
	seenAgeMs float64
	// dispatchAgeMs is how long ago the peer last proved it is actually serving this shard. A replica that
	// has never dispatched carries the column's decades-stale default, so this reads as enormous.
	dispatchAgeMs float64
	// zeroWorkers is the peer's own statement that it is configured to run no steps. It keeps the peer out of
	// the ranking and nothing else - it still counts in replicas, and the partition never reads it.
	zeroWorkers bool
}

// windows are the three thresholds one classification applies, in milliseconds to match the ages the
// database computes.
type windows struct {
	fresh     float64
	dispatch  float64
	straggler float64
}

// view is everything one snapshot implies about the fleet on one shard.
type view struct {
	// replicas divides the shard's connection pool: every fresh peer holds connections whether or not it
	// claims work.
	replicas int
	// dispatchers divides the candidate partition - only a replica that demonstrably serves this shard may
	// own a residue class of step ids.
	dispatchers int
	// ordinal is this replica's 0-based position among the dispatchers, or -1 when it is not among them.
	ordinal int
	// candidates is how many of the replicas counted in replicas may dispatch - every one not configured
	// await-only - and rank is this replica's 0-based rendezvous place among them.
	candidates int
	rank       int
	// selfSeen reports whether this replica has a row at all - the trigger for the registration repair.
	// Distinct from having a FRESH one: a row that exists but has aged out is a liveness problem the beat
	// fixes on its own, while a missing row is refreshed by nobody.
	selfSeen bool
	// dead are the ids stale past the straggler age, never including self.
	dead []int64
}

// classify turns one snapshot into the two counts, this replica's ordinal, and the hygiene delete list.
// Pure: every time-dependent input arrives as a database-computed age, so this needs no clock.
//
// Input order is preserved and must be engine_id-ascending (the read's ORDER BY). That ordering is what
// lets every replica derive a DISTINCT ordinal from the same rows with no coordination between them -
// sorting in Go instead would work equally well only until two replicas disagreed about collation.
//
// The two counts treat this replica's own absence in OPPOSITE ways, and both directions are deliberate:
//
//   - SELF IS ALWAYS COUNTED IN replicas, whether its row is missing or merely stale. This process
//     demonstrably exists and holds connections, so excluding it would under-count - and the error
//     directions are not symmetric: under-counting over-sizes every pool derived from the count, which is
//     the direction that collapses a database, while over-counting merely under-connects and stays healthy.
//     A stale own row is also exactly the shape a heartbeat starved of a connection produces, which is the
//     moment when growing pools would be most harmful.
//   - SELF IS NEVER FUDGED INTO dispatchers. That divisor has to agree with what every peer computes from
//     the same table, so a replica whose row is absent must decline to partition (ordinal -1) rather than
//     claim a residue class its peers have already handed to somebody else. Declining costs overlapping
//     selection, which the claim CAS arbitrates; claiming a class nobody else believes is yours strands the
//     work in it.
//
// The candidates and the rank follow replicas, self absence included: a replica that cannot see its own row
// still counts and ranks itself among the peers it can see, unless selfZeroWorkers says it may not dispatch.
// Over-claiming a dispatcher slot over-connects by one share until the row is repaired; under-claiming one
// leaves the shard short of a dispatcher, and at one replica leaves it with none.
func classify(rows []peer, self int64, selfZeroWorkers bool, shard int, w windows) view {
	v := view{ordinal: -1}
	selfFresh := false
	selfScore := rendezvousScore(self, shard)
	for _, p := range rows {
		if p.engineID == self {
			v.selfSeen = true
			selfFresh = p.seenAgeMs <= w.fresh
		}
		if p.seenAgeMs > w.straggler && p.engineID != self {
			// Never self: a replica that deleted its own row is refreshed by nobody afterward, since the beat
			// only ever UPDATEs. Excluding self here makes the fleet-wide wipe unreachable even if every other
			// guard were wrong.
			v.dead = append(v.dead, p.engineID)
		}
		if p.seenAgeMs > w.fresh {
			continue
		}
		v.replicas++
		if !p.zeroWorkers {
			v.candidates++
			if p.engineID != self && outranks(rendezvousScore(p.engineID, shard), p.engineID, selfScore, self) {
				v.rank++
			}
		}
		if p.dispatchAgeMs > w.dispatch {
			continue
		}
		if p.engineID == self {
			v.ordinal = v.dispatchers
		}
		v.dispatchers++
	}
	if !selfFresh {
		v.replicas++
		if !selfZeroWorkers {
			v.candidates++
		}
	}
	return v
}

// rendezvousScore is one replica's highest-random-weight score for one shard. Every replica computes the
// same score for the same pair, so ranking a roster needs no coordination, and a replica's score on one
// shard is independent of every other shard - which is what spreads the top ranks across the fleet rather
// than handing every shard to the same few replicas.
//
// A splitmix64 finalizer over the pair: a fixed function, not a seeded hash, because the value has to agree
// across processes and across releases.
func rendezvousScore(engineID int64, shard int) uint64 {
	z := uint64(engineID) ^ (uint64(shard) * 0x9e3779b97f4a7c15)
	z += 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// outranks reports whether a replica with score a (and id aID) ranks ahead of one with score b (id bID). A
// tie falls to the lower id, so the order is total and every replica agrees on it.
func outranks(a uint64, aID int64, b uint64, bID int64) bool {
	if a != b {
		return a > b
	}
	return aID < bID
}
