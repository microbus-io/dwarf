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
	// joinAgeMs is how long ago the peer's row was created: when it joined this shard's registry. It places
	// the peer in the shared order, oldest first.
	joinAgeMs float64
	// working is the peer's own statement that it has workers and is taking on work. A peer that is not -
	// configured to run no steps, or draining at shutdown - is kept out of the ranking and nothing else: it
	// still counts in replicas, and the partition never reads it.
	working bool
	// draining is the peer's statement that it holds a dispatcher-sized share of the shard's connection
	// budget from OUTSIDE the dispatcher cut: it stopped dispatching - demoted, or withdrawn at shutdown - and
	// is still finishing the steps it claimed.
	draining bool
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
	// await-only - and rank is this replica's 0-based place among them in the shared order.
	candidates int
	rank       int
	// draining is how many OTHER fresh replicas state they are draining a dispatcher-sized share.
	draining int
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
// still counts and ranks itself among the peers it can see, unless selfWorking says it is not working.
// Over-claiming a dispatcher slot over-connects by one share until the row is repaired; under-claiming one
// leaves the shard short of a dispatcher, and at one replica leaves it with none.
func classify(rows []peer, self int64, selfWorking bool, w windows) view {
	v := view{ordinal: -1}
	selfFresh := false
	// A replica that cannot see its own row places itself as the newest, behind every peer it can see.
	selfJoinAge := -1.0
	for _, p := range rows {
		if p.engineID == self {
			selfJoinAge = p.joinAgeMs
		}
	}
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
		if p.draining && p.engineID != self {
			v.draining++
		}
		if p.working {
			v.candidates++
			if p.engineID != self && joinedBefore(p.joinAgeMs, p.engineID, selfJoinAge, self) {
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
		if selfWorking {
			v.candidates++
		}
	}
	return v
}

// joinedBefore reports whether a replica that joined a (ms) ago, with id aID, comes ahead in the shared order of
// one that joined b ago with id bID: the earlier join first, a tie to the lower id, so the order is total and
// every replica reading the same rows agrees on it. Ages are computed by the shard's database in one
// statement, so they compare exactly within a reading and are never compared across shards.
func joinedBefore(a float64, aID int64, b float64, bID int64) bool {
	if a != b {
		return a > b
	}
	return aID < bID
}
