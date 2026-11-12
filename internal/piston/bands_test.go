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

package piston

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/testarossa"
)

// countsByBand runs the scan's counts statement directly and returns each band's count, the overflow at
// maxBand+1.
func (r *rig) countsByBand(t *testing.T, maxBand, capacity int) map[int]int {
	t.Helper()
	rows, err := r.db.QueryContext(context.Background(), bandCountsQuery(r.db.DriverName(), "", maxBand, capacity))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int]int{}
	for rows.Next() {
		var b, n int
		if err := rows.Scan(&b, &n); err != nil {
			t.Fatal(err)
		}
		out[b] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestPiston_OverflowStartsJustPastTheMaximum pins the overflow's two boundaries against real SQL, and that its
// work goes the whole way through a cycle into the cache.
//
// The maximum itself is a band, never overflow: counted there too, its work would be tallied twice, and the
// planner would sum the two into demand that does not exist. The first priority past it is overflow: left out,
// a flow at that priority sits pending forever with no error anywhere. The existing overflow test uses
// priorities far past the maximum, so neither edge was exercised.
//
// It also pins the counts statement's per-band cap by its RESULT. The cap is what bounds the seek on a deep
// band, and dropping it changes no plan - the reach is the same with exact counts - so only the statement's own
// output shows it is gone.
func TestPiston_OverflowStartsJustPastTheMaximum(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t) // capacity 8
	r.p.SetMaxPriority(3)

	a1 := r.insertStep(t, 1, 1, "a", 1)
	a2 := r.insertStep(t, 2, 1, "a", 1)
	edge := r.insertStep(t, 3, 3, "edge", 1) // at the maximum
	next := r.insertStep(t, 4, 4, "next", 1) // the first priority past it
	far := r.insertStep(t, 5, 9, "far", 1)

	assert.Equal(map[int]int{1: 2, 2: 0, 3: 1, 4: 2}, r.countsByBand(t, 3, 8),
		"the maximum counts once, in its own band; the overflow counts 4 and 9 together")

	_, tallies, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	got := map[planner.Slot]int{}
	for _, tl := range tallies {
		got[planner.Slot{Band: tl.Band, Key: tl.Key}] += tl.Count
	}
	assert.Equal(map[planner.Slot]int{
		{Band: 1, Key: "a"}:    2,
		{Band: 3, Key: "edge"}: 1,
		{Band: 4, Key: "next"}: 1,
		{Band: 9, Key: "far"}:  1,
	}, got, "one tally per key per band, the overflow at its actual priorities")
	assert.Equal(4, len(tallies), "no (band, key) is reported twice")

	// Through a whole cycle: the fetch matches the overflow priorities exactly, and the push stamps them.
	tr, sr := r.p.Cycle(ctx)
	assert.NoError(tr.Err)
	assert.NoError(sr.Err)
	var ids, bands []int
	for r.cache.Len() > 0 {
		j, ok := r.cache.Pop()
		if !ok {
			break
		}
		ids = append(ids, j.StepID)
		bands = append(bands, j.Priority)
	}
	assert.Equal([]int{a1, a2, edge, next, far}, ids, "the range first, then the overflow in its own order")
	assert.Equal([]int{1, 1, 3, 4, 9}, bands)

	// A deep band is counted only up to the capacity: the LIMIT inside each branch is what stops the seek.
	for i := range 10 {
		r.insertStep(t, 10+i, 1, "a", 1)
	}
	assert.Equal(8, r.countsByBand(t, 3, 8)[1], "a band's count stops at the capacity, not at the 12 present")
}

// TestPiston_ReachIsCountedInThisReplicasOwnClass pins that a partitioned replica's reach comes from the bands
// its own residue class holds, and that a peer's better band enters it only past the steal grace.
//
// The band-1 work below is all in the peer's class and fills the cache on its own. Counted unpartitioned, it
// would stop this replica's counts at band 1, its tally there would find nothing of its own, and the replica
// would idle while its own band-7 work waited - the peer is serving band 1 from its own plan. TestPiston_
// PartitionNarrowsTheBands holds a single band-1 step, which never fills the counts, so it cannot see this.
//
// Once the peer's band-1 work has been due past the grace, the peer is evidently not reaching it, and it fills
// this replica's counts ahead of its own worse band: cross-replica priority is strict within the grace.
func TestPiston_ReachIsCountedInThisReplicasOwnClass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t) // capacity 8
	// FetchSteps and ScanBand are driven directly, so the cadence feeds nothing but the steal grace. Until the
	// last scan it is an hour, so the grace (4h) admits nothing foreign whatever the scans cost: a 2s grace was
	// lost to the first scan's compile on a loaded SQL Server.
	r.p.SetTallyCadence(time.Hour, 0)
	r.p.SetSupplyCadence(time.Hour, 0)
	r.p.SetStealAfter(4)

	// Sixteen at band 1, then the odd ids dropped: eight remain, all in ordinal 0's class.
	for i := range 16 {
		r.insertStep(t, 1+i, 1, "urgent", 1)
	}
	_, err := r.db.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE step_id % 2 = 1")
	assert.NoError(err)
	ownBulk := 0
	for i := range 4 {
		if r.insertStep(t, 100+i, 7, "bulk", 1)%2 == 1 {
			ownBulk++
		}
	}
	assert.True(ownBulk > 0, "the fixture needs band-7 work in ordinal 1's class")
	r.setResidency(2, 1, true)
	band, tallies, err := r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	assert.Equal(7, band, "ordinal 1 holds nothing at band 1, so its reach is its own band 7")
	if assert.Equal(1, len(tallies)) {
		assert.Equal(planner.Tally{Band: 7, Key: "bulk", Count: ownBulk}, planner.Tally{Band: tallies[0].Band, Key: tallies[0].Key, Count: tallies[0].Count})
	}

	r.setResidency(2, 0, true)
	band, tallies, err = r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	assert.Equal(1, band)
	for _, tl := range tallies {
		assert.Equal(1, tl.Band, "ordinal 0's band 1 fills the cache, so its band 7 is past the reach")
	}

	// The peer's band 1 has now been due past the grace, 2s from here: it enters ordinal 1's counts ahead of its
	// own band 7. Database clock, never a bound Go time.
	r.p.SetTallyCadence(500*time.Millisecond, 0)
	r.p.SetSupplyCadence(500*time.Millisecond, 0)
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -5000) WHERE priority=1")
	assert.NoError(err)
	r.setResidency(2, 1, true)
	band, tallies, err = r.p.ScanBand(ctx, 1)
	assert.NoError(err)
	assert.Equal(1, band)
	for _, tl := range tallies {
		assert.Equal(1, tl.Band, "a long-due band-1 step outranks this replica's own band 7")
	}
}

// TestPiston_DispatchLagReadsTheBandAtTheReach pins the gauge's boundary within one shard holding two bands: the
// band the fill ran out at is served, so its lag is read; a band past it is held back by strict priority, so its
// lag is not. The existing lag test only moves a whole shard past the reach, so the reach never equals a band the
// reading holds, and reading `>=` for `>` passes it.
func TestPiston_DispatchLagReadsTheBandAtTheReach(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t) // capacity 8

	r.insertStep(t, 1, 1, "hi", 1) // freshly due
	r.insertStep(t, 2, 5, "lo", 1)
	r.insertStep(t, 3, 5, "lo", 1)
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), -30000) WHERE priority=5")
	assert.NoError(err)
	assert.NoError(r.p.TallyCycle(ctx).Err) // this shard's own tally is in the planner: band 1 x1, band 5 x2

	// A peer's band 1 brings it to 7 of 8, so the fill runs out of capacity at band 5: band 5 is being served.
	r.planner.Tally(2, []planner.Tally{{Band: 1, Key: "peer", Weight: 1, Count: 6}})
	assert.Equal(5, r.planner.Reach(r.cache.Capacity()))
	lag, ok := r.p.DispatchLag()
	assert.True(ok)
	assert.True(lag >= 29*time.Second, "the band at the reach is served, so its lag counts: got %v", lag)

	// One more at the peer's band 1 fills the cache there: band 5 is held back, and only band 1's lag is read.
	r.planner.Tally(2, []planner.Tally{{Band: 1, Key: "peer", Weight: 1, Count: 7}})
	assert.Equal(1, r.planner.Reach(r.cache.Capacity()))
	lag, ok = r.p.DispatchLag()
	assert.True(ok)
	assert.True(lag < 29*time.Second, "a band past the reach is held back, not lagging: got %v", lag)
}

// TestPiston_SpillCrossesShards drives two pistons over two real databases sharing one planner and one cache, the
// shape an engine with two shards has. A better band on one shard that cannot fill the cache leaves the rest to a
// worse band on the other, in one plan; a better band on the other that does fill it holds the first shard's
// worse band back, though the first shard's own scan reaches it. Each piston only ever sees its own database, so
// this is the one place below a fixture where the spill is decided across shards from real scans.
func TestPiston_SpillCrossesShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var set database.ShardSet
	err := set.Open(ctx, database.Config{
		Shards: map[int]database.ShardConfig{
			1: {MaxIdleConns: 2, MaxOpenConns: 4},
			2: {MaxIdleConns: 2, MaxOpenConns: 4},
		},
		TestID:      database.TestID(t.Name()),
		TestConnCap: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	cache := &candidates.Cache{}
	cache.Init(4) // capacity 8
	defer cache.Close()
	pl := planner.New()
	shard := map[int]*rig{}
	for _, i := range []int{1, 2} {
		sh, err := set.Shard(i)
		if err != nil {
			t.Fatal(err)
		}
		db := sh.DB()
		p, err := New(sh, pl, cache)
		if err != nil {
			t.Fatal(err)
		}
		p.SetTallyCadence(0, 0)
		p.SetSupplyCadence(0, 0)
		shard[i] = &rig{p: p, sh: sh, db: db, planner: pl, cache: cache}
	}
	// Both tally before either plans, so each plan sees both shards' fresh reports.
	cycle := func() (selected map[int]int) {
		selected = map[int]int{}
		for _, i := range []int{1, 2} {
			assert.NoError(shard[i].p.TallyCycle(ctx).Err)
		}
		for _, i := range []int{1, 2} {
			sr := shard[i].p.SupplyCycle(ctx)
			assert.NoError(sr.Err)
			assert.True(sr.Reconciled)
			selected[i] = sr.Selected
		}
		return selected
	}
	popAll := func() (order []string) {
		for cache.Len() > 0 {
			j, ok := cache.Pop()
			if !ok {
				break
			}
			order = append(order, fmt.Sprintf("s%d@%d", j.Shard, j.Priority))
		}
		return order
	}
	insert := func(i, band, n int) {
		for k := range n {
			shard[i].insertStep(t, 100*band+k, band, "k", 1)
		}
	}
	clear := func() {
		for _, i := range []int{1, 2} {
			_, err := shard[i].db.ExecContext(ctx, "DELETE FROM dwarf_steps")
			assert.NoError(err)
		}
	}

	// Shard 1's band 1 leaves five slots, and shard 2's band 5 takes exactly those.
	insert(1, 1, 3)
	insert(2, 5, 10)
	assert.Equal(map[int]int{1: 3, 2: 5}, cycle())
	assert.Equal([]string{"s1@1", "s1@1", "s1@1", "s2@5", "s2@5", "s2@5", "s2@5", "s2@5"}, popAll(),
		"the better band first, then the spill from the other shard")

	// Reversed: shard 2's band 1 fills the cache, so shard 1's band 5 - which its own scan reaches - waits.
	clear()
	insert(1, 5, 3)
	insert(2, 1, 8)
	assert.Equal(map[int]int{1: 0, 2: 8}, cycle(), "shard 1 is past the reach and fetches nothing")
	assert.Equal([]string{"s2@1", "s2@1", "s2@1", "s2@1", "s2@1", "s2@1", "s2@1", "s2@1"}, popAll())
}
