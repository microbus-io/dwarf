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
	"github.com/microbus-io/testarossa"
)

// TestRefillScan_BoundedPerFairnessKey pins that the refiller's band scan cost scales with fairness-key
// CARDINALITY, not with the backlog. Phase 1 (scanBandKeys) collapses each key to a single aggregate row
// server-side, so a deep backlog on N tenants returns N rows, not N*capacity. Phase 3 (fetchBandSteps)
// then reads only the selected steps - at most perKey OLDEST per chosen key. The old single-query scan
// cut each key at the cache capacity and streamed up to capacity rows PER KEY across the wire, only to
// discard all but `capacity` of them total - so under a deep backlog with high key cardinality it
// materialized hundreds of thousands of rows every refillPace (20ms).
//
// The fetch keeps every key oldest-first because the picker dispatches oldest-first within a key; a bound
// that kept the newest would starve the head of every queue. This test checks all of it: one aggregate
// row per key (not per step), the per-key fetch cut, and that the fetched steps are the oldest.
func TestRefillScan_BoundedPerFairnessKey(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Scan")
	g.SetEndpoint("A", "scan/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("scan/g", g)
	proxy.HandleTask("scan/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	// SetWorkers(0): nothing dispatches, so every created flow's entry step stays `pending` and the scan
	// sees a real backlog.
	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetWorkers(0))
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// A deep backlog on two tenants: 40 steps each, far past any per-key limit used below.
	const perTenant = 40
	tenants := []string{"tenant-a", "tenant-b"}
	for _, tenant := range tenants {
		for range perTenant {
			_, err := e.Create(ctx, "scan/g", nil, &workflow.FlowOptions{FairnessKey: tenant})
			assert.NoError(err)
		}
	}

	// Phase 1: one aggregate row per key regardless of backlog depth, carrying that key's due count
	// CAPPED at the cache capacity (min(due, capacity)) - the scan stops counting a key past capacity,
	// which is all planBatch can ever consume from one key, so the cap is lossless. SetWorkers(0) makes
	// this fixture's capacity small, so the assertion is the capped value, not the raw backlog depth.
	band, rows, err := e.scanShardBandKeys(ctx, 1)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(2, len(rows), "one aggregate row per tenant, not per step in the %d-step backlog", 2*perTenant)
	counts := map[string]int{}
	for _, r := range rows {
		counts[r.key] = r.count
	}
	capped := min(perTenant, e.cache.Capacity())
	assert.Equal(capped, counts["tenant-a"])
	assert.Equal(capped, counts["tenant-b"])

	// Phase 3: the fetch cuts each key at perKey - not the backlog - and every key still reaches the batch.
	for _, perKey := range []int{1, 3, 10} {
		byKey, err := e.fetchShardBandSteps(ctx, 1, band, tenants, perKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(2, len(byKey), "both tenants are represented (fairness is not sacrificed to the bound)")
		for key, list := range byKey {
			assert.Equal(perKey, len(list), "key %q is cut at perKey", key)
		}
	}

	// And the fetch keeps the OLDEST step of each key: the single row for a key at perKey=1 is its oldest
	// (smallest step_id here, since steps are created in age order).
	one, err := e.fetchShardBandSteps(ctx, 1, band, tenants, 1)
	if !assert.NoError(err) {
		return
	}
	all, err := e.fetchShardBandSteps(ctx, 1, band, tenants, perTenant)
	if !assert.NoError(err) {
		return
	}
	for key, list := range all {
		oldest := list[0].stepID
		for _, fs := range list {
			if fs.stepID < oldest {
				oldest = fs.stepID
			}
		}
		if assert.Equal(1, len(one[key])) {
			assert.Equal(oldest, one[key][0].stepID, "the single fetched row for %q is its oldest step", key)
		}
	}
}
