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

// TestRefillScan_BoundedPerFairnessKey pins that the refiller's band scan reads at most perKeyLimit rows
// per fairness key, instead of streaming the entire due band. The old scan had no LIMIT: every due row of
// every key crossed the wire and was allocated in Go, only to be discarded down to the cache capacity by
// the weighted pick - so its cost grew with the BACKLOG, and under a deep one (exactly what the refiller
// exists for) it re-read the whole backlog every refillPace (20ms).
//
// The bound is per KEY rather than a plain LIMIT because fairness is the point of the query: a global
// `ORDER BY created_at LIMIT n` would let one key's old backlog fill the window and starve the others.
// This test therefore checks both halves - the cut, and that every key still reaches the batch.
func TestRefillScan_BoundedPerFairnessKey(t *testing.T) {
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
	e := NewEngine()
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetWorkers(0))
	e.RunInTest(t)

	// A deep backlog on two tenants: 40 steps each, far past any per-key limit used below.
	const perTenant = 40
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		for range perTenant {
			_, err := e.Create(ctx, "scan/g", nil, &workflow.FlowOptions{FairnessKey: tenant})
			assert.NoError(err)
		}
	}

	// The scan must cut each key at the limit - not stream the backlog.
	for _, limit := range []int{1, 3, 10} {
		_, rows, err := e.scanPriorityBand(ctx, limit)
		if !assert.NoError(err) {
			return
		}
		perKey := map[string]int{}
		for _, r := range rows {
			perKey[r.key]++
		}
		assert.Equal(2, len(perKey), "both tenants are represented (fairness is not sacrificed to the bound)")
		for key, n := range perKey {
			assert.Equal(limit, n, "key %q is cut at the per-key limit", key)
		}
		assert.Equal(2*limit, len(rows), "the scan reads limit-per-key, not the %d-step backlog", 2*perTenant)
	}

	// And the cut takes the OLDEST steps of each key: the picker dispatches oldest-first within a key, so
	// a bound that kept the newest would starve the head of every queue.
	_, rows, err := e.scanPriorityBand(ctx, 1)
	if !assert.NoError(err) {
		return
	}
	_, all, err := e.scanPriorityBand(ctx, perTenant)
	if !assert.NoError(err) {
		return
	}
	oldest := map[string]int{}
	for _, r := range all {
		if cur, ok := oldest[r.key]; !ok || r.stepID < cur {
			oldest[r.key] = r.stepID
		}
	}
	for _, r := range rows {
		assert.Equal(oldest[r.key], r.stepID, "the single row kept for %q is its oldest step", r.key)
	}
}
