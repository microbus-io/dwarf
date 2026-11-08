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

package main

import (
	"context"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// archivedReadReport is what -archived-readers measured: concurrent History calls on archived flows, beside whatever
// load the run itself puts on the database.
type archivedReadReport struct {
	Readers     int     `json:"readers"`
	Reads       int64   `json:"reads"`
	ReadsPerSec float64 `json:"readsPerSec"`
	NotFound    int64   `json:"notFound"` // a flow whose day the retention dropped between listing and reading
	Errors      int64   `json:"errors"`
	P50Ms       float64 `json:"p50Ms"`
	P99Ms       float64 `json:"p99Ms"`
	MaxMs       float64 `json:"maxMs"`
	KeysListed  int     `json:"keysListed"` // the largest pool of archived keys the readers drew from
}

// archivedReadLatencyCap bounds the latencies kept for the percentiles; past it a reservoir sample stands in.
const archivedReadLatencyCap = 200_000

// startArchivedReaders runs n goroutines that read the History of archived flows until stop is called: the read load
// a fleet's callers put on the archive, for whatever a run measures beside it (on PostgreSQL, lock-manager pressure:
// locating an archived flow plans the directory and the day tables with all their indexes). The keys come from
// ListClosed, refreshed every few seconds, so the readers need nothing from the load generator and reach only flows
// that are archived. stop returns the report.
func startArchivedReaders(ctx context.Context, eng *engine.Engine, n int) (stop func() *archivedReadReport) {
	ctx, cancel := context.WithCancel(ctx)
	var (
		mu        sync.Mutex
		keys      []string
		maxKeys   int
		lat       []float64
		seen      int64
		reads     atomic.Int64
		notFound  atomic.Int64
		errCount  atomic.Int64
		wg        sync.WaitGroup
		startedAt = time.Now()
	)
	refresh := func() {
		lctx, lcancel := context.WithTimeout(ctx, 30*time.Second)
		defer lcancel()
		now := time.Now()
		sums, err := eng.ListClosed(lctx, workflow.Query{FromTime: now.Add(-24 * time.Hour), ToTime: now, Limit: 1000})
		if err != nil || len(sums) == 0 {
			return
		}
		fresh := make([]string, 0, len(sums))
		for _, s := range sums {
			fresh = append(fresh, s.FlowKey)
		}
		mu.Lock()
		keys = fresh
		maxKeys = max(maxKeys, len(fresh))
		mu.Unlock()
	}
	wg.Go(func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			refresh()
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	})
	for range n {
		wg.Go(func() {
			for ctx.Err() == nil {
				mu.Lock()
				var key string
				if len(keys) > 0 {
					key = keys[rand.IntN(len(keys))]
				}
				mu.Unlock()
				if key == "" {
					select {
					case <-ctx.Done():
					case <-time.After(200 * time.Millisecond):
					}
					continue
				}
				t := time.Now()
				_, err := eng.History(ctx, key)
				ms := float64(time.Since(t)) / float64(time.Millisecond)
				switch {
				case ctx.Err() != nil:
					return
				case errors.StatusCode(err) == http.StatusNotFound:
					notFound.Add(1)
					continue
				case err != nil:
					errCount.Add(1)
					continue
				}
				reads.Add(1)
				mu.Lock()
				seen++
				if len(lat) < archivedReadLatencyCap {
					lat = append(lat, ms)
				} else if j := rand.Int64N(seen); j < archivedReadLatencyCap {
					lat[j] = ms
				}
				mu.Unlock()
			}
		})
	}
	return func() *archivedReadReport {
		cancel()
		wg.Wait()
		r := &archivedReadReport{Readers: n, Reads: reads.Load(), NotFound: notFound.Load(), Errors: errCount.Load(),
			KeysListed: maxKeys}
		if secs := time.Since(startedAt).Seconds(); secs > 0 {
			r.ReadsPerSec = float64(r.Reads) / secs
		}
		if len(lat) > 0 {
			slices.Sort(lat)
			r.P50Ms = lat[len(lat)/2]
			r.P99Ms = lat[min(len(lat)-1, len(lat)*99/100)]
			r.MaxMs = lat[len(lat)-1]
		}
		return r
	}
}
