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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/microbus-io/sequel"
)

// waitSampler answers the one question a throughput plateau always raises and no client-side instrument
// can settle: when the database stops going faster, WHAT are its backends stopped ON?
//
// A pool short of the knee, WAL commit serialization, and lock contention are indistinguishable from the
// client - each shows queries getting slower as concurrency rises - so a connection-count sweep cannot
// interpret itself. This says what the backends were stopped on while it happened.
//
// SAMPLED, not accumulated, because pg_stat_activity is an instantaneous view rather than a counter: each
// poll is one observation of what every backend happens to be doing, and the profile is the proportion of
// observations. That makes the numbers a share of backend-time, not a duration - a 40% WALWrite share
// means 40% of observed active backend-time was in that wait, which is the quantity worth comparing
// across arms.
//
// READ IT ONLY ALONGSIDE THE INSTANCE'S CPU UTILIZATION, WHICH IT DOES NOT CONTAIN. This is a
// distribution over WAITING backends, not a utilization figure, and the two diverge exactly when it
// matters. Measured: a 16-vCPU instance at its ceiling reported 77% LWLock:WALWrite against 7%
// CPU:running while the instance was at 80% CPU - because a handful of running backends plus the
// background processes can saturate the cores while seventy others queue, and only the queue is
// visible here. Reading the 7% as "the database has CPU to spare" inverted the diagnosis.
//
// So a WALWrite-dominated profile does NOT by itself mean the commit path is the wall, and a
// CPU:running-dominated one does NOT by itself mean the pool is short. The profile names the QUEUE; what
// the queue is limited by needs the utilization number next to it. Note also that a pool can be
// saturated while this reports far fewer active backends than its size, since a backend idle between
// statements is not `state='active'` and is not counted - 70 active out of 96 was a FULL pool.
//
// Postgres-only. Any other dialect yields a nil sampler, which every method tolerates. Holds one
// dedicated connection and never competes with the engine's pool.
type waitSampler struct {
	db     *sequel.DB
	stop   chan struct{}
	done   chan struct{}
	mu     sync.Mutex
	counts map[string]int64 // "type:event" -> observations, cumulative since start
	polls  int64            // total polls, so an idle backend-free interval stays visible
	active int64            // summed active backends across polls, for the mean below
}

// waitRow is one wait event's share of a window, or the cumulative total behind one. Pct is the share of
// all active-backend observations in the window, which is the comparable number across arms.
type waitRow struct {
	Event       string  `json:"event"`
	Samples     int64   `json:"samples"`
	Pct         float64 `json:"pct"`
	MeanBackend float64 `json:"meanBackends,omitzero"` // set only on the synthetic total row
}

// waitPollInterval is how often the profile is sampled. Fine enough that a 60s window yields ~1,200
// observations per backend (ample for a stable proportion), coarse enough that the query itself - a scan
// of one small in-memory view - is unmeasurable against the load under test.
const waitPollInterval = 50 * time.Millisecond

// startWaitSampler opens the sampler for a Postgres DSN and begins polling, or returns nil for any other
// dialect. A nil return is non-fatal: the run proceeds recording no wait profile.
func startWaitSampler(dsn string) *waitSampler {
	if !strings.HasPrefix(dsn, "postgres://") {
		return nil
	}
	db, err := sequel.Open("", dsn)
	if err != nil {
		return nil
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &waitSampler{
		db:     db,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		counts: map[string]int64{},
	}
	go s.run()
	return s
}

// run polls until stopped. A failed poll is skipped rather than fatal - the profile is statistical, so
// losing observations costs resolution and nothing else.
func (s *waitSampler) run() {
	defer close(s.done)
	ticker := time.NewTicker(waitPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.poll()
		}
	}
}

// poll records one observation of every active backend in this database.
//
// The sampler's own connection is excluded (pg_backend_pid), and so is everything but client backends:
// the question is where the ENGINE's connections spend their time, and the background writer/checkpointer
// keep their own rhythm regardless of load. A backend that is running rather than waiting reports a NULL
// wait_event_type, which is recorded as "CPU" - that bucket is the whole point of the instrument, since
// it is what distinguishes a short pool from a saturated one.
func (s *waitSampler) poll() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx,
		"SELECT COALESCE(wait_event_type, 'CPU') || ':' || COALESCE(wait_event, 'running'), COUNT(*)"+
			" FROM pg_stat_activity"+
			" WHERE state = 'active'"+
			" AND backend_type = 'client backend'"+
			" AND pid <> pg_backend_pid()"+
			" AND datname = current_database()"+
			" GROUP BY 1")
	if err != nil {
		return
	}
	defer rows.Close()
	seen := map[string]int64{}
	var total int64
	for rows.Next() {
		var event string
		var n int64
		if err := rows.Scan(&event, &n); err != nil {
			return
		}
		seen[event] = n
		total += n
	}
	s.mu.Lock()
	for event, n := range seen {
		s.counts[event] += n
	}
	s.polls++
	s.active += total
	s.mu.Unlock()
}

// snapshot copies the cumulative totals, for differencing at a window's edges the same way pgss is.
func (s *waitSampler) snapshot() map[string]int64 {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.counts)+2)
	for k, v := range s.counts {
		out[k] = v
	}
	// Carried in the same map so a window delta needs no second type. The leading space keeps them
	// sorted away from real event names and out of collision range - Postgres has no such event.
	out[" polls"] = s.polls
	out[" active"] = s.active
	return out
}

// waitDelta turns two snapshots into the window's profile, largest share first. The synthetic total row
// carries the mean active backend count, which is the reading that says whether the pool was even busy:
// a WALWrite-dominated profile over a mean of 3 active backends is a different finding from the same
// profile over 90.
func waitDelta(before, after map[string]int64) []waitRow {
	if len(after) == 0 {
		return nil
	}
	polls := after[" polls"] - before[" polls"]
	active := after[" active"] - before[" active"]
	if active <= 0 {
		return nil
	}
	var out []waitRow
	for event, a := range after {
		if strings.HasPrefix(event, " ") {
			continue
		}
		d := a - before[event]
		if d <= 0 {
			continue
		}
		out = append(out, waitRow{
			Event:   event,
			Samples: d,
			Pct:     float64(d) / float64(active) * 100,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Samples > out[j].Samples })
	if len(out) > 20 {
		out = out[:20]
	}
	if polls > 0 {
		out = append(out, waitRow{
			Event:       "ALL:active",
			Samples:     active,
			Pct:         100,
			MeanBackend: float64(active) / float64(polls),
		})
	}
	return out
}

func (s *waitSampler) close() {
	if s == nil {
		return
	}
	close(s.stop)
	<-s.done
	s.db.Close()
}
