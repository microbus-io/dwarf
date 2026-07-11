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
	"sync"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// rttSampler continuously measures the database round-trip latency on its own dedicated connection: one
// SELECT 1 every 250ms for the whole run. This is the L term of the sizing model - always measured, never
// inferred from instance placement.
type rttSampler struct {
	db      *sequel.DB
	stop    chan struct{}
	done    sync.WaitGroup
	mu      sync.Mutex
	samples []time.Duration
}

// startRTTSampler opens its own connection to the given DSN and starts sampling.
func startRTTSampler(dsn string) (*rttSampler, error) {
	db, err := sequel.Open("", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// A dedicated single connection: the sampler must never compete with the engine's pool, and RTT
	// must not include pool-wait time.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &rttSampler{
		db:   db,
		stop: make(chan struct{}),
	}
	s.done.Go(s.loop)
	return s, nil
}

func (s *rttSampler) loop() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			start := time.Now()
			var one int
			err := s.db.QueryRowContext(context.Background(), "SELECT 1").Scan(&one)
			if err != nil {
				continue // transient errors are not RTT samples
			}
			d := time.Since(start)
			s.mu.Lock()
			s.samples = append(s.samples, d)
			s.mu.Unlock()
		}
	}
}

// stats returns the RTT percentiles collected so far.
type rttStats struct {
	Samples int     `json:"samples"`
	P50ms   float64 `json:"p50Ms"`
	P95ms   float64 `json:"p95Ms"`
}

func (s *rttSampler) stats() rttStats {
	s.mu.Lock()
	samples := make([]time.Duration, len(s.samples))
	copy(samples, s.samples)
	s.mu.Unlock()
	return rttStats{
		Samples: len(samples),
		P50ms:   percentileMs(samples, 0.50),
		P95ms:   percentileMs(samples, 0.95),
	}
}

func (s *rttSampler) close() {
	close(s.stop)
	s.done.Wait()
	s.db.Close()
}
