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

package pipeline

import (
	"context"
	"math"
	"time"

	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/errors"
)

// NoBand is the band reported when nothing is due - on this shard (TallyResult.Band) or anywhere in the
// fleet (SupplyResult.GlobalBand).
const NoBand = math.MaxInt

// BandSource is the Tallier's database side - the one query it makes, read-only.
type BandSource interface {
	// ScanBand reports this shard's minimum due priority band and one tally per fairness key at that
	// band, or NoBand and no tallies when nothing is due here. It returns O(distinct keys) rows, never
	// O(backlog); each tally's Count is expected to be capped at the planning capacity.
	ScanBand(ctx context.Context, shard int) (band int, tallies []planner.Tally, err error)
}

// TallyResult is one tally cycle's outcome, for logging and metrics. Nothing here is a control signal: a
// caller reads it and carries on.
type TallyResult struct {
	// Slept is the pace this cycle waited out before looking. Total is what the cycle itself cost and
	// EXCLUDES it, so the period is Slept+Total.
	Slept    time.Duration
	Tallying time.Duration
	Total    time.Duration

	// Band is this shard's own minimum due band, or NoBand when nothing is due here or the scan failed.
	Band int

	// Err is set when the scan failed. The cycle has already dealt with it; this is for the log line.
	Err error
}

// Tallier publishes what one shard has due, into the planner every shard plans from.
//
// Cycle is NOT safe for concurrent use - one Tallier per shard, driven by one goroutine, is the whole
// intended shape, and the cadence timestamps are unsynchronized on that basis. The cadence setters are the
// deliberate exception and may be called from anywhere.
type Tallier struct {
	pacer

	shard   int
	source  BandSource
	planner *planner.Planner
}

// NewTallier returns a Tallier for one shard, paced at DefaultInterval and DefaultMinGap until told
// otherwise. It is where a wiring mistake is caught, which is why Cycle itself has no error return to spend
// on one.
func NewTallier(shard int, source BandSource, plan *planner.Planner) (*Tallier, error) {
	if shard < 1 {
		return nil, errors.New("shard must be positive, got %d", shard)
	}
	if source == nil {
		return nil, errors.New("source is required")
	}
	if plan == nil {
		return nil, errors.New("planner is required")
	}
	t := &Tallier{shard: shard, source: source, planner: plan}
	t.init()
	return t, nil
}

// Shard is the shard this Tallier reports on.
func (t *Tallier) Shard() int { return t.shard }

// Cycle scans this shard's band and reports it to the planner, sleeping first for whatever the cadence
// still owes. It never returns an error; see the package doc.
//
// A failed scan means this shard could not LOOK, and it must then leave planning: a stale claim on the best
// band makes every peer find no keys of its own there and dispatch nothing. It deliberately does NOT touch
// the candidate cache - which is not this loop's to touch at all - because the failure means "unknown", not
// "nothing is due".
func (t *Tallier) Cycle(ctx context.Context) TallyResult {
	r := TallyResult{Band: NoBand}
	r.Slept = t.wait(ctx)
	// A cancellation during the sleep ends the cycle before it touches anything. The caller's own loop sees
	// the same cancellation and stops; nothing here needs to signal it.
	if err := ctx.Err(); err != nil {
		r.Err = errors.Trace(err)
		return r
	}
	start := t.begin()
	band, tallies, err := t.source.ScanBand(ctx, t.shard)
	r.Tallying = time.Since(start)
	if err != nil {
		t.planner.Clear(t.shard)
		r.Err = errors.New("tallying", err)
	} else {
		r.Band = band
		t.planner.Tally(t.shard, band, tallies)
	}
	r.Total = t.end(start)
	return r
}
