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
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/latch"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

const (
	// latchResolveChunk caps how many flow ids ride in one IN list. SQL Server's 2,100-parameter ceiling is
	// the tightest of the four dialects and sets this; a chunk well under it also keeps one slow shard's
	// statement from carrying every awaiter on the replica.
	latchResolveChunk = 512
	// flowUnresolved releases a caller parked on a key that names no row. It is NOT a flow status and never
	// reaches one: the woken caller reads the flow and reports the not-found it finds. It only has to be a
	// value no real status collides with, since a release is what carries meaning here, not its content.
	flowUnresolved = "unresolved"
)

// latchBoard is the board a caller waiting on flowKey parks on: the one for the shard the key names, or nil
// when it names no open shard. Each shard has its own board and detector so that a stalled shard holds only
// its own awaiters, never the others' (TestLatch_StalledShardHoldsOnlyItsOwnAwaiters).
func (e *Engine) latchBoard(flowKey string) *latch.Board {
	shard, _, _, _, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return nil
	}
	return e.latches[shard]
}

// latchLoop drives one shard's Await latch detector. One pass asks the shard which parked flows have
// settled; with nobody awaiting it asks nothing, so an engine whose host never calls Await pays only the
// ticker.
//
// The sweep is what makes a CROSS-REPLICA stop observable: this replica's own stops reach a waiter through
// signalStop the instant they commit, but a flow finished by a peer is visible only in the shared
// database. Its cost scales with concurrent AWAITERS, not with step throughput - which is what lets the
// cadence stay tight without tracking how fast the engine is running.
//
// It ends on ctx, which drainRuntime cancels, abandoning a sweep in flight: a sweep only reads, and a chunk it
// could not finish settles nothing.
func (e *Engine) latchLoop(ctx context.Context, shard int, board *latch.Board) {
	ticker := time.NewTicker(e.latchSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// The pass has already dealt with whatever failed - a chunk that could not answer is simply asked
		// again next tick - so the error is a log line, never a reason to stop sweeping.
		if r := board.Sweep(ctx); r.Err != nil && ctx.Err() == nil {
			e.logger.ErrorContext(ctx, "Await latch sweep", "shard", shard, "keys", r.Latched, "error", r.Err)
		}
	}
}

// resolveStoppedFlows is one shard's latch board resolver: given the flow keys callers are parked on there,
// it reports the ones that have SETTLED. A key it omits stays parked, so a chunk that failed to answer and a
// flow that is still running are both simply left for the next pass.
//
// Settled covers two outcomes, and reporting both is what lets a caller park without reading first:
//
//   - The flow reached a stopped status; the key resolves to it.
//   - The key resolves to NO row (deleted, or a wrong/forged id or token). It is reported as
//     flowUnresolved, and the woken caller's own read turns that into the not-found it deserves. Without
//     this, a key that names nothing would hold its caller until the caller's deadline, since a row that
//     does not exist can never change status.
//
// Four shapes matter here:
//
//   - ONE LOOKUP PER PASS, not per key (chunked at latchResolveChunk), so the queries a pass costs do not
//     grow with the awaiters.
//   - A RECENT-STOP PRE-SCAN settles keys without naming them, shrinking the IN list to what it could not
//     answer. Optimization only - it never resolves a key as absent.
//   - The status filter is applied IN GO, not in the WHERE clause. Binding a status defeats the filtered
//     index on the other dialects, and the rows are already in hand.
//   - A FAILED CHUNK IS NOT A FAILED PASS. Whatever the other chunks resolved is returned alongside the
//     error. This is also why "no row came back" may be read as absence only for a chunk that FULLY
//     succeeded - on a failed chunk every row is missing, and calling those keys unresolved would 404
//     existing flows on a blip.
func (e *Engine) resolveStoppedFlows(ctx context.Context, shard int, flowKeys []string) (map[string]string, error) {
	// Nothing is indexed but the key itself: it already carries its flow id and its token
	// (keys.ParseFlowKey), and the token is what makes a row the caller's - two keys can name one flow_id
	// when a caller holds a stale or forged token, and resolving on flow_id alone would answer a caller that
	// does not hold the capability.
	settled := map[string]string{}
	wanted := map[string]bool{}
	for _, flowKey := range flowKeys {
		keyShard, _, _, _, err := keys.ParseFlowKey(flowKey)
		if err != nil || keyShard != shard {
			// Names no row on this shard, so no query can settle it and no waiting can improve it.
			settled[flowKey] = flowUnresolved
			continue
		}
		wanted[flowKey] = true
	}
	if len(wanted) == 0 {
		return settled, nil
	}
	db, err := e.db.Shard(shard)
	if err != nil {
		return settled, errors.Trace(err)
	}
	var failures []error
	fail := func(err error) {
		failures = append(failures, errors.New("shard %d: %w", shard, err))
	}

	// This pass takes a turn on the shard it is about to read, so a background sweep queues for its
	// connections like any other caller instead of taking them from work already under way.
	ctx, doneTurn := e.dbTurn(ctx, shard)
	defer doneTurn()

	// A key parked on a running flow cannot have moved, so most of the lookup's binds below re-ask about
	// rows that did not change. This scan settles the only ones that CAN have - the flows stopped inside
	// the window - while NAMING none of them, and drops each from the lookup. It only ever REMOVES work:
	// it never resolves a key as absent, so a row it misses simply stays in the lookup.
	//
	// TWO SWEEPS IS THE WHOLE REQUIREMENT, and widening it is pure cost: a key stays on the board until
	// some pass settles it, so the window only has to span the gap between CONSECUTIVE scans of this
	// shard - one interval of coverage, one of slack for a pass that ran late. Rows scanned grow
	// linearly with it (stop rate x window) on a scan that already costs by throughput rather than by
	// awaiters, so a wider window buys nothing the next pass would not have caught. There is no clock
	// term to pad against either: updated_at is written with NOW_UTC() and compared against NOW_UTC() on
	// the same shard, so the two cancel exactly.
	//
	// The statuses are inlined - binding one is the filtered-index landmine - and are exactly the set
	// isStoppedStatus admits. The window is bound, so the statement text is fixed.
	recent, err := db.QueryContext(ctx,
		"SELECT flow_id, flow_token, created_hour, status FROM dwarf_flows WHERE status IN ('"+
			workflow.StatusCompleted+"','"+workflow.StatusFailed+"','"+
			workflow.StatusTerminated+"','"+workflow.StatusCancelled+"','"+workflow.StatusInterrupted+
			"') AND updated_at>=DATE_ADD_MILLIS(NOW_UTC(), ?)",
		-2*e.latchSweepInterval.Milliseconds())
	if err != nil {
		fail(err) // recorded, not fatal - the lookup still covers every key this shard holds
	} else {
		for recent.Next() {
			var flowID, flowHour int
			var flowToken, status string
			if err := recent.Scan(&flowID, &flowToken, &flowHour, &status); err != nil {
				fail(err)
				break
			}
			// The row spells its own key, so a parked caller is found by lookup and the token check
			// falls out of it: a forged key matches nothing here and is left to the IN lookup, which
			// owns the absent-or-mismatched verdict.
			flowKey := keys.NewFlowKey(shard, flowHour, flowID, flowToken)
			if !wanted[flowKey] {
				continue // nobody is parked on it, which is most of what a busy shard stops
			}
			delete(wanted, flowKey)
			settled[flowKey] = status
		}
		if err := recent.Err(); err != nil {
			fail(err)
		}
		recent.Close()
	}

	// Sorted for stable chunk boundaries across passes, for the same reason the board sorts.
	parked := slices.Sorted(maps.Keys(wanted))
	for chunk := range slices.Chunk(parked, latchResolveChunk) {
		// Keyed the way the scan above keys its rows - by the key the row itself spells - so the token
		// check is the lookup: a key absent from this map is a row that does not exist OR one whose
		// token is not the caller's, which are the same verdict.
		found := make(map[string]string, len(chunk))
		// Two keys naming one flow repeat its id, which IN treats as the set test it is - the same
		// reason the padding may repeat one.
		ids := make([]int, len(chunk))
		for i, flowKey := range chunk {
			_, _, ids[i], _, _ = keys.ParseFlowKey(flowKey) // parsed once already, so it cannot fail here
		}
		binds := latchPadBinds(ids)
		// Deliberately UNFILTERED on status - the filter is applied in Go, see above.
		rows, err := db.QueryContext(ctx,
			"SELECT flow_id, flow_token, created_hour, status FROM dwarf_flows WHERE flow_id IN ("+
				strings.Repeat("?,", len(binds)-1)+"?)", binds...)
		if err != nil {
			fail(err)
			continue
		}
		complete := true
		for rows.Next() {
			var flowID, flowHour int
			var flowToken, status string
			if err := rows.Scan(&flowID, &flowToken, &flowHour, &status); err != nil {
				fail(err)
				complete = false
				break
			}
			found[keys.NewFlowKey(shard, flowHour, flowID, flowToken)] = status
		}
		if err := rows.Err(); err != nil {
			fail(err)
			complete = false
		}
		rows.Close()
		if !complete {
			continue // a partial result set says nothing about which rows are absent
		}
		for _, flowKey := range chunk {
			status, ok := found[flowKey]
			switch {
			case !ok:
				settled[flowKey] = flowUnresolved
			case isStoppedStatus(status):
				settled[flowKey] = status
			}
		}
	}
	return settled, errors.Join(failures...)
}

// latchPadBinds turns one chunk of flow ids into bind arguments, padded up to the next bucket size by
// repeating the last id.
//
// Arity is part of the statement TEXT, so an unbucketed list produces a distinct prepared statement for
// every awaiter count the replica happens to hold - one hard parse each, on a statement that runs several
// times a second forever. Rounding to these ten sizes bounds the query-text set at ten per shard.
// Repeating an id is free: IN is a set test, so a duplicate matches the same row and returns it once.
func latchPadBinds(ids []int) []any {
	size := len(ids)
	for _, bucket := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, latchResolveChunk} {
		if bucket >= len(ids) {
			size = bucket
			break
		}
	}
	binds := make([]any, 0, size)
	for _, id := range ids {
		binds = append(binds, id)
	}
	for len(binds) < size {
		binds = append(binds, ids[len(ids)-1])
	}
	return binds
}

// isStoppedStatus reports whether a flow has reached a status Await returns on. `interrupted` counts: it
// parks the flow for a human, and a caller blocked on the outcome must be told rather than left waiting
// on a flow that will not move until someone resumes it.
func isStoppedStatus(status string) bool {
	return status != "" &&
		status != workflow.StatusCreated &&
		status != workflow.StatusPending &&
		status != workflow.StatusRunning
}
