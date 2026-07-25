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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

const (
	// latchResolveChunk caps how many flow ids ride in one IN list. SQL Server's 2,100-parameter ceiling is
	// the tightest of the four dialects and sets this; a chunk well under it also keeps one slow shard's
	// statement from carrying every awaiter on the replica.
	latchResolveChunk = 512
	// latchStatusQuery is completed by the padded placeholder list. It is deliberately UNFILTERED on status
	// - see resolveStoppedFlows for why the filter is applied in Go.
	latchStatusQuery = "SELECT flow_id, flow_token, status FROM dwarf_flows WHERE flow_id IN ("
)

// latchLoop drives the Await latch's detector. One pass asks the shards which parked flows have stopped;
// with nobody awaiting it asks nothing, so an engine whose host never calls Await pays only the ticker.
//
// The sweep is what makes a CROSS-REPLICA stop observable: this replica's own stops reach a waiter through
// notifyStatusChange the instant they commit, but a flow finished by a peer is visible only in the shared
// database. Its cost scales with concurrent AWAITERS, not with step throughput - which is what lets the
// cadence stay tight without tracking how fast the engine is running.
func (e *Engine) latchLoop(ctx context.Context) {
	ticker := time.NewTicker(e.latchSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.latchStop:
			return
		case <-ticker.C:
		}
		// The pass has already dealt with whatever failed - a shard that could not answer is simply asked
		// again next tick - so the error is a log line, never a reason to stop sweeping.
		if r := e.latches.Sweep(ctx); r.Err != nil {
			e.logger.ErrorContext(ctx, "Await latch sweep", "keys", r.Latched, "error", r.Err)
		}
	}
}

// resolveStoppedFlows is the latch board's status resolver: given the flow keys callers are parked on, it
// reports the ones that have stopped. A key it omits stays parked, so every case it cannot answer for -
// a shard that failed, a malformed key, a flow still running - is silently left for the next pass.
//
// Three shapes matter here:
//
//   - ONE QUERY PER SHARD, not per key. A flow key encodes its shard, so the keys are grouped and each
//     shard is asked once (chunked) - the cost of a pass is O(shards), not O(awaiters).
//   - The status filter is applied IN GO, not in the WHERE clause. Binding a status defeats the filtered
//     index on the other dialects, and the rows are already in hand.
//   - A SHARD ERROR IS NOT A FAILED PASS. Whatever the other shards resolved is returned alongside the
//     error, so one sick shard cannot hold up awaiters whose flows live elsewhere.
func (e *Engine) resolveStoppedFlows(ctx context.Context, flowKeys []string) (map[string]string, error) {
	// A parked key, indexed by the shard and row it names. Several keys can name one flow_id (a caller
	// holding a stale or forged token), so the token is carried through and compared against the row -
	// resolving on flow_id alone would answer a caller that does not hold the capability.
	type parked struct {
		key   string
		token string
	}
	byShard := map[int]map[int][]parked{}
	for _, flowKey := range flowKeys {
		shard, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
		if err != nil {
			// Unparseable, so it names no row and no sweep can ever resolve it. The caller's own ctx ends
			// its wait; nothing here can.
			continue
		}
		rows := byShard[shard]
		if rows == nil {
			rows = map[int][]parked{}
			byShard[shard] = rows
		}
		rows[flowID] = append(rows[flowID], parked{key: flowKey, token: flowToken})
	}

	var lock sync.Mutex
	stopped := map[string]string{}
	var failures []error
	// OnEach visits every open shard; the ones holding none of these keys return immediately.
	_ = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		wanted := byShard[shard]
		if len(wanted) == 0 {
			return nil
		}
		ids := make([]int, 0, len(wanted))
		for flowID := range wanted {
			ids = append(ids, flowID)
		}
		slices.Sort(ids) // stable chunk boundaries across passes, for the same reason the board sorts
		for chunk := range slices.Chunk(ids, latchResolveChunk) {
			binds := latchPadBinds(chunk)
			rows, err := db.QueryContext(ctx, latchStatusQuery+strings.Repeat("?,", len(binds)-1)+"?)", binds...)
			if err != nil {
				lock.Lock()
				failures = append(failures, errors.New("shard %d: %w", shard, err))
				lock.Unlock()
				continue
			}
			for rows.Next() {
				var flowID int
				var flowToken, status string
				// TRIM BOTH. status and flow_token are CHAR(16), which Postgres blank-pads on read, so an
				// untrimmed status matches none of the non-stopped constants and every running flow reads as
				// done - releasing its waiter on every sweep, to re-park on the re-read that does trim.
				if err := rows.Scan(&flowID, &flowToken, &status); err != nil {
					lock.Lock()
					failures = append(failures, errors.New("shard %d: %w", shard, err))
					lock.Unlock()
					break
				}
				status = strings.TrimSpace(status)
				flowToken = strings.TrimSpace(flowToken)
				if !isStoppedStatus(status) {
					continue
				}
				lock.Lock()
				for _, p := range wanted[flowID] {
					if p.token == flowToken {
						stopped[p.key] = status
					}
				}
				lock.Unlock()
			}
			if err := rows.Err(); err != nil {
				lock.Lock()
				failures = append(failures, errors.New("shard %d: %w", shard, err))
				lock.Unlock()
			}
			rows.Close()
		}
		// Never propagated: OnEach is all-or-nothing, and one shard's blip must not discard what its peers
		// resolved. The failures slice carries it out instead.
		return nil
	})
	return stopped, errors.Join(failures...)
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
