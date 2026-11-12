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

package enginetest

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// NoopHost is a Host that does nothing - LoadGraph returns no graph and ExecuteTask is a no-op. It
// satisfies the engine's Host interface structurally (the methods reference only workflow types, so this
// package needs no import of engine), and is used by tests that stand an engine up purely to exercise
// wiring/config paths where no task ever runs.
type NoopHost struct{}

func (NoopHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) { return nil, nil }
func (NoopHost) ExecuteTask(ctx context.Context, name string, flow *workflow.Flow) error {
	return nil
}

// WaitUntil polls cond until it returns true or timeout elapses, returning cond's final value. A generic
// timing helper with no engine dependency; the cadence (5ms) is fine-grained enough for test observation
// without busy-spinning. The timeout is a "did it hang" ceiling and stretches under -race like the other
// helpers' ceilings, so pass it unscaled.
func WaitUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout * testTimeoutScale)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// FlowStatus returns the flow's current status column, read by direct SQL on the flow's shard.
func FlowStatus(t *testing.T, e Engine, flowKey string) string {
	t.Helper()
	assert := testarossa.For(t)
	shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	sh, err := accessors.DB(e).Shard(shard)
	assert.NoError(err)
	db := sh.DB()
	var status string
	assert.NoError(db.QueryRowContext(context.Background(), "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status))
	return status
}

// CountRows runs a COUNT(*)-style query on the given shard and returns the scalar count (-1 on a shard error).
func CountRows(t *testing.T, e Engine, shard int, query string, args ...any) int {
	t.Helper()
	assert := testarossa.For(t)
	sh, err := accessors.DB(e).Shard(shard)
	if !assert.NoError(err) {
		return -1
	}
	db := sh.DB()
	var n int
	assert.NoError(db.QueryRowContext(context.Background(), query, args...).Scan(&n))
	return n
}

// Lister is the slice of *engine.Engine's exported surface the list helpers use.
type Lister interface {
	ListOpen(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, error)
	ListClosed(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, error)
}

// Recent returns query with a window over the last day, reaching an hour past now, and a Limit of 1,000, each only where
// the query sets none of its own.
func Recent(query workflow.Query) workflow.Query {
	if query.FromTime.IsZero() {
		query.FromTime = time.Now().Add(-24 * time.Hour)
	}
	if query.ToTime.IsZero() {
		query.ToTime = time.Now().Add(time.Hour)
	}
	if query.Limit == 0 {
		query.Limit = 1000
	}
	return query
}

// ListAll returns the flows Recent(query) matches in every status: the live tables' (accessors.ListLive), then
// ListClosed's, a flow archived between the two calls listed once. ListClosed is asked only for a status that may be
// archived, or none. A test sees in it a flow it just closed, which is in neither public list until it is archived.
func ListAll(ctx context.Context, e Lister, query workflow.Query) ([]workflow.FlowSummary, error) {
	query = Recent(query)
	all, err := accessors.ListLive(e, ctx, query)
	if err != nil {
		return nil, err
	}
	if query.Status != "" && !workflow.IsTerminalStatus(query.Status) {
		return all, nil
	}
	archived, err := e.ListClosed(ctx, query)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(all))
	for _, f := range all {
		seen[f.FlowKey] = true
	}
	for _, f := range archived {
		if !seen[f.FlowKey] {
			all = append(all, f)
		}
	}
	return all, nil
}

// ArchiveNow archives every flow the engine has closed, however recently, and removes the flows scheduled for deletion
// whose time has come, for a test that purges what it just closed: Purge reads only the archive.
func ArchiveNow(ctx context.Context, e Lister) {
	accessors.ArchiveNow(e, ctx)
}
