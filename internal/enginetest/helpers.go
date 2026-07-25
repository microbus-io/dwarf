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

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// NoopHost is a Host that does nothing - LoadGraph returns no graph, ExecuteTask is a no-op, SignalPeers is a
// no-op. It satisfies the engine's Host interface structurally (the methods reference only workflow types, so
// this package needs no import of engine), and is used by tests that stand an engine up purely to exercise
// wiring/config paths where no task ever runs.
type NoopHost struct{}

func (NoopHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) { return nil, nil }
func (NoopHost) ExecuteTask(ctx context.Context, name string, flow *workflow.Flow) error {
	return nil
}
func (NoopHost) SignalPeers(context.Context, string, []byte) {}

// WaitUntil polls cond until it returns true or timeout elapses, returning cond's final value. A generic
// timing helper with no engine dependency; the cadence (5ms) is fine-grained enough for test observation
// without busy-spinning.
func WaitUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
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
	shard, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	db, err := e.DB().Shard(shard)
	assert.NoError(err)
	var status string
	assert.NoError(db.QueryRowContext(context.Background(), "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status))
	return status
}

// CountRows runs a COUNT(*)-style query on the given shard and returns the scalar count (-1 on a shard error).
func CountRows(t *testing.T, e Engine, shard int, query string, args ...any) int {
	t.Helper()
	assert := testarossa.For(t)
	db, err := e.DB().Shard(shard)
	if !assert.NoError(err) {
		return -1
	}
	var n int
	assert.NoError(db.QueryRowContext(context.Background(), query, args...).Scan(&n))
	return n
}
