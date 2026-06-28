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

package fixtures

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestSleepRetryComposeflow pins the engine's Sleep+Retry composition: when a task sets both, the
// re-dispatch delay is Sleep + min(backoff, maxDelay), not the backoff alone. A regression that
// overwrote the Sleep floor with the backoff (the original bug) would drop the Sleep contribution and
// the per-attempt gap would collapse to the backoff only.
//
// The flaky task retries twice with Sleep=200ms and a constant backoff of 200ms (multiplier 1.0), so
// each gap between attempts must be ~400ms. The lower-bound assertion of 350ms proves the two were
// summed: Sleep-only or backoff-only would be ~200ms.
func TestSleepRetryComposeflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const sleep = 200 * time.Millisecond
	const backoff = 200 * time.Millisecond
	const lowerBound = 350.0 // ms; below Sleep+backoff (400) but well above either alone (200)

	graph := workflow.NewGraph("SleepRetryCompose")
	graph.SetEndpoint("Flaky", "sleepretrycomposeflow.verify:428/flaky")
	graph.AddTransitionChain("Flaky", workflow.END)
	commonProxy.HandleGraph("sleepretrycomposeflow.verify:428/compose", graph)

	commonProxy.HandleTask("sleepretrycomposeflow.verify:428/flaky", func(ctx context.Context, f *workflow.Flow) error {
		n := f.GetInt("attempts") + 1
		f.SetInt("attempts", n)
		f.SetFloat(fmt.Sprintf("t%d", n), float64(time.Now().UnixMilli()))
		if n >= 3 {
			return nil
		}
		// Sleep is the floor; Retry adds a constant backoff on top (multiplier 1.0).
		f.Sleep(sleep)
		f.Retry(backoff, 1.0, time.Second, 0)
		return nil
	})

	assert := testarossa.For(t)

	_, outcome, err := commonEngine.Run(ctx, "sleepretrycomposeflow.verify:428/compose", map[string]any{}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	t1, ok1 := outcome.State["t1"].(float64)
	t2, ok2 := outcome.State["t2"].(float64)
	t3, ok3 := outcome.State["t3"].(float64)
	assert.True(ok1 && ok2 && ok3, "expected three attempt timestamps, got %v", outcome.State)

	gap1 := t2 - t1
	gap2 := t3 - t2
	assert.True(gap1 >= lowerBound, "first gap %vms should be >= %vms (Sleep+backoff composed)", gap1, lowerBound)
	assert.True(gap2 >= lowerBound, "second gap %vms should be >= %vms (Sleep+backoff composed)", gap2, lowerBound)
}
