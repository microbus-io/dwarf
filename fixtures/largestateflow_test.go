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
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestLargeFinalStateflow pins that a flow's terminal state larger than 64 KB round-trips intact on every
// dialect. Regression guard: `dwarf_flows.final_state` was `TEXT` on MySQL (a 64 KB cap) while `state`/
// `changes` were `JSON`, so a large-output flow that completes fine on PostgreSQL / SQL Server / SQLite would
// silently truncate — or fail to commit under strict mode — on MySQL alone. `final_state` is now `JSON` on
// MySQL too. This fixture runs against whatever `SEQUEL_TESTING_DSN` points at, so on a regressed schema it
// fails on MySQL and passes on the other three — a clean four-dialect tripwire.
func TestLargeFinalStateflow(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	g := workflow.NewGraph("LargeState")
	g.SetEndpoint("Carry", "largestateflow.verify:428/carry")
	g.AddTransition("Carry", workflow.END)
	proxy.HandleGraph("largestateflow.verify:428/g", g)
	// No-op task: the initial-state payload rides through unchanged into final_state at completion.
	proxy.HandleTask("largestateflow.verify:428/carry", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	const payloadLen = 200 * 1024 // comfortably past MySQL TEXT's 64 KB ceiling
	payload := strings.Repeat("x", payloadLen)

	_, out, err := eng.Run(ctx, "largestateflow.verify:428/g", map[string]any{"payload": payload}, nil)
	assert.NoError(err)
	if assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		got, _ := out.State["payload"].(string)
		assert.Equal(payloadLen, len(got))                              // not truncated
		assert.True(got == payload, "payload corrupted in final_state") // and byte-identical (no huge diff dump)
	}
}
