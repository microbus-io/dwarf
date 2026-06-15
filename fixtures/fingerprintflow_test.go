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

/*
Fingerprint returns an opaque hash plus the flow status, for cheap change
detection (e.g. a UI polling whether a flow advanced). The fingerprint is stable
while the flow is unchanged and differs once the flow progresses. An interrupt
provides two stable, distinct observation points.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestFingerprintflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Fingerprint", "fingerprintflow.verify:428/fingerprint")
	graph.AddTask("TaskA", "fingerprintflow.verify:428/task-a")
	graph.AddTask("Pause", "fingerprintflow.verify:428/pause")
	graph.AddTask("Done", "fingerprintflow.verify:428/done")
	graph.AddTransition("TaskA", "Pause")
	graph.AddTransition("Pause", "Done")
	graph.AddTransition("Done", workflow.END)
	proxy.HandleGraph("fingerprintflow.verify:428/fingerprint", graph)

	proxy.HandleTask("fingerprintflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("fingerprintflow.verify:428/pause", func(ctx context.Context, f *workflow.Flow) error {
		_, yield, err := f.Interrupt(map[string]any{"need": "input"})
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("fingerprintflow.verify:428/done", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", "finished")
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("fingerprint_stable_then_changes_on_progress", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "fingerprintflow.verify:428/fingerprint", nil, nil)
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(eng.Start(ctx, flowKey)) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)

		fpA, statusA, err := eng.Fingerprint(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, statusA)
		assert.NotEqual("", fpA)

		// Re-reading an unchanged flow yields the identical fingerprint.
		fpA2, statusA2, err := eng.Fingerprint(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(fpA, fpA2)
		assert.Equal(statusA, statusA2)

		// Advance the flow to completion; the fingerprint must change.
		if !assert.NoError(eng.Resume(ctx, flowKey, nil)) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		fpB, statusB, err := eng.Fingerprint(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, statusB)
		assert.NotEqual(fpA, fpB)
	})

	t.Run("fingerprint_unknown_flow_errors", func(t *testing.T) {
		assert := testarossa.For(t)
		_, _, err := eng.Fingerprint(ctx, "1-999999-deadbeef")
		assert.Error(err)
	})
}
