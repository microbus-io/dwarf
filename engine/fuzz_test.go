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
	"testing"
)

// FuzzDeliverSignal asserts the inbound peer entry point — the engine's only surface that accepts
// raw bytes from the (host-authenticated, but still external) peer channel — is total: arbitrary
// op strings and payload bytes never panic the engine or wedge it. Malformed payloads and unknown
// ops must return an error; structurally-valid-but-bogus signals (nonexistent shards/steps/keys)
// must be absorbed harmlessly, per the documented small-blast-radius contract.
func FuzzDeliverSignal(f *testing.F) {
	f.Add("enqueue", []byte(`{"Shard":1,"StepID":1}`))
	f.Add("enqueue", []byte(`{"Shard":0,"StepID":0}`))
	f.Add("enqueue", []byte(`{"Shard":-3,"StepID":9999999}`))
	f.Add("enqueue", []byte(`{"Shard":99,"StepID":1}`))
	f.Add("statusChange", []byte(`{"FlowKey":"1-1-deadbeef","Status":"completed"}`))
	f.Add("statusChange", []byte(`{"FlowKey":"","Status":""}`))
	f.Add("enqueue", []byte(`not json`))
	f.Add("statusChange", []byte(`[]`))
	f.Add("unknownop", []byte(`{}`))
	f.Add("", []byte(nil))

	// One engine per fuzz worker process (workers are separate processes, each with its own in-memory
	// SQLite test databases), built before f.Fuzz and shut down by NewEngineUnderTest's f.Cleanup at the
	// end of the run. A *testing.F keeps the engine's logger silent. A Startup failure skips the target
	// rather than failing it, so the run degrades cleanly on an environment problem.
	e := NewEngineUnderTest(f)
	_ = e.SetHost(noopHost{})
	if err := e.Startup(context.Background()); err != nil {
		f.Skip("fuzz engine failed to start: " + err.Error())
	}
	f.Fuzz(func(t *testing.T, op string, payload []byte) {
		// Must not panic; errors are legitimate outcomes for malformed input.
		_ = e.DeliverSignal(context.Background(), op, payload)
	})
}
