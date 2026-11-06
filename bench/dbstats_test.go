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
	"testing"

	"github.com/microbus-io/testarossa"
)

func TestDBStats_WindowDeltaCountsTheArchive(t *testing.T) {
	assert := testarossa.For(t)

	s := &dbStatsSampler{}
	before := dbStats{StepsBytes: 100, FlowsBytes: 10, ArchiveStepsBytes: 1000, ArchiveFlowsBytes: 200,
		WALBytes: 5000, WALFPI: 7, WALBuffersFull: 1, WALSyncs: 3, Commits: 50, ok: true}
	after := dbStats{StepsBytes: 104, FlowsBytes: 11, ArchiveStepsBytes: 9000, ArchiveFlowsBytes: 1700,
		WALBytes: 25000, WALFPI: 17, WALBuffersFull: 1, WALSyncs: 13, Commits: 450, ok: true}
	d := s.windowDelta(before, after)
	if assert.NotNil(d) {
		assert.Equal(dbDelta{StepsBytes: 4, FlowsBytes: 1, ArchiveStepsBytes: 8000, ArchiveFlowsBytes: 1500,
			WALBytes: 20000, WALFPI: 10, WALSyncs: 10, Commits: 400}, *d)
	}

	// A failed reading on either side yields no delta.
	before.ok = false
	assert.Nil(s.windowDelta(before, after))
}
