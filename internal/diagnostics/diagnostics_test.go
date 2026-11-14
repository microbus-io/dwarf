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

package diagnostics

import (
	"testing"

	"github.com/microbus-io/seamster"
	"github.com/microbus-io/testarossa"
)

func TestCheckpointBy_PassesBareThenTargeted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	d := New()
	s := seamster.New(true)
	d.SetSeams(s)
	d.CheckpointBy(t.Context(), "point", 3, 12345)
	assert.Equal(1, s.Visits("point"))
	assert.Equal(1, s.Visits(Join("point", "3", "12345")))
}

// TestCheckpointBy_FreeWhenDisabled pins what lets a production site call it unguarded: with the seams inert it
// builds nothing, the ids' slice included. Not parallel: AllocsPerRun refuses to run beside other tests.
func TestCheckpointBy_FreeWhenDisabled(t *testing.T) {
	assert := testarossa.For(t)
	ctx := t.Context()
	shard, stepID := 3, 12345
	for _, d := range []*Diags{nil, New()} {
		allocs := testing.AllocsPerRun(100, func() {
			d.CheckpointBy(ctx, CheckpointBeforeTransitionTx, shard, stepID)
		})
		assert.Equal(0.0, allocs)
	}
}
