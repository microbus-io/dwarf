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

package workers

import (
	"context"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/testarossa"
)

// TestCrew_FullShardIsPassedOver pins the take loop's reroute: a shard with no free permit does not hold the
// crew while another shard has work and permits. Shard 1 holds far more candidates, so routing alone sends
// every worker there, where its one permit is held for the whole test; without the reroute shard 2's work is
// never served.
func TestCrew_FullShardIsPassedOver(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := newCache(t)
	s := turnstile.NewSet()
	s.Resize(1, 1)
	s.Resize(2, 4)
	t.Cleanup(s.Close)
	held, ok := s.Turnstile(1).TryWaitTurn()
	if !assert.True(ok) {
		return
	}
	defer held.Return()

	var mu sync.Mutex
	served := map[int]int{}
	crew, err := New(c, s.Gate(1), func(_ context.Context, shard, stepID int, release func()) error {
		release()
		mu.Lock()
		served[shard]++
		mu.Unlock()
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(4)

	deep := make([]candidates.Job, 50)
	for i := range deep {
		deep[i] = candidates.Job{StepID: i + 1, Shard: 1}
	}
	c.Refill(1, deep, 0)
	shallow := make([]candidates.Job, 5)
	for i := range shallow {
		shallow[i] = candidates.Job{StepID: 100 + i, Shard: 2}
	}
	c.Refill(2, shallow, 0)

	crew.Start(context.Background(), 4)
	assert.True(eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return served[2] == 5
	}), "shard 2's work must be served while shard 1 has no permit to give")
	mu.Lock()
	assert.Equal(0, served[1], "shard 1's permit is held throughout")
	mu.Unlock()

	c.Close()
	s.Close()
	crew.Drain()
}

// TestCrew_EveryShardFullWaitsWhereRouted pins where a worker waits once every shard it tried is full: on the
// shard routing sent it to, not on the last one it tried. Waiting on the last one would, under full
// saturation, pile the queue in front of dispatch onto the worst-ranked shard. It also pins that a crew
// facing nothing but full shards does not grow.
func TestCrew_EveryShardFullWaitsWhereRouted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := newCache(t)
	s := turnstile.NewSet()
	s.Resize(1, 1)
	s.Resize(2, 1)
	t.Cleanup(s.Close)
	for _, shard := range []int{1, 2} {
		held, ok := s.Turnstile(shard).TryWaitTurn()
		if !assert.True(ok) {
			return
		}
		defer held.Return()
	}

	crew, err := New(c, s.Gate(1), func(_ context.Context, shard, stepID int, release func()) error {
		release()
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(8)

	deep := make([]candidates.Job, 50)
	for i := range deep {
		deep[i] = candidates.Job{StepID: i + 1, Shard: 1}
	}
	c.Refill(1, deep, 0)
	c.Refill(2, []candidates.Job{{StepID: 100, Shard: 2}}, 0)

	crew.Start(context.Background(), 1)
	assert.True(eventually(func() bool {
		_, waiting := s.Turnstile(1).Snapshot()
		return waiting == 1
	}), "the worker waits on shard 1, where it was routed")
	_, waiting := s.Turnstile(2).Snapshot()
	assert.Equal(0, waiting, "not on shard 2, the last shard it tried")
	assert.Equal(1, settles(crew), "a crew facing only full shards does not grow")

	c.Close()
	s.Close()
	crew.Drain()
}
