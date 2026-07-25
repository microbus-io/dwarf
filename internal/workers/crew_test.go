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
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// newCache returns a cache sized well past anything these tests push through it, so the bound is never
// what a test is accidentally measuring.
func newCache(t *testing.T) *candidatecache.Cache {
	t.Helper()
	c := &candidatecache.Cache{}
	c.Init(64) // capacity 128
	t.Cleanup(c.Close)
	return c
}

// fill pushes n candidates onto shard 1 as a refill would.
func fill(c *candidatecache.Cache, n int) {
	batch := make([]candidatecache.Job, 0, n)
	for i := range n {
		batch = append(batch, candidatecache.Job{StepID: i + 1, Shard: 1})
	}
	c.Refill(1, batch, 100)
}

// eventually waits briefly for cond, so a test never depends on goroutine scheduling order.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func TestCrew_NewValidates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	_, err := New(nil, func(context.Context, int, int) error { return nil })
	assert.Error(err, "cache is required")
	_, err = New(&candidatecache.Cache{}, nil)
	assert.Error(err, "process is required")
}

// TestPool_ProcessesEveryCandidate is the baseline: the resident set drains the cache and each candidate
// reaches the callback exactly once.
func TestCrew_ProcessesEveryCandidate(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var mu sync.Mutex
	seen := map[int]int{}
	crew, err := New(c, func(_ context.Context, shard, stepID int) error {
		mu.Lock()
		seen[stepID]++
		mu.Unlock()
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(4)

	fill(c, 20)
	crew.Start(context.Background(), 4)
	assert.True(eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 20
	}), "every candidate must reach the callback")

	c.Close()
	crew.Drain()
	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		assert.Equal(1, n, "step %d handled once", id)
	}
}

// TestPool_SaturationDoesNotGrowThePool is the NEGATIVE half of the growth trigger, and it is the half
// that matters. A worker merely BUSY in the callback must not grow the crew. An earlier version of this
// counter (in the engine) wrapped the whole handler, which includes waiting for a database connection, so
// ordinary saturation read as "every worker is away": each new goroutine queued on the same connections and
// made the signal truer the worse it got. Measured cost before it was fixed - ~20% throughput on a
// saturated shard, and a pool bloated to ~1,300 goroutines where ~512 sufficed.
func TestCrew_SaturationDoesNotGrowThePool(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	hold := make(chan struct{})
	var busy atomic.Int32
	crew, err := New(c, func(context.Context, int, int) error {
		busy.Add(1)
		<-hold // busy, and deliberately never offsite
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(8)

	fill(c, 20)
	crew.Start(context.Background(), 2)
	assert.True(eventually(func() bool { return busy.Load() == 2 }), "both workers entered the callback")
	time.Sleep(50 * time.Millisecond) // long enough for a spurious spawn to show up
	assert.Equal(2, crew.Resident(), "saturation alone must not grow the pool")

	close(hold)
	c.Close()
	crew.Drain()
}

// TestPool_GrowsWhenAllOffsite is the positive half: with every worker away, the pool adds capacity.
func TestCrew_GrowsWhenAllOffsite(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var crew *Crew
	hold := make(chan struct{})
	var away atomic.Int32

	crew, err := New(c, func(_ context.Context, shard, stepID int) error {
		onsite := crew.Offsite()
		away.Add(1)
		<-hold
		away.Add(-1)
		onsite()
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(6)

	fill(c, 20)
	crew.Start(context.Background(), 2)

	// Each worker that goes away leaves nobody to dispatch, so the pool keeps adding until it hits Max.
	assert.True(eventually(func() bool { return crew.Resident() == 6 }),
		"every worker offsite must grow the pool to Max, got %d resident", crew.Resident())
	assert.True(eventually(func() bool { return away.Load() == 6 }))

	close(hold)
	c.Close()
	crew.Drain()
	assert.Equal(0, crew.AwayCount(), "every release ran")
}

// TestPool_MaxIsRespectedAndLive pins that growth stops at Max and that lowering Max stops further growth
// without retiring goroutines that already exist.
func TestCrew_MaxIsRespectedAndLive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var crew *Crew
	hold := make(chan struct{})
	crew, err := New(c, func(_ context.Context, shard, stepID int) error {
		onsite := crew.Offsite()
		defer onsite()
		<-hold
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(3)

	fill(c, 20)
	crew.Start(context.Background(), 1)
	assert.True(eventually(func() bool { return crew.Resident() == 3 }))
	time.Sleep(30 * time.Millisecond)
	assert.Equal(3, crew.Resident(), "growth stops at Max")

	// Lowering Max stops growth but never shrinks: retiring a goroutine would need a whole protocol.
	crew.SetMax(1)
	assert.Equal(3, crew.Resident(), "lowering Max does not retire existing workers")

	close(hold)
	c.Close()
	crew.Drain()
}

// TestPool_StartSpawnsNoMoreThanMax pins that even the resident set is capped, so a caller that asks for
// more residents than the ceiling gets the ceiling rather than an over-full crew.
func TestCrew_StartSpawnsNoMoreThanMax(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	crew, err := New(c, func(context.Context, int, int) error { return nil })
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(2)
	crew.Start(context.Background(), 10)
	assert.Equal(2, crew.Resident())

	c.Close()
	crew.Drain()
}

// TestPool_DrainRacesGrowthWithoutPanicking is the reason this package is separable at all: the shutdown
// protocol is subtle and was previously untestable in isolation. A WaitGroup.Add concurrent with a Wait
// PANICS, and a worker can go offsite - and so try to spawn a peer - at any instant, including while Drain
// is waiting. The spawnClosed flag, set under the same lock that guards the Add, is what makes it safe.
func TestCrew_DrainRacesGrowthWithoutPanicking(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var crew *Crew
	stop := make(chan struct{})
	crew, err := New(c, func(_ context.Context, shard, stepID int) error {
		// Hammer the spawn path for as long as the pool lives, so Drain is guaranteed to overlap it.
		for {
			select {
			case <-stop:
				return nil
			default:
			}
			onsite := crew.Offsite()
			onsite()
		}
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(16)

	fill(c, 64)
	crew.Start(context.Background(), 8)
	assert.True(eventually(func() bool { return crew.Resident() > 8 }), "the pool grew while spinning")

	// Drain while every worker is still trying to spawn peers. Without the flag this panics.
	close(stop)
	c.Close()
	crew.Drain()
	assert.Equal(0, crew.AwayCount())
}

// TestPool_SpawnAfterDrainIsInert pins that a late spawn attempt - a worker on its way out calling Offsite
// after Drain has closed the pool - adds nothing rather than joining a WaitGroup nobody waits on.
func TestCrew_SpawnAfterDrainIsInert(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	crew, err := New(c, func(context.Context, int, int) error { return nil })
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(4)
	crew.Start(context.Background(), 1)
	c.Close()
	crew.Drain()

	before := crew.Resident()
	onsite := crew.Offsite()
	onsite()
	assert.Equal(before, crew.Resident(), "the pool is closed to new goroutines")
}

// TestPool_LeakedReleaseIsReported pins the alarm that stands in for the defer this API deliberately does
// not do. offsite can never legitimately exceed resident - a goroutine is away at most once and resident
// never shrinks - so exceeding it means a release was skipped.
//
// Note WHERE it becomes visible, because it is not immediately: below Max a leak is absorbed by GROWTH,
// since each leaked increment spawns a replacement and keeps resident ahead of offsite. It surfaces once
// the pool is at Max - which is exactly the state a leak strands you in, and exactly why the alarm is worth
// having: at that point the pool is pinned at its ceiling and simply looks busy.
func TestCrew_LeakedReleaseIsReported(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var buf bytes.Buffer
	crew, err := New(c, func(context.Context, int, int) error { return nil })
	if !assert.NoError(err) {
		return
	}
	crew.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	crew.SetMax(1) // already at the ceiling, so growth cannot absorb the leak
	crew.Start(context.Background(), 1)

	crew.Offsite() // legitimate: one worker away of one resident
	assert.False(strings.Contains(buf.String(), "leaked"), "offsite == resident is the normal trigger")

	crew.Offsite() // a second, with nothing to spawn and nobody released: impossible unless leaked
	assert.True(strings.Contains(buf.String(), "leaked"),
		"a leaked release must be reported once growth cannot mask it, got %q", buf.String())

	c.Close()
	crew.Drain()
}

// TestPool_HandlerErrorAndPanicKeepTheWorkerAlive pins that neither an error nor a panic from the callback
// takes a goroutine down: the pool logs and goes back for the next candidate. A panic that escaped would
// kill the process, and a worker that exited on an error would silently shrink the crew.
func TestCrew_HandlerErrorAndPanicKeepTheWorkerAlive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var buf bytes.Buffer
	var handled atomic.Int32
	crew, err := New(c, func(_ context.Context, shard, stepID int) error {
		switch n := handled.Add(1); n {
		case 1:
			return errors.New("boom")
		case 2:
			panic("panic in the handler")
		}
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	crew.SetMax(1)

	fill(c, 4)
	crew.Start(context.Background(), 1)
	assert.True(eventually(func() bool { return handled.Load() == 4 }),
		"one worker must survive both an error and a panic, handled %d of 4", handled.Load())
	assert.Equal(1, crew.Resident())

	c.Close()
	crew.Drain()
	assert.True(strings.Contains(buf.String(), "Processing candidate"))
}
