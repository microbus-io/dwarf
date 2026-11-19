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
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// How TestImpairedShard works. The engine has two shards on PostgreSQL, and the second shard's connections go through
// a TCP proxy in the test (shardProxy) that can forward, refuse, delay or freeze them on demand - a server that is
// down, slow or hung, without touching the server. The test measures how long a flow takes from Create to Await on the
// healthy shard with the proxy forwarding, impairs the other shard, measures again, and compares; it also
// counts the replica's goroutines and its worker crew, and requires the impaired shard's own flows to complete once
// the proxy forwards again.

// The ways shardProxy impairs the connections it carries.
const (
	proxyPass   int32 = iota // forwards both ways
	proxyRefuse              // closes every connection and refuses new ones: the server is down
	proxySlow                // forwards every chunk proxySlowDelay late: the server is slow
	proxyHang                // keeps every connection open and forwards nothing: the server stopped answering
)

const proxySlowDelay = 100 * time.Millisecond

// shardProxy forwards TCP connections to a database server and impairs them on demand.
type shardProxy struct {
	ln     net.Listener
	target string
	mode   atomic.Int32
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	healed chan struct{} // closed, and replaced, whenever the mode leaves proxyHang
}

func newShardProxy(t *testing.T, target string) *shardProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &shardProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}, healed: make(chan struct{})}
	t.Cleanup(func() {
		p.set(proxyPass)
		ln.Close()
		p.mu.Lock()
		for c := range p.conns {
			c.Close()
		}
		p.mu.Unlock()
	})
	go p.accept()
	return p
}

func (p *shardProxy) addr() string { return p.ln.Addr().String() }

func (p *shardProxy) set(mode int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	was := p.mode.Swap(mode)
	if was == proxyHang && mode != proxyHang {
		close(p.healed)
		p.healed = make(chan struct{})
	}
	if mode == proxyRefuse {
		for c := range p.conns {
			c.Close()
		}
	}
}

func (p *shardProxy) accept() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.mode.Load() == proxyRefuse {
			client.Close()
			continue
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			client.Close()
			continue
		}
		p.mu.Lock()
		p.conns[client], p.conns[server] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(server, client)
		go p.pipe(client, server)
	}
}

// pipe copies src to dst a chunk at a time, impairing each chunk as the mode says, until either side closes.
func (p *shardProxy) pipe(dst, src net.Conn) {
	defer func() {
		dst.Close()
		src.Close()
		p.mu.Lock()
		delete(p.conns, dst)
		delete(p.conns, src)
		p.mu.Unlock()
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			for {
				p.mu.Lock()
				mode, healed := p.mode.Load(), p.healed
				p.mu.Unlock()
				if mode != proxyHang {
					break
				}
				<-healed
			}
			switch p.mode.Load() {
			case proxyRefuse:
				return
			case proxySlow:
				time.Sleep(proxySlowDelay)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				return
			}
			return
		}
	}
}

// impairBudget is every flow's time budget: with impairEngine's lease margin, how long a stopped engine's claim holds.
const impairBudget = 2 * time.Second

// impairEngine is an engine on the two shards outside test mode, with a short lease margin and recovery interval, and
// a grace that keeps closed flows live.
func impairEngine(t *testing.T, healthy, impaired string) *Engine {
	t.Helper()
	e := NewEngine()
	for _, err := range []error{
		e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))),
		e.SetShard(ShardSpec{Index: 1, DSN: healthy, MaxConnections: 8}),
		e.SetShard(ShardSpec{Index: 2, DSN: impaired, MaxConnections: 8}),
		e.SetHost(impairMux()),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	e.leaseMargin = 500 * time.Millisecond
	e.recoveryInterval = 200 * time.Millisecond
	e.archiveGrace = time.Hour
	return e
}

// impairMux serves a three-step linear flow whose tasks take a moment each, so a flow has steps in flight.
func impairMux() *hosts.Mux {
	mux := hosts.NewMux()
	g := workflow.NewGraph("Impair")
	g.SetURL("A", "impair/step")
	g.SetURL("B", "impair/step")
	g.SetURL("C", "impair/step")
	g.AddTransitionChain("A", "B", "C", workflow.End)
	mux.HandleGraph("impair/g", g)
	mux.HandleTask("impair/step", func(ctx context.Context, f *workflow.Flow) error {
		time.Sleep(20 * time.Millisecond)
		return f.Set("n", f.GetInt("n")+1)
	})
	return mux
}

// createOnShard creates a flow of impair/g on one shard, as Create does once it has placed it there.
func createOnShard(ctx context.Context, e *Engine, shard int) (string, error) {
	graph, err := e.host.LoadGraph(ctx, "impair/g")
	if err != nil {
		return "", err
	}
	ctx, done := e.opTurn(ctx, shard, "create")
	defer done()
	state, _ := workflow.NewState(nil)
	opts := e.resolveFlowOptions(&workflow.FlowOptions{TimeBudget: impairBudget})
	return e.createWithGraph(ctx, shard, "impair/g", graph, state, "", opts, 0, 0, 0, 0, nil)
}

// runOnShard runs n flows on a shard at once and returns how long each took from Create to its Await returning, and
// how many did not complete.
func runOnShard(e *Engine, shard, n int, timeout time.Duration) (took []time.Duration, failed int) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			start := time.Now()
			fk, err := createOnShard(ctx, e, shard)
			var out *workflow.FlowOutcome
			if err == nil {
				out, err = e.Await(ctx, fk)
			}
			if err == nil {
				_, err = e.Snapshot(ctx, fk)
			}
			if err == nil {
				_, err = e.History(ctx, fk)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil || out.Status != workflow.StatusCompleted {
				failed++
				return
			}
			took = append(took, time.Since(start))
		})
	}
	wg.Wait()
	slices.Sort(took)
	return took, failed
}

func pct(took []time.Duration, p float64) time.Duration {
	if len(took) == 0 {
		return 0
	}
	return took[min(len(took)-1, int(float64(len(took))*p))]
}

// TestImpairedShard_HealthyShardProgresses impairs one of two shards - its server refusing connections, slow, or
// holding its connections open without answering - and runs flows on the other meanwhile, each Created on it, Awaited,
// and read by Snapshot and History. They must all complete at the pace they kept before, with the replica's goroutines
// and crew where they were, and the impaired shard's own flows must complete once it heals. Run serially: it bounds
// the healthy shard's latency.
//
// A Create whose shard is not chosen must land on the healthy one once this replica reads the refused or hung shard
// as blind. It logs, without asserting, what still couples the shards: such a Create against a slow shard, a ListOpen,
// a replica starting and this one stopping while the shard is impaired. Each is an open decision, not a contract to
// pin.
func TestImpairedShard_HealthyShardProgresses(t *testing.T) {
	base := os.Getenv("SEQUEL_TESTING_DSN")
	if !strings.HasPrefix(base, "postgres") {
		t.Skip("impairs a PostgreSQL server's connections")
	}
	for _, tc := range []struct {
		name string
		mode int32
	}{{"refused", proxyRefuse}, {"slow", proxySlow}, {"hung", proxyHang}} {
		t.Run(tc.name, func(t *testing.T) {
			impairedShard(t, tc.mode)
		})
	}
}

func impairedShard(t *testing.T, mode int32) {
	assert := testarossa.For(t)
	ctx := context.Background()
	healthy := ownPostgresDatabase(t, "impair")
	impaired := ownPostgresDatabase(t, "impair")
	proxy := newShardProxy(t, impaired.Host)
	impaired.Host = proxy.addr()

	e := impairEngine(t, healthy.String(), impaired.String())
	assert.Must(assert.NoError(e.Startup(ctx)))
	stopped := make(chan time.Duration, 1)
	stop := sync.OnceFunc(func() {
		go func() {
			start := time.Now()
			e.Shutdown(ctx)
			stopped <- time.Since(start)
		}()
	})
	t.Cleanup(func() {
		proxy.set(proxyPass)
		stop()
		<-stopped
	})

	const n = 20
	timeout := 30 * time.Second * enginetest.TimeoutScale()
	control, failed := runOnShard(e, 1, n, timeout)
	assert.Zero(failed)
	t.Logf("control: p50 %s, max %s", pct(control, 0.5), pct(control, 1))

	// Flows of the impaired shard's own in flight when it is impaired.
	var stranded []string
	for range 4 {
		fk, err := createOnShard(ctx, e, 2)
		assert.Must(assert.NoError(err))
		stranded = append(stranded, fk)
	}
	goroutines := runtime.NumGoroutine()
	proxy.set(mode)
	impairedAt := time.Now()

	took, failed := runOnShard(e, 1, n, timeout)
	assert.Zero(failed, "flows on the healthy shard did not complete")
	t.Logf("impaired: p50 %s, max %s; goroutines %d -> %d, crew %d of %d", pct(took, 0.5), pct(took, 1),
		goroutines, runtime.NumGoroutine(), e.crew.Resident(), e.crew.Max())
	assert.True(pct(took, 0.5) <= 2*pct(control, 0.5)+500*time.Millisecond,
		"the healthy shard slowed from a p50 of %s to %s", pct(control, 0.5), pct(took, 0.5))
	assert.True(runtime.NumGoroutine()-goroutines < 100, "goroutines grew from %d to %d", goroutines, runtime.NumGoroutine())
	assert.True(e.crew.Resident() <= e.crew.Min()+n, "the crew grew to %d workers", e.crew.Resident())

	// Placement leaves out a shard this replica's Sonar reads as blind. A refused or hung shard's registry reads stop
	// landing, so once it reads blind every Create must land on the healthy shard. A slow shard's reads may still
	// land, so its Creates are logged, not asserted. The wait polls the clock: a hung read blocks the Sonar's next
	// pass, and blindness turns true while it hangs.
	if mode != proxySlow {
		deadline := time.Now().Add(timeout)
		for !e.blindOn(2) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		assert.Must(assert.True(e.blindOn(2), "the impaired shard never read as blind"))
	}
	var placedBad int
	for range 10 {
		cctx, cancel := context.WithTimeout(ctx, time.Second)
		if _, err := e.Create(cctx, "impair/g", nil, nil); err != nil {
			placedBad++
		}
		cancel()
	}
	if mode == proxySlow {
		t.Logf("Create with placement: %d of 10 failed or timed out (blind: %v)", placedBad, e.blindOn(2))
	} else {
		assert.Zero(placedBad, "a Create was placed on the shard this replica cannot read")
	}
	lctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	start := time.Now()
	_, lerr := e.ListOpen(lctx, workflow.Query{Limit: 10})
	cancel()
	t.Logf("ListOpen: %v after %s", lerr, time.Since(start).Round(time.Millisecond))
	sctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	start = time.Now()
	info, serr := e.ShardInfo(sctx)
	cancel()
	t.Logf("ShardInfo: %+v, %v after %s", info, serr, time.Since(start).Round(time.Millisecond))

	// A replica starting, and this one stopping, while the shard is impaired.
	e2 := impairEngine(t, healthy.String(), impaired.String())
	started := make(chan error, 1)
	start = time.Now()
	go func() { started <- e2.Startup(ctx) }()
	stop()
	const watch = time.Second
	select {
	case err := <-started:
		t.Logf("Startup while impaired: %v after %s", err, time.Since(start).Round(time.Millisecond))
		started <- err
	case <-time.After(watch):
		t.Logf("Startup while impaired: still waiting after %s", watch)
	}
	select {
	case took := <-stopped:
		t.Logf("Shutdown while impaired: returned after %s", took.Round(time.Millisecond))
		stopped <- took
	case <-time.After(time.Second):
		t.Logf("Shutdown while impaired: still waiting after %s", time.Since(start).Round(time.Millisecond))
	}

	proxy.set(proxyPass)
	healedAt := time.Now()
	t.Logf("impaired for %s", time.Since(impairedAt).Round(time.Millisecond))
	select {
	case took := <-stopped:
		t.Logf("Shutdown returned %s after it began", took.Round(time.Millisecond))
		stopped <- took
	case <-time.After(timeout):
		t.Fatal("Shutdown never returned after the shard healed")
	}
	// The stopped replica's Leave and its lease releases may not have reached the impaired shard: its row would then hold
	// a residue class there until the dispatch window lapsed, and its claimed steps wait out their leases. The test
	// deletes the row, as that Leave would have, and lapses the leases, as the time would.
	db, err := sequel.Open("", impaired.String())
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=?", e.engineID)
	if err == nil {
		_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE status='"+
			workflow.StatusRunning+"' AND parked=0")
	}
	db.Close()
	assert.Must(assert.NoError(err))
	select {
	case err = <-started:
	case <-time.After(timeout):
		t.Fatal("Startup never returned after the shard healed")
	}
	if err != nil {
		t.Logf("Startup failed: %v; a third replica takes over", err)
		e2 = impairEngine(t, healthy.String(), impaired.String())
		assert.Must(assert.NoError(e2.Startup(ctx)))
	}
	defer e2.Shutdown(ctx)
	for _, fk := range stranded {
		actx, cancel := context.WithTimeout(ctx, timeout)
		out, err := e2.Await(actx, fk)
		cancel()
		if assert.NoError(err, "a flow of the impaired shard after it healed") {
			assert.Equal(workflow.StatusCompleted, out.Status)
		}
	}
	t.Logf("the impaired shard's flows completed %s after it healed", time.Since(healedAt).Round(time.Millisecond))
}
