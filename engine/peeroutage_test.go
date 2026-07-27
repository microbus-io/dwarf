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
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// stallProxy is a TCP relay the test puts between a whole fleet and its database server, so it can stop the
// server answering ANY of them - the correlated stall, which is the shape that matters and the one a
// per-replica fault seam cannot produce.
//
// It stalls rather than resets: bytes already read are held at the gate and delivered on resume, so a
// blocked replica sits in a hung round trip exactly as it would against an unwell server, instead of getting
// a prompt error. The two are different inputs to the same policy - an error is an event, a hang is an
// absence - and only the hang exercises the freshness windows, which are what the design actually rests on.
//
// Preferred over pausing the server itself: the container is shared with every other test in the suite, and
// a relay blackholes only the connections opened through it.
type stallProxy struct {
	addr     string
	upstream string
	ln       net.Listener
	mu       sync.Mutex
	blocked  chan struct{} // nil when passing bytes; open when stalled, closed to release
	closed   bool
}

// newStallProxy relays to the host:port named in dsn, returning the proxy and dsn rewritten to reach it.
func newStallProxy(t *testing.T, dsn string) (*stallProxy, string) {
	t.Helper()
	assert := testarossa.For(t)
	at := strings.LastIndex(dsn, "@")
	if !assert.True(at >= 0, "no credentials@host in the base DSN") {
		return nil, dsn
	}
	slash := strings.Index(dsn[at:], "/")
	if !assert.True(slash > 0, "no /database in the base DSN") {
		return nil, dsn
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if !assert.NoError(err) {
		return nil, dsn
	}
	p := &stallProxy{addr: ln.Addr().String(), upstream: dsn[at+1 : at+slash], ln: ln}
	t.Cleanup(p.close)
	go p.serve()
	return p, dsn[:at+1] + p.addr + dsn[at+slash:]
}

func (p *stallProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.upstream)
		if err != nil {
			client.Close()
			continue
		}
		go p.relay(client, server)
		go p.relay(server, client)
	}
}

// relay copies one direction, holding every chunk at the gate before it is delivered.
func (p *stallProxy) relay(src, dst net.Conn) {
	defer src.Close()
	defer dst.Close()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.await()
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *stallProxy) await() {
	p.mu.Lock()
	ch := p.blocked
	p.mu.Unlock()
	if ch != nil {
		<-ch
	}
}

// stall stops delivery in both directions until resume. Idempotent.
func (p *stallProxy) stall() {
	p.mu.Lock()
	if p.blocked == nil && !p.closed {
		p.blocked = make(chan struct{})
	}
	p.mu.Unlock()
}

// resume releases everything held at the gate. Idempotent.
func (p *stallProxy) resume() {
	p.mu.Lock()
	if p.blocked != nil {
		close(p.blocked)
		p.blocked = nil
	}
	p.mu.Unlock()
}

// close releases the gate before dropping the listener, so no relay is left parked while the engines are
// being torn down.
func (p *stallProxy) close() {
	p.resume()
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.ln.Close()
}

// runFleetOutage stands up a four-replica fleet behind a stall proxy, takes the database away from all of
// them for the given duration, and returns the worst readings taken continuously from before the outage
// until after the fleet has re-settled.
//
// The dangerous direction is GROWTH. A replica reads the registry to learn how many ways to split its
// shard's connection budget, so a reading that returns nothing looks exactly like a fleet that shrank, and
// acting on it makes every replica size for a smaller fleet SIMULTANEOUSLY - four replicas each taking the
// whole budget, against a server whose failure to answer is the evidence it is already unwell. So the pools
// must sit exactly where the last good reading put them, for as long as the outage lasts and through the
// recovery afterwards.
//
// Sampling spans the recovery because that is where a storm would actually land: when the server answers
// again, every row is momentarily stale by the length of the outage. A pair of before-and-after readings
// would miss a transient spike there entirely, which is the whole failure mode.
func runFleetOutage(t *testing.T, outage time.Duration) (worstOpen, worstReplicas, blindPartitions int) {
	t.Helper()
	assert := testarossa.For(t)
	base := os.Getenv("SEQUEL_TESTING_DSN")
	const (
		shard    = 1
		vCPUs    = 8
		replicas = 4
	)
	// Derived from the policy, not restated: the invariant under test is how the budget DIVIDES across a
	// fleet, which must hold whatever ratio connsPerVCPUFor picks for this size.
	budget := connsPerVCPUFor(vCPUs) * vCPUs
	share := budget / replicas

	proxy, proxied := newStallProxy(t, base)
	if proxy == nil {
		return 0, replicas, 0
	}

	// One fleet, every replica reaching the server only through the proxy - so the stall below is fleet-wide
	// by construction rather than by arranging for four separate faults to coincide.
	fleet := &restartFleet{}
	for range replicas {
		e := NewEngineUnderTest(t)
		e.testConnCap = 0 // assert the real derived sizes, not the test-mode cap
		assert.NoError(e.SetHost(noopHost{}))
		assert.NoError(e.SetWorkers(1))
		assert.NoError(e.SetShard(ShardSpec{Index: shard, DSN: proxied, VirtualCPUs: vCPUs}))
		fleet.add(e)
		assert.NoError(e.Startup(t.Context()))
	}
	awaitFleetSettled(t, fleet, shard, replicas, share)

	// Sample from before the outage until after the fleet has re-settled. Every reading is pure memory - a
	// published count, a published partition, an applied pool ceiling - so the sampler keeps running at full
	// rate while every replica's database round trips are hung.
	type reading struct {
		open       int
		replicas   int
		partitions int
	}
	var samples []reading
	samplerStop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			select {
			case <-samplerStop:
				return
			default:
			}
			for _, e := range fleet.snapshot() {
				db, err := e.db.Shard(shard)
				if err != nil {
					continue
				}
				r := reading{open: db.DB.Stats().MaxOpenConnections, replicas: e.replicasOn(shard)}
				if _, _, ok := e.partitionOn(shard); ok {
					r.partitions = 1
				}
				samples = append(samples, r)
			}
			time.Sleep(time.Millisecond)
		}
	})

	// The server goes away for every replica at once, then comes back.
	proxy.stall()
	time.Sleep(outage)
	for _, e := range fleet.snapshot() {
		if _, _, ok := e.partitionOn(shard); ok {
			blindPartitions++
		}
	}
	proxy.resume()
	awaitFleetSettled(t, fleet, shard, replicas, share)
	close(samplerStop)
	sampler.Wait()

	worstOpen, worstReplicas = 0, replicas
	for _, s := range samples {
		worstOpen = max(worstOpen, s.open)
		worstReplicas = min(worstReplicas, s.replicas)
	}
	assert.True(len(samples) > 1000, "the sampler only took %d readings, too few to have covered the outage", len(samples))
	return worstOpen, worstReplicas, blindPartitions
}

// TestPeerOutage_FleetWideStallGrowsNoPool takes the database away from a whole fleet and pins that nothing
// grows: not while it is away, and not on the way back.
//
// **A fleet-wide outage cannot produce a recovery storm at all, and the reason is worth stating** because it
// is not the mechanism one reaches for. The obvious worry is the freshness window lapsing: past it, every
// peer's row is legitimately stale, so the first reading back correctly reports a registry nobody has beaten
// in, every replica reads it at the same instant, and every replica sizes for a fleet of one at the same
// instant - four times the shard's whole budget, arriving on a server that has only just come back. The gap
// rule exists for exactly that (a reading which ends a gap may only raise the count), and it is pinned in
// internal/peers against an injected clock.
//
// But the stall that makes the rows look dead is the same stall that holds each replica's own BEAT, and both
// are released together. Measured at a 45-second outage - comfortably past the window - no replica ever
// published a smaller fleet, because the held beats re-stamp every row alongside the reads that resume.
// Suppressing the beats at the moment of resume (FaultPeerBeatErr, injected just before the bytes flow) is
// what makes the fleet fall to one and the pools grow to the full 48 - confirming both that the window really
// had lapsed and that the beats are what carry the fleet across it. So the storm needs peers that genuinely
// died, which is a different test and an instant one (see TestPeerFault_BlindHoldsThePoolsAndFailsOpen, which
// deletes the rows outright); it is not reachable by taking one database away.
//
// A few seconds is therefore the honest duration for this arm - the length a failover or a bad minute on a
// managed instance actually lasts - and at that length the pools are held by the plainest mechanism there is:
// a reading that HUNG produced no count, so there was nothing to publish and nothing to act on.
//
// What the arm exercises beyond that is the opposite-facing half: partitioning switches off within two read
// cadences of going blind, long before any freshness window is in question. Excluding rows by residue class
// strands whatever falls in a class nobody claims, so an unjustifiable partition is abandoned even though
// abandoning it costs overlapping selection.
//
// Requires a real database server (SEQUEL_TESTING_DSN); there is nothing to stall on SQLite's in-memory
// default, whose connections never touch a socket.
func TestPeerOutage_FleetWideStallGrowsNoPool(t *testing.T) {
	assert := testarossa.For(t)
	if !strings.Contains(os.Getenv("SEQUEL_TESTING_DSN"), "://") {
		t.Skip("needs a real database server: set SEQUEL_TESTING_DSN")
	}
	worstOpen, worstReplicas, blindPartitions := runFleetOutage(t, 4*time.Second)
	assert.Equal(0, blindPartitions,
		"a blind replica must select everything rather than trust a residue class it can no longer justify")
	assert.Equal(connsPerVCPUFor(8)*8/4, worstOpen,
		"a replica grew its pool to %d during the outage: an unanswered reading was taken for a smaller fleet", worstOpen)
	assert.Equal(4, worstReplicas,
		"a replica published a fleet of %d during the outage: a reading that did not happen is not an observation of absence", worstReplicas)
}
