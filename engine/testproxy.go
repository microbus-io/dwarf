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
	"net/http"
	"sync"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// TaskHandler is the signature for a test task handler. Read the flow's baggage, if any, with
// workflow.BaggageFrom(ctx).
type TaskHandler func(ctx context.Context, flow *workflow.Flow) error

// TestProxy routes graph fetches and task dispatches to registered handlers. It implements the Host
// interface for use with Engine.WithHost / Engine.RunInTest: LoadGraph and ExecuteTask dispatch to the
// registered handlers, FlowStopped invokes an optional callback set via OnFlowStopped, and SignalPeers
// relays to the peer engines registered with AddPeer (none by default, i.e. single-replica). For a
// multi-replica test, give each replica its own proxy and AddPeer the other engines.
type TestProxy struct {
	mu          sync.RWMutex
	graphs      map[string]*workflow.Graph
	tasks       map[string]TaskHandler
	flowStopped func(ctx context.Context, outcome *workflow.FlowOutcome)
	peers       []*Engine
}

// NewTestProxy creates a new test proxy with empty handler registries.
func NewTestProxy() *TestProxy {
	return &TestProxy{
		graphs: make(map[string]*workflow.Graph),
		tasks:  make(map[string]TaskHandler),
	}
}

// HandleGraph registers a workflow graph under the given name.
// The name should match the workflow URL passed to Engine.Create or Engine.Run.
func (p *TestProxy) HandleGraph(name string, graph *workflow.Graph) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.graphs[name] = graph
}

// HandleTask registers a task handler under the given name.
// The name should match the task URL registered via graph.AddTask.
func (p *TestProxy) HandleTask(name string, handler TaskHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tasks[name] = handler
}

// OnFlowStopped registers a callback invoked by FlowStopped (only for flows created with
// FlowOptions.NotifyOnStop). Nil (the default) makes FlowStopped a no-op. The flow's baggage is on the ctx.
func (p *TestProxy) OnFlowStopped(cb func(ctx context.Context, outcome *workflow.FlowOutcome)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flowStopped = cb
}

// LoadGraph implements Host.
func (p *TestProxy) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	g, ok := p.graphs[workflowURL]
	if !ok {
		return nil, errors.New("graph not found: %s", workflowURL, http.StatusNotFound)
	}
	return g, nil
}

// ExecuteTask implements Host.
func (p *TestProxy) ExecuteTask(ctx context.Context, taskURL string, flow *workflow.Flow) error {
	p.mu.RLock()
	h, ok := p.tasks[taskURL]
	p.mu.RUnlock()
	if !ok {
		return errors.New("task not found: %s", taskURL, http.StatusNotFound)
	}
	return h(ctx, flow)
}

// FlowStopped implements Host; it invokes the callback set via OnFlowStopped, if any.
func (p *TestProxy) FlowStopped(ctx context.Context, outcome *workflow.FlowOutcome) {
	p.mu.RLock()
	cb := p.flowStopped
	p.mu.RUnlock()
	if cb != nil {
		cb(ctx, outcome)
	}
}

// AddPeer registers a peer engine that SignalPeers relays to, standing in for the bus in a
// single-process multi-replica test. Add every OTHER replica's engine (not this proxy's own), so the
// engine's self-exclusion contract holds. Call before Startup/RunInTest.
func (p *TestProxy) AddPeer(peer *Engine) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = append(p.peers, peer)
}

// SignalPeers implements Host; it relays the signal to every peer engine registered with AddPeer. With
// no peers (the default) it is a single-replica no-op. The relay is async to mirror bus semantics and
// avoid synchronous reentrancy into a peer mid-processStep.
func (p *TestProxy) SignalPeers(ctx context.Context, op string, payload []byte) {
	p.mu.RLock()
	peers := p.peers
	p.mu.RUnlock()
	for _, peer := range peers {
		go peer.DeliverSignal(context.WithoutCancel(ctx), op, payload)
	}
}
