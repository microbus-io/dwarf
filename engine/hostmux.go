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

// HostMux is a Host that routes graph fetches and task dispatches, by URL, to graphs and Go functions
// registered on it. It suits tests, and an application whose tasks run in-process, with no transport
// between the engine and its tasks. Pass it to Engine.SetHost.
//
// A multi-replica test needs nothing else from it: replicas coordinate through the database they share,
// so several engines pointed at one test database ARE a fleet, with no relay between them to stand up.
type HostMux struct {
	mu     sync.RWMutex
	graphs map[string]*workflow.Graph
	tasks  map[string]func(ctx context.Context, flow *workflow.Flow) error
}

// NewHostMux creates a HostMux with no graphs or tasks registered.
func NewHostMux() *HostMux {
	return &HostMux{
		graphs: make(map[string]*workflow.Graph),
		tasks:  make(map[string]func(ctx context.Context, flow *workflow.Flow) error),
	}
}

// HandleGraph registers a workflow graph under the given name.
// The name should match the workflow URL passed to Engine.Create or Engine.Run.
func (p *HostMux) HandleGraph(name string, graph *workflow.Graph) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.graphs[name] = graph
}

// HandleTask registers a task handler under the given name.
// The name should match the task URL registered via graph.SetURL. The handler reads the flow's baggage,
// if any, with workflow.BaggageFrom(ctx).
func (p *HostMux) HandleTask(name string, handler func(ctx context.Context, flow *workflow.Flow) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tasks[name] = handler
}

// LoadGraph implements Host.
func (p *HostMux) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	g, ok := p.graphs[workflowURL]
	if !ok {
		return nil, errors.New("graph not found: %s", workflowURL, http.StatusNotFound)
	}
	return g, nil
}

// ExecuteTask implements Host.
func (p *HostMux) ExecuteTask(ctx context.Context, taskURL string, flow *workflow.Flow) error {
	p.mu.RLock()
	h, ok := p.tasks[taskURL]
	p.mu.RUnlock()
	if !ok {
		return errors.New("task not found: %s", taskURL, http.StatusNotFound)
	}
	return h(ctx, flow)
}
