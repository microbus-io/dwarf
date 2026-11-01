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

// Package hosts provides ready-made implementations of the engine's Host interface: the seam through which
// the engine fetches workflow graphs and dispatches tasks.
//
// It depends only on the workflow package, not on the engine or its database drivers.
package hosts

import (
	"context"
	"net/http"
	"sync"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// Mux is an engine.Host that routes graph fetches and task dispatches, by URL, to graphs and Go functions
// registered on it. It suits tests, and an application whose tasks run in-process, with no transport
// between the engine and its tasks. Pass it to Engine.SetHost.
//
// A multi-replica test needs nothing else from it: replicas coordinate through the database they share,
// so several engines pointed at one test database ARE a fleet, with no relay between them to stand up.
type Mux struct {
	mu     sync.RWMutex
	graphs map[string]*workflow.Graph
	tasks  map[string]func(ctx context.Context, flow *workflow.Flow) error
}

// NewMux creates a Mux with no graphs or tasks registered.
func NewMux() *Mux {
	return &Mux{
		graphs: make(map[string]*workflow.Graph),
		tasks:  make(map[string]func(ctx context.Context, flow *workflow.Flow) error),
	}
}

// HandleGraph registers a workflow graph under workflowURL, the URL passed to Engine.Create or Engine.Run.
func (p *Mux) HandleGraph(workflowURL string, graph *workflow.Graph) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.graphs[workflowURL] = graph
}

// HandleTask registers a task handler under taskURL, the URL a graph binds to a task with SetURL. The
// handler reads the flow's baggage, if any, with workflow.BaggageFrom(ctx).
func (p *Mux) HandleTask(taskURL string, handler func(ctx context.Context, flow *workflow.Flow) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tasks[taskURL] = handler
}

// LoadGraph implements engine.Host.
func (p *Mux) LoadGraph(ctx context.Context, workflowURL string) (graph *workflow.Graph, err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	g, ok := p.graphs[workflowURL]
	if !ok {
		return nil, errors.New("graph not found: %s", workflowURL, http.StatusNotFound)
	}
	return g, nil
}

// ExecuteTask implements engine.Host.
func (p *Mux) ExecuteTask(ctx context.Context, taskURL string, flow *workflow.Flow) error {
	p.mu.RLock()
	h, ok := p.tasks[taskURL]
	p.mu.RUnlock()
	if !ok {
		return errors.New("task not found: %s", taskURL, http.StatusNotFound)
	}
	return h(ctx, flow)
}
