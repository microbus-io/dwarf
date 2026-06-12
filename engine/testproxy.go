/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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

// TaskHandler is the signature for a test task handler.
type TaskHandler func(ctx context.Context, flow *workflow.Flow, metadata map[string]any) error

// TestProxy routes graph fetches and task dispatches to registered handlers.
// It implements both GraphLoader and TaskExecutor for use with Engine.RunInTest.
type TestProxy struct {
	mu     sync.RWMutex
	graphs map[string]*workflow.Graph
	tasks  map[string]TaskHandler
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

// LoadGraph implements the GraphLoader signature.
func (p *TestProxy) LoadGraph(ctx context.Context, workflowName string, metadata map[string]any) (*workflow.Graph, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	g, ok := p.graphs[workflowName]
	if !ok {
		return nil, errors.New("graph not found: %s", workflowName, http.StatusNotFound)
	}
	return g, nil
}

// ExecuteTask implements the TaskExecutor signature.
func (p *TestProxy) ExecuteTask(ctx context.Context, taskName string, flow *workflow.Flow, metadata map[string]any) error {
	p.mu.RLock()
	h, ok := p.tasks[taskName]
	p.mu.RUnlock()
	if !ok {
		return errors.New("task not found: %s", taskName, http.StatusNotFound)
	}
	return h(ctx, flow, metadata)
}
