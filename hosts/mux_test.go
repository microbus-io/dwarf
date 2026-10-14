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

package hosts_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// The package must not import engine - a task server imports it without the engine's database drivers - so
// the interface is asserted here, from the external test package.
var _ engine.Host = (*hosts.Mux)(nil)

func TestMux_RoutesByURL(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	graph := workflow.NewGraph("Greet")
	mux.HandleGraph("https://example.com/greet", graph)
	mux.HandleTask("https://example.com/hello", func(ctx context.Context, flow *workflow.Flow) error {
		return flow.Set("greeting", "hello")
	})

	g, err := mux.LoadGraph(ctx, "https://example.com/greet")
	if assert.NoError(err) {
		assert.Equal(graph, g)
	}
	flow := workflow.NewFlow()
	err = mux.ExecuteTask(ctx, "https://example.com/hello", flow)
	if assert.NoError(err) {
		assert.Equal("hello", flow.GetString("greeting"))
	}
}

func TestMux_UnknownURLIsNotFound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	_, err := mux.LoadGraph(ctx, "https://example.com/missing")
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	err = mux.ExecuteTask(ctx, "https://example.com/missing", workflow.NewFlow())
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}
