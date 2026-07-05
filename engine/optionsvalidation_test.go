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
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestCreate_RejectsNegativeOptions pins that Create rejects a negative Priority or FairnessWeight with a 400
// rather than silently coercing it to the engine default (the pre-fix behavior, which hid a caller bug). 0
// still means "use the default" for both, so a zero-valued FlowOptions and a nil FlowOptions both create fine.
func TestCreate_RejectsNegativeOptions(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "fopt/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("fopt/g", g)
	proxy.HandleTask("fopt/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// Negative priority -> 400, no flow created.
	_, err := e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{Priority: -1})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// Negative fairness weight -> 400.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{FairnessWeight: -0.5})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// Run shares the same create path, so it rejects too.
	_, _, err = e.Run(ctx, "fopt/g", nil, &workflow.FlowOptions{Priority: -7})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// Zero values (the "unset -> default" sentinel) and nil options are accepted.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{Priority: 0, FairnessWeight: 0})
	assert.NoError(err)
	_, err = e.Create(ctx, "fopt/g", nil, nil)
	assert.NoError(err)
}
