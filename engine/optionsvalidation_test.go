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
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestCreate_RejectsNegativeOptions pins that Create rejects a negative Priority, FairnessWeight, or
// TimeBudget with a 400 rather than silently coercing it to the engine default (the pre-fix behavior, which
// hid a caller bug). 0 still means "use the default" for all three, so a zero-valued FlowOptions and a nil
// FlowOptions both create fine.
func TestCreate_RejectsNegativeOptions(t *testing.T) {
	t.Parallel()
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

	// Negative time budget -> 400. Silently coerced to the engine default before the fix.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{TimeBudget: -time.Second})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// A POSITIVE budget below 1ms is rejected too, and this is the case worth pinning: the budget is
	// persisted in milliseconds, so 500us truncates to a stored 0 - and a step with a 0 budget is dispatched
	// with a context deadline that has already passed, failing every attempt instantly. Truncation makes a
	// value that looks valid behave exactly like the negative one.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{TimeBudget: 500 * time.Microsecond})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// Exactly 1ms is the floor, and is accepted.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{TimeBudget: time.Millisecond})
	assert.NoError(err)

	// Zero values (the "unset -> default" sentinel) and nil options are accepted.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{Priority: 0, FairnessWeight: 0, TimeBudget: 0})
	assert.NoError(err)
	_, err = e.Create(ctx, "fopt/g", nil, nil)
	assert.NoError(err)
}

// SetTimeBudget is the engine-wide default, so 0 cannot mean "unset" the way it does on FlowOptions - this
// setter IS the default. A non-positive (or sub-millisecond, hence truncating-to-zero) value would hand
// EVERY new flow a task deadline that has already passed, silently and engine-wide, so it is rejected.
func TestSetTimeBudget_RejectsNonPositive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngine()

	assert.Error(e.SetTimeBudget(0))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetTimeBudget(0)))
	assert.Error(e.SetTimeBudget(-time.Minute))
	assert.Error(e.SetTimeBudget(500*time.Microsecond), "sub-millisecond truncates to a zero budget")

	assert.NoError(e.SetTimeBudget(time.Millisecond))
	assert.NoError(e.SetTimeBudget(30 * time.Second))

	// A rejected value leaves the previous default intact rather than half-applying.
	assert.NoError(e.SetTimeBudget(30 * time.Second))
	assert.Error(e.SetTimeBudget(-1))
	assert.Equal(30*time.Second, e.taskTimeBudget())
}
