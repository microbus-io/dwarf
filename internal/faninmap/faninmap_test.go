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

package faninmap

import (
	"encoding/json"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestFanInMap_SimpleFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := workflow.NewGraph("SimpleFanin")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", workflow.END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	assert.Equal("join", New(g).For("s"))
}

func TestFanInMap_NestedFanOutFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := workflow.NewGraph("Nested")
	g.AddTransition("s", "outer1")
	g.AddTransition("s", "outer2")
	g.AddTransition("outer1", "inner1")
	g.AddTransition("outer1", "inner2")
	g.AddTransition("inner1", "innerJoin")
	g.AddTransition("inner2", "innerJoin")
	g.AddTransition("innerJoin", "outerJoin")
	g.AddTransition("outer2", "outerJoin")
	g.AddTransition("outerJoin", workflow.END)
	g.SetFanIn("innerJoin")
	g.SetFanIn("outerJoin")
	assert.NoError(g.Validate())

	fim := New(g)
	assert.Equal("innerJoin", fim.For("outer1"))
	assert.Equal("outerJoin", fim.For("s"))
}

func TestFanInMap_ForEachThenFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := workflow.NewGraph("ForeachFanin")
	g.AddTransitionForEach("s", "a", "items", "item")
	g.AddTransition("a", "join")
	g.AddTransition("join", workflow.END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	assert.Equal("join", New(g).For("s"))
}

func TestFanInMap_ConditionalWhenFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := workflow.NewGraph("WhenFanin")
	g.AddTransitionWhen("s", "a", "x > 0")
	g.AddTransitionWhen("s", "b", "x <= 0")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", workflow.END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	assert.Equal("join", New(g).For("s"))
}

func TestFanInMap_DirectEdgeToFanIn(t *testing.T) {
	assert := testarossa.For(t)

	// One branch goes through "a", the other reaches "join" directly; both arrive at the same scope.
	g := workflow.NewGraph("DirectFanin")
	g.AddTransition("s", "a")
	g.AddTransition("s", "join")
	g.AddTransition("a", "join")
	g.AddTransition("join", workflow.END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	assert.Equal("join", New(g).For("s"))
}

func TestFanInMap_NonFanOutSourceHasNoMapping(t *testing.T) {
	assert := testarossa.For(t)

	g := workflow.NewGraph("Linear")
	g.AddTransition("a", "b")
	g.AddTransition("b", workflow.END)
	assert.NoError(g.Validate())

	assert.Equal("", New(g).For("a"))
}

func TestFanInMap_RecomputesAfterJSONRoundTrip(t *testing.T) {
	assert := testarossa.For(t)

	// The map is not persisted; the engine recomputes it from the restored definition. This pins that a
	// graph carried over the wire (definition only) still yields the same fan-in routing.
	g := workflow.NewGraph("Roundtrip")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", workflow.END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	data, err := json.Marshal(g)
	assert.NoError(err)
	var restored workflow.Graph
	assert.NoError(json.Unmarshal(data, &restored))

	assert.Equal("join", New(&restored).For("s"))
}
