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

package workflow

import (
	"encoding/json"
	"testing"

	"github.com/microbus-io/testarossa"
)

// added returns Changes holding one addition of elems to "f", as the orchestrator records it.
func added(elems string) Changes {
	c := NewChanges()
	c.Additions().SetJSON("f", json.RawMessage(elems))
	return c
}

// TestChanges_AdditionsApplyAndCompose pins what an addition means wherever Changes are folded: applied onto the
// input it was recorded against it gives the value written, it survives a JSON round trip under the reserved
// suffix, and composing writes after it follows the table in the godoc of add and setPlain.
func TestChanges_AdditionsApplyAndCompose(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := added(`[3]`)
	in, _ := NewState(json.RawMessage(`{"f":[1,2]}`))
	assert.NoError(in.Apply(c, nil))
	assert.Equal(`[1,2,3]`, string(in.GetJSON("f")))

	data, err := json.Marshal(c)
	assert.NoError(err)
	assert.Equal(`{"f+":[3]}`, string(data))
	var back Changes
	assert.NoError(json.Unmarshal(data, &back))
	assert.Equal(`[3]`, string(back.Additions().GetJSON("f")))
	assert.Equal(1, back.Len())
	assert.True(back.Mentions("f"))

	// Addition then addition: one addition of both.
	c = added(`[3]`)
	assert.NoError(c.Compose(added(`[4]`), nil))
	assert.Equal(`[3,4]`, string(c.Additions().GetJSON("f")))
	// Addition then replace: the replace.
	c = added(`[3]`)
	later := NewChanges()
	assert.NoError(later.Set("f", []int{9}, nil))
	assert.NoError(c.Compose(later, nil))
	assert.False(c.Additions().Has("f"))
	assert.Equal(`[9]`, string(c.Contributions().GetJSON("f")))
	// Replace then addition: the replace, extended.
	c = NewChanges()
	assert.NoError(c.Set("f", []int{7}, nil))
	assert.NoError(c.Compose(added(`[8]`), nil))
	assert.Equal(`[7,8]`, string(c.Contributions().GetJSON("f")))
	assert.False(c.Additions().Has("f"))
	// Addition then overwrite: the overwrite.
	c = added(`[3]`)
	assert.NoError(c.Overwrite("f", nil))
	assert.False(c.Additions().Has("f"))
	assert.True(c.IsDeleted("f"))
}

// TestChanges_AdditionSuffixIsReserved pins that a task cannot write a name ending with the addition suffix: it
// would read back as an addition to another field.
func TestChanges_AdditionSuffixIsReserved(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := NewChanges()
	assert.Error(c.Set("x+", 1, nil))
	assert.Error(c.Overwrite("x+", 1))
	var f Flow
	assert.Error(f.Set("x+", 1))
	assert.Equal(0, c.Len())
}
