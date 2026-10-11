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

func TestChanges_JSON(t *testing.T) {
	assert := testarossa.For(t)
	reducers := map[string]Reducer{"log": ReducerAppend}

	c := NewChanges()
	assert.NoError(c.Set("log", []string{"m"}, reducers))
	assert.NoError(c.Overwrite("summary", "trimmed"))
	assert.NoError(c.Overwrite("scratch", nil))
	assert.NoError(c.Set("note", nil, reducers)) // a nil on a plain field deletes it

	data, err := json.Marshal(c)
	assert.NoError(err)
	assert.Equal(`{"log":["m"],"note!":null,"scratch!":null,"summary!":"trimmed"}`, string(data))

	var back Changes
	assert.NoError(json.Unmarshal(data, &back))
	assert.Equal([]string{"m"}, back.Contributions().GetStrings("log"))
	assert.Equal("trimmed", back.Overwrites().GetString("summary"))
	assert.True(back.IsDeleted("scratch"))
	assert.True(back.IsDeleted("note"))
	assert.Equal(4, back.Len())

	// A field written both ways reads as an overwrite; empty input reads as empty Changes.
	assert.NoError(json.Unmarshal([]byte(`{"k":1,"k!":2}`), &back))
	assert.Equal(1, back.Len())
	assert.Equal(2, back.Overwrites().GetInt("k"))
	for _, empty := range []string{"", "null", "{}"} {
		assert.NoError(back.UnmarshalJSON([]byte(empty)))
		assert.Equal(0, back.Len(), "%q", empty)
	}
	assert.Error(back.UnmarshalJSON([]byte(`[1]`)))
}

func TestChanges_ReservedSuffix(t *testing.T) {
	assert := testarossa.For(t)
	c := NewChanges()
	assert.Error(c.Set("x!", 1, nil))
	assert.Error(c.Overwrite("x!", 1))
	assert.Equal(0, c.Len())
}

func TestChanges_ComposeTable(t *testing.T) {
	assert := testarossa.For(t)
	reducers := map[string]Reducer{"f": ReducerAppend}
	write := func(v any, overwrite bool) Changes {
		c := NewChanges()
		if overwrite {
			assert.NoError(c.Overwrite("f", v))
		} else {
			assert.NoError(c.Set("f", v, reducers))
		}
		return c
	}
	cases := []struct {
		name          string
		earlier       Changes
		later         Changes
		want          any
		wantOverwrite bool
	}{
		{"contribution then contribution", write([]any{"a"}, false), write([]any{"b"}, false), []any{"a", "b"}, false},
		{"overwrite then contribution", write([]any{"x"}, true), write([]any{"b"}, false), []any{"x", "b"}, true},
		{"delete then contribution", write(nil, true), write([]any{"b"}, false), []any{"b"}, true},
		{"contribution then overwrite", write([]any{"a"}, false), write([]any{"y"}, true), []any{"y"}, true},
		{"contribution then delete", write([]any{"a"}, false), write(nil, true), nil, true},
	}
	for _, tc := range cases {
		assert.NoError(tc.earlier.Compose(tc.later, reducers), tc.name)
		var got any
		if tc.wantOverwrite {
			_, _ = tc.earlier.Overwrites().Get("f", &got)
		} else {
			_, _ = tc.earlier.Contributions().Get("f", &got)
		}
		assert.Equal(tc.want, got, tc.name)
		assert.Equal(tc.wantOverwrite, tc.earlier.IsOverwritten("f"), tc.name)
		assert.Equal(1, tc.earlier.Len(), "%s: a field is in one map only", tc.name)
	}
}

// TestChanges_ComposeIsApplyInTurn pins, for every reducer, that applying two Changes composed into one equals
// applying them in turn - with and without an overwrite on either side. It is what lets a step's runs, and a
// task's writes within a run, accumulate in one Changes.
func TestChanges_ComposeIsApplyInTurn(t *testing.T) {
	t.Parallel()
	values := map[Reducer][3]any{ // base, first write, second write
		ReducerReplace: {"base", "a", "b"},
		ReducerAppend:  {[]any{"x"}, []any{"a"}, "b"},
		ReducerUnion:   {[]any{"x", "a"}, []any{"a", "b"}, []any{"b", "c", "x"}},
		ReducerConcat:  {"x", "a", "b"},
		ReducerMerge:   {map[string]any{"x": 1.0, "k": 0.0}, map[string]any{"k": 1.0}, map[string]any{"k": 2.0, "y": 3.0}},
		ReducerAdd:     {10.0, 1.0, 2.0},
		ReducerMin:     {5.0, 7.0, 3.0},
		ReducerMax:     {5.0, 1.0, 9.0},
		ReducerAnd:     {true, true, false},
		ReducerOr:      {false, false, true},
	}
	for r, v := range values {
		for _, overwrite := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
			assert := testarossa.For(t)
			reducers := map[string]Reducer{"f": r}
			write := func(value any, ow bool) Changes {
				c := NewChanges()
				if ow {
					assert.NoError(c.Overwrite("f", value))
				} else {
					assert.NoError(c.Set("f", value, reducers))
				}
				return c
			}
			c1, c2 := write(v[1], overwrite[0]), write(v[2], overwrite[1])

			inTurn, _ := NewState("f", v[0])
			assert.NoError(inTurn.Apply(c1, reducers))
			assert.NoError(inTurn.Apply(c2, reducers))

			composed := write(v[1], overwrite[0])
			assert.NoError(composed.Compose(c2, reducers))
			once, _ := NewState("f", v[0])
			assert.NoError(once.Apply(composed, reducers))

			want, _ := inTurn.MarshalJSON()
			got, _ := once.MarshalJSON()
			assert.Equal(string(want), string(got), "%s overwrite=%v", r, overwrite)
		}
	}
}

// TestChanges_ComposeNormalizesAStoredValue pins that a contribution stored by an older writer - a bare value
// under append, rather than the list a fold produces - composes as Apply would read it.
func TestChanges_ComposeNormalizesAStoredValue(t *testing.T) {
	assert := testarossa.For(t)
	reducers := map[string]Reducer{"log": ReducerAppend}
	var stored Changes
	assert.NoError(json.Unmarshal([]byte(`{"log":"m"}`), &stored))
	later := NewChanges()
	assert.NoError(later.Set("log", []any{"n"}, reducers))
	assert.NoError(stored.Compose(later, reducers))
	assert.Equal([]string{"m", "n"}, stored.Contributions().GetStrings("log"))
}
