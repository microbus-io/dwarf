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
	"bytes"
	"encoding/json"
	"math/big"
	"reflect"
	"strings"
)

// isCleared reports whether a stored field is a cleared slot - the delete tombstone flow.Del and
// Set(name, nil) write, and the form a JSON null takes coming from a column or off the wire.
//
// A tombstone has two spellings and both must match: a Go nil (what deleteOne writes) and the bytes `null`
// (what a delete read back from a column holds). encoding/json marshals a nil json.RawMessage as `null`, so
// the two are one value; a check that missed either would make a delete silently stop taking effect.
func isCleared(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
}

// eachJSONField calls fn with the name and value of every field of struct v that encoding/json would marshal,
// named and chosen by its rules: a tagged field under its tag's name and an untagged exported one under its Go
// name, the fields of an untagged embedded struct promoted to the outer level, and among fields sharing a name
// the shallowest wins, then a tagged one over an untagged one; a tie past that is dropped, as encoding/json drops
// it. A field tagged "-", an unexported field and a nil embedded pointer are skipped. quoted reports the ",string"
// option, which encodes a scalar as a JSON string.
func eachJSONField(v reflect.Value, fn func(name string, fv reflect.Value, quoted bool) error) error {
	type candidate struct {
		fv     reflect.Value
		depth  int
		tagged bool
		quoted bool
	}
	byName := map[string][]candidate{}
	var order []string
	var walk func(v reflect.Value, depth int)
	walk = func(v reflect.Value, depth int) {
		t := v.Type()
		for i := range t.NumField() {
			field := t.Field(i)
			tag := field.Tag.Get("json")
			if tag == "-" {
				continue
			}
			name, opts, _ := strings.Cut(tag, ",")
			fv := v.Field(i)
			if field.Anonymous && name == "" {
				ft := field.Type
				if ft.Kind() == reflect.Pointer {
					if fv.IsNil() {
						continue
					}
					fv, ft = fv.Elem(), ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					walk(fv, depth+1)
					continue
				}
			}
			if !field.IsExported() {
				continue
			}
			tagged := name != ""
			if !tagged {
				name = field.Name
			}
			if _, seen := byName[name]; !seen {
				order = append(order, name)
			}
			quoted := false
			for opt := range strings.SplitSeq(opts, ",") {
				quoted = quoted || opt == "string"
			}
			byName[name] = append(byName[name], candidate{fv: fv, depth: depth, tagged: tagged, quoted: quoted})
		}
	}
	walk(v, 0)
	for _, name := range order {
		cands := byName[name]
		best := cands[0].depth
		for _, c := range cands {
			best = min(best, c.depth)
		}
		var shallow, tagged []candidate
		for _, c := range cands {
			if c.depth == best {
				shallow = append(shallow, c)
				if c.tagged {
					tagged = append(tagged, c)
				}
			}
		}
		var pick candidate
		switch {
		case len(tagged) == 1:
			pick = tagged[0]
		case len(tagged) == 0 && len(shallow) == 1:
			pick = shallow[0]
		default:
			continue // ambiguous
		}
		if err := fn(name, pick.fv, pick.quoted); err != nil {
			return err
		}
	}
	return nil
}

// jsonEqual reports whether two JSON encodings hold the same value. Equal bytes short-circuit; otherwise
// both are decoded and compared, because a value read back from a database column may be re-spelled
// (PostgreSQL's JSONB and MySQL's JSON add whitespace, reorder object keys, and may spell a number
// differently). Numbers compare by exact value - as rationals, not through a float64 - so 1e2 equals 100 while
// two integers beyond 2^53 that differ in their last digit do not.
func jsonEqual(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	decode := func(data []byte) (any, bool) {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		var v any
		if dec.Decode(&v) != nil {
			return nil, false
		}
		return exactNumbers(v), true
	}
	va, okA := decode(a)
	vb, okB := decode(b)
	return okA && okB && reflect.DeepEqual(va, vb)
}

// exactNumber is a number's exact rational spelling, typed apart from a string so a number never equals one.
type exactNumber string

// exactNumbers replaces every json.Number in a decoded value with its exactNumber, so two spellings of one
// number compare equal.
func exactNumbers(v any) any {
	switch x := v.(type) {
	case json.Number:
		if r, ok := new(big.Rat).SetString(string(x)); ok {
			return exactNumber(r.RatString())
		}
		return exactNumber(x)
	case map[string]any:
		for k, e := range x {
			x[k] = exactNumbers(e)
		}
	case []any:
		for i, e := range x {
			x[i] = exactNumbers(e)
		}
	}
	return v
}
