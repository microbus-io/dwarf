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
	"maps"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/errors"
)

// Reducer names how writes to a state field combine: on a field bound to one with graph.SetReducer, a write is a
// contribution, folded with what the field holds (see State.Apply). A name other than the constants below is
// rejected by Graph.Validate.
type Reducer string

const (
	ReducerReplace Reducer = "replace" // Last write wins (default)
	ReducerAppend  Reducer = "append"  // Concatenate arrays
	ReducerAdd     Reducer = "add"     // Sum numeric values
	ReducerMin     Reducer = "min"     // Smaller of two numeric values
	ReducerMax     Reducer = "max"     // Larger of two numeric values
	ReducerUnion   Reducer = "union"   // Merge arrays, deduplicate (numbers compare by value)
	ReducerMerge   Reducer = "merge"   // Merge objects, new key wins
	ReducerAnd     Reducer = "and"     // Logical AND of booleans
	ReducerOr      Reducer = "or"      // Logical OR of booleans
	ReducerConcat  Reducer = "concat"  // Concatenate strings
)

// isKnown reports whether r names one of the reducers above. The empty Reducer means replace.
func (r Reducer) isKnown() bool {
	switch r {
	case "", ReducerReplace, ReducerAppend, ReducerAdd, ReducerMin, ReducerMax, ReducerUnion, ReducerMerge,
		ReducerAnd, ReducerOr, ReducerConcat:
		return true
	}
	return false
}

// The reducer functions below are the Go-native fold implementations selected by the Reducer string type
// (the vocabulary the graph produces and State.Apply consumes). They are unexported: a caller names
// a reducer with a Reducer constant, never by calling the function directly. Each operates on the Go
// values (no JSON round trip) and treats a nil base - an absent field on the accumulating State - as
// "seed from incoming", letting the field appear for the first time. None mutates its arguments; results
// are freshly allocated.
//
// Replace (and the empty Reducer) is handled by State.Apply itself, not here: it needs no base, and a null
// incoming there is a delete, which Reduce has no way to express.

// Reduce folds incoming onto base using the strategy named by r, on Go values. State.Apply folds stored JSON
// fields to the same result as Reduce over their decoded values ([]any, map[string]any, json.Number, string,
// bool; a merge's maps hold json.RawMessage values).
// Over other Go types the two can differ: under append, a typed slice such as []int added to a []any base is
// one element, where a JSON array is spread. The empty and Replace reducers return incoming.
func (r Reducer) Reduce(base any, incoming any) (result any, err error) {
	switch r {
	case ReducerAppend:
		return doAppend(base, incoming)
	case ReducerUnion:
		return doUnion(base, incoming)
	case ReducerAdd:
		return doAdd(base, incoming)
	case ReducerMin:
		return doMin(base, incoming)
	case ReducerMax:
		return doMax(base, incoming)
	case ReducerAnd:
		return doAnd(base, incoming)
	case ReducerOr:
		return doOr(base, incoming)
	case ReducerConcat:
		return doConcat(base, incoming)
	case ReducerMerge:
		return doMerge(base, incoming)
	case ReducerReplace, "":
		return incoming, nil
	default:
		return nil, errors.New("unknown reducer: %s", string(r))
	}
}

// doAppend appends incoming onto base's slice and returns the combined slice.
//
// A nil base seeds from incoming: a slice/array incoming becomes a fresh copy of itself, and a
// single-value incoming becomes a one-element slice of its type.
//
// Otherwise base must be a slice or array, and incoming may be either:
//   - a slice/array whose elements are appended individually (a spread), or
//   - a single value assignable to base's element type, appended as one element.
//
// When incoming could be read either way - it is a slice/array of assignable elements yet also a valid
// single element of base (e.g. a []int into a []any base) - it is appended as a single element, since a
// list-typed field of base is more specific than a spread. The result is a newly allocated slice; base
// is never modified.
func doAppend(base any, incoming any) (any, error) {
	return appendInto("append", base, incoming, false)
}

// doUnion appends incoming onto base's slice like doAppend, but skips any element already present, yielding a
// set-like slice. Elements compare by value (unionKey): equal numbers match however they are spelled. base's own
// elements are preserved as-is; only newly added elements are deduplicated, against everything accumulated so far.
func doUnion(base any, incoming any) (any, error) {
	return appendInto("union", base, incoming, true)
}

// appendInto is the shared core of doAppend and doUnion. It builds a fresh slice from base (or, for a nil
// base, an empty slice typed from incoming), then adds incoming's element(s) - spread if incoming is a
// list of base's elements, otherwise as a single element. With dedupe set, an element equal to one
// already in the result is skipped.
func appendInto(name string, base any, incoming any, dedupe bool) (any, error) {
	if incoming == nil {
		return nil, errors.New("%s reducer: incoming must not be nil", name)
	}
	incVal := reflect.ValueOf(incoming)
	incIsList := incVal.Kind() == reflect.Slice || incVal.Kind() == reflect.Array

	var elemType reflect.Type
	var baseVal reflect.Value
	hasBase := base != nil
	if hasBase {
		baseVal = reflect.ValueOf(base)
		if k := baseVal.Kind(); k != reflect.Slice && k != reflect.Array {
			return nil, errors.New("%s reducer: base must be a slice or array, got %T", name, base)
		}
		elemType = baseVal.Type().Elem()
	} else if incIsList {
		elemType = incVal.Type().Elem()
	} else {
		elemType = incVal.Type()
	}

	capHint := 1
	if incIsList {
		capHint = incVal.Len()
	}
	if hasBase {
		capHint += baseVal.Len()
	}
	out := reflect.MakeSlice(reflect.SliceOf(elemType), 0, capHint)
	// keys holds each element's unionKey, computed once, so the quadratic dedupe compares prepared values.
	var keys []any
	if hasBase {
		for i := range baseVal.Len() {
			out = reflect.Append(out, baseVal.Index(i))
			if dedupe {
				keys = append(keys, unionKey(baseVal.Index(i).Interface()))
			}
		}
	}

	add := func(v reflect.Value) {
		if dedupe {
			key := unionKey(v.Interface())
			for _, k := range keys {
				if reflect.DeepEqual(k, key) {
					return
				}
			}
			keys = append(keys, key)
		}
		out = reflect.Append(out, v)
	}

	// Spread when incoming is a list of base's elements: its type is exactly base's slice type, or (for a
	// differently-shaped list) its elements are assignable to base's element type while incoming is not
	// itself a valid single element of base - the single-element reading wins that tie. With no base, a
	// list incoming always spreads (there is nothing to disambiguate against).
	spread := incIsList
	if incIsList && hasBase {
		incType := incVal.Type()
		spread = incType == reflect.SliceOf(elemType) ||
			(incType.Elem().AssignableTo(elemType) && !incType.AssignableTo(elemType))
	}
	if spread {
		for i := range incVal.Len() {
			add(incVal.Index(i))
		}
		return out.Interface(), nil
	}
	if incVal.Type().AssignableTo(elemType) {
		add(incVal)
		return out.Interface(), nil
	}
	return nil, errors.New("%s reducer: incoming %T is neither a slice of %s nor a single %s", name, incoming, elemType, elemType)
}

// unionNumber is a number's value in unionKey: a distinct type, so the number 1 never matches the string "1".
type unionNumber string

// unionKey is what union compares an element by: the element with every number in it, at any depth, replaced by
// its value. A fold decodes numbers as json.Number, and comparing those by literal would let the database's
// respelling split one value in two - Postgres writes 1e-7 back as 0.0000001 and 1e+21 in full - while Go writes
// a float64 past 1e21 or under 1e-6 in exponent form. Other values compare as decoded.
func unionKey(v any) any {
	switch v := v.(type) {
	case json.Number:
		return unionNumber(numberValueKey(string(v)))
	case float64:
		return unionNumber(numberValueKey(strconv.FormatFloat(v, 'g', -1, 64)))
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			out[k] = unionKey(e)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = unionKey(e)
		}
		return out
	}
	return v
}

// unionFold is ReducerUnion carried across a whole ApplyAll: one accumulated list and the set of its elements'
// keys, so each incoming element costs one lookup. Folding member by member through doUnion instead copies the
// whole list and compares against every element on every member - quadratic in a wide fan-in, inside its
// transaction (measured: 4,000 one-element branches took 8.2s). doUnion stays the reducer of record; the two
// agree element for element, and a value unionKeyString cannot key sends the field back to doUnion.
type unionFold struct {
	list []any
	seen map[string]struct{}
}

// newUnionFold starts a fold onto base, keeping base's own elements as they are, duplicates included, as doUnion
// does. ok is false when base is not a decoded JSON list.
func newUnionFold(base any) (u *unionFold, ok bool) {
	list, isList := base.([]any)
	if !isList {
		return nil, false
	}
	u = &unionFold{list: slices.Clone(list), seen: make(map[string]struct{}, len(list))}
	for _, e := range list {
		k, ok := unionKeyString(e)
		if !ok {
			return nil, false
		}
		u.seen[k] = struct{}{}
	}
	return u, true
}

// add folds one decoded contribution in as doUnion would onto a []any base: a list spreads, any other value is
// one element, and an element whose key is already present is skipped. ok is false, with the fold unchanged, when
// incoming is nil or holds a value unionKeyString cannot key.
func (u *unionFold) add(incoming any) (ok bool) {
	if incoming == nil {
		return false
	}
	elems, isList := incoming.([]any)
	if !isList {
		elems = []any{incoming}
	}
	keys := make([]string, len(elems))
	for i, e := range elems {
		k, ok := unionKeyString(e)
		if !ok {
			return false
		}
		keys[i] = k
	}
	for i, e := range elems {
		if _, dup := u.seen[keys[i]]; dup {
			continue
		}
		u.seen[keys[i]] = struct{}{}
		u.list = append(u.list, e)
	}
	return true
}

// unionKeyString spells unionKey as a string, so it can key a map: two decoded JSON values have the same spelling
// exactly when their unionKeys are DeepEqual. Every value is tagged by its kind, so the number 1 and the string "1"
// differ, and every string and number is quoted, so no spelling is a prefix of another. ok is false for a value a
// JSON decode does not yield.
func unionKeyString(v any) (key string, ok bool) {
	var b strings.Builder
	ok = writeUnionKey(&b, v)
	return b.String(), ok
}

func writeUnionKey(b *strings.Builder, v any) bool {
	switch v := v.(type) {
	case nil:
		b.WriteByte('z')
	case bool:
		if v {
			b.WriteByte('t')
		} else {
			b.WriteByte('f')
		}
	case json.Number:
		b.WriteByte('n')
		b.WriteString(strconv.Quote(numberValueKey(string(v))))
	case float64:
		b.WriteByte('n')
		b.WriteString(strconv.Quote(numberValueKey(strconv.FormatFloat(v, 'g', -1, 64))))
	case string:
		b.WriteByte('s')
		b.WriteString(strconv.Quote(v))
	case []any:
		b.WriteByte('[')
		for i, e := range v {
			if i > 0 {
				b.WriteByte(',')
			}
			if !writeUnionKey(b, e) {
				return false
			}
		}
		b.WriteByte(']')
	case map[string]any:
		b.WriteByte('{')
		for i, k := range slices.Sorted(maps.Keys(v)) {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Quote(k))
			b.WriteByte(':')
			if !writeUnionKey(b, v[k]) {
				return false
			}
		}
		b.WriteByte('}')
	default:
		return false
	}
	return true
}

// numberValueKey spells a JSON number by its value: an integer literal as its exact integer, so ids past 2^53 stay
// distinct; any other number by its float64 in Go's shortest form - in full when it is whole, so 1, 1.0 and 1e0
// match, and 1e23 matches the 100000000000000000000000 a database respells it as. A whole float is not keyed by
// its exact binary integer: past 2^53 that differs from the decimal every writer spells (1e23 is
// 99999999999999991611392 in binary). A literal that does not parse as a finite float64 keeps its literal.
func numberValueKey(lit string) string {
	if !strings.ContainsAny(lit, ".eE") {
		if i, ok := new(big.Int).SetString(lit, 10); ok {
			return i.String()
		}
	}
	f, err := strconv.ParseFloat(lit, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
		return lit
	}
	if f == 0 {
		return "0" // -0 too
	}
	if f == math.Trunc(f) {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// doAdd returns the numeric sum base + incoming. A nil base seeds from incoming.
func doAdd(base any, incoming any) (any, error) {
	return combineNumeric("add", opAdd, base, incoming)
}

// doMin returns the smaller of base and incoming. A nil base seeds from incoming.
func doMin(base any, incoming any) (any, error) {
	return combineNumeric("min", opMin, base, incoming)
}

// doMax returns the larger of base and incoming. A nil base seeds from incoming.
func doMax(base any, incoming any) (any, error) {
	return combineNumeric("max", opMax, base, incoming)
}

type numericOp int

const (
	opAdd numericOp = iota
	opMin
	opMax
)

// realNumber is every Go integer and floating-point type (and named types over them, e.g. time.Duration),
// used to fold two same-typed numbers natively.
type realNumber interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

func numericApply[T realNumber](op numericOp, a, b T) T {
	switch op {
	case opAdd:
		return a + b
	case opMin:
		return min(a, b)
	case opMax:
		return max(a, b)
	}
	return a // unreachable
}

// combineNumeric folds two numbers with a native operation in their own type - no float64 detour, so an
// int64 past 2^53 stays exact. Two json.Number values fold as numberApply does. base and incoming must be the same Go type (a fold across types would have
// to promote to float64 and lose integer precision); a mismatch is an error. A nil base seeds from
// incoming.
func combineNumeric(name string, op numericOp, base any, incoming any) (any, error) {
	if incoming == nil {
		return nil, errors.New("%s reducer: incoming must not be nil", name)
	}
	if base == nil {
		if !isNumeric(incoming) {
			return nil, errors.New("%s reducer: incoming must be numeric, got %T", name, incoming)
		}
		return incoming, nil
	}
	if reflect.TypeOf(base) != reflect.TypeOf(incoming) {
		return nil, errors.New("%s reducer: base (%T) and incoming (%T) must be the same type", name, base, incoming)
	}
	switch b := base.(type) {
	case json.Number:
		return numberApply(name, op, b, incoming.(json.Number))
	case time.Duration:
		return numericApply(op, b, incoming.(time.Duration)), nil
	case int:
		return numericApply(op, b, incoming.(int)), nil
	case int8:
		return numericApply(op, b, incoming.(int8)), nil
	case int16:
		return numericApply(op, b, incoming.(int16)), nil
	case int32:
		return numericApply(op, b, incoming.(int32)), nil
	case int64:
		return numericApply(op, b, incoming.(int64)), nil
	case uint:
		return numericApply(op, b, incoming.(uint)), nil
	case uint8:
		return numericApply(op, b, incoming.(uint8)), nil
	case uint16:
		return numericApply(op, b, incoming.(uint16)), nil
	case uint32:
		return numericApply(op, b, incoming.(uint32)), nil
	case uint64:
		return numericApply(op, b, incoming.(uint64)), nil
	case float32:
		return numericApply(op, b, incoming.(float32)), nil
	case float64:
		return numericApply(op, b, incoming.(float64)), nil
	default:
		return nil, errors.New("%s reducer: unsupported numeric type %T", name, base)
	}
}

// numberApply folds two JSON numbers. Integers fold exactly, in arbitrary precision. Anything else folds in
// float64, as a reader decoding it into any would see it. min and max return the winning literal itself.
func numberApply(name string, op numericOp, a, b json.Number) (json.Number, error) {
	x, xInt := new(big.Int).SetString(string(a), 10)
	y, yInt := new(big.Int).SetString(string(b), 10)
	if xInt && yInt {
		switch op {
		case opAdd:
			return json.Number(x.Add(x, y).String()), nil
		case opMin:
			if y.Cmp(x) < 0 {
				return b, nil
			}
			return a, nil
		default:
			if y.Cmp(x) > 0 {
				return b, nil
			}
			return a, nil
		}
	}
	fa, err := a.Float64()
	if err != nil {
		return "", errors.New("%s reducer: %w", name, err)
	}
	fb, err := b.Float64()
	if err != nil {
		return "", errors.New("%s reducer: %w", name, err)
	}
	switch op {
	case opAdd:
		sum, err := json.Marshal(fa + fb)
		if err != nil {
			return "", errors.New("%s reducer: %w", name, err)
		}
		return json.Number(sum), nil
	case opMin:
		if fb < fa {
			return b, nil
		}
		return a, nil
	default:
		if fb > fa {
			return b, nil
		}
		return a, nil
	}
}

// isNumeric reports whether v is any Go integer or floating-point kind, or a json.Number.
func isNumeric(v any) bool {
	if _, ok := v.(json.Number); ok {
		return true
	}
	switch reflect.ValueOf(v).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// doAnd returns the logical AND of two booleans. A nil base seeds from incoming.
func doAnd(base any, incoming any) (any, error) {
	return combineBool("and", base, incoming, func(a, b bool) bool { return a && b })
}

// doOr returns the logical OR of two booleans. A nil base seeds from incoming.
func doOr(base any, incoming any) (any, error) {
	return combineBool("or", base, incoming, func(a, b bool) bool { return a || b })
}

func combineBool(name string, base any, incoming any, op func(a, b bool) bool) (any, error) {
	if incoming == nil {
		return nil, errors.New("%s reducer: incoming must not be nil", name)
	}
	ib, ok := incoming.(bool)
	if !ok {
		return nil, errors.New("%s reducer: incoming must be a bool, got %T", name, incoming)
	}
	if base == nil {
		return incoming, nil
	}
	bb, ok := base.(bool)
	if !ok {
		return nil, errors.New("%s reducer: base must be a bool, got %T", name, base)
	}
	return op(bb, ib), nil
}

// doConcat returns the string base + incoming. A nil base seeds from incoming.
func doConcat(base any, incoming any) (any, error) {
	if incoming == nil {
		return nil, errors.New("concat reducer: incoming must not be nil")
	}
	is, ok := incoming.(string)
	if !ok {
		return nil, errors.New("concat reducer: incoming must be a string, got %T", incoming)
	}
	if base == nil {
		return incoming, nil
	}
	bs, ok := base.(string)
	if !ok {
		return nil, errors.New("concat reducer: base must be a string, got %T", base)
	}
	return bs + is, nil
}

// doMerge combines two maps and returns the result, with incoming's entries winning on key collision. A
// nil base seeds from a fresh copy of incoming. Both must be maps; incoming's key and element types must
// be assignable to base's. The result is a newly allocated map; neither argument is modified.
func doMerge(base any, incoming any) (any, error) {
	if incoming == nil {
		return nil, errors.New("merge reducer: incoming must not be nil")
	}
	iv := reflect.ValueOf(incoming)
	if iv.Kind() != reflect.Map {
		return nil, errors.New("merge reducer: incoming must be a map, got %T", incoming)
	}
	if base == nil {
		out := reflect.MakeMapWithSize(iv.Type(), iv.Len())
		for _, k := range iv.MapKeys() {
			out.SetMapIndex(k, iv.MapIndex(k))
		}
		return out.Interface(), nil
	}
	bv := reflect.ValueOf(base)
	if bv.Kind() != reflect.Map {
		return nil, errors.New("merge reducer: base must be a map, got %T", base)
	}
	if !iv.Type().Key().AssignableTo(bv.Type().Key()) || !iv.Type().Elem().AssignableTo(bv.Type().Elem()) {
		return nil, errors.New("merge reducer: incoming %s is not assignable to base %s", iv.Type(), bv.Type())
	}
	out := reflect.MakeMapWithSize(bv.Type(), bv.Len()+iv.Len())
	for _, k := range bv.MapKeys() {
		out.SetMapIndex(k, bv.MapIndex(k))
	}
	for _, k := range iv.MapKeys() {
		out.SetMapIndex(k, iv.MapIndex(k))
	}
	return out.Interface(), nil
}
