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

package staterefs

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/microbus-io/errors"
)

// Refs maps a state field name to where its bytes physically live.
type Refs map[string]Ref

// The columns a ref can name. ColAny reads the anchor's changes (a field it overwrote first), else its state:
// the lookup for a whole field, which a step that rewrote it holds in changes and one that carried it holds in
// state. A range names its column, because a step can hold one field in both.
const (
	ColAny     byte = 0
	ColState   byte = 's'
	ColChanges byte = 'c'
)

// Span is a Go-style slice of an array's elements, [From:To). A negative To runs to the end.
type Span struct {
	From int
	To   int
}

// Ref locates one field's bytes.
//
// A plain ref is the field of the same name at Anchor. Field names a different field there (a forEach source
// array), Path indexes into it one level at a time, outermost first, and Span takes a range of elements at the
// last level. Col picks the column.
//
// A list ref (List non-nil) is an array made of its entries, concatenated in order: an entry with a Span adds
// those elements, one without adds its value as one element.
type Ref struct {
	Anchor int    // the step whose row holds the bytes
	Col    byte   // ColAny, ColState or ColChanges
	Field  string // the field name at the anchor; empty means the refs key itself
	Path   []int  // indices into that field; empty means the whole field
	Span   *Span  // a range of elements at the end of Path
	List   []Ref  // a list ref's entries
}

// field is the name the ref's bytes have at the anchor, given the refs key it is stored under.
func (r Ref) field(key string) string {
	if r.Field != "" {
		return r.Field
	}
	return key
}

// count is how many elements a list entry adds, and false when its range runs to an end only the bytes know.
func (r Ref) count() (int, bool) {
	switch {
	case r.Span == nil:
		return 1, true
	case r.Span.To < 0:
		return 0, false
	default:
		return r.Span.To - r.Span.From, true
	}
}

// Remap returns the ref with every anchor it names passed through m. It is false, with the anchor m could not
// map, when one fails.
func (r Ref) Remap(m func(anchor int) (int, bool)) (Ref, int, bool) {
	if r.List == nil {
		to, ok := m(r.Anchor)
		if !ok {
			return Ref{}, r.Anchor, false
		}
		r.Anchor = to
		return r, 0, true
	}
	list := make([]Ref, len(r.List))
	for i, e := range r.List {
		to, ok := m(e.Anchor)
		if !ok {
			return Ref{}, e.Anchor, false
		}
		e.Anchor = to
		list[i] = e
	}
	r.List = list
	return r, 0, true
}

// MarshalJSON writes a plain ref as a bare number and any other as a string. One grammar covers every form:
// "<anchor><col> <field>[i][j]" for one ref, with any part but the anchor optional and "[a:b]" for a range,
// and entries joined by ';' for a list - a list of one entry ends with ';'. The number form is the only one a
// plain ref ever takes, so a refs column holding only plain refs is readable by a reader that knows nothing
// else.
func (r Ref) MarshalJSON() ([]byte, error) {
	if r.List == nil && r.Col == ColAny && r.Field == "" && len(r.Path) == 0 && r.Span == nil {
		return strconv.AppendInt(nil, int64(r.Anchor), 10), nil
	}
	var b []byte
	if r.List != nil {
		for i, e := range r.List {
			if i > 0 {
				b = append(b, ';')
			}
			b = e.appendEntry(b)
		}
		if len(r.List) == 1 {
			b = append(b, ';')
		}
	} else {
		b = r.appendEntry(b)
	}
	return json.Marshal(string(b))
}

// appendEntry appends one ref of the string form, without list separators.
func (r Ref) appendEntry(b []byte) []byte {
	if r.Anchor > 0 {
		b = strconv.AppendInt(b, int64(r.Anchor), 10)
	}
	if r.Col != ColAny {
		b = append(b, r.Col)
	}
	if r.Field != "" {
		b = append(b, ' ')
		b = append(b, r.Field...)
	}
	for _, i := range r.Path {
		b = append(b, '[')
		b = strconv.AppendInt(b, int64(i), 10)
		b = append(b, ']')
	}
	if r.Span != nil {
		b = append(b, '[')
		if r.Span.From > 0 {
			b = strconv.AppendInt(b, int64(r.Span.From), 10)
		}
		b = append(b, ':')
		if r.Span.To >= 0 {
			b = strconv.AppendInt(b, int64(r.Span.To), 10)
		}
		b = append(b, ']')
	}
	return b
}

// UnmarshalJSON reads any form MarshalJSON writes. A string holding a ';' is a list; any other is one ref.
func (r *Ref) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || data[0] != '"' {
		var anchor int
		if err := json.Unmarshal(data, &anchor); err != nil {
			return errors.Trace(err)
		}
		*r = Ref{Anchor: anchor}
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return errors.Trace(err)
	}
	if !strings.Contains(s, ";") {
		e, err := parseEntry(s)
		if err != nil {
			return errors.Trace(err)
		}
		*r = e
		return nil
	}
	parts := strings.Split(s, ";")
	if parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	list := make([]Ref, 0, len(parts))
	for _, part := range parts {
		e, err := parseEntry(part)
		if err != nil {
			return errors.New("malformed state ref %q: %v", s, err)
		}
		list = append(list, e)
	}
	if len(list) == 0 {
		return errors.New("malformed state ref %q", s)
	}
	*r = Ref{List: list}
	return nil
}

// parseEntry reads one ref of the string form, front to back: the anchor's digits, a column letter, a space and a
// field name running to the first '[',
// then each "[N]" as one index and a final "[a:b]" as a range. A field name cannot hold a '[', ';' or space
// (Mint names a field only for a forEach source array, which Graph.Validate requires to be an identifier), so
// the parse is unambiguous.
func parseEntry(s string) (Ref, error) {
	var r Ref
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i > 0 {
		anchor, err := strconv.Atoi(s[:i])
		if err != nil || anchor <= 0 {
			return Ref{}, errors.New("malformed state ref %q", s)
		}
		r.Anchor = anchor
	}
	if i < len(s) && (s[i] == ColState || s[i] == ColChanges) {
		r.Col = s[i]
		i++
	}
	if r.Anchor == 0 {
		return Ref{}, errors.New("malformed state ref %q", s)
	}
	rest := s[i:]
	if strings.HasPrefix(rest, " ") {
		rest = rest[1:]
		if !strings.HasPrefix(rest, "[") {
			end := strings.IndexByte(rest, '[')
			if end < 0 {
				end = len(rest)
			}
			if strings.ContainsAny(rest[:end], " ;:]") {
				return Ref{}, errors.New("malformed state ref %q", s)
			}
			r.Field = rest[:end]
			rest = rest[end:]
		}
	}
	for rest != "" {
		if rest[0] != '[' || r.Span != nil {
			return Ref{}, errors.New("malformed state ref %q", s)
		}
		num, tail, ok := strings.Cut(rest[1:], "]")
		if !ok {
			return Ref{}, errors.New("malformed state ref %q", s)
		}
		rest = tail
		if from, to, isRange := strings.Cut(num, ":"); isRange {
			span := Span{To: -1}
			var err error
			if from != "" {
				if span.From, err = strconv.Atoi(from); err != nil || span.From < 0 {
					return Ref{}, errors.New("malformed state ref %q", s)
				}
			}
			if to != "" {
				if span.To, err = strconv.Atoi(to); err != nil || span.To < span.From {
					return Ref{}, errors.New("malformed state ref %q", s)
				}
			}
			r.Span = &span
			continue
		}
		idx, err := strconv.Atoi(num)
		if err != nil || idx < 0 {
			return Ref{}, errors.New("malformed state ref %q", s)
		}
		r.Path = append(r.Path, idx)
	}
	return r, nil
}

// entryAt is the item ref to element index of a list stored under key, pointing at the row the entry holding
// it names. It is false past an entry whose length the ref does not state.
func (r Ref) entryAt(key string, index int) (Ref, bool) {
	for _, e := range r.List {
		c, ok := e.count()
		if !ok {
			return Ref{}, false
		}
		if index >= c {
			index -= c
			continue
		}
		ref := Ref{Anchor: e.Anchor, Col: e.Col, Field: e.field(key), Path: slices.Clone(e.Path)}
		if e.Span != nil {
			ref.Path = append(ref.Path, e.Span.From+index)
		}
		return ref, true
	}
	return Ref{}, false
}

// unresolvable marks an error that follows from stored data alone: a ref the rows cannot satisfy, or a column that
// cannot be read. Reading again gives the same error, unlike a failure of the read itself.
type unresolvable struct{ error }

func (u unresolvable) Unwrap() error { return u.error }

// unresolvableError is errors.New for an error that follows from stored data alone.
func unresolvableError(pattern string, args ...any) error {
	return unresolvable{errors.New(pattern, args...)}
}

// IsUnresolvable reports whether err follows from the stored data alone - a ref to a step that does not exist, a
// field or element that is not where a ref says, a refs or state column that cannot be read - rather than from a
// failure to read it. Reading again gives the same error, so a caller that retries on errors should not retry it.
func IsUnresolvable(err error) bool {
	var u unresolvable
	return errors.As(err, &u)
}

// Parse decodes a refs column. An empty or absent value yields no refs and allocates nothing, which is the
// overwhelmingly common case.
//
// A value it cannot decode is an error, never "no refs": the step's state is then missing every field the
// column points at, and running on without them loses them silently - for good, once the next mint writes a
// successor without them. Such a value is either a bug-state row or one written by a newer version in a
// format this one does not know.
func Parse(refsJSON []byte) (Refs, error) {
	refsJSON = bytes.TrimSpace(refsJSON)
	if len(refsJSON) == 0 || bytes.Equal(refsJSON, []byte("{}")) {
		return nil, nil
	}
	var refs Refs
	if err := json.Unmarshal(refsJSON, &refs); err != nil {
		return nil, unresolvableError("unreadable state refs (written in a format this version does not know?): %v", err)
	}
	if len(refs) == 0 {
		return nil, nil
	}
	return refs, nil
}
