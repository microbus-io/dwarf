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

// Package staterefs stores a large carried state field once instead of copying it into every step that
// merely passes it along.
//
// A step's state is a full input snapshot, so a field that is carried but not changed is re-serialized
// into every row it survives into. A field above a size bar is therefore not written into the successor's
// state at all: it is omitted, and a refs map records which step physically holds the bytes - the ANCHOR.
//
// A Linker holds the size policy for ONE shard and reads through a Cache of resolved field bytes that every
// shard's Linker shares, under one budget. It has two operations:
//
//	cache := staterefs.NewCache(1 << 30)
//	linker := staterefs.New(driver, shardIndex, cache)
//	stateJSON, refsJSON, err := linker.Mint(merged, changes, inherited, anchorID, successors, inlineOnly, item, folds)
//	materialized, err := linker.Resolve(ctx, state, refs, nil, load)
//
// Mint is the write side and performs no I/O: the caller's state is already materialized, so omitting a
// field is all there is to do. Resolve is the read side and reaches the database only through the caller's
// Loader, which is handed every anchor it needs at once. Both are safe for concurrent use. A caller holding no
// transaction resolves with ResolveShared instead, which takes a field another such call is already reading
// from that read.
//
// The encoding never escapes: Mint takes materialized state and Resolve returns materialized state, so a
// caller between the two - transition evaluation, a task carrier, an API response - only ever sees literals.
package staterefs

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/microbus-io/dwarf/internal/lru"
	"github.com/microbus-io/dwarf/workflow"
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
// those elements, one without adds its value as one element. An entry with no Anchor reads this step's own
// state, where the field holds only the elements stored inline.
type Ref struct {
	Anchor int    // the step whose row holds the bytes; 0 in a list entry means this step
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

// carried is the ref as a successor of the step anchorID stores it: a list's entries that read that step's own
// state now name it.
func (r Ref) carried(anchorID int) Ref {
	if r.List == nil {
		return r
	}
	list := make([]Ref, len(r.List))
	for i, e := range r.List {
		if e.Anchor == 0 {
			e.Anchor = anchorID
		}
		list[i] = e
	}
	r.List = list
	return r
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
		if e.Anchor > 0 {
			to, ok := m(e.Anchor)
			if !ok {
				return Ref{}, e.Anchor, false
			}
			e.Anchor = to
		}
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
		e, err := parseEntry(s, false)
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
		e, err := parseEntry(part, true)
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

// parseEntry reads one ref of the string form, front to back: the anchor's digits (absent only for a list
// entry reading this step's own state), a column letter, a space and a field name running to the first '[',
// then each "[N]" as one index and a final "[a:b]" as a range. A field name cannot hold a '[', ';' or space
// (Mint names a field only for a forEach source array, which Graph.Validate requires to be an identifier), so
// the parse is unambiguous.
func parseEntry(s string, inList bool) (Ref, error) {
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
	if r.Anchor == 0 && (!inList || r.Col != ColState) {
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
	if r.Anchor == 0 && len(r.Path) == 0 && r.Span == nil {
		return Ref{}, errors.New("malformed state ref %q", s)
	}
	return r, nil
}

// Item names a forEach branch's injected element for Mint: it is stored under Key, and it is element Index
// of the array held in field Source.
type Item struct {
	Key    string
	Source string
	Index  int
}

// Linear is the successor count of a step with one successor, for callers that would otherwise pass a
// bare 1 to Mint.
const Linear = 1

// Anchor is one anchor step's payload, as a Loader returns it. Changes shadows State for a whole-field ref;
// Refs is what lets Resolve assert that a ref is one hop rather than silently walking a chain. Each map is its
// column's JSON object as stored, so Changes names a field the step overwrote with workflow.OverwriteSuffix.
type Anchor struct {
	State   map[string]json.RawMessage
	Changes map[string]json.RawMessage
	Refs    Refs
}

// Read asks a Loader for one anchor step's payload, naming the columns a resolve needs from it. State asks for
// the state column and the refs column with it; Changes asks for the changes column. A list's entries mostly
// read a contribution from the changes column alone, and that step's state holds its own list, so reading it
// too would make a cold read of a long list cost the square of its length.
type Read struct {
	ID      int
	State   bool
	Changes bool
}

// Loader fetches anchors. It is called at most once per Resolve, with every anchor still needed after the
// cache is consulted, and must return one entry per anchor it could read, holding at least the columns asked
// for. A long list names an anchor per contributing step, so a Loader over SQL splits the ids into chunks its
// dialect's parameter limit allows.
//
// Taking the whole set at once is not an optimization: the size policy prices a whole anchor ROW (any
// number of fields living in one already-fetched row are free), which is only true if fetching k anchors
// costs one round trip rather than k.
//
// A caller metering payload bytes counts them HERE, where the untouched columns are in hand. Summing the
// decoded per-field values instead would undercount by every key, brace and separator, and would count zero
// for a column that failed to decode - wrong for a metric whose job is to track a byte-throughput ceiling.
type Loader func(ctx context.Context, reads []Read) (map[int]Anchor, error)

// Fold describes how a field under the append reducer came to its folded value, so Mint can store that value as
// a list pointing at the rows that already hold its elements instead of inline: the base the fold started from,
// then each contribution folded onto it, in fold order. The caller derives it from the fold; Mint reads no row.
type Fold struct {
	// Base is how many elements the field held before the first part. They are where the inherited ref points,
	// or, for a field held as a literal, in the minting step's own state.
	Base int
	// Parts are the contributions, in the order they were folded.
	Parts []Part
}

// Part is one contribution to a folded field, held in a step's changes.
type Part struct {
	// Step is the step whose changes hold the contribution.
	Step int
	// Overwrite reports that the part replaced the field: the base and every part before it are gone.
	Overwrite bool
	// Single reports that the contribution was added as one element rather than spread.
	Single bool
	// Added is how many elements a spread contribution holds.
	Added int
}

const (
	// floor is the LINEAR per-field candidacy bar: with one successor the bookkeeping (a refs entry, a
	// place in the resolve IN-list) is not worth it below this, and a small field costs little to copy. At
	// a fan-out it is divided by the branch count - see candidateFloor.
	floor = 1024
	// threshold is the bar for OPENING a new anchor for a single (linear) successor. It sits at the floor, far
	// below budget, because the round trip budget prices is usually not paid on a linear hop: the successor is
	// offered to the replica that minted it, and Seed has its resolve cache holding the anchored bytes. Below
	// the PostgreSQL TOAST threshold (~2KB) a field kept inline is stored twice - in this step's changes and
	// the successor's state - and every non-HOT update of the wide row re-logs it, ~4x the field in WAL.
	threshold = 1024
	// budget is the write saving a fan-out's extra resolve round trips must buy, an estimate at the eager end
	// of its range on purpose. Divided by the fan-out width, it is the bar for opening an anchor whose cost is
	// amortized over N branches - see openThreshold.
	budget = 24 * 1024
	// minField is the absolute floor no width scaling may cross. A ref replaces a field's bytes with a refs
	// entry (~20-25 bytes stored: two JEntries, the key, an integer), so a field smaller than a few times
	// that LOSES bytes on every branch. It is deliberately NOT adjusted by the dialect factor below: both
	// sides of that comparison are stored bytes, so the inflation cancels.
	minField = 64
	// postgresInflation is how much marshalled-JSON-TEXT length understates what PostgreSQL's jsonb actually
	// stores for the shape a fan-out replicates: measured 862 bytes stored for a 183-byte text array of 64
	// small ints (4.7x, rounded to 5). jsonb spends ~4 bytes per element on top of the value, so the gap is
	// widest for arrays/objects of many small scalars - exactly a forEach source array - and closes to ~1x
	// for one large string. This is an approximation with a known failure direction (string-heavy state refs
	// ~5x more eagerly than its true cost warrants), not a calibrated model: a structural estimator was
	// considered and rejected because at a fan-out the bar is already tiny (73 bytes at N=64, 18 at N=256),
	// so a 5x error moves nothing across it. That is also why it applies ONLY at n>1 - the linear bar is
	// where estimate accuracy would genuinely decide cases.
	postgresInflation = 5
	// minItem is the bar for storing a forEach element as an item ref: about three times the item ref's own
	// stored entry, which is a little larger than a plain ref's (the array's field name and the index ride
	// in it). Below it the element is cheaper inline.
	minItem = 96
	// maxAnchors is how many anchors a state may point at before its small ones are inlined, to shorten the
	// resolve IN-list on a cache miss. It is not a hard cap: an anchor at or above the bar for opening one is
	// kept however many there are (inlineExcessAnchors). The scheme is correct at any value.
	maxAnchors = 4

	// The resolved-bytes cache is bounded in bytes (given to NewCache) and in entries (cacheEntries), and an
	// entry expires once unread for cacheIdle. One cache serves every shard, so the budget goes wherever this
	// replica is dispatching. The entry cap sits high enough that bytes bind: a list caches one entry per
	// contributing step, so a long conversation of small messages is many small entries.
	cacheEntries = 1 << 20
	cacheIdle    = 5 * time.Minute
	// entryOverhead is what an entry costs beyond its bytes, so a cache of small entries is charged its real
	// memory: the LRU's list element (48) and entry (80), the map slot (~70), and the cachedField (48).
	entryOverhead = 256
	// splitOverhead is what one cached split costs beyond its element slices: its map slot and path key.
	splitOverhead = 64
)

// elemSize is the size of one element slice in a cached split. The elements alias the array's bytes, so a
// split costs its slice headers, not a second copy.
const elemSize = int64(unsafe.Sizeof(json.RawMessage(nil)))

// cacheKey identifies one ref'd field's bytes. Immutable once the anchor step settles (a terminal step is
// immutable, and a retry rewrites a step's changes only BEFORE any successor exists), so it is as good a key
// as a content hash. It carries the shard because a step id is unique only within one, and the cache is
// shared by every shard's Linker.
type cacheKey struct {
	shard  int
	anchor int
	field  string
	col    byte
}

// Cache holds resolved field bytes for every shard's Linker, under one budget. It is safe for concurrent use.
type Cache struct {
	lru *lru.Cache[cacheKey, *cachedField]
}

// NewCache creates a cache holding at most bytes of resolved field bytes, overheads included. A field larger
// than bytes is never cached, so each resolve of it reads it again.
func NewCache(bytes int64) *Cache {
	return &Cache{lru: lru.NewWeighted[cacheKey, *cachedField](max(bytes, 1), cacheEntries, cacheIdle)}
}

// Bytes reports the bytes the cache is charged for right now, overheads included.
func (c *Cache) Bytes() int64 {
	return c.lru.Weight()
}

// cachedField is one ref'd field's bytes in the resolve cache, with the splits of those bytes that item refs
// have needed. A split's elements are sub-slices of bytes, so holding the splits in the same entry is what
// lets an eviction free the bytes: a split cached on its own would keep an evicted array alive, unseen by the
// cache's byte count.
type cachedField struct {
	bytes json.RawMessage

	mu sync.Mutex
	// splits holds an item-ref'd array's elements, keyed by the path to the array ("data", then "data[3]").
	// Without it every branch of a fan-out would scan the whole array to find its one element - O(N) per
	// branch, O(N^2) per cohort - a cost a plain ref, spliced without decoding, never pays.
	splits map[string][]json.RawMessage
}

// Linker mints and resolves state refs for one shard. It is safe for concurrent use.
type Linker struct {
	driver string
	shard  int
	cache  *lru.Cache[cacheKey, *cachedField]

	// loading holds the fields a ResolveShared is reading right now. A concurrent one missing the same field
	// waits on it rather than reading the row again.
	mu      sync.Mutex
	loading map[cacheKey]*flight
}

// flight is one field a ResolveShared is reading. The reader sets entry, when the read succeeded, before
// closing done, so waiters take the field from it whether or not the cache kept it - a field larger than the
// cache, or one evicted before a waiter looks, still reaches every waiter from the one read.
type flight struct {
	done  chan struct{}
	entry *cachedField
}

// New creates a Linker for the shard with the given index and SQL driver name, caching in cache. The driver
// only corrects the size estimate at a fan-out (see inflation) and is fixed for a shard's life, which is why
// it is bound here rather than passed per call.
func New(driver string, shard int, cache *Cache) *Linker {
	return &Linker{
		driver:  driver,
		shard:   shard,
		cache:   cache.lru,
		loading: map[cacheKey]*flight{},
	}
}

// key is the cache key of a field at an anchor step of this Linker's shard, read from a column.
func (l *Linker) key(anchor int, field string, col byte) cacheKey {
	return cacheKey{l.shard, anchor, field, col}
}

// store caches one field's bytes and returns its entry. An entry already cached for the field is kept and
// returned instead, splits and all: a field's bytes are immutable, so the two hold the same value. The entry
// returned may be one the cache declined to keep (larger than the cache); the caller still serves and splits
// from it.
func (l *Linker) store(k cacheKey, value json.RawMessage) *cachedField {
	f, _ := l.cache.LoadOrStoreWeighted(k, &cachedField{bytes: value}, int64(len(value))+entryOverhead)
	return f
}

// inflation is the factor by which marshalled-JSON-text length understates this dialect's stored size.
// PostgreSQL is the ONLY dialect that needs correcting, which is a measured fact rather than a default:
// for the same 183-byte text array of 64 small ints, PostgreSQL jsonb stores 862 bytes (4.7x) while MySQL
// 8.4 binary JSON stores 197 (1.08x) and MariaDB 11.8 stores it verbatim (its JSON column is a LONGTEXT
// alias, 1.00x). The schema's other two column types cannot inflate at all - SQL Server holds
// VARBINARY(MAX) and SQLite TEXT, both the marshalled bytes as-is. PostgreSQL is the outlier because jsonb
// spends a 4-byte JEntry per element AND widens every number to arbitrary-precision numeric, so an array
// of small ints is close to its worst case; the other binary format (MySQL's) packs small integers inline
// behind variable-width offsets and stays near 1x.
//
// One consequence worth knowing: MySQL and MariaDB share the driver name "mysql" and have genuinely
// different JSON storage engines, so a factor keyed on the driver name could not tell them apart - but
// both measure ~1x, so the ambiguity is moot and no server-version probe is needed.
func (l *Linker) inflation(successors int) int {
	if successors > 1 && l.driver == "pgx" {
		return postgresInflation
	}
	return 1
}

// candidateFloor is the per-field bar for being a ref CANDIDATE at all. Once an anchor is open the marginal
// read cost of including one more field is zero (every candidate lives in that same row), so the only
// question left is whether the field outweighs its own refs entry - a test in which N CANCELS, because over
// N successors both the saving (N*size) and the cost (N*refsEntry) scale identically. That is what minField
// encodes, and above N~16 it is the whole rule; the floor/N term only shapes the narrow fan-outs in
// between, and at n=1 it reproduces the linear bar exactly.
func candidateFloor(successors int) int {
	if successors < 1 {
		successors = 1
	}
	return max(minField, floor/successors)
}

// openThreshold is the size bar for opening a NEW anchor, given how many successor steps are about to carry
// this state. Fan-out width is the primary axis, not a refinement:
//
//   - LINEAR (n=1): the bar is threshold. A resolve that misses would put break-even near budget, but the
//     successor usually resolves on the replica that minted the ref, from the cache Seed filled.
//   - FAN-OUT (n>1): the resolve cache collapses N resolves into one miss plus N-1 hits (every branch
//     resolves the SAME anchor set), while savings are S*N. Break-even becomes S*N >= budget, so the bar
//     falls as 1/N once that is below threshold (N > 24) - at N=100 even a few hundred bytes pays for itself.
//     A narrower fan-out takes the linear bar.
//
// The clamp is minField, NOT floor. Clamping at the floor made the 1/N term dead code for every fan-out
// wider than budget/floor = 24: the bar sat at a flat 1024 at width 25 and at width 10,000 alike, so the
// "at N=100 even a few hundred bytes pays" case above could never actually fire. A forEach's branch count
// IS its source array's element count, so an un-ref'd array costs N copies of an N-element array -
// quadratic, and worse again under nesting - which is what that dead clamp was silently buying.
func openThreshold(successors int, inflation int) int {
	if successors < 1 {
		successors = 1
	}
	if inflation < 1 {
		inflation = 1
	}
	return max(minField, min(threshold, budget/successors)) / inflation
}

// mentions reports whether a state holds a field AT ALL - a value, or a null. Has alone is wrong for the mint:
// a field merged to a null is still present in it. Whether a STEP rewrote a field is Changes.Mentions, which
// counts a delete too, so a deleted field's inherited ref is not carried forward.
func mentions(s workflow.State, field string) bool {
	return s.Has(field) || s.IsDeleted(field)
}

// Mint decides which of a successor step's state fields are stored by reference rather than by value. It
// returns the JSON to write into the successor's state column (the ref'd fields omitted) and into its refs
// column.
//
// merged is the successor's fully MATERIALIZED state (refs resolved at dispatch, then the task's changes
// overlaid), changes is the accumulated delta that produced it, inherited is the DISPATCHED step's own
// refs, and anchorID is that step - the only step whose row can newly anchor anything here, because every
// candidate field's bytes are either in its changes (the task just wrote them) or in its state (carried,
// and not itself a ref). That single-candidate-anchor fact is what makes the free tier free: once any one
// field opens the anchor, every other field in that row rides along at zero extra read cost.
//
// successors is how many steps are about to carry this state, and it is the primary policy axis - a ~100x
// swing in the bar between a linear hop and a wide fan-out. Pass Linear for one.
//
// inlineOnly names fields that must be stored as literals whatever their size, because their bytes are in
// NO step row: a fan-out's synthesized ordinal context, or a value only this merge produced. Ref'ing one
// would dangle. It is a distinct signal from a field appearing in changes (which only means the ref must
// not be CARRIED, and leaves the field free to anchor here).
//
// item, when non-nil, is a forEach branch's injected element. Its bytes are in no row as a field of their
// own, but they are one element of the source array, so when that array is stored by a ref the element
// is stored as an item ref into the same anchor - one copy of the array serves every branch's
// element, and resolving it costs no anchor the branch was not already reading. Otherwise it is inline.
//
// folds names the fields under the append reducer whose folded value merged holds, with how it came to be
// (Fold). Such a value exists in no row - not in the folding step's state, which predates the fold, nor any
// changes, which hold contributions - so it is never anchored here. Past the bar it becomes a list pointing at the rows
// that hold its elements; below it, or when the list cannot account for every element, it is inline.
//
// Minting needs no database read: merged was resolved at dispatch, so every literal is already in hand and
// "inlining" is simply declining to omit a field.
func (l *Linker) Mint(merged workflow.State, changes workflow.Changes, inherited Refs, anchorID int, successors int, inlineOnly map[string]bool, item *Item, folds map[string]Fold) ([]byte, []byte, error) {
	refs := Refs{}
	literal := func(k string) bool {
		return inlineOnly[k] || (item != nil && k == item.Key)
	}
	type candidate struct {
		key  string
		size int
	}
	var candidates []candidate
	// Encode fields for the state column as we go, but NEVER a carried inherited ref: it keeps its ref and
	// its (possibly large) bytes stay at the anchor, so re-serializing it here only to discard it is exactly
	// the cost state refs exists to avoid. Only fields headed for INLINE storage (or new-ref candidacy) are
	// marshalled here.
	raw := make(map[string]json.RawMessage, merged.Len())
	bar := openThreshold(successors, l.inflation(successors))
	for k := range merged.Names() {
		if f, folded := folds[k]; folded && !literal(k) {
			data := merged.GetJSON(k)
			raw[k] = data
			if len(data) >= bar {
				base, hasBase := inherited[k]
				if list, ok := foldList(f, base, hasBase, anchorID, data); ok {
					refs[k] = list
					delete(raw, k)
				}
			}
			continue
		}
		if !literal(k) {
			// A field the task did not rewrite, and which arrived as a ref, KEEPS that ref - it is never
			// re-minted against this step, whose row does not hold the bytes. This is the one-hop guard, and it
			// is unconditional precisely because the fan-out policy below is not size-based.
			if anchor, isRef := inherited[k]; isRef {
				if !changes.Mentions(k) {
					refs[k] = anchor.carried(anchorID)
					continue
				}
			}
		}
		// GetJSON, not a marshal of a decoded value: state stores fields as JSON, so sizing one costs
		// nothing and a carried payload is never expanded merely to be measured.
		data := merged.GetJSON(k)
		raw[k] = data
		if !literal(k) && len(data) >= candidateFloor(successors) {
			candidates = append(candidates, candidate{k, len(data)})
		}
	}

	// Does this step's row earn an anchor? Every candidate shares the same prospective anchor (this step), so
	// the SUM is the right test: four small fields justify one round-trip exactly as one field their size does.
	// Summing is legitimate only BECAUSE they are co-located - four fields at four different anchors would mean
	// four whole-row overfetches for the same bytes, a far worse trade, and that is never what is summed here.
	sum := 0
	for _, c := range candidates {
		sum += c.size
	}
	if sum >= bar {
		for _, c := range candidates {
			refs[c.key] = Ref{Anchor: anchorID}
			delete(raw, c.key)
		}
	}

	// Carry forward an inherited ref whose key never appeared in merged. The loop above only sees keys
	// present in merged (via raw), which is every inherited key on the paths that resolve state in FULL at
	// dispatch (linear, fan-out branch). But a fan-in resolves at most the fields its reducers fold, and a direct
	// fan-in with no members resolves none: a merely-CARRIED ref is deliberately not materialized, so its key is
	// absent from merged and the loop never carried it. Without this, that ref is neither materialized nor re-emitted and the carried field
	// is silently, permanently dropped from the fan-in step onward and from the flow's final state. A key
	// already handled above (rewritten -> re-anchored, or inline-only) is skipped; a tombstoned or
	// member-overwritten field appears in changes/inlineOnly and is correctly left dropped.
	for k, anchor := range inherited {
		if mentions(merged, k) {
			continue
		}
		if literal(k) {
			continue
		}
		if changes.Mentions(k) {
			continue
		}
		refs[k] = anchor.carried(anchorID)
	}

	if item != nil {
		mintItem(raw, refs, *item)
	}

	inlineExcessAnchors(raw, refs, merged, bar)

	stateJSON, err := json.Marshal(raw)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	refsJSON, err := json.Marshal(refs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return stateJSON, refsJSON, nil
}

// foldList builds the list a folded field is stored as: the base's elements where they already are, then each
// part in its step's changes. It declines - the field then stays inline - when the list cannot be shown to hold
// exactly the folded value's elements: a mismatch would silently drop or repeat them, so the count is checked
// against the value itself rather than trusted.
func foldList(f Fold, base Ref, hasBase bool, anchorID int, folded json.RawMessage) (Ref, bool) {
	start := 0
	for i, p := range f.Parts {
		if p.Overwrite {
			start = i
		}
	}
	var entries []Ref
	if (len(f.Parts) == 0 || !f.Parts[start].Overwrite) && f.Base > 0 {
		switch {
		case hasBase && base.List != nil:
			entries = append(entries, base.carried(anchorID).List...)
		case hasBase && base.Span == nil:
			// A whole field or an item ref at its anchor: the base is that value's elements.
			e := base
			e.Path = slices.Clone(base.Path)
			e.Span = &Span{From: 0, To: f.Base}
			entries = append(entries, e)
		case hasBase:
			return Ref{}, false
		default:
			// The field was held as a literal in the minting step's own state.
			entries = append(entries, Ref{Anchor: anchorID, Col: ColState, Span: &Span{From: 0, To: f.Base}})
		}
	}
	for _, p := range f.Parts[start:] {
		switch {
		case p.Single:
			entries = append(entries, Ref{Anchor: p.Step, Col: ColChanges})
		case p.Added > 0:
			entries = append(entries, Ref{Anchor: p.Step, Col: ColChanges, Span: &Span{From: 0, To: p.Added}})
		}
	}
	if len(entries) == 0 {
		return Ref{}, false
	}
	n := 0
	for _, e := range entries {
		c, ok := e.count()
		if !ok {
			return Ref{}, false
		}
		n += c
	}
	elems, err := splitArray(folded)
	if err != nil || len(elems) != n {
		return Ref{}, false
	}
	return Ref{List: entries}, true
}

// mintItem replaces a forEach element's literal with an item ref into its source array, when the array is
// stored by a ref and the element outweighs the ref. It runs after the array's fate is decided - carried,
// newly anchored here, or inline - because the item ref points at whichever anchor holds the array's bytes,
// never at the step merely carrying a ref to them, which would break the one-hop rule. An array stored as a
// list points the element at the entry holding it.
//
// A source that is itself an item ref - an inner forEach over the outer element, a nested array fanned out
// level by level - extends that ref's path, so every level points into the one anchor holding the whole
// nested array.
//
// The bar is flat, not scaled by the fan-out width: the array as a whole already got the width-scaled
// treatment, and each element appears in exactly one branch, so there is nothing to amortize over N.
func mintItem(raw map[string]json.RawMessage, refs Refs, item Item) {
	// forEach(items) as items leaves no array to point into.
	src, ok := refs[item.Source]
	if !ok || item.Key == item.Source {
		return
	}
	if len(raw[item.Key]) < minItem {
		return
	}
	var ref Ref
	switch {
	case src.List != nil:
		if ref, ok = src.entryAt(item.Source, item.Index); !ok {
			return
		}
	case src.Span != nil:
		return
	default:
		ref = Ref{Anchor: src.Anchor, Col: src.Col, Field: src.field(item.Source), Path: append(slices.Clone(src.Path), item.Index)}
	}
	refs[item.Key] = ref
	delete(raw, item.Key)
}

// entryAt is the item ref to element index of a list stored under key, pointing at the row the entry holding
// it names. It is false for an element in this step's own state, or past an entry whose length the ref does
// not state.
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
		if e.Anchor == 0 {
			return Ref{}, false
		}
		ref := Ref{Anchor: e.Anchor, Col: e.Col, Field: e.field(key), Path: slices.Clone(e.Path)}
		if e.Span != nil {
			ref.Path = append(ref.Path, e.Span.From+index)
		}
		return ref, true
	}
	return Ref{}, false
}

// inlineExcessAnchors inlines the smallest anchors' fields back into state as literals once the state points at
// more than maxAnchors - one copy, paid once, to shorten the resolve IN-list. It inlines only anchors whose
// fields sum below bar, the size this mint requires to open an anchor: a ref below it is not worth an anchor
// row by the policy's own pricing, while one above it is worth more than the shorter IN-list. Inlining a large
// anchor writes its bytes again; the next mint re-anchors it with that step's own write, the step after that
// inlines the then-smallest anchor, and so on - one large field rewritten at every step. And the IN-list it
// would shorten is usually not read at all, the minting replica's cache holding the anchors (Seed, and the
// dispatch resolve). It costs no database read: the literals are all present in merged
// (state was resolved at dispatch).
func inlineExcessAnchors(raw map[string]json.RawMessage, refs Refs, merged workflow.State, bar int) {
	if len(refs) == 0 {
		return
	}
	byAnchor := map[int][]string{}
	// An anchor is PINNED (un-droppable) if any of its fields' literals are absent from merged - a merely
	// carried ref whose payload this step never materialized. Inlining it would require a literal we do not
	// have, so dropping it would delete the field outright: the exact data loss the carry-forward above closes.
	// Detecting it needs only a presence check, no marshal.
	pinned := map[int]bool{}
	for k, ref := range refs {
		// A list names an anchor per contributing step by design, and its anchors are priced by the cache, not
		// by this IN-list: it neither counts nor is inlined.
		if ref.List != nil {
			continue
		}
		byAnchor[ref.Anchor] = append(byAnchor[ref.Anchor], k)
		if !mentions(merged, k) {
			pinned[ref.Anchor] = true
		}
	}
	if len(byAnchor) <= maxAnchors {
		return
	}
	// Over the count: only NOW size the droppable anchors, smallest first. This is the only path that measures
	// ref'd fields - a possibly-large carried payload is never touched in the common case above. Sizing reads
	// the raw JSON merged already holds, so it marshals nothing. Ties break on anchor id for determinism.
	bytesOf := map[int]int{}
	droppable := make([]int, 0, len(byAnchor))
	for a, keys := range byAnchor {
		if pinned[a] {
			continue
		}
		for _, k := range keys {
			bytesOf[a] += len(merged.GetJSON(k))
		}
		droppable = append(droppable, a)
	}
	sort.Slice(droppable, func(i, j int) bool {
		if bytesOf[droppable[i]] != bytesOf[droppable[j]] {
			return bytesOf[droppable[i]] < bytesOf[droppable[j]]
		}
		return droppable[i] < droppable[j]
	})
	kept := len(byAnchor)
	for _, a := range droppable {
		if kept <= maxAnchors || bytesOf[a] >= bar {
			break
		}
		for _, k := range byAnchor[a] {
			if data := merged.GetJSON(k); data != nil {
				raw[k] = data
			}
			delete(refs, k)
		}
		kept--
	}
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
		return nil, errors.New("unreadable state refs (written in a format this version does not know?): %v", err)
	}
	if len(refs) == 0 {
		return nil, nil
	}
	return refs, nil
}

// Resolve materializes ref'd fields into state, in place, and reports how many bytes of field value it
// spliced in. It is the read side of the whole design, and the reason the anchor - not the field - is
// what policy prices: the Loader fetches whole payload columns, so the cost is one row per DISTINCT anchor
// (and, on a cache miss, one round trip for all of them together), while any number of fields living in an
// already-fetched row are free.
//
// The returned count is NOT the Loader's count and the two are not interchangeable - they answer different
// questions and disagree in both directions. The Loader counts the RAW COLUMNS it scanned, which is the
// database's byte throughput: it includes the anchor's untouched other fields, and it is ZERO on a cache
// hit because nothing was read. This count is the bytes that ended up IN state, which is the caller's
// residency: it excludes the rest of the anchor row, and it is unchanged by a cache hit because the field
// is materialized either way. Meter throughput with the former and what a carrier holds with the latter.
//
// want, when non-nil, selects which refs to resolve; the rest are left for the caller to carry forward.
// That is what lets a fan-in reduce the fields it must while a large CARRIED field crosses the cohort as a
// ref (see ResolveReduced).
func (l *Linker) Resolve(ctx context.Context, state workflow.State, refs Refs, want map[string]bool, load Loader) (int, error) {
	return l.resolve(ctx, state, refs, want, load, false)
}

// ResolveShared is Resolve of every ref, for a caller that holds no transaction: a field a concurrent
// ResolveShared is already reading is taken from that read rather than read again. That is the dispatch of
// a fan-out's branches, which miss on the same anchor at the same moment. A caller inside a transaction
// must use Resolve, which never waits on another goroutine while holding its connection and locks.
func (l *Linker) ResolveShared(ctx context.Context, state workflow.State, refs Refs, load Loader) (int, error) {
	return l.resolve(ctx, state, refs, nil, load, true)
}

// Seed caches the bytes of every field a Mint anchored at anchorID, so a successor resolving them is served
// without a read. merged and changes are the state and the step's changes Mint was given, and refsJSON the
// refs it returned. It takes the plain refs pointing at anchorID, whose bytes are in merged, and a list's
// entries reading anchorID's changes, whose bytes are the contribution in changes. An inherited ref points at
// an earlier anchor and an item ref is served from its array's entry.
//
// Call it only once the anchor step's own payload has committed, and before its successors are offered for
// dispatch.
func (l *Linker) Seed(anchorID int, merged workflow.State, changes workflow.Changes, refsJSON []byte) {
	if len(refsJSON) <= 2 { // "{}"
		return
	}
	refs, err := Parse(refsJSON)
	if err != nil {
		return
	}
	for field, ref := range refs {
		if ref.List != nil {
			for _, e := range ref.List {
				if e.Anchor != anchorID || e.Col != ColChanges {
					continue
				}
				name := e.field(field)
				value := changes.Contributions().GetJSON(name)
				if changes.IsOverwritten(name) {
					value = changes.Overwrites().GetJSON(name)
				}
				if len(value) > 0 {
					l.store(l.key(anchorID, name, ColChanges), value)
				}
			}
			continue
		}
		if ref.Anchor != anchorID || len(ref.Path) > 0 || ref.Span != nil {
			continue
		}
		if value := merged.GetJSON(field); len(value) > 0 {
			l.store(l.key(anchorID, ref.field(field), ref.Col), value)
		}
	}
}

func (l *Linker) resolve(ctx context.Context, state workflow.State, refs Refs, want map[string]bool, load Loader, shared bool) (int, error) {
	if len(refs) == 0 {
		return 0, nil
	}
	if state.IsZero() {
		// Refs with nowhere to resolve into are a state that failed to load; resolving nothing would run the
		// caller on input missing every field they point at.
		return 0, errors.New("state refs given with no state to resolve them into")
	}
	values, err := l.fetch(ctx, refs, want, state.GetJSON, load, shared)
	if err != nil {
		return 0, errors.Trace(err)
	}
	materialized := 0
	for field, value := range values {
		// The anchor's bytes are spliced in AS BYTES and decoded only if something reads the field. This is
		// the case refs exist for - a large payload carried across a wide fan-out - so decoding here would
		// hand back the win at the point of collecting it: N branches resolving one anchor would produce N
		// decoded copies, live at once, of a document most of them never look at.
		//
		// Sharing one immutable byte slice across those branches is safe in a way sharing a decoded value is
		// not: bytes cannot be mutated in place by one branch's task, whereas a shared map or slice can, and
		// each branch's own read decodes into its own copy. The encoding still never escapes - every State
		// accessor materializes.
		state.SetJSON(field, value)
		materialized += len(value)
	}
	return materialized, nil
}

// fetch returns the raw bytes of every selected ref'd field, serving what it can from the cache and asking
// the Loader for the rest - once, in one call, unless shared. It is the shared core of the two read paths,
// which differ only in what they do with the bytes: Resolve decodes them into a live state map, Flatten
// splices them into a state document without decoding at all. self returns a field's value in the resolving
// step's own state column, which a list's self entries read.
//
// shared makes concurrent misses on one field read it once (see ResolveShared). Such a fetch reads the
// fields no other fetch is reading, then waits for the rest and takes them from the flight; one whose wait
// did not deliver - the other read failed - reads for itself. So it makes zero, one or two Loader calls.
func (l *Linker) fetch(ctx context.Context, refs Refs, want map[string]bool, self func(string) json.RawMessage, load Loader, shared bool) (map[string]json.RawMessage, error) {
	selected := make(map[string]Ref, len(refs))
	needed := map[cacheKey]bool{}
	for key, ref := range refs {
		if want != nil && !want[key] {
			continue
		}
		selected[key] = ref
		if ref.List == nil {
			needed[l.key(ref.Anchor, ref.field(key), ref.Col)] = true
			continue
		}
		for _, e := range ref.List {
			if e.Anchor > 0 {
				needed[l.key(e.Anchor, e.field(key), e.Col)] = true
			}
		}
	}
	entries := make(map[cacheKey]*cachedField, len(needed))
	var missing []cacheKey
	for k := range needed {
		if f, ok := l.cache.Load(k); ok {
			entries[k] = f
		} else {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		if err := l.read(ctx, missing, entries, load, shared); err != nil {
			return nil, errors.Trace(err)
		}
	}
	values := make(map[string]json.RawMessage, len(selected))
	for key, ref := range selected {
		value, err := l.value(key, ref, entries, self)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values[key] = value
	}
	return values, nil
}

// read fills entries with the missing fields, from the Loader.
func (l *Linker) read(ctx context.Context, missing []cacheKey, entries map[cacheKey]*cachedField, load Loader, shared bool) error {
	wanted := make(map[cacheKey]bool, len(missing))
	for _, k := range missing {
		wanted[k] = true
	}
	merge := func(loaded map[cacheKey]*cachedField) {
		for k, f := range loaded {
			entries[k] = f
		}
	}
	if !shared {
		loaded, err := l.loadInto(ctx, missing, wanted, load)
		merge(loaded)
		return errors.Trace(err)
	}
	// Read the claimed fields' anchors, which resolves every missing field on those rows - claimed or not - and
	// only then wait. Reading before waiting is what keeps two fetches that each claimed a field the other
	// needs from deadlocking.
	mine, theirs := l.claim(missing)
	if len(mine) > 0 {
		loaded, err := func() (loaded map[cacheKey]*cachedField, err error) {
			defer func() { l.release(mine, loaded) }()
			return l.loadInto(ctx, mine, wanted, load)
		}()
		merge(loaded)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for k, f := range theirs {
		if _, ok := entries[k]; ok {
			continue
		}
		select {
		case <-f.done:
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}
		// A waited-on field comes from its flight, which holds it even if the cache did not keep it.
		if f.entry != nil {
			entries[k] = f.entry
		}
	}
	var still []cacheKey
	for _, k := range missing {
		if _, ok := entries[k]; !ok {
			still = append(still, k)
		}
	}
	if len(still) == 0 {
		return nil
	}
	// The read waited on failed, so this fetch reads for itself. Unclaimed: another round of waiting would buy
	// nothing.
	loaded, err := l.loadInto(ctx, still, wanted, load)
	merge(loaded)
	return errors.Trace(err)
}

// claim splits the fields a fetch is missing into those it will read and those another fetch is already
// reading, returning the latter's flights.
func (l *Linker) claim(missing []cacheKey) (mine []cacheKey, theirs map[cacheKey]*flight) {
	l.mu.Lock()
	defer l.mu.Unlock()
	theirs = map[cacheKey]*flight{}
	for _, k := range missing {
		if f, busy := l.loading[k]; busy {
			theirs[k] = f
			continue
		}
		l.loading[k] = &flight{done: make(chan struct{})}
		mine = append(mine, k)
	}
	return mine, theirs
}

// release hands each claimed field's entry, if it was read, to its flight and wakes every fetch waiting on
// it - whether the read succeeded or not, since a waiter left without an entry reads the row itself.
func (l *Linker) release(keys []cacheKey, loaded map[cacheKey]*cachedField) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range keys {
		f := l.loading[k]
		f.entry = loaded[k]
		close(f.done)
		delete(l.loading, k)
	}
}

// loadInto reads the anchors of the given fields in one Loader call, asking each for the columns those fields
// name, and caches every wanted field the rows it read can serve - the given ones and any other on the same
// rows and columns. It returns the entry of every field it read.
func (l *Linker) loadInto(ctx context.Context, fields []cacheKey, wanted map[cacheKey]bool, load Loader) (map[cacheKey]*cachedField, error) {
	byAnchor := map[int]*Read{}
	for _, k := range fields {
		r := byAnchor[k.anchor]
		if r == nil {
			r = &Read{ID: k.anchor}
			byAnchor[k.anchor] = r
		}
		r.State = r.State || k.col != ColChanges
		r.Changes = r.Changes || k.col != ColState
	}
	reads := make([]Read, 0, len(byAnchor))
	for _, r := range byAnchor {
		reads = append(reads, *r)
	}
	sort.Slice(reads, func(i, j int) bool { return reads[i].ID < reads[j].ID })
	fetched, err := load(ctx, reads)
	if err != nil {
		return nil, errors.Trace(err)
	}
	loaded := map[cacheKey]*cachedField{}
	for k := range wanted {
		r, ok := byAnchor[k.anchor]
		if !ok || (k.col != ColChanges && !r.State) || (k.col != ColState && !r.Changes) {
			continue
		}
		row, ok := fetched[k.anchor]
		if !ok {
			return loaded, errors.New("state ref to %q points at missing step %d", k.field, k.anchor)
		}
		value, err := lookup(row, k)
		if err != nil {
			return loaded, errors.Trace(err)
		}
		// Cache the immutable BYTES, never a decoded value: a decoded one would alias the same map/slice
		// across the fan-out branch goroutines about to hand it to concurrent tasks. Refs sharing a field (the
		// array and an element of it) share one entry, so its split is made once.
		loaded[k] = l.store(k, value)
	}
	return loaded, nil
}

// lookup finds a field's bytes in an anchor row. changes shadows state: the anchor's changes hold the value
// its task PRODUCED, while its state holds a value it merely received (the flow's initial input at the entry
// step, or a fan-in's reduced output). Both are legitimate anchor locations; changes is the newer of the two.
// In changes, a field the task overwrote is named with the overwrite suffix.
func lookup(row Anchor, k cacheKey) (json.RawMessage, error) {
	if k.col != ColState {
		if value, found := row.Changes[k.field+workflow.OverwriteSuffix]; found {
			return value, nil
		}
		// A key ending with the suffix is always an overwrite of another field, never this one.
		if !strings.HasSuffix(k.field, workflow.OverwriteSuffix) {
			if value, found := row.Changes[k.field]; found {
				return value, nil
			}
		}
	}
	if k.col != ColChanges {
		if value, found := row.State[k.field]; found {
			return value, nil
		}
		if _, isRef := row.Refs[k.field]; isRef {
			return nil, errors.New("state ref to %q resolves to step %d, which itself refs %q - one-hop violated", k.field, k.anchor, k.field)
		}
	}
	return nil, errors.New("state ref to %q not found in step %d", k.field, k.anchor)
}

// value returns the bytes a ref resolves to, given the cache entries of every field it names: the field itself
// for a plain ref, what its path and range select otherwise, and for a list the array its entries make.
func (l *Linker) value(key string, ref Ref, entries map[cacheKey]*cachedField, self func(string) json.RawMessage) (json.RawMessage, error) {
	if ref.List == nil {
		k := l.key(ref.Anchor, ref.field(key), ref.Col)
		f := entries[k]
		if f == nil {
			return nil, errors.New("state ref %q: step %d was not read", key, ref.Anchor)
		}
		one, elems, err := l.pick(key, ref, f.bytes, l.splitter(k, f))
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ref.Span != nil {
			return joinArray(elems), nil
		}
		return one, nil
	}
	var out []json.RawMessage
	for _, e := range ref.List {
		var bytes json.RawMessage
		split := func(_ string, array json.RawMessage) ([]json.RawMessage, error) { return splitArray(array) }
		if e.Anchor == 0 {
			bytes = self(e.field(key))
		} else {
			k := l.key(e.Anchor, e.field(key), e.Col)
			f := entries[k]
			if f == nil {
				return nil, errors.New("state ref %q: step %d was not read", key, e.Anchor)
			}
			bytes, split = f.bytes, l.splitter(k, f)
		}
		one, elems, err := l.pick(key, e, bytes, split)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if e.Span != nil {
			out = append(out, elems...)
		} else {
			out = append(out, one)
		}
	}
	return joinArray(out), nil
}

// pick walks a ref's path into a field's bytes and returns what it selects: one value, or, under a range,
// the elements in it. Each level's split comes from split, keyed by the path to it ("data", then "data[3]").
// The values returned are sub-slices of the field's bytes, which is safe to share for the same reason the
// field's bytes are - they are immutable.
func (l *Linker) pick(key string, ref Ref, value json.RawMessage, split func(path string, array json.RawMessage) ([]json.RawMessage, error)) (json.RawMessage, []json.RawMessage, error) {
	at := ref.field(key)
	for level, i := range ref.Path {
		if level > 0 {
			at += "[" + strconv.Itoa(ref.Path[level-1]) + "]"
		}
		elems, err := split(at, value)
		if err != nil {
			return nil, nil, errors.New("state ref %q: %s in step %d: %v", key, at, ref.Anchor, err)
		}
		if i >= len(elems) {
			return nil, nil, errors.New("state ref %q: index %d out of range of %s in step %d (length %d)", key, i, at, ref.Anchor, len(elems))
		}
		value = elems[i]
	}
	if ref.Span == nil {
		return value, nil, nil
	}
	if n := len(ref.Path); n > 0 {
		at += "[" + strconv.Itoa(ref.Path[n-1]) + "]"
	}
	elems, err := split(at, value)
	if err != nil {
		return nil, nil, errors.New("state ref %q: %s in step %d: %v", key, at, ref.Anchor, err)
	}
	from, to := ref.Span.From, ref.Span.To
	if to < 0 {
		to = len(elems)
	}
	if from > to || to > len(elems) {
		return nil, nil, errors.New("state ref %q: range [%d:%d] out of range of %s in step %d (length %d)", key, from, to, at, ref.Anchor, len(elems))
	}
	return nil, elems[from:to], nil
}

// splitter splits arrays within a cached field, keeping each split in the field's entry (split).
func (l *Linker) splitter(k cacheKey, f *cachedField) func(string, json.RawMessage) ([]json.RawMessage, error) {
	return func(path string, array json.RawMessage) ([]json.RawMessage, error) {
		return l.split(k, f, path, array)
	}
}

// joinArray is the JSON array of the given elements.
func joinArray(elems []json.RawMessage) json.RawMessage {
	n := 2
	for _, e := range elems {
		n += len(e) + 1
	}
	b := make([]byte, 0, n)
	b = append(b, '[')
	for i, e := range elems {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, e...)
	}
	return append(b, ']')
}

// split returns the elements of the array at path within f, splitting it on first use and charging the split
// to the cache entry under k. The scan runs outside f's lock, so the arrays of one nested field split in
// parallel; when two calls split the same array, the first to store keeps its split and is the one charged.
//
// The charge is added, never set, and goes to whatever entry is under k. If f was evicted and the field
// re-stored, the new entry is charged for a split it does not hold - an overcharge, freed when that entry is.
func (l *Linker) split(k cacheKey, f *cachedField, path string, array json.RawMessage) ([]json.RawMessage, error) {
	f.mu.Lock()
	elems, ok := f.splits[path]
	f.mu.Unlock()
	if ok {
		return elems, nil
	}
	elems, err := splitArray(array)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	if prior, ok := f.splits[path]; ok {
		f.mu.Unlock()
		return prior, nil
	}
	if f.splits == nil {
		f.splits = map[string][]json.RawMessage{}
	}
	f.splits[path] = elems
	f.mu.Unlock()
	l.cache.AddWeight(k, int64(len(elems))*elemSize+splitOverhead)
	return elems, nil
}

// splitArray returns a JSON array's elements as sub-slices of data, without copying them, so an array split
// for its item refs is not held in memory twice. Each sub-slice is capped at its own length, so an append to
// one can never write over the next element of the shared bytes. data is expected to be valid JSON, which a
// payload column read back from the database is; anything else is an error, never a panic.
func splitArray(data []byte) ([]json.RawMessage, error) {
	i := skipSpace(data, 0)
	if i == len(data) || data[i] != '[' {
		return nil, errors.New("not an array")
	}
	elems := []json.RawMessage{}
	i = skipSpace(data, i+1)
	if i < len(data) && data[i] == ']' {
		return elems, nil
	}
	for i < len(data) {
		start := i
		i = valueEnd(data, i)
		elems = append(elems, json.RawMessage(data[start:i:i]))
		i = skipSpace(data, i)
		if i == len(data) {
			break
		}
		switch data[i] {
		case ',':
			i = skipSpace(data, i+1)
		case ']':
			return elems, nil
		default:
			return nil, errors.New("malformed array")
		}
	}
	return nil, errors.New("malformed array")
}

// valueEnd returns the index just past the JSON value that starts at data[i].
func valueEnd(data []byte, i int) int {
	depth := 0
	for ; i < len(data); i++ {
		switch data[i] {
		case '"':
			for i++; i < len(data) && data[i] != '"'; i++ {
				if data[i] == '\\' {
					i++
				}
			}
			if i >= len(data) {
				return len(data)
			}
			if depth == 0 {
				return i + 1
			}
		case '[', '{':
			depth++
		case ']', '}':
			if depth == 0 {
				return i
			}
			depth--
			if depth == 0 {
				return i + 1
			}
		case ',', ' ', '\t', '\n', '\r':
			if depth == 0 {
				return i
			}
		}
	}
	return i
}

// skipSpace returns the index of the first non-whitespace byte at or after i.
func skipSpace(data []byte, i int) int {
	for i < len(data) && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
		i++
	}
	return i
}

// ResolveReduced materializes only the ref'd fields a fan-in's reducers actually need, into state in place.
//
// A reducer that COMBINES (append/add/union/merge/min/max/and/or/concat) needs its accumulated base value,
// so a ref'd field it touches must be materialized or the fold would apply the delta to an absent base and
// lose everything accumulated so far. The default REPLACE reducer needs nothing: if a branch wrote the
// field, its literal wins outright (and the stale ref is dropped by the caller's mint); if no branch wrote
// it, the field is carried and the ref rides forward untouched.
//
// So a field is materialized iff the graph registers a reducer for it. That is a safe superset - a
// registered replace reducer merely costs a wasted fetch - and it is what keeps a large CARRIED field (the
// motivating case: a document fanned out over its pages) from being materialized and re-anchored at every
// fan-in, which would hand back the win in precisely the fan-out graphs the design exists for.
//
// A merely-carried (non-reduced) ref is left un-materialized here and re-emitted onto the fan-in step by
// Mint's inherited-carry-forward (which handles a ref whose key is absent from the merged state, exactly
// the case a non-materialized carry produces). That is why this returns no carried set: it is not the
// caller's to thread - Mint recovers it from the same inherited refs it is already passed.
func (l *Linker) ResolveReduced(ctx context.Context, state workflow.State, refs Refs, reducers map[string]workflow.Reducer, load Loader) error {
	if len(refs) == 0 {
		return nil
	}
	want := map[string]bool{}
	for field := range refs {
		if _, reduced := reducers[field]; reduced {
			want[field] = true
		}
	}
	if len(want) == 0 {
		return nil
	}
	// The materialized-bytes count is dropped: a fan-in's resolved state is folded and written inside the
	// caller's transaction, never held across a host call, so nothing downstream meters its residency.
	_, err := l.Resolve(ctx, state, refs, want, load)
	return err
}

// Flatten returns a step's state column with every ref'd field spliced back in - the form used where a
// state snapshot must stand on its own, detached from the anchors that backed it.
//
// It splices json.RawMessage values rather than decoding the state map, so a large payload is never
// decoded and re-encoded on the way through.
func (l *Linker) Flatten(ctx context.Context, stateJSON []byte, refs Refs, load Loader) ([]byte, error) {
	if len(refs) == 0 {
		return stateJSON, nil
	}
	var raw map[string]json.RawMessage
	if len(bytes.TrimSpace(stateJSON)) > 0 {
		if err := json.Unmarshal(stateJSON, &raw); err != nil {
			return nil, errors.Trace(err)
		}
	}
	if raw == nil {
		raw = map[string]json.RawMessage{}
	}
	values, err := l.fetch(ctx, refs, nil, func(k string) json.RawMessage { return raw[k] }, load, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// A ref'd field is absent from the state column, except a list whose own elements are stored there; either
	// way the resolved value is the whole field.
	for k, v := range values {
		raw[k] = v
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}
