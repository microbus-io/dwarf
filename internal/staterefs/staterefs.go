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
//	stateJSON, refsJSON, err := linker.Mint(merged, changes, inherited, anchorID, successors, inlineOnly, item)
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

// Ref locates one field's bytes. A plain ref is the field of the same name at Anchor. An item ref is one
// element of an array held at Anchor: Field names the array there, and Path holds one index per nesting
// level, outermost first.
type Ref struct {
	Anchor int    // the step whose row holds the bytes
	Field  string // the field name at the anchor; empty means the refs key itself
	Path   []int  // indices into that field; empty means the whole field
}

// field is the name the ref's bytes have at the anchor, given the refs key it is stored under.
func (r Ref) field(key string) string {
	if r.Field != "" {
		return r.Field
	}
	return key
}

// MarshalJSON writes a plain ref as a bare number and any other as a string "<anchor> <field>[i][j]...".
// The number form is the only one a plain ref ever takes, so a refs column holding no item refs is readable
// by a reader that knows nothing else.
func (r Ref) MarshalJSON() ([]byte, error) {
	if r.Field == "" && len(r.Path) == 0 {
		return strconv.AppendInt(nil, int64(r.Anchor), 10), nil
	}
	b := strconv.AppendInt(nil, int64(r.Anchor), 10)
	b = append(b, ' ')
	b = append(b, r.Field...)
	for _, i := range r.Path {
		b = append(b, '[')
		b = strconv.AppendInt(b, int64(i), 10)
		b = append(b, ']')
	}
	return json.Marshal(string(b))
}

// UnmarshalJSON reads either form MarshalJSON writes. The string form is parsed front to back: the anchor
// runs to the first space, the field name to the first '[', and each "[N]" after it is one index. A field
// name cannot hold a '[' (Mint never emits one that does), so the split is unambiguous.
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
	anchorText, rest, ok := strings.Cut(s, " ")
	anchor, err := strconv.Atoi(anchorText)
	if !ok || err != nil || anchor <= 0 {
		return errors.New("malformed state ref %q", s)
	}
	field, indices, hasPath := strings.Cut(rest, "[")
	if field == "" {
		return errors.New("malformed state ref %q", s)
	}
	var path []int
	for hasPath {
		var num string
		num, indices, ok = strings.Cut(indices, "]")
		i, err := strconv.Atoi(num)
		if !ok || err != nil || i < 0 {
			return errors.New("malformed state ref %q", s)
		}
		path = append(path, i)
		if indices == "" {
			break
		}
		if indices[0] != '[' {
			return errors.New("malformed state ref %q", s)
		}
		indices = indices[1:]
	}
	*r = Ref{Anchor: anchor, Field: field, Path: path}
	return nil
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

// Anchor is one anchor step's payload, as a Loader returns it. Both columns are searched because either
// can hold a ref'd field's bytes; Refs is what lets Resolve assert that a ref is one hop rather than
// silently walking a chain. Each map is its column's JSON object as stored, so Changes names a field the step
// overwrote with workflow.OverwriteSuffix.
type Anchor struct {
	State   map[string]json.RawMessage
	Changes map[string]json.RawMessage
	Refs    Refs
}

// Loader fetches anchors by step id. It is called at most once per Resolve, with every anchor still
// needed after the cache is consulted, and must return one entry per id it could read.
//
// Taking the whole set at once is not an optimization: the size policy prices a whole anchor ROW (any
// number of fields living in one already-fetched row are free), which is only true if fetching k anchors
// costs one round trip rather than k.
//
// A caller metering payload bytes counts them HERE, where the untouched columns are in hand. Summing the
// decoded per-field values instead would undercount by every key, brace and separator, and would count zero
// for a column that failed to decode - wrong for a metric whose job is to track a byte-throughput ceiling.
type Loader func(ctx context.Context, anchorIDs []int) (map[int]Anchor, error)

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
	// maxAnchors bounds the resolve IN-list. It is the SAME knob as the threshold seen from the other end
	// (the cost model prices anchor ROWS), and the scheme is correct at any value.
	maxAnchors = 4

	// The resolved-bytes cache is bounded in bytes (given to NewCache) and in entries (cacheEntries), and an
	// entry expires once unread for cacheIdle. One cache serves every shard, so the budget goes wherever this
	// replica is dispatching.
	cacheEntries = 65536
	cacheIdle    = 5 * time.Minute
	// minCharge is the least an entry is charged for its bytes. Small fields are the many: with the linear
	// threshold at 1KB and every anchored field seeded, a byte budget alone would hold millions of them, each
	// a handful of objects the garbage collector marks every cycle. Charging at least 4KB holds a quarter as
	// many.
	minCharge = 4096
	// entryOverhead is what an entry costs beyond its bytes: the LRU's list element and map slot, the key, the
	// cachedField itself.
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

// key is the cache key of a field at an anchor step of this Linker's shard.
func (l *Linker) key(anchor int, field string) cacheKey {
	return cacheKey{l.shard, anchor, field}
}

// store caches one field's bytes and returns its entry. An entry already cached for the field is kept and
// returned instead, splits and all: a field's bytes are immutable, so the two hold the same value. The entry
// returned may be one the cache declined to keep (larger than the cache); the caller still serves and splits
// from it.
func (l *Linker) store(k cacheKey, value json.RawMessage) *cachedField {
	f, _ := l.cache.LoadOrStoreWeighted(k, &cachedField{bytes: value}, max(int64(len(value)), minCharge)+entryOverhead)
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
// Minting needs no database read: merged was resolved at dispatch, so every literal is already in hand and
// "inlining" is simply declining to omit a field.
func (l *Linker) Mint(merged workflow.State, changes workflow.Changes, inherited Refs, anchorID int, successors int, inlineOnly map[string]bool, item *Item) ([]byte, []byte, error) {
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
	for k := range merged.Names() {
		if !literal(k) {
			// A field the task did not rewrite, and which arrived as a ref, KEEPS that ref - it is never
			// re-minted against this step, whose row does not hold the bytes. This is the one-hop guard, and it
			// is unconditional precisely because the fan-out policy below is not size-based.
			if anchor, isRef := inherited[k]; isRef {
				if !changes.Mentions(k) {
					refs[k] = anchor
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
	if sum >= openThreshold(successors, l.inflation(successors)) {
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
		refs[k] = anchor
	}

	if item != nil {
		mintItem(raw, refs, *item)
	}

	inlineExcessAnchors(raw, refs, merged)

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

// mintItem replaces a forEach element's literal with an item ref into its source array, when the array is
// stored by a ref and the element outweighs the ref. It runs after the array's fate is decided - carried,
// newly anchored here, or inline - because the item ref points at whichever anchor holds the array's bytes,
// never at the step merely carrying a ref to them, which would break the one-hop rule.
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
	field := src.field(item.Source)
	if len(raw[item.Key]) < minItem {
		return
	}
	path := append(slices.Clone(src.Path), item.Index)
	refs[item.Key] = Ref{Anchor: src.Anchor, Field: field, Path: path}
	delete(raw, item.Key)
}

// inlineExcessAnchors caps the pointer fan at maxAnchors by inlining the cheapest anchors' fields back into
// state as literals - one copy, paid once, to shorten the resolve IN-list. It costs no database read: the
// literals are all present in merged (state was resolved at dispatch). Anchors are dropped smallest-bytes
// first, since those are the ones whose refs buy the least.
func inlineExcessAnchors(raw map[string]json.RawMessage, refs Refs, merged workflow.State) {
	if len(refs) == 0 {
		return
	}
	byAnchor := map[int][]string{}
	// An anchor is PINNED (un-droppable) if any of its fields' literals are absent from merged - a merely
	// carried ref whose payload this step never materialized. Inlining it would require a literal we do not
	// have, so dropping it would delete the field outright: the exact data loss the carry-forward above closes.
	// Such a ref must survive the cap even if that means a wider IN-list (correctness over the perf bound).
	// Detecting it needs only a presence check, no marshal.
	pinned := map[int]bool{}
	for k, ref := range refs {
		byAnchor[ref.Anchor] = append(byAnchor[ref.Anchor], k)
		if !mentions(merged, k) {
			pinned[ref.Anchor] = true
		}
	}
	if len(byAnchor) <= maxAnchors {
		return
	}
	// Over the cap: only NOW size the droppable anchors (largest-bytes first so the survivors are the ones the
	// refs buy the most on). This is the only path that marshals ref'd fields - a possibly-large carried
	// payload is never re-serialized in the common under-cap case above. Ties break on anchor id for determinism.
	bytesOf := map[int]int{}
	droppable := make([]int, 0, len(byAnchor))
	pinnedCount := 0
	for a, keys := range byAnchor {
		if pinned[a] {
			pinnedCount++
			continue
		}
		for _, k := range keys {
			if data := merged.GetJSON(k); data != nil {
				bytesOf[a] += len(data)
			}
		}
		droppable = append(droppable, a)
	}
	sort.Slice(droppable, func(i, j int) bool {
		if bytesOf[droppable[i]] != bytesOf[droppable[j]] {
			return bytesOf[droppable[i]] > bytesOf[droppable[j]]
		}
		return droppable[i] < droppable[j]
	})
	// Keep the pinned anchors plus the largest droppable ones up to the cap; inline the rest.
	keepDroppable := max(0, maxAnchors-pinnedCount)
	for _, a := range droppable[min(keepDroppable, len(droppable)):] {
		for _, k := range byAnchor[a] {
			if data := merged.GetJSON(k); data != nil {
				raw[k] = data
			}
			delete(refs, k)
		}
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
// without a read. merged is the state Mint was given and refsJSON the refs it returned; only the plain refs
// pointing at anchorID are taken, since an inherited ref points at an earlier anchor and an item ref is
// served from its array's entry.
//
// Call it only once the anchor step's own payload has committed, and before its successors are offered for
// dispatch.
func (l *Linker) Seed(anchorID int, merged workflow.State, refsJSON []byte) {
	if len(refsJSON) <= 2 { // "{}"
		return
	}
	refs, err := Parse(refsJSON)
	if err != nil {
		return
	}
	for field, ref := range refs {
		if ref.Anchor != anchorID || len(ref.Path) > 0 {
			continue
		}
		if value := merged.GetJSON(field); len(value) > 0 {
			l.store(l.key(anchorID, ref.field(field)), value)
		}
	}
}

func (l *Linker) resolve(ctx context.Context, state workflow.State, refs Refs, want map[string]bool, load Loader, shared bool) (int, error) {
	if state.IsZero() || len(refs) == 0 {
		return 0, nil
	}
	values, err := l.fetch(ctx, refs, want, load, shared)
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
// the Loader for the anchors it still needs - once, in one call, unless shared. It is the shared core of the
// two read paths, which differ only in what they do with the bytes: Resolve decodes them into a live state
// map, Flatten splices them into a state document without decoding at all.
//
// shared makes concurrent misses on one field read it once (see ResolveShared). Such a fetch reads the
// fields no other fetch is reading, then waits for the rest and takes them from the flight; one whose wait
// did not deliver - the other read failed - reads for itself. So it makes zero, one or two Loader calls.
func (l *Linker) fetch(ctx context.Context, refs Refs, want map[string]bool, load Loader, shared bool) (map[string]json.RawMessage, error) {
	values := make(map[string]json.RawMessage, len(refs))
	pending := map[string]Ref{}
	for key, ref := range refs {
		if want == nil || want[key] {
			pending[key] = ref
		}
	}
	if err := l.serve(pending, values, l.cache.Load); err != nil {
		return nil, errors.Trace(err)
	}
	if len(pending) == 0 {
		return values, nil
	}
	if !shared {
		if _, err := l.loadInto(ctx, anchorsOf(pending), pending, values, load); err != nil {
			return nil, errors.Trace(err)
		}
		return values, nil
	}

	// Read the claimed fields' anchors, which resolves every pending ref on those rows - claimed or not - and
	// only then wait. Reading before waiting is what keeps two fetches that each claimed a field the other
	// needs from deadlocking.
	mine, theirs := l.claim(pending)
	if len(mine) > 0 {
		err := func() error {
			loaded := map[cacheKey]*cachedField{}
			defer func() { l.release(mine, loaded) }()
			var err error
			loaded, err = l.loadInto(ctx, anchorsOfKeys(mine), pending, values, load)
			return err
		}()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for k, f := range theirs {
		if !pendingAt(pending, k) {
			continue
		}
		select {
		case <-f.done:
		case <-ctx.Done():
			return nil, errors.Trace(ctx.Err())
		}
	}
	// A waited-on field comes from its flight, which holds it even if the cache did not keep it.
	err := l.serve(pending, values, func(k cacheKey) (*cachedField, bool) {
		if f, ok := theirs[k]; ok && f.entry != nil {
			return f.entry, true
		}
		return l.cache.Load(k)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(pending) == 0 {
		return values, nil
	}
	// The read waited on failed, so this fetch reads for itself. Unclaimed: another round of waiting would
	// buy nothing.
	if _, err := l.loadInto(ctx, anchorsOf(pending), pending, values, load); err != nil {
		return nil, errors.Trace(err)
	}
	return values, nil
}

// serve moves every pending ref that lookup can serve into values. An entry holds a whole field; an item ref
// is served from its array's entry, so a cohort's branches share one entry for the array.
func (l *Linker) serve(pending map[string]Ref, values map[string]json.RawMessage, lookup func(cacheKey) (*cachedField, bool)) error {
	for key, ref := range pending {
		k := l.key(ref.Anchor, ref.field(key))
		cached, ok := lookup(k)
		if !ok {
			continue
		}
		value, err := l.element(key, ref, k, cached)
		if err != nil {
			return errors.Trace(err)
		}
		values[key] = value
		delete(pending, key)
	}
	return nil
}

// claim splits the fields pending refs need into those this fetch will read and those another fetch is
// already reading, returning the latter's flights.
func (l *Linker) claim(pending map[string]Ref) (mine []cacheKey, theirs map[cacheKey]*flight) {
	l.mu.Lock()
	defer l.mu.Unlock()
	theirs = map[cacheKey]*flight{}
	claimed := map[cacheKey]bool{}
	for key, ref := range pending {
		k := l.key(ref.Anchor, ref.field(key))
		if claimed[k] {
			continue // two item refs into one array share the field
		}
		if f, busy := l.loading[k]; busy {
			theirs[k] = f
			continue
		}
		l.loading[k] = &flight{done: make(chan struct{})}
		claimed[k] = true
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

// pendingAt reports whether any pending ref still needs field k.
func pendingAt(pending map[string]Ref, k cacheKey) bool {
	for key, ref := range pending {
		if ref.Anchor == k.anchor && ref.field(key) == k.field {
			return true
		}
	}
	return false
}

// anchorsOf is the distinct anchors pending refs point at.
func anchorsOf(pending map[string]Ref) []int {
	seen := map[int]bool{}
	var out []int
	for _, ref := range pending {
		if !seen[ref.Anchor] {
			seen[ref.Anchor] = true
			out = append(out, ref.Anchor)
		}
	}
	return out
}

// anchorsOfKeys is the distinct anchors of the given fields.
func anchorsOfKeys(keys []cacheKey) []int {
	seen := map[int]bool{}
	var out []int
	for _, k := range keys {
		if !seen[k.anchor] {
			seen[k.anchor] = true
			out = append(out, k.anchor)
		}
	}
	return out
}

// loadInto reads the given anchors in one Loader call and resolves every pending ref that points at one of
// them, caching each field it reads and removing the ref from pending. It returns the entry of every field
// it read.
func (l *Linker) loadInto(ctx context.Context, anchors []int, pending map[string]Ref, values map[string]json.RawMessage, load Loader) (map[cacheKey]*cachedField, error) {
	fetched, err := load(ctx, anchors)
	if err != nil {
		return nil, errors.Trace(err)
	}
	loaded := map[cacheKey]*cachedField{}
	for key, ref := range pending {
		if !slices.Contains(anchors, ref.Anchor) {
			continue
		}
		row, ok := fetched[ref.Anchor]
		if !ok {
			return nil, errors.New("state ref %q points at missing step %d", key, ref.Anchor)
		}
		// changes shadows state: the anchor's changes hold the value its task PRODUCED, while its state holds
		// a value it merely received (the flow's initial input at the entry step, or a fan-in's reduced
		// output). Both are legitimate anchor locations; changes is the newer of the two. In changes, a field the
		// task overwrote is named with the overwrite suffix.
		field := ref.field(key)
		value, found := row.Changes[field+workflow.OverwriteSuffix]
		if !found && !strings.HasSuffix(field, workflow.OverwriteSuffix) {
			// A key ending with the suffix is always an overwrite of another field, never this one.
			value, found = row.Changes[field]
		}
		if !found {
			value, found = row.State[field]
		}
		if !found {
			if _, isRef := row.Refs[field]; isRef {
				return nil, errors.New("state ref %q resolves to step %d, which itself refs %q - one-hop violated", key, ref.Anchor, field)
			}
			return nil, errors.New("state ref %q not found in step %d", key, ref.Anchor)
		}
		// Cache the immutable BYTES, never a decoded value: a decoded one would alias the same map/slice
		// across the fan-out branch goroutines about to hand it to concurrent tasks. Refs sharing a field (the
		// array and an element of it) share one entry, so its split is made once.
		k := l.key(ref.Anchor, field)
		entry, ok := loaded[k]
		if !ok {
			entry = l.store(k, value)
			loaded[k] = entry
		}
		value, err = l.element(key, ref, k, entry)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values[key] = value
		delete(pending, key)
	}
	return loaded, nil
}

// element returns the bytes a ref resolves to, given its whole field's cache entry f, stored under k: the
// field itself for a plain ref, the element its path selects for an item ref. Each level's split is kept in
// f under the path to it ("data", then "data[3]"), so a nested cohort splits every inner array once. The
// element is a sub-slice of the field's bytes, which is safe to share for the same reason the field's bytes
// are - they are immutable.
func (l *Linker) element(key string, ref Ref, k cacheKey, f *cachedField) (json.RawMessage, error) {
	value := f.bytes
	at := ref.field(key)
	for level, i := range ref.Path {
		if level > 0 {
			at += "[" + strconv.Itoa(ref.Path[level-1]) + "]"
		}
		elems, err := l.split(k, f, at, value)
		if err != nil {
			return nil, errors.New("state ref %q: %s in step %d: %v", key, at, ref.Anchor, err)
		}
		if i >= len(elems) {
			return nil, errors.New("state ref %q: index %d out of range of %s in step %d (length %d)", key, i, at, ref.Anchor, len(elems))
		}
		value = elems[i]
	}
	return value, nil
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
	values, err := l.fetch(ctx, refs, nil, load, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// A ref'd field is absent from the state column by construction, so splicing can only add keys - it
	// never has to decide between a literal and a resolved value.
	for k, v := range values {
		raw[k] = v
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}
