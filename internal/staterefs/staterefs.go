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
// A Linker holds the size policy and the resolution cache for ONE shard, and has two operations:
//
//	stateJSON, refsJSON, err := linker.Mint(merged, changes, inherited, anchorID, successors, inlineOnly)
//	materialized, err := linker.Resolve(ctx, state, refs, nil, load)
//
// Mint is the write side and performs no I/O: the caller's state is already materialized, so omitting a
// field is all there is to do. Resolve is the read side and reaches the database only through the caller's
// Loader, which is handed every anchor it needs at once. Both are safe for concurrent use.
//
// The encoding never escapes: Mint takes materialized state and Resolve returns materialized state, so a
// caller between the two - transition evaluation, a task carrier, an API response - only ever sees literals.
package staterefs

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/microbus-io/dwarf/internal/lru"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// Refs maps a state field name to the step that physically holds its bytes.
type Refs map[string]int

// Linear is the successor count of a step with one successor, for callers that would otherwise pass a
// bare 1 to Mint.
const Linear = 1

// Anchor is one anchor step's payload, as a Loader returns it. Both columns are searched because either
// can hold a ref'd field's bytes; Refs is what lets Resolve assert that a ref is one hop rather than
// silently walking a chain.
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
	// threshold is the bar for OPENING a new anchor for a single (linear) successor.
	threshold = 4096
	// budget is one round-trip's worth of avoided writes, from the measured constants
	// (docs/benchmark-cloud.md): per-step DB time is k*L + s (k ~ 11-12 round-trips, s ~ 4.4ms) and the byte
	// ceiling is 46-60 MB/s, so same-zone (L ~ 0.5ms) one extra round-trip costs about what ~23KB of avoided
	// writes buys. Divided by the fan-out width, it is the bar for opening an anchor whose cost is amortized
	// over N branches - see openThreshold.
	budget = 23 * 1024
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
	// so a 5x error moves nothing across it. That is also why it applies ONLY at n>1 - the linear bar of
	// 4096 is where estimate accuracy would genuinely decide cases.
	postgresInflation = 5
	// maxAnchors bounds the resolve IN-list. It is the SAME knob as the threshold seen from the other end
	// (the cost model prices anchor ROWS), and the scheme is correct at any value.
	maxAnchors = 4

	// cacheEntries and cacheTTL bound one shard's resolved-bytes cache. Per shard rather than per engine so
	// a busy shard cannot evict another's anchors: in a fan-out every branch resolves the SAME anchor set,
	// so an eviction between sibling dispatches turns one miss plus N-1 hits into many misses.
	cacheEntries = 4096
	cacheTTL     = 15 * time.Minute
)

// cacheKey identifies one ref'd field's bytes within a shard. Immutable once the anchor step settles (a
// terminal step is immutable, and a retry rewrites a step's changes only BEFORE any successor exists), so
// it is as good a key as a content hash. It carries no shard: a Linker serves one shard, which is what
// keeps a per-shard step id from colliding across shards.
type cacheKey struct {
	anchor int
	field  string
}

// Linker mints and resolves state refs for one shard. It is safe for concurrent use.
type Linker struct {
	driver string
	cache  *lru.Cache[cacheKey, json.RawMessage]
}

// New creates a Linker for a shard with the given SQL driver name. The driver only corrects the size
// estimate at a fan-out (see inflation) and is fixed for a shard's life, which is why it is bound here
// rather than passed per call.
func New(driver string) *Linker {
	return &Linker{
		driver: driver,
		cache:  lru.New[cacheKey, json.RawMessage](cacheEntries, cacheTTL),
	}
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
//   - LINEAR (n=1): cost is ~D resolves down the chain and savings are S*D - the depth cancels, so
//     break-even sits near budget. Refs barely pay for a pure linear carry, which is why the bar stays high.
//   - FAN-OUT (n>1): the resolve cache collapses N resolves into one miss plus N-1 hits (every branch
//     resolves the SAME anchor set), while savings are S*N. Break-even becomes S*N >= budget, so the bar
//     falls as 1/N - at N=100 even a few hundred bytes pays for itself.
//
// The clamp is minField, NOT floor. Clamping at the floor made the 1/N term dead code for every fan-out
// wider than budget/floor = 23: the bar sat at a flat 1024 at width 24 and at width 10,000 alike, so the
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
// NO step row: a synthesized fan-out element, or a value only this merge produced. Ref'ing one would
// dangle. It is a distinct signal from a field appearing in changes (which only means the ref must not be
// CARRIED, and leaves the field free to anchor here).
//
// Minting needs no database read: merged was resolved at dispatch, so every literal is already in hand and
// "inlining" is simply declining to omit a field.
func (l *Linker) Mint(merged workflow.State, changes workflow.State, inherited Refs, anchorID int, successors int, inlineOnly map[string]bool) ([]byte, []byte, error) {
	refs := Refs{}
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
	for _, k := range merged.Names() {
		if !inlineOnly[k] {
			// A field the task did not rewrite, and which arrived as a ref, KEEPS that ref - it is never
			// re-minted against this step, whose row does not hold the bytes. This is the one-hop guard, and it
			// is unconditional precisely because the fan-out policy below is not size-based.
			if anchor, isRef := inherited[k]; isRef {
				if !changes.Contains(k) {
					refs[k] = anchor
					continue
				}
			}
		}
		// FieldJSON, not a marshal of the decoded value: a field still held as raw bytes is handed back
		// untouched, so the common case (a carried payload) is never expanded to be measured.
		data, err := merged.FieldJSON(k)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		raw[k] = data
		if !inlineOnly[k] && len(data) >= candidateFloor(successors) {
			candidates = append(candidates, candidate{k, len(data)})
		}
	}

	// Does this step's row earn an anchor? Every candidate shares the same prospective anchor (this step), so
	// the SUM is the right test: four 1KB fields justify one round-trip exactly as one 4KB field does. Summing
	// is legitimate only BECAUSE they are co-located - four 1KB fields at four different anchors would mean
	// four whole-row overfetches for the same 4KB, a far worse trade, and that is never what is summed here.
	sum := 0
	for _, c := range candidates {
		sum += c.size
	}
	if sum >= openThreshold(successors, l.inflation(successors)) {
		for _, c := range candidates {
			refs[c.key] = anchorID
			delete(raw, c.key)
		}
	}

	// Carry forward an inherited ref whose key never appeared in merged. The loop above only sees keys
	// present in merged (via raw), which is every inherited key on the paths that resolve state in FULL at
	// dispatch (linear, fan-out branch). But the fan-in paths resolve only the fields their reducers fold: a
	// merely-CARRIED ref is deliberately not materialized, so its key is absent from merged and the loop
	// never carried it. Without this, that ref is neither materialized nor re-emitted and the carried field
	// is silently, permanently dropped from the fan-in step onward and from the flow's final state. A key
	// already handled above (rewritten -> re-anchored, or inline-only) is skipped; a tombstoned or
	// member-overwritten field appears in changes/inlineOnly and is correctly left dropped.
	for k, anchor := range inherited {
		if merged.Contains(k) {
			continue
		}
		if inlineOnly[k] {
			continue
		}
		if changes.Contains(k) {
			continue
		}
		refs[k] = anchor
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

// CombinedReducerFields names the keys whose merged fan-in value came from a COMBINING (non-replace)
// reducer folding a delta the anchor step wrote onto a base it also held - reduce(state[k], changes[k]).
// That value exists in NO step row: the anchor's changes column holds only the delta, its state only the
// base. So it must never be minted as a ref against that anchor (resolution reads changes first and would
// splice back the bare delta, silently dropping the accumulated base). The fan-in mint therefore passes
// these as inlineOnly. A key present only in changes (no base) is left off: there merged[k] == changes[k],
// so the anchor's changes is a sound anchor and keeping the ref preserves the byte win.
func CombinedReducerFields(state, changes workflow.State, reducers map[string]workflow.Reducer) map[string]bool {
	out := map[string]bool{}
	for k := range changes.All() {
		if r := reducers[k]; r == "" || r == workflow.ReducerReplace {
			continue
		}
		if state.Contains(k) {
			out[k] = true
		}
	}
	return out
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
	for k, anchor := range refs {
		byAnchor[anchor] = append(byAnchor[anchor], k)
		if !merged.Contains(k) {
			pinned[anchor] = true
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
			if v, ok := merged.Lookup(k); ok {
				if data, err := json.Marshal(v); err == nil {
					bytesOf[a] += len(data)
				}
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
			if v, ok := merged.Lookup(k); ok {
				if data, err := json.Marshal(v); err == nil {
					raw[k] = data
				}
			}
			delete(refs, k)
		}
	}
}

// Parse decodes a refs column. An empty or absent value yields no refs and allocates nothing, which is the
// overwhelmingly common case.
//
// A MALFORMED value also yields no refs rather than an error, and that is a decision rather than convenience.
// The column is engine-written, so malformed means a bug-state row - but the alternative is worse: every read
// of that row (Snapshot, Step, History assembly, the fan-in merge, Fork) would fail permanently, bricking a
// flow that is otherwise intact. Degrading instead loses the ref'd fields, which is the same visible symptom
// the row already has, and leaves every other operation on the flow working. The loud checks are kept for the
// cases where the engine still has something to act on: a ref into a missing step, a violated one-hop, and
// Fork's missing clone mapping all error rather than degrade.
func Parse(refsJSON []byte) Refs {
	refsJSON = bytes.TrimSpace(refsJSON)
	if len(refsJSON) == 0 || bytes.Equal(refsJSON, []byte("{}")) {
		return nil
	}
	var refs Refs
	if err := json.Unmarshal(refsJSON, &refs); err != nil {
		return nil
	}
	if len(refs) == 0 {
		return nil
	}
	return refs
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
	if state.IsZero() || len(refs) == 0 {
		return 0, nil
	}
	values, err := l.fetch(ctx, refs, want, load)
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
		state.SetCanonicalJSON(field, value)
		materialized += len(value)
	}
	return materialized, nil
}

// fetch returns the raw bytes of every selected ref'd field, serving what it can from the cache and asking
// the Loader once for the anchors it still needs. It is the shared core of the two read paths, which differ
// only in what they do with the bytes: Resolve decodes them into a live state map, Flatten splices them
// into a state document without decoding at all.
func (l *Linker) fetch(ctx context.Context, refs Refs, want map[string]bool, load Loader) (map[string]json.RawMessage, error) {
	values := make(map[string]json.RawMessage, len(refs))
	// Serve what the cache can, and collect the anchors still needed.
	pending := map[string]int{}
	anchorSet := map[int]bool{}
	for field, anchor := range refs {
		if want != nil && !want[field] {
			continue
		}
		if cached, ok := l.cache.Load(cacheKey{anchor, field}); ok {
			values[field] = cached
			continue
		}
		pending[field] = anchor
		anchorSet[anchor] = true
	}
	if len(pending) == 0 {
		return values, nil
	}

	anchorIDs := make([]int, 0, len(anchorSet))
	for a := range anchorSet {
		anchorIDs = append(anchorIDs, a)
	}
	fetched, err := load(ctx, anchorIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for field, anchor := range pending {
		row, ok := fetched[anchor]
		if !ok {
			return nil, errors.New("state ref %q points at missing step %d", field, anchor)
		}
		// changes shadows state: the anchor's changes hold the value its task PRODUCED, while its state holds
		// a value it merely received (the flow's initial input at the entry step, or a fan-in's reduced
		// output). Both are legitimate anchor locations; changes is the newer of the two.
		value, found := row.Changes[field]
		if !found {
			value, found = row.State[field]
		}
		if !found {
			if _, isRef := row.Refs[field]; isRef {
				return nil, errors.New("state ref %q resolves to step %d, which itself refs it - one-hop violated", field, anchor)
			}
			return nil, errors.New("state ref %q not found in step %d", field, anchor)
		}
		// Cache the immutable BYTES, never a decoded value: a decoded one would alias the same map/slice
		// across the fan-out branch goroutines about to hand it to concurrent tasks.
		l.cache.Store(cacheKey{anchor, field}, value)
		values[field] = value
	}
	return values, nil
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
	values, err := l.fetch(ctx, refs, nil, load)
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
