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

package reaper

import (
	"sync"
	"time"
)

// walkKey names one walker: a shard, a kind, and whether it is the repair walker.
type walkKey struct {
	shard  int
	kind   string
	repair bool
}

// walk is where a walker stands. It is a scheduling hint, never ownership.
type walk struct {
	after, end         dueRoot
	started            time.Time
	batch              int
	failed, lastFailed int
	oldest             time.Duration
	observed           time.Time
	// startedAfter is where the scan for trees whose staged archive has started stands, while no new archive may
	// start (resumeStarted); apart from after, which stays on the trees the walk archives once the day opens.
	startedAfter dueRoot
}

// walks holds every walker's position. One walker per shard runs at a time in production; the mutex lets Observe
// read alongside, and overlapping passes in tests run without data races.
type walks struct {
	mu    sync.Mutex
	byKey map[walkKey]walk
}

func (w *walks) get(k walkKey) walk {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.byKey[k]
}

func (w *walks) put(k walkKey, v walk) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.byKey == nil {
		w.byKey = map[walkKey]walk{}
	}
	w.byKey[k] = v
}

func (w *walks) forget(shard int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for k := range w.byKey {
		if k.shard == shard {
			delete(w.byKey, k)
		}
	}
}

// failures is, per shard, since when archiving has been failing, and when it last failed.
type failures struct {
	mu   sync.Mutex
	byID map[int]failure
}

type failure struct{ since, last time.Time }

// fail records a failure on shard at now, and reports whether it began an episode.
func (f *failures) fail(shard int, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byID == nil {
		f.byID = map[int]failure{}
	}
	rec, failing := f.byID[shard]
	if !failing {
		rec.since = now
	}
	rec.last = now
	f.byID[shard] = rec
	return !failing
}

// clearUnlessFailedSince forgets the shard's failures, unless one was recorded at or after start: the work that began
// at start archived without failing.
func (f *failures) clearUnlessFailedSince(shard int, start time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.byID[shard]; ok && rec.last.Before(start) {
		delete(f.byID, shard)
	}
}

// failingFor returns how long archiving has been failing on shard at now, or 0.
func (f *failures) failingFor(shard int, now time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.byID[shard]
	if !ok {
		return 0
	}
	return max(now.Sub(rec.since), 0)
}

func (f *failures) forget(shard int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.byID, shard)
}

// lag is, per shard, how long past the archive grace the oldest due closed root had waited at the last scan.
type lag struct {
	mu   sync.Mutex
	byID map[int]time.Duration
}

func (l *lag) set(shard int, d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.byID == nil {
		l.byID = map[int]time.Duration{}
	}
	l.byID[shard] = d
}

// of returns shard's last reading, and false before its first.
func (l *lag) of(shard int) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	d, ok := l.byID[shard]
	return d, ok
}

func (l *lag) forget(shard int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.byID, shard)
}

// archiveCleanup holds a shard's discovery position and the observations from its last completed pass.
type archiveCleanup struct {
	from             markedTree
	pending, unknown int
	observed         bool
}

// archiveCleanups publishes a pass's position and observations together. The caller serializes passes on each shard;
// Observe can read concurrently. No mutex is held during database work.
type archiveCleanups struct {
	mu   sync.Mutex
	byID map[int]archiveCleanup
}

func (a *archiveCleanups) get(shard int) archiveCleanup {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.byID[shard]
}

func (a *archiveCleanups) put(shard int, state archiveCleanup) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.byID == nil {
		a.byID = map[int]archiveCleanup{}
	}
	a.byID[shard] = state
}

func (a *archiveCleanups) forget(shard int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.byID, shard)
}

// stageKey names a staged copy: a shard and the tree's root.
type stageKey struct{ shard, root int }

// maxStages bounds the staged copies a Reaper remembers per shard. A copy is forgotten when it publishes, its tree is
// stamped or gone, or its attempt's day drops; past the bound the least recently used one goes. Losing a position
// costs a copy a reread of what it copied, never a row.
const maxStages = 64

// stages is where each staged copy stands, shared by a shard's walkers so the fast and the repair walk on one tree
// carry one position.
type stages struct {
	mu    sync.Mutex
	byKey map[stageKey]stageEntry
	clock int64
}

type stageEntry struct {
	pos  stagePos
	used int64
}

func (s *stages) get(k stageKey) stagePos {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byKey[k].pos
}

func (s *stages) put(k stageKey, pos stagePos) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byKey == nil {
		s.byKey = map[stageKey]stageEntry{}
	}
	if _, ok := s.byKey[k]; !ok {
		n, oldest, oldestKey := 0, int64(0), stageKey{}
		for other, e := range s.byKey {
			if other.shard != k.shard {
				continue
			}
			if n == 0 || e.used < oldest {
				oldest, oldestKey = e.used, other
			}
			n++
		}
		if n >= maxStages {
			delete(s.byKey, oldestKey)
		}
	}
	s.clock++
	s.byKey[k] = stageEntry{pos: pos, used: s.clock}
}

func (s *stages) drop(k stageKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byKey, k)
}

func (s *stages) forget(shard int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.byKey {
		if k.shard == shard {
			delete(s.byKey, k)
		}
	}
}
