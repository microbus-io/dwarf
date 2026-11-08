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
	"fmt"
	"strings"
	"testing"
)

// history returns a conversation of n chat messages, each about 200 bytes of JSON.
func history(n int) []map[string]any {
	msgs := make([]map[string]any, n)
	for i := range msgs {
		msgs[i] = map[string]any{
			"role":    []string{"user", "assistant"}[i%2],
			"content": fmt.Sprintf("message %d: %s", i, strings.Repeat("lorem ipsum ", 12)),
			"tokens":  i * 7,
		}
	}
	return msgs
}

var historySizes = []int{5, 50, 500, 5000} // ~1 KB, ~10 KB, ~100 KB, ~1 MB

// BenchmarkFold_AppendOneMessage measures the fold an appending step pays at a hand-off: one message appended to
// a reduced field holding the whole history.
func BenchmarkFold_AppendOneMessage(b *testing.B) {
	reducers := map[string]Reducer{"messages": ReducerAppend}
	for _, n := range historySizes {
		base, _ := NewState("messages", history(n))
		one := NewChanges()
		if err := one.Set("messages", history(1), reducers); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("%dKB", len(base.GetJSON("messages"))/1024), func(b *testing.B) {
			b.SetBytes(int64(len(base.GetJSON("messages"))))
			b.ReportAllocs()
			for b.Loop() {
				s := base.Clone()
				if err := s.Apply(one, reducers); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFold_FlowSet measures a task's Set of one message onto a reduced field holding the whole history: the
// carrier folds it into the task's view.
func BenchmarkFold_FlowSet(b *testing.B) {
	reducers := map[string]Reducer{"messages": ReducerAppend}
	msg := history(1)
	for _, n := range historySizes {
		base, _ := NewState("messages", history(n))
		b.Run(fmt.Sprintf("%dKB", len(base.GetJSON("messages"))/1024), func(b *testing.B) {
			b.SetBytes(int64(len(base.GetJSON("messages"))))
			b.ReportAllocs()
			for b.Loop() {
				f := NewRawFlow()
				f.SetRawState(base)
				f.SetReducers(reducers)
				if err := f.Set("messages", msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFlow_SetPlain measures a task's two writes to fields with no reducer - the common case, which needs
// no fold.
func BenchmarkFlow_SetPlain(b *testing.B) {
	base, _ := NewState("orderID", "o-1", "amount", 42)
	b.ReportAllocs()
	for b.Loop() {
		f := NewRawFlow()
		f.SetRawState(base)
		if err := f.Set("status", "charged"); err != nil {
			b.Fatal(err)
		}
		if err := f.Set("receipt", map[string]any{"id": "r-1", "amount": 42}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFold_UnionFanIn measures a fan-in's union fold: n branches each contributing one distinct element (an
// id number and a small object), folded in one ApplyAll as the fan-in does. The cost must stay linear in n.
func BenchmarkFold_UnionFanIn(b *testing.B) {
	reducers := map[string]Reducer{"ids": ReducerUnion, "seen": ReducerUnion}
	for _, n := range []int{250, 1000, 4000} {
		members := make([]Changes, n)
		for i := range members {
			c := NewChanges()
			if err := c.Set("ids", []int{i}, reducers); err != nil {
				b.Fatal(err)
			}
			if err := c.Set("seen", []map[string]any{{"id": i, "tag": "t"}}, reducers); err != nil {
				b.Fatal(err)
			}
			members[i] = c
		}
		b.Run(fmt.Sprintf("branches=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				s, _ := NewState()
				if err := s.ApplyAll(members, reducers); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
