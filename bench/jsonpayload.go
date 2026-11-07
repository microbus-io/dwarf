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

package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
)

// jsonPayloads hands out JSON documents of about a fixed size that compress about as real JSON does: an array of
// records with the same keys in every record and words drawn from a small vocabulary, beside random numbers. The
// keys and words repeat, which is what TOAST compression and wal_compression find in real documents; the numbers
// do not, so a document does not shrink to nothing as a repeated-character payload would. freshBytes is the
// incompressible counterpart, for measuring raw write volume.
//
// The documents are built once, before the run, and handed out at random: building one per task would load the
// engine host the run is measuring. Documents in different rows may repeat, since each is compressed on its own.
//
// Each document is also known by its record count and the sum of its records' ids, so a task handed one can check it
// arrived whole (holds) without comparing bytes, which the database does not keep: PostgreSQL's JSONB reorders keys.
type jsonPayloads struct {
	docs []json.RawMessage
	keys map[jsonDocKey]struct{}
}

// jsonDocKey identifies a document by its records: ids are random up to 10M, so two documents agreeing on both is
// a coincidence a run will not meet.
type jsonDocKey struct {
	records int
	idSum   int64
}

// jsonPayloadPoolBytes bounds the memory the built documents take, so a large -payload builds a few rather than many.
const jsonPayloadPoolBytes = 64 << 20

// newJSONPayloads builds the documents for payloads of about size bytes.
func newJSONPayloads(size int) *jsonPayloads {
	n := min(1024, max(2, jsonPayloadPoolBytes/max(size, 1)))
	j := &jsonPayloads{docs: make([]json.RawMessage, n), keys: make(map[jsonDocKey]struct{}, n)}
	for i := range j.docs {
		j.docs[i] = jsonDoc(size)
		key, err := keyOfJSONDoc(j.docs[i])
		if err != nil {
			panic(err)
		}
		j.keys[key] = struct{}{}
	}
	return j
}

// nextN returns n of the documents, at random.
func (j *jsonPayloads) nextN(n int) []json.RawMessage {
	out := make([]json.RawMessage, n)
	for i := range out {
		out[i] = j.next()
	}
	return out
}

// holds reports whether doc is one of the documents, whole: the same records, by count and ids.
func (j *jsonPayloads) holds(doc []byte) bool {
	key, err := keyOfJSONDoc(doc)
	if err != nil {
		return false
	}
	_, ok := j.keys[key]
	return ok
}

// keyOfJSONDoc decodes a document's record ids into its key.
func keyOfJSONDoc(doc []byte) (jsonDocKey, error) {
	var records []struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(doc, &records); err != nil {
		return jsonDocKey{}, err
	}
	key := jsonDocKey{records: len(records)}
	for _, r := range records {
		key.idSum += r.ID
	}
	return key, nil
}

// next returns one of the documents, at random.
func (j *jsonPayloads) next() json.RawMessage {
	return j.docs[rand.IntN(len(j.docs))]
}

// jsonDoc builds an array of records no larger than size bytes, as many as fit, and at least one.
func jsonDoc(size int) json.RawMessage {
	b := make([]byte, 0, size)
	b = append(b, '[')
	for {
		rec := jsonRecord()
		if len(b) > 1 && len(b)+1+len(rec)+1 > size {
			break
		}
		if len(b) > 1 {
			b = append(b, ',')
		}
		b = append(b, rec...)
	}
	return append(b, ']')
}

// jsonWords is the vocabulary the records' text values draw from.
var jsonWords = []string{
	"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "harbor", "garden", "lamp", "table", "river", "stone",
	"silver", "copper", "maple", "cedar", "north", "south", "east", "west", "rapid", "quiet", "bright", "shadow",
	"market", "office", "studio", "kitchen", "outdoor", "travel", "winter", "summer", "classic", "modern", "compact",
	"deluxe", "standard", "premium", "basic", "limited", "sale", "new", "clearance", "featured", "bundle", "gift",
	"red", "blue", "green", "black", "white", "grey", "small", "medium", "large", "pending", "shipped", "delivered",
	"returned", "cancelled", "approved", "review", "draft", "archived",
}

// jsonRecord returns one record, about 250-300 bytes of JSON.
func jsonRecord() []byte {
	w := func() string { return jsonWords[rand.IntN(len(jsonWords))] }
	return fmt.Appendf(nil,
		`{"id":%d,"sku":"%s-%04d-%04d","name":"%s %s %s","category":"%s","status":"%s","qty":%d,"price":%d.%02d,`+
			`"active":%t,"tags":["%s","%s"],"warehouse":{"region":"%s","bin":"%s-%03d"},`+
			`"updated":"2026-%02d-%02dT%02d:%02d:%02dZ"}`,
		rand.IntN(10_000_000), w(), rand.IntN(10_000), rand.IntN(10_000), w(), w(), w(), w(), w(),
		1+rand.IntN(50), rand.IntN(1000), rand.IntN(100),
		rand.IntN(2) == 0, w(), w(), w(), w(), rand.IntN(1000),
		1+rand.IntN(12), 1+rand.IntN(28), rand.IntN(24), rand.IntN(60), rand.IntN(60))
}
