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
	"bytes"
	"compress/flate"
	"encoding/json"
	"testing"

	"github.com/microbus-io/testarossa"
)

// flateRatio is how many times smaller flate makes b: a stand-in for pglz and lz4, which find the same repeats.
func flateRatio(t *testing.T, b []byte) float64 {
	var out bytes.Buffer
	w, err := flate.NewWriter(&out, flate.DefaultCompression)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(b)
	w.Close()
	return float64(len(b)) / float64(out.Len())
}

func TestJSONPayload_SizedValidAndCompressible(t *testing.T) {
	assert := testarossa.For(t)

	const size = 8 * 1024
	j := newJSONPayloads(size)
	assert.Len(j.docs, 1024)
	for _, doc := range j.docs[:16] {
		var records []map[string]any
		assert.NoError(json.Unmarshal(doc, &records))
		assert.True(len(doc) <= size && len(doc) > size-400, "size %d", len(doc))
		assert.True(len(records) > 20, "records %d", len(records))
	}
	assert.NotEqual(string(j.docs[0]), string(j.docs[1]))

	// Real JSON compresses a few times over; a document that compresses far more would hide the payload's cost,
	// and one that barely compresses is what freshBytes already is.
	ratio := flateRatio(t, j.docs[0])
	assert.True(ratio > 2.5 && ratio < 8, "json ratio %.2f", ratio)
	fresh := flateRatio(t, []byte(newFreshBytes(size, 1).next()))
	t.Logf("flate ratio: json %.2f, fresh %.2f", ratio, fresh)
	assert.True(fresh < 1.5, "fresh ratio %.2f", fresh)

	// A size below one record still yields one whole record.
	var one []map[string]any
	assert.NoError(json.Unmarshal(jsonDoc(10), &one))
	assert.Len(one, 1)
	// A large size builds only as many documents as the pool bound allows.
	assert.Len(newJSONPayloads(jsonPayloadPoolBytes/4).docs, 4)
}
