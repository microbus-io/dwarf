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

package fixtures

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestDocextractionflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("docextractionflow.verify:428/doc-extraction")
	graph.AddTask("scanPDF", "docextractionflow.verify:428/scan-pdf")
	graph.AddTask("identifyChunks", "docextractionflow.verify:428/identify-chunks")
	graph.AddTask("transcribeChunk", "docextractionflow.verify:428/transcribe-chunk")
	graph.AddTask("joinPageTranscriptions", "docextractionflow.verify:428/join-page-transcriptions")
	graph.AddTask("joinDocTranscriptions", "docextractionflow.verify:428/join-doc-transcriptions")
	graph.SetFanIn("joinPageTranscriptions")
	graph.SetFanIn("joinDocTranscriptions")
	graph.SetReducer("transcriptions", workflow.ReducerAppend)
	graph.SetReducer("pageTexts", workflow.ReducerAppend)
	graph.AddTransitionForEach("scanPDF", "identifyChunks", "pageImages", "page")
	graph.AddTransitionForEach("identifyChunks", "transcribeChunk", "chunks", "chunk")
	graph.AddTransition("transcribeChunk", "joinPageTranscriptions")
	graph.AddTransition("joinPageTranscriptions", "joinDocTranscriptions")
	graph.AddTransition("joinDocTranscriptions", workflow.END)
	proxy.HandleGraph("docextractionflow.verify:428/doc-extraction", graph)

	words := []string{"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "a", "and"}

	proxy.HandleTask("docextractionflow.verify:428/scan-pdf", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("pdf", nil)
		time.Sleep(50 * time.Millisecond)
		pageCount := 5 + rand.IntN(18)
		pages := make([]string, pageCount)
		for i := range pages {
			pages[i] = fmt.Sprintf("page-%d", i)
		}
		f.Set("pageImages", pages)
		f.SetInt("pageCount", pageCount)
		return nil
	})
	proxy.HandleTask("docextractionflow.verify:428/identify-chunks", func(ctx context.Context, f *workflow.Flow) error {
		chunkCount := 2 + rand.IntN(4)
		chunks := make([]string, chunkCount)
		for i := range chunks {
			chunks[i] = fmt.Sprintf("chunk-%d", i)
		}
		f.Set("chunks", chunks)
		return nil
	})
	proxy.HandleTask("docextractionflow.verify:428/transcribe-chunk", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("page", nil)
		time.Sleep(time.Duration(50+rand.IntN(100)) * time.Millisecond)
		if rand.Float64() < 0.05 {
			if f.Retry(100, 500*time.Millisecond, 1.0, 500*time.Millisecond) {
				return nil
			}
			return errors.New("transcription failed")
		}
		wordCount := 8 + rand.IntN(13)
		sentence := make([]string, wordCount)
		for i := range sentence {
			sentence[i] = words[rand.IntN(len(words))]
		}
		f.Set("transcriptions", []string{strings.Join(sentence, " ")})
		return nil
	})
	proxy.HandleTask("docextractionflow.verify:428/join-page-transcriptions", func(ctx context.Context, f *workflow.Flow) error {
		var transcriptions []string
		f.Get("transcriptions", &transcriptions)
		f.Set("pageTexts", []string{strings.Join(transcriptions, " ")})
		return nil
	})
	proxy.HandleTask("docextractionflow.verify:428/join-doc-transcriptions", func(ctx context.Context, f *workflow.Flow) error {
		var pageTexts []string
		f.Get("pageTexts", &pageTexts)
		f.SetString("docTranscription", strings.Join(pageTexts, "\n"))
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithWorkers(4)
	eng.RunInTest(t)

	t.Run("extracts_every_page", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "docextractionflow.verify:428/doc-extraction",
			map[string]any{"pdf": "mock-pdf-bytes"}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		doc, _ := outcome.State["docTranscription"].(string)
		assert.True(doc != "")
		lines := strings.Split(doc, "\n")
		assert.True(len(lines) >= 5 && len(lines) <= 22)
		for i, line := range lines {
			assert.True(line != "", "page %d is empty", i)
		}
	})
}
