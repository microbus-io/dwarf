/*
Every JSON payload the engine stores must come back byte-identical, on every dialect.

This is a dialect-ENCODING pin, and it exists because a silent corruption shipped
once already. The "State object cleanup" changed the payload binds from Go string
to []byte and dropped the string(...) conversions, which read as redundant ceremony
and were in fact load-bearing: go-mssqldb sends a []byte as VARBINARY, and against
the then-NVARCHAR columns SQL Server implicitly converted it by reinterpreting byte
PAIRS as UTF-16 code units. Every flow on SQL Server was corrupted, with no error at
write time - it surfaced much later as a JSON decode failure on the read. The columns
are VARBINARY now so the bind type matches, and a mismatch is a hard error.

Two properties make this pin the one that catches that class, where the rest of the
suite structurally cannot:

  - It drives the ENGINE'S OWN write path (Create/task changes/Interrupt/Resume/
    subgraph/final_state). Tests that forge rows with hand-written INSERTs bind
    string literals, so they exercise a different path than the engine's binds and
    stayed green through the whole corruption.
  - It carries NON-ASCII, multi-byte payloads. A UTF-8/UTF-16 confusion is invisible
    to ASCII: the naive "bind []byte as VARCHAR" fix round-trips {"a":1} perfectly and
    still mangles "café" into "cafÃ©". Every string here is chosen to break under a
    byte/char-width or code-page error - combining marks, CJK, RTL, an astral-plane
    emoji (a surrogate pair in UTF-16), and a quote/backslash to keep JSON escaping honest.

It runs on all four dialects and is only interesting on the one that can fail, which
is exactly the point: nothing about it is SQL-Server-specific, so it cannot rot into
a test that quietly stops covering its dialect.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// Payloads picked so that any byte-width, code-page, or surrogate-pair error corrupts them.
const (
	uniAccents = "café naïve — Ω≈ç√" // combining marks + punctuation above ASCII
	uniCJK     = "日本語のテキスト"          // 3-byte UTF-8
	uniRTL     = "مرحبا بالعالم"     // right-to-left
	uniAstral  = "🚀🎉 astral"         // outside the BMP: a surrogate PAIR in UTF-16
)

func TestUnicoderoundtripflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	// Quotes and backslashes ride along so JSON escaping stays honest through the round trip.
	const uniEscaped = `quote " backslash \ slash / ünïcödé`

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("Ünicödé 日本語 Graph") // the graph itself is a stored payload (the `graph` column)
	graph.SetEndpoint("Write", "unicoderoundtripflow.verify:428/write")
	graph.SetEndpoint("Ask", "unicoderoundtripflow.verify:428/ask")
	graph.AddTransitionChain("Write", "Ask", workflow.END)
	assert.NoError(graph.Validate())
	proxy.HandleGraph("unicoderoundtripflow.verify:428/wf", graph)

	// Writes into `changes`, which folds into the successor's `state` and then `final_state`.
	proxy.HandleTask("unicoderoundtripflow.verify:428/write", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("cjk", uniCJK)
		f.SetString("rtl", uniRTL)
		f.SetString("astral", uniAstral)
		f.SetString("escaped", uniEscaped)
		return nil
	})
	// Interrupts with a non-ASCII payload (`interrupt_payload`) and reads back the resume data
	// (`resume_data`), so both human-in-the-loop columns are covered too.
	proxy.HandleTask("unicoderoundtripflow.verify:428/ask", func(ctx context.Context, f *workflow.Flow) error {
		// Resume data is deliberately NOT merged into state - it comes back through `out`, so the
		// round trip being asserted is Resume -> the resume_data column -> this task's own read.
		var resumed struct {
			Reply string `json:"reply"`
		}
		yield, err := f.Interrupt(map[string]any{"question": uniCJK, "detail": uniAccents}, &resumed)
		if yield || err != nil {
			return err
		}
		f.SetString("answered", resumed.Reply)
		return nil
	})

	// The initial state is carried in the entry step's `state`; baggage rides the `baggage` column.
	flowKey, err := eng.Create(ctx, "unicoderoundtripflow.verify:428/wf",
		map[string]any{"seed": uniAccents},
		&workflow.FlowOptions{Baggage: map[string]any{"tenant": uniCJK}},
	)
	if !assert.NoError(err) {
		return
	}

	// --- interrupt_payload: written by the engine, read back through the public reader ---
	// Await, not a poll loop: `interrupted` is a stop status, so this blocks until the flow actually parks.
	// A Snapshot spin here is a race that a fast dialect hides and a slow one fails on.
	parked, err := eng.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(workflow.StatusInterrupted, parked.Status, "the flow must park at the interrupt") {
		return
	}
	assert.Equal(uniCJK, parked.InterruptPayload["question"], "interrupt_payload must round-trip CJK")
	assert.Equal(uniAccents, parked.InterruptPayload["detail"], "interrupt_payload must round-trip accents")
	// The state carried to the interrupt point must be intact too.
	assert.Equal(uniAccents, parked.State["seed"], "the initial state must round-trip")
	assert.Equal(uniCJK, parked.State["cjk"])
	assert.Equal(uniRTL, parked.State["rtl"])
	assert.Equal(uniAstral, parked.State["astral"], "an astral-plane char is a surrogate pair in UTF-16")
	assert.Equal(uniEscaped, parked.State["escaped"])

	// --- resume_data: non-ASCII in, and the task reads it back out ---
	if !assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"reply": uniRTL})) {
		return
	}

	// --- final_state: the terminal merge, the column that outlives its steps ---
	outcome, err := eng.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal(uniAccents, outcome.State["seed"], "final_state must round-trip the initial state")
	assert.Equal(uniCJK, outcome.State["cjk"])
	assert.Equal(uniRTL, outcome.State["rtl"])
	assert.Equal(uniAstral, outcome.State["astral"])
	assert.Equal(uniEscaped, outcome.State["escaped"])
	assert.Equal(uniRTL, outcome.State["answered"], "resume_data must round-trip back through the task")

	// --- the `graph` column: a corrupt graph fails to unmarshal long before this, but assert the
	// display name explicitly so the failure names the cause rather than surfacing as a decode error ---
	summaries, _, err := eng.List(ctx, workflow.Query{})
	if assert.NoError(err) && assert.True(len(summaries) > 0) {
		assert.Equal("Ünicödé 日本語 Graph", summaries[0].WorkflowName, "the stored graph must round-trip")
	}

	// --- per-step payloads through the Step reader (`state` / `changes` as stored per row) ---
	steps, err := eng.History(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	for _, s := range steps {
		if s.TaskName != "Ask" {
			continue
		}
		full, err := eng.Step(ctx, s.StepKey)
		if !assert.NoError(err) {
			continue
		}
		assert.Equal(uniCJK, full.State["cjk"], "the step's stored state snapshot must round-trip")
	}
}
