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

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestYieldTurnAway_MovesTheClaimAndRetakesTheTurn pins the helper the subgraph LoadGraph runs under: the turn is
// free for the duration of the host call, the claim comes back moved forward by that duration (so the call does
// not age the job), and the turn is re-taken on the moved claim - on a panic as well as a return.
func TestYieldTurnAway_MovesTheClaimAndRetakesTheTurn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	sh := database.NewShard(1, nil)
	sh.SetTurnLimit(1)
	ctx, pass := sh.WaitTurn(context.Background())
	defer func() { pass.Return() }()
	start, _ := turnstile.ClaimFrom(ctx)

	yieldTurnAway(&ctx, &pass, func() {
		free, _ := sh.Turns()
		assert.Equal(1, free, "the turn is handed back for the host call")
		time.Sleep(30 * time.Millisecond)
	})
	moved, _ := turnstile.ClaimFrom(ctx)
	assert.True(moved.Since.Sub(start.Since) >= 30*time.Millisecond, "the claim moves forward by the host call, got %v", moved.Since.Sub(start.Since))
	assert.Equal(start.Seq, moved.Seq, "it is still the same job")
	free, _ := sh.Turns()
	assert.Equal(0, free, "the turn is re-taken after the host call")

	func() {
		defer func() { _ = recover() }()
		yieldTurnAway(&ctx, &pass, func() {
			time.Sleep(30 * time.Millisecond)
			panic("host panic")
		})
	}()
	again, _ := turnstile.ClaimFrom(ctx)
	assert.True(again.Since.Sub(moved.Since) >= 30*time.Millisecond, "a panicking host call moves the claim too, got %v", again.Since.Sub(moved.Since))
	free, _ = sh.Turns()
	assert.Equal(0, free, "and the turn is re-taken")
}

// TestRecoveryReset_TakesATurn pins that the processStep recovery defer's reset takes a turn of its own. Every
// turn the dispatch held has been handed back by the time the defer runs, so without one the reset would reach
// the database unordered. The defer is frozen at its checkpoint, after the turn and before the reset, and the
// shard's turnstile must show a turn held on every sample; with no turn taken, it reads free between the
// background loops' brief passes.
func TestRecoveryReset_TakesATurn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("ResetTurn")
	g.SetURL("A", "resetturn/a")
	g.SetURL("B", "resetturn/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph("resetturn/g", g)
	mux.HandleTask("resetturn/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("resetturn/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	// Contention through one full round of the transition's Transact retries sends the completed step to the
	// defer's reset.
	eng.seams.InjectN(engineseams.Join(engineseams.FaultContention, "A"), 8)
	eng.seams.Break(engineseams.CheckpointBeforeRecoveryReset)
	flowKey, err := eng.Create(ctx, "resetturn/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	if !assert.True(eng.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeRecoveryReset, 15*time.Second), "engine never reached the recovery reset") {
		return
	}
	shard, _, _, _, _ := keys.ParseFlowKey(flowKey)
	sh, err := eng.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	freeSamples := 0
	for range 20 {
		if free, _ := sh.Turns(); free >= sh.TurnLimit() {
			freeSamples++
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.Equal(0, freeSamples, "the reset must hold a turn while it waits to run")
	eng.seams.Resume(engineseams.CheckpointBeforeRecoveryReset)

	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := eng.Await(awaitCtx, flowKey)
	if assert.NoError(err) && assert.NotNil(out) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}
