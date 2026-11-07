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

/*
A state field name is an ASCII letter or underscore, followed by ASCII letters, digits, underscores or hyphens.
State from a caller with any other name is refused at the door with a 400 (Create, Run, Continue, Fork); a task
writing one fails its step, since every Flow writer panics on it; and a subgraph's input holding one comes back
to the task from flow.Subgraph as a 400. Nothing is dropped silently.
*/
package fixtures

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

const fieldNamePrefix = "fieldnameflow.verify:428"

func TestFieldNameFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	// Echo copies the hyphenated input to an output, proving such a name reaches the task and comes back.
	mux.HandleTask(fieldNamePrefix+"/echo", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("line-item-out", f.GetInt("line-item"))
		return nil
	})
	echo := workflow.NewGraph("Echo")
	echo.SetURL("Echo", fieldNamePrefix+"/echo")
	echo.AddTransition("Echo", workflow.End)
	mux.HandleGraph(fieldNamePrefix+"/echo", echo)

	badNames := []string{"a.b", "2nd", "-x", "a b", "total!", "total+", "naïve"}

	t.Run("hyphenated_names_flow_through", func(t *testing.T) {
		assert := testarossa.For(t)
		_, out, err := eng.Run(ctx, fieldNamePrefix+"/echo", map[string]any{"line-item": 7, "camelCase_123": 1}, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, out.Status, out.Error)
			assert.Equal(float64(7), stateVal(out.State, "line-item-out"))
			assert.Equal(float64(1), stateVal(out.State, "camelCase_123"))
		}
	})

	t.Run("create_and_run_refuse_a_bad_name", func(t *testing.T) {
		for _, name := range badNames {
			t.Run(name, func(t *testing.T) {
				assert := testarossa.For(t)
				flowKey, err := eng.Create(ctx, fieldNamePrefix+"/echo", map[string]any{"ok": 1, name: 1}, nil)
				assert.Error(err)
				assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
				assert.Equal("", flowKey)

				flowKey, out, err := eng.Run(ctx, fieldNamePrefix+"/echo", map[string]any{name: 1}, nil)
				assert.Error(err)
				assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
				assert.Equal("", flowKey)
				assert.Nil(out)
			})
		}
	})

	t.Run("continue_and_fork_refuse_a_bad_name", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, out, err := eng.Run(ctx, fieldNamePrefix+"/echo", map[string]any{"line-item": 1}, nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
			return
		}
		history, err := eng.History(ctx, flowKey)
		if !assert.NoError(err) || !assert.Len(history, 1) {
			return
		}
		for _, name := range badNames {
			nextKey, err := eng.Continue(ctx, flowKey, map[string]any{name: 1})
			assert.Error(err, name)
			assert.Equal(http.StatusBadRequest, errors.StatusCode(err), name)
			assert.Equal("", nextKey, name)

			forkKey, err := eng.Fork(ctx, history[0].StepKey, map[string]any{name: 1})
			assert.Error(err, name)
			assert.Equal(http.StatusBadRequest, errors.StatusCode(err), name)
			assert.Equal("", forkKey, name)
		}
		// A valid name is accepted by both.
		nextKey, err := eng.Continue(ctx, flowKey, map[string]any{"line-item": 2})
		if assert.NoError(err) {
			out, err := enginetest.BoundedAwait(t, eng, nextKey)
			if assert.NoError(err) {
				assert.Equal(float64(2), stateVal(out.State, "line-item-out"))
			}
		}
		forkKey, err := eng.Fork(ctx, history[0].StepKey, map[string]any{"line-item": 3})
		if assert.NoError(err) {
			out, err := enginetest.BoundedAwait(t, eng, forkKey)
			if assert.NoError(err) {
				assert.Equal(float64(3), stateVal(out.State, "line-item-out"))
			}
		}
	})

	t.Run("a_task_writing_a_bad_name_fails_its_step", func(t *testing.T) {
		writers := map[string]func(f *workflow.Flow, name string){
			"Set":       func(f *workflow.Flow, name string) { _ = f.Set(name, 1) },
			"Overwrite": func(f *workflow.Flow, name string) { _ = f.Overwrite(name, 1) },
			"Del":       func(f *workflow.Flow, name string) { f.Del(name) },
			"SetInt":    func(f *workflow.Flow, name string) { f.SetInt(name, 1) },
		}
		for writer, write := range writers {
			t.Run(writer, func(t *testing.T) {
				assert := testarossa.For(t)
				url := fieldNamePrefix + "/write-" + strings.ToLower(writer)
				mux.HandleTask(url, func(ctx context.Context, f *workflow.Flow) error {
					write(f, "a.b")
					f.SetBool("pastTheWrite", true)
					return nil
				})
				g := workflow.NewGraph("Write" + writer)
				g.SetURL("Write", url)
				g.AddTransition("Write", workflow.End)
				mux.HandleGraph(url, g)

				out := enginetest.BoundedRun(t, eng, url)
				if assert.NotNil(out) {
					assert.Equal(workflow.StatusFailed, out.Status)
					assert.Contains(out.Error, `invalid state field name "a.b"`)
					assert.Nil(stateVal(out.State, "pastTheWrite"))
				}
			})
		}
	})

	t.Run("a_bad_name_in_a_subgraphs_input_reaches_the_task", func(t *testing.T) {
		assert := testarossa.For(t)
		mux.HandleTask(fieldNamePrefix+"/call", func(ctx context.Context, f *workflow.Flow) error {
			in, _ := workflow.NewState(map[string]any{"line-item": 1, "a.b": 2})
			yield, err := f.Subgraph(fieldNamePrefix+"/echo", in, nil)
			if yield {
				return nil
			}
			if err != nil {
				f.SetInt("code", errors.StatusCode(err))
				f.SetString("message", err.Error())
			}
			return nil
		})
		g := workflow.NewGraph("Call")
		g.SetURL("Call", fieldNamePrefix+"/call")
		g.AddTransition("Call", workflow.End)
		mux.HandleGraph(fieldNamePrefix+"/call", g)

		flowKey, out, err := eng.Run(ctx, fieldNamePrefix+"/call", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, out.Status, out.Error)
		assert.Equal(float64(http.StatusBadRequest), stateVal(out.State, "code"))
		message, _ := stateVal(out.State, "message").(string)
		assert.Contains(message, `invalid state field name "a.b"`)
		// No child flow was created.
		history, err := eng.History(ctx, flowKey)
		if assert.NoError(err) && assert.Len(history, 1) {
			assert.False(history[0].Subgraph)
		}
	})
}
