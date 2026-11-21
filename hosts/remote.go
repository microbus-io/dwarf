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

package hosts

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf16"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// The headers Remote sets on a task request, beside Content-Type and Accept.
const (
	// HeaderIdempotencyKey carries the step key. See Remote for what it identifies.
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderAttempt carries the step's dispatch count, flow.Attempt.
	HeaderAttempt = "Dwarf-Attempt"
	// HeaderBudgetMillis carries the whole milliseconds left in the step's time budget.
	HeaderBudgetMillis = "Dwarf-Budget-Millis"
	// HeaderBaggage carries the flow's baggage as JSON in printable ASCII, on graph and task requests alike, when
	// it has any.
	HeaderBaggage = "Dwarf-Baggage"
)

// DefaultRetryHorizon is how long Remote keeps a step waiting for its task server when RetryHorizon is zero.
const DefaultRetryHorizon = 10 * time.Minute

// The backoff of a step waiting for its task server when the reply names no Retry-After.
const (
	retryDelay      = time.Second
	retryMultiplier = 2
	retryMaxDelay   = 30 * time.Second
)

// maxErrorBody bounds how much of an error reply's body becomes the error's message.
const maxErrorBody = 16 << 10

// maxReplyBytes bounds a result's or a graph's body: twice the largest value one state field can hold.
const maxReplyBytes = 512 << 20

// idleConnsPerHost is how many idle connections Remote's default client keeps to each task server. Over HTTP/1.1
// every request in flight holds a connection of its own, and the standard library keeps 2 idle per host, so under
// sustained concurrency it would close and reopen a connection for most requests, each closed one holding a port
// in TIME_WAIT.
const idleConnsPerHost = 512

// defaultClient is Remote's client when it is given none.
var defaultClient = &http.Client{Transport: newDefaultTransport()}

// newDefaultTransport is the standard library's default transport with a large idle pool per host and no bound on
// the total: an idle connection still closes after the transport's idle timeout.
func newDefaultTransport() *http.Transport {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t = &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true, IdleConnTimeout: 90 * time.Second}
	}
	t = t.Clone()
	t.MaxIdleConns = 0
	t.MaxIdleConnsPerHost = idleConnsPerHost
	return t
}

/*
Remote is an engine.Host that loads graphs and runs tasks across HTTP. It is meant for tests, development and
low-volume use on a trusted network, and is not hardened for production: it has no authentication of its own, it
has not been measured under load, over HTTP/1.1 every request in flight holds a connection of its own, and a step
waiting for an unreachable task server is polled by dispatching it again.

Its zero value is ready to use; pass it to Engine.SetHost. It handles http and https URLs only: for any other URL
LoadGraph reports no such graph, and ExecuteTask returns an error with status 404. It follows a redirect that
keeps the request as sent, whatever the client's redirect policy: any redirect of LoadGraph's GET, and a 307 or 308
of a task's POST. A 301, 302 or 303 of a task's POST, which would resend it as a GET without its body, is an error
with that status, and so is more than 10 redirects. A redirect carries the request's headers, baggage included, to
its target, on another host too.

LoadGraph sends a GET to the workflow URL and decodes the reply's body as a workflow.Graph in its JSON encoding
(Graph.MarshalJSON). A 404 is a graph that does not exist. A server that cannot be reached, does not reply in time,
or answers 429, 502, 503 or 504 is an error with status 503, which a caller of Create or a task's flow.Subgraph can
retry. Any other status is an error with that status.

ExecuteTask sends a POST to the task URL whose body is the step's carrier (workflow.RequestCarrier), with these
headers beside Content-Type and Accept, both application/json:

	Idempotency-Key       the step key
	Dwarf-Attempt         the step's dispatch count, flow.Attempt
	Dwarf-Budget-Millis   the whole milliseconds left in the step's time budget
	Dwarf-Baggage         the flow's baggage as JSON in printable ASCII, when it has any

A proxy in front of the task server may refuse large headers, often past about 8 KB, so baggage that travels this
way stays small. The request runs under the step's time budget, and the outcome is:

  - A 2xx reply: its body is the task's result (workflow.ResultCarrier), its changes and control signals. A 2xx
    without one, such as a 204, is an error with status 502.
  - A server that cannot be reached, a reply of 429, 502, 503 or 504, or a reply lost after the request was sent:
    the step is dispatched again later, after the reply's Retry-After if it names one, else after a backoff from
    1s doubling to 30s. Past RetryHorizon the step fails with status 503 and the server's message, if it sent one.
  - Any other status: the task's error, with that status and the body as its message (the "error" field of a JSON
    body, or the body's text).
  - No reply within the time budget, the request having been sent: the step fails with status 408.
  - A certificate the client refuses: status 502. A request the client cannot send: status 400.

A step waits the way flow.Retry retries it, with two consequences. A wait on a step run again after Resume, or
after its subgraph returned, discards that resume data or that subgraph's result, and the subgraph runs again. And
the horizon counts the step's earlier dispatches, the task's own retries included, so a step that has already
retried many times gets less of it.

Execution is at-least-once. The step key is the same on every run of the step: after a lost reply, after
flow.Retry, after Resume and after its subgraph returns. A task server makes the task's side effects idempotent by
it; one that stores replies by it must not replay a reply that carries a control signal, since the next run of the
step needs a different one. The standard library's client treats a request with an Idempotency-Key as safe to send
again, and may resend one on a reused connection that fails before the reply, under the same Dwarf-Attempt. A task
server that decodes the request with workflow.RequestCarrier and encodes its reply with workflow.ResultCarrier
replies in the request's carrier version.
*/
type Remote struct {
	// Client sends the requests. Nil uses a client of Remote's own: a copy, made when the package loads, of the
	// standard library's default transport, keeping up to 512 idle connections to each task server rather than 2.
	// Over HTTP/1.1 each request in flight holds a connection, so the requests in flight to one task server address
	// are bounded by the engine host's ephemeral ports; HTTP/2 over TLS carries them on a few connections.
	Client *http.Client
	// RetryHorizon bounds how long a step waits for a task server that is busy or cannot be reached: the
	// backoff summed over its dispatches, as flow.Retry's giveUpAfter bounds it. Zero is DefaultRetryHorizon; a
	// negative value fails the step at once.
	RetryHorizon time.Duration
}

// LoadGraph implements engine.Host.
func (r *Remote) LoadGraph(ctx context.Context, workflowURL string) (graph *workflow.Graph, err error) {
	if !isHTTP(workflowURL) {
		return nil, nil
	}
	var p progress
	req, err := http.NewRequestWithContext(p.trace(ctx), http.MethodGet, workflowURL, nil)
	if err != nil {
		return nil, errors.New("invalid workflow URL %s", workflowURL, err, http.StatusBadRequest)
	}
	req.Header.Set("Accept", "application/json")
	if err := setBaggage(ctx, req); err != nil {
		return nil, err
	}
	resp, err := r.client().Do(req)
	if err != nil {
		return nil, graphFailure(ctx, workflowURL, err, &p)
	}
	defer drain(resp.Body)
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, nil
	case busy(resp.StatusCode):
		return nil, errors.Trace(replyError(resp), http.StatusServiceUnavailable)
	case resp.StatusCode/100 != 2:
		return nil, replyError(resp)
	}
	body, err := readReply(resp.Body)
	if err == errReplyTooLarge {
		return nil, errors.New("graph from %s is over %d bytes", workflowURL, maxReplyBytes, http.StatusBadGateway)
	}
	if err != nil {
		return nil, graphFailure(ctx, workflowURL, err, &p)
	}
	graph = &workflow.Graph{}
	if err := json.Unmarshal(body, graph); err != nil {
		return nil, errors.New("graph from %s cannot be decoded", workflowURL, err, http.StatusBadGateway)
	}
	return graph, nil
}

// ExecuteTask implements engine.Host.
func (r *Remote) ExecuteTask(ctx context.Context, taskURL string, flow *workflow.Flow) error {
	if !isHTTP(taskURL) {
		return errors.New("task not found: %s", taskURL, http.StatusNotFound)
	}
	body, err := json.Marshal(workflow.RequestCarrier(flow))
	if err != nil {
		return errors.Trace(err)
	}
	var p progress
	req, err := http.NewRequestWithContext(p.trace(ctx), http.MethodPost, taskURL, bytes.NewReader(body))
	if err != nil {
		return errors.New("invalid task URL %s", taskURL, err, http.StatusBadRequest)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set(HeaderIdempotencyKey, flow.StepKey())
	req.Header.Set(HeaderAttempt, strconv.Itoa(flow.Attempt()))
	if deadline, ok := ctx.Deadline(); ok {
		req.Header.Set(HeaderBudgetMillis, strconv.FormatInt(max(0, time.Until(deadline).Milliseconds()), 10))
	}
	if err := setBaggage(ctx, req); err != nil {
		return err
	}
	resp, err := r.client().Do(req)
	if err != nil {
		return r.taskFailure(ctx, taskURL, flow, err, &p)
	}
	defer drain(resp.Body)
	switch {
	case busy(resp.StatusCode):
		return r.wait(flow, retryAfter(resp.Header.Get("Retry-After")), replyError(resp))
	case resp.StatusCode/100 == 3 && resp.Header.Get("Location") != "":
		return errors.New("task %s redirects with %s to %s, which would resend it without its body", taskURL, resp.Status, resp.Header.Get("Location"), resp.StatusCode)
	case resp.StatusCode/100 != 2:
		return replyError(resp)
	}
	reply, err := readReply(resp.Body)
	if err == errReplyTooLarge {
		return errors.New("reply from %s is over %d bytes", taskURL, maxReplyBytes, http.StatusBadGateway)
	}
	if err != nil {
		return r.taskFailure(ctx, taskURL, flow, err, &p)
	}
	if err := json.Unmarshal(reply, workflow.ResultCarrier(flow)); err != nil {
		return errors.New("reply from %s cannot be decoded", taskURL, err, http.StatusBadGateway)
	}
	return nil
}

// taskFailure is ExecuteTask's outcome for a request that got no complete reply.
func (r *Remote) taskFailure(ctx context.Context, taskURL string, flow *workflow.Flow, err error, p *progress) error {
	switch p.classify(ctx, err) {
	case failInvalid:
		return errors.New("request to %s cannot be sent", taskURL, err, http.StatusBadRequest)
	case failCertificate:
		return errors.New("certificate of %s refused", taskURL, err, http.StatusBadGateway)
	case failRedirects:
		return errors.New("task %s redirects more than %d times", taskURL, maxRedirects, http.StatusBadGateway)
	case failCancelled:
		return errors.Trace(ctx.Err())
	case failTimedOut:
		return errors.New("task %s did not reply within its time budget", taskURL, http.StatusRequestTimeout)
	case failAbsent:
		return r.wait(flow, 0, errors.New("task server for %s cannot be reached", taskURL, err))
	default:
		return r.wait(flow, 0, errors.New("reply from %s was lost", taskURL, err))
	}
}

// graphFailure is LoadGraph's outcome for a request that got no complete reply.
func graphFailure(ctx context.Context, workflowURL string, err error, p *progress) error {
	switch p.classify(ctx, err) {
	case failInvalid:
		return errors.New("request to %s cannot be sent", workflowURL, err, http.StatusBadRequest)
	case failCertificate:
		return errors.New("certificate of %s refused", workflowURL, err, http.StatusBadGateway)
	case failRedirects:
		return errors.New("graph %s redirects more than %d times", workflowURL, maxRedirects, http.StatusBadGateway)
	case failCancelled:
		return errors.Trace(ctx.Err())
	default:
		return errors.New("graph server for %s cannot be reached", workflowURL, err, http.StatusServiceUnavailable)
	}
}

// wait arms a retry of the step for a task server that is not taking it now, and returns nil; past the retry
// horizon it returns cause with status 503. A Retry-After replaces the backoff with that constant delay, so the
// horizon counts it.
func (r *Remote) wait(flow *workflow.Flow, after time.Duration, cause error) error {
	initial, multiplier, maxDelay := retryDelay, float64(retryMultiplier), retryMaxDelay
	if after > 0 {
		initial, multiplier, maxDelay = after, 1, after
	}
	if horizon := r.horizon(); horizon > 0 && flow.Retry(initial, multiplier, maxDelay, horizon) {
		return nil
	}
	return errors.Trace(cause, http.StatusServiceUnavailable)
}

// client is the client Remote sends with, following only a redirect that keeps the request's method.
func (r *Remote) client() *http.Client {
	c := *defaultClient
	if r.Client != nil {
		c = *r.Client
	}
	c.CheckRedirect = keepTheMethod
	return &c
}

// maxRedirects is how many redirects a request follows, as the standard library's default policy does.
const maxRedirects = 10

// errTooManyRedirects stops a request redirected more than maxRedirects times.
var errTooManyRedirects = errors.New("too many redirects")

// keepTheMethod follows a redirect only if the next request keeps the first one's method. On a 301, 302 or 303 the
// client turns a POST into a GET without its body, so it returns that reply to the caller instead, and a task whose
// input never arrived cannot reply with a result.
func keepTheMethod(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return errTooManyRedirects
	}
	if req.Method != via[0].Method {
		return http.ErrUseLastResponse
	}
	return nil
}

func (r *Remote) horizon() time.Duration {
	if r.RetryHorizon == 0 {
		return DefaultRetryHorizon
	}
	return r.RetryHorizon
}

// progress records how far a request got, which tells a server that never received it from a reply that was lost.
type progress struct {
	connecting atomic.Bool // the transport asked for a connection
	wrote      atomic.Bool // the whole request went out
}

// trace returns ctx carrying the hooks that record p.
func (p *progress) trace(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(string) { p.connecting.Store(true) },
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				p.wrote.Store(true)
			}
		},
	})
}

// failure is what became of a request that got no complete reply.
type failure int

const (
	failInvalid     failure = iota // refused before a connection was asked for: the request itself is at fault
	failCertificate                // the server's certificate was refused
	failRedirects                  // redirected more than maxRedirects times
	failCancelled                  // the caller's context was cancelled
	failAbsent                     // the request never reached the server
	failTimedOut                   // the request reached the server, and its deadline passed before the reply
	failLost                       // the request reached the server, and its reply was lost
)

// classify tells what became of a request whose round trip or body read failed with err.
func (p *progress) classify(ctx context.Context, err error) failure {
	var certErr *tls.CertificateVerificationError
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return failCancelled
	case !p.connecting.Load() && ctx.Err() == nil:
		return failInvalid
	case errors.As(err, &certErr):
		return failCertificate
	case errors.Is(err, errTooManyRedirects):
		return failRedirects
	case !p.wrote.Load():
		return failAbsent
	case ctx.Err() != nil:
		return failTimedOut
	}
	return failLost
}

// isHTTP reports whether u is an http or https URL.
func isHTTP(u string) bool {
	parsed, err := url.Parse(u)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}

// busy reports whether a status asks the caller to come back later, from the server or a proxy in front of it.
func busy(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// setBaggage puts the flow's baggage, if it has any, on req.
func setBaggage(ctx context.Context, req *http.Request) error {
	baggage := workflow.BaggageFrom(ctx)
	if baggage.Len() == 0 {
		return nil
	}
	b, err := json.Marshal(baggage)
	if err != nil {
		return errors.Trace(err)
	}
	req.Header.Set(HeaderBaggage, asciiJSON(b))
	return nil
}

// asciiJSON rewrites JSON so every character outside printable ASCII is a \u escape, for a header: the encoder
// leaves DEL and non-ASCII text as they are, which a header refuses or a proxy may mangle. Such characters can
// only be inside the JSON's strings, where an escape means the same.
func asciiJSON(b []byte) string {
	var sb strings.Builder
	for _, c := range string(b) {
		switch {
		case c >= 0x20 && c < 0x7F:
			sb.WriteRune(c)
		case c > 0xFFFF:
			hi, lo := utf16.EncodeRune(c)
			fmt.Fprintf(&sb, `\u%04x\u%04x`, hi, lo)
		default:
			fmt.Fprintf(&sb, `\u%04x`, c)
		}
	}
	return sb.String()
}

// retryAfter reads a Retry-After header, in seconds or as an HTTP date, as a delay; zero when absent, past or
// malformed.
func retryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(min(max(secs, 0), 1<<30)) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(0, time.Until(t))
	}
	return 0
}

// replyError is the error a reply other than a 2xx carries: its status, and the body as its message - the "error"
// field of a JSON body, else the body's text, else the status line.
func replyError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	msg := strings.TrimSpace(string(body))
	var jsonBody struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &jsonBody) == nil && jsonBody.Error != "" {
		msg = jsonBody.Error
	}
	if msg == "" {
		msg = resp.Status
	}
	return errors.New("%s", msg, resp.StatusCode)
}

// errReplyTooLarge is readReply's error for a body over maxReplyBytes.
var errReplyTooLarge = errors.New("reply too large")

// readReply reads a 2xx reply's body, up to maxReplyBytes.
func readReply(body io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(body, maxReplyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxReplyBytes {
		return nil, errReplyTooLarge
	}
	return b, nil
}

// drain reads what is left of a body, up to a bound, and closes it, so its connection can be reused.
func drain(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxErrorBody))
	_ = body.Close()
}
