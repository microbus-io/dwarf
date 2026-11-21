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
	"unicode/utf8"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// The headers Remote sets on a task request, beside Content-Type and Accept.
const (
	// HeaderIdempotencyKey carries the step key. See Remote for what it identifies.
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderAttempt carries the step's retry attempt, flow.Attempt.
	HeaderAttempt = "Dwarf-Attempt"
	// HeaderBudgetMillis carries the whole milliseconds left in the step's time budget.
	HeaderBudgetMillis = "Dwarf-Budget-Millis"
	// HeaderBaggage carries the flow's baggage as JSON in printable ASCII, on graph and task requests alike, when
	// it has any.
	HeaderBaggage = "Dwarf-Baggage"
)

// DefaultHTTPRetryHorizon is how long Remote keeps a step waiting for its task server when HTTPRetryHorizon is zero.
const DefaultHTTPRetryHorizon = 10 * time.Minute

// The backoff of a step waiting for a task server that gave no answer.
const (
	retryDelay      = time.Second
	retryMultiplier = 2
	retryMaxDelay   = 30 * time.Second
)

// maxErrorBody bounds how much of an error reply's body is read, and maxErrorMessage how much of it becomes the
// error's message. A JSON error whose properties or stack run past the message's bound still decodes whole.
const (
	maxErrorBody    = 1 << 20
	maxErrorMessage = 16 << 10
)

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
with that status. More than 10 redirects, and a redirect that cannot be followed - a Location that does not parse,
or names no http or https URL - are errors with status 502, a 301, 302 or 303 of a task's POST included when its
Location does not parse. A redirect carries the request's headers, baggage included, to
its target, on another host too.

LoadGraph sends a GET to the workflow URL and decodes the reply's body as a workflow.Graph in its JSON encoding
(Graph.MarshalJSON). A 404 is a graph that does not exist, and any other status that is not a 2xx is an error with
that status. A server that cannot be reached or does not reply in time is an error with status 503, which a caller
of Create or a task's flow.Subgraph can retry.

ExecuteTask sends a POST to the task URL whose body is the step's carrier (workflow.RequestCarrier), with these
headers beside Content-Type and Accept, both application/json:

	Idempotency-Key       the step key
	Dwarf-Attempt         the step's retry attempt, flow.Attempt
	Dwarf-Budget-Millis   the whole milliseconds left in the step's time budget
	Dwarf-Baggage         the flow's baggage as JSON in printable ASCII, when it has any

A proxy in front of the task server may refuse large headers, often past about 8 KB, so baggage that travels this
way stays small. The request runs under the step's time budget, and the outcome is:

  - A 2xx reply: its body is the task's result (workflow.ResultCarrier), its changes and control signals. A 2xx
    without one, such as a 204, is an error with status 502.
  - Any other status: the task's error, with that status and the body as its message (the "error" field of a JSON
    body, or the body's text). A JSON body's other fields are the error's properties, as the errors package encodes
    them: a task server that encodes the cancellation flow.Retry or flow.Defer returned ends the step cancelled.
    Remote reads no status as a request to wait, a 429 or a 503 included: a task server that wants the step run
    again later replies with a result carrying flow.Retry or flow.Defer.
  - No answer - a server that cannot be reached, or a reply lost after the request was sent: the step is
    dispatched again later, after a backoff from 1s doubling to 30s. Past HTTPRetryHorizon the step fails with
    status 503.
  - No reply within the time budget, the request having been sent: the step fails with status 408.
  - A certificate the client refuses: status 502. A request the client cannot send: status 400.

A step waits with flow.Defer, so it keeps what it has resolved: run again after Resume it has the same resume data,
and after its subgraph returned the same result. A request sent again after a wait carries the same Dwarf-Attempt.
The horizon counts the step's deferrals (flow.Deferrals) since it last started over or got its answer, never the
task's own retries, but the task's own Defers count: a task that has deferred itself many times leaves its step less
of the horizon. A step that runs after its flow was cancelled does not wait: ExecuteTask returns the cancellation.

Execution is at-least-once. The step key is the same on every run of the step: after a lost reply, after
flow.Retry or flow.Defer, after Resume and after its subgraph returns. A task server makes the task's side effects idempotent by
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
	//
	// Remote tells a server it could not reach from a reply it lost by the request's progress, which the client's
	// transport reports through net/http/httptrace, as the standard library's does; with a transport that reports
	// none it cannot tell them apart, and a refused connection is an error with status 400 rather than a wait. The
	// client's Timeout bounds the whole exchange, every redirect and the reply's body included, as the step's time
	// budget does: a request sent and unanswered by then fails with status 408.
	Client *http.Client
	// HTTPRetryHorizon bounds how long a step waits for a task server that gives no answer: the backoff summed over
	// the step's deferrals since it last started over or got its answer, its task's own included, as flow.Defer's
	// giveUpAfter bounds it. Zero is DefaultHTTPRetryHorizon; a negative value fails the step at once.
	HTTPRetryHorizon time.Duration
}

// LoadGraph implements engine.Host.
func (r *Remote) LoadGraph(ctx context.Context, workflowURL string) (graph *workflow.Graph, err error) {
	if !isHTTP(workflowURL) {
		return nil, nil
	}
	header := http.Header{}
	header.Set("Accept", "application/json")
	if err := setBaggage(ctx, header); err != nil {
		return nil, errors.Trace(err)
	}
	client, ctx, cancel := r.prepare(ctx)
	defer cancel()
	resp, p, err := r.send(ctx, client, http.MethodGet, workflowURL, nil, header)
	if err != nil {
		return nil, graphFailure(ctx, workflowURL, err, p)
	}
	defer drain(resp.Body)
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, nil
	case isRedirect(resp):
		return nil, redirectRefused("graph server for "+workflowURL, http.MethodGet, resp)
	case resp.StatusCode/100 != 2:
		return nil, replyError(resp)
	}
	body, err := readReply(resp.Body)
	if err == errReplyTooLarge {
		return nil, errors.New("graph from %s is over %d bytes", workflowURL, maxReplyBytes, http.StatusBadGateway)
	}
	if err != nil {
		return nil, graphFailure(ctx, workflowURL, err, p)
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
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Accept", "application/json")
	header.Set(HeaderIdempotencyKey, flow.StepKey())
	header.Set(HeaderAttempt, strconv.Itoa(flow.Attempt()))
	if err := setBaggage(ctx, header); err != nil {
		return errors.Trace(err)
	}
	client, ctx, cancel := r.prepare(ctx)
	defer cancel()
	resp, p, err := r.send(ctx, client, http.MethodPost, taskURL, body, header)
	if err != nil {
		return r.taskFailure(ctx, taskURL, flow, err, p)
	}
	defer drain(resp.Body)
	switch {
	case isRedirect(resp):
		return redirectRefused("task "+taskURL, http.MethodPost, resp)
	case resp.StatusCode/100 != 2:
		return replyError(resp)
	}
	reply, err := readReply(resp.Body)
	if err == errReplyTooLarge {
		return errors.New("reply from %s is over %d bytes", taskURL, maxReplyBytes, http.StatusBadGateway)
	}
	if err != nil {
		return r.taskFailure(ctx, taskURL, flow, err, p)
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
	case failUnfollowable:
		return errors.New("task %s replied with a redirect that cannot be followed", taskURL, err, http.StatusBadGateway)
	case failCancelled:
		return errors.Trace(ctx.Err())
	case failTimedOut:
		return errors.New("task %s did not reply within its time budget", taskURL, http.StatusRequestTimeout)
	case failAbsent:
		return r.wait(flow, errors.New("task server for %s cannot be reached", taskURL, err))
	default:
		return r.wait(flow, errors.New("reply from %s was lost", taskURL, err))
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
	case failUnfollowable:
		return errors.New("graph server for %s replied with a redirect that cannot be followed", workflowURL, err, http.StatusBadGateway)
	case failCancelled:
		return errors.Trace(ctx.Err())
	default:
		return errors.New("graph server for %s cannot be reached", workflowURL, err, http.StatusServiceUnavailable)
	}
}

// wait defers the step for a task server that gave no answer, and returns nil; past the retry horizon it returns
// cause with status 503, and on a cancelled flow the cancellation.
func (r *Remote) wait(flow *workflow.Flow, cause error) error {
	horizon := r.horizon()
	if horizon < 0 {
		// No wait at all, but a cancelled flow still answers with its cancellation: no backoff fits a bound this
		// short, so the Defer arms nothing.
		horizon = time.Nanosecond
	}
	yield, err := flow.Defer(retryDelay, retryMultiplier, retryMaxDelay, horizon)
	if yield || err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cause, http.StatusServiceUnavailable)
}

// client is the client Remote sends with. It follows no redirect itself: send does, a hop at a time.
func (r *Remote) client() *http.Client {
	c := *defaultClient
	if r.Client != nil {
		c = *r.Client
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base := c.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	c.Transport = replyRecorder{base: base}
	return &c
}

// replyRecorder marks a request's progress once its round trip brought a reply, so an error the client raises after
// it - on a Location it cannot parse - is told from a request that got no reply.
type replyRecorder struct{ base http.RoundTripper }

func (t replyRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if p, ok := req.Context().Value(progressKey{}).(*progress); ok && err == nil {
		p.replied.Store(true)
	}
	return resp, err
}

// errUnfollowable marks a reply whose redirect the client could not follow.
var errUnfollowable = errors.New("redirect cannot be followed")

// prepare returns the client to send with and ctx bounded by that client's Timeout, which the client then no longer
// applies itself. As a deadline on the context, the Timeout covers the whole exchange - every redirect and the reply's
// body, as the standard library defines it - and classify tells it from a connection the network gave up on, whose
// error also reports a timeout and is a reply lost.
func (r *Remote) prepare(ctx context.Context) (*http.Client, context.Context, context.CancelFunc) {
	c := r.client()
	if c.Timeout <= 0 {
		return c, ctx, func() {}
	}
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	c.Timeout = 0
	return c, ctx, cancel
}

// maxRedirects is how many redirects a request follows; one more is an error.
const maxRedirects = 10

// errTooManyRedirects stops a request redirected more than maxRedirects times.
var errTooManyRedirects = errors.New("too many redirects")

// send sends a request with the given body and header, and follows each redirect that keeps it as sent - any of a
// GET, a 307 or 308 of a POST - up to maxRedirects. Each hop is a request of its own with a progress of its own: a
// hop's trace callbacks can arrive after its round trip returned, so one record for the whole chain would count an
// earlier hop's write for a later hop's server. It returns the final reply, a redirect it did not follow included,
// with its hop's progress, or the error and progress of the hop that failed. A task's request is told the time left in
// its budget afresh at each hop.
func (r *Remote) send(ctx context.Context, client *http.Client, method, target string, body []byte, header http.Header) (resp *http.Response, p *progress, err error) {
	for hops := 0; ; hops++ {
		p = &progress{}
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(p.trace(ctx), method, target, reader)
		if err != nil {
			return nil, p, err
		}
		req.Header = header.Clone()
		if deadline, ok := ctx.Deadline(); ok && method == http.MethodPost {
			req.Header.Set(HeaderBudgetMillis, strconv.FormatInt(max(0, time.Until(deadline).Milliseconds()), 10))
		}
		resp, err = client.Do(req)
		if err != nil && p.replied.Load() {
			return nil, p, fmt.Errorf("%w: %w", errUnfollowable, err) // the server answered: no lost reply to wait for
		}
		if err != nil || !keepsTheRequest(method, resp) {
			return resp, p, err
		}
		next, err := req.URL.Parse(resp.Header.Get("Location"))
		if err != nil || !isHTTP(next.String()) {
			return resp, p, nil // a Location that cannot be followed: the caller reports the redirect
		}
		drain(resp.Body)
		if hops >= maxRedirects {
			return nil, p, errTooManyRedirects
		}
		target = next.String()
	}
}

// isRedirect reports whether a reply redirects to its Location.
func isRedirect(resp *http.Response) bool {
	switch resp.StatusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return resp.Header.Get("Location") != ""
	}
	return false
}

// keepsTheRequest reports whether following a redirect sends the request as it was: any redirect of a GET, but only a
// 307 or 308 of a POST - the client resends a POST redirected by a 301, 302 or 303 as a GET without its body, and a
// task whose input never arrived cannot reply with a result.
func keepsTheRequest(method string, resp *http.Response) bool {
	if !isRedirect(resp) {
		return false
	}
	return method == http.MethodGet || resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect
}

// redirectRefused is the error for a redirect send did not follow, from what: a 301, 302 or 303 of a POST, with that
// status, or one whose Location cannot be followed, with status 502. The reply's own message and properties stay
// with it.
func redirectRefused(what, method string, resp *http.Response) error {
	refused := errors.Convert(replyError(resp))
	location := resp.Header.Get("Location")
	if keepsTheRequest(method, resp) {
		refused.Err = fmt.Errorf("%s redirects with %s to %q, which cannot be followed: %w", what, resp.Status, location, refused.Err)
		refused.StatusCode = http.StatusBadGateway
		return refused
	}
	refused.Err = fmt.Errorf("%s redirects with %s to %s, which would resend it as a GET without its body: %w", what, resp.Status, location, refused.Err)
	return refused
}

func (r *Remote) horizon() time.Duration {
	if r.HTTPRetryHorizon == 0 {
		return DefaultHTTPRetryHorizon
	}
	return r.HTTPRetryHorizon
}

// progress records how far a request got, which tells a server that never received it from a reply that was lost.
type progress struct {
	connecting atomic.Bool // the transport asked for a connection
	wrote      atomic.Bool // the whole request went out
	replied    atomic.Bool // its round trip brought a reply
}

// progressKey carries a request's progress on its context, for replyRecorder.
type progressKey struct{}

// trace returns ctx carrying p and the hooks that record it, for one request: a redirect's is another request, with
// a progress of its own.
func (p *progress) trace(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, progressKey{}, p)
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
	failInvalid      failure = iota // refused before a connection was asked for: the request itself is at fault
	failCertificate                 // the server's certificate was refused
	failRedirects                   // redirected more than maxRedirects times
	failUnfollowable                // a reply came, with a redirect the client could not follow
	failCancelled                   // the caller's context was cancelled
	failAbsent                      // the request never reached the server
	failTimedOut                    // the request reached the server, and its deadline passed before the reply
	failLost                        // the request reached the server, and its reply was lost
)

// classify tells what became of a request whose round trip or body read failed with err.
func (p *progress) classify(ctx context.Context, err error) failure {
	var certErr *tls.CertificateVerificationError
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return failCancelled
	case errors.As(err, &certErr):
		return failCertificate
	case errors.Is(err, errTooManyRedirects):
		return failRedirects
	case errors.Is(err, errUnfollowable):
		return failUnfollowable
	case !p.connecting.Load() && !p.replied.Load() && ctx.Err() == nil:
		return failInvalid
	case !p.wrote.Load() && !p.replied.Load():
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

// setBaggage puts the flow's baggage, if it has any, on header.
func setBaggage(ctx context.Context, header http.Header) error {
	baggage := workflow.BaggageFrom(ctx)
	if baggage.Len() == 0 {
		return nil
	}
	b, err := json.Marshal(baggage)
	if err != nil {
		return errors.Trace(err)
	}
	header.Set(HeaderBaggage, asciiJSON(b))
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

// replyError is the error a reply other than a 2xx carries: its status, and the body as its message - the "error"
// field of a JSON object, else the body's text, else the status line. A JSON object's other fields are the error's
// properties, as the errors package encodes them, so the cancellation flow.Retry and flow.Defer return keeps its
// marker across the wire. Each field is read on its own, so one of an unexpected type costs only itself; the
// errors package's own statusCode, stack and trace are not properties, and the reply's status stands.
func replyError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	var fields map[string]json.RawMessage
	var msg string
	if json.Unmarshal(body, &fields) == nil && json.Unmarshal(fields["error"], &msg) == nil && msg != "" {
		traced := &errors.TracedError{Err: fmt.Errorf("%s", truncated(msg)), StatusCode: resp.StatusCode}
		for name, raw := range fields {
			switch name {
			case "error", "statusCode", "stack", "trace":
				continue
			}
			var v any
			if json.Unmarshal(raw, &v) == nil {
				if traced.Properties == nil {
					traced.Properties = map[string]any{}
				}
				traced.Properties[name] = v
			}
		}
		return traced
	}
	msg = strings.TrimSpace(string(body))
	if msg == "" {
		msg = resp.Status
	}
	return errors.New("%s", truncated(msg), resp.StatusCode)
}

// truncated cuts a message to maxErrorMessage bytes, on a character boundary.
func truncated(msg string) string {
	if len(msg) <= maxErrorMessage {
		return msg
	}
	cut := maxErrorMessage
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut]
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
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxErrorMessage))
	_ = body.Close()
}
