package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/protocol"
)

// fakePoll records every PollRequest and replays scripted results; once the
// script is exhausted it returns an empty ok response (mirroring the real
// server, which always answers a successful poll with ok:true).
type fakePoll struct {
	mu       sync.Mutex
	requests []*protocol.PollRequest
	script   []pollResult
}

type pollResult struct {
	resp *protocol.PollResponse
	err  error
}

func (f *fakePoll) poll(_ context.Context, req *protocol.PollRequest) (*protocol.PollResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if len(f.script) > 0 {
		r := f.script[0]
		f.script = f.script[1:]
		return r.resp, r.err
	}
	return &protocol.PollResponse{OK: true}, nil
}

func (f *fakePoll) numRequests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *fakePoll) request(i int) *protocol.PollRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[i]
}

// findReport scans all poll requests for a report matching commandID and
// status (empty status matches any).
func (f *fakePoll) findReport(commandID, status string) *protocol.Report {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, req := range f.requests {
		for _, rep := range req.Reports {
			if rep.CommandID == commandID && (status == "" || rep.Status == status) {
				return rep
			}
		}
	}
	return nil
}

// recordHandler records invocations.
type recordHandler struct {
	calls chan *protocol.Command
}

func (h *recordHandler) HandleCommand(_ context.Context, cmd *protocol.Command, _ func(*protocol.Report)) {
	h.calls <- cmd
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// logCapture is a slog.Handler that records log messages for assertions.
type logCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *logCapture) WithGroup(string) slog.Handler            { return c }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, r.Message)
	return nil
}

// count returns how many recorded messages contain substr.
func (c *logCapture) count(substr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.msgs {
		if strings.Contains(m, substr) {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type fixture struct {
	cfg  *config.Config
	priv ed25519.PrivateKey
	poll *fakePoll
	h    *recordHandler
}

// newFixture builds a scripted Receiver fixture with a short poll interval.
// It does not start the Receiver: tests may extend the script (e.g. with
// commands signed by f.priv) before calling start.
func newFixture(t *testing.T, writerEnabled bool, script ...pollResult) *fixture {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{
		cfg: &config.Config{
			ServerPublicKey: base64.StdEncoding.EncodeToString(pub),
			ClusterID:       "test-cluster",
			WriterEnabled:   writerEnabled,
			PollInterval:    5 * time.Millisecond,
		},
		priv: priv,
		poll: &fakePoll{script: script},
		h:    &recordHandler{calls: make(chan *protocol.Command, 16)},
	}
}

func (f *fixture) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go New(f.cfg, f.poll.poll, f.h, testLogger(), nil).Run(ctx)
}

// signedCommand returns a valid signed command; mutate after Sign to tamper.
func (f *fixture) signedCommand(id, nonce string) *protocol.Command {
	cmd := &protocol.Command{
		ID:        id,
		Nonce:     nonce,
		Timestamp: time.Now().Unix(),
		Verb:      protocol.VerbApply,
		Target:    protocol.ResourceRef{Version: "v1", Resource: "configmaps", Namespace: "default", Name: "cm"},
	}
	cmd.Sign(f.priv)
	return cmd
}

// noCallFor drains recorded handler calls and fails if refusedID was
// dispatched.
func (f *fixture) noCallFor(t *testing.T, refusedID string) {
	t.Helper()
	for {
		select {
		case cmd := <-f.h.calls:
			if cmd.ID == refusedID {
				t.Fatalf("handler unexpectedly invoked with refused command %s", cmd.ID)
			}
		case <-time.After(200 * time.Millisecond):
			return
		}
	}
}

func reportIn(req *protocol.PollRequest, commandID string) *protocol.Report {
	for _, rep := range req.Reports {
		if rep.CommandID == commandID {
			return rep
		}
	}
	return nil
}

func TestValidCommandDispatchedAndReportedOnNextPoll(t *testing.T) {
	f := newFixture(t, true)
	f.poll.script = []pollResult{{resp: &protocol.PollResponse{Commands: []*protocol.Command{f.signedCommand("cmd-1", "nonce-1")}}}}
	f.start(t)

	select {
	case got := <-f.h.calls:
		if got.ID != "cmd-1" {
			t.Fatalf("handler got command %q, want cmd-1", got.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// The received report is piggybacked on the next poll's request.
	waitFor(t, "second poll", func() bool { return f.poll.numRequests() >= 2 })
	rep := reportIn(f.poll.request(1), "cmd-1")
	if rep == nil {
		t.Fatalf("second poll carried no report for cmd-1: %+v", f.poll.request(1).Reports)
	}
	if rep.Status != protocol.StatusReceived {
		t.Fatalf("status = %q, want received", rep.Status)
	}
	if rep.ClusterID != "test-cluster" || rep.Timestamp == 0 {
		t.Fatalf("report not stamped: %+v", rep)
	}
	if req := f.poll.request(1); req.ClusterID != "test-cluster" || req.Version == "" {
		t.Fatalf("poll request not stamped: %+v", req)
	}
}

func TestPollErrorRetainsReports(t *testing.T) {
	f := newFixture(t, true)
	f.poll.script = []pollResult{
		{resp: &protocol.PollResponse{Commands: []*protocol.Command{f.signedCommand("cmd-e", "nonce-e")}}},
		{err: errors.New("connection refused")},
	}
	f.start(t)

	select {
	case <-f.h.calls:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// Poll 2 fails: the queued received report must survive to poll 3
	// (backoff starts at 1s, so this takes about a second).
	waitFor(t, "third poll", func() bool { return f.poll.numRequests() >= 3 })
	for i := 1; i < 3; i++ {
		if rep := reportIn(f.poll.request(i), "cmd-e"); rep == nil {
			t.Fatalf("poll %d lost the report for cmd-e after poll error", i+1)
		}
	}
	// After poll 3 succeeds, the report is cleared.
	waitFor(t, "report cleared", func() bool {
		return f.poll.numRequests() >= 4 && reportIn(f.poll.request(3), "cmd-e") == nil
	})
}

func TestPollNotOKRetainsReports(t *testing.T) {
	f := newFixture(t, true)
	f.poll.script = []pollResult{
		{resp: &protocol.PollResponse{OK: true, Commands: []*protocol.Command{f.signedCommand("cmd-nok", "nonce-nok")}}},
		{resp: &protocol.PollResponse{OK: false}}, // 200 but not ok: must not clear
	}
	logs := &logCapture{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go New(f.cfg, f.poll.poll, f.h, slog.New(logs), nil).Run(ctx)

	select {
	case <-f.h.calls:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// Poll 2 carries the queued received report but answers ok:false, so the
	// report must survive and be resent on poll 3.
	waitFor(t, "third poll", func() bool { return f.poll.numRequests() >= 3 })
	for i := 1; i < 3; i++ {
		if rep := reportIn(f.poll.request(i), "cmd-nok"); rep == nil {
			t.Fatalf("poll %d lost the report for cmd-nok after an ok:false response", i+1)
		}
	}
	if n := logs.count("poll response not ok"); n != 1 {
		t.Fatalf("ok:false logged %d times, want once", n)
	}
	// Poll 3 answers ok, so the report is cleared afterwards.
	waitFor(t, "report cleared", func() bool {
		return f.poll.numRequests() >= 4 && reportIn(f.poll.request(3), "cmd-nok") == nil
	})
}

func TestAuthErrorLoggedOncePerStreakAndRetried(t *testing.T) {
	f := newFixture(t, true)
	f.poll.script = []pollResult{
		{resp: &protocol.PollResponse{Commands: []*protocol.Command{f.signedCommand("cmd-a", "nonce-a")}}},
		{err: &protocol.AuthError{Status: http.StatusUnauthorized, Body: "bad token"}},
		{err: &protocol.AuthError{Status: http.StatusUnauthorized, Body: "bad token"}},
	}
	logs := &logCapture{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go New(f.cfg, f.poll.poll, f.h, slog.New(logs), nil).Run(ctx)

	select {
	case <-f.h.calls:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// Polls 2 and 3 are rejected; the receiver backs off (1s, then 2s) and
	// retries, retaining the queued received report on every attempt.
	waitFor(t, "poll retried after auth backoff", func() bool { return f.poll.numRequests() >= 4 })
	for i := 1; i < 4; i++ {
		if rep := reportIn(f.poll.request(i), "cmd-a"); rep == nil {
			t.Fatalf("poll %d lost the report for cmd-a after auth error", i+1)
		}
	}
	// The auth failure is shouted once per streak, not warned per retry.
	if n := logs.count("control plane rejected the agent token"); n != 1 {
		t.Fatalf("auth error logged %d times, want once per streak", n)
	}
	if n := logs.count("command poll failed"); n != 0 {
		t.Fatalf("auth error also logged as generic poll failure %d times", n)
	}
}

func TestRefusals(t *testing.T) {
	cases := []struct {
		name    string
		writer  bool
		prepare func(f *fixture) []*protocol.Command
	}{
		{
			name:   "tampered payload",
			writer: true,
			prepare: func(f *fixture) []*protocol.Command {
				cmd := f.signedCommand("cmd-t", "nonce-t")
				cmd.Payload = []byte("tampered")
				return []*protocol.Command{cmd}
			},
		},
		{
			name:   "stale timestamp",
			writer: true,
			prepare: func(f *fixture) []*protocol.Command {
				cmd := f.signedCommand("cmd-s", "nonce-s")
				cmd.Timestamp = time.Now().Add(-10 * time.Minute).Unix()
				cmd.Sign(f.priv)
				return []*protocol.Command{cmd}
			},
		},
		{
			name:   "replayed nonce",
			writer: true,
			prepare: func(f *fixture) []*protocol.Command {
				// Same nonce twice: first is accepted, second is a replay.
				return []*protocol.Command{f.signedCommand("cmd-r1", "nonce-r"), f.signedCommand("cmd-r2", "nonce-r")}
			},
		},
		{
			name:   "writer disabled",
			writer: false,
			prepare: func(f *fixture) []*protocol.Command {
				return []*protocol.Command{f.signedCommand("cmd-w", "nonce-w")}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.writer)
			cmds := tc.prepare(f)
			f.poll.script = []pollResult{{resp: &protocol.PollResponse{Commands: cmds}}}
			f.start(t)
			refusedID := cmds[len(cmds)-1].ID

			waitFor(t, "refused report on next poll", func() bool {
				return f.poll.numRequests() >= 2 && reportIn(f.poll.request(1), refusedID) != nil
			})
			rep := reportIn(f.poll.request(1), refusedID)
			if rep.Status != protocol.StatusRefused {
				t.Fatalf("status = %q, want refused (%s)", rep.Status, rep.Message)
			}
			if rep.ClusterID != "test-cluster" || rep.Timestamp == 0 {
				t.Fatalf("report not stamped: %+v", rep)
			}
			f.noCallFor(t, refusedID)
		})
	}
}

// reportingHandler reports a terminal status from the handler goroutine.
type reportingHandler struct {
	calls chan *protocol.Command
}

func (h *reportingHandler) HandleCommand(_ context.Context, cmd *protocol.Command, report func(*protocol.Report)) {
	report(&protocol.Report{Status: protocol.StatusSucceeded, Message: "done", Progress: 100})
	h.calls <- cmd
}

func TestHandlerReportEnqueued(t *testing.T) {
	f := newFixture(t, true)
	f.poll.script = []pollResult{{resp: &protocol.PollResponse{Commands: []*protocol.Command{f.signedCommand("cmd-h", "nonce-h")}}}}
	h := &reportingHandler{calls: make(chan *protocol.Command, 16)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go New(f.cfg, f.poll.poll, h, testLogger(), nil).Run(ctx)

	select {
	case <-h.calls:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// Both the received report and the handler's succeeded report are
	// stamped and piggybacked on subsequent polls.
	waitFor(t, "handler report on a later poll", func() bool {
		return f.poll.findReport("cmd-h", protocol.StatusSucceeded) != nil
	})
	if rep := f.poll.findReport("cmd-h", protocol.StatusReceived); rep == nil {
		t.Fatal("received report never piggybacked")
	}
	succeeded := f.poll.findReport("cmd-h", protocol.StatusSucceeded)
	if succeeded.ClusterID != "test-cluster" || succeeded.CommandID != "cmd-h" || succeeded.Timestamp == 0 {
		t.Fatalf("handler report not stamped: %+v", succeeded)
	}
	if succeeded.Message != "done" || succeeded.Progress != 100 {
		t.Fatalf("handler report fields lost: %+v", succeeded)
	}
}

func TestReportQueueCapDropsOldest(t *testing.T) {
	q := newReportQueue(3, testLogger())
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		q.enqueue(&protocol.Report{CommandID: id, Status: protocol.StatusSucceeded})
	}
	snap := q.snapshot()
	if len(snap) != 3 {
		t.Fatalf("queue len = %d, want 3", len(snap))
	}
	for i, want := range []string{"c", "d", "e"} {
		if snap[i].CommandID != want {
			t.Fatalf("snapshot[%d] = %q, want %q (oldest must be dropped)", i, snap[i].CommandID, want)
		}
	}
}

func TestReportQueueClearSentKeepsNew(t *testing.T) {
	q := newReportQueue(10, testLogger())
	q.enqueue(&protocol.Report{CommandID: "sent-1"})
	q.enqueue(&protocol.Report{CommandID: "sent-2"})
	snap := q.snapshot()
	q.enqueue(&protocol.Report{CommandID: "new"})
	q.clearSent(snap)
	got := q.snapshot()
	if len(got) != 1 || got[0].CommandID != "new" {
		t.Fatalf("queue after clearSent = %+v, want only the new report", got)
	}
}
