package wsclient

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/protocol"
)

// wsTestServer upgrades /audit_ready/k8s/ws and plays the control-plane side
// of the protocol: it records hellos and reports, answers with its scripted
// ack, and pushes its commands once per connection when the ack accepts.
type wsTestServer struct {
	token          string
	commands       []*protocol.Command
	closeAfterPush bool // close the first connection right after the ack + push

	mu      sync.Mutex
	conns   int
	hellos  []*protocol.WsHello
	reports []*protocol.Report
}

func newWSTestServer(t *testing.T, s *wsTestServer) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	t.Cleanup(ts.Close)
	return ts
}

func (s *wsTestServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/audit_ready/k8s/ws" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := r.Context()

	s.mu.Lock()
	s.conns++
	firstConn := s.conns == 1
	s.mu.Unlock()

	_, data, err := conn.Read(ctx)
	if err != nil {
		return
	}
	var hello protocol.WsHello
	if err := json.Unmarshal(data, &hello); err != nil {
		return
	}
	s.mu.Lock()
	s.hellos = append(s.hellos, &hello)
	s.mu.Unlock()

	ack := &protocol.WsHelloAck{Accepted: true}
	if hello.Token != s.token {
		ack = &protocol.WsHelloAck{Accepted: false, Message: "unauthorized"}
	}
	if err := writeTestFrame(ctx, conn, ack); err != nil || !ack.Accepted {
		return
	}
	for _, cmd := range s.commands {
		if err := writeTestFrame(ctx, conn, &protocol.WsCommandMessage{Type: protocol.WsTypeCommand, Command: cmd}); err != nil {
			return
		}
	}
	if s.closeAfterPush && firstConn {
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
		return
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var msg protocol.WsReportMessage
		if err := json.Unmarshal(data, &msg); err != nil || msg.Report == nil {
			continue
		}
		s.mu.Lock()
		s.reports = append(s.reports, msg.Report)
		s.mu.Unlock()
	}
}

func writeTestFrame(ctx context.Context, conn *websocket.Conn, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, raw)
}

func (s *wsTestServer) numConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns
}

func (s *wsTestServer) hello(i int) *protocol.WsHello {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hellos[i]
}

func (s *wsTestServer) findReport(commandID string) *protocol.Report {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rep := range s.reports {
		if rep.CommandID == commandID {
			return rep
		}
	}
	return nil
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

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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

// startClient builds and runs a Client against ts until the test ends.
func startClient(t *testing.T, ts *httptest.Server, token string, log *slog.Logger) *Client {
	t.Helper()
	cfg := &config.Config{
		ServerEndpoint: ts.URL + "/audit_ready",
		ServerToken:    token,
		ClusterID:      "ws-test",
	}
	c := New(cfg, log)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c.Run(ctx)
	return c
}

func TestWSURL(t *testing.T) {
	cases := []struct {
		base, want string
	}{
		{"http://host/audit_ready", "ws://host/audit_ready/k8s/ws"},
		{"https://host:8443/audit_ready/", "wss://host:8443/audit_ready/k8s/ws"},
	}
	for _, tc := range cases {
		if got := wsURL(tc.base); got != tc.want {
			t.Errorf("wsURL(%q) = %q, want %q", tc.base, got, tc.want)
		}
	}
}

func TestHandshakeConnectsAndSendReportRoundTrip(t *testing.T) {
	srv := &wsTestServer{token: "good-token"}
	ts := newWSTestServer(t, srv)
	c := startClient(t, ts, "good-token", testLogger())

	waitFor(t, "client connected", c.Connected)

	hello := srv.hello(0)
	if hello.Type != protocol.WsTypeAgentHello {
		t.Fatalf("hello type = %q, want %q", hello.Type, protocol.WsTypeAgentHello)
	}
	if hello.Token != "good-token" || hello.ClusterID != "ws-test" || hello.Version == "" {
		t.Fatalf("hello not stamped: %+v", hello)
	}

	rep := &protocol.Report{CommandID: "cmd-1", Status: protocol.StatusSucceeded, Message: "done", Progress: 100}
	if err := c.SendReport(context.Background(), rep); err != nil {
		t.Fatalf("SendReport: %v", err)
	}
	waitFor(t, "report on server", func() bool { return srv.findReport("cmd-1") != nil })
	got := srv.findReport("cmd-1")
	if got.Status != protocol.StatusSucceeded || got.Message != "done" || got.Progress != 100 {
		t.Fatalf("server observed report %+v, want the sent report", got)
	}
}

func TestHelloAckRejectedRetriesWithBackoff(t *testing.T) {
	srv := &wsTestServer{token: "good-token"}
	ts := newWSTestServer(t, srv)
	logs := &logCapture{}
	c := startClient(t, ts, "wrong-token", slog.New(logs))

	// The rejected handshake fails the attempt; the client backs off (~1s)
	// and retries instead of giving up.
	waitFor(t, "handshake retried", func() bool { return srv.numConns() >= 2 })
	if c.Connected() {
		t.Fatal("Connected = true, want false while the token is rejected")
	}
	if n := logs.count("check SERVER_TOKEN"); n != 1 {
		t.Fatalf("auth failure logged %d times, want once per streak", n)
	}
}

func TestCommandFrameInvokesHandler(t *testing.T) {
	cmd := &protocol.Command{ID: "ws-cmd-1", Nonce: "n1", Verb: protocol.VerbApply}
	srv := &wsTestServer{token: "good-token", commands: []*protocol.Command{cmd}}
	ts := newWSTestServer(t, srv)
	c := startClient(t, ts, "good-token", testLogger())

	got := make(chan *protocol.Command, 1)
	c.SetCommandHandler(func(cmd *protocol.Command) { got <- cmd })

	select {
	case cmd := <-got:
		if cmd.ID != "ws-cmd-1" {
			t.Fatalf("handler got command %q, want ws-cmd-1", cmd.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestServerCloseTriggersReconnect(t *testing.T) {
	srv := &wsTestServer{token: "good-token", closeAfterPush: true}
	ts := newWSTestServer(t, srv)
	c := startClient(t, ts, "good-token", testLogger())

	// The server closes the first connection right after the handshake; the
	// client must redial (backoff ~1s) and stay Connected on the next one.
	waitFor(t, "reconnect after server close", func() bool { return srv.numConns() >= 2 })
	waitFor(t, "connected again", c.Connected)
}

func TestSendReportNotConnected(t *testing.T) {
	cfg := &config.Config{ServerEndpoint: "http://127.0.0.1:1/audit_ready", ServerToken: "t"}
	c := New(cfg, testLogger())
	if err := c.SendReport(context.Background(), &protocol.Report{CommandID: "x"}); err == nil {
		t.Fatal("SendReport on a dead connection returned nil error, want failure")
	}
}
