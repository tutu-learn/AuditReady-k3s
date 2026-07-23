package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"AuditReady-k3s/internal/protocol"
)

const testToken = "test-token"

// newTestServer builds a server with one command loaded from a temp
// command file, mirroring how main wires it up.
func newTestServer(t *testing.T) (*server, *httptest.Server) {
	t.Helper()
	return newTestServerWithLog(t, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// newTestServerWithLog is newTestServer with a caller-provided logger.
func newTestServerWithLog(t *testing.T, log *slog.Logger) (*server, *httptest.Server) {
	t.Helper()

	cmds := []*protocol.Command{{
		Verb: protocol.VerbApply,
		Target: protocol.ResourceRef{
			Group:    "apps",
			Version:  "v1",
			Resource: "deployments",
			Name:     "nginx",
		},
		Payload: []byte("apiVersion: apps/v1"),
	}}
	data, err := json.Marshal(cmds)
	if err != nil {
		t.Fatal(err)
	}
	commandFile := filepath.Join(t.TempDir(), "commands.json")
	if err := os.WriteFile(commandFile, data, 0o600); err != nil {
		t.Fatal(err)
	}

	srv, err := newServer(log, testToken, commandFile)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.handler())
	t.Cleanup(ts.Close)
	return srv, ts
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

func post(t *testing.T, url, token string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestInventory(t *testing.T) {
	_, ts := newTestServer(t)
	url := ts.URL + "/audit_ready/k8s/inventory"

	tests := []struct {
		name       string
		token      string
		body       any
		wantStatus int
		wantOK     bool
		wantSeq    int64
	}{
		{name: "no token", token: "", body: validBatch(42), wantStatus: http.StatusUnauthorized, wantOK: false},
		{name: "wrong token", token: "nope", body: validBatch(42), wantStatus: http.StatusUnauthorized, wantOK: false},
		{name: "valid batch", token: testToken, body: validBatch(42), wantStatus: http.StatusOK, wantOK: true, wantSeq: 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := post(t, url, tt.token, tt.body)
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			ack := decode[protocol.InventoryAck](t, resp)
			if ack.OK != tt.wantOK {
				t.Errorf("ok = %v, want %v", ack.OK, tt.wantOK)
			}
			if ack.LastSeq != tt.wantSeq {
				t.Errorf("lastSeq = %d, want %d", ack.LastSeq, tt.wantSeq)
			}
		})
	}
}

func TestInventoryBadBody(t *testing.T) {
	_, ts := newTestServer(t)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/audit_ready/k8s/inventory", bytes.NewReader([]byte("{not json")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestPollDeliversSignedCommandsOnce(t *testing.T) {
	srv, ts := newTestServer(t)
	url := ts.URL + "/audit_ready/k8s/poll"

	req := &protocol.PollRequest{
		ClusterID: "dev",
		Reports: []*protocol.Report{{
			CommandID: "cmd-1",
			Status:    protocol.StatusSucceeded,
			Message:   "done",
			Timestamp: 1,
		}},
	}

	resp := post(t, url, testToken, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first poll status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	first := decode[protocol.PollResponse](t, resp)
	if !first.OK {
		t.Fatal("first poll ok = false, want true")
	}
	if len(first.Commands) != 1 {
		t.Fatalf("first poll returned %d commands, want 1", len(first.Commands))
	}
	cmd := first.Commands[0]
	if cmd.ID == "" || cmd.Nonce == "" || cmd.Timestamp == 0 {
		t.Errorf("command not filled: id=%q nonce=%q timestamp=%d", cmd.ID, cmd.Nonce, cmd.Timestamp)
	}
	if err := cmd.VerifySignature(srv.pub); err != nil {
		t.Errorf("signature does not verify: %v", err)
	}

	resp = post(t, url, testToken, &protocol.PollRequest{})
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(raw), `{"ok":true,"commands":[]}`+"\n"; got != want {
		t.Fatalf("second poll body = %q, want %q", got, want)
	}
}

func TestPollRequiresAuth(t *testing.T) {
	_, ts := newTestServer(t)
	resp := post(t, ts.URL+"/audit_ready/k8s/poll", "", &protocol.PollRequest{})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
	pr := decode[protocol.PollResponse](t, resp)
	if pr.OK {
		t.Fatal("ok = true, want false")
	}
}

func TestUnknownPath(t *testing.T) {
	_, ts := newTestServer(t)
	resp := post(t, ts.URL+"/nope", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func validBatch(seq int64) *protocol.InventoryBatch {
	return &protocol.InventoryBatch{
		ClusterID: "dev",
		Seq:       seq,
		Full:      true,
		Events: []*protocol.InventoryEvent{{
			Op:        protocol.OpSync,
			Ref:       protocol.ResourceRef{Version: "v1", Resource: "pods", Name: "p1"},
			Timestamp: 1,
		}},
	}
}

// dialWS opens the command channel and sends the first-frame hello.
func dialWS(t *testing.T, ts *httptest.Server, token string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/audit_ready/k8s/ws"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	hello, err := json.Marshal(&protocol.WsHello{Type: protocol.WsTypeAgentHello, Token: token, ClusterID: "dev"})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, hello); err != nil {
		t.Fatal(err)
	}
	return conn
}

// readFrame reads one text frame and unmarshals it into T.
func readFrame[T any](t *testing.T, conn *websocket.Conn) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestWSHelloWrongTokenRejected(t *testing.T) {
	_, ts := newTestServer(t)
	conn := dialWS(t, ts, "wrong-token")

	ack := readFrame[protocol.WsHelloAck](t, conn)
	if ack.Accepted {
		t.Fatal("ack accepted = true, want false for a wrong token")
	}
	if ack.Message != "unauthorized" {
		t.Fatalf("ack message = %q, want %q", ack.Message, "unauthorized")
	}
	// The server closes after the rejection.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("connection still open after a rejected hello")
	}
}

func TestWSPushesSignedCommandsOnce(t *testing.T) {
	srv, ts := newTestServer(t)

	conn := dialWS(t, ts, testToken)
	if ack := readFrame[protocol.WsHelloAck](t, conn); !ack.Accepted {
		t.Fatalf("ack accepted = false (%q), want true", ack.Message)
	}
	msg := readFrame[protocol.WsCommandMessage](t, conn)
	if msg.Type != protocol.WsTypeCommand || msg.Command == nil {
		t.Fatalf("command frame = %+v, want type command with a command", msg)
	}
	cmd := msg.Command
	if cmd.ID == "" || cmd.Nonce == "" || cmd.Timestamp == 0 {
		t.Errorf("command not filled: id=%q nonce=%q timestamp=%d", cmd.ID, cmd.Nonce, cmd.Timestamp)
	}
	if err := cmd.VerifySignature(srv.pub); err != nil {
		t.Errorf("signature does not verify: %v", err)
	}

	// A second connection gets no commands: they are pushed exactly once.
	conn2 := dialWS(t, ts, testToken)
	if ack := readFrame[protocol.WsHelloAck](t, conn2); !ack.Accepted {
		t.Fatalf("second ack accepted = false (%q), want true", ack.Message)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, data, err := conn2.Read(ctx); err == nil {
		t.Fatalf("second connection got an unexpected frame: %s", data)
	}
}

func TestWSReportFramesLogged(t *testing.T) {
	logs := &logCapture{}
	_, ts := newTestServerWithLog(t, slog.New(logs))

	conn := dialWS(t, ts, testToken)
	if ack := readFrame[protocol.WsHelloAck](t, conn); !ack.Accepted {
		t.Fatalf("ack accepted = false (%q), want true", ack.Message)
	}
	frame, err := json.Marshal(&protocol.WsReportMessage{
		Type:   protocol.WsTypeReport,
		Report: &protocol.Report{ClusterID: "dev", CommandID: "cmd-1", Status: protocol.StatusSucceeded, Timestamp: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if logs.count("report") > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("report frame never logged by the server")
}
