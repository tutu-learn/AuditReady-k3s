package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/protocol"
	"AuditReady-k3s/internal/wsclient"
)

// testAgentServer is a minimal in-process control plane: it answers the
// first poll with its commands and records every report it receives.
type testAgentServer struct {
	token    string
	commands []*protocol.Command

	mu      sync.Mutex
	polls   int
	reports []*protocol.Report
}

func (s *testAgentServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/audit_ready/k8s/poll" || r.Method != http.MethodPost {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+s.token {
		http.Error(w, "bad token", http.StatusUnauthorized)
		return
	}
	var req protocol.PollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.polls++
	s.reports = append(s.reports, req.Reports...)
	resp := &protocol.PollResponse{OK: true}
	if s.polls == 1 {
		resp.Commands = s.commands
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *testAgentServer) findReport(commandID, status string) *protocol.Report {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rep := range s.reports {
		if rep.CommandID == commandID && rep.Status == status {
			return rep
		}
	}
	return nil
}

// TestIntegration runs a real polling Receiver against a real HTTP control
// plane (httptest.Server + protocol.Client) and checks both directions.
func TestIntegration(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cmd := &protocol.Command{
		ID:        "int-1",
		Nonce:     "int-nonce-1",
		Timestamp: time.Now().Unix(),
		Verb:      protocol.VerbApply,
		Target:    protocol.ResourceRef{Version: "v1", Resource: "configmaps", Namespace: "default", Name: "cm"},
	}
	cmd.Sign(priv)

	srv := &testAgentServer{token: "it-token", commands: []*protocol.Command{cmd}}
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)

	client := protocol.NewClient(hs.URL+"/audit_ready", srv.token, testLogger())
	cfg := &config.Config{
		ServerPublicKey: base64.StdEncoding.EncodeToString(pub),
		ClusterID:       "it-cluster",
		WriterEnabled:   true,
		PollInterval:    10 * time.Millisecond,
	}
	h := &recordHandler{calls: make(chan *protocol.Command, 16)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go New(cfg, client.Poll, h, testLogger(), nil).Run(ctx)

	select {
	case got := <-h.calls:
		if got.ID != "int-1" {
			t.Fatalf("handler got command %q, want int-1", got.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// The received report reaches the server on a subsequent poll.
	waitFor(t, "received report on server", func() bool { return srv.findReport("int-1", protocol.StatusReceived) != nil })
	rep := srv.findReport("int-1", protocol.StatusReceived)
	if rep.ClusterID != "it-cluster" {
		t.Fatalf("server observed report %+v, want cluster it-cluster", rep)
	}
}

// dualServer is an in-process control plane serving both channels: the WS
// fast path pushes wsCmd once and collects reports, the HTTP poll serves
// the armed pollCmd once and collects piggybacked reports.
type dualServer struct {
	token string
	wsCmd *protocol.Command

	wsUp    atomic.Bool
	pollCmd atomic.Pointer[protocol.Command]

	mu          sync.Mutex
	wsConn      *websocket.Conn
	wsPushed    bool
	pollServed  bool
	wsReports   []*protocol.Report
	pollReports []*protocol.Report
}

func (s *dualServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/audit_ready/k8s/poll" && r.Method == http.MethodPost:
		s.servePoll(w, r)
	case r.URL.Path == "/audit_ready/k8s/ws" && r.Method == http.MethodGet:
		s.serveWS(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (s *dualServer) servePoll(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+s.token {
		http.Error(w, "bad token", http.StatusUnauthorized)
		return
	}
	var req protocol.PollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.pollReports = append(s.pollReports, req.Reports...)
	resp := &protocol.PollResponse{OK: true}
	if cmd := s.pollCmd.Load(); cmd != nil && !s.pollServed {
		s.pollServed = true
		resp.Commands = []*protocol.Command{cmd}
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *dualServer) serveWS(w http.ResponseWriter, r *http.Request) {
	if !s.wsUp.Load() {
		http.Error(w, "ws down", http.StatusServiceUnavailable)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := r.Context()

	_, data, err := conn.Read(ctx)
	if err != nil {
		return
	}
	var hello protocol.WsHello
	if err := json.Unmarshal(data, &hello); err != nil || hello.Token != s.token {
		return
	}
	ack, _ := json.Marshal(&protocol.WsHelloAck{Accepted: true})
	if err := conn.Write(ctx, websocket.MessageText, ack); err != nil {
		return
	}

	s.mu.Lock()
	s.wsConn = conn
	push := !s.wsPushed && s.wsCmd != nil
	s.wsPushed = true
	s.mu.Unlock()
	if push {
		frame, _ := json.Marshal(&protocol.WsCommandMessage{Type: protocol.WsTypeCommand, Command: s.wsCmd})
		if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
			return
		}
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
		s.wsReports = append(s.wsReports, msg.Report)
		s.mu.Unlock()
	}
}

// killWS takes the fast path down and arms the poll fallback command.
func (s *dualServer) killWS(cmd *protocol.Command) {
	s.pollCmd.Store(cmd)
	s.wsUp.Store(false)
	s.mu.Lock()
	conn := s.wsConn
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close(websocket.StatusInternalError, "server restart")
	}
}

func (s *dualServer) findWSReport(commandID, status string) *protocol.Report {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rep := range s.wsReports {
		if rep.CommandID == commandID && rep.Status == status {
			return rep
		}
	}
	return nil
}

func (s *dualServer) findPollReport(commandID, status string) *protocol.Report {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rep := range s.pollReports {
		if rep.CommandID == commandID && rep.Status == status {
			return rep
		}
	}
	return nil
}

// TestIntegrationWSFastPathAndPollFallback runs a real wsclient + Receiver
// against a control plane serving both channels: a command arrives over the
// WS and its report goes back over the WS; after the WS dies, a second
// command arrives via the poll fallback and its report piggybacks on a poll.
func TestIntegrationWSFastPathAndPollFallback(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(id, nonce string) *protocol.Command {
		cmd := &protocol.Command{
			ID:        id,
			Nonce:     nonce,
			Timestamp: time.Now().Unix(),
			Verb:      protocol.VerbApply,
			Target:    protocol.ResourceRef{Version: "v1", Resource: "configmaps", Namespace: "default", Name: "cm"},
		}
		cmd.Sign(priv)
		return cmd
	}

	srv := &dualServer{token: "it-token", wsCmd: sign("int-ws-1", "int-nonce-ws")}
	srv.wsUp.Store(true)
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)

	cfg := &config.Config{
		ServerEndpoint:  hs.URL + "/audit_ready",
		ServerToken:     srv.token,
		ServerPublicKey: base64.StdEncoding.EncodeToString(pub),
		ClusterID:       "it-cluster",
		WriterEnabled:   true,
		PollInterval:    10 * time.Millisecond,
	}
	client := protocol.NewClient(cfg.EndpointBase(), cfg.ServerToken, testLogger())
	ws := wsclient.New(cfg, testLogger())
	h := &recordHandler{calls: make(chan *protocol.Command, 16)}
	rcv := New(cfg, client.Poll, h, testLogger(), ws)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go ws.Run(ctx)
	go rcv.Run(ctx)

	// Phase 1: the command is pushed over the WS and the report returns over it.
	select {
	case got := <-h.calls:
		if got.ID != "int-ws-1" {
			t.Fatalf("handler got command %q, want int-ws-1", got.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}
	waitFor(t, "received report over ws", func() bool { return srv.findWSReport("int-ws-1", protocol.StatusReceived) != nil })
	if rep := srv.findPollReport("int-ws-1", ""); rep != nil {
		t.Fatalf("report %+v took the poll path while the ws was up", rep)
	}

	// Phase 2: the WS dies; the poll fallback delivers the next command and
	// carries its report.
	srv.killWS(sign("int-poll-1", "int-nonce-poll"))
	select {
	case got := <-h.calls:
		if got.ID != "int-poll-1" {
			t.Fatalf("handler got command %q, want int-poll-1", got.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked after ws died")
	}
	waitFor(t, "received report piggybacked on poll", func() bool {
		return srv.findPollReport("int-poll-1", protocol.StatusReceived) != nil
	})
}
