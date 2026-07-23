// Command mockserver is a plain-HTTP mock of the AuditReady control plane
// for local development. It requires a bearer token on every request, acks
// inventory batches, and hands out the signed commands from -command-file
// on the first poll. It also serves the WebSocket fast path at
// /audit_ready/k8s/ws: first-frame token auth, then the commands are pushed
// once and reports are read until close. On startup it prints the
// SERVER_TOKEN and SERVER_PUBLIC_KEY values the agent should be configured
// with.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"AuditReady-k3s/internal/protocol"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8443", "listen address")
	token := flag.String("token", "dev-token", "bearer token the mock requires on every request")
	commandFile := flag.String("command-file", "", "JSON array of commands to fill, sign, and hand out on the first poll")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	srv, err := newServer(log, *token, *commandFile)
	if err != nil {
		log.Error("init", "error", err)
		os.Exit(1)
	}

	fmt.Printf("SERVER_TOKEN=%s\n", *token)
	fmt.Printf("SERVER_PUBLIC_KEY=%s\n", base64.StdEncoding.EncodeToString(srv.pub))

	log.Info("mock control plane listening", "addr", *addr, "commands", len(srv.commands))
	if err := http.ListenAndServe(*addr, srv.handler()); err != nil {
		log.Error("serve", "error", err)
		os.Exit(1)
	}
}

// server is a mock control plane: it authenticates requests by bearer token,
// acks inventory batches, and delivers its commands exactly once per channel
// (first poll, first WebSocket connect).
type server struct {
	log      *slog.Logger
	token    string
	priv     ed25519.PrivateKey
	pub      ed25519.PublicKey
	commands []*protocol.Command

	delivered   atomic.Bool // commands already returned on a poll
	wsDelivered atomic.Bool // commands already pushed over the WebSocket
}

// newServer generates the signing keypair, loads the command file (if any),
// and fills and signs each command so polls can serve them as-is.
func newServer(log *slog.Logger, token, commandFile string) (*server, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate signing key: %w", err)
	}

	var commands []*protocol.Command
	if commandFile != "" {
		data, err := os.ReadFile(commandFile)
		if err != nil {
			return nil, fmt.Errorf("read command file: %w", err)
		}
		if err := json.Unmarshal(data, &commands); err != nil {
			return nil, fmt.Errorf("parse command file: %w", err)
		}
	}
	for _, cmd := range commands {
		if cmd.ID == "" {
			cmd.ID = randomID()
		}
		if cmd.Nonce == "" {
			cmd.Nonce = randomID()
		}
		if cmd.Timestamp == 0 {
			cmd.Timestamp = time.Now().Unix()
		}
		cmd.Sign(priv)
	}

	return &server{log: log, token: token, priv: priv, pub: pub, commands: commands}, nil
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /audit_ready/k8s/inventory", s.inventory)
	mux.HandleFunc("POST /audit_ready/k8s/poll", s.poll)
	mux.HandleFunc("GET /audit_ready/k8s/ws", s.ws)
	return mux
}

// authorized reports whether the request carries the configured bearer token.
func (s *server) authorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer "+s.token
}

// inventory logs each batch and acks its seq.
func (s *server) inventory(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, protocol.InventoryAck{OK: false})
		return
	}
	var batch protocol.InventoryBatch
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		writeJSON(w, http.StatusBadRequest, protocol.InventoryAck{OK: false, Error: err.Error()})
		return
	}
	s.log.Info("inventory batch", "clusterId", batch.ClusterID, "seq", batch.Seq, "full", batch.Full, "events", len(batch.Events))
	writeJSON(w, http.StatusOK, protocol.InventoryAck{OK: true, LastSeq: batch.Seq})
}

// poll logs every piggybacked report, returns the signed commands on the
// first poll, and an empty list afterwards.
func (s *server) poll(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, protocol.PollResponse{OK: false})
		return
	}
	var req protocol.PollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, protocol.PollResponse{OK: false})
		return
	}
	for _, rep := range req.Reports {
		s.log.Info("report", "clusterId", rep.ClusterID, "commandId", rep.CommandID, "status", rep.Status, "message", rep.Message, "progress", rep.Progress)
	}
	// pollResponse mirrors protocol.PollResponse but without omitempty on
	// Commands, so empty polls serialize as {"ok":true,"commands":[]} per
	// the spec.
	type pollResponse struct {
		OK       bool                `json:"ok"`
		Commands []*protocol.Command `json:"commands"`
	}
	resp := pollResponse{OK: true, Commands: []*protocol.Command{}}
	if s.delivered.CompareAndSwap(false, true) {
		resp.Commands = append(resp.Commands, s.commands...)
		for _, cmd := range s.commands {
			s.log.Info("command sent", "id", cmd.ID, "verb", cmd.Verb)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// ws is the real-time command channel: the first frame authenticates with a
// WsHello token, then the signed commands are pushed once and reports are
// read until the agent disconnects.
func (s *server) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.log.Warn("websocket accept failed", "error", err)
		return
	}
	defer conn.CloseNow()
	ctx := r.Context()

	_, data, err := conn.Read(ctx)
	if err != nil {
		s.log.Warn("websocket hello read failed", "error", err)
		return
	}
	var hello protocol.WsHello
	if err := json.Unmarshal(data, &hello); err != nil || hello.Type != protocol.WsTypeAgentHello {
		s.log.Warn("websocket first frame was not a hello")
		writeFrame(ctx, conn, &protocol.WsHelloAck{Type: protocol.WsTypeHelloAck, Accepted: false, Message: "expected agent_hello"})
		return
	}
	if hello.Token != s.token {
		s.log.Warn("websocket hello rejected, wrong token", "clusterId", hello.ClusterID)
		writeFrame(ctx, conn, &protocol.WsHelloAck{Type: protocol.WsTypeHelloAck, Accepted: false, Message: "unauthorized"})
		_ = conn.Close(websocket.StatusPolicyViolation, "unauthorized")
		return
	}
	s.log.Info("websocket connected", "clusterId", hello.ClusterID, "version", hello.Version)
	if err := writeFrame(ctx, conn, &protocol.WsHelloAck{Type: protocol.WsTypeHelloAck, Accepted: true}); err != nil {
		return
	}

	// Push the command file once, on the first WebSocket connection.
	if s.wsDelivered.CompareAndSwap(false, true) {
		for _, cmd := range s.commands {
			if err := writeFrame(ctx, conn, &protocol.WsCommandMessage{Type: protocol.WsTypeCommand, Command: cmd}); err != nil {
				return
			}
			s.log.Info("command sent", "id", cmd.ID, "verb", cmd.Verb, "channel", "ws")
		}
	}

	// Read report frames until close.
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			s.log.Info("websocket closed", "error", err)
			return
		}
		var env protocol.WsEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			s.log.Debug("dropping undecodable frame", "error", err)
			continue
		}
		if env.Type != protocol.WsTypeReport {
			s.log.Debug("dropping unexpected frame type", "type", env.Type)
			continue
		}
		var msg protocol.WsReportMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			s.log.Debug("dropping malformed report frame", "error", err)
			continue
		}
		rep := msg.Report
		s.log.Info("report", "clusterId", rep.ClusterID, "commandId", rep.CommandID, "status", rep.Status, "message", rep.Message, "progress", rep.Progress, "channel", "ws")
	}
}

// writeFrame marshals v as one JSON text frame.
func writeFrame(ctx context.Context, conn *websocket.Conn, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, raw)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func randomID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
