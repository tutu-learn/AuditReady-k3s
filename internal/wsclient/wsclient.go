// Package wsclient maintains the WebSocket fast path to the AuditReady
// control plane at <endpoint>/k8s/ws. The agent authenticates with a
// first-frame hello, then receives signed commands in real time and streams
// execution reports back over the same socket. HTTP polling remains as the
// fallback path — the receiver falls back to its report queue whenever this
// channel is down.
package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/metrics"
	"AuditReady-k3s/internal/protocol"
)

const (
	// readLimit bounds a single inbound frame.
	readLimit = 256 << 10
	// handshakeTimeout bounds the hello write + hello_ack wait.
	handshakeTimeout = 10 * time.Second
	// writeTimeout bounds a single outbound frame write.
	writeTimeout = 10 * time.Second
	// pingInterval is the keepalive cadence; the library answers server
	// pings automatically.
	pingInterval = 25 * time.Second
	// agentVersion is reported in WsHello.Version.
	agentVersion = "dev"
)

// Backoff bounds, mirroring the receiver's retry policy.
const (
	backoffMin = time.Second
	backoffMax = 30 * time.Second
)

// commandHandler receives signed commands pushed by the server.
type commandHandler func(*protocol.Command)

// Client is a reconnecting WebSocket control channel. The zero value is not
// usable; construct with New.
type Client struct {
	cfg *config.Config
	log *slog.Logger
	url string

	connected atomic.Bool
	handler   atomic.Value // stores commandHandler

	mu   sync.Mutex // serializes writes and guards conn
	conn *websocket.Conn
}

// New returns a Client dialing <cfg.EndpointBase()>/k8s/ws with the scheme
// swapped to ws/wss.
func New(cfg *config.Config, log *slog.Logger) *Client {
	return &Client{cfg: cfg, log: log, url: wsURL(cfg.EndpointBase())}
}

// Connected reports whether the channel is currently handshaken and live.
func (c *Client) Connected() bool { return c.connected.Load() }

// SetCommandHandler registers the callback for pushed commands. It is
// invoked synchronously from the read loop, so it must be cheap.
func (c *Client) SetCommandHandler(h func(*protocol.Command)) {
	c.handler.Store(commandHandler(h))
}

// Run connects, handshakes and reads until ctx is cancelled. Failures retry
// with exponential backoff; a successful handshake resets the backoff.
func (c *Client) Run(ctx context.Context) {
	bo := new(backoff)
	for {
		err := c.connectOnce(ctx, bo)
		if ctx.Err() != nil {
			return
		}
		// A rejected token needs operator action (rotate SERVER_TOKEN and
		// restart), so shout once per streak instead of warning on every
		// retry; it still backs off like any other connect failure.
		var authErr *authError
		if errors.As(err, &authErr) {
			if !bo.authLogged {
				c.log.Error("control plane rejected the agent token over websocket — check SERVER_TOKEN / token revoked",
					"error", authErr)
				bo.authLogged = true
			}
		} else {
			c.log.Warn("websocket connection failed", "error", err)
		}
		if !sleep(ctx, bo.next()) {
			return
		}
	}
}

// SendReport writes one report frame. It returns an error when the channel
// is down so the caller can fall back to the poll queue.
func (c *Client) SendReport(ctx context.Context, r *protocol.Report) error {
	if !c.Connected() {
		return errors.New("websocket not connected")
	}
	raw, err := json.Marshal(&protocol.WsReportMessage{Type: protocol.WsTypeReport, Report: r})
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("websocket not connected")
	}
	if err := c.conn.Write(wctx, websocket.MessageText, raw); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

// connectOnce dials, handshakes and runs the read loop until the connection
// drops. A successful handshake resets the backoff.
func (c *Client) connectOnce(ctx context.Context, bo *backoff) error {
	conn, _, err := websocket.Dial(ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.url, err)
	}
	conn.SetReadLimit(readLimit)
	if err := c.handshake(ctx, conn); err != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "handshake failed")
		return err
	}
	bo.reset()

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	c.connected.Store(true)
	metrics.SetTunnelConnected(true)
	c.log.Info("websocket connected", "url", c.url)
	defer func() {
		c.connected.Store(false)
		metrics.SetTunnelConnected(false)
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "closing")
	}()

	return c.readLoop(ctx, conn)
}

// handshake sends the first-frame hello and waits for the server's ack.
func (c *Client) handshake(ctx context.Context, conn *websocket.Conn) error {
	hctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	hello := &protocol.WsHello{
		Type:      protocol.WsTypeAgentHello,
		Token:     c.cfg.ServerToken,
		ClusterID: c.cfg.ClusterID,
		Version:   agentVersion,
	}
	raw, err := json.Marshal(hello)
	if err != nil {
		return fmt.Errorf("marshal hello: %w", err)
	}
	if err := conn.Write(hctx, websocket.MessageText, raw); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}

	_, data, err := conn.Read(hctx)
	if err != nil {
		return fmt.Errorf("read hello ack: %w", err)
	}
	// WsHelloAck carries no type tag on the wire, so the ack is any frame
	// without one (or one explicitly tagged hello_ack).
	var env protocol.WsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("decode hello ack: %w", err)
	}
	if env.Type != "" && env.Type != protocol.WsTypeHelloAck {
		return fmt.Errorf("expected %s frame, got %q", protocol.WsTypeHelloAck, env.Type)
	}
	var ack protocol.WsHelloAck
	if err := json.Unmarshal(data, &ack); err != nil {
		return fmt.Errorf("decode hello ack: %w", err)
	}
	if !ack.Accepted {
		msg := fmt.Sprintf("hello rejected: %s", ack.Message)
		if smellsAuth(ack.Message) {
			return &authError{msg: msg}
		}
		return errors.New(msg)
	}
	return nil
}

// readLoop dispatches inbound frames until the connection drops or ctx is
// cancelled. The command handler is invoked synchronously per frame.
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
	kctx, stopKeepalive := context.WithCancel(ctx)
	defer stopKeepalive()
	go keepalive(kctx, conn)

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if typ != websocket.MessageText {
			c.log.Debug("dropping non-text frame", "type", typ)
			continue
		}
		var env protocol.WsEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			c.log.Debug("dropping undecodable frame", "error", err)
			continue
		}
		switch env.Type {
		case protocol.WsTypeCommand:
			var msg protocol.WsCommandMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				c.log.Debug("dropping malformed command frame", "error", err)
				continue
			}
			h, _ := c.handler.Load().(commandHandler)
			if h == nil {
				c.log.Debug("dropping command, no handler registered", "id", msg.Command.ID)
				continue
			}
			h(msg.Command)
		case protocol.WsTypeError:
			var msg protocol.WsErrorMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				c.log.Debug("dropping malformed error frame", "error", err)
				continue
			}
			c.log.Warn("server sent an error frame", "message", msg.Message)
		default:
			c.log.Debug("dropping unknown frame type", "type", env.Type)
		}
	}
}

// keepalive sends a protocol-level ping every pingInterval. A failed ping
// closes the connection to unblock the reader.
func keepalive(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.Ping(ctx); err != nil {
				_ = conn.Close(websocket.StatusInternalError, "ping failed")
				return
			}
		}
	}
}

// wsURL converts an http(s) endpoint base to its ws(s) command-channel URL.
func wsURL(base string) string {
	u := strings.TrimRight(base, "/")
	scheme := "ws://"
	if strings.HasPrefix(u, "https://") {
		scheme = "wss://"
	}
	u = strings.TrimPrefix(u, "http://")
	u = strings.TrimPrefix(u, "https://")
	return scheme + u + "/k8s/ws"
}

// authError marks a handshake rejection that smells like a bad token, so a
// reconnect streak is logged once at error level instead of warn per retry.
type authError struct{ msg string }

func (e *authError) Error() string { return e.msg }

// smellsAuth reports whether a hello rejection looks like an auth failure.
func smellsAuth(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "unauthorized") || strings.Contains(m, "token") || strings.Contains(m, "401")
}

// sleep waits d or reports ctx cancellation.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// backoff is an exponential backoff with ±20% jitter, 1s up to 30s.
// authLogged suppresses repeated auth failure logs within one streak.
type backoff struct {
	d          time.Duration
	authLogged bool
}

func (b *backoff) next() time.Duration {
	if b.d < backoffMin {
		b.d = backoffMin
	}
	d := b.d
	b.d *= 2
	if b.d > backoffMax {
		b.d = backoffMax
	}
	return d + time.Duration((rand.Float64()*0.4-0.2)*float64(d))
}

func (b *backoff) reset() { b.d, b.authLogged = 0, false }
