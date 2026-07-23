// Package receiver polls the control plane for commands, validates them
// (writer enabled, signature, timestamp skew, nonce replay) and dispatches
// them to the writer's Handler. Commands may also arrive over the WebSocket
// fast path (see internal/wsclient); both paths share the same validation
// pipeline and are deduplicated by command ID. Execution reports go over the
// WebSocket when it is connected, and are otherwise queued locally and
// piggybacked on the next successful poll.
package receiver

import (
	"container/list"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/metrics"
	"AuditReady-k3s/internal/protocol"
)

const (
	// maxNonces bounds the replay cache; oldest nonces are evicted FIFO.
	maxNonces = 10_000
	// maxIDs bounds the seen-command-ID dedupe cache; oldest IDs are
	// evicted FIFO.
	maxIDs = 10_000
	// maxClockSkew is the accepted distance between command and local time.
	maxClockSkew = 5 * time.Minute
	// maxQueuedReports bounds the report queue; on overflow the oldest
	// report is dropped so a long outage cannot grow memory without bound.
	maxQueuedReports = 1_000
	// agentVersion is reported in PollRequest.Version.
	agentVersion = "dev"
)

// Backoff bounds, shared with the uploader's retry policy.
const (
	backoffMin = time.Second
	backoffMax = 30 * time.Second
)

// Handler executes a validated command and reports progress via report.
// Reports are streamed over the WebSocket fast path when it is connected and
// queued for the next successful poll otherwise.
type Handler interface {
	HandleCommand(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report))
}

// PushChannel is the optional WebSocket fast path for command delivery and
// reports (implemented by internal/wsclient.Client). A nil PushChannel
// disables the fast path; polling alone then carries commands and reports.
type PushChannel interface {
	Connected() bool
	SendReport(ctx context.Context, r *protocol.Report) error
	SetCommandHandler(func(*protocol.Command))
}

// Receiver runs the command polling loop.
type Receiver struct {
	cfg  *config.Config
	poll func(ctx context.Context, req *protocol.PollRequest) (*protocol.PollResponse, error)
	h    Handler
	log  *slog.Logger
	push PushChannel

	pub    ed25519.PublicKey
	pubErr error
	nonces *nonceCache
	ids    *nonceCache // FIFO seen-command-ID dedupe cache
	queue  *reportQueue

	runCtx atomic.Value // stores context.Context from Run for WS-pushed commands
}

// New returns a Receiver. The server public key is decoded once from cfg.
// When push is non-nil, pushed commands are wired into the same validation
// pipeline the poll path uses and reports are sent over the fast path first.
func New(cfg *config.Config, poll func(ctx context.Context, req *protocol.PollRequest) (*protocol.PollResponse, error), h Handler, log *slog.Logger, push PushChannel) *Receiver {
	pub, err := cfg.PublicKey()
	r := &Receiver{
		cfg:    cfg,
		poll:   poll,
		h:      h,
		log:    log,
		push:   push,
		pub:    pub,
		pubErr: err,
		nonces: newNonceCache(maxNonces),
		ids:    newNonceCache(maxIDs),
		queue:  newReportQueue(maxQueuedReports, log),
	}
	if push != nil {
		push.SetCommandHandler(r.process)
	}
	return r
}

// Run polls for commands until ctx is cancelled. Poll errors retry with
// exponential backoff; successful polls wait cfg.PollInterval. The loop runs
// unchanged as the fallback when the WebSocket fast path is down and keeps
// flushing queued reports.
func (r *Receiver) Run(ctx context.Context) {
	r.runCtx.Store(ctx)
	bo := new(backoff)
	for {
		req := &protocol.PollRequest{
			ClusterID: r.cfg.ClusterID,
			Version:   agentVersion,
			Reports:   r.queue.snapshot(),
		}
		resp, err := r.poll(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// A rejected token needs operator action (rotate SERVER_TOKEN
			// and restart), so shout once per streak instead of warning on
			// every retry; it still backs off like any other poll error.
			var authErr *protocol.AuthError
			if errors.As(err, &authErr) {
				if !bo.authLogged {
					r.log.Error("control plane rejected the agent token — check SERVER_TOKEN / token revoked",
						"error", authErr, "queuedReports", len(req.Reports))
					bo.authLogged = true
				}
			} else {
				r.log.Warn("command poll failed", "error", err, "queuedReports", len(req.Reports))
			}
			if !sleep(ctx, bo.next()) {
				return
			}
			continue
		}
		bo.reset()
		r.queue.clearSent(req.Reports)
		for _, cmd := range resp.Commands {
			r.process(cmd)
		}
		if !sleep(ctx, r.cfg.PollInterval) {
			return
		}
	}
}

// process runs one command — from a poll response or the WebSocket fast
// path — through dedupe and validation, then dispatches it to the Handler.
func (r *Receiver) process(cmd *protocol.Command) {
	// The server may deliver the same command over the WebSocket and a
	// later poll during failover races; drop repeats silently before the
	// validation pipeline (no refused report, no metric).
	if r.ids.seen(cmd.ID) {
		r.log.Debug("duplicate command delivery dropped", "id", cmd.ID, "verb", cmd.Verb)
		return
	}
	if reason := r.validate(cmd); reason != "" {
		r.log.Warn("command refused", "id", cmd.ID, "verb", cmd.Verb, "reason", reason)
		metrics.IncCommand(cmd.Verb, "refused")
		r.deliverReport(r.newReport(cmd, protocol.StatusRefused, reason))
		return
	}
	r.log.Info("command received", "id", cmd.ID, "verb", cmd.Verb)
	r.deliverReport(r.newReport(cmd, protocol.StatusReceived, ""))
	report := func(rep *protocol.Report) {
		rep.ClusterID = r.cfg.ClusterID
		rep.CommandID = cmd.ID
		rep.Timestamp = time.Now().Unix()
		r.deliverReport(rep)
	}
	go r.h.HandleCommand(r.commandCtx(), cmd, report)
}

// deliverReport sends rep over the WebSocket fast path when it is connected,
// falling back to the poll queue when the send fails or the path is down.
func (r *Receiver) deliverReport(rep *protocol.Report) {
	if r.push != nil && r.push.Connected() {
		if err := r.push.SendReport(context.Background(), rep); err == nil {
			return
		} else {
			r.log.Debug("websocket report fast path failed, queuing for poll",
				"commandId", rep.CommandID, "status", rep.Status, "error", err)
		}
	}
	r.queue.enqueue(rep)
}

// commandCtx returns the Run context for WS-pushed commands, or a background
// context when Run has not started yet.
func (r *Receiver) commandCtx() context.Context {
	if ctx, ok := r.runCtx.Load().(context.Context); ok {
		return ctx
	}
	return context.Background()
}

// validate applies the refusal checks in order and returns the refusal
// reason, or "" when the command is accepted.
func (r *Receiver) validate(cmd *protocol.Command) string {
	if !r.cfg.WriterEnabled {
		return "writer disabled (read-only mode)"
	}
	if r.pubErr != nil {
		return fmt.Sprintf("server public key misconfigured: %v", r.pubErr)
	}
	if err := cmd.VerifySignature(r.pub); err != nil {
		return err.Error()
	}
	if skew := time.Since(time.Unix(cmd.Timestamp, 0)); skew > maxClockSkew || skew < -maxClockSkew {
		return fmt.Sprintf("timestamp skew %s exceeds %s", skew.Round(time.Second), maxClockSkew)
	}
	if r.nonces.seen(cmd.Nonce) {
		return "replayed nonce"
	}
	return ""
}

func (r *Receiver) newReport(cmd *protocol.Command, status, msg string) *protocol.Report {
	return &protocol.Report{
		ClusterID: r.cfg.ClusterID,
		CommandID: cmd.ID,
		Status:    status,
		Message:   msg,
		Timestamp: time.Now().Unix(),
	}
}

// reportQueue is a bounded FIFO of reports awaiting delivery. The zero
// value is not usable; construct with newReportQueue.
type reportQueue struct {
	mu  sync.Mutex
	cap int
	log *slog.Logger
	ll  *list.List
}

func newReportQueue(cap int, log *slog.Logger) *reportQueue {
	return &reportQueue{cap: cap, log: log, ll: list.New()}
}

// enqueue appends rep, dropping the oldest report when full.
func (q *reportQueue) enqueue(rep *protocol.Report) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ll.Len() >= q.cap {
		front := q.ll.Remove(q.ll.Front()).(*protocol.Report)
		q.log.Warn("report queue full, dropping oldest report", "commandId", front.CommandID, "status", front.Status)
	}
	q.ll.PushBack(rep)
}

// snapshot returns the queued reports without clearing them.
func (q *reportQueue) snapshot() []*protocol.Report {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*protocol.Report, 0, q.ll.Len())
	for e := q.ll.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*protocol.Report))
	}
	return out
}

// clearSent removes the reports sent in a successful poll. Reports enqueued
// after the snapshot was taken (including ones that pushed sent reports out
// of a full queue) are kept.
func (q *reportQueue) clearSent(sent []*protocol.Report) {
	set := make(map[*protocol.Report]struct{}, len(sent))
	for _, rep := range sent {
		set[rep] = struct{}{}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for e := q.ll.Front(); e != nil; e = q.ll.Front() {
		if _, ok := set[e.Value.(*protocol.Report)]; !ok {
			return
		}
		q.ll.Remove(e)
	}
}

func (q *reportQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ll.Len()
}

// nonceCache remembers recently seen nonces with FIFO eviction.
type nonceCache struct {
	cap int
	ll  *list.List
	m   map[string]*list.Element
}

func newNonceCache(cap int) *nonceCache {
	return &nonceCache{cap: cap, ll: list.New(), m: make(map[string]*list.Element)}
}

// seen records nonce and reports whether it was already present.
func (c *nonceCache) seen(nonce string) bool {
	if _, ok := c.m[nonce]; ok {
		return true
	}
	c.m[nonce] = c.ll.PushBack(nonce)
	if c.ll.Len() > c.cap {
		front := c.ll.Front()
		delete(c.m, front.Value.(string))
		c.ll.Remove(front)
	}
	return false
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
// authLogged suppresses repeated AuthError logs within one failure streak.
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
