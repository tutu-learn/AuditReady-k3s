// Package uploader POSTs inventory event batches to the control plane over
// plain HTTP. Events are buffered in memory, spilled to disk when the buffer
// is full, and resent with the same sequence number until acknowledged — an
// event is dropped only after a successful ack (or if the spill file itself
// fails, which is logged).
package uploader

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/metrics"
	"AuditReady-k3s/internal/protocol"
)

const (
	defaultCapacity  = 10000
	batchSize        = 200
	flushInterval    = time.Second
	initialBackoff   = time.Second
	maxBackoff       = 30 * time.Second
	spillFileName    = "spill.jsonl"
	seqFileName      = "seq"
	maxSpillLineSize = 10 << 20 // 10 MiB scanner buffer
)

// BatchSender POSTs one inventory batch to the control plane and returns
// the server's ack. See protocol.Client.Inventory for the production
// implementation.
type BatchSender func(ctx context.Context, batch *protocol.InventoryBatch) (*protocol.InventoryAck, error)

// Snapshotter provides the full-state resync events sent on (re)connect.
type Snapshotter interface {
	Snapshot() []*protocol.InventoryEvent
}

// Uploader buffers inventory events and uploads them to the control plane,
// retrying with backoff on any send failure. The batch sequence number is
// persisted to disk so it survives restarts, and a failed batch is resent
// with the same seq — the server is idempotent per batch.
type Uploader struct {
	cfg  *config.Config
	send BatchSender
	snap Snapshotter
	log  *slog.Logger

	capacity int

	mu        sync.Mutex
	buf       []*protocol.InventoryEvent
	spill     *os.File
	spillPath string

	notify chan struct{} // buffered(1), signalled on enqueue

	// Only touched by the Run goroutine.
	seqPath      string
	seq          int64 // last acknowledged batch seq
	needSnapshot bool  // send the full snapshot before the next deltas
}

// New returns an Uploader with the default in-memory buffer capacity.
func New(cfg *config.Config, send BatchSender, snap Snapshotter, log *slog.Logger) *Uploader {
	return NewWithCapacity(cfg, send, snap, log, defaultCapacity)
}

// NewWithCapacity is like New but overrides the in-memory buffer capacity;
// intended for tests.
func NewWithCapacity(cfg *config.Config, send BatchSender, snap Snapshotter, log *slog.Logger, capacity int) *Uploader {
	seqPath := filepath.Join(cfg.SpillDir, seqFileName)
	return &Uploader{
		cfg:          cfg,
		send:         send,
		snap:         snap,
		log:          log,
		capacity:     capacity,
		spillPath:    filepath.Join(cfg.SpillDir, spillFileName),
		notify:       make(chan struct{}, 1),
		seqPath:      seqPath,
		seq:          readSeq(seqPath, log),
		needSnapshot: true,
	}
}

// Enqueue buffers an event for upload, spilling to disk when the buffer is
// full. Safe for concurrent use.
func (u *Uploader) Enqueue(ev *protocol.InventoryEvent) {
	u.mu.Lock()
	if len(u.buf) < u.capacity {
		u.buf = append(u.buf, ev)
		metrics.SetQueueDepth(len(u.buf))
	} else {
		u.spillLocked(ev)
	}
	u.mu.Unlock()
	u.signal()
}

// Run uploads events until ctx is done, retrying failed batches with
// backoff. After any failure the full snapshot is resent before deltas
// resume.
func (u *Uploader) Run(ctx context.Context) {
	backoff := initialBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		if u.needSnapshot {
			if err := u.sendSnapshot(ctx); err != nil {
				u.log.Warn("inventory snapshot send failed, retrying", "error", err, "backoff", backoff)
				if !u.sleep(ctx, backoff) {
					return
				}
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			u.needSnapshot = false
			backoff = initialBackoff
		}
		batch := u.take(batchSize)
		if len(batch) == 0 {
			timer := time.NewTimer(flushInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-u.notify:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		if err := u.sendBatch(ctx, batch, false); err != nil {
			u.requeueFront(batch)
			u.needSnapshot = true
			var authErr *protocol.AuthError
			if errors.As(err, &authErr) {
				u.log.Error("control plane rejected our credentials — check SERVER_TOKEN; backing off",
					"error", err, "backoff", backoff)
			} else {
				u.log.Warn("inventory send failed, retrying", "error", err, "backoff", backoff)
			}
			if !u.sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = initialBackoff
	}
}

// sendSnapshot sends the full snapshot in batches with Full set. An empty
// snapshot counts as sent: there is simply nothing to send.
func (u *Uploader) sendSnapshot(ctx context.Context) error {
	if u.snap == nil {
		return nil
	}
	events := u.snap.Snapshot()
	for start := 0; start < len(events); start += batchSize {
		end := min(start+batchSize, len(events))
		if err := u.sendBatch(ctx, events[start:end], true); err != nil {
			return err
		}
	}
	return nil
}

// sendBatch transmits one batch with the next seq. The seq is only advanced
// and persisted after a successful ack, so a failed batch is resent with the
// same seq — the server dedupes per batch. An ack.Error on an HTTP-200
// response is logged but still counts as success.
func (u *Uploader) sendBatch(ctx context.Context, events []*protocol.InventoryEvent, full bool) error {
	b := &protocol.InventoryBatch{
		ClusterID: u.cfg.ClusterID,
		Seq:       u.seq + 1,
		Full:      full,
		Events:    events,
	}
	ack, err := u.send(ctx, b)
	if err != nil {
		return err
	}
	if ack != nil && ack.Error != "" {
		u.log.Error("inventory ack error", "error", ack.Error, "lastSeq", ack.LastSeq)
	}
	u.seq = b.Seq
	u.persistSeq()
	metrics.IncInventoryEvents(len(events))
	return nil
}

// take pops up to n events from the front of the buffer, replaying spilled
// events first when the buffer has drained.
func (u *Uploader) take(n int) []*protocol.InventoryEvent {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.buf) == 0 {
		u.replaySpillLocked()
	}
	if len(u.buf) == 0 {
		return nil
	}
	n = min(n, len(u.buf))
	out := make([]*protocol.InventoryEvent, n)
	copy(out, u.buf[:n])
	u.buf = u.buf[n:]
	if len(u.buf) == 0 {
		u.buf = nil
	}
	metrics.SetQueueDepth(len(u.buf))
	return out
}

// requeueFront puts a failed batch back at the front of the buffer.
func (u *Uploader) requeueFront(events []*protocol.InventoryEvent) {
	u.mu.Lock()
	u.buf = append(events, u.buf...)
	metrics.SetQueueDepth(len(u.buf))
	u.mu.Unlock()
	u.signal()
}

// persistSeq writes the last acknowledged seq to disk; read at startup so
// seqs keep increasing across restarts.
func (u *Uploader) persistSeq() {
	if err := os.MkdirAll(u.cfg.SpillDir, 0o755); err != nil {
		u.log.Error("create spill dir for seq file", "dir", u.cfg.SpillDir, "error", err)
		return
	}
	if err := os.WriteFile(u.seqPath, []byte(strconv.FormatInt(u.seq, 10)), 0o644); err != nil {
		u.log.Error("persist seq", "path", u.seqPath, "error", err)
	}
}

// readSeq loads the persisted seq, defaulting to 0 when the file is absent
// or corrupt.
func readSeq(path string, log *slog.Logger) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	seq, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || seq < 0 {
		log.Warn("ignoring corrupt seq file, starting from 0", "path", path)
		return 0
	}
	return seq
}

// spillLocked appends an event to the spill file. Callers hold u.mu.
func (u *Uploader) spillLocked(ev *protocol.InventoryEvent) {
	if u.spill == nil {
		if err := os.MkdirAll(u.cfg.SpillDir, 0o755); err != nil {
			u.log.Error("create spill dir, event dropped", "dir", u.cfg.SpillDir, "error", err)
			return
		}
		f, err := os.OpenFile(u.spillPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			u.log.Error("open spill file, event dropped", "path", u.spillPath, "error", err)
			return
		}
		u.spill = f
	}
	b, err := json.Marshal(ev)
	if err != nil {
		u.log.Error("marshal spilled event, event dropped", "error", err)
		return
	}
	if _, err := u.spill.Write(append(b, '\n')); err != nil {
		u.log.Error("write spill file, event dropped", "path", u.spillPath, "error", err)
	}
}

// replaySpillLocked loads spilled events back into the buffer and removes
// the spill file. Callers hold u.mu.
func (u *Uploader) replaySpillLocked() {
	if u.spill != nil {
		_ = u.spill.Close()
		u.spill = nil
	}
	f, err := os.Open(u.spillPath)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		u.log.Error("open spill file for replay", "path", u.spillPath, "error", err)
		return
	}
	defer f.Close()
	var events []*protocol.InventoryEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), maxSpillLineSize)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		ev := new(protocol.InventoryEvent)
		if err := json.Unmarshal(line, ev); err != nil {
			u.log.Error("decode spilled event, event dropped", "path", u.spillPath, "error", err)
			continue
		}
		events = append(events, ev)
	}
	if err := sc.Err(); err != nil {
		u.log.Error("read spill file", "path", u.spillPath, "error", err)
	}
	if len(events) == 0 {
		return
	}
	u.buf = append(u.buf, events...)
	if err := os.Remove(u.spillPath); err != nil {
		u.log.Error("remove spill file after replay", "path", u.spillPath, "error", err)
	}
	metrics.SetQueueDepth(len(u.buf))
	u.log.Info("replayed spilled events", "count", len(events))
}

func (u *Uploader) signal() {
	select {
	case u.notify <- struct{}{}:
	default:
	}
}

// sleep waits for d with ±20% jitter; false means ctx is done.
func (u *Uploader) sleep(ctx context.Context, d time.Duration) bool {
	jitter := 0.8 + 0.4*rand.Float64()
	timer := time.NewTimer(time.Duration(float64(d) * jitter))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
