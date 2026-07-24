package uploader

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/protocol"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{ClusterID: "cluster-1", SpillDir: t.TempDir()}
}

// fakeSender is a scripted BatchSender: every call is recorded on attempts,
// calls listed in failAt (1-based) return the scripted error, the rest are
// accepted onto accepted with a successful ack.
type fakeSender struct {
	mu       sync.Mutex
	calls    int
	failAt   map[int]error
	attempts chan *protocol.InventoryBatch
	accepted chan *protocol.InventoryBatch
}

func newFakeSender() *fakeSender {
	return &fakeSender{
		failAt:   make(map[int]error),
		attempts: make(chan *protocol.InventoryBatch, 4096),
		accepted: make(chan *protocol.InventoryBatch, 4096),
	}
}

func (f *fakeSender) send(ctx context.Context, b *protocol.InventoryBatch) (*protocol.InventoryAck, error) {
	f.mu.Lock()
	f.calls++
	err := f.failAt[f.calls]
	f.mu.Unlock()
	f.attempts <- b
	if err != nil {
		return nil, err
	}
	f.accepted <- b
	return &protocol.InventoryAck{OK: true, LastSeq: b.Seq}, nil
}

func (f *fakeSender) failCall(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAt[n] = err
}

// staticSnapshotter returns a fixed snapshot.
type staticSnapshotter struct {
	events []*protocol.InventoryEvent
}

func (s staticSnapshotter) Snapshot() []*protocol.InventoryEvent { return s.events }

func ev(name string) *protocol.InventoryEvent {
	return &protocol.InventoryEvent{
		Op:        protocol.OpAdd,
		Ref:       protocol.ResourceRef{Version: "v1", Resource: "configmaps", Namespace: "default", Name: name},
		Timestamp: 1,
	}
}

// evSized returns an event whose estimated size is len(objectJSON)+256.
func evSized(name string, objectBytes int) *protocol.InventoryEvent {
	e := ev(name)
	e.ObjectJSON = make([]byte, objectBytes)
	return e
}

// collect reads n events from the sender's accepted batches, preserving order.
func collect(t *testing.T, f *fakeSender, n int) []*protocol.InventoryBatch {
	t.Helper()
	var batches []*protocol.InventoryBatch
	got := 0
	for got < n {
		select {
		case b := <-f.accepted:
			batches = append(batches, b)
			got += len(b.Events)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %d events, got %d", n, got)
		}
	}
	return batches
}

func eventsOf(batches []*protocol.InventoryBatch) []*protocol.InventoryEvent {
	var out []*protocol.InventoryEvent
	for _, b := range batches {
		out = append(out, b.Events...)
	}
	return out
}

func TestSnapshotThenOrderedBatches(t *testing.T) {
	sender := newFakeSender()
	snap := staticSnapshotter{events: []*protocol.InventoryEvent{ev("snap-1"), ev("snap-2"), ev("snap-3")}}
	u := New(testConfig(t), sender.send, snap, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	u.Enqueue(ev("live-1"))
	u.Enqueue(ev("live-2"))

	batches := collect(t, sender, 5)
	if len(batches) < 2 {
		t.Fatalf("expected at least 2 batches, got %d", len(batches))
	}
	first := batches[0]
	if !first.Full {
		t.Error("first batch must be the full snapshot")
	}
	if first.ClusterID != "cluster-1" {
		t.Errorf("ClusterID = %q, want cluster-1", first.ClusterID)
	}
	var prev int64
	for _, b := range batches {
		if b.Seq <= prev {
			t.Errorf("seq not increasing: %d after %d", b.Seq, prev)
		}
		prev = b.Seq
	}
	got := eventsOf(batches)
	wantOrder := []string{"snap-1", "snap-2", "snap-3", "live-1", "live-2"}
	for i, name := range wantOrder {
		if got[i].Ref.Name != name {
			t.Errorf("event %d = %q, want %q", i, got[i].Ref.Name, name)
		}
	}
}

func TestSendErrorRetriesSameSeq(t *testing.T) {
	sender := newFakeSender()
	sender.failCall(1, errors.New("boom"))
	u := New(testConfig(t), sender.send, staticSnapshotter{}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	u.Enqueue(ev("a"))
	u.Enqueue(ev("b"))

	// First attempt fails, the retry must resend the same batch with the
	// same seq.
	failed := <-sender.attempts
	select {
	case retried := <-sender.attempts:
		if retried.Seq != failed.Seq {
			t.Errorf("retry seq = %d, want same as failed attempt %d", retried.Seq, failed.Seq)
		}
		if len(retried.Events) != 2 || retried.Events[0].Ref.Name != "a" || retried.Events[1].Ref.Name != "b" {
			t.Errorf("retried batch events = %v, want [a b]", eventsOf([]*protocol.InventoryBatch{retried}))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the retry")
	}

	// The batch is eventually accepted with that same seq.
	batches := collect(t, sender, 2)
	if batches[0].Seq != failed.Seq {
		t.Errorf("accepted seq = %d, want %d", batches[0].Seq, failed.Seq)
	}
}

func TestSeqFileWrittenAndResumed(t *testing.T) {
	cfg := testConfig(t)
	seqPath := filepath.Join(cfg.SpillDir, seqFileName)

	sender := newFakeSender()
	u := New(cfg, sender.send, staticSnapshotter{}, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go u.Run(ctx)

	u.Enqueue(ev("a"))
	collect(t, sender, 1)

	// The seq file is written after the ack.
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(seqPath)
		if err == nil && strings.TrimSpace(string(data)) == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("seq file = %q, %v; want \"1\"", strings.TrimSpace(string(data)), err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	// A fresh Uploader on the same SpillDir resumes the persisted seq.
	sender2 := newFakeSender()
	u2 := New(cfg, sender2.send, staticSnapshotter{}, testLogger())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go u2.Run(ctx2)

	u2.Enqueue(ev("b"))
	batches := collect(t, sender2, 1)
	if batches[0].Seq != 2 {
		t.Errorf("resumed seq = %d, want 2", batches[0].Seq)
	}
}

func TestCorruptSeqFileStartsAtZero(t *testing.T) {
	cfg := testConfig(t)
	if err := os.WriteFile(filepath.Join(cfg.SpillDir, seqFileName), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	sender := newFakeSender()
	u := New(cfg, sender.send, staticSnapshotter{}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	u.Enqueue(ev("a"))
	batches := collect(t, sender, 1)
	if batches[0].Seq != 1 {
		t.Errorf("seq = %d, want 1 after corrupt seq file", batches[0].Seq)
	}
}

func TestSnapshotResentAfterError(t *testing.T) {
	sender := newFakeSender()
	// The snapshot (call 1) succeeds; the first delta send (call 2) fails.
	sender.failCall(2, errors.New("boom"))
	snap := staticSnapshotter{events: []*protocol.InventoryEvent{ev("snap-1"), ev("snap-2")}}
	u := New(testConfig(t), sender.send, snap, testLogger())

	u.Enqueue(ev("live-1"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	batches := collect(t, sender, 5) // snapshot(2) + snapshot(2) + delta(1)
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(batches))
	}
	if !batches[0].Full || !batches[1].Full {
		t.Error("snapshot must be sent, then resent after the send error")
	}
	if batches[2].Full {
		t.Error("third batch must be the delta")
	}
	for i, want := range []int64{1, 2, 3} {
		if batches[i].Seq != want {
			t.Errorf("batch %d seq = %d, want %d", i, batches[i].Seq, want)
		}
	}
	got := eventsOf(batches)
	wantOrder := []string{"snap-1", "snap-2", "snap-1", "snap-2", "live-1"}
	for i, name := range wantOrder {
		if got[i].Ref.Name != name {
			t.Errorf("event %d = %q, want %q", i, got[i].Ref.Name, name)
		}
	}
}

func TestAuthErrorKeepsBackingOff(t *testing.T) {
	sender := newFakeSender()
	sender.failCall(1, &protocol.AuthError{Status: 401, Body: "bad token"})
	sender.failCall(2, &protocol.AuthError{Status: 401, Body: "bad token"})
	sender.failCall(3, &protocol.AuthError{Status: 401, Body: "bad token"})
	u := New(testConfig(t), sender.send, staticSnapshotter{}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	u.Enqueue(ev("a"))

	// Auth errors back off (1s → 2s → ...): in 600ms there must be no hot
	// spin, only the first failed attempt.
	time.Sleep(600 * time.Millisecond)
	attempts := len(sender.attempts)
	if attempts == 0 {
		t.Fatal("expected at least one attempt")
	}
	if attempts > 2 {
		t.Errorf("hot spin on AuthError: %d attempts in 600ms", attempts)
	}
}

func TestBatchByteBudgetSnapshot(t *testing.T) {
	sender := newFakeSender()
	// 1 KiB budget, events of 44+256=300 estimated bytes each: three fit
	// (900), a fourth would exceed the budget (1200), so it starts a new
	// batch — even in the full snapshot.
	snap := staticSnapshotter{events: []*protocol.InventoryEvent{
		evSized("a", 44), evSized("b", 44), evSized("c", 44), evSized("d", 44),
	}}
	u := New(testConfig(t), sender.send, snap, testLogger())
	u.maxBatchBytes = 1024

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	batches := collect(t, sender, 4)
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	if !batches[0].Full || !batches[1].Full {
		t.Error("both batches must be part of the full snapshot")
	}
	if len(batches[0].Events) != 3 || len(batches[1].Events) != 1 {
		t.Errorf("batch sizes = %d, %d; want 3, 1", len(batches[0].Events), len(batches[1].Events))
	}
	got := eventsOf(batches)
	for i, name := range []string{"a", "b", "c", "d"} {
		if got[i].Ref.Name != name {
			t.Errorf("event %d = %q, want %q (order must be preserved)", i, got[i].Ref.Name, name)
		}
	}
}

func TestOversizedEventSentAlone(t *testing.T) {
	sender := newFakeSender()
	u := New(testConfig(t), sender.send, staticSnapshotter{}, testLogger())
	u.maxBatchBytes = 1024

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	// The big event (2000+256 estimated bytes) exceeds the budget: it must
	// go out alone, neither dropped nor stuck, with order preserved.
	u.Enqueue(evSized("small-1", 44))
	u.Enqueue(evSized("big", 2000))
	u.Enqueue(evSized("small-2", 44))

	batches := collect(t, sender, 3)
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(batches))
	}
	for i, b := range batches {
		if len(b.Events) != 1 {
			t.Fatalf("batch %d has %d events, want 1", i, len(b.Events))
		}
	}
	for i, name := range []string{"small-1", "big", "small-2"} {
		if batches[i].Events[0].Ref.Name != name {
			t.Errorf("batch %d = %q, want %q", i, batches[i].Events[0].Ref.Name, name)
		}
	}
}

func TestCountCapStillWorks(t *testing.T) {
	sender := newFakeSender()
	u := New(testConfig(t), sender.send, staticSnapshotter{}, testLogger())

	// 250 small events: well under the default byte budget, so the
	// 200-event count cap must split them 200 + 50.
	for i := 0; i < 250; i++ {
		u.Enqueue(ev("e"))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	batches := collect(t, sender, 250)
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	if len(batches[0].Events) != 200 || len(batches[1].Events) != 50 {
		t.Errorf("batch sizes = %d, %d; want 200, 50", len(batches[0].Events), len(batches[1].Events))
	}
}

func TestSpillAndReplay(t *testing.T) {
	cfg := testConfig(t)
	sender := newFakeSender()
	u := NewWithCapacity(cfg, sender.send, staticSnapshotter{}, testLogger(), 10)

	// Fill the buffer beyond capacity before Run starts: 10 buffered, 5 spilled.
	for i := 0; i < 15; i++ {
		u.Enqueue(ev(string(rune('a' + i))))
	}
	spillPath := filepath.Join(cfg.SpillDir, spillFileName)
	data, err := os.ReadFile(spillPath)
	if err != nil {
		t.Fatalf("read spill file: %v", err)
	}
	if lines := strings.Count(strings.TrimSpace(string(data)), "\n") + 1; lines != 5 {
		t.Fatalf("spill file has %d lines, want 5", lines)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	batches := collect(t, sender, 15)
	got := eventsOf(batches)
	for i, e := range got {
		want := string(rune('a' + i))
		if e.Ref.Name != want {
			t.Fatalf("event %d = %q, want %q (spilled events must replay oldest first)", i, e.Ref.Name, want)
		}
	}

	// The spill file is removed once replayed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(spillPath); os.IsNotExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("spill file not removed after replay")
}

func TestConcurrentEnqueue(t *testing.T) {
	sender := newFakeSender()
	u := New(testConfig(t), sender.send, staticSnapshotter{}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.Run(ctx)

	const workers = 8
	const perWorker = 100
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				u.Enqueue(ev("e"))
			}
		}(w)
	}

	batches := collect(t, sender, workers*perWorker)
	if got := len(eventsOf(batches)); got != workers*perWorker {
		t.Errorf("received %d events, want %d", got, workers*perWorker)
	}
	wg.Wait()
}
