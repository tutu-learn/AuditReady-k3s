package receiver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"AuditReady-k3s/internal/protocol"
)

var errPushDown = errors.New("ws write failed")

// fakePush is a scripted PushChannel: it records reports sent over the fast
// path and hands the registered command handler back to the test.
type fakePush struct {
	mu        sync.Mutex
	connected bool
	sendErr   error
	sent      []*protocol.Report
	handler   func(*protocol.Command)
}

func (f *fakePush) Connected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

func (f *fakePush) SendReport(_ context.Context, rep *protocol.Report) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, rep)
	return nil
}

func (f *fakePush) SetCommandHandler(h func(*protocol.Command)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handler = h
}

// push delivers a command as if it arrived over the WebSocket.
func (f *fakePush) push(cmd *protocol.Command) {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	h(cmd)
}

func (f *fakePush) findReport(commandID, status string) *protocol.Report {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rep := range f.sent {
		if rep.CommandID == commandID && (status == "" || rep.Status == status) {
			return rep
		}
	}
	return nil
}

// startWithPush runs the fixture's Receiver with push wired in.
func (f *fixture) startWithPush(t *testing.T, push *fakePush) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go New(f.cfg, f.poll.poll, f.h, testLogger(), push).Run(ctx)
}

func TestPushedCommandValidatedDispatchedAndReportedOverWS(t *testing.T) {
	f := newFixture(t, true)
	push := &fakePush{connected: true}
	f.startWithPush(t, push)

	push.push(f.signedCommand("cmd-ws", "nonce-ws"))

	select {
	case got := <-f.h.calls:
		if got.ID != "cmd-ws" {
			t.Fatalf("handler got command %q, want cmd-ws", got.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// The received report went over the fast path, not the poll queue.
	waitFor(t, "received report over ws", func() bool { return push.findReport("cmd-ws", protocol.StatusReceived) != nil })
	rep := push.findReport("cmd-ws", protocol.StatusReceived)
	if rep.ClusterID != "test-cluster" || rep.Timestamp == 0 {
		t.Fatalf("report not stamped: %+v", rep)
	}
	if rep := f.poll.findReport("cmd-ws", ""); rep != nil {
		t.Fatalf("report %+v fell back to the poll queue while ws was up", rep)
	}
}

func TestDuplicateCommandIDDroppedSilently(t *testing.T) {
	f := newFixture(t, true)
	push := &fakePush{connected: true}
	f.startWithPush(t, push)

	cmd := f.signedCommand("cmd-dup", "nonce-dup")
	push.push(cmd)

	select {
	case <-f.h.calls:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// Failover race: a later poll delivers the very same command. It must be
	// dropped silently — no second execution, no refused report, no metric.
	before := f.poll.numRequests()
	f.poll.scriptAdd(pollResult{resp: &protocol.PollResponse{Commands: []*protocol.Command{cmd}}})
	waitFor(t, "duplicate delivered by poll", func() bool { return f.poll.numRequests() > before })
	f.noCallFor(t, "cmd-dup")
	if rep := push.findReport("cmd-dup", protocol.StatusRefused); rep != nil {
		t.Fatalf("duplicate produced a refused report over ws: %+v", rep)
	}
	if rep := f.poll.findReport("cmd-dup", protocol.StatusRefused); rep != nil {
		t.Fatalf("duplicate produced a refused report on poll: %+v", rep)
	}
}

func TestReportFallsBackToPollQueueWhenPushFails(t *testing.T) {
	f := newFixture(t, true)
	push := &fakePush{connected: true, sendErr: errPushDown}
	f.startWithPush(t, push)

	push.push(f.signedCommand("cmd-fb", "nonce-fb"))

	select {
	case <-f.h.calls:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	// The fast path errors, so the received report lands in the poll queue
	// and piggybacks on a subsequent poll request.
	waitFor(t, "received report piggybacked on poll", func() bool {
		return f.poll.findReport("cmd-fb", protocol.StatusReceived) != nil
	})
}

func TestReportQueuedWhenPushDisconnected(t *testing.T) {
	f := newFixture(t, true)
	push := &fakePush{connected: false}
	f.startWithPush(t, push)

	push.push(f.signedCommand("cmd-off", "nonce-off"))

	select {
	case <-f.h.calls:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked")
	}

	waitFor(t, "received report piggybacked on poll", func() bool {
		return f.poll.findReport("cmd-off", protocol.StatusReceived) != nil
	})
	if rep := push.findReport("cmd-off", ""); rep != nil {
		t.Fatalf("report %+v sent over a disconnected push channel", rep)
	}
}

func TestRefusedPushedCommandReportedOverWS(t *testing.T) {
	f := newFixture(t, true)
	push := &fakePush{connected: true}
	f.startWithPush(t, push)

	cmd := f.signedCommand("cmd-bad", "nonce-bad")
	cmd.Payload = []byte("tampered") // breaks the signature
	push.push(cmd)

	waitFor(t, "refused report over ws", func() bool { return push.findReport("cmd-bad", protocol.StatusRefused) != nil })
	f.noCallFor(t, "cmd-bad")
}

// scriptAdd appends poll results under lock, for tests that extend the
// script while the Receiver is already running.
func (f *fakePoll) scriptAdd(results ...pollResult) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.script = append(f.script, results...)
}
