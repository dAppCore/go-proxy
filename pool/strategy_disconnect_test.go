package pool

import (
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

type disconnectSpy struct {
	disconnects atomic.Int64
}

func (s *disconnectSpy) OnJob(proxy.Job) {}

func (s *disconnectSpy) OnResultAccepted(int64, bool, string) {}

func (s *disconnectSpy) OnDisconnect() {
	s.disconnects.Add(1)
}

func TestFailoverStrategy_Disconnect_Good(t *testing.T) {
	spy := &disconnectSpy{}
	strategy := &FailoverStrategy{
		listener: spy,
		client:   &StratumClient{listener: nil},
	}
	strategy.client.listener = strategy

	strategy.Disconnect()
	time.Sleep(10 * time.Millisecond)

	if got := spy.disconnects.Load(); got != 0 {
		t.Fatalf("expected intentional disconnect to suppress reconnect, got %d listener calls", got)
	}
}

func TestFailoverStrategy_Disconnect_Bad(t *testing.T) {
	spy := &disconnectSpy{}
	strategy := &FailoverStrategy{listener: spy}

	strategy.OnDisconnect()

	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected external disconnect to notify listener once, got %d", got)
	}
}

func TestFailoverStrategy_Disconnect_Ugly(t *testing.T) {
	spy := &disconnectSpy{}
	strategy := &FailoverStrategy{
		listener: spy,
		client:   &StratumClient{listener: nil},
	}
	strategy.client.listener = strategy

	strategy.Disconnect()
	strategy.Disconnect()
	time.Sleep(10 * time.Millisecond)

	if got := spy.disconnects.Load(); got != 0 {
		t.Fatalf("expected repeated intentional disconnects to remain silent, got %d listener calls", got)
	}
}

func TestStratumClient_NotifyDisconnect_ClearsState_Good(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	spy := &disconnectSpy{}
	client := &StratumClient{
		conn:      clientConn,
		listener:  spy,
		sessionID: "session-1",
		active:    true,
		pending: map[int64]struct{}{
			7: {},
		},
	}

	client.notifyDisconnect()

	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected one disconnect notification, got %d", got)
	}
	if client.conn != nil {
		t.Fatalf("expected pooled connection to be cleared")
	}
	if client.sessionID != "" {
		t.Fatalf("expected session id to be cleared, got %q", client.sessionID)
	}
	if client.IsActive() {
		t.Fatalf("expected client to stop reporting active after disconnect")
	}
	if len(client.pending) != 0 {
		t.Fatalf("expected pending submit state to be cleared, got %d entries", len(client.pending))
	}
}

func TestFailoverStrategy_OnDisconnect_ClearsClient_Bad(t *testing.T) {
	spy := &disconnectSpy{}
	strategy := &FailoverStrategy{
		listener: spy,
		client:   &StratumClient{active: true, pending: make(map[int64]struct{})},
	}

	strategy.OnDisconnect()
	time.Sleep(10 * time.Millisecond)

	if strategy.client != nil {
		t.Fatalf("expected strategy to drop the stale client before reconnect")
	}
	if strategy.IsActive() {
		t.Fatalf("expected strategy to report inactive while reconnect is pending")
	}
	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected one disconnect notification, got %d", got)
	}
}

func TestStratumClient_HandleMessage_LoginErrorDisconnects_Ugly(t *testing.T) {
	spy := &disconnectSpy{}
	client := &StratumClient{
		listener: spy,
		pending:  make(map[int64]struct{}),
	}

	payload, err := json.Marshal(map[string]any{
		"id":      1,
		"jsonrpc": "2.0",
		"error": map[string]any{
			"code":    -1,
			"message": "Invalid payment address provided",
		},
	})
	if err != nil {
		t.Fatalf("marshal login error payload: %v", err)
	}

	client.handleMessage(payload)

	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected login failure to disconnect upstream once, got %d", got)
	}
}
