package pool

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

type clientTestConn struct {
	closed           atomic.Int64
	writeDeadline    time.Time
	writeDeadlineSet bool
}

func (c *clientTestConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (c *clientTestConn) Write(p []byte) (int, error)     { return len(p), nil }
func (c *clientTestConn) Close() error                    { c.closed.Add(1); return nil }
func (c *clientTestConn) LocalAddr() net.Addr             { return clientTestAddr("127.0.0.1:1") }
func (c *clientTestConn) RemoteAddr() net.Addr            { return clientTestAddr("127.0.0.1:2") }
func (c *clientTestConn) SetDeadline(time.Time) error     { return nil }
func (c *clientTestConn) SetReadDeadline(time.Time) error { return nil }
func (c *clientTestConn) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		c.writeDeadline = deadline
		c.writeDeadlineSet = true
	}
	return nil
}

type clientTestAddr string

func (a clientTestAddr) Network() string { return "tcp" }
func (a clientTestAddr) String() string  { return string(a) }

type clientDisconnectSpy struct {
	disconnects atomic.Int64
}

func (s *clientDisconnectSpy) OnJob(proxy.Job)                      {}
func (s *clientDisconnectSpy) OnResultAccepted(int64, bool, string) {}
func (s *clientDisconnectSpy) OnDisconnect()                        { s.disconnects.Add(1) }

func TestClient_Disconnect_Good(t *testing.T) {
	conn := &clientTestConn{}
	spy := &clientDisconnectSpy{}
	client := &StratumClient{
		config:    proxy.PoolConfig{URL: "pool.example:3333"},
		listener:  spy,
		conn:      conn,
		sessionID: "session-1",
		active:    true,
		pending:   map[int64]struct{}{7: {}},
	}

	client.Disconnect()

	if got := conn.closed.Load(); got != 1 {
		t.Fatalf("expected underlying connection to be closed once, got %d", got)
	}
	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected one disconnect notification, got %d", got)
	}
	if client.conn != nil {
		t.Fatal("expected connection to be cleared")
	}
	if got := client.SessionID(); got != "" {
		t.Fatalf("expected session id to be cleared, got %q", got)
	}
	if client.IsActive() {
		t.Fatal("expected client to stop reporting active")
	}
	if len(client.pending) != 0 {
		t.Fatalf("expected pending state to be reset, got %d entries", len(client.pending))
	}
}

func TestClient_Disconnect_Bad(t *testing.T) {
	var client *StratumClient
	client.Disconnect()

	client = &StratumClient{}
	client.Disconnect()
}

func TestClient_Disconnect_Ugly(t *testing.T) {
	conn := &clientTestConn{}
	spy := &clientDisconnectSpy{}
	client := &StratumClient{
		listener: spy,
		conn:     conn,
		pending:  map[int64]struct{}{},
	}

	client.Disconnect()
	client.Disconnect()

	if got := conn.closed.Load(); got != 1 {
		t.Fatalf("expected disconnect to be idempotent, got %d closes", got)
	}
	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected listener to be notified once, got %d", got)
	}
}

func TestClient_writeJSON_Good(t *testing.T) {
	conn := &clientTestConn{}
	client := &StratumClient{
		conn:    conn,
		pending: map[int64]struct{}{},
	}

	if err := client.writeJSON(map[string]any{"hello": "world"}); err != nil {
		t.Fatalf("expected writeJSON to succeed, got %v", err)
	}
	if !conn.writeDeadlineSet {
		t.Fatal("expected writeJSON to set a write deadline")
	}
	if conn.writeDeadline.IsZero() {
		t.Fatal("expected write deadline to be finite")
	}
	if time.Until(conn.writeDeadline) <= 0 {
		t.Fatal("expected write deadline to be in the future")
	}
}
