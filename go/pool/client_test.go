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

	if r := client.writeJSON(map[string]any{"hello": "world"}); !r.OK {
		t.Fatalf("expected writeJSON to succeed, got %v", r.Value)
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

func TestClient_NewStratumClient_Good(t *testing.T) {
	listener := &clientDisconnectSpy{}
	client := NewStratumClient(proxy.PoolConfig{URL: "pool.example:3333"}, listener)
	if client == nil || client.listener != listener || client.pending == nil {
		t.Fatalf("expected initialized stratum client, got %+v", client)
	}
}

func TestClient_NewStratumClient_Bad(t *testing.T) {
	client := NewStratumClient(proxy.PoolConfig{}, nil)
	if client == nil || client.config.URL != "" || client.listener != nil {
		t.Fatalf("expected empty client config with nil listener, got %+v", client)
	}
}

func TestClient_NewStratumClient_Ugly(t *testing.T) {
	client := NewStratumClient(proxy.PoolConfig{TLS: true, TLSFingerprint: "abc"}, nil)
	if !client.config.TLS || client.config.TLSFingerprint != "abc" {
		t.Fatalf("expected TLS config retained, got %+v", client.config)
	}
}

func TestClient_StratumClient_IsActive_Good(t *testing.T) {
	client := &StratumClient{active: true}
	if !client.IsActive() {
		t.Fatal("expected active client")
	}
}

func TestClient_StratumClient_IsActive_Bad(t *testing.T) {
	var client *StratumClient
	if client.IsActive() {
		t.Fatal("expected nil client inactive")
	}
}

func TestClient_StratumClient_IsActive_Ugly(t *testing.T) {
	client := &StratumClient{}
	if client.IsActive() {
		t.Fatal("expected zero-value client inactive")
	}
}

func TestClient_StratumClient_SessionID_Good(t *testing.T) {
	client := &StratumClient{sessionID: "session-1"}
	if got := client.SessionID(); got != "session-1" {
		t.Fatalf("expected session id, got %q", got)
	}
}

func TestClient_StratumClient_SessionID_Bad(t *testing.T) {
	var client *StratumClient
	if got := client.SessionID(); got != "" {
		t.Fatalf("expected nil client session empty, got %q", got)
	}
}

func TestClient_StratumClient_SessionID_Ugly(t *testing.T) {
	client := &StratumClient{}
	if got := client.SessionID(); got != "" {
		t.Fatalf("expected zero-value session empty, got %q", got)
	}
}

func TestClient_StratumClient_Connect_Ugly(t *testing.T) {
	var client *StratumClient
	result := client.Connect()
	if result.OK || result.Error == nil {
		t.Fatalf("expected nil client connect failure, got %+v", result)
	}
}

func TestClient_StratumClient_Login_Good(t *testing.T) {
	conn := &tickConn{}
	client := &StratumClient{config: proxy.PoolConfig{User: "wallet", Pass: "x"}, conn: conn, pending: make(map[int64]struct{})}
	client.Login()
	if got := conn.writes.Load(); got != 1 {
		t.Fatalf("expected one login write, got %d", got)
	}
}

func TestClient_StratumClient_Login_Bad(t *testing.T) {
	client := &StratumClient{}
	client.Login()
	if client.seq != 0 {
		t.Fatalf("expected login without conn ignored, seq=%d", client.seq)
	}
}

func TestClient_StratumClient_Login_Ugly(t *testing.T) {
	conn := &tickConn{}
	client := &StratumClient{config: proxy.PoolConfig{User: "wallet", RigID: "rig", Algo: "rx/0"}, conn: conn, seq: 5, pending: make(map[int64]struct{})}
	client.Login()
	if client.seq != 5 || conn.writes.Load() != 1 {
		t.Fatalf("expected existing seq retained and one write, seq=%d writes=%d", client.seq, conn.writes.Load())
	}
}

func TestClient_StratumClient_Disconnect_Good(t *testing.T) {
	conn := &clientTestConn{}
	spy := &clientDisconnectSpy{}
	client := &StratumClient{listener: spy, conn: conn, sessionID: "session", active: true, pending: map[int64]struct{}{1: {}}}
	client.Disconnect()
	if conn.closed.Load() != 1 || spy.disconnects.Load() != 1 || client.conn != nil || client.IsActive() {
		t.Fatalf("expected disconnect cleanup, closed=%d disconnects=%d conn=%v active=%v", conn.closed.Load(), spy.disconnects.Load(), client.conn, client.IsActive())
	}
}

func TestClient_StratumClient_Disconnect_Bad(t *testing.T) {
	var client *StratumClient
	client.Disconnect()
	if client != nil {
		t.Fatal("expected nil client to remain nil")
	}
}

func TestClient_StratumClient_Disconnect_Ugly(t *testing.T) {
	conn := &clientTestConn{}
	spy := &clientDisconnectSpy{}
	client := &StratumClient{listener: spy, conn: conn, pending: map[int64]struct{}{}}
	client.Disconnect()
	client.Disconnect()
	if conn.closed.Load() != 1 || spy.disconnects.Load() != 1 {
		t.Fatalf("expected idempotent disconnect, closed=%d disconnects=%d", conn.closed.Load(), spy.disconnects.Load())
	}
}
