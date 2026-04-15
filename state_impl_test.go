package proxy

import (
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"
)

type minerTestAddr string

func (a minerTestAddr) Network() string { return "tcp" }
func (a minerTestAddr) String() string  { return string(a) }

type minerTestConn struct {
	remote net.Addr
	local  net.Addr
}

func (c *minerTestConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *minerTestConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *minerTestConn) Close() error                     { return nil }
func (c *minerTestConn) LocalAddr() net.Addr              { return c.local }
func (c *minerTestConn) RemoteAddr() net.Addr             { return c.remote }
func (c *minerTestConn) SetDeadline(time.Time) error      { return nil }
func (c *minerTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *minerTestConn) SetWriteDeadline(time.Time) error { return nil }

func TestStateImpl_NewMiner_Good(t *testing.T) {
	conn := &minerTestConn{
		remote: minerTestAddr("203.0.113.7:49152"),
		local:  minerTestAddr("127.0.0.1:3333"),
	}

	miner := NewMiner(conn, 3333, nil)
	if miner == nil {
		t.Fatal("expected miner instance")
	}
	if miner.State() != MinerStateWaitLogin {
		t.Fatalf("expected wait-login state, got %d", miner.State())
	}
	if miner.MapperID() != -1 {
		t.Fatalf("expected mapper id to default to -1, got %d", miner.MapperID())
	}
	if miner.RouteID() != -1 {
		t.Fatalf("expected route id to default to -1, got %d", miner.RouteID())
	}
	if got := miner.RemoteAddr(); got != "203.0.113.7:49152" {
		t.Fatalf("expected remote addr to be captured, got %q", got)
	}
	if got := miner.IP(); got != "203.0.113.7" {
		t.Fatalf("expected IP to strip port, got %q", got)
	}
	if got := miner.localPort; got != 3333 {
		t.Fatalf("expected local port to be recorded, got %d", got)
	}
	if miner.connectedAt.IsZero() || miner.lastActivityAt.IsZero() {
		t.Fatal("expected timestamps to be initialised")
	}
	if miner.tlsConn != nil {
		t.Fatal("expected plain TCP miner to have no TLS conn")
	}
}

func TestStateImpl_NewMiner_Bad(t *testing.T) {
	conn := &minerTestConn{
		local: minerTestAddr("127.0.0.1:4444"),
	}

	miner := NewMiner(conn, 4444, nil)
	if miner == nil {
		t.Fatal("expected miner instance")
	}
	if got := miner.RemoteAddr(); got != "" {
		t.Fatalf("expected empty remote addr when connection has none, got %q", got)
	}
	if got := miner.IP(); got != "" {
		t.Fatalf("expected empty IP when connection has none, got %q", got)
	}
	if got := miner.localPort; got != 4444 {
		t.Fatalf("expected local port to be recorded, got %d", got)
	}
	if miner.tlsConn != nil {
		t.Fatal("expected plain TCP miner to have no TLS conn")
	}
}

func TestStateImpl_NewMiner_Ugly(t *testing.T) {
	conn := &minerTestConn{
		remote: minerTestAddr("198.51.100.9:5555"),
		local:  minerTestAddr("127.0.0.1:5555"),
	}

	miner := NewMiner(conn, 5555, &tls.Config{})
	if miner == nil {
		t.Fatal("expected miner instance")
	}
	if miner.tlsConn == nil {
		t.Fatal("expected TLS config to wrap the miner connection")
	}
	if _, ok := miner.conn.(*tls.Conn); !ok {
		t.Fatalf("expected wrapped connection to be a tls.Conn, got %T", miner.conn)
	}
	if got := miner.RemoteAddr(); got != "198.51.100.9:5555" {
		t.Fatalf("expected remote addr to remain available after TLS wrapping, got %q", got)
	}
	if got := miner.IP(); got != "198.51.100.9" {
		t.Fatalf("expected IP to remain available after TLS wrapping, got %q", got)
	}
}
