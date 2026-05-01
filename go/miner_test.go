package proxy

import (
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"
)

type minerNewTestAddr string

func (a minerNewTestAddr) Network() string { return "tcp" }
func (a minerNewTestAddr) String() string  { return string(a) }

type minerNewTestConn struct {
	remote           net.Addr
	local            net.Addr
	closed           bool
	writeDeadline    time.Time
	writeDeadlineSet bool
}

func (c *minerNewTestConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (c *minerNewTestConn) Write(p []byte) (int, error)     { return len(p), nil }
func (c *minerNewTestConn) Close() error                    { c.closed = true; return nil }
func (c *minerNewTestConn) LocalAddr() net.Addr             { return c.local }
func (c *minerNewTestConn) RemoteAddr() net.Addr            { return c.remote }
func (c *minerNewTestConn) SetDeadline(time.Time) error     { return nil }
func (c *minerNewTestConn) SetReadDeadline(time.Time) error { return nil }
func (c *minerNewTestConn) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		c.writeDeadline = deadline
		c.writeDeadlineSet = true
	}
	return nil
}

func TestMiner_NewMiner_Good(t *testing.T) {
	conn := &minerNewTestConn{
		remote: minerNewTestAddr("10.0.0.1:49152"),
		local:  minerNewTestAddr("0.0.0.0:3333"),
	}
	miner := NewMiner(conn, 3333, nil)

	if miner.State() != MinerStateWaitLogin {
		t.Fatalf("expected miner to start in wait-login, got %v", miner.State())
	}
	if got := miner.MapperID(); got != -1 {
		t.Fatalf("expected mapper id to be unassigned, got %d", got)
	}
	if got := miner.RouteID(); got != -1 {
		t.Fatalf("expected route id to be unassigned, got %d", got)
	}
	if got := miner.IP(); got != "10.0.0.1" {
		t.Fatalf("expected host-only IP, got %q", got)
	}
	if got := miner.RemoteAddr(); got != "10.0.0.1:49152" {
		t.Fatalf("expected remote address to be preserved, got %q", got)
	}
	if miner.tlsConn != nil {
		t.Fatalf("expected plain connection to remain non-TLS")
	}
}

func TestMiner_NewMiner_Bad(t *testing.T) {
	conn := &minerNewTestConn{
		remote: minerNewTestAddr("10.0.0.2:49153"),
		local:  minerNewTestAddr("0.0.0.0:4444"),
	}
	miner := NewMiner(conn, 4444, &tls.Config{})

	if miner.tlsConn == nil {
		t.Fatalf("expected TLS config to wrap the miner connection")
	}
	if miner.localPort != 4444 {
		t.Fatalf("expected local port to be recorded, got %d", miner.localPort)
	}
}

func TestMiner_NewMiner_Ugly(t *testing.T) {
	conn := &minerNewTestConn{}
	miner := NewMiner(conn, 0, nil)

	if got := miner.IP(); got != "" {
		t.Fatalf("expected empty remote address to yield empty IP, got %q", got)
	}
	if got := miner.RemoteAddr(); got != "" {
		t.Fatalf("expected empty remote address to be preserved as empty, got %q", got)
	}
	if miner.State() != MinerStateWaitLogin {
		t.Fatalf("expected miner state to remain initialised, got %v", miner.State())
	}
}

func TestMiner_writeJSON_Good(t *testing.T) {
	conn := &minerNewTestConn{
		remote: minerNewTestAddr("10.0.0.1:49152"),
		local:  minerNewTestAddr("0.0.0.0:3333"),
	}
	miner := NewMiner(conn, 3333, nil)

	if r := miner.writeJSON(map[string]any{"hello": "world"}); !r.OK {
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
