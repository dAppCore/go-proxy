package proxy

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProxy_AccessLog_WritesLifecycleLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")

	cfg := &Config{
		Mode:          "nicehash",
		Workers:       WorkersByRigID,
		AccessLogFile: path,
		Bind:          []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:         []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{
		ip:    "10.0.0.1",
		user:  "WALLET",
		agent: "XMRig/6.21.0",
		rx:    512,
		tx:    4096,
		conn:  noopConn{},
		state: MinerStateReady,
		rpcID: "session",
	}
	p.events.Dispatch(Event{Type: EventLogin, Miner: miner})
	p.events.Dispatch(Event{Type: EventClose, Miner: miner})
	p.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "CONNECT  10.0.0.1  WALLET  XMRig/6.21.0") {
		t.Fatalf("expected CONNECT line, got %q", text)
	}
	if !strings.Contains(text, "CLOSE  10.0.0.1  WALLET  rx=512  tx=4096") {
		t.Fatalf("expected CLOSE line, got %q", text)
	}
}

func TestProxy_AccessLog_WritesFixedColumns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")

	cfg := &Config{
		Mode:          "nicehash",
		Workers:       WorkersByRigID,
		AccessLogFile: path,
		Bind:          []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:         []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{
		ip:   "10.0.0.1",
		user: "WALLET",
		conn: noopConn{},
	}
	p.events.Dispatch(Event{Type: EventLogin, Miner: miner})
	p.events.Dispatch(Event{Type: EventClose, Miner: miner})
	p.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "CONNECT  10.0.0.1  WALLET") {
		t.Fatalf("expected CONNECT line without counters, got %q", text)
	}
	if !strings.Contains(text, "CLOSE  10.0.0.1  WALLET  rx=0  tx=0") {
		t.Fatalf("expected CLOSE line with counters only, got %q", text)
	}
}

type noopConn struct{}

func (noopConn) Read([]byte) (int, error)         { return 0, os.ErrClosed }
func (noopConn) Write([]byte) (int, error)        { return 0, os.ErrClosed }
func (noopConn) Close() error                     { return nil }
func (noopConn) LocalAddr() net.Addr              { return nil }
func (noopConn) RemoteAddr() net.Addr             { return nil }
func (noopConn) SetDeadline(time.Time) error      { return nil }
func (noopConn) SetReadDeadline(time.Time) error  { return nil }
func (noopConn) SetWriteDeadline(time.Time) error { return nil }
