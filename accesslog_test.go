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

func TestProxy_AccessLog_SanitizesFields(t *testing.T) {
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
		user:  "WALLET\nINJECT",
		agent: "XMRig\r\nBAD",
		conn:  noopConn{},
	}
	p.events.Dispatch(Event{Type: EventLogin, Miner: miner})
	p.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if strings.Count(text, "\n") != 1 {
		t.Fatalf("expected a single log line, got %q", text)
	}
	if strings.ContainsAny(text, "\r\t") {
		t.Fatalf("expected control characters to be stripped or escaped, got %q", text)
	}
}

func TestProxy_AccessLog_SanitizesColumns(t *testing.T) {
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
		user:  "WALLET MALICIOUS\nENTRY",
		agent: "XMRig 6.21.0\u2028BAD",
		conn:  noopConn{},
	}
	p.events.Dispatch(Event{Type: EventLogin, Miner: miner})
	p.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if strings.Contains(text, "WALLET MALICIOUS") || strings.Contains(text, "XMRig 6.21.0") {
		t.Fatalf("expected whitespace in column fields to be encoded, got %q", text)
	}
	if !strings.Contains(text, "WALLET_MALICIOUS_ENTRY") || !strings.Contains(text, "XMRig_6.21.0_BAD") {
		t.Fatalf("expected sanitized column fields to remain readable, got %q", text)
	}
	if strings.Count(text, "\n") != 1 {
		t.Fatalf("expected a single log line, got %q", text)
	}
}

func TestAccessLogSink_SetPath_Good(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.log")
	second := filepath.Join(dir, "second.log")

	sink := newAccessLogSink(first)
	sink.writeConnectLine("10.0.0.1", "WALLET", "XMRig")
	sink.SetPath(second)
	sink.writeConnectLine("10.0.0.2", "WALLET2", "XMRig/2")
	sink.Close()

	firstData, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read first log: %v", err)
	}
	secondData, err := os.ReadFile(second)
	if err != nil {
		t.Fatalf("read second log: %v", err)
	}
	if strings.Count(string(firstData), "CONNECT") != 1 {
		t.Fatalf("expected one line in first log, got %q", string(firstData))
	}
	if strings.Count(string(secondData), "CONNECT") != 1 {
		t.Fatalf("expected one line in second log, got %q", string(secondData))
	}
}

func TestAccessLogSink_SetPath_Bad(t *testing.T) {
	var sink *accessLogSink
	sink.SetPath("ignored")
}

func TestAccessLogSink_SetPath_Ugly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")

	sink := newAccessLogSink(path)
	sink.writeConnectLine("10.0.0.1", "WALLET", "XMRig")
	sink.SetPath(path)
	sink.writeCloseLine("10.0.0.1", "WALLET", 1, 2)
	sink.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if strings.Count(text, "CONNECT") != 1 || strings.Count(text, "CLOSE") != 1 {
		t.Fatalf("expected both lines to be preserved when path is unchanged, got %q", text)
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
