package proxy

import (
	"net"
	"testing"
	"time"

	core "dappco.re/go"
)

func TestProxy_AccessLog_WritesLifecycleLines(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "access.log")

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

	data, err := readFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if !containsString(text, "CONNECT  10.0.0.1  WALLET  XMRig/6.21.0") {
		t.Fatalf("expected CONNECT line, got %q", text)
	}
	if !containsString(text, "CLOSE  10.0.0.1  WALLET  rx=512  tx=4096") {
		t.Fatalf("expected CLOSE line, got %q", text)
	}
}

func TestProxy_AccessLog_WritesFixedColumns(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "access.log")

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

	data, err := readFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if !containsString(text, "CONNECT  10.0.0.1  WALLET") {
		t.Fatalf("expected CONNECT line without counters, got %q", text)
	}
	if !containsString(text, "CLOSE  10.0.0.1  WALLET  rx=0  tx=0") {
		t.Fatalf("expected CLOSE line with counters only, got %q", text)
	}
}

func TestProxy_AccessLog_SanitizesFields(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "access.log")

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

	data, err := readFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if countString(text, "\n") != 1 {
		t.Fatalf("expected a single log line, got %q", text)
	}
	if containsAnyString(text, "\r\t") {
		t.Fatalf("expected control characters to be stripped or escaped, got %q", text)
	}
}

func TestProxy_AccessLog_SanitizesColumns(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "access.log")

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

	data, err := readFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if containsString(text, "WALLET MALICIOUS") || containsString(text, "XMRig 6.21.0") {
		t.Fatalf("expected whitespace in column fields to be encoded, got %q", text)
	}
	if !containsString(text, "WALLET_MALICIOUS_ENTRY") || !containsString(text, "XMRig_6.21.0_BAD") {
		t.Fatalf("expected sanitized column fields to remain readable, got %q", text)
	}
	if countString(text, "\n") != 1 {
		t.Fatalf("expected a single log line, got %q", text)
	}
}

func TestAccessLogSink_SetPath_Good(t *testing.T) {
	dir := t.TempDir()
	first := pathJoin(dir, "first.log")
	second := pathJoin(dir, "second.log")

	sink := newAccessLogSink(first)
	sink.writeConnectLine("10.0.0.1", "WALLET", "XMRig")
	sink.SetPath(second)
	sink.writeConnectLine("10.0.0.2", "WALLET2", "XMRig/2")
	sink.Close()

	firstData, err := readFile(first)
	if err != nil {
		t.Fatalf("read first log: %v", err)
	}
	secondData, err := readFile(second)
	if err != nil {
		t.Fatalf("read second log: %v", err)
	}
	if countString(string(firstData), "CONNECT") != 1 {
		t.Fatalf("expected one line in first log, got %q", string(firstData))
	}
	if countString(string(secondData), "CONNECT") != 1 {
		t.Fatalf("expected one line in second log, got %q", string(secondData))
	}
}

func TestAccessLogSink_SetPath_Bad(t *testing.T) {
	var sink *accessLogSink
	sink.SetPath("ignored")
	if sink != nil {
		t.Fatal("expected nil access log sink to remain nil")
	}
}

func TestAccessLogSink_SetPath_Ugly(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "access.log")

	sink := newAccessLogSink(path)
	sink.writeConnectLine("10.0.0.1", "WALLET", "XMRig")
	sink.SetPath(path)
	sink.writeCloseLine("10.0.0.1", "WALLET", 1, 2)
	sink.Close()

	data, err := readFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	text := string(data)
	if countString(text, "CONNECT") != 1 || countString(text, "CLOSE") != 1 {
		t.Fatalf("expected both lines to be preserved when path is unchanged, got %q", text)
	}
}

func TestAccessLogSink_sanitizeLogColumnField_Good(t *testing.T) {
	if got := sanitizeLogColumnField("10.0.0.1"); got != "10.0.0.1" {
		t.Fatalf("expected plain column field to remain unchanged, got %q", got)
	}
}

func TestAccessLogSink_sanitizeLogColumnField_Bad(t *testing.T) {
	if got := sanitizeLogColumnField(""); got != "" {
		t.Fatalf("expected empty column field to remain empty, got %q", got)
	}
}

func TestAccessLogSink_sanitizeLogColumnField_Ugly(t *testing.T) {
	got := sanitizeLogColumnField("A B\tC\nD\rE\u2028F\u2029G\x07H\"\\I")
	want := "A_B_C_D_E_F_G_H\\\"\\\\I"
	if got != want {
		t.Fatalf("expected sanitized column field %q, got %q", want, got)
	}
}

func TestAccessLogSink_OpenAppendFile_Bad(t *testing.T) {
	// target symbol: OpenAppendFile
	path := pathJoin(t.TempDir(), "access.log")
	if err := mkdir(path, 0o700); err != nil {
		t.Fatalf("create directory path: %v", err)
	}
	sink := newAccessLogSink(path)

	sink.writeConnectLine("10.0.0.1", "WALLET", "XMRig")
	sink.writeCloseLine("10.0.0.1", "WALLET", 1, 2)
	sink.Close()

	if sink.file != nil {
		t.Fatal("expected directory path to remain unopened")
	}
}

func TestAccessLogSink_EmptyPath_Ugly(t *testing.T) {
	// target symbol: EmptyPath
	sink := newAccessLogSink("")

	sink.writeConnectLine("10.0.0.1", "WALLET", "XMRig")
	sink.writeCloseLine("10.0.0.1", "WALLET", 1, 2)
	sink.Close()
}

func TestAccessLogSink_sanitizeLogField_Good(t *testing.T) {
	if got := sanitizeLogField("reason text"); got != "reason text" {
		t.Fatalf("expected plain message to remain unchanged, got %q", got)
	}
}

func TestAccessLogSink_sanitizeLogField_Bad(t *testing.T) {
	if got := sanitizeLogField(""); got != "" {
		t.Fatalf("expected empty message to remain empty, got %q", got)
	}
}

func TestAccessLogSink_sanitizeLogField_Ugly(t *testing.T) {
	got := sanitizeLogField("A\"B\\C\nD\rE\tF\u2028G\u2029H\x07I")
	if !containsString(got, "\\\"") {
		t.Fatalf("expected quotes to be escaped, got %q", got)
	}
	if !containsString(got, "\\\\") {
		t.Fatalf("expected backslashes to be escaped, got %q", got)
	}
	if containsAnyString(got, "\n\r\t") {
		t.Fatalf("expected control whitespace to be normalised, got %q", got)
	}
	if containsString(got, "\u2028") || containsString(got, "\u2029") {
		t.Fatalf("expected unicode line separators to be normalised, got %q", got)
	}
}

func TestAccessLogSink_OnLogin_Bad(t *testing.T) {
	var sink *accessLogSink
	sink.OnLogin(Event{})

	sink = newAccessLogSink("")
	sink.OnLogin(Event{})
}

func TestAccessLogSink_OnClose_Bad(t *testing.T) {
	var sink *accessLogSink
	sink.OnClose(Event{})

	sink = newAccessLogSink("")
	sink.OnClose(Event{})
}

type noopConn struct{}

func (noopConn) Read([]byte) (int, error)         { return 0, core.NewError("closed") }
func (noopConn) Write([]byte) (int, error)        { return 0, core.NewError("closed") }
func (noopConn) Close() error                     { return nil }
func (noopConn) LocalAddr() net.Addr              { return nil }
func (noopConn) RemoteAddr() net.Addr             { return nil }
func (noopConn) SetDeadline(time.Time) error      { return nil }
func (noopConn) SetReadDeadline(time.Time) error  { return nil }
func (noopConn) SetWriteDeadline(time.Time) error { return nil }
