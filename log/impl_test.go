package log

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"dappco.re/go/proxy"
)

// TestAccessLog_OnLogin_Good verifies a CONNECT line is written with the expected columns.
//
//	al := log.NewAccessLog("/tmp/test-access.log")
//	al.OnLogin(proxy.Event{Miner: miner}) // writes "CONNECT  10.0.0.1  WALLET  XMRig/6.21.0"
func TestAccessLog_OnLogin_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	al := NewAccessLog(path)
	defer al.Close()

	miner := newTestMiner(t)
	al.OnLogin(proxy.Event{Miner: miner})
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected log file to exist: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if !strings.Contains(line, "CONNECT") {
		t.Fatalf("expected CONNECT in log line, got %q", line)
	}
}

// TestAccessLog_OnLogin_Bad verifies a nil miner event does not panic or write anything.
//
//	al := log.NewAccessLog("/tmp/test-access.log")
//	al.OnLogin(proxy.Event{Miner: nil}) // no-op
func TestAccessLog_OnLogin_Bad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	al := NewAccessLog(path)
	defer al.Close()

	al.OnLogin(proxy.Event{Miner: nil})
	al.Close()

	if _, err := os.Stat(path); err == nil {
		data, _ := os.ReadFile(path)
		if len(data) > 0 {
			t.Fatalf("expected no output for nil miner, got %q", string(data))
		}
	}
}

// TestAccessLog_OnLogin_Ugly verifies a nil AccessLog does not panic.
//
//	var al *log.AccessLog
//	al.OnLogin(proxy.Event{Miner: miner}) // no-op, no panic
func TestAccessLog_OnLogin_Ugly(t *testing.T) {
	var al *AccessLog
	miner := newTestMiner(t)
	al.OnLogin(proxy.Event{Miner: miner})
}

// TestAccessLog_OnClose_Good verifies a CLOSE line includes rx and tx byte counts.
//
//	al := log.NewAccessLog("/tmp/test-access.log")
//	al.OnClose(proxy.Event{Miner: miner}) // writes "CLOSE  <ip>  <user>  rx=0  tx=0"
func TestAccessLog_OnClose_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	al := NewAccessLog(path)
	defer al.Close()

	miner := newTestMiner(t)
	al.OnClose(proxy.Event{Miner: miner})
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected log file to exist: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if !strings.Contains(line, "CLOSE") {
		t.Fatalf("expected CLOSE in log line, got %q", line)
	}
	if !strings.Contains(line, "rx=") {
		t.Fatalf("expected rx= in log line, got %q", line)
	}
	if !strings.Contains(line, "tx=") {
		t.Fatalf("expected tx= in log line, got %q", line)
	}
}

// TestAccessLog_OnClose_Bad verifies a nil miner close event produces no output.
//
//	al := log.NewAccessLog("/tmp/test-access.log")
//	al.OnClose(proxy.Event{Miner: nil}) // no-op
func TestAccessLog_OnClose_Bad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	al := NewAccessLog(path)
	defer al.Close()

	al.OnClose(proxy.Event{Miner: nil})
	al.Close()

	if _, err := os.Stat(path); err == nil {
		data, _ := os.ReadFile(path)
		if len(data) > 0 {
			t.Fatalf("expected no output for nil miner, got %q", string(data))
		}
	}
}

// TestAccessLog_OnClose_Ugly verifies close on an empty-path log is a no-op.
//
//	al := log.NewAccessLog("")
//	al.OnClose(proxy.Event{Miner: miner}) // no-op, empty path
func TestAccessLog_OnClose_Ugly(t *testing.T) {
	al := NewAccessLog("")
	defer al.Close()

	miner := newTestMiner(t)
	al.OnClose(proxy.Event{Miner: miner})
}

// TestShareLog_OnAccept_Good verifies an ACCEPT line is written with diff and latency.
//
//	sl := log.NewShareLog("/tmp/test-shares.log")
//	sl.OnAccept(proxy.Event{Miner: miner, Diff: 100000, Latency: 82})
func TestShareLog_OnAccept_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sl := NewShareLog(path)
	defer sl.Close()

	miner := newTestMiner(t)
	sl.OnAccept(proxy.Event{Miner: miner, Diff: 100000, Latency: 82})
	sl.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected log file to exist: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if !strings.Contains(line, "ACCEPT") {
		t.Fatalf("expected ACCEPT in log line, got %q", line)
	}
	if !strings.Contains(line, "diff=100000") {
		t.Fatalf("expected diff=100000 in log line, got %q", line)
	}
	if !strings.Contains(line, "latency=82ms") {
		t.Fatalf("expected latency=82ms in log line, got %q", line)
	}
}

// TestShareLog_OnAccept_Bad verifies a nil miner accept event produces no output.
//
//	sl := log.NewShareLog("/tmp/test-shares.log")
//	sl.OnAccept(proxy.Event{Miner: nil}) // no-op
func TestShareLog_OnAccept_Bad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sl := NewShareLog(path)
	defer sl.Close()

	sl.OnAccept(proxy.Event{Miner: nil, Diff: 100000})
	sl.Close()

	if _, err := os.Stat(path); err == nil {
		data, _ := os.ReadFile(path)
		if len(data) > 0 {
			t.Fatalf("expected no output for nil miner, got %q", string(data))
		}
	}
}

// TestShareLog_OnAccept_Ugly verifies a nil ShareLog does not panic.
//
//	var sl *log.ShareLog
//	sl.OnAccept(proxy.Event{Miner: miner}) // no-op, no panic
func TestShareLog_OnAccept_Ugly(t *testing.T) {
	var sl *ShareLog
	miner := newTestMiner(t)
	sl.OnAccept(proxy.Event{Miner: miner, Diff: 100000})
}

// TestShareLog_OnReject_Good verifies a REJECT line is written with the rejection reason.
//
//	sl := log.NewShareLog("/tmp/test-shares.log")
//	sl.OnReject(proxy.Event{Miner: miner, Error: "Low difficulty share"})
func TestShareLog_OnReject_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sl := NewShareLog(path)
	defer sl.Close()

	miner := newTestMiner(t)
	sl.OnReject(proxy.Event{Miner: miner, Error: "Low difficulty share"})
	sl.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected log file to exist: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if !strings.Contains(line, "REJECT") {
		t.Fatalf("expected REJECT in log line, got %q", line)
	}
	if !strings.Contains(line, "Low difficulty share") {
		t.Fatalf("expected rejection reason in log line, got %q", line)
	}
}

// TestShareLog_OnReject_Bad verifies a nil miner reject event produces no output.
//
//	sl := log.NewShareLog("/tmp/test-shares.log")
//	sl.OnReject(proxy.Event{Miner: nil}) // no-op
func TestShareLog_OnReject_Bad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sl := NewShareLog(path)
	defer sl.Close()

	sl.OnReject(proxy.Event{Miner: nil, Error: "Low difficulty share"})
	sl.Close()

	if _, err := os.Stat(path); err == nil {
		data, _ := os.ReadFile(path)
		if len(data) > 0 {
			t.Fatalf("expected no output for nil miner, got %q", string(data))
		}
	}
}

// TestShareLog_OnReject_Ugly verifies an empty-path ShareLog silently discards the reject line.
//
//	sl := log.NewShareLog("")
//	sl.OnReject(proxy.Event{Miner: miner, Error: "reason"}) // no-op, empty path
func TestShareLog_OnReject_Ugly(t *testing.T) {
	sl := NewShareLog("")
	defer sl.Close()

	miner := newTestMiner(t)
	sl.OnReject(proxy.Event{Miner: miner, Error: "reason"})
}

// TestImpl_sanitizeLogField_Good verifies a clean message is preserved verbatim.
func TestImpl_sanitizeLogField_Good(t *testing.T) {
	if got := sanitizeLogField("Low difficulty share"); got != "Low difficulty share" {
		t.Fatalf("expected plain message to remain unchanged, got %q", got)
	}
}

// TestImpl_sanitizeLogField_Bad verifies an empty message stays empty.
func TestImpl_sanitizeLogField_Bad(t *testing.T) {
	if got := sanitizeLogField(""); got != "" {
		t.Fatalf("expected empty message to remain empty, got %q", got)
	}
}

// TestImpl_sanitizeLogField_Ugly verifies control characters are normalised and quoting is escaped.
func TestImpl_sanitizeLogField_Ugly(t *testing.T) {
	got := sanitizeLogField("bad\"\nreason\r\t\u2028\u2029\\")
	if !strings.Contains(got, "\\\"") {
		t.Fatalf("expected quotes to be escaped, got %q", got)
	}
	if !strings.Contains(got, "\\\\") {
		t.Fatalf("expected backslashes to be escaped, got %q", got)
	}
	if strings.ContainsAny(got, "\n\r\t") {
		t.Fatalf("expected control whitespace to be normalised, got %q", got)
	}
	if strings.Contains(got, "\u2028") || strings.Contains(got, "\u2029") {
		t.Fatalf("expected unicode line separators to be normalised, got %q", got)
	}
}

// TestImpl_sanitizeLogColumnField_Good verifies a clean column value is preserved verbatim.
func TestImpl_sanitizeLogColumnField_Good(t *testing.T) {
	if got := sanitizeLogColumnField("WALLET"); got != "WALLET" {
		t.Fatalf("expected plain column field to remain unchanged, got %q", got)
	}
}

// TestImpl_sanitizeLogColumnField_Bad verifies an empty column value stays empty.
func TestImpl_sanitizeLogColumnField_Bad(t *testing.T) {
	if got := sanitizeLogColumnField(""); got != "" {
		t.Fatalf("expected empty column field to remain empty, got %q", got)
	}
}

// TestImpl_sanitizeLogColumnField_Ugly verifies whitespace and control characters are encoded.
func TestImpl_sanitizeLogColumnField_Ugly(t *testing.T) {
	got := sanitizeLogColumnField("WALLET MALICIOUS\nENTRY\u2028TAB\t\"\\")
	want := "WALLET_MALICIOUS_ENTRY_TAB_\\\"\\\\"
	if got != want {
		t.Fatalf("expected sanitized column field %q, got %q", want, got)
	}
}

// TestShareLog_OnAccept_ColumnsSanitized verifies whitespace in the user
// column is encoded so a miner cannot inject malformed log columns.
func TestShareLog_OnAccept_ColumnsSanitized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sl := NewShareLog(path)
	defer sl.Close()

	miner := newTestMiner(t)
	setMinerStringField(t, miner, "user", "WALLET MALICIOUS\nENTRY")
	sl.OnAccept(proxy.Event{Miner: miner, Diff: 1000, Latency: 1})
	sl.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected log file to exist: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if strings.Contains(line, "WALLET MALICIOUS") {
		t.Fatalf("expected whitespace in user column to be encoded, got %q", line)
	}
	if !strings.Contains(line, "WALLET_MALICIOUS_ENTRY") {
		t.Fatalf("expected sanitized user column, got %q", line)
	}
}

// TestAccessLog_OnLogin_ColumnsSanitized verifies whitespace in user-controlled
// columns is encoded so a miner cannot inject malformed log columns.
func TestAccessLog_OnLogin_ColumnsSanitized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	al := NewAccessLog(path)
	defer al.Close()

	miner := newTestMiner(t)
	setMinerStringField(t, miner, "user", "WALLET MALICIOUS\nENTRY")
	setMinerStringField(t, miner, "agent", "XMRig 6.21.0\u2028BAD")
	al.OnLogin(proxy.Event{Miner: miner})
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected log file to exist: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if strings.Contains(line, "WALLET MALICIOUS") || strings.Contains(line, "XMRig 6.21.0") {
		t.Fatalf("expected whitespace in columns to be encoded, got %q", line)
	}
	if !strings.Contains(line, "WALLET_MALICIOUS_ENTRY") || !strings.Contains(line, "XMRig_6.21.0_BAD") {
		t.Fatalf("expected sanitized column values, got %q", line)
	}
}

func setMinerStringField(t *testing.T, miner *proxy.Miner, field, value string) {
	t.Helper()
	target := reflect.ValueOf(miner).Elem().FieldByName(field)
	if !target.IsValid() {
		t.Fatalf("expected miner field %q to exist", field)
	}
	reflect.NewAt(target.Type(), unsafe.Pointer(target.UnsafeAddr())).Elem().SetString(value)
}

// TestAccessLog_Close_Good verifies Close releases the file handle and is safe to call twice.
//
//	al := log.NewAccessLog("/tmp/test-access.log")
//	al.OnLogin(proxy.Event{Miner: miner})
//	al.Close()
//	al.Close() // double close is safe
func TestAccessLog_Close_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	al := NewAccessLog(path)

	miner := newTestMiner(t)
	al.OnLogin(proxy.Event{Miner: miner})
	al.Close()
	al.Close()
}

// TestAccessLog_Close_Bad verifies Close on a nil AccessLog does not panic.
//
//	var al *log.AccessLog
//	al.Close() // no-op, no panic
func TestAccessLog_Close_Bad(t *testing.T) {
	var al *AccessLog
	al.Close()
}

// TestAccessLog_Close_Ugly verifies Close on a never-opened log does not panic.
//
//	al := log.NewAccessLog("/nonexistent/dir/access.log")
//	al.Close() // no file was ever opened
func TestAccessLog_Close_Ugly(t *testing.T) {
	al := NewAccessLog("/nonexistent/dir/access.log")
	al.Close()
}

// TestShareLog_Close_Good verifies Close releases the file handle and is safe to call twice.
//
//	sl := log.NewShareLog("/tmp/test-shares.log")
//	sl.OnAccept(proxy.Event{Miner: miner, Diff: 1000})
//	sl.Close()
//	sl.Close() // double close is safe
func TestShareLog_Close_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sl := NewShareLog(path)

	miner := newTestMiner(t)
	sl.OnAccept(proxy.Event{Miner: miner, Diff: 1000})
	sl.Close()
	sl.Close()
}

// TestShareLog_Close_Bad verifies Close on a nil ShareLog does not panic.
//
//	var sl *log.ShareLog
//	sl.Close() // no-op, no panic
func TestShareLog_Close_Bad(t *testing.T) {
	var sl *ShareLog
	sl.Close()
}

// TestShareLog_Close_Ugly verifies Close on a never-opened log does not panic.
//
//	sl := log.NewShareLog("/nonexistent/dir/shares.log")
//	sl.Close() // no file was ever opened
func TestShareLog_Close_Ugly(t *testing.T) {
	sl := NewShareLog("/nonexistent/dir/shares.log")
	sl.Close()
}

// newTestMiner creates a minimal miner for log testing using a net.Pipe connection.
func newTestMiner(t *testing.T) *proxy.Miner {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	miner := proxy.NewMiner(client, 3333, nil)
	miner.SetID(1)
	return miner
}

// pipeAddr satisfies the net.Addr interface for pipe-based test connections.
type pipeAddr struct{}

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return "pipe" }

// pipeConn wraps an os.Pipe as a net.Conn for tests that need a closeable socket.
type pipeConn struct {
	*os.File
}

func (p *pipeConn) RemoteAddr() net.Addr               { return pipeAddr{} }
func (p *pipeConn) LocalAddr() net.Addr                { return pipeAddr{} }
func (p *pipeConn) SetDeadline(_ time.Time) error      { return nil }
func (p *pipeConn) SetReadDeadline(_ time.Time) error  { return nil }
func (p *pipeConn) SetWriteDeadline(_ time.Time) error { return nil }
