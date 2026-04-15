package proxy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProxy_ShareLog_WritesOutcomeLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shares.log")

	cfg := &Config{
		Mode:         "nicehash",
		Workers:      WorkersByRigID,
		ShareLogFile: path,
		Bind:         []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:        []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{
		user:  "WALLET",
		conn:  noopConn{},
		state: MinerStateReady,
	}
	p.events.Dispatch(Event{Type: EventAccept, Miner: miner, Diff: 1234, Latency: 56})
	p.events.Dispatch(Event{Type: EventReject, Miner: miner, Error: "Invalid nonce"})
	p.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read share log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "ACCEPT  WALLET  diff=1234  latency=56ms") {
		t.Fatalf("expected ACCEPT line, got %q", text)
	}
	if !strings.Contains(text, "REJECT  WALLET  reason=\"Invalid nonce\"") {
		t.Fatalf("expected REJECT line, got %q", text)
	}
}

func TestProxy_ShareLog_SanitizesReason(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shares.log")

	cfg := &Config{
		Mode:         "nicehash",
		Workers:      WorkersByRigID,
		ShareLogFile: path,
		Bind:         []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:        []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{
		user:  "WALLET\nINJECT",
		conn:  noopConn{},
		state: MinerStateReady,
	}
	p.events.Dispatch(Event{Type: EventReject, Miner: miner, Error: "bad\"\nreason"})
	p.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read share log: %v", err)
	}
	text := string(data)
	if strings.Count(text, "\n") != 1 {
		t.Fatalf("expected a single log line, got %q", text)
	}
	if strings.Contains(text, "bad\"\nreason") || strings.Contains(text, "\nINJECT") {
		t.Fatalf("expected log fields to be sanitized, got %q", text)
	}
}

func TestProxy_ShareLog_SanitizesColumns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shares.log")

	cfg := &Config{
		Mode:         "nicehash",
		Workers:      WorkersByRigID,
		ShareLogFile: path,
		Bind:         []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:        []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{
		user:  "WALLET MALICIOUS\nENTRY",
		conn:  noopConn{},
		state: MinerStateReady,
	}
	p.events.Dispatch(Event{Type: EventAccept, Miner: miner, Diff: 1234, Latency: 56})
	p.events.Dispatch(Event{Type: EventReject, Miner: miner, Error: "reason"})
	p.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read share log: %v", err)
	}
	text := string(data)
	if strings.Contains(text, "WALLET MALICIOUS") {
		t.Fatalf("expected whitespace in user column to be encoded, got %q", text)
	}
	if !strings.Contains(text, "WALLET_MALICIOUS_ENTRY") {
		t.Fatalf("expected sanitized user column to remain readable, got %q", text)
	}
	if strings.Count(text, "\n") != 2 {
		t.Fatalf("expected two log lines, got %q", text)
	}
}

func TestShareLogSink_SetPath_Good(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.log")
	second := filepath.Join(dir, "second.log")

	sink := newShareLogSink(first)
	sink.writeLine("ACCEPT", "WALLET", 10, 2, "")
	sink.SetPath(second)
	sink.writeLine("REJECT", "WALLET2", 0, 0, "Invalid job id")
	sink.Close()

	firstData, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read first log: %v", err)
	}
	secondData, err := os.ReadFile(second)
	if err != nil {
		t.Fatalf("read second log: %v", err)
	}
	if strings.Count(string(firstData), "ACCEPT") != 1 {
		t.Fatalf("expected one ACCEPT line in first log, got %q", string(firstData))
	}
	if strings.Count(string(secondData), "REJECT") != 1 {
		t.Fatalf("expected one REJECT line in second log, got %q", string(secondData))
	}
}

func TestShareLogSink_SetPath_Bad(t *testing.T) {
	var sink *shareLogSink
	sink.SetPath("ignored")
}

func TestShareLogSink_SetPath_Ugly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shares.log")

	sink := newShareLogSink(path)
	sink.writeLine("ACCEPT", "WALLET", 10, 2, "")
	sink.SetPath(path)
	sink.writeLine("REJECT", "WALLET", 0, 0, "Invalid nonce")
	sink.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read share log: %v", err)
	}
	text := string(data)
	if strings.Count(text, "ACCEPT") != 1 || strings.Count(text, "REJECT") != 1 {
		t.Fatalf("expected both lines to be preserved when path is unchanged, got %q", text)
	}
}

func TestShareLogSink_sanitizeLogColumnField_Good(t *testing.T) {
	if got := sanitizeLogColumnField("WALLET"); got != "WALLET" {
		t.Fatalf("expected plain user field to remain unchanged, got %q", got)
	}
}

func TestShareLogSink_sanitizeLogColumnField_Bad(t *testing.T) {
	if got := sanitizeLogColumnField(""); got != "" {
		t.Fatalf("expected empty user field to remain empty, got %q", got)
	}
}

func TestShareLogSink_sanitizeLogColumnField_Ugly(t *testing.T) {
	got := sanitizeLogColumnField("WALLET MALICIOUS\nENTRY\u2028TAB\t\"\\")
	want := "WALLET_MALICIOUS_ENTRY_TAB_\\\"\\\\"
	if got != want {
		t.Fatalf("expected sanitized column field %q, got %q", want, got)
	}
}

func TestShareLogSink_sanitizeLogField_Good(t *testing.T) {
	if got := sanitizeLogField("Low difficulty share"); got != "Low difficulty share" {
		t.Fatalf("expected plain reason to remain unchanged, got %q", got)
	}
}

func TestShareLogSink_sanitizeLogField_Bad(t *testing.T) {
	if got := sanitizeLogField(""); got != "" {
		t.Fatalf("expected empty reason to remain empty, got %q", got)
	}
}

func TestShareLogSink_sanitizeLogField_Ugly(t *testing.T) {
	got := sanitizeLogField("bad\"\nreason\r\u2028\u2029\\")
	if !strings.Contains(got, "\\\"") {
		t.Fatalf("expected quotes to be escaped, got %q", got)
	}
	if !strings.Contains(got, "\\\\") {
		t.Fatalf("expected backslashes to be escaped, got %q", got)
	}
	if strings.ContainsAny(got, "\n\r") {
		t.Fatalf("expected newlines to be normalised, got %q", got)
	}
	if strings.Contains(got, "\u2028") || strings.Contains(got, "\u2029") {
		t.Fatalf("expected unicode line separators to be normalised, got %q", got)
	}
}

func TestShareLogSink_OnAccept_Bad(t *testing.T) {
	var sink *shareLogSink
	sink.OnAccept(Event{})

	sink = newShareLogSink("")
	sink.OnAccept(Event{})
}

func TestShareLogSink_OnReject_Bad(t *testing.T) {
	var sink *shareLogSink
	sink.OnReject(Event{})

	sink = newShareLogSink("")
	sink.OnReject(Event{})
}
