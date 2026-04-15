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
