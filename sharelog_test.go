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
