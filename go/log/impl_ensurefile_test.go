package proxylog

import (
	"testing"

	"dappco.re/go/proxy"
)

// TestImpl_AccessLog_OpenFailure verifies that when the configured access-log
// path cannot be opened for append (here a directory), every write path takes
// the ensureFile failure branch and silently discards the line rather than
// panicking. Logging must never take down the proxy.
func TestImpl_AccessLog_OpenFailure(t *testing.T) {
	// A directory path cannot be opened for append, so ensureFile fails.
	dir := t.TempDir()
	al := NewAccessLog(dir)
	defer al.Close()

	miner := newTestMiner(t)
	// Both write paths must tolerate the open failure.
	al.OnLogin(proxy.Event{Miner: miner})
	al.OnClose(proxy.Event{Miner: miner})

	if al.file != nil {
		t.Fatal("expected file to remain nil after open failure")
	}
}

// TestImpl_ShareLog_OpenFailure verifies the share-log write paths likewise
// tolerate an un-openable path.
func TestImpl_ShareLog_OpenFailure(t *testing.T) {
	dir := t.TempDir()
	sl := NewShareLog(dir)
	defer sl.Close()

	miner := newTestMiner(t)
	sl.OnAccept(proxy.Event{Miner: miner, Diff: 1000, Latency: 5})
	sl.OnReject(proxy.Event{Miner: miner, Error: "bad share"})

	if sl.file != nil {
		t.Fatal("expected file to remain nil after open failure")
	}
}
