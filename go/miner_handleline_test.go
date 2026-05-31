package proxy

import (
	"net"
	"testing"
)

// TestMiner_handleLine_Good verifies a well-formed keepalived line is routed
// and the dispatcher reports the connection should stay open (returns true).
func TestMiner_handleLine_Good(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.state = MinerStateReady
	miner.rpcID = "session-1"

	// Drain the keepalived reply the handler writes so the pipe doesn't block.
	go func() {
		buf := make([]byte, 256)
		_, _ = clientConn.Read(buf)
	}()

	if !miner.handleLine([]byte(`{"id":1,"method":"keepalived","params":{"id":"session-1"}}`)) {
		t.Fatal("expected well-formed keepalived line to keep the connection open")
	}
}

// TestMiner_handleLine_Bad verifies malformed JSON causes the dispatcher to
// signal the connection should close (returns false). Networking code must
// fail closed on garbage input rather than continue parsing.
func TestMiner_handleLine_Bad(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)

	if miner.handleLine([]byte(`{"id":1,"method":`)) {
		t.Fatal("expected malformed JSON to close the connection")
	}
	if miner.handleLine([]byte(`not json at all`)) {
		t.Fatal("expected non-JSON garbage to close the connection")
	}
}

// TestMiner_handleLine_Ugly verifies an unknown method is tolerated — the
// dispatcher keeps the connection open (returns true) without acting on it.
func TestMiner_handleLine_Ugly(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.state = MinerStateReady

	if !miner.handleLine([]byte(`{"id":1,"method":"unknown_method","params":{}}`)) {
		t.Fatal("expected unknown method to be tolerated, keeping the connection open")
	}
	// An empty method is equally tolerated — no panic, connection stays open.
	if !miner.handleLine([]byte(`{"id":2}`)) {
		t.Fatal("expected missing method to be tolerated")
	}
}

// TestNoopSplitter_Methods verifies the no-op splitter used in non-splitter
// modes accepts every lifecycle call without side effects and returns a zero
// UpstreamStats.
func TestNoopSplitter_Methods(t *testing.T) {
	var s noopSplitter
	// None of these may panic.
	s.Connect()
	s.OnLogin(&LoginEvent{})
	s.OnSubmit(&SubmitEvent{})
	s.OnClose(&CloseEvent{})
	s.Tick(1)
	s.GC()
	if got := s.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected zero UpstreamStats, got %+v", got)
	}
}
