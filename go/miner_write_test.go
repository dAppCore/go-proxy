package proxy

import (
	"net"
	"testing"
)

// TestMiner_writeJSON_NilConn_Good verifies writing when the transport has
// already been torn down is a no-op success — a late reply on a closed miner
// must not error or panic.
func TestMiner_writeJSON_NilConn_Good(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.mu.Lock()
	miner.conn = nil
	miner.mu.Unlock()

	if r := miner.writeJSON(map[string]any{"id": 1}); !r.OK {
		t.Fatalf("expected nil-conn write to be a no-op success, got %v", r.Value)
	}
}

// TestMiner_writeJSON_LargePayload_Ugly verifies a payload larger than the
// per-miner send buffer takes the direct-write branch rather than the buffered
// copy, and still lands on the wire intact.
func TestMiner_writeJSON_LargePayload_Ugly(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)

	// A value well over the 16 KiB per-miner buffer forces the direct path.
	big := make([]byte, 32*1024)
	for i := range big {
		big[i] = 'a'
	}
	payload := map[string]any{"id": 1, "blob": string(big)}

	read := make(chan int, 1)
	go func() {
		buf := make([]byte, 64*1024)
		total := 0
		for total < 32*1024 {
			n, err := clientConn.Read(buf)
			total += n
			if err != nil {
				break
			}
		}
		read <- total
	}()

	if r := miner.writeJSON(payload); !r.OK {
		t.Fatalf("expected large-payload write to succeed, got %v", r.Value)
	}
	if got := <-read; got < 32*1024 {
		t.Fatalf("expected the full large payload on the wire, got %d bytes", got)
	}
}

// TestMiner_writeJSON_WriteError_Bad verifies that a write to a closed
// connection fails closed: writeJSON returns a failed Result and tears the
// miner down.
func TestMiner_writeJSON_WriteError_Bad(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	_ = clientConn.Close()
	defer serverConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	// Closing the peer makes the next pipe write fail.
	_ = serverConn.Close()

	r := miner.writeJSON(map[string]any{"id": 1, "method": "x"})
	if r.OK {
		t.Fatal("expected write to a closed connection to fail")
	}
	if miner.State() != MinerStateClosing {
		t.Fatalf("expected failed write to close the miner, got state %v", miner.State())
	}
}

// TestMiner_closeTransport_NilConn verifies tearing down a miner whose
// transport is already nil is a safe no-op.
func TestMiner_closeTransport_NilConn(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.mu.Lock()
	miner.conn = nil
	miner.mu.Unlock()

	miner.closeTransport() // must not panic
}

// TestMiner_ReplyWithError_NilConn_NoError verifies ReplyWithError on a torn
// down miner is a silent no-op rather than a panic.
func TestMiner_ReplyWithError_NilConn_NoError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.mu.Lock()
	miner.conn = nil
	miner.mu.Unlock()

	miner.ReplyWithError(7, "stale reply") // must not panic
}
