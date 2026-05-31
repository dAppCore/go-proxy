package proxy

import (
	"net"
	"testing"
)

// TestMiner_handleSubmit_MapParams verifies submit params supplied as a decoded
// map[string]any (rather than raw JSON bytes) are read field-by-field and a
// valid share is forwarded to the submit callback.
func TestMiner_handleSubmit_MapParams(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.state = MinerStateReady
	miner.rpcID = "session-1"

	forwarded := make(chan *SubmitEvent, 1)
	miner.onSubmit = func(_ *Miner, event *SubmitEvent) {
		forwarded <- event
	}

	miner.handleSubmit(stratumRequest{
		ID:     7,
		Method: "submit",
		Params: map[string]any{
			"id":     "session-1",
			"job_id": "job-1",
			"nonce":  "deadbeef",
			"result": "hash",
			"algo":   "rx/0",
		},
	})

	select {
	case event := <-forwarded:
		if event.JobID != "job-1" || event.Nonce != "deadbeef" || event.Algo != "rx/0" {
			t.Fatalf("unexpected forwarded submit: %+v", event)
		}
	default:
		t.Fatal("expected map-params submit to be forwarded to the callback")
	}
}

// TestMiner_handleSubmit_NotReady verifies a submit received before the miner
// has authenticated (state != Ready) is rejected as Unauthenticated.
func TestMiner_handleSubmit_NotReady(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	// Left in the default MinerStateWaitLogin state — not yet authenticated.

	called := false
	miner.onSubmit = func(_ *Miner, _ *SubmitEvent) { called = true }

	// Drain the rejection reply so the handler's write does not block.
	go func() {
		buf := make([]byte, 256)
		_, _ = clientConn.Read(buf)
	}()

	miner.handleSubmit(stratumRequest{
		ID:     7,
		Method: "submit",
		Params: map[string]any{"id": "session-1", "job_id": "job-1", "nonce": "deadbeef"},
	})

	if called {
		t.Fatal("expected a pre-auth submit not to reach the submit callback")
	}
}
