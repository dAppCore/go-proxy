package proxy

import (
	"bufio"
	"net"
	"testing"
)

func TestMiner_handleSubmit_Good(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.state = MinerStateReady
	miner.rpcID = "session-1"
	miner.onSubmit = func(m *Miner, event *SubmitEvent) {
		if event.RequestID != 7 || event.JobID != "job-1" || event.Nonce != "deadbeef" || event.Algo != "cn/r" {
			t.Fatalf("unexpected submit event: %+v", event)
		}
	}

	params, err := testJSONMarshal(map[string]any{
		"id":     "session-1",
		"job_id": "job-1",
		"nonce":  "deadbeef",
		"result": "hash",
		"algo":   "cn/r",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	miner.handleSubmit(stratumRequest{ID: 7, Method: "submit", Params: params})

	if miner.lastActivityAt.IsZero() {
		t.Fatal("expected submit to update activity timestamp")
	}
}

func TestMiner_handleSubmit_Bad(t *testing.T) {
	t.Run("unauthenticated", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		miner := NewMiner(serverConn, 3333, nil)
		miner.state = MinerStateReady
		miner.rpcID = "session-1"

		params, err := testJSONMarshal(map[string]any{
			"id":     "wrong-session",
			"job_id": "job-1",
			"nonce":  "deadbeef",
			"result": "hash",
		})
		if err != nil {
			t.Fatalf("marshal params: %v", err)
		}

		done := make(chan struct{})
		go func() {
			miner.handleSubmit(stratumRequest{ID: 8, Method: "submit", Params: params})
			close(done)
		}()

		line, err := bufio.NewReader(clientConn).ReadBytes('\n')
		if err != nil {
			t.Fatalf("read submit error: %v", err)
		}
		<-done

		var payload struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := testJSONUnmarshal(line, &payload); err != nil {
			t.Fatalf("decode submit error: %v", err)
		}
		if payload.Error.Message != "Unauthenticated" {
			t.Fatalf("expected unauthenticated submit to be rejected, got %q", payload.Error.Message)
		}
	})

	t.Run("missing_job_id", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		miner := NewMiner(serverConn, 3333, nil)
		miner.state = MinerStateReady
		miner.rpcID = "session-1"

		params, err := testJSONMarshal(map[string]any{
			"id":     "session-1",
			"job_id": "",
			"nonce":  "deadbeef",
			"result": "hash",
		})
		if err != nil {
			t.Fatalf("marshal params: %v", err)
		}

		done := make(chan struct{})
		go func() {
			miner.handleSubmit(stratumRequest{ID: 81, Method: "submit", Params: params})
			close(done)
		}()

		line, err := bufio.NewReader(clientConn).ReadBytes('\n')
		if err != nil {
			t.Fatalf("read submit error: %v", err)
		}
		<-done

		var payload struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := testJSONUnmarshal(line, &payload); err != nil {
			t.Fatalf("decode submit error: %v", err)
		}
		if payload.Error.Message != "Missing job id" {
			t.Fatalf("expected missing job id to be rejected, got %q", payload.Error.Message)
		}
	})
}

func TestMiner_handleSubmit_Ugly(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.state = MinerStateReady
	miner.rpcID = "session-1"

	params, err := testJSONMarshal(map[string]any{
		"id":     "session-1",
		"job_id": "job-1",
		"nonce":  "DEADBEEF",
		"result": "hash",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	done := make(chan struct{})
	go func() {
		miner.handleSubmit(stratumRequest{ID: 9, Method: "submit", Params: params})
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read submit error: %v", err)
	}
	<-done

	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := testJSONUnmarshal(line, &payload); err != nil {
		t.Fatalf("decode submit error: %v", err)
	}
	if payload.Error.Message != "Invalid nonce" {
		t.Fatalf("expected invalid nonce rejection, got %q", payload.Error.Message)
	}
}
