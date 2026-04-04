package pool

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestStratumClient_Keepalive_Good(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	client := &StratumClient{
		conn:      clientConn,
		active:    true,
		sessionID: "session-1",
	}

	done := make(chan struct{})
	go func() {
		client.Keepalive()
		close(done)
	}()

	line, err := bufio.NewReader(serverConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read keepalive request: %v", err)
	}
	<-done

	var payload map[string]any
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal keepalive request: %v", err)
	}
	if got := payload["method"]; got != "keepalived" {
		t.Fatalf("expected keepalived method, got %#v", got)
	}
	params, ok := payload["params"].(map[string]any)
	if !ok {
		t.Fatalf("expected params object, got %#v", payload["params"])
	}
	if got := params["id"]; got != "session-1" {
		t.Fatalf("expected session id in keepalive payload, got %#v", got)
	}
}

func TestStratumClient_Keepalive_Bad(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	client := &StratumClient{
		conn:   clientConn,
		active: false,
	}

	client.Keepalive()

	if err := serverConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := serverConn.Read(buf); err == nil {
		t.Fatalf("expected no keepalive data while inactive")
	}
}

func TestStratumClient_Keepalive_Ugly(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	client := &StratumClient{
		conn:      clientConn,
		active:    true,
		sessionID: "session-2",
	}

	reader := bufio.NewReader(serverConn)
	done := make(chan struct{})
	go func() {
		client.Keepalive()
		client.Keepalive()
		close(done)
	}()

	first, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read first keepalive request: %v", err)
	}
	second, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read second keepalive request: %v", err)
	}
	<-done

	var firstPayload map[string]any
	if err := json.Unmarshal(first, &firstPayload); err != nil {
		t.Fatalf("unmarshal first keepalive request: %v", err)
	}
	var secondPayload map[string]any
	if err := json.Unmarshal(second, &secondPayload); err != nil {
		t.Fatalf("unmarshal second keepalive request: %v", err)
	}

	if firstPayload["id"] == secondPayload["id"] {
		t.Fatalf("expected keepalive request ids to be unique")
	}
}
