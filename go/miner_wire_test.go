package proxy

import (
	"bufio"
	"net"
	"testing"
)

func TestMiner_Success_WritesNullError_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := NewMiner(minerConn, 3333, nil)
	done := make(chan struct{})
	go func() {
		miner.Success(7, "OK")
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read success response: %v", err)
	}
	<-done

	var payload struct {
		Error  testRawMessage `json:"error"`
		Result struct {
			Status string `json:"status"`
		} `json:"result"`
	}
	if err := testJSONUnmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal success response: %v", err)
	}
	if string(payload.Error) != "null" {
		t.Fatalf("expected success response error to be null, got %s", string(payload.Error))
	}
	if payload.Result.Status != "OK" {
		t.Fatalf("expected success status OK, got %q", payload.Result.Status)
	}
}
