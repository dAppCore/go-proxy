package pool

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

type clientListenerSpy struct {
	mu          sync.Mutex
	jobs        []proxy.Job
	results     []resultEvent
	disconnects int
}

type resultEvent struct {
	seq      int64
	accepted bool
	message  string
}

func (s *clientListenerSpy) OnJob(job proxy.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, job)
}

func (s *clientListenerSpy) OnResultAccepted(sequence int64, accepted bool, errorMessage string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, resultEvent{seq: sequence, accepted: accepted, message: errorMessage})
}

func (s *clientListenerSpy) OnDisconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disconnects++
}

func startStratumServer(t *testing.T, handler func(net.Conn, *bufio.Reader)) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn, bufio.NewReader(conn))
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func readLine(t *testing.T, reader *bufio.Reader) []byte {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return line
}

func TestStratumClient_Connect_Good(t *testing.T) {
	addr, shutdown := startStratumServer(t, func(conn net.Conn, reader *bufio.Reader) {
		loginLine := readLine(t, reader)
		var loginReq struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(loginLine, &loginReq); err != nil {
			t.Fatalf("decode login request: %v", err)
		}
		if loginReq.Method != "login" {
			t.Fatalf("expected login method, got %q", loginReq.Method)
		}

		_, _ = io.WriteString(conn, `{"id":"session-1","result":{"id":"session-1","job":{"blob":"`+strings.Repeat("0", 160)+`","job_id":"job-1","target":"b88d0600","algo":"cn/r","height":7,"seed_hash":"seed","id":"session-1"}}}`+"\n")

		submitLine := readLine(t, reader)
		var submitReq struct {
			Method string `json:"method"`
			Params struct {
				ID     string `json:"id"`
				JobID  string `json:"job_id"`
				Nonce  string `json:"nonce"`
				Result string `json:"result"`
				Algo   string `json:"algo"`
			} `json:"params"`
		}
		if err := json.Unmarshal(submitLine, &submitReq); err != nil {
			t.Fatalf("decode submit request: %v", err)
		}
		if submitReq.Method != "submit" {
			t.Fatalf("expected submit method, got %q", submitReq.Method)
		}
		if submitReq.Params.ID != "session-1" {
			t.Fatalf("expected submit session id to be propagated, got %q", submitReq.Params.ID)
		}
		if submitReq.Params.JobID != "job-1" || submitReq.Params.Nonce != "deadbeef" {
			t.Fatalf("unexpected submit params: %+v", submitReq.Params)
		}

		_, _ = io.WriteString(conn, `{"id":2,"result":{"status":"OK"}}`+"\n")
		time.Sleep(25 * time.Millisecond)
	})
	defer shutdown()

	spy := &clientListenerSpy{}
	client := NewStratumClient(proxy.PoolConfig{
		URL:   addr,
		User:  "WALLET",
		Pass:  "x",
		RigID: "rig-1",
		Algo:  "cn/r",
	}, spy)

	if result := client.Connect(); !result.OK {
		t.Fatalf("expected connect to succeed, got %v", result.Error)
	}
	client.Login()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotJobs := len(spy.jobs)
		spy.mu.Unlock()
		if gotJobs > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	spy.mu.Lock()
	if len(spy.jobs) != 1 {
		spy.mu.Unlock()
		t.Fatalf("expected one job notification, got %+v", spy.jobs)
	}
	spy.mu.Unlock()
	if !client.IsActive() {
		t.Fatal("expected client to become active after job notification")
	}

	if seq := client.Submit("job-1", "deadbeef", "HASH64HEX", "cn/r"); seq != 2 {
		t.Fatalf("expected submit sequence 2, got %d", seq)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotResults := len(spy.results)
		spy.mu.Unlock()
		if gotResults > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	spy.mu.Lock()
	if len(spy.results) != 1 {
		spy.mu.Unlock()
		t.Fatalf("expected one result notification, got %+v", spy.results)
	}
	if !spy.results[0].accepted || spy.results[0].message != "" {
		spy.mu.Unlock()
		t.Fatalf("expected accepted result with no message, got %+v", spy.results[0])
	}
	spy.mu.Unlock()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotDisconnects := spy.disconnects
		spy.mu.Unlock()
		if gotDisconnects > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	spy.mu.Lock()
	if spy.disconnects != 1 {
		spy.mu.Unlock()
		t.Fatalf("expected one disconnect notification, got %d", spy.disconnects)
	}
	spy.mu.Unlock()
}

func TestStratumClient_Connect_Bad(t *testing.T) {
	client := NewStratumClient(proxy.PoolConfig{}, nil)
	if result := client.Connect(); result.OK {
		t.Fatal("expected empty pool URL to fail")
	}
}

func TestStratumClient_HandleMessage_Ugly(t *testing.T) {
	spy := &clientListenerSpy{}
	client := NewStratumClient(proxy.PoolConfig{}, spy)

	client.handleMessage([]byte("not-json"))
	client.handleMessage([]byte(`{"id":1,"error":{"message":"bad"}}`))

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.jobs) != 0 || len(spy.results) != 0 || spy.disconnects != 1 {
		t.Fatalf("expected malformed payloads to be ignored and login error to disconnect once, got jobs=%d results=%d disconnects=%d", len(spy.jobs), len(spy.results), spy.disconnects)
	}
}
