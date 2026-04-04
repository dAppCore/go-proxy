package nicehash

import (
	"bufio"
	"encoding/json"
	"net"
	"sync"
	"testing"

	"dappco.re/go/proxy"
)

type startCountingStrategy struct {
	mu      sync.Mutex
	connect int
}

func (s *startCountingStrategy) Connect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connect++
}

func (s *startCountingStrategy) Submit(jobID, nonce, result, algo string) int64 {
	return 0
}

func (s *startCountingStrategy) Disconnect() {}

func (s *startCountingStrategy) IsActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connect > 0
}

func TestMapper_Start_Good(t *testing.T) {
	strategy := &startCountingStrategy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)

	mapper.Start()

	if strategy.connect != 1 {
		t.Fatalf("expected one connect call, got %d", strategy.connect)
	}
}

func TestMapper_Start_Bad(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, nil)

	mapper.Start()
}

func TestMapper_Start_Ugly(t *testing.T) {
	strategy := &startCountingStrategy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)

	mapper.Start()
	mapper.Start()

	if strategy.connect != 1 {
		t.Fatalf("expected Start to be idempotent, got %d connect calls", strategy.connect)
	}
}

func TestMapper_Submit_InvalidJob_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	miner.SetID(7)
	strategy := &startCountingStrategy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)
	mapper.storage.job = proxy.Job{JobID: "job-1", Blob: "blob", Target: "b88d0600"}

	done := make(chan struct{})
	go func() {
		mapper.Submit(&proxy.SubmitEvent{
			Miner:     miner,
			JobID:     "job-missing",
			Nonce:     "deadbeef",
			Result:    "hash",
			RequestID: 42,
		})
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read error reply: %v", err)
	}
	<-done

	var payload struct {
		ID    float64 `json:"id"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal error reply: %v", err)
	}
	if payload.ID != 42 {
		t.Fatalf("expected request id 42, got %v", payload.ID)
	}
	if payload.Error.Message != "Invalid job id" {
		t.Fatalf("expected invalid job error, got %q", payload.Error.Message)
	}
	if len(mapper.pending) != 0 {
		t.Fatalf("expected invalid submit not to create a pending entry")
	}
}
