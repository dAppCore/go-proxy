package nicehash

import (
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
