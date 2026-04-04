package nicehash

import (
	"sync"
	"testing"
	"time"
)

type gcStrategy struct {
	mu           sync.Mutex
	disconnected bool
	active       bool
}

func (s *gcStrategy) Connect() {}

func (s *gcStrategy) Submit(jobID, nonce, result, algo string) int64 {
	return 0
}

func (s *gcStrategy) Disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disconnected = true
}

func (s *gcStrategy) IsActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func TestNonceSplitter_GC_Good(t *testing.T) {
	strategy := &gcStrategy{active: false}
	mapper := &NonceMapper{
		id:       42,
		storage:  NewNonceStorage(),
		strategy: strategy,
		lastUsed: time.Now().Add(-2 * time.Minute),
		pending:  make(map[int64]SubmitContext),
	}
	mapper.storage.slots[0] = -1

	splitter := &NonceSplitter{
		mappers: []*NonceMapper{mapper},
		byID:    map[int64]*NonceMapper{mapper.id: mapper},
	}

	splitter.GC()

	if len(splitter.mappers) != 0 {
		t.Fatalf("expected idle mapper to be reclaimed, got %d mapper(s)", len(splitter.mappers))
	}
	if _, ok := splitter.byID[mapper.id]; ok {
		t.Fatalf("expected reclaimed mapper to be removed from lookup table")
	}
	if !strategy.disconnected {
		t.Fatalf("expected reclaimed mapper strategy to be disconnected")
	}
}

func TestNonceSplitter_GC_Bad(t *testing.T) {
	var splitter *NonceSplitter

	splitter.GC()
}

func TestNonceSplitter_GC_Ugly(t *testing.T) {
	strategy := &gcStrategy{active: true}
	mapper := &NonceMapper{
		id:       99,
		storage:  NewNonceStorage(),
		strategy: strategy,
		lastUsed: time.Now().Add(-2 * time.Minute),
		pending:  make(map[int64]SubmitContext),
	}
	mapper.storage.slots[0] = 7

	splitter := &NonceSplitter{
		mappers: []*NonceMapper{mapper},
		byID:    map[int64]*NonceMapper{mapper.id: mapper},
	}

	splitter.GC()

	if len(splitter.mappers) != 1 {
		t.Fatalf("expected active mapper to remain, got %d mapper(s)", len(splitter.mappers))
	}
	if strategy.disconnected {
		t.Fatalf("expected active mapper to stay connected")
	}
}
