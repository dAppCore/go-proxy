package nicehash

import (
	"testing"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

type reloadableStrategy struct {
	reloads int
}

func (s *reloadableStrategy) Connect()                                       {}
func (s *reloadableStrategy) Submit(jobID, nonce, result, algo string) int64 { return 0 }
func (s *reloadableStrategy) Disconnect()                                    {}
func (s *reloadableStrategy) IsActive() bool                                 { return true }
func (s *reloadableStrategy) ReloadPools()                                   { s.reloads++ }

var _ pool.ReloadableStrategy = (*reloadableStrategy)(nil)

func TestImpl_NonceSplitter_ReloadPools_Good(t *testing.T) {
	strategy := &reloadableStrategy{}
	splitter := &NonceSplitter{
		mappers: []*NonceMapper{
			{strategy: strategy},
		},
	}

	splitter.ReloadPools()

	if strategy.reloads != 1 {
		t.Fatalf("expected mapper strategy to reload once, got %d", strategy.reloads)
	}
}

func TestImpl_NonceSplitter_ReloadPools_Bad(t *testing.T) {
	splitter := &NonceSplitter{
		mappers: []*NonceMapper{
			{strategy: nil},
		},
	}

	splitter.ReloadPools()
}

func TestImpl_NonceSplitter_ReloadPools_Ugly(t *testing.T) {
	splitter := NewNonceSplitter(&proxy.Config{}, proxy.NewEventBus(), func(listener pool.StratumListener) pool.Strategy {
		return &reloadableStrategy{}
	})
	splitter.mappers = []*NonceMapper{
		{strategy: &reloadableStrategy{}},
		{strategy: &reloadableStrategy{}},
	}

	splitter.ReloadPools()

	for index, mapper := range splitter.mappers {
		strategy, ok := mapper.strategy.(*reloadableStrategy)
		if !ok {
			t.Fatalf("expected reloadable strategy at mapper %d", index)
		}
		if strategy.reloads != 1 {
			t.Fatalf("expected mapper %d to reload once, got %d", index, strategy.reloads)
		}
	}
}
