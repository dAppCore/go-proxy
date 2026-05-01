package simple

import (
	"testing"

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

func TestReload_SimpleSplitter_ReloadPools_Good(t *testing.T) {
	strategy := &reloadableStrategy{}
	splitter := &SimpleSplitter{
		active: map[int64]*SimpleMapper{
			1: {strategy: strategy},
		},
		idle: map[int64]*SimpleMapper{},
	}

	splitter.ReloadPools()

	if strategy.reloads != 1 {
		t.Fatalf("expected active mapper strategy to reload once, got %d", strategy.reloads)
	}
}

func TestReload_SimpleSplitter_ReloadPools_Bad(t *testing.T) {
	splitter := &SimpleSplitter{
		active: map[int64]*SimpleMapper{
			1: {strategy: nil},
		},
		idle: map[int64]*SimpleMapper{},
	}

	splitter.ReloadPools()
}

func TestReload_SimpleSplitter_ReloadPools_Ugly(t *testing.T) {
	active := &reloadableStrategy{}
	idle := &reloadableStrategy{}
	splitter := &SimpleSplitter{
		active: map[int64]*SimpleMapper{
			1: {strategy: active},
		},
		idle: map[int64]*SimpleMapper{
			2: {strategy: idle},
		},
	}

	splitter.ReloadPools()

	if active.reloads != 1 {
		t.Fatalf("expected active mapper reload, got %d", active.reloads)
	}
	if idle.reloads != 1 {
		t.Fatalf("expected idle mapper reload, got %d", idle.reloads)
	}
}
