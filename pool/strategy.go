package pool

import (
	"sync"

	"dappco.re/go/proxy"
)

// FailoverStrategy wraps an ordered slice of PoolConfig entries.
//
//	strategy := pool.NewFailoverStrategy([]proxy.PoolConfig{
//	    {URL: "primary.example:3333", Enabled: true},
//	    {URL: "backup.example:3333", Enabled: true},
//	}, listener, cfg)
//	strategy.Connect()
type FailoverStrategy struct {
	pools    []proxy.PoolConfig
	current  int
	client   *StratumClient
	listener StratumListener
	config   *proxy.Config
	closing  bool
	mu       sync.Mutex
}

// CurrentIndex returns the active pool index selected by the strategy.
func (s *FailoverStrategy) CurrentIndex() int {
	if s == nil {
		return -1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

// Client returns the current pool client, if any.
func (s *FailoverStrategy) Client() *StratumClient {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

// StrategyFactory creates a FailoverStrategy for a given StratumListener.
//
//	factory := pool.NewStrategyFactory(cfg)
//	strategy := factory(listener)
type StrategyFactory func(listener StratumListener) Strategy

// Strategy is the interface splitters use to submit shares and inspect pool state.
type Strategy interface {
	Connect()
	Submit(jobID, nonce, result, algo string) int64
	Disconnect()
	IsActive() bool
}

// ReloadableStrategy re-establishes an upstream connection after config changes.
//
//	strategy.ReloadPools()
type ReloadableStrategy interface {
	ReloadPools()
}
