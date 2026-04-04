package pool

import (
	"sync"

	"dappco.re/go/proxy"
)

// FailoverStrategy wraps an ordered slice of PoolConfig entries.
// It connects to the first enabled pool and fails over in order on error.
// On reconnect it always retries from the primary first.
//
//	strategy := pool.NewFailoverStrategy(cfg.Pools, listener, cfg)
//	strategy.Connect()
type FailoverStrategy struct {
	pools    []proxy.PoolConfig
	current  int
	client   *StratumClient
	listener StratumListener
	cfg      *proxy.Config
	mu       sync.Mutex
}

// StrategyFactory creates a new FailoverStrategy for a given StratumListener.
// Used by splitters to create per-mapper strategies without coupling to Config.
//
//	factory := pool.NewStrategyFactory(cfg)
//	strategy := factory(listener)   // each mapper calls this
type StrategyFactory func(listener StratumListener) Strategy

// Strategy is the interface the splitters use to submit shares and check pool state.
type Strategy interface {
	Connect()
	Submit(jobID, nonce, result, algo string) int64
	Disconnect()
	IsActive() bool
}
