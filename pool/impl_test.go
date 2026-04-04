package pool

import (
	"testing"

	"dappco.re/go/proxy"
)

func TestFailoverStrategy_CurrentPools_Good(t *testing.T) {
	cfg := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool-a.example:3333", Enabled: true}},
	}
	strategy := NewFailoverStrategy(cfg.Pools, nil, cfg)

	if got := len(strategy.currentPools()); got != 1 {
		t.Fatalf("expected 1 pool, got %d", got)
	}

	cfg.Pools = []proxy.PoolConfig{{URL: "pool-b.example:4444", Enabled: true}}

	if got := strategy.currentPools(); len(got) != 1 || got[0].URL != "pool-b.example:4444" {
		t.Fatalf("expected current pools to follow config reload, got %+v", got)
	}
}
