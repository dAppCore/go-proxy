package pool

import (
	"testing"

	"dappco.re/go/proxy"
)

// TestFailoverStrategy_CurrentPools_Good verifies that currentPools follows the live config.
//
//	strategy := pool.NewFailoverStrategy(cfg.Pools, nil, cfg)
//	strategy.currentPools() // returns cfg.Pools
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

// TestFailoverStrategy_CurrentPools_Bad verifies that a nil strategy returns an empty pool list.
//
//	var strategy *pool.FailoverStrategy
//	strategy.currentPools() // nil
func TestFailoverStrategy_CurrentPools_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	pools := strategy.currentPools()
	if pools != nil {
		t.Fatalf("expected nil pools from nil strategy, got %+v", pools)
	}
}

// TestFailoverStrategy_CurrentPools_Ugly verifies that a strategy with a nil config
// falls back to the pools passed at construction time.
//
//	strategy := pool.NewFailoverStrategy(initialPools, nil, nil)
//	strategy.currentPools() // returns initialPools
func TestFailoverStrategy_CurrentPools_Ugly(t *testing.T) {
	initialPools := []proxy.PoolConfig{
		{URL: "fallback.example:3333", Enabled: true},
		{URL: "fallback.example:4444", Enabled: false},
	}
	strategy := NewFailoverStrategy(initialPools, nil, nil)

	got := strategy.currentPools()
	if len(got) != 2 {
		t.Fatalf("expected 2 pools from constructor fallback, got %d", len(got))
	}
	if got[0].URL != "fallback.example:3333" {
		t.Fatalf("expected constructor pool URL, got %q", got[0].URL)
	}
}

// TestFailoverStrategy_EnabledPools_Good verifies that only enabled pools are selected.
//
//	enabled := pool.enabledPools(pools) // filters to enabled-only
func TestFailoverStrategy_EnabledPools_Good(t *testing.T) {
	pools := []proxy.PoolConfig{
		{URL: "active.example:3333", Enabled: true},
		{URL: "disabled.example:3333", Enabled: false},
		{URL: "active2.example:3333", Enabled: true},
	}
	got := enabledPools(pools)
	if len(got) != 2 {
		t.Fatalf("expected 2 enabled pools, got %d", len(got))
	}
	if got[0].URL != "active.example:3333" || got[1].URL != "active2.example:3333" {
		t.Fatalf("expected only enabled pool URLs, got %+v", got)
	}
}

// TestFailoverStrategy_EnabledPools_Bad verifies that an empty pool list returns empty.
//
//	pool.enabledPools(nil) // empty
func TestFailoverStrategy_EnabledPools_Bad(t *testing.T) {
	got := enabledPools(nil)
	if len(got) != 0 {
		t.Fatalf("expected 0 pools from nil input, got %d", len(got))
	}
}

// TestFailoverStrategy_EnabledPools_Ugly verifies that all-disabled pools return empty.
//
//	pool.enabledPools([]proxy.PoolConfig{{Enabled: false}}) // empty
func TestFailoverStrategy_EnabledPools_Ugly(t *testing.T) {
	pools := []proxy.PoolConfig{
		{URL: "a.example:3333", Enabled: false},
		{URL: "b.example:3333", Enabled: false},
	}
	got := enabledPools(pools)
	if len(got) != 0 {
		t.Fatalf("expected 0 enabled pools when all disabled, got %d", len(got))
	}
}

// TestNewStrategyFactory_Good verifies the factory creates a strategy connected to the config.
//
//	factory := pool.NewStrategyFactory(cfg)
//	strategy := factory(listener) // creates FailoverStrategy
func TestNewStrategyFactory_Good(t *testing.T) {
	cfg := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	factory := NewStrategyFactory(cfg)
	if factory == nil {
		t.Fatal("expected a non-nil factory")
	}
	strategy := factory(nil)
	if strategy == nil {
		t.Fatal("expected a non-nil strategy from factory")
	}
	if strategy.IsActive() {
		t.Fatal("expected new strategy to be inactive before connecting")
	}
}

// TestNewStrategyFactory_Bad verifies a factory created with nil config does not panic.
//
//	factory := pool.NewStrategyFactory(nil)
//	strategy := factory(nil)
func TestNewStrategyFactory_Bad(t *testing.T) {
	factory := NewStrategyFactory(nil)
	strategy := factory(nil)
	if strategy == nil {
		t.Fatal("expected a non-nil strategy even from nil config")
	}
}

// TestNewStrategyFactory_Ugly verifies the factory forwards the correct pool list to the strategy.
//
//	cfg.Pools = append(cfg.Pools, proxy.PoolConfig{URL: "added.example:3333", Enabled: true})
//	strategy := factory(nil)
//	// strategy sees the updated pools via the shared config pointer
func TestNewStrategyFactory_Ugly(t *testing.T) {
	cfg := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	factory := NewStrategyFactory(cfg)
	cfg.Pools = append(cfg.Pools, proxy.PoolConfig{URL: "added.example:3333", Enabled: true})

	strategy := factory(nil)
	fs, ok := strategy.(*FailoverStrategy)
	if !ok {
		t.Fatal("expected FailoverStrategy")
	}
	pools := fs.currentPools()
	if len(pools) != 2 {
		t.Fatalf("expected 2 pools after config update, got %d", len(pools))
	}
}

func TestPoolImpl_requestID_Good(t *testing.T) {
	cases := []struct {
		name string
		id   any
		want int64
	}{
		{name: "float64", id: float64(7), want: 7},
		{name: "int64", id: int64(8), want: 8},
		{name: "int", id: int(9), want: 9},
		{name: "string", id: "10", want: 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := requestID(tc.id); got != tc.want {
				t.Fatalf("expected request id %d, got %d", tc.want, got)
			}
		})
	}
}

func TestPoolImpl_requestID_Bad(t *testing.T) {
	if got := requestID("not-a-number"); got != 0 {
		t.Fatalf("expected non-numeric string to map to 0, got %d", got)
	}
	if got := requestID(nil); got != 0 {
		t.Fatalf("expected nil request id to map to 0, got %d", got)
	}
}

func TestPoolImpl_requestID_Ugly(t *testing.T) {
	if got := requestID(float64(12.9)); got != 12 {
		t.Fatalf("expected fractional float request id to truncate, got %d", got)
	}
	if got := requestID(true); got != 0 {
		t.Fatalf("expected unsupported request id type to map to 0, got %d", got)
	}
}
