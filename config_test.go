package proxy

import "testing"

func TestConfig_Validate_Good(t *testing.T) {
	cfg := &Config{
		Mode:    "nicehash",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}

	if result := cfg.Validate(); !result.OK {
		t.Fatalf("expected valid config, got error: %v", result.Error)
	}
}

func TestConfig_Validate_Bad(t *testing.T) {
	t.Run("missing_mode", func(t *testing.T) {
		cfg := &Config{
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected missing mode to fail validation")
		}
	})

	t.Run("unsupported_mode", func(t *testing.T) {
		cfg := &Config{
			Mode:    "bogus",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected unsupported mode to fail validation")
		}
	})

	t.Run("empty_bind_list", func(t *testing.T) {
		cfg := &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected empty bind list to fail validation")
		}
	})

	t.Run("empty_pool_list", func(t *testing.T) {
		cfg := &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected empty pool list to fail validation")
		}
	})
}

func TestConfig_Validate_Ugly(t *testing.T) {
	t.Run("enabled_pool_without_url", func(t *testing.T) {
		cfg := &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
			Pools:   []PoolConfig{{URL: "", Enabled: true}},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected enabled pool without url to fail validation")
		}
	})
}

func TestConfig_Validate_NoEnabledPool_Good(t *testing.T) {
	cfg := &Config{
		Mode:    "simple",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
		Pools: []PoolConfig{
			{URL: "pool-a.example:3333", Enabled: false},
			{URL: "pool-b.example:4444", Enabled: false},
		},
	}

	if result := cfg.Validate(); !result.OK {
		t.Fatalf("expected config with no enabled pools to be valid, got error: %v", result.Error)
	}
}

func TestProxy_New_WhitespaceMode_Good(t *testing.T) {
	originalFactory, hadFactory := splitterFactoryForMode("nicehash")
	if hadFactory {
		t.Cleanup(func() {
			RegisterSplitterFactory("nicehash", originalFactory)
		})
	}

	called := false
	RegisterSplitterFactory("nicehash", func(*Config, *EventBus) Splitter {
		called = true
		return &noopSplitter{}
	})

	cfg := &Config{
		Mode:    " nicehash ",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}

	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected whitespace-padded mode to remain valid, got error: %v", result.Error)
	}
	if !called {
		t.Fatalf("expected trimmed mode lookup to invoke the registered splitter factory")
	}
	if _, ok := p.splitter.(*noopSplitter); !ok {
		t.Fatalf("expected test splitter to be wired, got %#v", p.splitter)
	}
}
