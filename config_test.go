package proxy

import "testing"

func TestCoreImpl_Config_Validate_Good(t *testing.T) {
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

func TestConfig_Validate_HTTPLoopbackNoToken_Good(t *testing.T) {
	cfg := &Config{
		Mode:    "nicehash",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		HTTP:    HTTPConfig{Enabled: true, Host: "127.0.0.1", Port: 8080},
	}

	if result := cfg.Validate(); !result.OK {
		t.Fatalf("expected loopback http monitoring without token to remain valid, got error: %v", result.Error)
	}
}

func TestCoreImpl_Config_Validate_Bad(t *testing.T) {
	t.Run("nil_config", func(t *testing.T) {
		var cfg *Config
		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected nil config to fail validation")
		}
	})

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

	t.Run("empty_bind_host", func(t *testing.T) {
		cfg := &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: " ", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected empty bind host to fail validation")
		}
	})

	t.Run("empty_http_host", func(t *testing.T) {
		cfg := &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
			HTTP:    HTTPConfig{Enabled: true, Port: 8080},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected empty http host to fail validation")
		}
	})

	t.Run("public_http_without_token", func(t *testing.T) {
		cfg := &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
			HTTP:    HTTPConfig{Enabled: true, Host: "0.0.0.0", Port: 8080},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected public http monitoring without token to fail validation")
		}
	})

	t.Run("negative_numeric_values", func(t *testing.T) {
		cfg := &Config{
			Mode:         "nicehash",
			Workers:      WorkersByRigID,
			Bind:         []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:        []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
			ReuseTimeout: -1,
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected negative reuse timeout to fail validation")
		}
	})

	t.Run("unsupported_workers_mode", func(t *testing.T) {
		cfg := &Config{
			Mode:    "nicehash",
			Workers: WorkersMode("mystery"),
			Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		}

		if result := cfg.Validate(); result.OK {
			t.Fatalf("expected unsupported workers mode to fail validation")
		}
	})

	t.Run("negative_retry_values", func(t *testing.T) {
		base := Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		}

		for name, mutate := range map[string]func(*Config){
			"retries":      func(cfg *Config) { cfg.Retries = -1 },
			"retry_pause":  func(cfg *Config) { cfg.RetryPause = -1 },
			"rate_limit":   func(cfg *Config) { cfg.RateLimit.MaxConnectionsPerMinute = -1 },
			"ban_duration": func(cfg *Config) { cfg.RateLimit.BanDurationSeconds = -1 },
		} {
			t.Run(name, func(t *testing.T) {
				cfg := base
				mutate(&cfg)
				if result := cfg.Validate(); result.OK {
					t.Fatalf("expected %s to fail validation", name)
				}
			})
		}
	})
}

func TestCoreImpl_Config_Validate_Ugly(t *testing.T) {
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

func TestProxy_New_Bad(t *testing.T) {
	if p, result := New(nil); result.OK || p != nil {
		t.Fatalf("expected nil config to fail construction, got p=%#v result=%+v", p, result)
	}
}

func TestProxy_New_Ugly(t *testing.T) {
	cfg := &Config{
		Mode:    "nicehash",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
		Pools:   []PoolConfig{{URL: "", Enabled: true}},
	}

	if p, result := New(cfg); result.OK || p != nil {
		t.Fatalf("expected malformed config to fail construction, got p=%#v result=%+v", p, result)
	}
}

func TestConfig_normalizeConfigValues_Good(t *testing.T) {
	cfg := &Config{
		Mode:    " NiceHash ",
		Workers: " Rig-ID ",
	}

	normalizeConfigValues(cfg)

	if cfg.Mode != "nicehash" {
		t.Fatalf("expected mode to be normalised, got %q", cfg.Mode)
	}
	if cfg.Workers != WorkersByRigID {
		t.Fatalf("expected workers mode to be normalised, got %q", cfg.Workers)
	}
}

func TestConfig_normalizeConfigValues_Bad(t *testing.T) {
	var cfg *Config
	normalizeConfigValues(cfg)
	if cfg != nil {
		t.Fatal("expected nil config to remain nil")
	}
}

func TestConfig_normalizeConfigValues_Ugly(t *testing.T) {
	cfg := &Config{
		Mode:    "  SIMPLE  ",
		Workers: "  FALSE  ",
	}

	normalizeConfigValues(cfg)

	if cfg.Mode != "simple" {
		t.Fatalf("expected mode to be trimmed and lower-cased, got %q", cfg.Mode)
	}
	if cfg.Workers != WorkersDisabled {
		t.Fatalf("expected workers mode to be trimmed and lower-cased, got %q", cfg.Workers)
	}
}
