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
	cfg := &Config{
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}

	if result := cfg.Validate(); result.OK {
		t.Fatalf("expected missing mode to fail validation")
	}
}

func TestConfig_Validate_Ugly(t *testing.T) {
	cfg := &Config{
		Mode:    "nicehash",
		Workers: WorkersMode("unknown"),
		Bind:    []BindAddr{{Host: "0.0.0.0", Port: 3333}},
		Pools:   []PoolConfig{{URL: "", Enabled: true}},
	}

	if result := cfg.Validate(); result.OK {
		t.Fatalf("expected invalid workers and empty pool url to fail validation")
	}
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
