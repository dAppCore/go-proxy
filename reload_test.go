package proxy

import "testing"

type reloadableSplitter struct {
	reloads int
}

func (s *reloadableSplitter) Connect()                    {}
func (s *reloadableSplitter) OnLogin(event *LoginEvent)   {}
func (s *reloadableSplitter) OnSubmit(event *SubmitEvent) {}
func (s *reloadableSplitter) OnClose(event *CloseEvent)   {}
func (s *reloadableSplitter) Tick(ticks uint64)           {}
func (s *reloadableSplitter) GC()                         {}
func (s *reloadableSplitter) Upstreams() UpstreamStats    { return UpstreamStats{} }
func (s *reloadableSplitter) ReloadPools()                { s.reloads++ }

func TestProxy_Reload_Good(t *testing.T) {
	original := &Config{
		Mode:    "nicehash",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []PoolConfig{{URL: "pool-a.example:3333", Enabled: true}},
	}
	p := &Proxy{
		config:     original,
		customDiff: NewCustomDiff(1),
		rateLimit:  NewRateLimiter(RateLimit{}),
	}

	updated := &Config{
		Mode:            "simple",
		Workers:         WorkersByUser,
		Bind:            []BindAddr{{Host: "0.0.0.0", Port: 4444}},
		Pools:           []PoolConfig{{URL: "pool-b.example:4444", Enabled: true}},
		CustomDiff:      50000,
		AccessPassword:  "secret",
		CustomDiffStats: true,
		AlgoExtension:   true,
		AccessLogFile:   "/tmp/access.log",
		ReuseTimeout:    30,
		Retries:         5,
		RetryPause:      2,
		Watch:           true,
		RateLimit:       RateLimit{MaxConnectionsPerMinute: 10, BanDurationSeconds: 60},
	}

	p.Reload(updated)

	if p.config != original {
		t.Fatalf("expected reload to preserve the existing config pointer")
	}
	if got := p.config.Bind[0]; got.Host != "127.0.0.1" || got.Port != 3333 {
		t.Fatalf("expected bind addresses to remain unchanged, got %+v", got)
	}
	if p.config.Mode != "nicehash" {
		t.Fatalf("expected mode to remain unchanged, got %q", p.config.Mode)
	}
	if p.config.Workers != WorkersByUser {
		t.Fatalf("expected workers mode to reload, got %q", p.config.Workers)
	}
	if got := p.config.Pools[0].URL; got != "pool-b.example:4444" {
		t.Fatalf("expected pools to reload, got %q", got)
	}
	if got := p.customDiff.globalDiff.Load(); got != 50000 {
		t.Fatalf("expected custom diff to reload, got %d", got)
	}
	if !p.rateLimit.IsActive() {
		t.Fatalf("expected rate limiter to be replaced with active configuration")
	}
}

func TestProxy_Reload_WorkersMode_Good(t *testing.T) {
	miner := &Miner{id: 7, user: "wallet-a", rigID: "rig-a", ip: "10.0.0.7"}
	workers := NewWorkers(WorkersByRigID, nil)
	workers.OnLogin(Event{Miner: miner})

	p := &Proxy{
		config: &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool-a.example:3333", Enabled: true}},
		},
		workers: workers,
		miners:  map[int64]*Miner{miner.id: miner},
	}

	p.Reload(&Config{
		Mode:    "nicehash",
		Workers: WorkersByUser,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []PoolConfig{{URL: "pool-a.example:3333", Enabled: true}},
	})

	if got := p.WorkersMode(); got != WorkersByUser {
		t.Fatalf("expected proxy workers mode %q, got %q", WorkersByUser, got)
	}
	records := p.WorkerRecords()
	if len(records) != 1 {
		t.Fatalf("expected one rebuilt worker record, got %d", len(records))
	}
	if got := records[0].Name; got != "wallet-a" {
		t.Fatalf("expected worker record to rebuild using user mode, got %q", got)
	}
}

func TestProxy_Reload_UpdatesServers(t *testing.T) {
	originalLimiter := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1})
	p := &Proxy{
		config:    &Config{Mode: "nicehash", Workers: WorkersByRigID},
		rateLimit: originalLimiter,
		servers: []*Server{
			{limiter: originalLimiter},
		},
	}

	p.Reload(&Config{
		Mode:          "nicehash",
		Workers:       WorkersByRigID,
		Bind:          []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:         []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		RateLimit:     RateLimit{MaxConnectionsPerMinute: 10},
		AccessLogFile: "",
	})

	if got := p.servers[0].limiter; got != p.rateLimit {
		t.Fatalf("expected server limiter to be updated")
	}
	if p.rateLimit == originalLimiter {
		t.Fatalf("expected rate limiter instance to be replaced")
	}
}

func TestProxy_Reload_WatchEnabled_Good(t *testing.T) {
	p := &Proxy{
		config: &Config{
			Mode:       "nicehash",
			Workers:    WorkersByRigID,
			Bind:       []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:      []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
			configPath: "/tmp/proxy.json",
		},
	}

	p.Reload(&Config{
		Mode:       "nicehash",
		Workers:    WorkersByRigID,
		Bind:       []BindAddr{{Host: "127.0.0.1", Port: 4444}},
		Pools:      []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		Watch:      true,
		configPath: "/tmp/ignored.json",
	})

	if p.watcher == nil {
		t.Fatalf("expected reload to create a watcher when watch is enabled")
	}
	if got := p.watcher.path; got != "/tmp/proxy.json" {
		t.Fatalf("expected watcher to keep the original config path, got %q", got)
	}
	p.watcher.Stop()
}

func TestProxy_Reload_WatchDisabled_Bad(t *testing.T) {
	watcher := NewConfigWatcher("/tmp/proxy.json", func(*Config) {})
	p := &Proxy{
		config: &Config{
			Mode:       "nicehash",
			Workers:    WorkersByRigID,
			Bind:       []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:      []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
			configPath: "/tmp/proxy.json",
			Watch:      true,
		},
		watcher: watcher,
	}

	p.Reload(&Config{
		Mode:       "nicehash",
		Workers:    WorkersByRigID,
		Bind:       []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:      []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		Watch:      false,
		configPath: "/tmp/ignored.json",
	})

	if p.watcher != nil {
		t.Fatalf("expected reload to stop and clear the watcher when watch is disabled")
	}
	select {
	case <-watcher.done:
	default:
		t.Fatalf("expected existing watcher to be stopped")
	}
}

func TestProxy_Reload_PoolsChanged_ReloadsSplitter_Good(t *testing.T) {
	splitter := &reloadableSplitter{}
	p := &Proxy{
		config: &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool-a.example:3333", Enabled: true}},
		},
		splitter: splitter,
	}

	p.Reload(&Config{
		Mode:    "nicehash",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []PoolConfig{{URL: "pool-b.example:3333", Enabled: true}},
	})

	if splitter.reloads != 1 {
		t.Fatalf("expected pool reload to reconnect upstreams once, got %d", splitter.reloads)
	}
}

func TestProxy_Reload_PoolsUnchanged_DoesNotReloadSplitter_Ugly(t *testing.T) {
	splitter := &reloadableSplitter{}
	p := &Proxy{
		config: &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool-a.example:3333", Enabled: true}},
		},
		splitter: splitter,
	}

	p.Reload(&Config{
		Mode:    "nicehash",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []PoolConfig{{URL: "pool-a.example:3333", Enabled: true}},
	})

	if splitter.reloads != 0 {
		t.Fatalf("expected unchanged pool config to skip reconnect, got %d", splitter.reloads)
	}
}
