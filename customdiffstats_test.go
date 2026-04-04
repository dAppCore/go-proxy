package proxy

import "testing"

func TestProxy_CustomDiffStats_Good(t *testing.T) {
	cfg := &Config{
		Mode:            "nicehash",
		Workers:         WorkersByRigID,
		CustomDiffStats: true,
		Bind:            []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:           []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{customDiff: 50000}
	p.events.Dispatch(Event{Type: EventAccept, Miner: miner, Diff: 75, Expired: true})

	summary := p.Summary()
	bucket, ok := summary.CustomDiffStats[50000]
	if !ok {
		t.Fatalf("expected custom diff bucket 50000 to be present")
	}
	if bucket.Accepted != 1 || bucket.Expired != 1 || bucket.Hashes != 75 {
		t.Fatalf("unexpected bucket totals: %+v", bucket)
	}
}

func TestProxy_CustomDiffStats_Bad(t *testing.T) {
	cfg := &Config{
		Mode:            "nicehash",
		Workers:         WorkersByRigID,
		CustomDiffStats: true,
		Bind:            []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:           []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{customDiff: 10000}
	p.events.Dispatch(Event{Type: EventReject, Miner: miner, Error: "Invalid nonce"})

	summary := p.Summary()
	bucket, ok := summary.CustomDiffStats[10000]
	if !ok {
		t.Fatalf("expected custom diff bucket 10000 to be present")
	}
	if bucket.Rejected != 1 || bucket.Invalid != 1 {
		t.Fatalf("unexpected bucket totals: %+v", bucket)
	}
}

func TestProxy_CustomDiffStats_Ugly(t *testing.T) {
	cfg := &Config{
		Mode:            "nicehash",
		Workers:         WorkersByRigID,
		CustomDiffStats: false,
		Bind:            []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:           []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{customDiff: 25000}
	p.events.Dispatch(Event{Type: EventAccept, Miner: miner, Diff: 1})

	summary := p.Summary()
	if len(summary.CustomDiffStats) != 0 {
		t.Fatalf("expected custom diff stats to remain disabled, got %+v", summary.CustomDiffStats)
	}
}
