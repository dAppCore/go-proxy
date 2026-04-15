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
	if bucket.Accepted != 1 || bucket.Expired != 1 || bucket.HashesTotal != 75 {
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
	p.events.Dispatch(Event{Type: EventReject, Miner: miner, Error: "Low difficulty share"})
	p.events.Dispatch(Event{Type: EventReject, Miner: miner, Error: "Malformed share"})

	summary := p.Summary()
	bucket, ok := summary.CustomDiffStats[10000]
	if !ok {
		t.Fatalf("expected custom diff bucket 10000 to be present")
	}
	if bucket.Rejected != 2 || bucket.Invalid != 2 {
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

func TestProxy_CustomDiffStats_SetEnabled_Good(t *testing.T) {
	buckets := NewCustomDiffBuckets(false)
	miner := &Miner{customDiff: 10000}

	buckets.SetEnabled(true)
	buckets.OnAccept(Event{Miner: miner, Diff: 10})

	snapshot := buckets.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected one bucket after enabling, got %#v", snapshot)
	}
	if got := snapshot[10000].Accepted; got != 1 {
		t.Fatalf("expected accepted count to be recorded after enabling, got %d", got)
	}
}

func TestProxy_CustomDiffStats_SetEnabled_Bad(t *testing.T) {
	var buckets *CustomDiffBuckets
	buckets.SetEnabled(true)
}

func TestProxy_CustomDiffStats_SetEnabled_Ugly(t *testing.T) {
	buckets := NewCustomDiffBuckets(true)
	miner := &Miner{customDiff: 5000}

	buckets.OnAccept(Event{Miner: miner, Diff: 5})
	buckets.SetEnabled(false)

	if snapshot := buckets.Snapshot(); snapshot != nil {
		t.Fatalf("expected snapshot to be nil while disabled, got %#v", snapshot)
	}

	buckets.SetEnabled(true)
	snapshot := buckets.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected existing bucket to survive toggling, got %#v", snapshot)
	}
}

func TestProxy_CustomDiffStats_Snapshot_Good(t *testing.T) {
	buckets := NewCustomDiffBuckets(true)
	miner := &Miner{customDiff: 7500}

	buckets.OnAccept(Event{Miner: miner, Diff: 42, Expired: true})
	buckets.OnReject(Event{Miner: miner, Error: "Invalid nonce"})

	snapshot := buckets.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected one snapshot bucket, got %#v", snapshot)
	}
	bucket := snapshot[7500]
	if bucket.Accepted != 1 || bucket.Rejected != 1 || bucket.Invalid != 1 || bucket.Expired != 1 || bucket.HashesTotal != 42 {
		t.Fatalf("unexpected snapshot totals: %+v", bucket)
	}
}

func TestProxy_CustomDiffStats_Snapshot_Bad(t *testing.T) {
	var buckets *CustomDiffBuckets
	if snapshot := buckets.Snapshot(); snapshot != nil {
		t.Fatalf("expected nil snapshot from nil buckets, got %#v", snapshot)
	}

	buckets = NewCustomDiffBuckets(false)
	if snapshot := buckets.Snapshot(); snapshot != nil {
		t.Fatalf("expected nil snapshot while disabled, got %#v", snapshot)
	}
}

func TestProxy_CustomDiffStats_Snapshot_Ugly(t *testing.T) {
	cases := map[string]struct {
		reason string
		want   bool
	}{
		"empty":            {reason: "", want: false},
		"low diff":         {reason: "low diff", want: true},
		"lowdifficulty":     {reason: "lowdifficulty", want: true},
		"low difficulty":    {reason: "low difficulty share", want: true},
		"malformed":         {reason: "malformed share", want: true},
		"difficulty":        {reason: "difficulty target mismatch", want: true},
		"invalid":           {reason: "invalid nonce", want: true},
		"nonce":             {reason: "bad nonce", want: true},
		"unrelated":         {reason: "job rejected by pool", want: false},
		"case-folding":      {reason: "LOW DIFFICULTY SHARE", want: true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := isInvalidShareReason(tc.reason); got != tc.want {
				t.Fatalf("expected %q to map to %v, got %v", tc.reason, tc.want, got)
			}
		})
	}
}

func TestProxy_CustomDiffStats_bucketLocked_Ugly(t *testing.T) {
	var buckets CustomDiffBuckets
	buckets.enabled = true

	miner := &Miner{customDiff: 91000}
	buckets.OnAccept(Event{Miner: miner, Diff: 11})

	if snapshot := buckets.Snapshot(); len(snapshot) != 1 {
		t.Fatalf("expected zero-value bucket map to be initialised, got %#v", snapshot)
	}
}
