package proxy

import (
	"sync"
	"testing"
	"time"
)

// TestStats_OnAccept_Good verifies that accepted counter, hashes, and topDiff are updated.
//
//	stats := proxy.NewStats()
//	stats.OnAccept(proxy.Event{Diff: 100000, Latency: 82})
//	summary := stats.Summary()
//	_ = summary.Accepted  // 1
//	_ = summary.Hashes    // 100000
func TestStateImpl_Stats_OnAccept_Good(t *testing.T) {
	stats := NewStats()

	stats.OnAccept(Event{Diff: 100000, Latency: 82})

	summary := stats.Summary()
	if summary.Accepted != 1 {
		t.Fatalf("expected accepted 1, got %d", summary.Accepted)
	}
	if summary.Hashes != 100000 {
		t.Fatalf("expected hashes 100000, got %d", summary.Hashes)
	}
	if summary.TopDiff[0] != 100000 {
		t.Fatalf("expected top diff 100000, got %d", summary.TopDiff[0])
	}
}

// TestStats_OnAccept_Bad verifies concurrent OnAccept calls do not race.
//
//	stats := proxy.NewStats()
//	// 100 goroutines each call OnAccept — no data race under -race flag.
func TestStateImpl_Stats_OnAccept_Bad(t *testing.T) {
	stats := NewStats()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(diff uint64) {
			defer wg.Done()
			stats.OnAccept(Event{Diff: diff, Latency: 10})
		}(uint64(i + 1))
	}
	wg.Wait()

	summary := stats.Summary()
	if summary.Accepted != 100 {
		t.Fatalf("expected 100 accepted, got %d", summary.Accepted)
	}
}

// TestStats_OnAccept_Ugly verifies that 15 accepts with varying diffs fill all topDiff slots.
//
//	stats := proxy.NewStats()
//	// 15 accepts with diffs 1..15 → topDiff[9] is 6 (10th highest), not 0
func TestStateImpl_Stats_OnAccept_Ugly(t *testing.T) {
	stats := NewStats()

	for i := 1; i <= 15; i++ {
		stats.OnAccept(Event{Diff: uint64(i)})
	}

	summary := stats.Summary()
	// top 10 should be 15, 14, 13, ..., 6
	if summary.TopDiff[0] != 15 {
		t.Fatalf("expected top diff[0]=15, got %d", summary.TopDiff[0])
	}
	if summary.TopDiff[9] != 6 {
		t.Fatalf("expected top diff[9]=6, got %d", summary.TopDiff[9])
	}
}

// TestStats_OnReject_Good verifies that rejected and invalid counters are updated.
//
//	stats := proxy.NewStats()
//	stats.OnReject(proxy.Event{Error: "Low difficulty share"})
func TestStateImpl_Stats_OnReject_Good(t *testing.T) {
	stats := NewStats()

	stats.OnReject(Event{Error: "Low difficulty share"})
	stats.OnReject(Event{Error: "Malformed share"})

	summary := stats.Summary()
	if summary.Rejected != 2 {
		t.Fatalf("expected two rejected shares, got %d", summary.Rejected)
	}
	if summary.Invalid != 2 {
		t.Fatalf("expected two invalid shares, got %d", summary.Invalid)
	}
}

// TestStats_OnReject_Bad verifies that a non-invalid rejection increments rejected but not invalid.
//
//	stats := proxy.NewStats()
//	stats.OnReject(proxy.Event{Error: "Stale share"})
func TestStateImpl_Stats_OnReject_Bad(t *testing.T) {
	stats := NewStats()

	stats.OnReject(Event{Error: "Stale share"})

	summary := stats.Summary()
	if summary.Rejected != 1 {
		t.Fatalf("expected one rejected, got %d", summary.Rejected)
	}
	if summary.Invalid != 0 {
		t.Fatalf("expected zero invalid for non-invalid reason, got %d", summary.Invalid)
	}
}

// TestStats_OnReject_Ugly verifies an expired accepted share increments both accepted and expired.
//
//	stats := proxy.NewStats()
//	stats.OnAccept(proxy.Event{Diff: 1000, Expired: true})
func TestStateImpl_Stats_OnReject_Ugly(t *testing.T) {
	stats := NewStats()

	stats.OnAccept(Event{Diff: 1000, Expired: true})

	summary := stats.Summary()
	if summary.Accepted != 1 {
		t.Fatalf("expected accepted 1, got %d", summary.Accepted)
	}
	if summary.Expired != 1 {
		t.Fatalf("expected expired 1, got %d", summary.Expired)
	}
}

// TestStats_Tick_Good verifies that Tick advances the rolling window position.
//
//	stats := proxy.NewStats()
//	stats.OnAccept(proxy.Event{Diff: 500})
//	stats.Tick()
//	summary := stats.Summary()
func TestStateImpl_Stats_Tick_Good(t *testing.T) {
	stats := NewStats()

	stats.OnAccept(Event{Diff: 500})
	stats.Tick()

	summary := stats.Summary()
	// After one tick, the hashrate should still include the 500 diff
	if summary.Hashrate[HashrateWindow60s] == 0 {
		t.Fatalf("expected non-zero 60s hashrate after accept and tick")
	}
}

// TestStats_Tick_Bad verifies that a nil stats receiver is ignored.
func TestStateImpl_Stats_Tick_Bad(t *testing.T) {
	var stats *Stats
	stats.Tick()
	if stats != nil {
		t.Fatal("expected nil stats to remain nil")
	}
}

// TestStats_Tick_Ugly verifies that a zero-value stats instance can be ticked safely.
func TestStateImpl_Stats_Tick_Ugly(t *testing.T) {
	var stats Stats

	stats.Tick()

	summary := stats.Summary()
	if summary.Accepted != 0 || summary.Hashrate[HashrateWindow60s] != 0 {
		t.Fatalf("expected zero-value stats to remain unchanged after tick, got %+v", summary)
	}
}

// TestStats_OnLogin_OnClose_Good verifies miner count tracking.
//
//	stats := proxy.NewStats()
//	stats.OnLogin(proxy.Event{Miner: &proxy.Miner{}})
//	stats.OnClose(proxy.Event{Miner: &proxy.Miner{}})
func TestStats_OnLogin_OnClose_Good(t *testing.T) {
	stats := NewStats()
	m := &Miner{}

	stats.OnLogin(Event{Miner: m})
	if got := stats.miners.Load(); got != 1 {
		t.Fatalf("expected 1 miner, got %d", got)
	}
	if got := stats.maxMiners.Load(); got != 1 {
		t.Fatalf("expected max miners 1, got %d", got)
	}

	stats.OnClose(Event{Miner: m})
	if got := stats.miners.Load(); got != 0 {
		t.Fatalf("expected 0 miners after close, got %d", got)
	}
	if got := stats.maxMiners.Load(); got != 1 {
		t.Fatalf("expected max miners to remain 1, got %d", got)
	}
}

// TestStats_OnClose_Good verifies that closing a miner decrements the active count.
//
//	stats := proxy.NewStats()
//	stats.OnLogin(proxy.Event{Miner: &proxy.Miner{}})
//	stats.OnClose(proxy.Event{Miner: &proxy.Miner{}})
func TestStateImpl_Stats_OnClose_Good(t *testing.T) {
	stats := NewStats()
	miner := &Miner{}

	stats.OnLogin(Event{Miner: miner})
	stats.OnClose(Event{Miner: miner})

	if got := stats.miners.Load(); got != 0 {
		t.Fatalf("expected active miner count to drop to zero, got %d", got)
	}
	if got := stats.maxMiners.Load(); got != 1 {
		t.Fatalf("expected peak miner count to remain recorded, got %d", got)
	}
}

// TestStats_OnClose_Bad verifies that nil inputs are ignored.
func TestStateImpl_Stats_OnClose_Bad(t *testing.T) {
	var stats *Stats
	stats.OnClose(Event{})

	stats = NewStats()
	stats.OnClose(Event{})
	stats.OnClose(Event{Miner: nil})

	summary := stats.Summary()
	if summary.Accepted != 0 || summary.Rejected != 0 {
		t.Fatalf("expected nil close events to be ignored, got %+v", summary)
	}
}

// TestStats_OnClose_Ugly verifies that repeated closes do not underflow the miner counter.
func TestStateImpl_Stats_OnClose_Ugly(t *testing.T) {
	stats := NewStats()
	miner := &Miner{}

	stats.OnLogin(Event{Miner: miner})
	stats.OnClose(Event{Miner: miner})
	stats.OnClose(Event{Miner: miner})

	if got := stats.miners.Load(); got != 0 {
		t.Fatalf("expected miner count to stay at zero after repeated closes, got %d", got)
	}
	if got := stats.maxMiners.Load(); got != 1 {
		t.Fatalf("expected peak miner count to remain recorded, got %d", got)
	}
}

func TestStateImpl_Stats_Summary_Good(t *testing.T) {
	stats := NewStats()
	stats.startTime = time.Now().Add(-30 * time.Second)

	stats.OnAccept(Event{Diff: 10, Latency: 30})
	stats.OnAccept(Event{Diff: 20, Latency: 10})
	stats.OnAccept(Event{Diff: 40, Latency: 20})

	summary := stats.Summary()
	if summary.Accepted != 3 {
		t.Fatalf("expected 3 accepted shares, got %d", summary.Accepted)
	}
	if summary.AvgTime != 10 {
		t.Fatalf("expected avg time 10 seconds/share, got %d", summary.AvgTime)
	}
	if summary.AvgLatency != 20 {
		t.Fatalf("expected median latency 20ms, got %d", summary.AvgLatency)
	}
	if summary.Hashrate[HashrateWindowAll] <= 0 {
		t.Fatalf("expected all-time hashrate to be populated, got %f", summary.Hashrate[HashrateWindowAll])
	}
	if summary.TopDiff[0] != 40 {
		t.Fatalf("expected top diff to be sorted descending, got %d", summary.TopDiff[0])
	}
}

func TestStateImpl_Stats_Summary_Bad(t *testing.T) {
	var stats *Stats

	summary := stats.Summary()
	if summary.Accepted != 0 || summary.Rejected != 0 || summary.Invalid != 0 || summary.Expired != 0 || summary.Hashes != 0 {
		t.Fatalf("expected nil stats to yield zero counts, got %+v", summary)
	}
	if summary.AvgTime != 0 || summary.AvgLatency != 0 {
		t.Fatalf("expected nil stats to yield zero averages, got %+v", summary)
	}
}

func TestStateImpl_Stats_Summary_Ugly(t *testing.T) {
	var stats Stats

	summary := stats.Summary()
	if summary.Accepted != 0 || summary.Rejected != 0 || summary.Hashes != 0 {
		t.Fatalf("expected zero-value stats to produce zero counts, got %+v", summary)
	}
	if summary.AvgLatency != 0 || summary.AvgTime != 0 {
		t.Fatalf("expected zero-value stats to have zero averages, got %+v", summary)
	}
}

func TestStats_Stats_Connections_Good(t *testing.T) {
	stats := NewStats()
	stats.connections.Store(2)
	if got := stats.Connections(); got != 2 {
		t.Fatalf("expected two connections, got %d", got)
	}
}

func TestStats_Stats_Connections_Bad(t *testing.T) {
	var stats *Stats
	if got := stats.Connections(); got != 0 {
		t.Fatalf("expected nil stats connections 0, got %d", got)
	}
}

func TestStats_Stats_Connections_Ugly(t *testing.T) {
	stats := NewStats()
	stats.connections.Store(^uint64(0))
	if got := stats.Connections(); got != ^uint64(0) {
		t.Fatalf("expected max connection count, got %d", got)
	}
}
