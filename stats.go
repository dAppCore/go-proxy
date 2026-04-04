package proxy

import (
	"sync"
	"sync/atomic"
	"time"
)

// Stats tracks global proxy metrics. Hot-path counters are atomic. Hashrate windows
// use a ring buffer per window size, advanced by Tick().
//
//	stats := proxy.NewStats()
//	bus.Subscribe(proxy.EventAccept, stats.OnAccept)
//	bus.Subscribe(proxy.EventReject, stats.OnReject)
type Stats struct {
	accepted    atomic.Uint64
	rejected    atomic.Uint64
	invalid     atomic.Uint64
	expired     atomic.Uint64
	hashes      atomic.Uint64 // cumulative sum of accepted share difficulties
	connections atomic.Uint64 // total TCP connections accepted (ever)
	miners      atomic.Uint64 // current connected miners
	maxMiners   atomic.Uint64 // peak concurrent miner count
	topDiff     [10]uint64    // top-10 accepted difficulties, sorted descending; guarded by mu
	latency     []uint16      // pool response latencies in ms; capped at 10000 samples; guarded by mu
	windows     [6]tickWindow // one per hashrate reporting period
	startTime   time.Time
	mu          sync.Mutex
}

// Hashrate window sizes in seconds. Index maps to Stats.windows and SummaryResponse.Hashrate.
const (
	HashrateWindow60s   = 0 // 1 minute
	HashrateWindow600s  = 1 // 10 minutes
	HashrateWindow3600s = 2 // 1 hour
	HashrateWindow12h   = 3 // 12 hours
	HashrateWindow24h   = 4 // 24 hours
	HashrateWindowAll   = 5 // all-time (single accumulator, no window)
)

// tickWindow is a fixed-capacity ring buffer of per-second difficulty sums.
type tickWindow struct {
	buckets []uint64
	pos     int
	size    int // window size in seconds = len(buckets)
}

// StatsSummary is the serialisable snapshot returned by Summary().
//
//	summary := stats.Summary()
type StatsSummary struct {
	Accepted        uint64                           `json:"accepted"`
	Rejected        uint64                           `json:"rejected"`
	Invalid         uint64                           `json:"invalid"`
	Expired         uint64                           `json:"expired"`
	Hashes          uint64                           `json:"hashes_total"`
	AvgTime         uint32                           `json:"avg_time"` // seconds per accepted share
	AvgLatency      uint32                           `json:"latency"`  // median pool response latency in ms
	Hashrate        [6]float64                       `json:"hashrate"` // H/s per window (index = HashrateWindow* constants)
	TopDiff         [10]uint64                       `json:"best"`
	CustomDiffStats map[uint64]CustomDiffBucketStats `json:"custom_diff_stats,omitempty"`
}
