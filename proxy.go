// Package proxy is a CryptoNote stratum mining proxy library.
//
// It accepts miner connections over TCP (optionally TLS), splits the 32-bit nonce
// space across up to 256 simultaneous miners per upstream pool connection (NiceHash
// mode), and presents a small monitoring API.
//
// Full specification: docs/RFC.md
//
//	p, result := proxy.New(cfg)
//	if result.OK { p.Start() }
package proxy

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Proxy owns the servers, splitters, stats, workers, and monitoring API.
//
// p, result := proxy.New(cfg)
// if result.OK { p.Start() }
type Proxy struct {
	config            *Config
	splitter          Splitter
	stats             *Stats
	workers           *Workers
	events            *EventBus
	servers           []*Server
	ticker            *time.Ticker
	watcher           *ConfigWatcher
	done              chan struct{}
	stopOnce          sync.Once
	minersMu          sync.RWMutex
	miners            map[int64]*Miner
	customDiff        *CustomDiff
	customDiffBuckets *CustomDiffBuckets
	rateLimit         *RateLimiter
	httpServer        *http.Server
	accessLog         *accessLogSink
	shareLog          *shareLogSink
	submitCount       atomic.Int64
}

// Splitter is the shared interface implemented by the NiceHash and simple modes.
//
// type stubSplitter struct{}
//
// func (stubSplitter) Connect() {}
type Splitter interface {
	// Connect establishes the first pool upstream connection.
	Connect()
	// OnLogin routes a newly authenticated miner to an upstream slot.
	OnLogin(event *LoginEvent)
	// OnSubmit routes a share submission to the correct upstream.
	OnSubmit(event *SubmitEvent)
	// OnClose releases the upstream slot for a disconnecting miner.
	OnClose(event *CloseEvent)
	// Tick is called every second for keepalive and GC housekeeping.
	Tick(ticks uint64)
	// GC runs every 60 ticks to reclaim disconnected upstream slots.
	GC()
	// Upstreams returns current upstream pool connection counts.
	Upstreams() UpstreamStats
}

// UpstreamStats reports pool connection counts.
//
// stats := proxy.UpstreamStats{Active: 1, Sleep: 0, Error: 0, Total: 1}
type UpstreamStats struct {
	Active uint64 // connections currently receiving jobs
	Sleep  uint64 // idle connections (simple mode reuse pool)
	Error  uint64 // connections in error/reconnecting state
	Total  uint64 // Active + Sleep + Error
}

// LoginEvent is dispatched when a miner completes login.
//
// event := proxy.LoginEvent{Miner: miner}
type LoginEvent struct {
	Miner *Miner
}

// SubmitEvent carries one miner share submission.
//
// event := proxy.SubmitEvent{Miner: miner, JobID: "job-1", Nonce: "deadbeef", Result: "HASH", RequestID: 2}
type SubmitEvent struct {
	Miner     *Miner
	JobID     string
	Nonce     string
	Result    string
	Algo      string
	RequestID int64
}

// CloseEvent is dispatched when a miner connection closes.
//
// event := proxy.CloseEvent{Miner: miner}
type CloseEvent struct {
	Miner *Miner
}

// ConfigWatcher polls a config file for changes.
//
// watcher := proxy.NewConfigWatcher("config.json", func(cfg *proxy.Config) { p.Reload(cfg) })
type ConfigWatcher struct {
	path     string
	onChange func(*Config)
	lastMod  time.Time
	done     chan struct{}
}

// RateLimiter throttles new connections per source IP.
//
// limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 30, BanDurationSeconds: 300})
// limiter.Allow("1.2.3.4:3333")
type RateLimiter struct {
	config  RateLimit
	buckets map[string]*tokenBucket
	banned  map[string]time.Time
	mu      sync.Mutex
}

// tokenBucket is the per-IP refillable counter.
//
// bucket := tokenBucket{tokens: 30, lastRefill: time.Now()}
type tokenBucket struct {
	tokens     int
	lastRefill time.Time
}

// CustomDiff applies a login-time difficulty override.
//
// resolver := proxy.NewCustomDiff(50000)
// resolver.Apply(&Miner{user: "WALLET+75000"})
type CustomDiff struct {
	globalDiff uint64
}
