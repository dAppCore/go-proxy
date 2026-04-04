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

// Proxy is the top-level orchestrator. It owns the server, splitter, stats, workers,
// event bus, tick goroutine, and optional HTTP API.
//
//	p, result := proxy.New(cfg)
//	if result.OK { p.Start() }
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

// Splitter is the interface both NonceSplitter and SimpleSplitter satisfy.
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

// UpstreamStats carries pool connection state counts for monitoring.
type UpstreamStats struct {
	Active uint64 // connections currently receiving jobs
	Sleep  uint64 // idle connections (simple mode reuse pool)
	Error  uint64 // connections in error/reconnecting state
	Total  uint64 // Active + Sleep + Error
}

// LoginEvent is dispatched when a miner completes the login handshake.
type LoginEvent struct {
	Miner *Miner
}

// SubmitEvent is dispatched when a miner submits a share.
type SubmitEvent struct {
	Miner     *Miner
	JobID     string
	Nonce     string
	Result    string
	Algo      string
	RequestID int64
}

// CloseEvent is dispatched when a miner TCP connection closes.
type CloseEvent struct {
	Miner *Miner
}

// ConfigWatcher polls a config file for mtime changes and calls onChange on modification.
//
//	w := proxy.NewConfigWatcher("config.json", func(cfg *proxy.Config) { p.Reload(cfg) })
//	w.Start()
type ConfigWatcher struct {
	path     string
	onChange func(*Config)
	lastMod  time.Time
	done     chan struct{}
}

// RateLimiter implements per-IP token bucket connection rate limiting.
//
//	rl := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 30, BanDurationSeconds: 300})
//	if rl.Allow("1.2.3.4:3333") { proceed() }
type RateLimiter struct {
	config  RateLimit
	buckets map[string]*tokenBucket
	banned  map[string]time.Time
	mu      sync.Mutex
}

// tokenBucket is a simple token bucket for one IP.
type tokenBucket struct {
	tokens     int
	lastRefill time.Time
}

// CustomDiff resolves and applies per-miner difficulty overrides at login time.
// Resolution order: user-suffix (+N) > Config.CustomDiff > pool difficulty.
//
//	cd := proxy.NewCustomDiff(cfg.CustomDiff)
//	bus.Subscribe(proxy.EventLogin, cd.OnLogin)
type CustomDiff struct {
	globalDiff uint64
}
