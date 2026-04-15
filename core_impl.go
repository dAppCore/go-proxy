package proxy

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"
)

// Result is the success/error carrier used by constructors and loaders.
//
//	cfg, result := proxy.LoadConfig("config.json")
//	if !result.OK {
//	    return result.Error
//	}
type Result struct {
	Value any
	OK    bool
	Error error
}

func newSuccessResult() Result {
	return Result{OK: true}
}

func newErrorResult(err error) Result {
	return Result{Value: err, OK: false, Error: err}
}

var splitterFactoriesMu sync.RWMutex
var splitterFactoriesByMode = map[string]func(*Config, *EventBus) Splitter{}

// RegisterSplitterFactory installs the constructor used for one proxy mode.
//
//	proxy.RegisterSplitterFactory("simple", func(cfg *proxy.Config, bus *proxy.EventBus) proxy.Splitter {
//	    return simple.NewSimpleSplitter(cfg, bus, nil)
//	})
func RegisterSplitterFactory(mode string, factory func(*Config, *EventBus) Splitter) {
	splitterFactoriesMu.Lock()
	defer splitterFactoriesMu.Unlock()
	splitterFactoriesByMode[lowerString(trimString(mode))] = factory
}

func splitterFactoryForMode(mode string) (func(*Config, *EventBus) Splitter, bool) {
	splitterFactoriesMu.RLock()
	defer splitterFactoriesMu.RUnlock()
	factory, ok := splitterFactoriesByMode[lowerString(trimString(mode))]
	return factory, ok
}

// cfg, result := proxy.LoadConfig("/etc/proxy.json")
//
//	if !result.OK {
//	    return result.Error
//	}
func LoadConfig(path string) (*Config, Result) {
	readResult := proxyFileSystem().Read(path)
	if !readResult.OK {
		cause, _ := readResult.Value.(error)
		return nil, newErrorResult(NewScopedError("proxy.config", "read config failed", cause))
	}

	config := &Config{}
	data, _ := readResult.Value.(string)
	if !jsonUnmarshalString(data, config) {
		return nil, newErrorResult(NewScopedError("proxy.config", "parse config failed", nil))
	}
	normalizeConfigValues(config)
	config.configPath = path
	return config, Result{Value: config, OK: true}
}

//	cfg := &proxy.Config{
//	    Mode:    "nicehash",
//	    Bind:    []proxy.BindAddr{{Host: "0.0.0.0", Port: 3333}},
//	    Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
//	    Workers: proxy.WorkersByRigID,
//	}
//
//	if result := cfg.Validate(); !result.OK {
//	    return result
//	}
func (c *Config) Validate() Result {
	if c == nil {
		return newErrorResult(NewScopedError("proxy.config", "config is nil", nil))
	}
	mode := lowerString(trimString(c.Mode))
	if mode == "" {
		return newErrorResult(NewScopedError("proxy.config", "mode is empty", nil))
	}
	if !isSupportedMode(mode) {
		return newErrorResult(NewScopedError("proxy.config", "unsupported mode", nil))
	}
	if len(c.Bind) == 0 {
		return newErrorResult(NewScopedError("proxy.config", "bind list is empty", nil))
	}
	if len(c.Pools) == 0 {
		return newErrorResult(NewScopedError("proxy.config", "pool list is empty", nil))
	}
	workers := lowerString(trimString(string(c.Workers)))
	if workers != "" && !isSupportedWorkersMode(workers) {
		return newErrorResult(NewScopedError("proxy.config", "unsupported workers mode", nil))
	}
	for _, pool := range c.Pools {
		if pool.Enabled && trimString(pool.URL) == "" {
			return newErrorResult(NewScopedError("proxy.config", "enabled pool url is empty", nil))
		}
	}
	return newSuccessResult()
}

func isSupportedMode(mode string) bool {
	switch lowerString(trimString(mode)) {
	case "nicehash", "simple":
		return true
	default:
		return false
	}
}

func isSupportedWorkersMode(mode string) bool {
	switch lowerString(trimString(mode)) {
	case "", string(WorkersByRigID), string(WorkersByUser), string(WorkersByPass), string(WorkersByAgent), string(WorkersByIP), string(WorkersDisabled):
		return true
	default:
		return false
	}
}

// bus := proxy.NewEventBus()
//
//	bus.Subscribe(proxy.EventLogin, func(e proxy.Event) {
//	    _ = e.Miner
//	})
func NewEventBus() *EventBus {
	return &EventBus{listeners: make(map[EventType][]EventHandler)}
}

// bus.Subscribe(proxy.EventAccept, stats.OnAccept)
func (b *EventBus) Subscribe(t EventType, h EventHandler) {
	if b == nil || h == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listeners == nil {
		b.listeners = make(map[EventType][]EventHandler)
	}
	b.listeners[t] = append(b.listeners[t], h)
}

// bus.Dispatch(proxy.Event{Type: proxy.EventLogin, Miner: miner})
func (b *EventBus) Dispatch(e Event) {
	if b == nil {
		return
	}
	b.mu.RLock()
	handlers := append([]EventHandler(nil), b.listeners[e.Type]...)
	b.mu.RUnlock()
	for _, handler := range handlers {
		func() {
			defer func() {
				_ = recover()
			}()
			handler(e)
		}()
	}
}

type shareSinkGroup struct {
	sinks []ShareSink
}

func newShareSinkGroup(sinks ...ShareSink) *shareSinkGroup {
	group := &shareSinkGroup{sinks: make([]ShareSink, 0, len(sinks))}
	for _, sink := range sinks {
		if sink != nil {
			group.sinks = append(group.sinks, sink)
		}
	}
	return group
}

func (g *shareSinkGroup) OnAccept(e Event) {
	if g == nil {
		return
	}
	for _, sink := range g.sinks {
		func() {
			defer func() {
				_ = recover()
			}()
			sink.OnAccept(e)
		}()
	}
}

func (g *shareSinkGroup) OnReject(e Event) {
	if g == nil {
		return
	}
	for _, sink := range g.sinks {
		func() {
			defer func() {
				_ = recover()
			}()
			sink.OnReject(e)
		}()
	}
}

// IsValid returns true when the job contains a blob and job id.
//
//	if !job.IsValid() {
//	    return
//	}
func (j Job) IsValid() bool {
	return j.Blob != "" && j.JobID != ""
}

// BlobWithFixedByte replaces the blob byte at position 39 with fixedByte.
//
//	partitioned := job.BlobWithFixedByte(0x2A)
func (j Job) BlobWithFixedByte(fixedByte uint8) string {
	if len(j.Blob) < 80 {
		return j.Blob
	}
	blob := []byte(j.Blob)
	encoded := make([]byte, 2)
	hex.Encode(encoded, []byte{fixedByte})
	blob[78] = encoded[0]
	blob[79] = encoded[1]
	return string(blob)
}

// DifficultyFromTarget converts the 8-char little-endian target into a difficulty.
//
//	diff := job.DifficultyFromTarget()
func (j Job) DifficultyFromTarget() uint64 {
	if len(j.Target) != 8 {
		return 0
	}
	raw, err := hex.DecodeString(j.Target)
	if err != nil || len(raw) != 4 {
		return 0
	}
	target := uint32(raw[0]) | uint32(raw[1])<<8 | uint32(raw[2])<<16 | uint32(raw[3])<<24
	if target == 0 {
		return 0
	}
	return uint64(math.MaxUint32) / uint64(target)
}

// targetFromDifficulty converts a difficulty into the 8-char little-endian hex target.
//
//	target := targetFromDifficulty(10000) // "b88d0600"
func targetFromDifficulty(diff uint64) string {
	if diff <= 1 {
		return "ffffffff"
	}
	maxTarget := uint64(math.MaxUint32)
	target := (maxTarget + diff - 1) / diff
	if target == 0 {
		target = 1
	}
	if target > maxTarget {
		target = maxTarget
	}
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], uint32(target))
	return hex.EncodeToString(raw[:])
}

// EffectiveShareDifficulty returns the share difficulty capped by the miner's custom diff.
// If no custom diff is set or the pool diff is already lower, the pool diff is returned.
//
//	diff := proxy.EffectiveShareDifficulty(job, miner) // 25000 when customDiff < poolDiff
func EffectiveShareDifficulty(job Job, miner *Miner) uint64 {
	diff := job.DifficultyFromTarget()
	if miner == nil {
		return diff
	}
	miner.mu.RLock()
	customDiff := miner.customDiff
	miner.mu.RUnlock()
	if customDiff == 0 || diff == 0 || diff <= customDiff {
		return diff
	}
	return customDiff
}

// NewCustomDiff creates a login-time custom difficulty resolver.
//
//	resolver := proxy.NewCustomDiff(50000)
//	resolver.OnLogin(proxy.Event{Miner: miner})
func NewCustomDiff(globalDiff uint64) *CustomDiff {
	cd := &CustomDiff{}
	cd.globalDiff.Store(globalDiff)
	return cd
}

// OnLogin normalises the login user once during handshake.
//
//	cd.OnLogin(proxy.Event{Miner: &proxy.Miner{user: "WALLET+50000"}})
func (cd *CustomDiff) OnLogin(e Event) {
	if cd == nil || e.Miner == nil {
		return
	}
	if e.Miner.customDiffResolved {
		return
	}
	resolved := resolveLoginCustomDiff(e.Miner.user, cd.globalDiff.Load())
	e.Miner.user = resolved.user
	e.Miner.customDiff = resolved.diff
	e.Miner.customDiffFromLogin = resolved.fromLogin
	e.Miner.customDiffResolved = true
}

// limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 30, BanDurationSeconds: 300})
//
//	if limiter.Allow("203.0.113.42:3333") {
//	    // first 30 connection attempts per minute are allowed
//	}
func NewRateLimiter(config RateLimit) *RateLimiter {
	return &RateLimiter{
		limit:          config,
		bucketByHost:   make(map[string]*tokenBucket),
		banUntilByHost: make(map[string]time.Time),
	}
}

//	if limiter.Allow("203.0.113.42:3333") {
//	    // hostOnly("203.0.113.42:3333") == "203.0.113.42"
//	}
func (rl *RateLimiter) Allow(ip string) bool {
	if rl == nil || rl.limit.MaxConnectionsPerMinute <= 0 {
		return true
	}
	host := hostOnly(ip)
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if until, banned := rl.banUntilByHost[host]; banned {
		if now.Before(until) {
			return false
		}
		delete(rl.banUntilByHost, host)
	}

	bucket, ok := rl.bucketByHost[host]
	if !ok {
		bucket = &tokenBucket{tokens: rl.limit.MaxConnectionsPerMinute, lastRefill: now}
		rl.bucketByHost[host] = bucket
	}

	refillBucket(bucket, rl.limit.MaxConnectionsPerMinute, now)
	if bucket.tokens <= 0 {
		if rl.limit.BanDurationSeconds > 0 {
			rl.banUntilByHost[host] = now.Add(time.Duration(rl.limit.BanDurationSeconds) * time.Second)
		}
		return false
	}

	bucket.tokens--
	bucket.lastRefill = now
	if bucket.tokens == 0 && rl.limit.BanDurationSeconds > 0 {
		rl.banUntilByHost[host] = now.Add(time.Duration(rl.limit.BanDurationSeconds) * time.Second)
	}
	return true
}

// Tick removes expired ban entries and refills token buckets.
//
//	limiter.Tick()
func (rl *RateLimiter) Tick() {
	if rl == nil || rl.limit.MaxConnectionsPerMinute <= 0 {
		return
	}
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	for host, until := range rl.banUntilByHost {
		if !now.Before(until) {
			delete(rl.banUntilByHost, host)
		}
	}
	for _, bucket := range rl.bucketByHost {
		refillBucket(bucket, rl.limit.MaxConnectionsPerMinute, now)
	}
}

// UpdateConfig replaces the active rate-limit policy without swapping the limiter pointer.
//
//	rl.UpdateConfig(proxy.RateLimit{MaxConnectionsPerMinute: 30, BanDurationSeconds: 300})
func (rl *RateLimiter) UpdateConfig(config RateLimit) {
	if rl == nil {
		return
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limit = config
	rl.bucketByHost = make(map[string]*tokenBucket)
	rl.banUntilByHost = make(map[string]time.Time)
}

//	watcher := proxy.NewConfigWatcher("config.json", func(cfg *proxy.Config) {
//	    p.Reload(cfg)
//	})
//
// watcher.Start() // polls once per second and reloads after the file mtime changes
func NewConfigWatcher(configPath string, onChange func(*Config)) *ConfigWatcher {
	watcher := &ConfigWatcher{
		configPath:     configPath,
		onConfigChange: onChange,
		stopCh:         make(chan struct{}),
	}
	if infoResult := proxyFileSystem().Stat(configPath); infoResult.OK {
		if info, ok := infoResult.Value.(interface{ ModTime() time.Time }); ok {
			watcher.lastModifiedAt = info.ModTime()
		}
	}
	return watcher
}

// watcher.Start()
func (w *ConfigWatcher) Start() {
	if w == nil || w.configPath == "" || w.onConfigChange == nil {
		return
	}
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return
	}
	if w.stopCh == nil {
		w.stopCh = make(chan struct{})
	} else {
		select {
		case <-w.stopCh:
			w.stopCh = make(chan struct{})
		default:
		}
	}
	stopCh := w.stopCh
	configPath := w.configPath
	onConfigChange := w.onConfigChange
	w.started = true
	w.mu.Unlock()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if infoResult := proxyFileSystem().Stat(configPath); infoResult.OK {
					info, ok := infoResult.Value.(interface{ ModTime() time.Time })
					if !ok {
						continue
					}
					w.mu.Lock()
					changed := info.ModTime() != w.lastModifiedAt
					if changed {
						w.lastModifiedAt = info.ModTime()
					}
					w.mu.Unlock()
					if !changed {
						continue
					}
					config, result := LoadConfig(configPath)
					if result.OK && config != nil {
						onConfigChange(config)
					}
				}
			case <-stopCh:
				return
			}
		}
	}()
}

// watcher.Stop()
func (w *ConfigWatcher) Stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	stopCh := w.stopCh
	w.started = false
	w.mu.Unlock()
	if stopCh == nil {
		return
	}
	select {
	case <-stopCh:
	default:
		close(stopCh)
	}
}

func hostOnly(ip string) string {
	host, _, err := net.SplitHostPort(ip)
	if err == nil {
		return host
	}
	return ip
}

func refillBucket(bucket *tokenBucket, limit int, now time.Time) {
	if bucket == nil || limit <= 0 {
		return
	}
	if bucket.lastRefill.IsZero() {
		bucket.lastRefill = now
		if bucket.tokens <= 0 {
			bucket.tokens = limit
		}
		return
	}
	interval := time.Duration(time.Minute) / time.Duration(limit)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	elapsed := now.Sub(bucket.lastRefill)
	if elapsed < interval {
		return
	}
	add := int(elapsed / interval)
	bucket.tokens += add
	if bucket.tokens > limit {
		bucket.tokens = limit
	}
	bucket.lastRefill = bucket.lastRefill.Add(time.Duration(add) * interval)
}

func generateUUID() string {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
