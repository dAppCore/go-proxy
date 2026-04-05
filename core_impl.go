package proxy

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Result is a small success/error carrier used by constructors and loaders.
type Result struct {
	OK    bool
	Error error
}

func newSuccessResult() Result {
	return Result{OK: true}
}

func newErrorResult(err error) Result {
	return Result{OK: false, Error: err}
}

var splitterFactoriesMu sync.RWMutex
var splitterFactoriesByMode = map[string]func(*Config, *EventBus) Splitter{}

// Register a mode-specific splitter constructor.
//
//	proxy.RegisterSplitterFactory("simple", func(cfg *proxy.Config, bus *proxy.EventBus) proxy.Splitter {
//	    return simple.NewSimpleSplitter(cfg, bus, nil)
//	})
func RegisterSplitterFactory(mode string, factory func(*Config, *EventBus) Splitter) {
	splitterFactoriesMu.Lock()
	defer splitterFactoriesMu.Unlock()
	splitterFactoriesByMode[strings.ToLower(mode)] = factory
}

func splitterFactoryForMode(mode string) (func(*Config, *EventBus) Splitter, bool) {
	splitterFactoriesMu.RLock()
	defer splitterFactoriesMu.RUnlock()
	factory, ok := splitterFactoriesByMode[strings.ToLower(mode)]
	return factory, ok
}

// cfg, result := proxy.LoadConfig("/etc/proxy.json")
// if !result.OK { return result.Error }
func LoadConfig(path string) (*Config, Result) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, newErrorResult(err)
	}

	config := &Config{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, newErrorResult(err)
	}
	config.configPath = path
	return config, config.Validate()
}

// cfg := &proxy.Config{Mode: "nicehash", Bind: []proxy.BindAddr{{Host: "0.0.0.0", Port: 3333}}, Pools: []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}}, Workers: proxy.WorkersByRigID}
// if result := cfg.Validate(); !result.OK { return result }
func (c *Config) Validate() Result {
	if c == nil {
		return newErrorResult(errors.New("config is nil"))
	}
	if !isValidMode(c.Mode) {
		return newErrorResult(errors.New("mode must be \"nicehash\" or \"simple\""))
	}
	if !isValidWorkersMode(c.Workers) {
		return newErrorResult(errors.New("workers must be one of \"rig-id\", \"user\", \"password\", \"agent\", \"ip\", or \"false\""))
	}
	if len(c.Bind) == 0 {
		return newErrorResult(errors.New("bind list is empty"))
	}
	if len(c.Pools) == 0 {
		return newErrorResult(errors.New("pool list is empty"))
	}
	for _, pool := range c.Pools {
		if pool.Enabled && strings.TrimSpace(pool.URL) == "" {
			return newErrorResult(errors.New("enabled pool url is empty"))
		}
	}
	return newSuccessResult()
}

func isValidMode(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "nicehash", "simple":
		return true
	default:
		return false
	}
}

func isValidWorkersMode(mode WorkersMode) bool {
	switch mode {
	case WorkersByRigID, WorkersByUser, WorkersByPass, WorkersByAgent, WorkersByIP, WorkersDisabled:
		return true
	default:
		return false
	}
}

// bus := proxy.NewEventBus()
// bus.Subscribe(proxy.EventLogin, func(e proxy.Event) { _ = e.Miner })
func NewEventBus() *EventBus {
	return &EventBus{listeners: make(map[EventType][]EventHandler)}
}

// Subscribe registers a handler for the given event type.
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

// Dispatch calls all registered handlers for the event's type.
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

// DifficultyFromTarget converts the target to a rough integer difficulty.
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
	if target == math.MaxUint32 {
		return 1
	}
	return uint64((uint64(math.MaxUint32) * 10) / uint64(target))
}

func targetFromDifficulty(diff uint64) string {
	if diff <= 1 {
		return "ffffffff"
	}
	maxTarget := uint64(math.MaxUint32)
	target := (maxTarget*10 + diff - 1) / diff
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

func EffectiveShareDifficulty(job Job, miner *Miner) uint64 {
	diff := job.DifficultyFromTarget()
	if miner == nil || miner.customDiff == 0 || diff == 0 || diff <= miner.customDiff {
		return diff
	}
	return miner.customDiff
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
	e.Miner.user, e.Miner.customDiff = parseLoginUser(e.Miner.user, cd.globalDiff.Load())
	e.Miner.customDiffResolved = true
}

// limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 30, BanDurationSeconds: 300})
// if limiter.Allow("203.0.113.42:3333") { /* accept the socket */ }
func NewRateLimiter(config RateLimit) *RateLimiter {
	return &RateLimiter{
		config:  config,
		buckets: make(map[string]*tokenBucket),
		banned:  make(map[string]time.Time),
	}
}

// if limiter.Allow("203.0.113.42:3333") { /* accept the socket */ }
func (rl *RateLimiter) Allow(ip string) bool {
	if rl == nil || rl.config.MaxConnectionsPerMinute <= 0 {
		return true
	}
	host := hostOnly(ip)
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if until, banned := rl.banned[host]; banned {
		if now.Before(until) {
			return false
		}
		delete(rl.banned, host)
	}

	bucket, ok := rl.buckets[host]
	if !ok {
		bucket = &tokenBucket{tokens: rl.config.MaxConnectionsPerMinute, lastRefill: now}
		rl.buckets[host] = bucket
	}

	refillBucket(bucket, rl.config.MaxConnectionsPerMinute, now)
	if bucket.tokens <= 0 {
		if rl.config.BanDurationSeconds > 0 {
			rl.banned[host] = now.Add(time.Duration(rl.config.BanDurationSeconds) * time.Second)
		}
		return false
	}

	bucket.tokens--
	bucket.lastRefill = now
	return true
}

// Tick removes expired ban entries and refills token buckets.
//
//	limiter.Tick()
func (rl *RateLimiter) Tick() {
	if rl == nil || rl.config.MaxConnectionsPerMinute <= 0 {
		return
	}
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	for host, until := range rl.banned {
		if !now.Before(until) {
			delete(rl.banned, host)
		}
	}
	for _, bucket := range rl.buckets {
		refillBucket(bucket, rl.config.MaxConnectionsPerMinute, now)
	}
}

// watcher := proxy.NewConfigWatcher("config.json", func(cfg *proxy.Config) { p.Reload(cfg) })
// watcher.Start()
func NewConfigWatcher(configPath string, onChange func(*Config)) *ConfigWatcher {
	watcher := &ConfigWatcher{
		path:     configPath,
		onChange: onChange,
		done:     make(chan struct{}),
	}
	if info, err := os.Stat(configPath); err == nil {
		watcher.lastMod = info.ModTime()
	}
	return watcher
}

// watcher.Start()
func (w *ConfigWatcher) Start() {
	if w == nil || w.path == "" || w.onChange == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				info, err := os.Stat(w.path)
				if err != nil {
					continue
				}
				mod := info.ModTime()
				if mod.After(w.lastMod) {
					w.lastMod = mod
					config, result := LoadConfig(w.path)
					if result.OK && config != nil {
						w.onChange(config)
					}
				}
			case <-w.done:
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
	select {
	case <-w.done:
	default:
		close(w.done)
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
