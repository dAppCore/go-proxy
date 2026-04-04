package proxy

import (
	"crypto/rand"
	"crypto/sha256"
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

func successResult() Result {
	return Result{OK: true}
}

func errorResult(err error) Result {
	return Result{OK: false, Error: err}
}

var splitterRegistryMu sync.RWMutex
var splitterRegistry = map[string]func(*Config, *EventBus) Splitter{}

// RegisterSplitterFactory registers a mode-specific splitter constructor.
// Packages such as splitter/nicehash and splitter/simple call this from init.
func RegisterSplitterFactory(mode string, factory func(*Config, *EventBus) Splitter) {
	splitterRegistryMu.Lock()
	defer splitterRegistryMu.Unlock()
	splitterRegistry[strings.ToLower(mode)] = factory
}

func getSplitterFactory(mode string) (func(*Config, *EventBus) Splitter, bool) {
	splitterRegistryMu.RLock()
	defer splitterRegistryMu.RUnlock()
	factory, ok := splitterRegistry[strings.ToLower(mode)]
	return factory, ok
}

// LoadConfig reads and unmarshals a JSON config file.
func LoadConfig(path string) (*Config, Result) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errorResult(err)
	}

	cfg := &Config{}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, errorResult(err)
	}
	cfg.sourcePath = path
	return cfg, cfg.Validate()
}

// Validate checks that mandatory bind and pool settings are present.
func (c *Config) Validate() Result {
	if c == nil {
		return errorResult(errors.New("config is nil"))
	}
	if !isValidMode(c.Mode) {
		return errorResult(errors.New("mode must be \"nicehash\" or \"simple\""))
	}
	if !isValidWorkersMode(c.Workers) {
		return errorResult(errors.New("workers must be one of \"rig-id\", \"user\", \"password\", \"agent\", \"ip\", or \"false\""))
	}
	if len(c.Bind) == 0 {
		return errorResult(errors.New("bind list is empty"))
	}
	if len(c.Pools) == 0 {
		return errorResult(errors.New("pool list is empty"))
	}
	for _, pool := range c.Pools {
		if pool.Enabled && strings.TrimSpace(pool.URL) == "" {
			return errorResult(errors.New("enabled pool url is empty"))
		}
	}
	return successResult()
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

// NewEventBus creates an empty synchronous event dispatcher.
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
		handler(e)
	}
}

// IsValid returns true when the job contains a blob and job id.
func (j Job) IsValid() bool {
	return j.Blob != "" && j.JobID != ""
}

// BlobWithFixedByte replaces the blob byte at position 39 with fixedByte.
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
	return uint64(math.MaxUint32 / uint64(target))
}

// NewCustomDiff creates a login-time custom difficulty resolver.
func NewCustomDiff(globalDiff uint64) *CustomDiff {
	return &CustomDiff{globalDiff: globalDiff}
}

// OnLogin parses +N suffixes and applies global difficulty fallbacks.
func (cd *CustomDiff) OnLogin(e Event) {
	if cd == nil || e.Miner == nil {
		return
	}
	e.Miner.user, e.Miner.customDiff = parseLoginUser(e.Miner.user, cd.globalDiff)
}

// NewRateLimiter creates a per-IP token bucket limiter.
func NewRateLimiter(cfg RateLimit) *RateLimiter {
	return &RateLimiter{
		cfg:     cfg,
		buckets: make(map[string]*tokenBucket),
		banned:  make(map[string]time.Time),
	}
}

// Allow returns true if the IP address is permitted to open a new connection.
func (rl *RateLimiter) Allow(ip string) bool {
	if rl == nil || rl.cfg.MaxConnectionsPerMinute <= 0 {
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
		bucket = &tokenBucket{tokens: rl.cfg.MaxConnectionsPerMinute, lastRefill: now}
		rl.buckets[host] = bucket
	}

	refillBucket(bucket, rl.cfg.MaxConnectionsPerMinute, now)
	if bucket.tokens <= 0 {
		if rl.cfg.BanDurationSeconds > 0 {
			rl.banned[host] = now.Add(time.Duration(rl.cfg.BanDurationSeconds) * time.Second)
		}
		return false
	}

	bucket.tokens--
	bucket.lastRefill = now
	return true
}

// Tick removes expired ban entries and refills token buckets.
func (rl *RateLimiter) Tick() {
	if rl == nil || rl.cfg.MaxConnectionsPerMinute <= 0 {
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
		refillBucket(bucket, rl.cfg.MaxConnectionsPerMinute, now)
	}
}

// NewConfigWatcher creates a polling watcher for a config file.
//
//	watcher := proxy.NewConfigWatcher("config.json", func(cfg *proxy.Config) {
//		proxyInstance.Reload(cfg)
//	})
func NewConfigWatcher(path string, onChange func(*Config)) *ConfigWatcher {
	return &ConfigWatcher{
		path:     path,
		onChange: onChange,
		done:     make(chan struct{}),
	}
}

// Start begins the 1-second polling loop.
//
//	watcher.Start()
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
					cfg, result := LoadConfig(w.path)
					if result.OK && cfg != nil {
						w.onChange(cfg)
					}
				}
			case <-w.done:
				return
			}
		}
	}()
}

// Stop ends the watcher goroutine.
//
//	watcher.Stop()
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
