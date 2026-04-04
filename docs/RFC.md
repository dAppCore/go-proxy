---
module: dappco.re/go/core/proxy
repo: core/go-proxy
lang: go
tier: lib
depends:
  - code/core/go
  - code/core/go/api
tags:
  - stratum
  - mining
  - proxy
  - nicehash
  - tcp
  - tls
---
# go-proxy RFC — Stratum Mining Proxy

> An agent should be able to implement this library from this document alone.

**Module:** `dappco.re/go/core/proxy`
**Repository:** `core/go-proxy`
**Files:** 18

---

## 1. Overview

go-proxy is a CryptoNote stratum protocol proxy library. It accepts miner connections over TCP (optionally TLS), splits the 32-bit nonce space across up to 256 simultaneous miners per upstream pool connection (NiceHash mode), and presents a small monitoring API.

The v1 scope covers:
- NiceHash nonce-splitting mode (256-slot table, one fixed byte per miner)
- Simple passthrough mode (one upstream per miner group, reuse on disconnect)
- Stratum JSON-RPC over TCP: login, job, submit, keepalived
- Algorithm negotiation extension (algo field in login + job)
- RigID extension (rigid field in login)
- Pool-side stratum client with primary/failover strategy
- TLS for inbound (miners) and outbound (pool)
- HTTP monitoring API: GET /1/summary, /1/workers, /1/miners
- Per-worker and per-miner stats with rolling hashrate windows (60s, 600s, 3600s, 12h, 24h)
- Access log and share log (append-only line files)
- JSON config file with hot-reload via filesystem watcher
- Connection rate limiting per IP (token bucket, configurable)
- Access password (applied to login params.pass)
- Custom difficulty override per-user (from login user suffix or global setting)

---

## 1.1 Import Graph (no circular imports)

Shared types (`Job`, `PoolConfig`, `Config`, `Miner`, `UpstreamStats`, event types) are defined in the root `proxy` package. Sub-packages import `proxy` but `proxy` never imports sub-packages directly — it uses interfaces (`Splitter`, `ShareSink`) injected at construction time.

```
proxy (root)        ← defines shared types, Splitter interface, Proxy orchestrator
  ├── pool          ← imports proxy (for Job, PoolConfig). proxy does NOT import pool.
  ├── nicehash      ← imports proxy (for Miner, Job, events) and pool (for Strategy)
  ├── simple        ← imports proxy and pool
  ├── log           ← imports proxy (for Event)
  └── api           ← imports proxy (for Proxy) and core/api
```

The `Proxy` orchestrator wires sub-packages via interface injection:
```go
// proxy.go receives a Splitter (implemented by nicehash or simple)
// and a pool.StrategyFactory (closure that creates pool.Strategy instances).
// No import of nicehash, simple, or pool packages from proxy.go.
```

---

## 2. File Map

| File | Package | Purpose |
|------|---------|---------|
| `proxy.go` | `proxy` | `Proxy` orchestrator — owns tick loop, listeners, splitter, stats |
| `config.go` | `proxy` | `Config` struct, JSON unmarshal, hot-reload watcher |
| `server.go` | `proxy` | TCP server — accepts connections, applies rate limiter |
| `miner.go` | `proxy` | `Miner` state machine — one per connection |
| `job.go` | `proxy` | `Job` value type — blob, job_id, target, algo, height |
| `worker.go` | `proxy` | `Worker` aggregate — rolling hashrate, share counts |
| `stats.go` | `proxy` | `Stats` aggregate — global counters, hashrate windows |
| `events.go` | `proxy` | Event bus — LoginEvent, AcceptEvent, SubmitEvent, CloseEvent |
| `splitter/nicehash/splitter.go` | `nicehash` | NonceSplitter — owns mapper pool, routes miners |
| `splitter/nicehash/mapper.go` | `nicehash` | NonceMapper — one upstream connection, owns NonceStorage |
| `splitter/nicehash/storage.go` | `nicehash` | NonceStorage — 256-slot table, fixed-byte allocation |
| `splitter/simple/splitter.go` | `simple` | SimpleSplitter — passthrough, upstream reuse pool |
| `splitter/simple/mapper.go` | `simple` | SimpleMapper — one upstream per miner group |
| `pool/client.go` | `pool` | StratumClient — outbound pool TCP/TLS connection |
| `pool/strategy.go` | `pool` | FailoverStrategy — primary + ordered fallbacks |
| `log/access.go` | `log` | AccessLog — connection open/close lines |
| `log/share.go` | `log` | ShareLog — accept/reject lines per share |
| `api/router.go` | `api` | HTTP handlers — /1/summary, /1/workers, /1/miners |

---

## 3. Data Flow

```
Miners (TCP) → Server.accept()
                 → ratelimit check (per-IP token bucket)
                 → Miner.handleLogin()
                      → Events.Dispatch(LoginEvent)
                           → CustomDiff.Apply(miner)     (sets miner.customDiff)
                           → Workers.OnLogin(event)      (upsert worker record)
                           → Splitter.OnLogin(event)     (assigns mapper slot)
                                → NonceMapper.Add(miner)
                                     → NonceStorage.Add(miner) → miner.FixedByte = slot
                                → if mapper active: miner.ForwardJob(currentJob)

Pool (TCP) → pool.StratumClient read loop → OnJob(job)
               → NonceMapper.onJob(job)
                    → NonceStorage.SetJob(job)
                         → for each active slot: miner.ForwardJob(job)
                              → Miner sends JSON over TCP

Miner submit → Miner.handleSubmit()
                 → Events.Dispatch(SubmitEvent)
                      → Splitter.OnSubmit(event)
                           → NonceMapper.Submit(event)
                                → SubmitContext stored by sequence
                                → pool.StratumClient.Submit(jobID, nonce, result, algo)
                                     → pool reply → OnResultAccepted(seq, ok, err)
                                          → Events.Dispatch(AcceptEvent | RejectEvent)
                                               → Workers.OnAccept / OnReject
                                               → Stats.OnAccept / OnReject
                                               → ShareLog.OnAccept / OnReject
                                               → Miner.Success or Miner.ReplyWithError
```

---

## 4. Config

```go
// Config is the top-level proxy configuration, loaded from JSON and hot-reloaded on change.
//
//   cfg, result := proxy.LoadConfig("config.json")
//   if !result.OK { log.Fatal(result.Error) }
type Config struct {
    Mode             string       `json:"mode"`               // "nicehash" or "simple"
    Bind             []BindAddr   `json:"bind"`               // listen addresses
    Pools            []PoolConfig `json:"pools"`              // ordered primary + fallbacks
    TLS              TLSConfig    `json:"tls"`                // inbound TLS (miner-facing)
    HTTP             HTTPConfig   `json:"http"`               // monitoring API
    AccessPassword   string       `json:"access-password"`    // "" = no auth required
    CustomDiff       uint64       `json:"custom-diff"`        // 0 = disabled
    CustomDiffStats  bool         `json:"custom-diff-stats"`  // report per custom-diff bucket
    AlgoExtension    bool         `json:"algo-ext"`           // forward algo field in jobs
    Workers          WorkersMode  `json:"workers"`            // "rig-id", "user", "password", "agent", "ip", "false"
    AccessLogFile    string       `json:"access-log-file"`    // "" = disabled
    ReuseTimeout     int          `json:"reuse-timeout"`      // seconds; simple mode upstream reuse
    Retries          int          `json:"retries"`            // pool reconnect attempts
    RetryPause       int          `json:"retry-pause"`        // seconds between retries
    Watch            bool         `json:"watch"`              // hot-reload on file change
    RateLimit        RateLimit    `json:"rate-limit"`         // per-IP connection rate limit
}

// BindAddr is one TCP listen endpoint.
//
//   proxy.BindAddr{Host: "0.0.0.0", Port: 3333, TLS: false}
type BindAddr struct {
    Host string `json:"host"`
    Port uint16 `json:"port"`
    TLS  bool   `json:"tls"`
}

// PoolConfig is one upstream pool entry.
//
//   proxy.PoolConfig{URL: "pool.lthn.io:3333", User: "WALLET", Pass: "x", Enabled: true}
type PoolConfig struct {
    URL            string `json:"url"`
    User           string `json:"user"`
    Pass           string `json:"pass"`
    RigID          string `json:"rig-id"`
    Algo           string `json:"algo"`
    TLS            bool   `json:"tls"`
    TLSFingerprint string `json:"tls-fingerprint"` // SHA-256 hex; "" = skip pin
    Keepalive      bool   `json:"keepalive"`
    Enabled        bool   `json:"enabled"`
}

// TLSConfig controls inbound TLS on bind addresses that have TLS: true.
//
//   proxy.TLSConfig{Enabled: true, CertFile: "/etc/proxy/cert.pem", KeyFile: "/etc/proxy/key.pem"}
type TLSConfig struct {
    Enabled  bool   `json:"enabled"`
    CertFile string `json:"cert"`
    KeyFile  string `json:"cert_key"`
    Ciphers  string `json:"ciphers"`    // OpenSSL cipher string; "" = default
    Protocols string `json:"protocols"` // TLS version string; "" = default
}

// HTTPConfig controls the monitoring API server.
//
//   proxy.HTTPConfig{Enabled: true, Host: "127.0.0.1", Port: 8080, Restricted: true}
type HTTPConfig struct {
    Enabled     bool   `json:"enabled"`
    Host        string `json:"host"`
    Port        uint16 `json:"port"`
    AccessToken string `json:"access-token"` // Bearer token; "" = no auth
    Restricted  bool   `json:"restricted"`   // true = read-only GET only
}

// RateLimit controls per-IP connection rate limiting using a token bucket.
//
//   proxy.RateLimit{MaxConnectionsPerMinute: 30, BanDurationSeconds: 300}
type RateLimit struct {
    MaxConnectionsPerMinute int `json:"max-connections-per-minute"` // 0 = disabled
    BanDurationSeconds      int `json:"ban-duration"`               // 0 = no ban
}

// WorkersMode controls which login field becomes the worker name.
type WorkersMode string

const (
    WorkersByRigID  WorkersMode = "rig-id"   // rigid field, fallback to user
    WorkersByUser   WorkersMode = "user"
    WorkersByPass   WorkersMode = "password"
    WorkersByAgent  WorkersMode = "agent"
    WorkersByIP     WorkersMode = "ip"
    WorkersDisabled WorkersMode = "false"
)

// LoadConfig reads and unmarshals a JSON config file. Returns core.E on I/O or parse error.
//
//   cfg, result := proxy.LoadConfig("config.json")
func LoadConfig(path string) (*Config, core.Result) {}

// Validate checks required fields. Returns core.E if pool list or bind list is empty,
// or if any enabled pool has an empty URL.
//
//   if result := cfg.Validate(); !result.OK { return result }
func (c *Config) Validate() core.Result {}
```

---

## 5. Proxy Orchestrator

```go
// Proxy is the top-level orchestrator. It owns the server, splitter, stats, workers,
// event bus, tick goroutine, and optional HTTP API.
//
//   p, result := proxy.New(cfg)
//   if result.OK { p.Start() }
type Proxy struct {
    config   *Config
    splitter Splitter
    stats    *Stats
    workers  *Workers
    events   *EventBus
    servers  []*Server
    ticker   *time.Ticker
    watcher  *ConfigWatcher
    done     chan struct{}
}

// New creates and wires all subsystems but does not start the tick loop or TCP listeners.
//
//   p, result := proxy.New(cfg)
func New(cfg *Config) (*Proxy, core.Result) {}

// Start begins the TCP listener(s), pool connections, tick loop, and (if configured) HTTP API.
// Blocks until Stop() is called.
//
//   p.Start()
func (p *Proxy) Start() {}

// Stop shuts down all subsystems cleanly. Waits up to 5 seconds for in-flight submits to drain.
//
//   p.Stop()
func (p *Proxy) Stop() {}

// Reload replaces the live config. Hot-reloads pool list and custom diff.
// Cannot change bind addresses at runtime (ignored if changed).
//
//   p.Reload(newCfg)
func (p *Proxy) Reload(cfg *Config) {}

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
```

---

## 6. Miner Connection State Machine

Each accepted TCP connection is represented by a `Miner`. State transitions are linear:

```
WaitLogin → WaitReady → Ready → Closing
```

- `WaitLogin`: connection open, awaiting `login` request. 10-second timeout.
- `WaitReady`: login validated, awaiting upstream pool job. 600-second timeout.
- `Ready`: receiving jobs, accepting submit requests. 600-second inactivity timeout reset on each job or submit.
- `Closing`: TCP close in progress.

```go
// MinerState represents the lifecycle state of one miner connection.
type MinerState int

const (
    MinerStateWaitLogin MinerState = iota
    MinerStateWaitReady
    MinerStateReady
    MinerStateClosing
)

// Miner is the state machine for one miner TCP connection.
//
//   // created by Server on accept:
//   m := proxy.NewMiner(conn, 3333, nil)
//   m.Start()
type Miner struct {
    id         int64      // monotonically increasing per-process; atomic assignment
    rpcID      string     // UUID v4 sent to miner as session id
    state      MinerState
    extAlgo    bool       // miner sent algo list in login params
    extNH      bool       // NiceHash mode active (fixed byte splitting)
    ip         string     // remote IP (without port, for logging)
    localPort  uint16
    user       string     // login params.login (wallet address), custom diff suffix stripped
    password   string     // login params.pass
    agent      string     // login params.agent
    rigID      string     // login params.rigid (optional extension)
    fixedByte  uint8      // NiceHash slot index (0-255)
    mapperID   int64      // which NonceMapper owns this miner; -1 = unassigned
    routeID    int64      // SimpleMapper ID in simple mode; -1 = unassigned
    customDiff uint64     // 0 = use pool diff; non-zero = cap diff to this value
    diff       uint64     // last difficulty sent to this miner from the pool
    rx         uint64     // bytes received from miner
    tx         uint64     // bytes sent to miner
    connectedAt time.Time
    lastActivityAt time.Time
    conn       net.Conn
    tlsConn    *tls.Conn  // nil if plain TCP
    sendMu     sync.Mutex // serialises writes to conn
    buf        [16384]byte // per-miner send buffer; avoids per-write allocations
}

// SetID assigns the miner's internal ID. Used by NonceStorage tests.
//
//   m.SetID(42)
func (m *Miner) SetID(id int64) {}

// FixedByte returns the NiceHash slot index assigned to this miner.
//
//   slot := m.FixedByte() // 0x2A
func (m *Miner) FixedByte() uint8 {}

// NewMiner creates a Miner for an accepted net.Conn. Does not start reading yet.
//
//   m := proxy.NewMiner(conn, 3333, nil)
//   m := proxy.NewMiner(tlsConn, 3443, tlsCfg) // TLS variant
func NewMiner(conn net.Conn, localPort uint16, tlsCfg *tls.Config) *Miner {}

// Start begins the read loop in a goroutine and arms the login timeout timer.
//
//   m.Start()
func (m *Miner) Start() {}

// ForwardJob encodes the job as a stratum `job` notification and writes it to the miner.
// In NiceHash mode, byte 39 of the blob hex (chars 78-79) is replaced with FixedByte.
//
//   m.ForwardJob(job, "cn/r")
func (m *Miner) ForwardJob(job Job, algo string) {}

// ReplyWithError sends a JSON-RPC error response for the given request id.
//
//   m.ReplyWithError(requestID, "Low difficulty share")
func (m *Miner) ReplyWithError(id int64, message string) {}

// Success sends a JSON-RPC success response with the given status string.
//
//   m.Success(requestID, "OK")
func (m *Miner) Success(id int64, status string) {}

// Close initiates graceful TCP shutdown. Safe to call multiple times.
//
//   m.Close()
func (m *Miner) Close() {}
```

### 6.1 Login Parsing

On login request arrival:

1. Reject if `params.login` is empty: `"Invalid payment address provided"`.
2. If `Config.AccessPassword != ""`, compare to `params.pass`; reject if mismatch: `"Invalid password"`.
3. Parse custom difficulty suffix: if `params.login` ends with `+{number}` (e.g. `WALLET+50000`), strip the suffix, set `miner.customDiff = number`. If no suffix and `Config.CustomDiff > 0`, set `miner.customDiff = Config.CustomDiff`.
4. Store `params.rigid` as `miner.rigID` if present.
5. Store `params.algo` list; set `miner.extAlgo = true` if list is non-empty.
6. Assign `miner.rpcID` (UUID v4).
7. Dispatch `LoginEvent`.
8. Transition to `WaitReady`.
9. If a job is already available from upstream, send it immediately and transition to `Ready`.

### 6.2 Submit Handling

On submit request arrival (state must be `Ready`):

1. Validate `params.id == miner.rpcID`; reject otherwise: `"Unauthenticated"`.
2. Validate `params.job_id` is non-empty; reject otherwise: `"Missing job id"`.
3. Validate `params.nonce` is exactly 8 lowercase hex characters; reject otherwise: `"Invalid nonce"`.
4. Dispatch `SubmitEvent`. The splitter performs upstream forwarding.

### 6.3 Keepalived Handling

On `keepalived` request arrival:

1. Reset `lastActivityAt` to now.
2. Reply `{"result": {"status": "KEEPALIVED"}}`.

---

## 7. Job Value Type

```go
// Job holds the current work unit received from a pool. Immutable once assigned.
//
//   j := proxy.Job{
//       Blob:   "0707d5ef...b01",
//       JobID:  "4BiGm3/RgGQzgkTI",
//       Target: "b88d0600",
//       Algo:   "cn/r",
//   }
type Job struct {
    Blob      string // hex-encoded block template (160 hex chars = 80 bytes)
    JobID     string // pool-assigned identifier
    Target    string // 8-char hex little-endian uint32 difficulty target
    Algo      string // algorithm e.g. "cn/r", "rx/0"; "" if not negotiated
    Height    uint64 // block height (0 if pool did not provide)
    SeedHash  string // RandomX seed hash hex (empty if not RandomX)
    ClientID  string // pool session ID that issued this job (for stale detection)
}

// IsValid returns true if Blob and JobID are non-empty.
//
//   if !job.IsValid() { return }
func (j Job) IsValid() bool {}

// BlobWithFixedByte returns a copy of Blob with hex characters at positions 78-79
// (blob byte index 39) replaced by the two-digit lowercase hex of fixedByte.
// Returns the original blob unchanged if len(Blob) < 80.
//
//   partitioned := job.BlobWithFixedByte(0x2A)  // chars 78-79 become "2a"
func (j Job) BlobWithFixedByte(fixedByte uint8) string {}

// DifficultyFromTarget converts the 8-char little-endian hex Target field to a uint64 difficulty.
// Formula: difficulty = 0xFFFFFFFF / uint32(target_le)
//
//   diff := job.DifficultyFromTarget()  // "b88d0600" → ~100000
func (j Job) DifficultyFromTarget() uint64 {}
```

---

## 8. NiceHash Splitter

The NiceHash splitter partitions the 32-bit nonce space among miners by fixing one byte (byte 39 of the blob). Each upstream pool connection (NonceMapper) owns a 256-slot table. Up to 256 miners share one pool connection. The 257th miner triggers creation of a new NonceMapper with a new pool connection.

### 8.1 NonceSplitter

```go
// NonceSplitter is the Splitter implementation for NiceHash mode.
// It manages a dynamic slice of NonceMapper upstreams, creating new ones on demand.
//
//   s := nicehash.NewNonceSplitter(cfg, eventBus, strategyFactory)
//   s.Connect()
type NonceSplitter struct {
    mappers         []*NonceMapper
    cfg             *proxy.Config
    events          *proxy.EventBus
    strategyFactory pool.StrategyFactory
    mu              sync.RWMutex
}

func NewNonceSplitter(cfg *proxy.Config, events *proxy.EventBus, factory pool.StrategyFactory) *NonceSplitter {}

// OnLogin assigns the miner to the first NonceMapper with a free slot.
// If all existing mappers are full, a new NonceMapper is created (new pool connection).
// Sets miner.MapperID to identify which mapper owns this miner.
//
//   s.OnLogin(loginEvent)
func (s *NonceSplitter) OnLogin(event *proxy.LoginEvent) {}

// OnSubmit routes the submit to the NonceMapper identified by miner.MapperID.
//
//   s.OnSubmit(submitEvent)
func (s *NonceSplitter) OnSubmit(event *proxy.SubmitEvent) {}

// OnClose removes the miner from its mapper's slot.
//
//   s.OnClose(closeEvent)
func (s *NonceSplitter) OnClose(event *proxy.CloseEvent) {}

// GC removes NonceMappers that are empty and have been idle more than 60 seconds.
//
//   s.GC()  // called by Proxy tick loop every 60 ticks
func (s *NonceSplitter) GC() {}

// Connect establishes the first pool upstream connection via the strategy factory.
//
//   s.Connect()
func (s *NonceSplitter) Connect() {}

// Tick is called every second by the proxy tick loop. Runs keepalive on idle mappers.
//
//   s.Tick(ticks)
func (s *NonceSplitter) Tick(ticks uint64) {}

// Upstreams returns current upstream pool connection counts.
//
//   stats := s.Upstreams()
func (s *NonceSplitter) Upstreams() proxy.UpstreamStats {}
```

### 8.2 NonceMapper

```go
// NonceMapper manages one outbound pool connection and the 256-slot NonceStorage.
// It implements pool.StratumListener to receive job and result events from the pool.
//
//   m := nicehash.NewNonceMapper(id, cfg, strategy)
//   m.Start()
type NonceMapper struct {
    id        int64
    storage   *NonceStorage
    strategy  pool.Strategy       // manages pool client lifecycle and failover
    pending   map[int64]SubmitContext // sequence → {requestID, minerID}
    cfg       *proxy.Config
    active    bool                // true once pool has sent at least one job
    suspended int                 // > 0 when pool connection is in error/reconnecting
    mu        sync.Mutex
}

// SubmitContext tracks one in-flight share submission waiting for pool reply.
type SubmitContext struct {
    RequestID int64 // JSON-RPC id from the miner's submit request
    MinerID   int64 // miner that submitted
}

func NewNonceMapper(id int64, cfg *proxy.Config, strategy pool.Strategy) *NonceMapper {}

// Add assigns a miner to a free slot. Returns false if all 256 slots are occupied.
// Sets miner.FixedByte to the allocated slot index.
//
//   ok := mapper.Add(miner)
func (m *NonceMapper) Add(miner *proxy.Miner) bool {}

// Remove releases the miner's slot (marks it dead until next job clears it).
//
//   mapper.Remove(miner)
func (m *NonceMapper) Remove(miner *proxy.Miner) {}

// Submit forwards a share to the pool. Stores a SubmitContext keyed by the pool sequence number
// so the pool reply can be correlated back to the originating miner.
//
//   mapper.Submit(submitEvent)
func (m *NonceMapper) Submit(event *proxy.SubmitEvent) {}

// IsActive returns true when the pool has delivered at least one valid job.
//
//   if mapper.IsActive() { /* safe to assign miners */ }
func (m *NonceMapper) IsActive() bool {}

// Start connects the pool strategy. Called by NonceSplitter after creating the mapper.
//
//   mapper.Start()
func (m *NonceMapper) Start() {}

// OnJob receives a new job from the pool. Implements pool.StratumListener.
// Calls storage.SetJob to distribute to all active miners.
//
//   // called by pool.StratumClient when pool pushes a job
func (m *NonceMapper) OnJob(job proxy.Job) {}

// OnResultAccepted receives a share result from the pool. Implements pool.StratumListener.
// Correlates by sequence to the originating miner and sends success/error reply.
//
//   // called by pool.StratumClient on pool reply
func (m *NonceMapper) OnResultAccepted(sequence int64, accepted bool, errorMessage string) {}

// OnDisconnect handles pool connection loss. Implements pool.StratumListener.
// Suspends the mapper; miners keep their slots but receive no new jobs until reconnect.
//
//   // called by pool.StratumClient on disconnect
func (m *NonceMapper) OnDisconnect() {}
```

### 8.3 NonceStorage

```go
// NonceStorage is the 256-slot fixed-byte allocation table for one NonceMapper.
//
// Slot encoding:
//   0           = free
//   +minerID    = active miner
//   -minerID    = disconnected miner (dead slot, cleared on next SetJob)
//
//   storage := nicehash.NewNonceStorage()
type NonceStorage struct {
    slots   [256]int64              // slot state per above encoding
    miners  map[int64]*proxy.Miner  // minerID → Miner pointer for active miners
    job     proxy.Job               // current job from pool
    prevJob proxy.Job               // previous job (for stale submit validation)
    cursor  int                     // search starts here (round-robin allocation)
    mu      sync.Mutex
}

func NewNonceStorage() *NonceStorage {}

// Add finds the next free slot starting from cursor (wrapping), sets slot[index] = minerID,
// sets miner.FixedByte = index. Returns false if all 256 slots are occupied (active or dead).
//
//   ok := storage.Add(miner)
func (s *NonceStorage) Add(miner *proxy.Miner) bool {}

// Remove marks slot[miner.FixedByte] = -minerID (dead). Does not clear immediately;
// dead slots are cleared by the next SetJob call.
//
//   storage.Remove(miner)
func (s *NonceStorage) Remove(miner *proxy.Miner) {}

// SetJob replaces the current job. Clears all dead slots (sets them to 0).
// Archives current job to prevJob (same ClientID) or resets prevJob (new pool).
// Sends the new job (with per-slot blob patch) to every active miner.
//
//   storage.SetJob(job)
func (s *NonceStorage) SetJob(job proxy.Job) {}

// IsValidJobID returns true if id matches the current or previous job ID.
// A match on the previous job increments the global expired counter and still returns true
// (the share is accepted but flagged as expired in stats).
//
//   if !storage.IsValidJobID(submitJobID) { reject }
func (s *NonceStorage) IsValidJobID(id string) bool {}

// SlotCount returns free, dead, and active slot counts for monitoring output.
//
//   free, dead, active := storage.SlotCount()
func (s *NonceStorage) SlotCount() (free, dead, active int) {}
```

---

## 9. Simple Splitter

Simple mode creates one upstream pool connection per miner. When `ReuseTimeout > 0`, the upstream connection is held idle for that many seconds after the miner disconnects, allowing the next miner to inherit it and avoid reconnect latency.

```go
// SimpleSplitter is the Splitter implementation for simple (passthrough) mode.
//
//   s := simple.NewSimpleSplitter(cfg, eventBus, strategyFactory)
type SimpleSplitter struct {
    active  map[int64]*SimpleMapper  // minerID → mapper
    idle    map[int64]*SimpleMapper  // mapperID → mapper (reuse pool, keyed by mapper seq)
    cfg     *proxy.Config
    events  *proxy.EventBus
    factory pool.StrategyFactory
    mu      sync.Mutex
    seq     int64                    // monotonic mapper sequence counter
}

func NewSimpleSplitter(cfg *proxy.Config, events *proxy.EventBus, factory pool.StrategyFactory) *SimpleSplitter {}

// OnLogin creates a new SimpleMapper, or reclaims one from the idle pool if
// ReuseTimeout > 0 and an idle mapper's pool connection is still active.
// Sets miner.RouteID to the mapper's sequence ID.
//
//   s.OnLogin(event)
func (s *SimpleSplitter) OnLogin(event *proxy.LoginEvent) {}

// OnSubmit forwards to the mapper owning this miner (looked up by miner.RouteID).
//
//   s.OnSubmit(event)
func (s *SimpleSplitter) OnSubmit(event *proxy.SubmitEvent) {}

// OnClose moves the mapper to the idle pool (if ReuseTimeout > 0) or stops it immediately.
//
//   s.OnClose(event)
func (s *SimpleSplitter) OnClose(event *proxy.CloseEvent) {}

// GC removes idle mappers whose idle duration exceeds ReuseTimeout, and stopped mappers.
//
//   s.GC()
func (s *SimpleSplitter) GC() {}

// Connect establishes pool connections for any pre-existing idle mappers.
//
//   s.Connect()
func (s *SimpleSplitter) Connect() {}

// Tick is called every second. Runs idle mapper timeout checks.
//
//   s.Tick(ticks)
func (s *SimpleSplitter) Tick(ticks uint64) {}

// Upstreams returns current upstream connection counts (active + idle).
//
//   stats := s.Upstreams()
func (s *SimpleSplitter) Upstreams() proxy.UpstreamStats {}
```

```go
// SimpleMapper holds one outbound pool connection and serves at most one active miner at a time.
// It becomes idle when the miner disconnects and may be reclaimed for the next login.
//
//   m := simple.NewSimpleMapper(id, strategy)
type SimpleMapper struct {
    id       int64
    miner    *proxy.Miner  // nil when idle
    strategy pool.Strategy
    idleAt   time.Time     // zero when active
    stopped  bool
}
```

---

## 10. Pool Client

### 10.1 StratumClient

```go
// StratumClient is one outbound stratum TCP (optionally TLS) connection to a pool.
// The proxy presents itself to the pool as a standard stratum miner using the
// wallet address and password from PoolConfig.
//
//   client := pool.NewStratumClient(poolCfg, listener)
//   client.Connect()
type StratumClient struct {
    cfg      proxy.PoolConfig
    listener StratumListener
    conn     net.Conn
    tlsConn  *tls.Conn   // nil if plain TCP
    sessionID string     // pool-assigned session id from login reply
    seq      int64       // atomic JSON-RPC request id counter
    active   bool        // true once first job received
    sendMu   sync.Mutex
}

// StratumListener receives events from the pool connection.
type StratumListener interface {
    // OnJob is called when the pool pushes a new job notification or the login reply contains a job.
    OnJob(job proxy.Job)
    // OnResultAccepted is called when the pool accepts or rejects a submitted share.
    // sequence matches the value returned by Submit(). errorMessage is "" on accept.
    OnResultAccepted(sequence int64, accepted bool, errorMessage string)
    // OnDisconnect is called when the pool TCP connection closes for any reason.
    OnDisconnect()
}

func NewStratumClient(cfg proxy.PoolConfig, listener StratumListener) *StratumClient {}

// Connect dials the pool. Applies TLS if cfg.TLS is true.
// If cfg.TLSFingerprint is non-empty, pins the server certificate by SHA-256 of DER bytes.
//
//   result := client.Connect()
func (c *StratumClient) Connect() core.Result {}

// Login sends the stratum login request using cfg.User and cfg.Pass.
// The reply triggers StratumListener.OnJob when the pool's first job arrives.
//
//   client.Login()
func (c *StratumClient) Login() {}

// Submit sends a share submission. Returns the sequence number for result correlation.
// algo is "" if algorithm extension is not active.
//
//   seq := client.Submit(jobID, "deadbeef", "HASH64HEX", "cn/r")
func (c *StratumClient) Submit(jobID, nonce, result, algo string) int64 {}

// Disconnect closes the connection cleanly. Triggers OnDisconnect on the listener.
//
//   client.Disconnect()
func (c *StratumClient) Disconnect() {}
```

### 10.2 FailoverStrategy

```go
// FailoverStrategy wraps an ordered slice of PoolConfig entries.
// It connects to the first enabled pool and fails over in order on error.
// On reconnect it always retries from the primary first.
//
//   strategy := pool.NewFailoverStrategy(cfg.Pools, listener, cfg)
//   strategy.Connect()
type FailoverStrategy struct {
    pools    []proxy.PoolConfig
    current  int
    client   *StratumClient
    listener StratumListener
    cfg      *proxy.Config
    mu       sync.Mutex
}

// StrategyFactory creates a new FailoverStrategy for a given StratumListener.
// Used by splitters to create per-mapper strategies without coupling to Config.
//
//   factory := pool.NewStrategyFactory(cfg)
//   strategy := factory(listener)   // each mapper calls this
type StrategyFactory func(listener StratumListener) Strategy

// Strategy is the interface the splitters use to submit shares and check pool state.
type Strategy interface {
    Connect()
    Submit(jobID, nonce, result, algo string) int64
    Disconnect()
    IsActive() bool
}

func NewFailoverStrategy(pools []proxy.PoolConfig, listener StratumListener, cfg *proxy.Config) *FailoverStrategy {}

// Connect dials the current pool. On failure, advances to the next pool (modulo len),
// respecting cfg.Retries and cfg.RetryPause between attempts.
//
//   strategy.Connect()
func (s *FailoverStrategy) Connect() {}
```

---

## 11. Event Bus

```go
// EventBus dispatches proxy lifecycle events to registered listeners.
// Dispatch is synchronous on the calling goroutine. Listeners must not block.
//
//   bus := proxy.NewEventBus()
//   bus.Subscribe(proxy.EventLogin, customDiff.OnLogin)
//   bus.Subscribe(proxy.EventAccept, stats.OnAccept)
type EventBus struct {
    listeners map[EventType][]EventHandler
    mu        sync.RWMutex
}

// EventType identifies the proxy lifecycle event.
type EventType int

const (
    EventLogin  EventType = iota // miner completed login
    EventAccept                  // pool accepted a submitted share
    EventReject                  // pool rejected a share (or share expired)
    EventClose                   // miner TCP connection closed
)

// EventHandler is the callback signature for all event types.
type EventHandler func(Event)

// Event carries the data for any proxy lifecycle event.
// Fields not relevant to the event type are zero/nil.
type Event struct {
    Type    EventType
    Miner   *Miner    // always set
    Job     *Job      // set for Accept and Reject events
    Diff    uint64    // effective difficulty of the share (Accept and Reject)
    Error   string    // rejection reason (Reject only)
    Latency uint16    // pool response time in ms (Accept and Reject)
    Expired bool      // true if the share was accepted but against the previous job
}

func NewEventBus() *EventBus {}

// LoginEvent is the typed event passed to Splitter.OnLogin.
//
//   splitter.OnLogin(&LoginEvent{Miner: m})
type LoginEvent struct {
    Miner *Miner
}

// SubmitEvent is the typed event passed to Splitter.OnSubmit.
//
//   splitter.OnSubmit(&SubmitEvent{Miner: m, JobID: "abc", Nonce: "deadbeef"})
type SubmitEvent struct {
    Miner     *Miner
    JobID     string
    Nonce     string
    Result    string
    Algo      string
    RequestID int64
}

// CloseEvent is the typed event passed to Splitter.OnClose.
//
//   splitter.OnClose(&CloseEvent{Miner: m})
type CloseEvent struct {
    Miner *Miner
}

// Subscribe registers a handler for the given event type. Safe to call before Start.
//
//   bus.Subscribe(proxy.EventAccept, func(e proxy.Event) { stats.OnAccept(e.Diff) })
func (b *EventBus) Subscribe(t EventType, h EventHandler) {}

// Dispatch calls all registered handlers for the event's type in subscription order.
//
//   bus.Dispatch(proxy.Event{Type: proxy.EventLogin, Miner: m})
func (b *EventBus) Dispatch(e Event) {}
```

---

## 12. Stats

```go
// Stats tracks global proxy metrics. Hot-path counters are atomic. Hashrate windows
// use a ring buffer per window size, advanced by Tick().
//
//   s := proxy.NewStats()
//   bus.Subscribe(proxy.EventAccept, s.OnAccept)
//   bus.Subscribe(proxy.EventReject, s.OnReject)
type Stats struct {
    accepted     atomic.Uint64
    rejected     atomic.Uint64
    invalid      atomic.Uint64
    expired      atomic.Uint64
    hashes       atomic.Uint64  // cumulative sum of accepted share difficulties
    connections  atomic.Uint64  // total TCP connections accepted (ever)
    maxMiners    atomic.Uint64  // peak concurrent miner count
    topDiff      [10]uint64     // top-10 accepted difficulties, sorted descending; guarded by mu
    latency      []uint16       // pool response latencies in ms; capped at 10000 samples; guarded by mu
    windows      [6]tickWindow  // one per hashrate reporting period (see constants below)
    startTime    time.Time
    mu           sync.Mutex
}

// Hashrate window sizes in seconds. Index maps to Stats.windows and SummaryResponse.Hashrate.
const (
    HashrateWindow60s  = 0   // 1 minute
    HashrateWindow600s = 1   // 10 minutes
    HashrateWindow3600s = 2  // 1 hour
    HashrateWindow12h  = 3   // 12 hours
    HashrateWindow24h  = 4   // 24 hours
    HashrateWindowAll  = 5   // all-time (single accumulator, no window)
)

// tickWindow is a fixed-capacity ring buffer of per-second difficulty sums.
type tickWindow struct {
    buckets []uint64
    pos     int
    size    int // window size in seconds = len(buckets)
}

// StatsSummary is the serialisable snapshot returned by Summary().
type StatsSummary struct {
    Accepted    uint64     `json:"accepted"`
    Rejected    uint64     `json:"rejected"`
    Invalid     uint64     `json:"invalid"`
    Expired     uint64     `json:"expired"`
    Hashes      uint64     `json:"hashes_total"`
    AvgTime     uint32     `json:"avg_time"`    // seconds per accepted share
    AvgLatency  uint32     `json:"latency"`     // median pool response latency in ms
    Hashrate    [6]float64 `json:"hashrate"`    // H/s per window (index = HashrateWindow* constants)
    TopDiff     [10]uint64 `json:"best"`
}

func NewStats() *Stats {}

// OnAccept records an accepted share. Adds diff to the current second's bucket in all windows.
//
//   stats.OnAccept(proxy.Event{Diff: 100000, Latency: 82})
func (s *Stats) OnAccept(e proxy.Event) {}

// OnReject records a rejected share. If e.Error indicates low diff or malformed, increments invalid.
//
//   stats.OnReject(proxy.Event{Error: "Low difficulty share"})
func (s *Stats) OnReject(e proxy.Event) {}

// Tick advances all rolling windows by one second bucket. Called by the proxy tick loop.
//
//   stats.Tick()
func (s *Stats) Tick() {}

// Summary returns a point-in-time snapshot of all stats fields for API serialisation.
//
//   summary := stats.Summary()
func (s *Stats) Summary() StatsSummary {}
```

---

## 13. Workers

Workers are aggregate identity records built from miner login fields. Which field becomes the worker name is controlled by `Config.Workers` (WorkersMode).

```go
// Workers maintains per-worker aggregate stats. Workers are identified by name,
// derived from the miner's login fields per WorkersMode.
//
//   w := proxy.NewWorkers(proxy.WorkersByRigID, bus)
type Workers struct {
    mode      WorkersMode
    entries   []WorkerRecord                // ordered by first-seen (stable)
    nameIndex map[string]int                // workerName → entries index
    idIndex   map[int64]int                 // minerID → entries index
    mu        sync.RWMutex
}

// WorkerRecord is the per-identity aggregate.
type WorkerRecord struct {
    Name        string
    LastIP      string
    Connections uint64
    Accepted    uint64
    Rejected    uint64
    Invalid     uint64
    Hashes      uint64     // sum of accepted share difficulties
    LastHashAt  time.Time
    windows     [5]tickWindow // 60s, 600s, 3600s, 12h, 24h
}

// Hashrate returns the H/s for a given window (seconds: 60, 600, 3600, 43200, 86400).
//
//   hr60 := record.Hashrate(60)
func (r *WorkerRecord) Hashrate(seconds int) float64 {}

func NewWorkers(mode WorkersMode, bus *proxy.EventBus) *Workers {}

// List returns a snapshot of all worker records in first-seen order.
//
//   workers := w.List()
func (w *Workers) List() []WorkerRecord {}

// Tick advances all worker hashrate windows. Called by the proxy tick loop every second.
//
//   w.Tick()
func (w *Workers) Tick() {}

// OnLogin upserts the worker record for the miner's login. Called via EventBus subscription.
//
//   bus.Subscribe(proxy.EventLogin, func(e proxy.Event) { w.OnLogin(e) })
func (w *Workers) OnLogin(e Event) {}

// OnAccept records an accepted share for the worker. Called via EventBus subscription.
//
//   bus.Subscribe(proxy.EventAccept, func(e proxy.Event) { w.OnAccept(e) })
func (w *Workers) OnAccept(e Event) {}

// OnReject records a rejected share for the worker. Called via EventBus subscription.
//
//   bus.Subscribe(proxy.EventReject, func(e proxy.Event) { w.OnReject(e) })
func (w *Workers) OnReject(e Event) {}
```

---

## 14. Custom Difficulty

```go
// CustomDiff resolves and applies per-miner difficulty overrides at login time.
// Resolution order: user-suffix (+N) > Config.CustomDiff > pool difficulty.
//
//   cd := proxy.NewCustomDiff(cfg.CustomDiff)
//   bus.Subscribe(proxy.EventLogin, cd.OnLogin)
type CustomDiff struct {
    globalDiff uint64
}

func NewCustomDiff(globalDiff uint64) *CustomDiff {}

// OnLogin parses miner.User for a "+{number}" suffix and sets miner.CustomDiff.
// Strips the suffix from miner.User so the clean wallet address is forwarded to the pool.
// Falls back to globalDiff if no suffix is present.
//
//   cd.OnLogin(event)
//   // "WALLET+50000" → miner.User="WALLET", miner.CustomDiff=50000
//   // "WALLET"       → miner.User="WALLET", miner.CustomDiff=globalDiff (if >0)
func (cd *CustomDiff) OnLogin(e proxy.Event) {}
```

---

## 15. Logging

### 15.1 AccessLog

```go
// AccessLog writes connection lifecycle lines to an append-only text file.
//
// Line format (connect): 2026-04-04T12:00:00Z CONNECT  <ip>  <user>  <agent>
// Line format (close):   2026-04-04T12:00:00Z CLOSE    <ip>  <user>  rx=<bytes>  tx=<bytes>
//
//   al, result := log.NewAccessLog("/var/log/proxy-access.log")
//   bus.Subscribe(proxy.EventLogin, al.OnLogin)
//   bus.Subscribe(proxy.EventClose, al.OnClose)
type AccessLog struct {
    path string
    mu   sync.Mutex
    f    io.WriteCloser // opened append-only on first write; nil until first event
}

func NewAccessLog(path string) *AccessLog {}

// OnLogin writes a CONNECT line. Called synchronously from the event bus.
//
//   al.OnLogin(event)
func (l *AccessLog) OnLogin(e proxy.Event) {}

// OnClose writes a CLOSE line with byte counts.
//
//   al.OnClose(event)
func (l *AccessLog) OnClose(e proxy.Event) {}
```

### 15.2 ShareLog

```go
// ShareLog writes share result lines to an append-only text file.
//
// Line format (accept): 2026-04-04T12:00:00Z ACCEPT  <user>  diff=<diff>  latency=<ms>ms
// Line format (reject): 2026-04-04T12:00:00Z REJECT  <user>  reason="<message>"
//
//   sl := log.NewShareLog("/var/log/proxy-shares.log")
//   bus.Subscribe(proxy.EventAccept, sl.OnAccept)
//   bus.Subscribe(proxy.EventReject, sl.OnReject)
type ShareLog struct {
    path string
    mu   sync.Mutex
    f    io.WriteCloser
}

func NewShareLog(path string) *ShareLog {}

// OnAccept writes an ACCEPT line.
//
//   sl.OnAccept(event)
func (l *ShareLog) OnAccept(e proxy.Event) {}

// OnReject writes a REJECT line with the rejection reason.
//
//   sl.OnReject(event)
func (l *ShareLog) OnReject(e proxy.Event) {}
```

---

## 16. HTTP Monitoring API

```go
// RegisterRoutes registers the proxy monitoring routes on a core/api Engine.
// GET /1/summary — aggregated proxy stats
// GET /1/workers — per-worker hashrate table
// GET /1/miners  — per-connection state table
//
//   proxyapi.RegisterRoutes(engine, p)
func RegisterRoutes(r *api.Engine, p *proxy.Proxy) {}
```

### GET /1/summary — response shape

```json
{
  "version": "1.0.0",
  "mode": "nicehash",
  "hashrate": {
    "total": [12345.67, 11900.00, 12100.00, 11800.00, 12000.00, 12200.00]
  },
  "miners":    {"now": 142, "max": 200},
  "workers":   38,
  "upstreams": {"active": 1, "sleep": 0, "error": 0, "total": 1, "ratio": 142.0},
  "results": {
    "accepted": 4821, "rejected": 3, "invalid": 0, "expired": 12,
    "avg_time": 47, "latency": 82, "hashes_total": 4821000000,
    "best": [999999999, 888888888, 777777777, 0, 0, 0, 0, 0, 0, 0]
  }
}
```

Hashrate array index → window: [0]=60s, [1]=600s, [2]=3600s, [3]=12h, [4]=24h, [5]=all-time.

### GET /1/workers — response shape

```json
{
  "mode": "rig-id",
  "workers": [
    ["rig-alpha", "10.0.0.1", 2, 1240, 1, 0, 124000000000, 1712232000, 4321.0, 4200.0, 4100.0, 4050.0, 4000.0]
  ]
}
```

Worker array columns: name, last_ip, connections, accepted, rejected, invalid, hashes, last_hash_unix, hashrate_60s, hashrate_600s, hashrate_3600s, hashrate_12h, hashrate_24h.

### GET /1/miners — response shape

```json
{
  "format": ["id","ip","tx","rx","state","diff","user","password","rig_id","agent"],
  "miners": [
    [1, "10.0.0.1:49152", 4096, 512, 2, 100000, "WALLET", "********", "rig-alpha", "XMRig/6.21.0"]
  ]
}
```

`state` values: 0=WaitLogin, 1=WaitReady, 2=Ready, 3=Closing. Password is always `"********"`.

```go
// SummaryResponse is the /1/summary JSON body.
type SummaryResponse struct {
    Version   string            `json:"version"`
    Mode      string            `json:"mode"`
    Hashrate  HashrateResponse  `json:"hashrate"`
    Miners    MinersCountResponse `json:"miners"`
    Workers   uint64            `json:"workers"`
    Upstreams UpstreamResponse  `json:"upstreams"`
    Results   ResultsResponse   `json:"results"`
}

type HashrateResponse   struct { Total [6]float64 `json:"total"` }
type MinersCountResponse struct { Now uint64 `json:"now"`; Max uint64 `json:"max"` }

type UpstreamResponse struct {
    Active uint64  `json:"active"`
    Sleep  uint64  `json:"sleep"`
    Error  uint64  `json:"error"`
    Total  uint64  `json:"total"`
    Ratio  float64 `json:"ratio"`
}

type ResultsResponse struct {
    Accepted    uint64     `json:"accepted"`
    Rejected    uint64     `json:"rejected"`
    Invalid     uint64     `json:"invalid"`
    Expired     uint64     `json:"expired"`
    AvgTime     uint32     `json:"avg_time"`
    Latency     uint32     `json:"latency"`
    HashesTotal uint64     `json:"hashes_total"`
    Best        [10]uint64 `json:"best"`
}
```

---

## 17. Server and Rate Limiter

```go
// Server listens on one BindAddr and creates a Miner for each accepted connection.
//
//   srv, result := proxy.NewServer(bind, tlsCfg, rateLimiter, onAccept)
//   srv.Start()
type Server struct {
    addr      BindAddr
    tlsCfg    *tls.Config   // nil for plain TCP
    limiter   *RateLimiter
    onAccept  func(net.Conn, uint16)
    listener  net.Listener
    done      chan struct{}
}

func NewServer(bind BindAddr, tlsCfg *tls.Config, limiter *RateLimiter, onAccept func(net.Conn, uint16)) (*Server, core.Result) {}

// Start begins accepting connections in a goroutine.
//
//   srv.Start()
func (s *Server) Start() {}

// Stop closes the listener. In-flight connections are not forcibly closed.
//
//   srv.Stop()
func (s *Server) Stop() {}
```

```go
// RateLimiter implements per-IP token bucket connection rate limiting.
// Each unique IP has a bucket initialised to MaxConnectionsPerMinute tokens.
// Each connection attempt consumes one token. Tokens refill at 1 per (60/max) seconds.
// An IP that empties its bucket is added to a ban list for BanDurationSeconds.
//
//   rl := proxy.NewRateLimiter(cfg.RateLimit)
//   if !rl.Allow("1.2.3.4") { conn.Close(); return }
type RateLimiter struct {
    cfg     RateLimit
    buckets map[string]*tokenBucket
    banned  map[string]time.Time
    mu      sync.Mutex
}

// tokenBucket is a simple token bucket for one IP.
type tokenBucket struct {
    tokens    int
    lastRefill time.Time
}

func NewRateLimiter(cfg RateLimit) *RateLimiter {}

// Allow returns true if the IP address is permitted to open a new connection. Thread-safe.
// Extracts the host from ip (strips port if present).
//
//   if rl.Allow(conn.RemoteAddr().String()) { proceed() }
func (rl *RateLimiter) Allow(ip string) bool {}

// Tick removes expired ban entries and refills all token buckets. Called every second.
//
//   rl.Tick()
func (rl *RateLimiter) Tick() {}
```

---

## 18. Config Hot-Reload

```go
// ConfigWatcher polls a config file for mtime changes and calls onChange on modification.
// Uses 1-second polling; does not require fsnotify.
//
//   w := proxy.NewConfigWatcher("config.json", func(cfg *proxy.Config) {
//       p.Reload(cfg)
//   })
//   w.Start()
type ConfigWatcher struct {
    path     string
    onChange func(*Config)
    lastMod  time.Time
    done     chan struct{}
}

func NewConfigWatcher(path string, onChange func(*Config)) *ConfigWatcher {}

// Start begins the polling goroutine. No-op if Watch is false in config.
//
//   w.Start()
func (w *ConfigWatcher) Start() {}

// Stop ends the polling goroutine cleanly.
//
//   w.Stop()
func (w *ConfigWatcher) Stop() {}
```

---

## 19. Stratum Wire Format

All stratum messages are newline-delimited JSON (`\n` terminated). Maximum line length is 16384 bytes. The proxy reads with a buffered line reader that discards lines exceeding this limit and closes the connection.

### Miner → Proxy (login)
```json
{"id":1,"jsonrpc":"2.0","method":"login","params":{"login":"WALLET","pass":"x","agent":"XMRig/6.21.0","algo":["cn/r","rx/0"],"rigid":"rig-name"}}
```

### Miner → Proxy (submit)
```json
{"id":2,"jsonrpc":"2.0","method":"submit","params":{"id":"SESSION-UUID","job_id":"JOBID","nonce":"deadbeef","result":"HASH64HEX","algo":"cn/r"}}
```

### Miner → Proxy (keepalived)
```json
{"id":3,"method":"keepalived","params":{"id":"SESSION-UUID"}}
```

### Proxy → Miner (login success, with algo extension)
```json
{"id":1,"jsonrpc":"2.0","error":null,"result":{"id":"SESSION-UUID","job":{"blob":"BLOB160HEX","job_id":"JOBID","target":"b88d0600","algo":"cn/r","id":"SESSION-UUID"},"extensions":["algo"],"status":"OK"}}
```

### Proxy → Miner (job notification)
```json
{"jsonrpc":"2.0","method":"job","params":{"blob":"BLOB160HEX","job_id":"JOBID","target":"b88d0600","algo":"cn/r","id":"SESSION-UUID"}}
```

### Proxy → Miner (submit success)
```json
{"id":2,"jsonrpc":"2.0","error":null,"result":{"status":"OK"}}
```

### Proxy → Miner (error)
```json
{"id":1,"jsonrpc":"2.0","error":{"code":-1,"message":"Invalid payment address provided"}}
```

### Proxy → Miner (keepalived reply)
```json
{"id":3,"jsonrpc":"2.0","error":null,"result":{"status":"KEEPALIVED"}}
```

### NiceHash Nonce Patching Detail

Before sending a job to a miner in NiceHash mode, byte index 39 of the blob (hex characters at positions 78–79, zero-indexed) is replaced with the miner's `fixedByte` rendered as two lowercase hex digits.

Example: original blob `"07070000...0000"` (160 chars), fixedByte `0x2A` → positions 78–79 become `"2a"`.

When the miner submits, the `nonce` field will contain `fixedByte` at byte 39 naturally (the miner searched only its partition). The proxy forwards the nonce to the pool without modification — the pool sees valid nonces from the full 32-bit space partitioned correctly by the fixed byte scheme.

---

## 20. Concurrency Model

The proxy uses per-miner goroutines rather than an event loop. Each accepted connection runs in its own goroutine for reading. Writes to a miner connection are serialised by `Miner.sendMu`.

| Shared resource | Protection |
|-----------------|------------|
| `Stats` hot counters (accepted, rejected, hashes) | `atomic.Uint64` — no lock |
| `Stats.topDiff`, `Stats.latency` | `Stats.mu` Mutex |
| `Stats.windows` | Single writer (tick loop goroutine) — no lock needed |
| `Workers` read (List, Tick) | `Workers.mu` RWMutex |
| `Workers` write (OnLogin, OnAccept) | `Workers.mu` RWMutex |
| `NonceStorage` | Per-storage `sync.Mutex` |
| `EventBus` subscriptions | `EventBus.mu` RWMutex; dispatch holds read lock |
| `RateLimiter` | `RateLimiter.mu` Mutex |
| `Miner` TCP writes | `Miner.sendMu` Mutex |
| `StratumClient` TCP writes | `StratumClient.sendMu` Mutex |
| `NonceSplitter.mappers` slice | `NonceSplitter.mu` RWMutex |
| `SimpleSplitter.active/idle` maps | `SimpleSplitter.mu` Mutex |

The tick loop is a single goroutine on a 1-second interval. It calls `Stats.Tick()`, `Workers.Tick()`, and `RateLimiter.Tick()` sequentially without further synchronisation.

The pool StratumClient read loop runs in its own goroutine. `Submit()` and `Disconnect()` serialise through `sendMu`.

---

## 21. Error Handling

All errors use `core.E(scope, message, cause)`. No `fmt.Errorf`, `errors.New`, `log`, or `os` imports.

| Error condition | Handling |
|-----------------|----------|
| Config parse failure | `core.E("proxy.config", "parse failed", err)` — fatal, proxy does not start |
| Bind address in use | `core.E("proxy.server", "listen failed", err)` — fatal |
| TLS cert/key load failure | `core.E("proxy.tls", "load certificate failed", err)` — fatal |
| Login timeout (10s) | Close connection silently |
| Inactivity timeout (600s) | Close connection with WARN log |
| Line too long (>16384 bytes) | Close connection with WARN log |
| Pool connect failure | Retry via FailoverStrategy; miners in WaitReady remain waiting |
| Pool sends invalid job | Drop job; log at WARN |
| Submit sequence mismatch | Log at WARN; reply error to miner |
| Nonce table full (256/256) | Reject login: `"Proxy is full, try again later"` |
| Access password mismatch | Reject login: `"Invalid password"` |
| TLS fingerprint mismatch | Close outbound connection; log at ERROR; try next pool |

---

## 22. Tests

Test naming: `TestFilename_Function_{Good,Bad,Ugly}` — all three mandatory per function.

### Unit Tests

**`storage_test.go`**

```go
// TestStorage_Add_Good: 256 sequential Add calls fill all slots; cursor wraps correctly;
// each miner.FixedByte is unique 0-255.
// TestStorage_Add_Bad: 257th Add returns false when table is full.
// TestStorage_Add_Ugly: Add, Remove, SetJob, Add — removed slot is reclaimed (was dead, now free).
func TestStorage_Add_Good(t *testing.T) {
    s := nicehash.NewNonceStorage()
    seen := make(map[uint8]bool)
    for i := 0; i < 256; i++ {
        m := &proxy.Miner{}
        m.SetID(int64(i + 1))
        ok := s.Add(m)
        require.True(t, ok)
        require.False(t, seen[m.FixedByte()])
        seen[m.FixedByte()] = true
    }
}
```

**`job_test.go`**

```go
// TestJob_BlobWithFixedByte_Good: 160-char blob, fixedByte 0x2A → chars[78:80] == "2a", total len 160.
// TestJob_BlobWithFixedByte_Bad: blob shorter than 80 chars → returns original blob unchanged.
// TestJob_BlobWithFixedByte_Ugly: fixedByte 0xFF → "ff" (lowercase, not "FF").
func TestJob_BlobWithFixedByte_Good(t *testing.T) {
    j := proxy.Job{Blob: strings.Repeat("0", 160)}
    result := j.BlobWithFixedByte(0x2A)
    require.Equal(t, "2a", result[78:80])
    require.Equal(t, 160, len(result))
}
```

**`stats_test.go`**

```go
// TestStats_OnAccept_Good: accepted counter +1, hashes += diff, topDiff updated.
// TestStats_OnAccept_Bad: 100 concurrent goroutines each calling OnAccept — no race (-race flag).
// TestStats_OnAccept_Ugly: 15 accepts with varying diffs — topDiff[9] is the 10th highest, not 0.
```

**`customdiff_test.go`**

```go
// TestCustomDiff_Apply_Good: "WALLET+50000" → user="WALLET", customDiff=50000.
// TestCustomDiff_Apply_Bad: "WALLET+abc" → user unchanged "WALLET+abc", customDiff=0.
// TestCustomDiff_Apply_Ugly: globalDiff=10000, user has no suffix → customDiff=10000.
```

**`ratelimit_test.go`**

```go
// TestRateLimiter_Allow_Good: budget of 10/min, first 10 calls return true.
// TestRateLimiter_Allow_Bad: 11th call returns false (bucket exhausted).
// TestRateLimiter_Allow_Ugly: banned IP returns false for BanDurationSeconds even with new bucket.
```

**`job_difficulty_test.go`**

```go
// TestJob_DifficultyFromTarget_Good: target "b88d0600" → difficulty 100000 (known value).
// TestJob_DifficultyFromTarget_Bad: target "00000000" → difficulty 0 (no divide by zero panic).
// TestJob_DifficultyFromTarget_Ugly: target "ffffffff" → difficulty 1 (minimum).
```

### Integration Tests

**`integration/nicehash_test.go`** — in-process mock pool server

```go
// TestNicehash_FullFlow_Good: start proxy, connect 3 miners, receive job broadcast,
// submit share, verify accept reply and stats.accepted == 1.
// TestNicehash_FullFlow_Bad: submit with wrong session id → error reply "Unauthenticated",
// miner connection stays open.
// TestNicehash_FullFlow_Ugly: 256 miners connected → 257th login rejected "Proxy is full".
```

**`integration/failover_test.go`**

```go
// TestFailover_PrimaryDown_Good: primary pool closes listener, proxy reconnects to fallback
// within 3 seconds, miners receive next job.
// TestFailover_AllDown_Bad: all pool addresses unavailable, miners in WaitReady stay open
// (no spurious close on the miner side).
// TestFailover_Recovery_Ugly: primary recovers while proxy is using fallback,
// next reconnect attempt uses primary (not fallback).
```

---

## 23. Example config.json

```json
{
    "mode": "nicehash",
    "bind": [
        {"host": "0.0.0.0", "port": 3333, "tls": false},
        {"host": "0.0.0.0", "port": 3443, "tls": true}
    ],
    "pools": [
        {"url": "pool.lthn.io:3333",        "user": "WALLET", "pass": "x", "tls": false, "enabled": true},
        {"url": "pool-backup.lthn.io:3333", "user": "WALLET", "pass": "x", "tls": false, "enabled": true}
    ],
    "tls": {
        "enabled":  true,
        "cert":     "/etc/proxy/cert.pem",
        "cert_key": "/etc/proxy/key.pem"
    },
    "http": {
        "enabled":      true,
        "host":         "127.0.0.1",
        "port":         8080,
        "access-token": "secret",
        "restricted":   true
    },
    "access-password":  "",
    "access-log-file":  "/var/log/proxy-access.log",
    "custom-diff":      0,
    "custom-diff-stats": false,
    "algo-ext":         true,
    "workers":          "rig-id",
    "rate-limit":       {"max-connections-per-minute": 30, "ban-duration": 300},
    "retries":          3,
    "retry-pause":      2,
    "reuse-timeout":    0,
    "watch":            true
}
```

---

## 24. Reference

| Resource | Location |
|----------|----------|
| Core Go RFC | `code/core/go/RFC.md` |
| Core API RFC | `code/core/go/api/RFC.md` |
