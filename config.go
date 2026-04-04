package proxy

// Config is the top-level proxy configuration, loaded from JSON and hot-reloaded on change.
//
//	cfg, result := proxy.LoadConfig("config.json")
//	if !result.OK { log.Fatal(result.Error) }
type Config struct {
	Mode            string       `json:"mode"`              // "nicehash" or "simple"
	Bind            []BindAddr   `json:"bind"`              // listen addresses
	Pools           []PoolConfig `json:"pools"`             // ordered primary + fallbacks
	TLS             TLSConfig    `json:"tls"`               // inbound TLS (miner-facing)
	HTTP            HTTPConfig   `json:"http"`              // monitoring API
	AccessPassword  string       `json:"access-password"`   // "" = no auth required
	CustomDiff      uint64       `json:"custom-diff"`       // 0 = disabled
	CustomDiffStats bool         `json:"custom-diff-stats"` // report per custom-diff bucket
	AlgoExtension   bool         `json:"algo-ext"`          // forward algo field in jobs
	Workers         WorkersMode  `json:"workers"`           // "rig-id", "user", "password", "agent", "ip", "false"
	AccessLogFile   string       `json:"access-log-file"`   // "" = disabled
	ShareLogFile    string       `json:"share-log-file"`    // "" = disabled
	ReuseTimeout    int          `json:"reuse-timeout"`     // seconds; simple mode upstream reuse
	Retries         int          `json:"retries"`           // pool reconnect attempts
	RetryPause      int          `json:"retry-pause"`       // seconds between retries
	Watch           bool         `json:"watch"`             // hot-reload on file change
	RateLimit       RateLimit    `json:"rate-limit"`        // per-IP connection rate limit
	sourcePath      string
}

// BindAddr is one TCP listen endpoint.
//
//	proxy.BindAddr{Host: "0.0.0.0", Port: 3333, TLS: false}
type BindAddr struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
	TLS  bool   `json:"tls"`
}

// PoolConfig is one upstream pool entry.
//
//	proxy.PoolConfig{URL: "pool.lthn.io:3333", User: "WALLET", Pass: "x", Enabled: true}
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
//	proxy.TLSConfig{Enabled: true, CertFile: "/etc/proxy/cert.pem", KeyFile: "/etc/proxy/key.pem"}
type TLSConfig struct {
	Enabled   bool   `json:"enabled"`
	CertFile  string `json:"cert"`
	KeyFile   string `json:"cert_key"`
	Ciphers   string `json:"ciphers"`   // OpenSSL cipher string; "" = default
	Protocols string `json:"protocols"` // TLS version string; "" = default
}

// HTTPConfig controls the monitoring API server.
//
//	proxy.HTTPConfig{Enabled: true, Host: "127.0.0.1", Port: 8080, Restricted: true}
type HTTPConfig struct {
	Enabled     bool   `json:"enabled"`
	Host        string `json:"host"`
	Port        uint16 `json:"port"`
	AccessToken string `json:"access-token"` // Bearer token; "" = no auth
	Restricted  bool   `json:"restricted"`   // true = read-only GET only
}

// RateLimit controls per-IP connection rate limiting using a token bucket.
//
//	proxy.RateLimit{MaxConnectionsPerMinute: 30, BanDurationSeconds: 300}
type RateLimit struct {
	MaxConnectionsPerMinute int `json:"max-connections-per-minute"` // 0 = disabled
	BanDurationSeconds      int `json:"ban-duration"`               // 0 = no ban
}

// WorkersMode controls which login field becomes the worker name.
type WorkersMode string

const (
	WorkersByRigID  WorkersMode = "rig-id" // rigid field, fallback to user
	WorkersByUser   WorkersMode = "user"
	WorkersByPass   WorkersMode = "password"
	WorkersByAgent  WorkersMode = "agent"
	WorkersByIP     WorkersMode = "ip"
	WorkersDisabled WorkersMode = "false"
)
