package proxy

import (
	"bufio"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	maxStratumLineLength     = 16384
	minerLoginTimeout        = 10 * time.Second
	minerReadyTimeout        = 600 * time.Second
	minerTLSHandshakeTimeout = 5 * time.Second
	submitDrainTimeout       = 5 * time.Second
	httpReadHeaderTimeout    = 5 * time.Second
	httpReadTimeout          = 10 * time.Second
	httpWriteTimeout         = 10 * time.Second
	httpIdleTimeout          = 60 * time.Second
	maskedPassword           = "********"
)

// MinerSnapshot is a serialisable view of one miner connection.
//
//	snapshots := p.MinerSnapshots()
//	for _, s := range snapshots {
//	    _ = s.ID   // 1
//	    _ = s.IP   // "10.0.0.1:49152"
//	    _ = s.Diff // 100000
//	}
type MinerSnapshot struct {
	ID       int64
	IP       string
	TX       uint64
	RX       uint64
	State    MinerState
	Diff     uint64
	User     string
	Password string
	RigID    string
	Agent    string
}

// cfg := &proxy.Config{Mode: "nicehash", Bind: []proxy.BindAddr{{Host: "0.0.0.0", Port: 3333}}, Pools: []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}}}
// p, result := proxy.New(cfg)
//
//	if !result.OK {
//	    return result.Error
//	}
func New(config *Config) (*Proxy, Result) {
	if config == nil {
		return nil, newErrorResult(NewScopedError("proxy", "config is nil", nil))
	}
	if result := config.Validate(); !result.OK {
		return nil, result
	}
	normalizeConfigValues(config)

	p := &Proxy{
		config:            config,
		events:            NewEventBus(),
		stats:             NewStats(),
		workers:           NewWorkers(config.Workers, nil),
		miners:            make(map[int64]*Miner),
		customDiff:        NewCustomDiff(config.CustomDiff),
		customDiffBuckets: NewCustomDiffBuckets(config.CustomDiffStats),
		rateLimit:         NewRateLimiter(config.RateLimit),
		accessLog:         newAccessLogSink(config.AccessLogFile),
		shareLog:          newShareLogSink(config.ShareLogFile),
		done:              make(chan struct{}),
	}
	p.events.Subscribe(EventLogin, p.customDiff.OnLogin)
	p.workers.bindEvents(p.events)
	if p.accessLog != nil {
		p.events.Subscribe(EventLogin, p.accessLog.OnLogin)
		p.events.Subscribe(EventClose, p.accessLog.OnClose)
	}
	p.events.Subscribe(EventLogin, p.stats.OnLogin)
	p.events.Subscribe(EventClose, p.stats.OnClose)
	p.events.Subscribe(EventAccept, p.stats.OnAccept)
	p.events.Subscribe(EventReject, p.stats.OnReject)
	shareSinks := make([]ShareSink, 0, 2)
	if p.shareLog != nil {
		shareSinks = append(shareSinks, p.shareLog)
	}
	if p.customDiffBuckets != nil {
		shareSinks = append(shareSinks, p.customDiffBuckets)
	}
	if len(shareSinks) > 0 {
		p.shareSink = newShareSinkGroup(shareSinks...)
		p.events.Subscribe(EventAccept, p.shareSink.OnAccept)
		p.events.Subscribe(EventReject, p.shareSink.OnReject)
	}
	p.events.Subscribe(EventAccept, p.onShareSettled)
	p.events.Subscribe(EventReject, p.onShareSettled)
	if config.Watch && config.configPath != "" {
		p.watcher = NewConfigWatcher(config.configPath, p.Reload)
	}

	factory, ok := splitterFactoryForMode(config.Mode)
	if !ok && !isSupportedMode(config.Mode) {
		return nil, newErrorResult(NewScopedError("proxy", "unsupported mode", nil))
	}
	if ok {
		p.splitter = factory(config, p.events)
	} else {
		p.splitter = &noopSplitter{}
	}

	return p, newSuccessResult()
}

// Mode returns the runtime mode, for example "nicehash" or "simple".
//
//	mode := p.Mode() // "nicehash"
func (p *Proxy) Mode() string {
	if p == nil || p.config == nil {
		return ""
	}
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return p.config.Mode
}

// WorkersMode returns the active worker identity strategy, for example proxy.WorkersByRigID.
//
//	mode := p.WorkersMode() // proxy.WorkersByRigID
func (p *Proxy) WorkersMode() WorkersMode {
	if p == nil || p.config == nil {
		return WorkersDisabled
	}
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return p.config.Workers
}

// Summary returns a snapshot of the current global metrics.
//
//	summary := p.Summary()
//	_ = summary.Accepted
func (p *Proxy) Summary() StatsSummary {
	if p == nil || p.stats == nil {
		return StatsSummary{}
	}
	summary := p.stats.Summary()
	if p.customDiffBuckets != nil {
		summary.CustomDiffStats = p.customDiffBuckets.Snapshot()
	}
	return summary
}

// WorkerRecords returns a snapshot of the current worker aggregates.
//
//	records := p.WorkerRecords()
//	for _, r := range records { _ = r.Name }
func (p *Proxy) WorkerRecords() []WorkerRecord {
	if p == nil || p.workers == nil {
		return nil
	}
	return p.workers.List()
}

// MinerSnapshots returns a snapshot of the live miner connections.
//
//	snapshots := p.MinerSnapshots()
//	for _, s := range snapshots { _ = s.IP }
func (p *Proxy) MinerSnapshots() []MinerSnapshot {
	if p == nil {
		return nil
	}
	p.minersMu.RLock()
	defer p.minersMu.RUnlock()
	rows := make([]MinerSnapshot, 0, len(p.miners))
	for _, miner := range p.miners {
		ip := miner.RemoteAddr()
		if ip == "" {
			ip = miner.IP()
		}
		rows = append(rows, MinerSnapshot{
			ID:       miner.ID(),
			IP:       ip,
			TX:       miner.TX(),
			RX:       miner.RX(),
			State:    miner.State(),
			Diff:     miner.Diff(),
			User:     miner.User(),
			Password: maskedPassword,
			RigID:    miner.RigID(),
			Agent:    miner.Agent(),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

// MinerCount returns the current and peak miner counts.
//
//	now, max := p.MinerCount() // 142, 200
func (p *Proxy) MinerCount() (now, max uint64) {
	if p == nil || p.stats == nil {
		return 0, 0
	}
	return p.stats.miners.Load(), p.stats.maxMiners.Load()
}

// Upstreams returns the current upstream connection counts.
//
//	stats := p.Upstreams()
//	_ = stats.Active // 1
func (p *Proxy) Upstreams() UpstreamStats {
	if p == nil || p.splitter == nil {
		return UpstreamStats{}
	}
	return p.splitter.Upstreams()
}

// Events returns the proxy event bus for subscription.
//
//	bus := p.Events()
//	bus.Subscribe(proxy.EventAccept, handler)
func (p *Proxy) Events() *EventBus {
	if p == nil {
		return nil
	}
	return p.events
}

// p.Start()
//
//	go func() {
//	    time.Sleep(30 * time.Second)
//	    p.Stop()
//	}()
//	p.Start()
func (p *Proxy) Start() {
	if p == nil {
		return
	}
	if p.config == nil {
		return
	}
	if result := p.buildServers(); !result.OK {
		p.Stop()
		return
	}
	if p.splitter != nil {
		p.splitter.Connect()
	}
	p.lifecycleMu.Lock()
	servers := append([]*Server(nil), p.servers...)
	p.lifecycleMu.Unlock()
	for _, server := range servers {
		if server != nil {
			server.Start()
		}
	}
	if p.watcher != nil {
		p.watcher.Start()
	}
	if httpCfg := p.currentHTTPConfig(); httpCfg.Enabled {
		if !p.startMonitoringServer() {
			p.Stop()
			return
		}
	}
	p.lifecycleMu.Lock()
	if p.ticker == nil {
		p.ticker = time.NewTicker(time.Second)
	}
	ticker := p.ticker
	p.lifecycleMu.Unlock()
	go func() {
		var ticks uint64
		for {
			select {
			case <-ticker.C:
				ticks++
				if p.stats != nil {
					p.stats.Tick()
				}
				if p.workers != nil {
					p.workers.Tick()
				}
				if p.rateLimit != nil {
					p.rateLimit.Tick()
				}
				if p.splitter != nil {
					p.splitter.Tick(ticks)
					if ticks%60 == 0 {
						p.splitter.GC()
					}
				}
			case <-p.done:
				return
			}
		}
	}()
	<-p.done
	p.Stop()
}

// Stop shuts down listeners, pool connections, the config watcher, and log sinks.
//
//	p.Stop()
func (p *Proxy) Stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		close(p.done)
		p.lifecycleMu.RLock()
		servers := append([]*Server(nil), p.servers...)
		ticker := p.ticker
		accessLog := p.accessLog
		shareLog := p.shareLog
		watcher := p.watcher
		p.lifecycleMu.RUnlock()
		if ticker != nil {
			ticker.Stop()
			p.lifecycleMu.Lock()
			if p.ticker == ticker {
				p.ticker = nil
			}
			p.lifecycleMu.Unlock()
		}
		for _, server := range servers {
			server.Stop()
		}
		if watcher != nil {
			watcher.Stop()
		}
		p.stopMonitoringServer()
		deadline := time.Now().Add(submitDrainTimeout)
		for p.submitCount.Load() > 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		p.closeAllMiners()
		if splitter, ok := p.splitter.(interface{ Disconnect() }); ok {
			splitter.Disconnect()
		}
		if accessLog != nil {
			accessLog.Close()
		}
		if shareLog != nil {
			shareLog.Close()
		}
	})
}

func (p *Proxy) closeAllMiners() {
	if p == nil {
		return
	}
	for _, miner := range p.activeMiners() {
		if miner != nil {
			miner.Close()
		}
	}
}

func (p *Proxy) activeMiners() []*Miner {
	if p == nil {
		return nil
	}
	p.minersMu.RLock()
	defer p.minersMu.RUnlock()
	miners := make([]*Miner, 0, len(p.miners))
	for _, miner := range p.miners {
		miners = append(miners, miner)
	}
	sort.Slice(miners, func(i, j int) bool {
		if miners[i] == nil || miners[j] == nil {
			return miners[j] != nil
		}
		return miners[i].ID() < miners[j].ID()
	})
	return miners
}

// p.Reload(&proxy.Config{Mode: "simple", Pools: []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}}})
//
//	p.Reload(&proxy.Config{
//	    Mode:  "simple",
//	    Pools: []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
//	})
func (p *Proxy) Reload(config *Config) {
	if p == nil || config == nil {
		return
	}
	if result := config.Validate(); !result.OK {
		return
	}
	normalizeConfigValues(config)
	p.configMu.Lock()
	poolsChanged := p.config == nil || !reflect.DeepEqual(p.config.Pools, config.Pools)
	workersChanged := p.config == nil || p.config.Workers != config.Workers
	httpChanged := p.config == nil || monitoringConfigChanged(p.config.HTTP, config.HTTP)
	nextWorkersMode := config.Workers
	if p.config == nil {
		p.config = config
	} else {
		preservedBind := append([]BindAddr(nil), p.config.Bind...)
		// Splitter wiring is established at start-up, so reload only swaps the
		// knobs that live subsystems can absorb without reconnecting listeners.
		preservedMode := p.config.Mode
		preservedConfigPath := p.config.configPath
		*p.config = *config
		p.config.Bind = preservedBind
		p.config.Mode = preservedMode
		p.config.configPath = preservedConfigPath
	}
	p.configMu.Unlock()
	if workersChanged && p.workers != nil {
		p.workers.ResetMode(nextWorkersMode, p.activeMiners())
	}
	if p.customDiff != nil {
		p.customDiff.globalDiff.Store(config.CustomDiff)
	}
	p.reloadCustomDiff(config.CustomDiff)
	if p.customDiffBuckets != nil {
		p.customDiffBuckets.SetEnabled(config.CustomDiffStats)
	}
	if p.rateLimit == nil {
		p.rateLimit = NewRateLimiter(config.RateLimit)
	} else {
		p.rateLimit.UpdateConfig(config.RateLimit)
	}
	if p.accessLog != nil {
		p.accessLog.SetPath(config.AccessLogFile)
	}
	if p.shareLog != nil {
		p.shareLog.SetPath(config.ShareLogFile)
	}
	p.reloadWatcher(config.Watch)
	if httpChanged {
		p.reloadMonitoringServer()
	}
	if poolsChanged {
		if reloadable, ok := p.splitter.(interface{ ReloadPools() }); ok {
			reloadable.ReloadPools()
		}
	}
}

func (p *Proxy) reloadCustomDiff(globalDiff uint64) {
	if p == nil {
		return
	}
	for _, miner := range p.activeMiners() {
		if miner == nil {
			continue
		}
		miner.mu.Lock()
		miner.globalDiff = globalDiff
		customDiffFromLogin := miner.customDiffFromLogin
		if !customDiffFromLogin {
			miner.customDiff = globalDiff
		}
		state := miner.state
		miner.mu.Unlock()
		if customDiffFromLogin {
			continue
		}
		switch state {
		case MinerStateWaitReady, MinerStateReady:
			job := miner.CurrentJob()
			if job.IsValid() {
				miner.ForwardJob(job, job.Algo)
			}
		}
	}
}

func (p *Proxy) reloadWatcher(enabled bool) {
	if p == nil || p.config == nil || p.config.configPath == "" {
		return
	}
	if enabled {
		p.lifecycleMu.RLock()
		watcher := p.watcher
		p.lifecycleMu.RUnlock()
		if watcher != nil {
			return
		}
		watcher = NewConfigWatcher(p.config.configPath, p.Reload)
		p.lifecycleMu.Lock()
		p.watcher = watcher
		p.lifecycleMu.Unlock()
		watcher.Start()
		return
	}
	p.lifecycleMu.RLock()
	watcher := p.watcher
	p.lifecycleMu.RUnlock()
	if watcher == nil {
		return
	}
	watcher.Stop()
	p.lifecycleMu.Lock()
	p.watcher = nil
	p.lifecycleMu.Unlock()
}

func (p *Proxy) buildServers() Result {
	if p == nil || p.config == nil {
		return newSuccessResult()
	}
	servers := make([]*Server, 0, len(p.config.Bind))
	for _, bind := range p.config.Bind {
		var tlsConfig *tls.Config
		if bind.TLS {
			if !p.config.TLS.Enabled {
				for _, server := range servers {
					if server != nil {
						server.Stop()
					}
				}
				return newErrorResult(NewScopedError("proxy.server", "tls listener requires tls to be enabled", nil))
			}
			result := Result{}
			tlsConfig, result = buildTLSConfig(p.config.TLS)
			if !result.OK {
				for _, server := range servers {
					if server != nil {
						server.Stop()
					}
				}
				return result
			}
		}
		server, result := NewServer(bind, tlsConfig, p.rateLimit, p.acceptMiner)
		if !result.OK {
			for _, server := range servers {
				if server != nil {
					server.Stop()
				}
			}
			return result
		}
		servers = append(servers, server)
	}
	p.lifecycleMu.Lock()
	p.servers = servers
	p.lifecycleMu.Unlock()
	return newSuccessResult()
}

// ServerListenerAddr returns the bound listener address for one configured server.
func (p *Proxy) ServerListenerAddr(index int) string {
	if p == nil || index < 0 {
		return ""
	}
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	if index >= len(p.servers) || p.servers[index] == nil || p.servers[index].listener == nil {
		return ""
	}
	return p.servers[index].listener.Addr().String()
}

func (p *Proxy) onShareSettled(Event) {
	if p == nil {
		return
	}
	for {
		current := p.submitCount.Load()
		if current == 0 {
			return
		}
		if p.submitCount.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func (p *Proxy) acceptMiner(conn net.Conn, localPort uint16) {
	if p == nil {
		_ = conn.Close()
		return
	}
	p.configMu.RLock()
	accessPassword := ""
	algoExtension := false
	customDiff := uint64(0)
	mode := ""
	if p.config != nil {
		accessPassword = p.config.AccessPassword
		algoExtension = p.config.AlgoExtension
		customDiff = p.config.CustomDiff
		mode = p.config.Mode
	}
	p.configMu.RUnlock()
	if p.stats != nil {
		p.stats.connections.Add(1)
	}
	miner := NewMiner(conn, localPort, nil)
	miner.accessPassword = accessPassword
	miner.algoEnabled = algoExtension
	miner.mu.Lock()
	miner.globalDiff = customDiff
	miner.mu.Unlock()
	miner.extNH = equalFoldString(mode, "nicehash")
	miner.onLogin = func(m *Miner) {
		if p.splitter != nil {
			p.splitter.OnLogin(&LoginEvent{Miner: m})
		}
	}
	loginEvent := func(m *Miner) {
		if p.events != nil {
			p.events.Dispatch(Event{Type: EventLogin, Miner: m})
		}
	}
	miner.onLoginReady = loginEvent
	miner.onLoginEvent = loginEvent
	miner.onSubmit = func(m *Miner, event *SubmitEvent) {
		if p.splitter != nil {
			if _, ok := p.splitter.(*noopSplitter); !ok {
				p.submitCount.Add(1)
			}
			p.splitter.OnSubmit(event)
		}
	}
	miner.onClose = func(m *Miner) {
		if p.events != nil {
			p.events.Dispatch(Event{Type: EventClose, Miner: m})
		}
		if p.splitter != nil {
			p.splitter.OnClose(&CloseEvent{Miner: m})
		}
		p.minersMu.Lock()
		delete(p.miners, m.id)
		p.minersMu.Unlock()
	}
	p.minersMu.Lock()
	p.miners[miner.id] = miner
	p.minersMu.Unlock()
	miner.Start()
}

func buildTLSConfig(cfg TLSConfig) (*tls.Config, Result) {
	if !cfg.Enabled {
		return nil, newSuccessResult()
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, newErrorResult(NewScopedError("proxy.tls", "tls certificate or key path is empty", nil))
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, newErrorResult(NewScopedError("proxy.tls", "load certificate failed", err))
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	applyTLSProtocols(tlsConfig, cfg.Protocols)
	applyTLSCiphers(tlsConfig, cfg.Ciphers)
	return tlsConfig, newSuccessResult()
}

func applyTLSProtocols(tlsConfig *tls.Config, protocols string) {
	if tlsConfig == nil || trimString(protocols) == "" {
		return
	}
	parts := splitTLSConfigList(protocols)
	minVersion := uint16(0)
	maxVersion := uint16(0)
	for _, part := range parts {
		if part == "" {
			continue
		}
		if containsString(part, "-") {
			bounds := splitStringN(part, "-", 2)
			low := parseTLSVersion(bounds[0])
			high := parseTLSVersion(bounds[1])
			if low == 0 || high == 0 {
				continue
			}
			if minVersion == 0 || low < minVersion {
				minVersion = low
			}
			if high > maxVersion {
				maxVersion = high
			}
			continue
		}
		version := parseTLSVersion(part)
		if version == 0 {
			continue
		}
		if minVersion == 0 || version < minVersion {
			minVersion = version
		}
		if version > maxVersion {
			maxVersion = version
		}
	}
	if minVersion != 0 {
		tlsConfig.MinVersion = minVersion
	}
	if maxVersion != 0 {
		tlsConfig.MaxVersion = maxVersion
	}
}

func applyTLSCiphers(tlsConfig *tls.Config, ciphers string) {
	if tlsConfig == nil || trimString(ciphers) == "" {
		return
	}
	parts := splitTLSConfigList(ciphers)
	for _, part := range parts {
		if id, ok := lookupTLSCipherSuite(part); ok {
			tlsConfig.CipherSuites = append(tlsConfig.CipherSuites, id)
		}
	}
}

func lookupTLSCipherSuite(value string) (uint16, bool) {
	name := lowerString(trimString(value))
	if name == "" {
		return 0, false
	}

	allowed := map[string]uint16{}
	for _, suite := range tls.CipherSuites() {
		allowed[lowerString(suite.Name)] = suite.ID
	}
	for _, suite := range tls.InsecureCipherSuites() {
		allowed[lowerString(suite.Name)] = suite.ID
	}
	if id, ok := allowed[name]; ok {
		return id, true
	}

	if alias, ok := tlsCipherSuiteAliases[name]; ok {
		if id, ok := allowed[lowerString(alias)]; ok {
			return id, true
		}
	}

	return 0, false
}

var tlsCipherSuiteAliases = map[string]string{
	"ecdhe-ecdsa-aes128-gcm-sha256": "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
	"ecdhe-rsa-aes128-gcm-sha256":   "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
	"ecdhe-ecdsa-aes256-gcm-sha384": "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
	"ecdhe-rsa-aes256-gcm-sha384":   "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
	"ecdhe-ecdsa-chacha20-poly1305": "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305",
	"ecdhe-rsa-chacha20-poly1305":   "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305",
	"dhe-rsa-aes128-gcm-sha256":     "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
	"dhe-rsa-aes256-gcm-sha384":     "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
	"aes128-gcm-sha256":             "TLS_RSA_WITH_AES_128_GCM_SHA256",
	"aes256-gcm-sha384":             "TLS_RSA_WITH_AES_256_GCM_SHA384",
	"aes128-sha":                    "TLS_RSA_WITH_AES_128_CBC_SHA",
	"aes256-sha":                    "TLS_RSA_WITH_AES_256_CBC_SHA",
	"ecdhe-ecdsa-aes128-sha256":     "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
	"ecdhe-rsa-aes128-sha256":       "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
	"ecdhe-ecdsa-aes256-sha384":     "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
	"ecdhe-rsa-aes256-sha384":       "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
}

func splitTLSConfigList(value string) []string {
	return splitFieldsBySeparators(value)
}

func parseTLSVersion(value string) uint16 {
	switch lowerString(trimString(value)) {
	case "tls1.0", "tlsv1.0", "tls1", "tlsv1", "1.0", "1", "tls10", "tlsv10":
		return tls.VersionTLS10
	case "tls1.1", "tlsv1.1", "1.1", "tls11", "tlsv11":
		return tls.VersionTLS11
	case "tls1.2", "tlsv1.2", "1.2", "tls12", "tlsv12":
		return tls.VersionTLS12
	case "tls1.3", "tlsv1.3", "1.3", "tls13", "tlsv13":
		return tls.VersionTLS13
	default:
		return 0
	}
}

func (p *Proxy) startMonitoringServer() bool {
	if p == nil || p.config == nil {
		return false
	}
	httpCfg := p.currentHTTPConfig()
	if !httpCfg.Enabled {
		return false
	}
	if trimString(httpCfg.Host) == "" {
		return false
	}
	if !isLoopbackHTTPHost(httpCfg.Host) && trimString(httpCfg.AccessToken) == "" {
		return false
	}
	mux := http.NewServeMux()
	p.registerMonitoringRoute(mux, MonitoringRouteSummary, func() any { return p.SummaryDocument() })
	p.registerMonitoringRoute(mux, MonitoringRouteWorkers, func() any { return p.WorkersDocument() })
	p.registerMonitoringRoute(mux, MonitoringRouteMiners, func() any { return p.MinersDocument() })
	addr := net.JoinHostPort(httpCfg.Host, strconv.Itoa(int(httpCfg.Port)))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
	p.lifecycleMu.Lock()
	p.httpServer = httpServer
	p.lifecycleMu.Unlock()
	go func() {
		err := httpServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			p.Stop()
		}
	}()
	return true
}

func (p *Proxy) reloadMonitoringServer() {
	if p == nil || !p.isRunning() {
		return
	}
	httpCfg := p.currentHTTPConfig()
	if !httpCfg.Enabled {
		p.stopMonitoringServer()
		return
	}
	p.stopMonitoringServer()
	_ = p.startMonitoringServer()
}

func (p *Proxy) stopMonitoringServer() {
	if p == nil {
		return
	}
	p.lifecycleMu.Lock()
	httpServer := p.httpServer
	p.httpServer = nil
	p.lifecycleMu.Unlock()
	if httpServer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), submitDrainTimeout)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
}

func (p *Proxy) isRunning() bool {
	if p == nil {
		return false
	}
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	return p.ticker != nil
}

func (p *Proxy) registerMonitoringRoute(mux *http.ServeMux, pattern string, renderDocument func() any) {
	if p == nil || mux == nil || renderDocument == nil {
		return
	}
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if status, ok := p.AllowMonitoringRequest(r); !ok {
			switch status {
			case http.StatusUnauthorized:
				w.Header().Set("WWW-Authenticate", "Bearer")
			case http.StatusMethodNotAllowed:
				w.Header().Set("Allow", http.MethodGet)
			}
			w.WriteHeader(status)
			return
		}
		p.writeJSONResponse(w, renderDocument())
	})
}

// AllowMonitoringRequest applies the configured monitoring API access checks.
// Monitoring documents are GET-only regardless of HTTP.Restricted.
//
//	status, ok := p.AllowMonitoringRequest(request)
func (p *Proxy) AllowMonitoringRequest(r *http.Request) (int, bool) {
	if p == nil || p.config == nil {
		return http.StatusServiceUnavailable, false
	}
	if r == nil {
		return http.StatusServiceUnavailable, false
	}
	httpCfg := p.currentHTTPConfig()
	if r.Method != http.MethodGet {
		return http.StatusMethodNotAllowed, false
	}
	if host := trimString(httpCfg.Host); host != "" && !isLoopbackHTTPHost(host) && trimString(httpCfg.AccessToken) == "" {
		return http.StatusUnauthorized, false
	}
	if token := httpCfg.AccessToken; token != "" {
		parts := splitStringN(r.Header.Get("Authorization"), " ", 2)
		if len(parts) != 2 || !equalFoldString(parts[0], "bearer") || !secureStringEqual(parts[1], token) {
			return http.StatusUnauthorized, false
		}
	}
	return http.StatusOK, true
}

func (p *Proxy) currentHTTPConfig() HTTPConfig {
	if p == nil || p.config == nil {
		return HTTPConfig{}
	}
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return p.config.HTTP
}

func (p *Proxy) writeJSONResponse(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(jsonMarshalString(payload) + "\n"))
}

// SummaryDocument builds the RFC-shaped /1/summary response body.
//
//	doc := p.SummaryDocument()
//	_ = doc.Results.Accepted
func (p *Proxy) SummaryDocument() SummaryDocument {
	summary := p.Summary()
	now, max := p.MinerCount()
	upstreams := p.Upstreams()
	return SummaryDocument{
		Version: SummaryDocumentVersion,
		Mode:    p.Mode(),
		Hashrate: HashrateDocument{
			Total: summary.Hashrate,
		},
		Miners: MinersCountDocument{
			Now: now,
			Max: max,
		},
		Workers:   uint64(len(p.WorkerRecords())),
		Upstreams: UpstreamDocument{Active: upstreams.Active, Sleep: upstreams.Sleep, Error: upstreams.Error, Total: upstreams.Total, Ratio: upstreamRatio(now, upstreams)},
		Results: ResultsDocument{
			Accepted:    summary.Accepted,
			Rejected:    summary.Rejected,
			Invalid:     summary.Invalid,
			Expired:     summary.Expired,
			AvgTime:     summary.AvgTime,
			Latency:     summary.AvgLatency,
			HashesTotal: summary.Hashes,
			Best:        summary.TopDiff,
		},
	}
}

// WorkersDocument builds the RFC-shaped /1/workers response body.
//
//	doc := p.WorkersDocument()
//	_ = doc.Workers[0][0]
func (p *Proxy) WorkersDocument() WorkersDocument {
	records := p.WorkerRecords()
	rows := make([]WorkerRow, 0, len(records))
	for _, record := range records {
		hashrates := make([]float64, len(workerHashrateWindows))
		for index, seconds := range workerHashrateWindows {
			hashrates[index] = record.Hashrate(seconds)
		}
		rows = append(rows, WorkerRow{
			record.Name,
			record.LastIP,
			record.Connections,
			record.Accepted,
			record.Rejected,
			record.Invalid,
			record.Hashes,
			unixOrZero(record.LastHashAt),
			hashrates[0],
			hashrates[1],
			hashrates[2],
			hashrates[3],
			hashrates[4],
		})
	}
	return WorkersDocument{
		Mode:    string(p.WorkersMode()),
		Workers: rows,
	}
}

// MinersDocument builds the RFC-shaped /1/miners response body.
//
//	doc := p.MinersDocument()
//	_ = doc.Miners[0][7]
func (p *Proxy) MinersDocument() MinersDocument {
	records := p.MinerSnapshots()
	rows := make([]MinerRow, 0, len(records))
	for _, miner := range records {
		rows = append(rows, MinerRow{
			miner.ID,
			miner.IP,
			miner.TX,
			miner.RX,
			miner.State,
			miner.Diff,
			miner.User,
			"********",
			miner.RigID,
			miner.Agent,
		})
	}
	return MinersDocument{
		Format: append([]string(nil), MinersDocumentFormat...),
		Miners: rows,
	}
}

func upstreamRatio(now uint64, upstreams UpstreamStats) float64 {
	if upstreams.Total == 0 {
		return 0
	}
	return float64(now) / float64(upstreams.Total)
}

func unixOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.Unix()
}

func monitoringConfigChanged(current, next HTTPConfig) bool {
	if current.Enabled != next.Enabled {
		return true
	}
	if !current.Enabled && !next.Enabled {
		return false
	}
	return current.Host != next.Host || current.Port != next.Port
}

func secureStringEqual(a, b string) bool {
	max := len(a)
	if len(b) > max {
		max = len(b)
	}
	var diff byte
	diff |= byte(len(a) ^ len(b))
	for index := 0; index < max; index++ {
		var left byte
		var right byte
		if index < len(a) {
			left = a[index]
		}
		if index < len(b) {
			right = b[index]
		}
		diff |= left ^ right
	}
	return subtle.ConstantTimeByteEq(diff, 0) == 1
}

func NewMiner(conn net.Conn, localPort uint16, tlsCfg *tls.Config) *Miner {
	if tlsCfg != nil {
		if _, ok := conn.(*tls.Conn); !ok {
			conn = tls.Server(conn, tlsCfg)
		}
	}
	miner := &Miner{
		id:             nextMinerID(),
		state:          MinerStateWaitLogin,
		localPort:      localPort,
		mapperID:       -1,
		routeID:        -1,
		connectedAt:    time.Now().UTC(),
		lastActivityAt: time.Now().UTC(),
		conn:           conn,
	}
	if tlsConn, ok := conn.(*tls.Conn); ok {
		miner.tlsConn = tlsConn
	}
	if remote := conn.RemoteAddr(); remote != nil {
		miner.remoteAddr = remote.String()
		miner.ip = hostOnly(miner.remoteAddr)
	}
	return miner
}

// SetID assigns the miner's internal ID. Used by NonceStorage tests.
//
//	m.SetID(42)
func (m *Miner) SetID(id int64) {
	m.mu.Lock()
	m.id = id
	m.mu.Unlock()
}

// ID returns the miner's monotonically increasing per-process identifier.
//
//	id := m.ID() // 42
func (m *Miner) ID() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.id
}

// SetMapperID assigns which NonceMapper owns this miner in NiceHash mode.
//
//	m.SetMapperID(0) // assigned to mapper 0
func (m *Miner) SetMapperID(id int64) {
	m.mu.Lock()
	m.mapperID = id
	m.mu.Unlock()
}

// MapperID returns the owning NonceMapper's ID, or -1 if unassigned.
//
//	if m.MapperID() < 0 { /* miner not assigned to any mapper */ }
func (m *Miner) MapperID() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mapperID
}

// SetRouteID assigns the SimpleMapper ID in simple mode.
//
//	m.SetRouteID(3)
func (m *Miner) SetRouteID(id int64) {
	m.mu.Lock()
	m.routeID = id
	m.mu.Unlock()
}

// RouteID returns the SimpleMapper ID, or -1 if unassigned.
//
//	if m.RouteID() < 0 { /* miner not routed */ }
func (m *Miner) RouteID() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.routeID
}

// SetExtendedNiceHash enables or disables NiceHash nonce-splitting mode for this miner.
//
//	m.SetExtendedNiceHash(true)
func (m *Miner) SetExtendedNiceHash(enabled bool) {
	m.mu.Lock()
	m.extNH = enabled
	m.mu.Unlock()
}

// ExtendedNiceHash reports whether this miner is in NiceHash nonce-splitting mode.
//
//	if m.ExtendedNiceHash() { /* blob byte 39 is patched */ }
func (m *Miner) ExtendedNiceHash() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.extNH
}

// SetCurrentJob assigns the current pool work unit to this miner.
//
//	m.SetCurrentJob(proxy.Job{Blob: "...", JobID: "job-1"})
func (m *Miner) SetCurrentJob(job Job) {
	m.mu.Lock()
	m.currentJob = job
	m.mu.Unlock()
}

// CurrentJob returns the last job forwarded to this miner.
//
//	job := m.CurrentJob()
//	if job.IsValid() { /* miner has a valid job */ }
func (m *Miner) CurrentJob() Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentJob
}

// LoginAlgos returns the algorithm list sent by the miner during login, or nil if empty.
//
//	algos := m.LoginAlgos() // ["cn/r", "rx/0"]
func (m *Miner) LoginAlgos() []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.loginAlgos) == 0 {
		return nil
	}
	return append([]string(nil), m.loginAlgos...)
}

// FixedByte returns the NiceHash slot index (0-255) assigned to this miner.
//
//	slot := m.FixedByte() // 0x2A
func (m *Miner) FixedByte() uint8 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.fixedByte
}

// SetFixedByte assigns the NiceHash slot index for this miner.
//
//	m.SetFixedByte(0x2A)
func (m *Miner) SetFixedByte(value uint8) {
	m.mu.Lock()
	m.fixedByte = value
	m.mu.Unlock()
}

// IP returns the remote IP address (without port) for logging.
//
//	ip := m.IP() // "10.0.0.1"
func (m *Miner) IP() string {
	return m.ip
}

// RemoteAddr returns the full remote address including port.
//
//	addr := m.RemoteAddr() // "10.0.0.1:49152"
func (m *Miner) RemoteAddr() string {
	if m == nil {
		return ""
	}
	return m.remoteAddr
}

// User returns the wallet address from login params, with any custom diff suffix stripped.
//
//	user := m.User() // "WALLET" (even if login was "WALLET+50000")
func (m *Miner) User() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.user
}

// Password returns the login params.pass value.
//
//	pass := m.Password() // "x"
func (m *Miner) Password() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.password
}

// Agent returns the mining software identifier from login params.
//
//	agent := m.Agent() // "XMRig/6.21.0"
func (m *Miner) Agent() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.agent
}

// RigID returns the optional rigid extension field from login params.
//
//	rigid := m.RigID() // "rig-alpha"
func (m *Miner) RigID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rigID
}

// RX returns the total bytes received from this miner.
//
//	rx := m.RX() // 4096
func (m *Miner) RX() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rx
}

// TX returns the total bytes sent to this miner.
//
//	tx := m.TX() // 8192
func (m *Miner) TX() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tx
}

// Diff returns the last difficulty sent to this miner from the pool.
//
//	diff := m.Diff() // 100000
func (m *Miner) Diff() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.diff
}

// State returns the current lifecycle state of this miner connection.
//
//	if m.State() == proxy.MinerStateReady { /* miner is active */ }
func (m *Miner) State() MinerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

func (m *Miner) sessionID() string {
	if m == nil {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rpcID
}

func (m *Miner) supportsAlgoExtension() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.algoEnabled && m.extAlgo
}

// Start launches the read loop.
func (m *Miner) Start() {
	if m == nil {
		return
	}
	go m.readLoop()
}

func (m *Miner) readLoop() {
	defer func() {
		if m.onClose != nil {
			m.onClose(m)
		}
	}()

	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return
	}
	reader := bufio.NewReaderSize(conn, maxStratumLineLength+1)
	for {
		if m.State() == MinerStateClosing {
			return
		}
		if timeout := m.readTimeout(); timeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(timeout))
		} else if m.State() == MinerStateWaitLogin {
			m.Close()
			return
		}
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			m.Close()
			return
		}
		if isPrefix || len(line) > maxStratumLineLength {
			m.Close()
			return
		}
		if len(line) == 0 {
			continue
		}
		m.mu.Lock()
		m.rx += uint64(len(line) + 1)
		m.mu.Unlock()
		if !m.handleLine(line) {
			m.Close()
			return
		}
	}
}

func (m *Miner) readTimeout() time.Duration {
	m.mu.RLock()
	lastActivityAt := m.lastActivityAt
	connectedAt := m.connectedAt
	m.mu.RUnlock()
	switch m.State() {
	case MinerStateWaitLogin:
		if connectedAt.IsZero() {
			return minerLoginTimeout
		}
		return time.Until(connectedAt.Add(minerLoginTimeout))
	case MinerStateWaitReady, MinerStateReady:
		if lastActivityAt.IsZero() {
			return minerReadyTimeout
		}
		return time.Until(lastActivityAt.Add(minerReadyTimeout))
	default:
		return 0
	}
}

type stratumRequest struct {
	ID      any    `json:"id"`
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

func (m *Miner) handleLine(line []byte) bool {
	var request stratumRequest
	if !jsonUnmarshalBytes(line, &request) {
		return false
	}
	switch request.Method {
	case "login":
		m.handleLogin(request)
	case "submit":
		m.handleSubmit(request)
	case "keepalived":
		m.handleKeepalived(request)
	}
	return true
}

type loginParams struct {
	Login string   `json:"login"`
	Pass  string   `json:"pass"`
	Agent string   `json:"agent"`
	Algo  []string `json:"algo"`
	RigID string   `json:"rigid"`
}

func (m *Miner) handleLogin(request stratumRequest) {
	if m.State() != MinerStateWaitLogin {
		return
	}
	paramsMap := valueMap(request.Params)
	if paramsMap == nil {
		var params loginParams
		if data, ok := request.Params.([]byte); ok && jsonUnmarshalBytes(data, &params) {
			paramsMap = map[string]any{
				"login": params.Login,
				"pass":  params.Pass,
				"agent": params.Agent,
				"algo":  params.Algo,
				"rigid": params.RigID,
			}
		}
	}
	params := loginParams{
		Login: valueString(paramsMap["login"]),
		Pass:  valueString(paramsMap["pass"]),
		Agent: valueString(paramsMap["agent"]),
		Algo:  valueStringSlice(paramsMap["algo"]),
		RigID: valueString(paramsMap["rigid"]),
	}
	if trimString(params.Login) == "" {
		m.rejectLogin(requestID(request.ID), "Invalid payment address provided")
		return
	}
	m.mu.RLock()
	accessPassword := m.accessPassword
	globalDiff := m.globalDiff
	extNH := m.extNH
	m.mu.RUnlock()
	if accessPassword != "" && !secureStringEqual(params.Pass, accessPassword) {
		m.rejectLogin(requestID(request.ID), "Invalid password")
		return
	}
	resolved := resolveLoginCustomDiff(params.Login, globalDiff)
	m.mu.Lock()
	m.user = resolved.user
	m.customDiff = resolved.diff
	m.customDiffFromLogin = resolved.fromLogin
	m.customDiffResolved = true
	m.password = params.Pass
	m.agent = params.Agent
	m.rigID = params.RigID
	m.loginAlgos = append([]string(nil), params.Algo...)
	m.extAlgo = len(m.loginAlgos) > 0
	m.rpcID = generateUUID()
	m.mu.Unlock()
	if m.onLogin != nil {
		m.onLogin(m)
	}
	if m.State() == MinerStateClosing {
		return
	}
	if extNH {
		if m.MapperID() < 0 {
			m.rejectLogin(requestID(request.ID), "Proxy is full, try again later")
			return
		}
	} else if m.RouteID() < 0 {
		m.rejectLogin(requestID(request.ID), "Proxy is unavailable, try again later")
		return
	}
	m.mu.Lock()
	m.state = MinerStateWaitReady
	m.mu.Unlock()
	m.touchActivity()
	if pinger := m.onLoginEvent; pinger != nil {
		pinger(m)
	} else if pinger := m.onLoginReady; pinger != nil {
		pinger(m)
	}
	if m.State() == MinerStateClosing {
		return
	}
	m.replyLoginSuccess(requestID(request.ID))
}

func (m *Miner) rejectLogin(id int64, message string) {
	if m == nil {
		return
	}
	m.ReplyWithError(id, message)
	m.closeTransport()
}

type resolvedCustomDiff struct {
	user      string
	diff      uint64
	fromLogin bool
}

func resolveLoginCustomDiff(login string, globalDiff uint64) resolvedCustomDiff {
	plus := lastIndexByte(login, '+')
	if plus >= 0 && plus < len(login)-1 {
		suffix := login[plus+1:]
		if isDecimalDigits(suffix) {
			if parsed, err := strconv.ParseUint(suffix, 10, 64); err == nil {
				return resolvedCustomDiff{user: login[:plus], diff: parsed, fromLogin: true}
			}
		}
		return resolvedCustomDiff{user: login}
	}
	if globalDiff > 0 {
		return resolvedCustomDiff{user: login, diff: globalDiff}
	}
	return resolvedCustomDiff{user: login}
}

func isDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (m *Miner) handleSubmit(request stratumRequest) {
	if m.State() != MinerStateReady {
		m.ReplyWithError(requestID(request.ID), "Unauthenticated")
		return
	}
	var params struct {
		ID     string `json:"id"`
		JobID  string `json:"job_id"`
		Nonce  string `json:"nonce"`
		Result string `json:"result"`
		Algo   string `json:"algo"`
	}
	paramsMap := valueMap(request.Params)
	if paramsMap == nil {
		if data, ok := request.Params.([]byte); ok {
			_ = jsonUnmarshalBytes(data, &params)
		}
	} else {
		params.ID = valueString(paramsMap["id"])
		params.JobID = valueString(paramsMap["job_id"])
		params.Nonce = valueString(paramsMap["nonce"])
		params.Result = valueString(paramsMap["result"])
		params.Algo = valueString(paramsMap["algo"])
	}
	m.mu.RLock()
	rpcID := m.rpcID
	m.mu.RUnlock()
	if params.ID != rpcID {
		m.ReplyWithError(requestID(request.ID), "Unauthenticated")
		return
	}
	if params.JobID == "" {
		m.ReplyWithError(requestID(request.ID), "Missing job id")
		return
	}
	if !isLowerHex8(params.Nonce) {
		m.ReplyWithError(requestID(request.ID), "Invalid nonce")
		return
	}
	if m.onSubmit != nil {
		m.onSubmit(m, &SubmitEvent{
			Miner:     m,
			JobID:     params.JobID,
			Nonce:     params.Nonce,
			Result:    params.Result,
			Algo:      params.Algo,
			RequestID: requestID(request.ID),
		})
	}
	m.touchActivity()
}

func (m *Miner) handleKeepalived(request stratumRequest) {
	m.touchActivity()
	m.Success(requestID(request.ID), "KEEPALIVED")
}

func requestID(id any) int64 {
	switch v := id.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	default:
		return 0
	}
}

func isLowerHex8(value string) bool {
	if len(value) != 8 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func (m *Miner) ForwardJob(job Job, algo string) {
	if m == nil || !job.IsValid() {
		return
	}
	m.mu.Lock()
	m.currentJob = job
	m.mu.Unlock()
	renderedJob, effectiveAlgo := m.renderJob(job, algo)
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "job",
		"params":  buildMinerJobPayload(renderedJob, m.sessionID(), m.supportsAlgoExtension(), effectiveAlgo),
	}
	_ = m.writeJSON(payload)
	m.touchActivity()
	m.mu.Lock()
	if m.state == MinerStateWaitReady {
		m.state = MinerStateReady
	}
	m.mu.Unlock()
}

func (m *Miner) replyLoginSuccess(id int64) {
	if m == nil {
		return
	}
	result := map[string]any{
		"id":     m.sessionID(),
		"status": "OK",
	}
	if m.supportsAlgoExtension() {
		result["extensions"] = []string{"algo"}
	}
	if job := m.CurrentJob(); job.IsValid() {
		renderedJob, effectiveAlgo := m.renderJob(job, job.Algo)
		result["job"] = buildMinerJobPayload(renderedJob, m.sessionID(), m.supportsAlgoExtension(), effectiveAlgo)
		m.touchActivity()
		m.mu.Lock()
		m.state = MinerStateReady
		m.mu.Unlock()
	}
	payload := map[string]any{
		"id":      id,
		"jsonrpc": "2.0",
		"error":   nil,
		"result":  result,
	}
	_ = m.writeJSON(payload)
}

func (m *Miner) renderJob(job Job, algo string) (Job, string) {
	if m == nil {
		return job, algo
	}
	rendered := job
	m.mu.RLock()
	fixedByte := m.fixedByte
	customDiff := m.customDiff
	extNH := m.extNH
	m.mu.RUnlock()
	if algo == "" {
		algo = job.Algo
	}
	if extNH {
		rendered.Blob = job.BlobWithFixedByte(fixedByte)
	}
	effectiveDiff := job.DifficultyFromTarget()
	if customDiff > 0 && effectiveDiff > 0 && effectiveDiff > customDiff {
		rendered.Target = targetFromDifficulty(customDiff)
		effectiveDiff = rendered.DifficultyFromTarget()
	}
	m.mu.Lock()
	m.diff = effectiveDiff
	m.mu.Unlock()
	return rendered, algo
}

func buildMinerJobPayload(job Job, sessionID string, includeAlgo bool, algo string) map[string]any {
	payload := map[string]any{
		"blob":   job.Blob,
		"job_id": job.JobID,
		"target": job.Target,
		"id":     sessionID,
	}
	if includeAlgo && algo != "" {
		payload["algo"] = algo
	}
	if job.Height > 0 {
		payload["height"] = job.Height
	}
	if job.SeedHash != "" {
		payload["seed_hash"] = job.SeedHash
	}
	return payload
}

func (m *Miner) ReplyWithError(id int64, message string) {
	if m == nil {
		return
	}
	payload := map[string]any{
		"id":      id,
		"jsonrpc": "2.0",
		"error": map[string]any{
			"code":    -1,
			"message": message,
		},
	}
	_ = m.writeJSON(payload)
}

func (m *Miner) Success(id int64, status string) {
	if m == nil {
		return
	}
	payload := map[string]any{
		"id":      id,
		"jsonrpc": "2.0",
		"error":   nil,
		"result": map[string]any{
			"status": status,
		},
	}
	_ = m.writeJSON(payload)
}

func (m *Miner) touchActivity() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.lastActivityAt = time.Now().UTC()
	state := m.state
	conn := m.conn
	m.mu.Unlock()
	if state == MinerStateWaitLogin {
		return
	}
	if conn != nil {
		_ = conn.SetReadDeadline(time.Now().Add(minerReadyTimeout))
	}
}

func (m *Miner) writeJSON(payload any) error {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return nil
	}
	data := []byte(jsonMarshalString(payload))
	data = append(data, '\n')
	var written int
	var err error
	if len(data) <= len(m.buf) {
		copy(m.buf[:], data)
		written, err = conn.Write(m.buf[:len(data)])
	} else {
		written, err = conn.Write(data)
	}
	m.mu.Lock()
	m.tx += uint64(written)
	m.mu.Unlock()
	if err != nil {
		m.Close()
	}
	return err
}

func (m *Miner) Close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.state = MinerStateClosing
		m.mu.Unlock()
		m.closeTransport()
	})
}

func (m *Miner) closeTransport() {
	if m == nil {
		return
	}
	m.mu.Lock()
	conn := m.conn
	m.conn = nil
	m.tlsConn = nil
	m.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// NewStats creates zeroed global metrics.
//
// stats := proxy.NewStats()
// _ = stats.Summary()
func NewStats() *Stats {
	stats := &Stats{startTime: time.Now().UTC(), latency: make([]uint16, 0, 1024)}
	stats.windows[HashrateWindow60s] = newTickWindow(60)
	stats.windows[HashrateWindow600s] = newTickWindow(600)
	stats.windows[HashrateWindow3600s] = newTickWindow(3600)
	stats.windows[HashrateWindow12h] = newTickWindow(43200)
	stats.windows[HashrateWindow24h] = newTickWindow(86400)
	return stats
}

// OnLogin increments the current miner count.
func (s *Stats) OnLogin(e Event) {
	if s == nil || e.Miner == nil {
		return
	}
	now := s.miners.Add(1)
	for {
		max := s.maxMiners.Load()
		if now <= max || s.maxMiners.CompareAndSwap(max, now) {
			break
		}
	}
}

// OnClose decrements the current miner count.
func (s *Stats) OnClose(e Event) {
	if s == nil || e.Miner == nil {
		return
	}
	for {
		current := s.miners.Load()
		if current == 0 || s.miners.CompareAndSwap(current, current-1) {
			return
		}
	}
}

// OnAccept records an accepted share.
//
// stats.OnAccept(proxy.Event{Diff: 100000, Latency: 82})
func (s *Stats) OnAccept(e Event) {
	if s == nil {
		return
	}
	s.accepted.Add(1)
	if e.Expired {
		s.expired.Add(1)
	}
	s.mu.Lock()
	if len(s.latency) < 10000 {
		s.latency = append(s.latency, e.Latency)
	}
	insertTopDiff(&s.topDiff, e.Diff)
	for i := range s.windows {
		if s.windows[i].size > 0 {
			s.windows[i].buckets[s.windows[i].pos] += e.Diff
		}
	}
	s.mu.Unlock()
	if e.Diff > 0 {
		s.hashes.Add(e.Diff)
	}
}

// OnReject records a rejected share.
//
// stats.OnReject(proxy.Event{Error: "Low difficulty share"})
func (s *Stats) OnReject(e Event) {
	if s == nil {
		return
	}
	s.rejected.Add(1)
	if isInvalidShareReason(e.Error) {
		s.invalid.Add(1)
	}
}

// Tick advances the rolling windows.
//
// stats.Tick()
func (s *Stats) Tick() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.windows {
		if s.windows[i].size == 0 {
			continue
		}
		s.windows[i].pos = (s.windows[i].pos + 1) % s.windows[i].size
		s.windows[i].buckets[s.windows[i].pos] = 0
	}
}

// Summary returns a snapshot of the current metrics.
//
//	summary := stats.Summary()
func (s *Stats) Summary() StatsSummary {
	if s == nil {
		return StatsSummary{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	summary := StatsSummary{
		Accepted: s.accepted.Load(),
		Rejected: s.rejected.Load(),
		Invalid:  s.invalid.Load(),
		Expired:  s.expired.Load(),
		Hashes:   s.hashes.Load(),
	}
	if summary.Accepted > 0 {
		uptime := uint64(time.Since(s.startTime).Seconds())
		if uptime == 0 {
			uptime = 1
		}
		summary.AvgTime = uint32(uptime / summary.Accepted)
	}
	if len(s.latency) > 0 {
		samples := append([]uint16(nil), s.latency...)
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		middle := len(samples) / 2
		if len(samples)%2 == 0 {
			left := uint32(samples[middle-1])
			right := uint32(samples[middle])
			summary.AvgLatency = (left + right) / 2
		} else {
			summary.AvgLatency = uint32(samples[middle])
		}
	}
	summary.TopDiff = s.topDiff
	for i := range s.windows {
		if i == HashrateWindowAll {
			uptime := time.Since(s.startTime).Seconds()
			if uptime > 0 {
				summary.Hashrate[i] = float64(summary.Hashes) / uptime
			}
			continue
		}
		total := uint64(0)
		for _, bucket := range s.windows[i].buckets {
			total += bucket
		}
		if s.windows[i].size > 0 {
			summary.Hashrate[i] = float64(total) / float64(s.windows[i].size)
		}
	}
	return summary
}

func insertTopDiff(top *[10]uint64, diff uint64) {
	if diff == 0 {
		return
	}
	for i := range top {
		if diff > top[i] {
			for j := len(top) - 1; j > i; j-- {
				top[j] = top[j-1]
			}
			top[i] = diff
			return
		}
	}
}

// NewWorkers creates a worker aggregate tracker.
//
//	workers := proxy.NewWorkers(proxy.WorkersByRigID, bus)
//	workers.OnLogin(proxy.Event{Miner: miner})
func NewWorkers(mode WorkersMode, eventBus *EventBus) *Workers {
	workers := &Workers{
		mode:      mode,
		nameIndex: make(map[string]int),
		idIndex:   make(map[int64]int),
	}
	workers.bindEvents(eventBus)
	return workers
}

func (w *Workers) bindEvents(eventBus *EventBus) {
	if w == nil || eventBus == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.subscribed {
		return
	}
	eventBus.Subscribe(EventLogin, w.OnLogin)
	eventBus.Subscribe(EventAccept, w.OnAccept)
	eventBus.Subscribe(EventReject, w.OnReject)
	eventBus.Subscribe(EventClose, w.OnClose)
	w.subscribed = true
}

func workerNameFor(mode WorkersMode, miner *Miner) string {
	if miner == nil {
		return ""
	}
	switch mode {
	case WorkersByRigID:
		if rigID := miner.RigID(); rigID != "" {
			return rigID
		}
		return miner.User()
	case WorkersByUser:
		return miner.User()
	case WorkersByPass:
		return miner.Password()
	case WorkersByAgent:
		return miner.Agent()
	case WorkersByIP:
		return miner.IP()
	case WorkersDisabled:
		return ""
	default:
		return miner.User()
	}
}

// OnLogin creates or updates a worker record.
//
//	workers.OnLogin(proxy.Event{Miner: miner})
func (w *Workers) OnLogin(e Event) {
	if w == nil || e.Miner == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.recordLoginLocked(e.Miner)
}

func newTickWindow(size int) tickWindow {
	return tickWindow{
		buckets: make([]uint64, size),
		size:    size,
	}
}

// ResetMode switches the worker identity strategy and rebuilds the live worker index.
//
//	workers.ResetMode(proxy.WorkersByUser, activeMiners)
func (w *Workers) ResetMode(mode WorkersMode, miners []*Miner) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mode = mode
	w.entries = nil
	w.nameIndex = make(map[string]int)
	w.idIndex = make(map[int64]int)
	sort.Slice(miners, func(i, j int) bool {
		if miners[i] == nil || miners[j] == nil {
			return miners[j] != nil
		}
		return miners[i].ID() < miners[j].ID()
	})
	for _, miner := range miners {
		w.recordLoginLocked(miner)
	}
}

// OnAccept updates the owning worker with the accepted share.
//
//	workers.OnAccept(proxy.Event{Miner: miner, Diff: 100000})
func (w *Workers) OnAccept(e Event) {
	if w == nil || e.Miner == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	index, ok := w.idIndex[e.Miner.id]
	if !ok || index < 0 || index >= len(w.entries) {
		return
	}
	record := &w.entries[index]
	record.Accepted++
	record.Hashes += e.Diff
	record.LastHashAt = time.Now().UTC()
	record.LastIP = e.Miner.ip
	for i := range record.windows {
		if record.windows[i].size > 0 {
			record.windows[i].buckets[record.windows[i].pos] += e.Diff
		}
	}
}

// OnReject updates the owning worker with the rejected share.
//
//	workers.OnReject(proxy.Event{Miner: miner, Error: "Low difficulty share"})
func (w *Workers) OnReject(e Event) {
	if w == nil || e.Miner == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	index, ok := w.idIndex[e.Miner.id]
	if !ok || index < 0 || index >= len(w.entries) {
		return
	}
	record := &w.entries[index]
	record.Rejected++
	if isInvalidShareReason(e.Error) {
		record.Invalid++
	}
	record.LastIP = e.Miner.ip
}

// OnClose removes the miner mapping from the worker table.
func (w *Workers) OnClose(e Event) {
	if w == nil || e.Miner == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	index, ok := w.idIndex[e.Miner.id]
	if ok && index >= 0 && index < len(w.entries) {
		record := &w.entries[index]
		if record.Connections > 0 {
			record.Connections--
		}
	}
	delete(w.idIndex, e.Miner.id)
}

// List returns a snapshot of all workers.
//
//	records := workers.List()
func (w *Workers) List() []WorkerRecord {
	if w == nil {
		return nil
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]WorkerRecord, len(w.entries))
	for i := range w.entries {
		out[i] = cloneWorkerRecord(w.entries[i])
	}
	return out
}

// Tick advances worker hash windows.
//
//	workers.Tick()
func (w *Workers) Tick() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.entries {
		for j := range w.entries[i].windows {
			if w.entries[i].windows[j].size == 0 {
				continue
			}
			w.entries[i].windows[j].pos = (w.entries[i].windows[j].pos + 1) % w.entries[i].windows[j].size
			w.entries[i].windows[j].buckets[w.entries[i].windows[j].pos] = 0
		}
	}
}

// Hashrate returns the configured worker hashrate window.
//
// hr60 := record.Hashrate(60)
func (r *WorkerRecord) Hashrate(seconds int) float64 {
	if r == nil || seconds <= 0 {
		return 0
	}
	index := -1
	switch seconds {
	case 60:
		index = 0
	case 600:
		index = 1
	case 3600:
		index = 2
	case 43200:
		index = 3
	case 86400:
		index = 4
	}
	if index < 0 || index >= len(r.windows) {
		return 0
	}
	total := uint64(0)
	for _, bucket := range r.windows[index].buckets {
		total += bucket
	}
	if seconds == 0 {
		return 0
	}
	return float64(total) / float64(seconds)
}

func cloneWorkerRecord(record WorkerRecord) WorkerRecord {
	cloned := record
	for i := range record.windows {
		if len(record.windows[i].buckets) == 0 {
			continue
		}
		cloned.windows[i].buckets = append([]uint64(nil), record.windows[i].buckets...)
	}
	return cloned
}

func (w *Workers) recordLoginLocked(miner *Miner) {
	if w == nil || miner == nil || w.mode == WorkersDisabled {
		return
	}
	name := workerNameFor(w.mode, miner)
	if name == "" {
		return
	}
	index, ok := w.nameIndex[name]
	if !ok {
		index = len(w.entries)
		record := WorkerRecord{Name: name}
		record.windows[0] = newTickWindow(60)
		record.windows[1] = newTickWindow(600)
		record.windows[2] = newTickWindow(3600)
		record.windows[3] = newTickWindow(43200)
		record.windows[4] = newTickWindow(86400)
		w.entries = append(w.entries, record)
		w.nameIndex[name] = index
	}
	record := &w.entries[index]
	record.Name = name
	record.LastIP = miner.ip
	record.Connections++
	w.idIndex[miner.id] = index
}

// Apply normalises one miner login at the same point the handshake does.
//
//	cd.Apply(&proxy.Miner{user: "WALLET+50000"})
func (cd *CustomDiff) Apply(miner *Miner) {
	if cd == nil || miner == nil {
		return
	}
	cd.OnLogin(Event{Miner: miner})
}

// NewServer constructs a server instance.
//
//	server, result := proxy.NewServer(bind, tlsConfig, limiter, func(conn net.Conn, port uint16) {
//	    _ = conn
//	    _ = port
//	})
func NewServer(bind BindAddr, tlsConfig *tls.Config, limiter *RateLimiter, onAccept func(net.Conn, uint16)) (*Server, Result) {
	if onAccept == nil {
		onAccept = func(net.Conn, uint16) {}
	}
	server := &Server{
		addr:      bind,
		tlsConfig: tlsConfig,
		limiter:   limiter,
		onAccept:  onAccept,
		done:      make(chan struct{}),
	}
	if result := server.listen(); !result.OK {
		return nil, result
	}
	return server, newSuccessResult()
}

// Start begins accepting connections in a goroutine.
//
//	server.Start()
func (s *Server) Start() {
	if s == nil {
		return
	}
	if result := s.listen(); !result.OK {
		return
	}
	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.done:
					return
				default:
					if ne, ok := err.(net.Error); ok && (ne.Temporary() || ne.Timeout()) {
						time.Sleep(100 * time.Millisecond)
					}
					continue
				}
			}
			if s.limiter != nil && !s.limiter.Allow(conn.RemoteAddr().String()) {
				_ = conn.Close()
				continue
			}
			if s.tlsConfig != nil {
				tlsConn := tls.Server(conn, s.tlsConfig)
				_ = conn.SetDeadline(time.Now().Add(minerTLSHandshakeTimeout))
				if err := tlsConn.Handshake(); err != nil {
					_ = conn.Close()
					continue
				}
				_ = tlsConn.SetDeadline(time.Time{})
				conn = tlsConn
			}
			if s.onAccept != nil {
				s.onAccept(conn, s.addr.Port)
			}
		}
	}()
}

// Stop closes the listener.
//
//	server.Stop()
func (s *Server) Stop() {
	if s == nil {
		return
	}
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
}

func (s *Server) listen() Result {
	if s == nil {
		return newErrorResult(NewScopedError("proxy.server", "server is nil", nil))
	}
	if s.listener != nil {
		return newSuccessResult()
	}
	if trimString(s.addr.Host) == "" {
		return newErrorResult(NewScopedError("proxy.server", "listener host is empty", nil))
	}
	if s.addr.TLS && s.tlsConfig == nil {
		return newErrorResult(NewScopedError("proxy.server", "tls listener requires a tls config", nil))
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(s.addr.Host, strconv.Itoa(int(s.addr.Port))))
	if err != nil {
		return newErrorResult(NewScopedError("proxy.server", "listen failed", err))
	}
	s.listener = ln
	return newSuccessResult()
}

// IsActive reports whether the limiter has enabled rate limiting.
//
//	if rl.IsActive() { /* rate limiting is enabled */ }
func (rl *RateLimiter) IsActive() bool {
	return rl != nil && rl.limit.MaxConnectionsPerMinute > 0
}

func nextMinerID() int64 { return atomic.AddInt64(&minerSeq, 1) }

var minerSeq int64

type noopSplitter struct{}

func (n *noopSplitter) Connect()                    {}
func (n *noopSplitter) OnLogin(event *LoginEvent)   {}
func (n *noopSplitter) OnSubmit(event *SubmitEvent) {}
func (n *noopSplitter) OnClose(event *CloseEvent)   {}
func (n *noopSplitter) Tick(ticks uint64)           {}
func (n *noopSplitter) GC()                         {}
func (n *noopSplitter) Upstreams() UpstreamStats    { return UpstreamStats{} }
