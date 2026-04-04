package proxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// MinerSnapshot is a serialisable view of one miner connection.
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

// New creates the proxy and wires the default event handlers.
func New(cfg *Config) (*Proxy, Result) {
	if cfg == nil {
		return nil, errorResult(errors.New("config is nil"))
	}
	if result := cfg.Validate(); !result.OK {
		return nil, result
	}

	p := &Proxy{
		config:     cfg,
		events:     NewEventBus(),
		stats:      NewStats(),
		workers:    NewWorkers(cfg.Workers, nil),
		miners:     make(map[int64]*Miner),
		customDiff: NewCustomDiff(cfg.CustomDiff),
		rateLimit:  NewRateLimiter(cfg.RateLimit),
		done:       make(chan struct{}),
	}
	p.workers.bindEvents(p.events)

	p.events.Subscribe(EventLogin, p.customDiff.OnLogin)
	p.events.Subscribe(EventLogin, p.stats.OnLogin)
	p.events.Subscribe(EventLogin, p.workers.OnLogin)
	p.events.Subscribe(EventClose, p.stats.OnClose)
	p.events.Subscribe(EventClose, p.workers.OnClose)
	p.events.Subscribe(EventAccept, p.stats.OnAccept)
	p.events.Subscribe(EventAccept, p.workers.OnAccept)
	p.events.Subscribe(EventReject, p.stats.OnReject)
	p.events.Subscribe(EventReject, p.workers.OnReject)
	if cfg.Watch && cfg.sourcePath != "" {
		p.watcher = NewConfigWatcher(cfg.sourcePath, p.Reload)
	}

	if factory, ok := getSplitterFactory(cfg.Mode); ok {
		p.splitter = factory(cfg, p.events)
	} else {
		p.splitter = &noopSplitter{}
	}

	return p, successResult()
}

// Mode returns the active proxy mode.
func (p *Proxy) Mode() string {
	if p == nil || p.config == nil {
		return ""
	}
	return p.config.Mode
}

// WorkersMode returns the worker naming strategy.
func (p *Proxy) WorkersMode() WorkersMode {
	if p == nil || p.config == nil {
		return WorkersDisabled
	}
	return p.config.Workers
}

// Summary returns the current global stats snapshot.
func (p *Proxy) Summary() StatsSummary {
	if p == nil || p.stats == nil {
		return StatsSummary{}
	}
	return p.stats.Summary()
}

// WorkerRecords returns a stable snapshot of worker rows.
func (p *Proxy) WorkerRecords() []WorkerRecord {
	if p == nil || p.workers == nil {
		return nil
	}
	return p.workers.List()
}

// MinerSnapshots returns a stable snapshot of connected miners.
func (p *Proxy) MinerSnapshots() []MinerSnapshot {
	if p == nil {
		return nil
	}
	p.minersMu.RLock()
	defer p.minersMu.RUnlock()
	rows := make([]MinerSnapshot, 0, len(p.miners))
	for _, miner := range p.miners {
		rows = append(rows, MinerSnapshot{
			ID:       miner.id,
			IP:       miner.ip,
			TX:       miner.tx,
			RX:       miner.rx,
			State:    miner.state,
			Diff:     miner.diff,
			User:     miner.user,
			Password: "********",
			RigID:    miner.rigID,
			Agent:    miner.agent,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

// MinerCount returns current and peak connected miner counts.
func (p *Proxy) MinerCount() (now, max uint64) {
	if p == nil || p.stats == nil {
		return 0, 0
	}
	return p.stats.miners.Load(), p.stats.maxMiners.Load()
}

// Upstreams returns splitter upstream counts.
func (p *Proxy) Upstreams() UpstreamStats {
	if p == nil || p.splitter == nil {
		return UpstreamStats{}
	}
	return p.splitter.Upstreams()
}

// Events returns the proxy event bus for external composition.
func (p *Proxy) Events() *EventBus {
	if p == nil {
		return nil
	}
	return p.events
}

// Start starts the TCP listeners, ticker loop, and optional HTTP API.
func (p *Proxy) Start() {
	if p == nil {
		return
	}
	for _, bind := range p.config.Bind {
		var tlsCfg *tls.Config
		if bind.TLS && p.config.TLS.Enabled {
			tlsCfg = buildTLSConfig(p.config.TLS)
		}
		server, _ := NewServer(bind, tlsCfg, p.rateLimit, p.acceptMiner)
		p.servers = append(p.servers, server)
		server.Start()
	}
	if p.splitter != nil {
		p.splitter.Connect()
	}
	if p.watcher != nil {
		p.watcher.Start()
	}
	if p.config.HTTP.Enabled {
		p.startHTTP()
	}
	p.ticker = time.NewTicker(time.Second)
	go func() {
		var ticks uint64
		for {
			select {
			case <-p.ticker.C:
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

// Stop shuts down listeners, background tasks, and HTTP.
func (p *Proxy) Stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		close(p.done)
		if p.ticker != nil {
			p.ticker.Stop()
		}
		for _, server := range p.servers {
			server.Stop()
		}
		if p.watcher != nil {
			p.watcher.Stop()
		}
		if p.httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = p.httpServer.Shutdown(ctx)
		}
	})
}

// Reload swaps the live configuration and updates dependent state.
func (p *Proxy) Reload(cfg *Config) {
	if p == nil || cfg == nil {
		return
	}
	if p.config == nil {
		p.config = cfg
	} else {
		preservedBind := append([]BindAddr(nil), p.config.Bind...)
		preservedMode := p.config.Mode
		preservedWorkers := p.config.Workers
		preservedSourcePath := p.config.sourcePath
		*p.config = *cfg
		p.config.Bind = preservedBind
		p.config.Mode = preservedMode
		p.config.Workers = preservedWorkers
		p.config.sourcePath = preservedSourcePath
	}
	if p.customDiff != nil {
		p.customDiff.globalDiff = cfg.CustomDiff
	}
	p.rateLimit = NewRateLimiter(cfg.RateLimit)
}

func (p *Proxy) acceptMiner(conn net.Conn, localPort uint16) {
	if p == nil {
		_ = conn.Close()
		return
	}
	if p.stats != nil {
		p.stats.connections.Add(1)
	}
	miner := NewMiner(conn, localPort, nil)
	miner.accessPassword = p.config.AccessPassword
	miner.algoEnabled = p.config.AlgoExtension
	miner.globalDiff = p.config.CustomDiff
	miner.extNH = strings.EqualFold(p.config.Mode, "nicehash")
	miner.onLogin = func(m *Miner) {
		if p.events != nil {
			p.events.Dispatch(Event{Type: EventLogin, Miner: m})
		}
		if p.splitter != nil {
			p.splitter.OnLogin(&LoginEvent{Miner: m})
		}
	}
	miner.onSubmit = func(m *Miner, event *SubmitEvent) {
		if p.splitter != nil {
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

func buildTLSConfig(cfg TLSConfig) *tls.Config {
	if !cfg.Enabled || cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil
	}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
	applyTLSProtocols(tlsConfig, cfg.Protocols)
	applyTLSCiphers(tlsConfig, cfg.Ciphers)
	return tlsConfig
}

func applyTLSProtocols(tlsConfig *tls.Config, protocols string) {
	if tlsConfig == nil || strings.TrimSpace(protocols) == "" {
		return
	}
	parts := splitTLSConfigList(protocols)
	minVersion := uint16(0)
	maxVersion := uint16(0)
	for _, part := range parts {
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			bounds := strings.SplitN(part, "-", 2)
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
	if tlsConfig == nil || strings.TrimSpace(ciphers) == "" {
		return
	}
	allowed := map[string]uint16{}
	for _, suite := range tls.CipherSuites() {
		allowed[strings.ToLower(suite.Name)] = suite.ID
	}
	for _, suite := range tls.InsecureCipherSuites() {
		allowed[strings.ToLower(suite.Name)] = suite.ID
	}
	parts := splitTLSConfigList(ciphers)
	for _, part := range parts {
		if id, ok := allowed[strings.ToLower(part)]; ok {
			tlsConfig.CipherSuites = append(tlsConfig.CipherSuites, id)
		}
	}
}

func splitTLSConfigList(value string) []string {
	return strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case ',', ';', ':', '|', ' ':
			return true
		default:
			return false
		}
	})
}

func parseTLSVersion(value string) uint16 {
	switch strings.ToLower(strings.TrimSpace(value)) {
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

func (p *Proxy) startHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("/1/summary", func(w http.ResponseWriter, r *http.Request) {
		if !p.allowHTTP(r) {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		p.writeJSON(w, p.summaryDocument())
	})
	mux.HandleFunc("/1/workers", func(w http.ResponseWriter, r *http.Request) {
		if !p.allowHTTP(r) {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		p.writeJSON(w, p.workersDocument())
	})
	mux.HandleFunc("/1/miners", func(w http.ResponseWriter, r *http.Request) {
		if !p.allowHTTP(r) {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		p.writeJSON(w, p.minersDocument())
	})
	addr := net.JoinHostPort(p.config.HTTP.Host, strconv.Itoa(int(p.config.HTTP.Port)))
	p.httpServer = &http.Server{Addr: addr, Handler: mux}
	go func() {
		_ = p.httpServer.ListenAndServe()
	}()
}

func (p *Proxy) allowHTTP(r *http.Request) bool {
	if p == nil {
		return false
	}
	if p.config.HTTP.Restricted && r.Method != http.MethodGet {
		return false
	}
	if token := p.config.HTTP.AccessToken; token != "" {
		parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") || parts[1] != token {
			return false
		}
	}
	return true
}

func (p *Proxy) writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func (p *Proxy) summaryDocument() any {
	summary := p.Summary()
	now, max := p.MinerCount()
	upstreams := p.Upstreams()
	workerCount := uint64(len(p.WorkerRecords()))
	return map[string]any{
		"version": "1.0.0",
		"mode":    p.Mode(),
		"hashrate": map[string]any{
			"total": summary.Hashrate,
		},
		"miners": map[string]any{
			"now": now,
			"max": max,
		},
		"workers":   workerCount,
		"upstreams": map[string]any{"active": upstreams.Active, "sleep": upstreams.Sleep, "error": upstreams.Error, "total": upstreams.Total, "ratio": upstreamRatio(now, upstreams)},
		"results": map[string]any{
			"accepted":     summary.Accepted,
			"rejected":     summary.Rejected,
			"invalid":      summary.Invalid,
			"expired":      summary.Expired,
			"avg_time":     summary.AvgTime,
			"latency":      summary.AvgLatency,
			"hashes_total": summary.Hashes,
			"best":         summary.TopDiff,
		},
	}
}

func (p *Proxy) workersDocument() any {
	records := p.WorkerRecords()
	rows := make([]any, 0, len(records))
	for _, record := range records {
		rows = append(rows, []any{
			record.Name,
			record.LastIP,
			record.Connections,
			record.Accepted,
			record.Rejected,
			record.Invalid,
			record.Hashes,
			record.LastHashAt.Unix(),
			record.Hashrate(60),
			record.Hashrate(600),
			record.Hashrate(3600),
			record.Hashrate(43200),
			record.Hashrate(86400),
		})
	}
	return map[string]any{
		"mode":    string(p.WorkersMode()),
		"workers": rows,
	}
}

func (p *Proxy) minersDocument() any {
	records := p.MinerSnapshots()
	rows := make([]any, 0, len(records))
	for _, miner := range records {
		rows = append(rows, []any{
			miner.ID,
			miner.IP,
			miner.TX,
			miner.RX,
			miner.State,
			miner.Diff,
			miner.User,
			miner.Password,
			miner.RigID,
			miner.Agent,
		})
	}
	return map[string]any{
		"format": []string{"id", "ip", "tx", "rx", "state", "diff", "user", "password", "rig_id", "agent"},
		"miners": rows,
	}
}

func upstreamRatio(now uint64, upstreams UpstreamStats) float64 {
	if upstreams.Total == 0 {
		return 0
	}
	return float64(now) / float64(upstreams.Total)
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
		miner.ip = hostOnly(remote.String())
	}
	return miner
}

func (m *Miner) SetID(id int64) { m.id = id }
func (m *Miner) ID() int64      { return m.id }
func (m *Miner) SetMapperID(id int64) {
	m.mapperID = id
}
func (m *Miner) MapperID() int64 {
	return m.mapperID
}
func (m *Miner) SetRouteID(id int64) {
	m.routeID = id
}
func (m *Miner) RouteID() int64 {
	return m.routeID
}
func (m *Miner) SetExtendedNiceHash(enabled bool) {
	m.extNH = enabled
}
func (m *Miner) ExtendedNiceHash() bool {
	return m.extNH
}
func (m *Miner) SetCurrentJob(job Job) {
	m.currentJob = job
}
func (m *Miner) CurrentJob() Job {
	return m.currentJob
}
func (m *Miner) FixedByte() uint8 {
	return m.fixedByte
}
func (m *Miner) SetFixedByte(value uint8) {
	m.fixedByte = value
}
func (m *Miner) IP() string {
	return m.ip
}
func (m *Miner) User() string {
	return m.user
}
func (m *Miner) Password() string {
	return m.password
}
func (m *Miner) Agent() string {
	return m.agent
}
func (m *Miner) RigID() string {
	return m.rigID
}
func (m *Miner) RX() uint64 {
	return m.rx
}
func (m *Miner) TX() uint64 {
	return m.tx
}
func (m *Miner) State() MinerState {
	return m.state
}

func (m *Miner) supportsAlgoExtension() bool {
	return m != nil && m.algoEnabled && m.extAlgo
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

	reader := bufio.NewReader(m.conn)
	for {
		if m.state == MinerStateClosing {
			return
		}
		if timeout := m.readTimeout(); timeout > 0 {
			_ = m.conn.SetReadDeadline(time.Now().Add(timeout))
		}
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			m.Close()
			return
		}
		if isPrefix {
			m.Close()
			return
		}
		if len(line) == 0 {
			continue
		}
		m.rx += uint64(len(line) + 1)
		m.lastActivityAt = time.Now().UTC()
		if !m.handleLine(line) {
			m.Close()
			return
		}
	}
}

func (m *Miner) readTimeout() time.Duration {
	switch m.state {
	case MinerStateWaitLogin:
		return 10 * time.Second
	case MinerStateWaitReady, MinerStateReady:
		return 600 * time.Second
	default:
		return 0
	}
}

type stratumRequest struct {
	ID      any             `json:"id"`
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func (m *Miner) handleLine(line []byte) bool {
	var req stratumRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return false
	}
	switch req.Method {
	case "login":
		m.handleLogin(req)
	case "submit":
		m.handleSubmit(req)
	case "keepalived":
		m.handleKeepalived(req)
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

func (m *Miner) handleLogin(req stratumRequest) {
	if m.state != MinerStateWaitLogin {
		return
	}
	var params loginParams
	if err := json.Unmarshal(req.Params, &params); err != nil || strings.TrimSpace(params.Login) == "" {
		m.ReplyWithError(requestID(req.ID), "Invalid payment address provided")
		return
	}
	if m.accessPassword != "" && params.Pass != m.accessPassword {
		m.ReplyWithError(requestID(req.ID), "Invalid password")
		return
	}
	m.user = params.Login
	m.customDiff = 0
	m.password = params.Pass
	m.agent = params.Agent
	m.rigID = params.RigID
	m.extAlgo = len(params.Algo) > 0
	m.rpcID = generateUUID()
	m.state = MinerStateWaitReady
	if m.onLogin != nil {
		m.onLogin(m)
	}
	m.replyLoginSuccess(requestID(req.ID))
}

func parseLoginUser(login string, globalDiff uint64) (string, uint64) {
	plus := strings.LastIndex(login, "+")
	if plus >= 0 && plus < len(login)-1 {
		if parsed, err := strconv.ParseUint(login[plus+1:], 10, 64); err == nil {
			return login[:plus], parsed
		}
		return login, 0
	}
	if globalDiff > 0 {
		return login, globalDiff
	}
	return login, 0
}

func (m *Miner) handleSubmit(req stratumRequest) {
	if m.state != MinerStateReady {
		m.ReplyWithError(requestID(req.ID), "Unauthenticated")
		return
	}
	var params struct {
		ID     string `json:"id"`
		JobID  string `json:"job_id"`
		Nonce  string `json:"nonce"`
		Result string `json:"result"`
		Algo   string `json:"algo"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		m.ReplyWithError(requestID(req.ID), "Invalid nonce")
		return
	}
	if params.ID != m.rpcID {
		m.ReplyWithError(requestID(req.ID), "Unauthenticated")
		return
	}
	if params.JobID == "" {
		m.ReplyWithError(requestID(req.ID), "Missing job id")
		return
	}
	if !isLowerHex8(params.Nonce) {
		m.ReplyWithError(requestID(req.ID), "Invalid nonce")
		return
	}
	if m.onSubmit != nil {
		m.onSubmit(m, &SubmitEvent{
			Miner:     m,
			JobID:     params.JobID,
			Nonce:     params.Nonce,
			Result:    params.Result,
			Algo:      params.Algo,
			RequestID: requestID(req.ID),
		})
	}
	m.touchActivity()
}

func (m *Miner) handleKeepalived(req stratumRequest) {
	m.touchActivity()
	m.Success(requestID(req.ID), "KEEPALIVED")
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
	m.currentJob = job
	m.diff = job.DifficultyFromTarget()
	if algo == "" {
		algo = job.Algo
	}
	blob := job.Blob
	if m.extNH {
		blob = job.BlobWithFixedByte(m.fixedByte)
	}
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "job",
		"params": map[string]any{
			"blob":      blob,
			"job_id":    job.JobID,
			"target":    job.Target,
			"id":        m.rpcID,
			"height":    job.Height,
			"seed_hash": job.SeedHash,
		},
	}
	if m.supportsAlgoExtension() && algo != "" {
		payload["params"].(map[string]any)["algo"] = algo
	}
	_ = m.writeJSON(payload)
	m.touchActivity()
	if m.state == MinerStateWaitReady {
		m.state = MinerStateReady
	}
}

func (m *Miner) replyLoginSuccess(id int64) {
	if m == nil {
		return
	}
	result := map[string]any{
		"id":     m.rpcID,
		"status": "OK",
	}
	if m.supportsAlgoExtension() {
		result["extensions"] = []string{"algo"}
	}
	if job := m.CurrentJob(); job.IsValid() {
		blob := job.Blob
		if m.extNH {
			blob = job.BlobWithFixedByte(m.fixedByte)
		}
		m.diff = job.DifficultyFromTarget()
		jobPayload := map[string]any{
			"blob":      blob,
			"job_id":    job.JobID,
			"target":    job.Target,
			"id":        m.rpcID,
			"height":    job.Height,
			"seed_hash": job.SeedHash,
		}
		if m.supportsAlgoExtension() && job.Algo != "" {
			jobPayload["algo"] = job.Algo
		}
		result["job"] = jobPayload
		m.touchActivity()
		m.state = MinerStateReady
	}
	payload := map[string]any{
		"id":      id,
		"jsonrpc": "2.0",
		"error":   nil,
		"result":  result,
	}
	_ = m.writeJSON(payload)
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
	m.lastActivityAt = time.Now().UTC()
	if m.conn != nil {
		_ = m.conn.SetReadDeadline(time.Now().Add(600 * time.Second))
	}
}

func (m *Miner) writeJSON(payload any) error {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	if m.conn == nil {
		return nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	n, err := m.conn.Write(data)
	m.tx += uint64(n)
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
		m.state = MinerStateClosing
		if m.conn != nil {
			_ = m.conn.Close()
		}
	})
}

// NewStats creates zeroed global metrics.
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
func (s *Stats) OnReject(e Event) {
	if s == nil {
		return
	}
	s.rejected.Add(1)
	if strings.Contains(strings.ToLower(e.Error), "difficulty") || strings.Contains(strings.ToLower(e.Error), "invalid") || strings.Contains(strings.ToLower(e.Error), "nonce") {
		s.invalid.Add(1)
	}
}

// Tick advances the rolling windows.
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
		summary.AvgLatency = uint32(samples[len(samples)/2])
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
func NewWorkers(mode WorkersMode, _ *EventBus) *Workers {
	return &Workers{
		mode:      mode,
		nameIndex: make(map[string]int),
		idIndex:   make(map[int64]int),
	}
}

func (w *Workers) bindEvents(bus *EventBus) {
	if w == nil || bus == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
}

func workerNameFor(mode WorkersMode, miner *Miner) string {
	if miner == nil {
		return ""
	}
	switch mode {
	case WorkersByRigID:
		if miner.rigID != "" {
			return miner.rigID
		}
		return miner.user
	case WorkersByUser:
		return miner.user
	case WorkersByPass:
		return miner.password
	case WorkersByAgent:
		return miner.agent
	case WorkersByIP:
		return miner.ip
	case WorkersDisabled:
		return ""
	default:
		return miner.user
	}
}

// OnLogin creates or updates a worker record.
func (w *Workers) OnLogin(e Event) {
	if w == nil || e.Miner == nil {
		return
	}
	if w.mode == WorkersDisabled {
		return
	}
	name := workerNameFor(w.mode, e.Miner)
	if name == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
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
	record.LastIP = e.Miner.ip
	record.Connections++
	w.idIndex[e.Miner.id] = index
}

func newTickWindow(size int) tickWindow {
	return tickWindow{
		buckets: make([]uint64, size),
		size:    size,
	}
}

// OnAccept updates the owning worker with the accepted share.
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
	if strings.Contains(strings.ToLower(e.Error), "difficulty") || strings.Contains(strings.ToLower(e.Error), "invalid") || strings.Contains(strings.ToLower(e.Error), "nonce") {
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
	delete(w.idIndex, e.Miner.id)
}

// List returns a snapshot of all workers.
func (w *Workers) List() []WorkerRecord {
	if w == nil {
		return nil
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]WorkerRecord, len(w.entries))
	copy(out, w.entries)
	return out
}

// Tick advances worker hash windows.
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

// Apply parses login suffixes and applies the configured global difficulty.
func (cd *CustomDiff) Apply(miner *Miner) {
	if cd == nil || miner == nil {
		return
	}
	miner.user, miner.customDiff = parseLoginUser(miner.user, cd.globalDiff)
}

// NewServer constructs a server instance.
func NewServer(bind BindAddr, tlsCfg *tls.Config, limiter *RateLimiter, onAccept func(net.Conn, uint16)) (*Server, Result) {
	if onAccept == nil {
		onAccept = func(net.Conn, uint16) {}
	}
	return &Server{
		addr:     bind,
		tlsCfg:   tlsCfg,
		limiter:  limiter,
		onAccept: onAccept,
		done:     make(chan struct{}),
	}, successResult()
}

// Start begins accepting connections in a goroutine.
func (s *Server) Start() {
	if s == nil {
		return
	}
	go func() {
		ln, err := net.Listen("tcp", net.JoinHostPort(s.addr.Host, strconv.Itoa(int(s.addr.Port))))
		if err != nil {
			return
		}
		if s.tlsCfg != nil || s.addr.TLS {
			if s.tlsCfg != nil {
				ln = tls.NewListener(ln, s.tlsCfg)
			}
		}
		s.listener = ln
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-s.done:
					return
				default:
					continue
				}
			}
			if s.limiter != nil && !s.limiter.Allow(conn.RemoteAddr().String()) {
				_ = conn.Close()
				continue
			}
			if s.onAccept != nil {
				s.onAccept(conn, s.addr.Port)
			}
		}
	}()
}

// Stop closes the listener.
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

// NewConfig returns a minimal config? not used.

// NewRateLimiter, Allow, Tick are defined in core_impl.go.

// IsActive reports whether the limiter has enabled rate limiting.
func (rl *RateLimiter) IsActive() bool {
	return rl != nil && rl.cfg.MaxConnectionsPerMinute > 0
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

// Difficulty helper for the HTTP summary.
func workerSummaryNow(workers []WorkerRecord) uint64 {
	return uint64(len(workers))
}
