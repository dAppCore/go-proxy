package pool

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	core "dappco.re/go/core"
	"dappco.re/go/proxy"
)

const maxStratumLineLength = 16384
const poolConnectTimeout = 10 * time.Second

// NewStrategyFactory creates a StrategyFactory for the supplied config.
//
//	factory := pool.NewStrategyFactory(&proxy.Config{Pools: []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}}})
//	strategy := factory(listener)
func NewStrategyFactory(config *proxy.Config) StrategyFactory {
	return func(listener StratumListener) Strategy {
		var pools []proxy.PoolConfig
		if config != nil {
			pools = config.Pools
		}
		return NewFailoverStrategy(pools, listener, config)
	}
}

// client := pool.NewStratumClient(proxy.PoolConfig{URL: "pool.example:3333", User: "WALLET", Pass: "x"}, listener)
//
//	if result := client.Connect(); result.OK {
//	    client.Login()
//	}
func NewStratumClient(poolConfig proxy.PoolConfig, listener StratumListener) *StratumClient {
	return &StratumClient{
		config:   poolConfig,
		listener: listener,
		pending:  make(map[int64]struct{}),
	}
}

// IsActive reports whether the client has received at least one job.
func (c *StratumClient) IsActive() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}

// SessionID returns the current pool session identifier.
func (c *StratumClient) SessionID() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// result := client.Connect()
func (c *StratumClient) Connect() proxy.Result {
	if c == nil {
		return proxy.Result{Result: core.Result{OK: false}, Error: proxy.NewScopedError("proxy.pool.client", "client is nil", nil)}
	}
	addr := c.config.URL
	if addr == "" {
		return proxy.Result{Result: core.Result{OK: false}, Error: proxy.NewScopedError("proxy.pool.client", "pool url is empty", nil)}
	}
	dialer := net.Dialer{Timeout: poolConnectTimeout}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return proxy.Result{Result: core.Result{OK: false}, Error: proxy.NewScopedError("proxy.pool.client", "dial pool failed", err)}
	}
	if c.config.TLS {
		host := addr
		if containsString(addr, ":") {
			host, _, _ = net.SplitHostPort(addr)
		}
		tlsCfg := &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		}
		if trimString(c.config.TLSFingerprint) != "" {
			tlsCfg.InsecureSkipVerify = true
		}
		tlsConn := tls.Client(conn, tlsCfg)
		_ = tlsConn.SetDeadline(time.Now().Add(poolConnectTimeout))
		if err := tlsConn.Handshake(); err != nil {
			_ = conn.Close()
			return proxy.Result{Result: core.Result{OK: false}, Error: proxy.NewScopedError("proxy.pool.tls", "handshake failed", err)}
		}
		_ = tlsConn.SetDeadline(time.Time{})
		if fp := lowerString(trimString(c.config.TLSFingerprint)); fp != "" {
			cert := tlsConn.ConnectionState().PeerCertificates
			if len(cert) == 0 {
				_ = tlsConn.Close()
				return proxy.Result{Result: core.Result{OK: false}, Error: proxy.NewScopedError("proxy.pool.tls", "missing certificate", nil)}
			}
			sum := sha256.Sum256(cert[0].Raw)
			if hex.EncodeToString(sum[:]) != fp {
				_ = tlsConn.Close()
				return proxy.Result{Result: core.Result{OK: false}, Error: proxy.NewScopedError("proxy.pool.tls", "tls fingerprint mismatch", nil)}
			}
		}
		c.conn = tlsConn
		c.tlsConn = tlsConn
	} else {
		c.conn = conn
	}
	go c.readLoop()
	return proxy.Result{Result: core.Result{OK: true}}
}

// client.Login()
//
// A login reply with a job triggers `OnJob` immediately.
func (c *StratumClient) Login() {
	if c == nil || c.conn == nil {
		return
	}
	loginID := c.reserveRequestID(1)
	params := map[string]any{
		"login": c.config.User,
		"pass":  c.config.Pass,
	}
	if c.config.RigID != "" {
		params["rigid"] = c.config.RigID
	}
	if c.config.Algo != "" {
		params["algo"] = []string{c.config.Algo}
	}
	req := map[string]any{
		"id":      loginID,
		"jsonrpc": "2.0",
		"method":  "login",
		"params":  params,
	}
	_ = c.writeJSON(req)
}

// seq := client.Submit("job-1", "deadbeef", "HASH64HEX", "cn/r")
func (c *StratumClient) Submit(jobID, nonce, result, algo string) int64 {
	if c == nil {
		return 0
	}
	seq := atomic.AddInt64(&c.seq, 1)
	c.mu.Lock()
	c.pending[seq] = struct{}{}
	c.mu.Unlock()
	req := map[string]any{
		"id":      seq,
		"jsonrpc": "2.0",
		"method":  "submit",
		"params": map[string]any{
			"id":     c.SessionID(),
			"job_id": jobID,
			"nonce":  nonce,
			"result": result,
		},
	}
	if algo != "" {
		req["params"].(map[string]any)["algo"] = algo
	}
	if err := c.writeJSON(req); err != nil {
		c.mu.Lock()
		delete(c.pending, seq)
		c.mu.Unlock()
	}
	return seq
}

// client.Keepalive()
func (c *StratumClient) Keepalive() {
	if c == nil || c.conn == nil || !c.IsActive() {
		return
	}
	req := map[string]any{
		"id":      atomic.AddInt64(&c.seq, 1),
		"jsonrpc": "2.0",
		"method":  "keepalived",
		"params": map[string]any{
			"id": c.SessionID(),
		},
	}
	_ = c.writeJSON(req)
}

func (c *StratumClient) reserveRequestID(minimum int64) int64 {
	if c == nil || minimum <= 0 {
		return minimum
	}
	for {
		current := atomic.LoadInt64(&c.seq)
		if current >= minimum {
			return current
		}
		if atomic.CompareAndSwapInt64(&c.seq, current, minimum) {
			return minimum
		}
	}
}

// client.Disconnect()
func (c *StratumClient) Disconnect() {
	if c == nil {
		return
	}
	c.closedOnce.Do(func() {
		conn := c.resetConnectionState()
		if conn != nil {
			_ = conn.Close()
		}
		if c.listener != nil {
			c.listener.OnDisconnect()
		}
	})
}

func (c *StratumClient) notifyDisconnect() {
	c.closedOnce.Do(func() {
		conn := c.resetConnectionState()
		if conn != nil {
			_ = conn.Close()
		}
		if c.listener != nil {
			c.listener.OnDisconnect()
		}
	})
}

func (c *StratumClient) resetConnectionState() net.Conn {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	conn := c.conn
	c.conn = nil
	c.tlsConn = nil
	c.sessionID = ""
	c.active = false
	c.pending = make(map[int64]struct{})
	return conn
}

func (c *StratumClient) writeJSON(payload any) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.conn == nil {
		return proxy.NewScopedError("proxy.pool.client", "connection is nil", nil)
	}
	data := []byte(jsonMarshalString(payload))
	var err error
	data = append(data, '\n')
	_, err = c.conn.Write(data)
	if err != nil {
		c.notifyDisconnect()
		return proxy.NewScopedError("proxy.pool.client", "write request failed", err)
	}
	return err
}

func (c *StratumClient) readLoop() {
	defer c.notifyDisconnect()
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return
	}
	reader := bufio.NewReaderSize(conn, maxStratumLineLength+1)
	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}
		if isPrefix || len(line) > maxStratumLineLength {
			return
		}
		if len(line) == 0 {
			continue
		}
		c.handleMessage(line)
	}
}

func (c *StratumClient) handleMessage(line []byte) {
	var base struct {
		ID     any            `json:"id"`
		Method string         `json:"method"`
		Result map[string]any `json:"result"`
		Error  map[string]any `json:"error"`
		Params map[string]any `json:"params"`
	}
	if !jsonUnmarshalBytes(line, &base) {
		return
	}

	if len(base.Result) > 0 {
		sessionID := valueString(base.Result["id"])
		if sessionID != "" {
			c.mu.Lock()
			c.sessionID = sessionID
			c.mu.Unlock()
		}
		if jobMap, ok := base.Result["job"].(map[string]any); ok && valueString(jobMap["job_id"]) != "" {
			c.mu.Lock()
			c.active = true
			c.mu.Unlock()
			if c.listener != nil {
				c.listener.OnJob(proxy.Job{
					Blob:     valueString(jobMap["blob"]),
					JobID:    valueString(jobMap["job_id"]),
					Target:   valueString(jobMap["target"]),
					Algo:     valueString(jobMap["algo"]),
					Height:   valueUint64(jobMap["height"]),
					SeedHash: valueString(jobMap["seed_hash"]),
					ClientID: valueString(jobMap["id"]),
				})
			}
			return
		}
	}

	if len(base.Error) > 0 && requestID(base.ID) == 1 {
		c.notifyDisconnect()
		return
	}

	if base.Method == "job" {
		c.mu.Lock()
		c.active = true
		c.mu.Unlock()
		if c.listener != nil {
			c.listener.OnJob(proxy.Job{
				Blob:     valueString(base.Params["blob"]),
				JobID:    valueString(base.Params["job_id"]),
				Target:   valueString(base.Params["target"]),
				Algo:     valueString(base.Params["algo"]),
				Height:   valueUint64(base.Params["height"]),
				SeedHash: valueString(base.Params["seed_hash"]),
				ClientID: valueString(base.Params["id"]),
			})
		}
		return
	}

	seq := requestID(base.ID)
	if seq == 0 {
		return
	}
	c.mu.Lock()
	_, ok := c.pending[seq]
	if ok {
		delete(c.pending, seq)
	}
	c.mu.Unlock()
	if !ok {
		return
	}

	accepted := len(base.Error) == 0
	if status := valueString(base.Result["status"]); status != "" && equalFoldString(status, "OK") {
		accepted = true
	}
	errorMessage := ""
	if !accepted && len(base.Error) > 0 {
		errorMessage = valueString(base.Error["message"])
	}
	if c.listener != nil {
		c.listener.OnResultAccepted(seq, accepted, errorMessage)
	}
}

// NewFailoverStrategy creates the ordered pool failover wrapper.
func NewFailoverStrategy(pools []proxy.PoolConfig, listener StratumListener, config *proxy.Config) *FailoverStrategy {
	return &FailoverStrategy{
		pools:    pools,
		listener: listener,
		config:   config,
	}
}

// strategy.Connect()
func (s *FailoverStrategy) Connect() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closing = false
	s.mu.Unlock()
	s.connectLocked(0)
}

func (s *FailoverStrategy) connectLocked(start int) {
	enabled := enabledPools(s.currentPools())
	if len(enabled) == 0 {
		return
	}
	retries := 1
	retryPause := time.Second
	if s.config != nil {
		if s.config.Retries > 0 {
			retries = s.config.Retries
		}
		if s.config.RetryPause > 0 {
			retryPause = time.Duration(s.config.RetryPause) * time.Second
		}
	}
	for attempt := 0; attempt < retries; attempt++ {
		if s.isClosing() {
			return
		}
		for i := 0; i < len(enabled); i++ {
			if s.isClosing() {
				return
			}
			index := (start + i) % len(enabled)
			poolCfg := enabled[index]
			client := NewStratumClient(poolCfg, s)
			if result := client.Connect(); result.OK {
				s.mu.Lock()
				if s.closing {
					s.mu.Unlock()
					client.Disconnect()
					return
				}
				s.client = client
				s.current = index
				s.mu.Unlock()
				client.Login()
				return
			}
		}
		if s.isClosing() {
			return
		}
		time.Sleep(retryPause)
	}
}

func (s *FailoverStrategy) isClosing() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

func (s *FailoverStrategy) currentPools() []proxy.PoolConfig {
	if s == nil {
		return nil
	}
	if s.config != nil && len(s.config.Pools) > 0 {
		return s.config.Pools
	}
	return s.pools
}

// seq := strategy.Submit(jobID, nonce, result, algo)
func (s *FailoverStrategy) Submit(jobID, nonce, result, algo string) int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()
	if client == nil || !client.IsActive() {
		return 0
	}
	return client.Submit(jobID, nonce, result, algo)
}

// strategy.Disconnect()
func (s *FailoverStrategy) Disconnect() {
	if s == nil {
		return
	}
	s.mu.Lock()
	client := s.client
	s.closing = true
	s.client = nil
	s.mu.Unlock()
	if client != nil {
		client.Disconnect()
	}
}

// strategy.ReloadPools()
func (s *FailoverStrategy) ReloadPools() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.current = 0
	s.mu.Unlock()
	s.Disconnect()
	s.Connect()
}

// active := strategy.IsActive()
func (s *FailoverStrategy) IsActive() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	client := s.client
	closing := s.closing
	s.mu.Unlock()
	if closing || client == nil {
		return false
	}
	return client.IsActive()
}

// Tick keeps an active pool connection alive when configured.
func (s *FailoverStrategy) Tick(ticks uint64) {
	if s == nil || ticks == 0 || ticks%60 != 0 {
		return
	}
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()
	if client != nil && client.config.Keepalive {
		client.Keepalive()
	}
}

// OnJob forwards the pool job to the outer listener.
func (s *FailoverStrategy) OnJob(job proxy.Job) {
	if s != nil && s.listener != nil {
		s.listener.OnJob(job)
	}
}

// OnResultAccepted forwards the result status to the outer listener.
func (s *FailoverStrategy) OnResultAccepted(sequence int64, accepted bool, errorMessage string) {
	if s != nil && s.listener != nil {
		s.listener.OnResultAccepted(sequence, accepted, errorMessage)
	}
}

// strategy.OnDisconnect()
func (s *FailoverStrategy) OnDisconnect() {
	if s == nil {
		return
	}
	s.mu.Lock()
	closing := s.closing
	if closing {
		s.closing = false
	}
	s.client = nil
	s.mu.Unlock()
	if closing {
		return
	}
	if s.listener != nil {
		s.listener.OnDisconnect()
	}
	go s.Connect()
}

func enabledPools(pools []proxy.PoolConfig) []proxy.PoolConfig {
	out := make([]proxy.PoolConfig, 0, len(pools))
	for _, poolCfg := range pools {
		if poolCfg.Enabled {
			out = append(out, poolCfg)
		}
	}
	return out
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
