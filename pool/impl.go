package pool

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"dappco.re/go/core/proxy"
)

// NewStrategyFactory creates a StrategyFactory for the supplied config.
func NewStrategyFactory(cfg *proxy.Config) StrategyFactory {
	return func(listener StratumListener) Strategy {
		return NewFailoverStrategy(cfg.Pools, listener, cfg)
	}
}

// NewStratumClient constructs a pool client.
func NewStratumClient(cfg proxy.PoolConfig, listener StratumListener) *StratumClient {
	return &StratumClient{
		cfg:      cfg,
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

// Connect dials the pool.
func (c *StratumClient) Connect() proxy.Result {
	if c == nil {
		return proxy.Result{OK: false, Error: errors.New("client is nil")}
	}
	addr := c.cfg.URL
	if addr == "" {
		return proxy.Result{OK: false, Error: errors.New("pool url is empty")}
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return proxy.Result{OK: false, Error: err}
	}
	if c.cfg.TLS {
		host := addr
		if strings.Contains(addr, ":") {
			host, _, _ = net.SplitHostPort(addr)
		}
		tlsCfg := &tls.Config{InsecureSkipVerify: true, ServerName: host}
		tlsConn := tls.Client(conn, tlsCfg)
		if err := tlsConn.Handshake(); err != nil {
			_ = conn.Close()
			return proxy.Result{OK: false, Error: err}
		}
		if fp := strings.TrimSpace(strings.ToLower(c.cfg.TLSFingerprint)); fp != "" {
			cert := tlsConn.ConnectionState().PeerCertificates
			if len(cert) == 0 {
				_ = tlsConn.Close()
				return proxy.Result{OK: false, Error: errors.New("missing certificate")}
			}
			sum := sha256.Sum256(cert[0].Raw)
			if hex.EncodeToString(sum[:]) != fp {
				_ = tlsConn.Close()
				return proxy.Result{OK: false, Error: errors.New("tls fingerprint mismatch")}
			}
		}
		c.conn = tlsConn
		c.tlsConn = tlsConn
	} else {
		c.conn = conn
	}
	go c.readLoop()
	return proxy.Result{OK: true}
}

// Login sends the miner-style login request to the pool.
func (c *StratumClient) Login() {
	if c == nil || c.conn == nil {
		return
	}
	params := map[string]any{
		"login": c.cfg.User,
		"pass":  c.cfg.Pass,
	}
	if c.cfg.RigID != "" {
		params["rigid"] = c.cfg.RigID
	}
	if c.cfg.Algo != "" {
		params["algo"] = []string{c.cfg.Algo}
	}
	req := map[string]any{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "login",
		"params":  params,
	}
	_ = c.writeJSON(req)
}

// Submit forwards a share to the pool.
func (c *StratumClient) Submit(jobID, nonce, result, algo string) int64 {
	if c == nil {
		return 0
	}
	seq := atomic.AddInt64(&c.seq, 1)
	c.mu.Lock()
	c.pending[seq] = struct{}{}
	sessionID := c.sessionID
	c.mu.Unlock()
	req := map[string]any{
		"id":      seq,
		"jsonrpc": "2.0",
		"method":  "submit",
		"params": map[string]any{
			"id":     sessionID,
			"job_id": jobID,
			"nonce":  nonce,
			"result": result,
			"algo":   algo,
		},
	}
	_ = c.writeJSON(req)
	return seq
}

// Disconnect closes the connection and notifies the listener.
func (c *StratumClient) Disconnect() {
	if c == nil {
		return
	}
	c.closedOnce.Do(func() {
		if c.conn != nil {
			_ = c.conn.Close()
		}
		if c.listener != nil {
			c.listener.OnDisconnect()
		}
	})
}

func (c *StratumClient) notifyDisconnect() {
	c.closedOnce.Do(func() {
		if c.listener != nil {
			c.listener.OnDisconnect()
		}
	})
}

func (c *StratumClient) writeJSON(payload any) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.conn == nil {
		return errors.New("connection is nil")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = c.conn.Write(data)
	if err != nil {
		c.notifyDisconnect()
	}
	return err
}

func (c *StratumClient) readLoop() {
	defer c.notifyDisconnect()
	reader := bufio.NewReader(c.conn)
	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}
		if isPrefix {
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
		ID     any             `json:"id"`
		Method string          `json:"method"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(line, &base); err != nil {
		return
	}

	if len(base.Result) > 0 {
		var loginReply struct {
			ID  string `json:"id"`
			Job *struct {
				Blob     string `json:"blob"`
				JobID    string `json:"job_id"`
				Target   string `json:"target"`
				Algo     string `json:"algo"`
				Height   uint64 `json:"height"`
				SeedHash string `json:"seed_hash"`
				ID       string `json:"id"`
			} `json:"job"`
		}
		if err := json.Unmarshal(base.Result, &loginReply); err == nil {
			if loginReply.ID != "" {
				c.mu.Lock()
				c.sessionID = loginReply.ID
				c.mu.Unlock()
			}
			if loginReply.Job != nil && loginReply.Job.JobID != "" {
				c.mu.Lock()
				c.active = true
				c.mu.Unlock()
				if c.listener != nil {
					c.listener.OnJob(proxy.Job{
						Blob:     loginReply.Job.Blob,
						JobID:    loginReply.Job.JobID,
						Target:   loginReply.Job.Target,
						Algo:     loginReply.Job.Algo,
						Height:   loginReply.Job.Height,
						SeedHash: loginReply.Job.SeedHash,
						ClientID: loginReply.Job.ID,
					})
				}
				return
			}
		}
	}

	if base.Method == "job" {
		var params struct {
			Blob     string `json:"blob"`
			JobID    string `json:"job_id"`
			Target   string `json:"target"`
			Algo     string `json:"algo"`
			Height   uint64 `json:"height"`
			SeedHash string `json:"seed_hash"`
			ID       string `json:"id"`
		}
		if err := json.Unmarshal(base.Params, &params); err != nil {
			return
		}
		c.mu.Lock()
		c.active = true
		c.mu.Unlock()
		if c.listener != nil {
			c.listener.OnJob(proxy.Job{
				Blob:     params.Blob,
				JobID:    params.JobID,
				Target:   params.Target,
				Algo:     params.Algo,
				Height:   params.Height,
				SeedHash: params.SeedHash,
				ClientID: params.ID,
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

	var payload struct {
		Status string `json:"status"`
	}
	if len(base.Result) > 0 {
		_ = json.Unmarshal(base.Result, &payload)
	}
	accepted := len(base.Error) == 0
	if payload.Status != "" && strings.EqualFold(payload.Status, "OK") {
		accepted = true
	}
	errorMessage := ""
	if !accepted && len(base.Error) > 0 {
		var errPayload struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(base.Error, &errPayload)
		errorMessage = errPayload.Message
	}
	if c.listener != nil {
		c.listener.OnResultAccepted(seq, accepted, errorMessage)
	}
}

// NewFailoverStrategy creates the ordered pool failover wrapper.
func NewFailoverStrategy(pools []proxy.PoolConfig, listener StratumListener, cfg *proxy.Config) *FailoverStrategy {
	return &FailoverStrategy{
		pools:    pools,
		listener: listener,
		cfg:      cfg,
	}
}

// Connect establishes the first reachable pool connection.
func (s *FailoverStrategy) Connect() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connectLocked(0)
}

func (s *FailoverStrategy) connectLocked(start int) {
	enabled := enabledPools(s.pools)
	if len(enabled) == 0 {
		return
	}
	retries := 1
	retryPause := time.Second
	if s.cfg != nil {
		if s.cfg.Retries > 0 {
			retries = s.cfg.Retries
		}
		if s.cfg.RetryPause > 0 {
			retryPause = time.Duration(s.cfg.RetryPause) * time.Second
		}
	}
	for attempt := 0; attempt < retries; attempt++ {
		for i := 0; i < len(enabled); i++ {
			index := (start + i) % len(enabled)
			poolCfg := enabled[index]
			client := NewStratumClient(poolCfg, s)
			if result := client.Connect(); result.OK {
				s.client = client
				s.current = index
				client.Login()
				return
			}
		}
		time.Sleep(retryPause)
	}
}

// Submit sends the share through the active client.
func (s *FailoverStrategy) Submit(jobID, nonce, result, algo string) int64 {
	if s == nil || s.client == nil {
		return 0
	}
	return s.client.Submit(jobID, nonce, result, algo)
}

// Disconnect closes the active client.
func (s *FailoverStrategy) Disconnect() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		s.client.Disconnect()
		s.client = nil
	}
}

// IsActive reports whether the current client has received a job.
func (s *FailoverStrategy) IsActive() bool {
	return s != nil && s.client != nil && s.client.IsActive()
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

// OnDisconnect retries from the primary pool and forwards the disconnect.
func (s *FailoverStrategy) OnDisconnect() {
	if s == nil {
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
