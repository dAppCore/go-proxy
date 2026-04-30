package pool

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

type strategyListenerSpy struct {
	jobs        atomic.Int64
	results     atomic.Int64
	disconnects atomic.Int64
}

func (s *strategyListenerSpy) OnJob(proxy.Job) { s.jobs.Add(1) }

func (s *strategyListenerSpy) OnResultAccepted(int64, bool, string) { s.results.Add(1) }

func (s *strategyListenerSpy) OnDisconnect() { s.disconnects.Add(1) }

type tickConn struct {
	writes atomic.Int64
}

func (c *tickConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *tickConn) Write(p []byte) (int, error)      { c.writes.Add(1); return len(p), nil }
func (c *tickConn) Close() error                     { return nil }
func (c *tickConn) LocalAddr() net.Addr              { return tickAddr("127.0.0.1:1") }
func (c *tickConn) RemoteAddr() net.Addr             { return tickAddr("127.0.0.1:2") }
func (c *tickConn) SetDeadline(time.Time) error      { return nil }
func (c *tickConn) SetReadDeadline(time.Time) error  { return nil }
func (c *tickConn) SetWriteDeadline(time.Time) error { return nil }

type tickAddr string

func (a tickAddr) Network() string { return "tcp" }
func (a tickAddr) String() string  { return string(a) }

func TestPoolStrategy_CurrentIndex_Good(t *testing.T) {
	strategy := &FailoverStrategy{current: 2}
	if got := strategy.CurrentIndex(); got != 2 {
		t.Fatalf("expected current index 2, got %d", got)
	}
}

func TestPoolStrategy_CurrentIndex_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	if got := strategy.CurrentIndex(); got != -1 {
		t.Fatalf("expected nil strategy to return -1, got %d", got)
	}
}

func TestPoolStrategy_Client_Ugly(t *testing.T) {
	client := &StratumClient{}
	strategy := &FailoverStrategy{client: client}
	if got := strategy.Client(); got != client {
		t.Fatalf("expected current client to be returned, got %+v", got)
	}
}

func TestPoolStrategy_Tick_Good(t *testing.T) {
	conn := &tickConn{}
	client := &StratumClient{config: proxy.PoolConfig{Keepalive: true}, conn: conn}
	client.active = true
	strategy := &FailoverStrategy{client: client}

	strategy.Tick(59)
	strategy.Tick(60)

	if got := conn.writes.Load(); got != 1 {
		t.Fatalf("expected keepalive only on 60th tick, got %d", got)
	}
}

func TestPoolStrategy_Tick_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.Tick(60)
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestPoolStrategy_Tick_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	strategy.Tick(0)
	strategy.Tick(1)
	if strategy.client != nil {
		t.Fatalf("expected ticks without client to leave client nil, got %+v", strategy.client)
	}
}

func TestPoolStrategy_OnJob_Good(t *testing.T) {
	spy := &strategyListenerSpy{}
	strategy := &FailoverStrategy{listener: spy}

	strategy.OnJob(proxy.Job{JobID: "job-1"})

	if got := spy.jobs.Load(); got != 1 {
		t.Fatalf("expected one forwarded job, got %d", got)
	}
}

func TestPoolStrategy_OnJob_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.OnJob(proxy.Job{JobID: "job-1"})
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestPoolStrategy_OnJob_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	strategy.OnJob(proxy.Job{})
	if strategy.listener != nil {
		t.Fatalf("expected missing listener to remain nil, got %+v", strategy.listener)
	}
}

func TestPoolStrategy_OnResultAccepted_Good(t *testing.T) {
	spy := &strategyListenerSpy{}
	strategy := &FailoverStrategy{listener: spy}

	strategy.OnResultAccepted(7, true, "")

	if got := spy.results.Load(); got != 1 {
		t.Fatalf("expected one forwarded result, got %d", got)
	}
}

func TestPoolStrategy_OnResultAccepted_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.OnResultAccepted(7, true, "")
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestPoolStrategy_OnResultAccepted_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	strategy.OnResultAccepted(0, false, "Low difficulty share")
	if strategy.listener != nil {
		t.Fatalf("expected missing listener to remain nil, got %+v", strategy.listener)
	}
}

func TestPoolStrategy_OnDisconnect_Good(t *testing.T) {
	spy := &strategyListenerSpy{}
	strategy := &FailoverStrategy{listener: spy}

	strategy.OnDisconnect()

	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected one disconnect notification, got %d", got)
	}
}

func TestPoolStrategy_OnDisconnect_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.OnDisconnect()
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestPoolStrategy_OnDisconnect_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{closing: true}
	strategy.OnDisconnect()
	if strategy.closing {
		t.Fatal("expected closing disconnect to clear closing flag")
	}
}

func TestStrategy_FailoverStrategy_CurrentIndex_Good(t *testing.T) {
	strategy := &FailoverStrategy{current: 2}
	if got := strategy.CurrentIndex(); got != 2 {
		t.Fatalf("expected current index 2, got %d", got)
	}
}

func TestStrategy_FailoverStrategy_CurrentIndex_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	if got := strategy.CurrentIndex(); got != -1 {
		t.Fatalf("expected nil current index -1, got %d", got)
	}
}

func TestStrategy_FailoverStrategy_CurrentIndex_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{current: -1}
	if got := strategy.CurrentIndex(); got != -1 {
		t.Fatalf("expected negative current index preserved, got %d", got)
	}
}

func TestStrategy_FailoverStrategy_Client_Good(t *testing.T) {
	client := &StratumClient{}
	strategy := &FailoverStrategy{client: client}
	if got := strategy.Client(); got != client {
		t.Fatalf("expected strategy client, got %v", got)
	}
}

func TestStrategy_FailoverStrategy_Client_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	if got := strategy.Client(); got != nil {
		t.Fatalf("expected nil strategy client nil, got %v", got)
	}
}

func TestStrategy_FailoverStrategy_Client_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	if got := strategy.Client(); got != nil {
		t.Fatalf("expected empty strategy client nil, got %v", got)
	}
}

func TestImpl_NewFailoverStrategy_Good(t *testing.T) {
	listener := &strategyListenerSpy{}
	pools := []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}}
	strategy := NewFailoverStrategy(pools, listener, nil)
	if strategy == nil || strategy.listener != listener || len(strategy.pools) != 1 {
		t.Fatalf("expected initialized failover strategy, got %+v", strategy)
	}
}

func TestImpl_NewFailoverStrategy_Bad(t *testing.T) {
	strategy := NewFailoverStrategy(nil, nil, nil)
	if strategy == nil || strategy.listener != nil || strategy.pools != nil {
		t.Fatalf("expected empty failover strategy, got %+v", strategy)
	}
}

func TestImpl_NewFailoverStrategy_Ugly(t *testing.T) {
	cfg := &proxy.Config{Pools: []proxy.PoolConfig{{URL: "config.example:3333", Enabled: true}}}
	strategy := NewFailoverStrategy([]proxy.PoolConfig{{URL: "constructor.example:3333", Enabled: true}}, nil, cfg)
	if got := strategy.currentPools()[0].URL; got != "config.example:3333" {
		t.Fatalf("expected config pools to take precedence, got %q", got)
	}
}

func TestImpl_FailoverStrategy_Submit_Good(t *testing.T) {
	conn := &tickConn{}
	client := &StratumClient{conn: conn, active: true, pending: make(map[int64]struct{}), sessionID: "session-1"}
	strategy := &FailoverStrategy{client: client}
	if seq := strategy.Submit("job-1", "deadbeef", "hash", "rx/0"); seq == 0 || conn.writes.Load() != 1 {
		t.Fatalf("expected submit sequence and write, seq=%d writes=%d", seq, conn.writes.Load())
	}
}

func TestImpl_FailoverStrategy_Submit_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	if seq := strategy.Submit("job-1", "deadbeef", "hash", ""); seq != 0 {
		t.Fatalf("expected nil strategy submit 0, got %d", seq)
	}
}

func TestImpl_FailoverStrategy_Submit_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{client: &StratumClient{}}
	if seq := strategy.Submit("job-1", "deadbeef", "hash", ""); seq != 0 {
		t.Fatalf("expected inactive client submit 0, got %d", seq)
	}
}

func TestImpl_FailoverStrategy_ReloadPools_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.ReloadPools()
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestImpl_FailoverStrategy_IsActive_Good(t *testing.T) {
	strategy := &FailoverStrategy{client: &StratumClient{active: true}}
	if !strategy.IsActive() {
		t.Fatal("expected strategy active with active client")
	}
}

func TestImpl_FailoverStrategy_IsActive_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	if strategy.IsActive() {
		t.Fatal("expected nil strategy inactive")
	}
}

func TestImpl_FailoverStrategy_IsActive_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{client: &StratumClient{active: true}, closing: true}
	if strategy.IsActive() {
		t.Fatal("expected closing strategy inactive")
	}
}

func TestImpl_FailoverStrategy_Tick_Good(t *testing.T) {
	conn := &tickConn{}
	client := &StratumClient{config: proxy.PoolConfig{Keepalive: true}, conn: conn, active: true}
	strategy := &FailoverStrategy{client: client}
	strategy.Tick(60)
	if got := conn.writes.Load(); got != 1 {
		t.Fatalf("expected keepalive tick write, got %d", got)
	}
}

func TestImpl_FailoverStrategy_Tick_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.Tick(60)
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestImpl_FailoverStrategy_Tick_Ugly(t *testing.T) {
	conn := &tickConn{}
	client := &StratumClient{config: proxy.PoolConfig{Keepalive: true}, conn: conn, active: true}
	strategy := &FailoverStrategy{client: client}
	strategy.Tick(59)
	if got := conn.writes.Load(); got != 0 {
		t.Fatalf("expected non-60 tick ignored, got %d", got)
	}
}

func TestImpl_FailoverStrategy_OnJob_Good(t *testing.T) {
	spy := &strategyListenerSpy{}
	strategy := &FailoverStrategy{listener: spy}
	strategy.OnJob(proxy.Job{JobID: "job-1"})
	if got := spy.jobs.Load(); got != 1 {
		t.Fatalf("expected job forwarded once, got %d", got)
	}
}

func TestImpl_FailoverStrategy_OnJob_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.OnJob(proxy.Job{JobID: "job-1"})
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestImpl_FailoverStrategy_OnJob_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	strategy.OnJob(proxy.Job{})
	if strategy.listener != nil {
		t.Fatalf("expected missing listener unchanged, got %+v", strategy.listener)
	}
}

func TestImpl_FailoverStrategy_OnResultAccepted_Good(t *testing.T) {
	spy := &strategyListenerSpy{}
	strategy := &FailoverStrategy{listener: spy}
	strategy.OnResultAccepted(7, true, "")
	if got := spy.results.Load(); got != 1 {
		t.Fatalf("expected result forwarded once, got %d", got)
	}
}

func TestImpl_FailoverStrategy_OnResultAccepted_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.OnResultAccepted(7, true, "")
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestImpl_FailoverStrategy_OnResultAccepted_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	strategy.OnResultAccepted(0, false, "Low difficulty share")
	if strategy.listener != nil {
		t.Fatalf("expected missing listener unchanged, got %+v", strategy.listener)
	}
}

func TestImpl_FailoverStrategy_OnDisconnect_Good(t *testing.T) {
	spy := &strategyListenerSpy{}
	strategy := &FailoverStrategy{listener: spy}
	strategy.OnDisconnect()
	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected disconnect forwarded once, got %d", got)
	}
}

func TestImpl_FailoverStrategy_OnDisconnect_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.OnDisconnect()
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestImpl_FailoverStrategy_OnDisconnect_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{closing: true}
	strategy.OnDisconnect()
	if strategy.closing {
		t.Fatal("expected closing disconnect to clear closing flag")
	}
}
