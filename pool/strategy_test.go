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
}

func TestPoolStrategy_Tick_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	strategy.Tick(0)
	strategy.Tick(1)
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
}

func TestPoolStrategy_OnJob_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	strategy.OnJob(proxy.Job{})
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
}

func TestPoolStrategy_OnResultAccepted_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{}
	strategy.OnResultAccepted(0, false, "Low difficulty share")
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
}

func TestPoolStrategy_OnDisconnect_Ugly(t *testing.T) {
	strategy := &FailoverStrategy{closing: true}
	strategy.OnDisconnect()
}
