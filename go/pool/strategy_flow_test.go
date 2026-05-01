package pool

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

func TestStrategyFlow_FailoverStrategy_Connect_Good(t *testing.T) {
	addr, shutdown := startStratumServer(t, func(conn net.Conn, reader *bufio.Reader) {
		loginLine := readLine(t, reader)
		var loginReq struct {
			Method string `json:"method"`
		}
		if err := testJSONUnmarshal(loginLine, &loginReq); err != nil {
			t.Fatalf("decode login request: %v", err)
		}
		if loginReq.Method != "login" {
			t.Fatalf("expected login method, got %q", loginReq.Method)
		}
		_, _ = io.WriteString(conn, `{"id":"session-1","result":{"id":"session-1","job":{"blob":"`+repeatString("0", 160)+`","job_id":"job-1","target":"b88d0600","id":"session-1"}}}`+"\n")

		keepaliveLine := readLine(t, reader)
		var keepaliveReq struct {
			Method string `json:"method"`
		}
		if err := testJSONUnmarshal(keepaliveLine, &keepaliveReq); err != nil {
			t.Fatalf("decode keepalive request: %v", err)
		}
		if keepaliveReq.Method != "keepalived" {
			t.Fatalf("expected keepalived method, got %q", keepaliveReq.Method)
		}

		submitLine := readLine(t, reader)
		var submitReq struct {
			Method string `json:"method"`
		}
		if err := testJSONUnmarshal(submitLine, &submitReq); err != nil {
			t.Fatalf("decode submit request: %v", err)
		}
		if submitReq.Method != "submit" {
			t.Fatalf("expected submit method, got %q", submitReq.Method)
		}

		_, _ = io.WriteString(conn, `{"id":3,"result":{"status":"OK"}}`+"\n")
		time.Sleep(25 * time.Millisecond)
	})
	defer shutdown()

	spy := &clientListenerSpy{}
	cfg := &proxy.Config{
		Retries:    1,
		RetryPause: 0,
		Pools: []proxy.PoolConfig{{
			URL:       addr,
			User:      "WALLET",
			Pass:      "x",
			Enabled:   true,
			Keepalive: true,
		}},
	}
	strategy := NewFailoverStrategy(cfg.Pools, spy, cfg)

	strategy.Connect()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotJobs := len(spy.jobs)
		spy.mu.Unlock()
		if gotJobs > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	spy.mu.Lock()
	if len(spy.jobs) != 1 {
		spy.mu.Unlock()
		t.Fatalf("expected login job notification, got %+v", spy.jobs)
	}
	spy.mu.Unlock()

	if !strategy.IsActive() {
		t.Fatal("expected strategy to become active after login job")
	}

	strategy.Tick(60)
	if seq := strategy.Submit("job-1", "deadbeef", "HASH64HEX", "cn/r"); seq != 3 {
		t.Fatalf("expected submit sequence 3 after keepalive tick, got %d", seq)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotResults := len(spy.results)
		spy.mu.Unlock()
		if gotResults > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	spy.mu.Lock()
	if len(spy.results) != 1 || !spy.results[0].accepted {
		spy.mu.Unlock()
		t.Fatalf("expected accepted result, got %+v", spy.results)
	}
	spy.mu.Unlock()

	strategy.OnJob(proxy.Job{JobID: "job-direct", Blob: repeatString("0", 160)})
	strategy.OnResultAccepted(99, true, "")
	spy.mu.Lock()
	if len(spy.jobs) < 2 || len(spy.results) < 2 {
		spy.mu.Unlock()
		t.Fatalf("expected forwarded job/result callbacks, got jobs=%d results=%d", len(spy.jobs), len(spy.results))
	}
	spy.mu.Unlock()
}

func TestStrategyFlow_FailoverStrategy_Connect_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	strategy.Connect()
	if strategy != nil {
		t.Fatal("expected nil strategy to remain nil")
	}
}

func TestStrategyFlow_FailoverStrategy_Connect_Ugly(t *testing.T) {
	badListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for bad upstream: %v", err)
	}
	badAddr := badListener.Addr().String()
	_ = badListener.Close()

	addr, shutdown := startStratumServer(t, func(conn net.Conn, reader *bufio.Reader) {
		loginLine := readLine(t, reader)
		var loginReq struct {
			Method string `json:"method"`
		}
		if err := testJSONUnmarshal(loginLine, &loginReq); err != nil {
			t.Fatalf("decode login request: %v", err)
		}
		if loginReq.Method != "login" {
			t.Fatalf("expected login method, got %q", loginReq.Method)
		}
		_, _ = io.WriteString(conn, `{"id":"session-1","result":{"id":"session-1","job":{"blob":"`+repeatString("0", 160)+`","job_id":"job-1","target":"b88d0600","id":"session-1"}}}`+"\n")
		time.Sleep(25 * time.Millisecond)
	})
	defer shutdown()

	spy := &clientListenerSpy{}
	cfg := &proxy.Config{
		Retries:    1,
		RetryPause: 0,
		Pools: []proxy.PoolConfig{
			{URL: badAddr, User: "WALLET", Pass: "x", Enabled: true},
			{URL: addr, User: "WALLET", Pass: "x", Enabled: true},
		},
	}
	strategy := NewFailoverStrategy(cfg.Pools, spy, cfg)

	strategy.Connect()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotJobs := len(spy.jobs)
		spy.mu.Unlock()
		if gotJobs > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	spy.mu.Lock()
	if len(spy.jobs) != 1 {
		spy.mu.Unlock()
		t.Fatalf("expected failover to connect to the second pool, got jobs=%+v", spy.jobs)
	}
	spy.mu.Unlock()

	if got := strategy.CurrentIndex(); got != 1 {
		t.Fatalf("expected failover to advance to the second enabled pool, got index %d", got)
	}
	if strategy.Client() == nil {
		t.Fatal("expected strategy to retain the connected client")
	}
	if got := strategy.Client().config.URL; got != addr {
		t.Fatalf("expected strategy to connect to %q, got %q", addr, got)
	}
}

func TestStrategyFlow_FailoverStrategy_ReloadPools_Good(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	accepts := make(chan struct{}, 2)
	done := make(chan struct{})
	handlerErrs := make(chan string, 2)
	go func() {
		defer close(done)
		for i := 0; i < 2; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts <- struct{}{}
			go func(c net.Conn) {
				defer c.Close()
				reader := bufio.NewReader(c)
				loginLine, err := reader.ReadBytes('\n')
				if err != nil {
					handlerErrs <- err.Error()
					return
				}
				var loginReq struct {
					Method string `json:"method"`
				}
				if err := testJSONUnmarshal(loginLine, &loginReq); err != nil {
					handlerErrs <- err.Error()
					return
				}
				if loginReq.Method != "login" {
					handlerErrs <- "expected login method"
					return
				}
				_, _ = io.WriteString(c, `{"id":"session-1","result":{"id":"session-1"}}`+"\n")
				time.Sleep(10 * time.Millisecond)
				handlerErrs <- ""
			}(conn)
		}
	}()
	addr := ln.Addr().String()

	cfg := &proxy.Config{
		Retries:    1,
		RetryPause: 0,
		Pools: []proxy.PoolConfig{{
			URL:     addr,
			User:    "WALLET",
			Pass:    "x",
			Enabled: true,
		}},
	}
	strategy := NewFailoverStrategy(cfg.Pools, nil, cfg)
	strategy.Connect()
	<-accepts
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if client := strategy.Client(); client != nil && client.SessionID() != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	strategy.ReloadPools()
	<-accepts
	_ = ln.Close()
	<-done
	for i := 0; i < 2; i++ {
		if msg := <-handlerErrs; msg != "" {
			t.Fatal(msg)
		}
	}
}

func TestStrategyFlow_FailoverStrategy_ReloadPools_Ugly(t *testing.T) {
	strategy := NewFailoverStrategy(nil, nil, &proxy.Config{})
	strategy.ReloadPools()
	if strategy.Client() != nil {
		t.Fatalf("expected reload with no pools to leave client nil, got %+v", strategy.Client())
	}
}
