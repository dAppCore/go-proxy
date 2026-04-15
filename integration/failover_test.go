package integration_test

import (
	"bufio"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"dappco.re/go/proxy"
	_ "dappco.re/go/proxy/splitter/simple"
)

func TestFailover_PrimaryDown_Good(t *testing.T) {
	primary := newFailoverPool(t, reserveAddress(t).address, "job-primary")
	defer primary.Stop()
	fallback := newFailoverPool(t, reserveAddress(t).address, "job-fallback")
	defer fallback.Stop()

	minerAddress := reserveAddress(t)
	proxyInstance := startProxyInstance(t, &proxy.Config{
		Mode:    "simple",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: minerAddress.port}},
		Pools: []proxy.PoolConfig{
			{URL: primary.Address(), User: "WALLET", Pass: "x", Enabled: true},
			{URL: fallback.Address(), User: "WALLET", Pass: "x", Enabled: true},
		},
		Retries:    3,
		RetryPause: 1,
	})
	defer proxyInstance.Stop()

	conn, reader, response := loginMiner(t, minerAddress.address, "wallet-failover")
	defer conn.Close()
	if response.Error != nil {
		t.Fatalf("expected login success, got %+v", response.Error)
	}
	if response.Result == nil || response.Result.Job.JobID != "job-primary" {
		waitForJobID(t, reader, 3*time.Second, "job-primary")
	}

	primary.CloseAndStop()
	waitForJobID(t, reader, 3*time.Second, "job-fallback")
}

func TestFailover_AllDown_Bad(t *testing.T) {
	primary := reserveAddress(t)
	fallback := reserveAddress(t)
	minerAddress := reserveAddress(t)

	proxyInstance := startProxyInstance(t, &proxy.Config{
		Mode:    "simple",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: minerAddress.port}},
		Pools: []proxy.PoolConfig{
			{URL: primary.address, User: "WALLET", Pass: "x", Enabled: true},
			{URL: fallback.address, User: "WALLET", Pass: "x", Enabled: true},
		},
		Retries:    1,
		RetryPause: 1,
	})
	defer proxyInstance.Stop()

	conn, err := net.DialTimeout("tcp", minerAddress.address, time.Second)
	if err != nil {
		t.Fatalf("dial miner endpoint: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write(encodeLine(t, map[string]any{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "login",
		"params": map[string]any{
			"login": "wallet-all-down",
			"pass":  "x",
			"agent": "integration-test",
		},
	})); err != nil {
		t.Fatalf("write login: %v", err)
	}

	reader := bufio.NewReader(conn)
	line := readLineWithTimeout(t, reader, 2*time.Second)
	var loginResponse minerLoginResponse
	if err := json.Unmarshal(line, &loginResponse); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(1200 * time.Millisecond))
	_, err = reader.ReadBytes('\n')
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("expected open miner socket with read timeout, got %v", err)
	}
}

func TestFailover_Recovery_Ugly(t *testing.T) {
	primaryAddress := reserveAddress(t)
	primaryPhaseOne := newFailoverPool(t, primaryAddress.address, "job-primary-1")
	fallback := newFailoverPool(t, reserveAddress(t).address, "job-fallback")
	defer fallback.Stop()

	minerAddress := reserveAddress(t)
	proxyInstance := startProxyInstance(t, &proxy.Config{
		Mode:    "simple",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: minerAddress.port}},
		Pools: []proxy.PoolConfig{
			{URL: primaryAddress.address, User: "WALLET", Pass: "x", Enabled: true},
			{URL: fallback.Address(), User: "WALLET", Pass: "x", Enabled: true},
		},
		Retries:    3,
		RetryPause: 1,
	})
	defer proxyInstance.Stop()

	conn, reader, response := loginMiner(t, minerAddress.address, "wallet-recovery")
	defer conn.Close()
	if response.Error != nil {
		t.Fatalf("expected login success, got %+v", response.Error)
	}

	primaryPhaseOne.CloseAndStop()
	waitForJobID(t, reader, 3*time.Second, "job-fallback")

	primaryPhaseTwo := newFailoverPool(t, primaryAddress.address, "job-primary-2")
	defer primaryPhaseTwo.Stop()
	fallback.CloseActive()
	waitForJobID(t, reader, 3*time.Second, "job-primary-2")
}

type failoverPool struct {
	listener   net.Listener
	jobID      string
	activeMu   sync.Mutex
	activeConn net.Conn
	done       chan struct{}
}

func newFailoverPool(t *testing.T, address, jobID string) *failoverPool {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen failover pool: %v", err)
	}
	pool := &failoverPool{
		listener: listener,
		jobID:    jobID,
		done:     make(chan struct{}),
	}
	go pool.serve()
	return pool
}

func (pool *failoverPool) Address() string {
	return pool.listener.Addr().String()
}

func (pool *failoverPool) serve() {
	defer close(pool.done)
	for {
		conn, err := pool.listener.Accept()
		if err != nil {
			return
		}
		pool.activeMu.Lock()
		pool.activeConn = conn
		pool.activeMu.Unlock()
		go pool.handle(conn)
	}
}

func (pool *failoverPool) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var request poolRequest
		if err := json.Unmarshal(line, &request); err != nil {
			return
		}
		switch request.Method {
		case "login":
			_, _ = conn.Write(encodeLineBytes(map[string]any{
				"id": "session-1",
				"result": map[string]any{
					"id": "session-1",
					"job": map[string]any{
						"blob":   zeroBlob(),
						"job_id": pool.jobID,
						"target": "b88d0600",
						"id":     "session-1",
					},
				},
			}))
		case "keepalived":
			_, _ = conn.Write(encodeLineBytes(map[string]any{
				"id":     request.ID,
				"result": map[string]any{"status": "KEEPALIVED"},
			}))
		case "submit":
			_, _ = conn.Write(encodeLineBytes(map[string]any{
				"id":     request.ID,
				"result": map[string]any{"status": "OK"},
			}))
		}
	}
}

func (pool *failoverPool) CloseActive() {
	pool.activeMu.Lock()
	defer pool.activeMu.Unlock()
	if pool.activeConn != nil {
		_ = pool.activeConn.Close()
		pool.activeConn = nil
	}
}

func (pool *failoverPool) Stop() {
	if pool == nil {
		return
	}
	_ = pool.listener.Close()
	pool.CloseActive()
	<-pool.done
}

func (pool *failoverPool) CloseAndStop() {
	pool.CloseActive()
	pool.Stop()
}

func readLineWithTimeout(t *testing.T, reader *bufio.Reader, timeout time.Duration) []byte {
	t.Helper()
	type result struct {
		line []byte
		err  error
	}
	resultChannel := make(chan result, 1)
	go func() {
		line, err := reader.ReadBytes('\n')
		resultChannel <- result{line: line, err: err}
	}()
	select {
	case outcome := <-resultChannel:
		if outcome.err != nil {
			t.Fatalf("read line: %v", outcome.err)
		}
		return outcome.line
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for line after %s", timeout)
		return nil
	}
}

func decodeJobID(t *testing.T, line []byte) string {
	t.Helper()
	var payload struct {
		Method string `json:"method"`
		Params struct {
			JobID string `json:"job_id"`
		} `json:"params"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("decode job line: %v", err)
	}
	return payload.Params.JobID
}

func waitForJobID(t *testing.T, reader *bufio.Reader, timeout time.Duration, expected string) {
	t.Helper()
	if timeout <= 0 {
		t.Fatalf("timed out waiting for job %q", expected)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		line := readLineWithTimeout(t, reader, time.Until(deadline))
		if decodeJobID(t, line) == expected {
			return
		}
	}
	t.Fatalf("timed out waiting for job %q", expected)
}
