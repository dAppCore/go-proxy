package integration_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"dappco.re/go/proxy"
	_ "dappco.re/go/proxy/splitter/nicehash"
)

type poolRequest struct {
	ID     any            `json:"id"`
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

type minerLoginResponse struct {
	ID    any `json:"id"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
	Result *struct {
		ID  string `json:"id"`
		Job struct {
			Blob   string `json:"blob"`
			JobID  string `json:"job_id"`
			Target string `json:"target"`
		} `json:"job"`
	} `json:"result"`
}

type minerResultResponse struct {
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
	Result map[string]any `json:"result"`
}

func TestNicehash_FullFlow_Good(t *testing.T) {
	poolAddress, stopPool := startNicehashPool(t)
	defer stopPool()

	minerAddress := reserveAddress(t)
	proxyInstance := startProxyInstance(t, &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: minerAddress.port}},
		Pools:   []proxy.PoolConfig{{URL: poolAddress, User: "WALLET", Pass: "x", Enabled: true}},
	})
	defer proxyInstance.Stop()

	var miners []net.Conn
	firstSessionID := ""
	for index := 0; index < 3; index++ {
		conn, _, response := loginMiner(t, minerAddress.address, fmt.Sprintf("wallet-%d", index))
		miners = append(miners, conn)
		if response.Result == nil || response.Result.Job.JobID != "job-1" {
			t.Fatalf("expected login job for miner %d, got %+v", index, response)
		}
		if index == 0 {
			firstSessionID = response.Result.ID
		}
	}
	defer closeAll(miners)

	submitLine := encodeLine(t, map[string]any{
		"id":      2,
		"jsonrpc": "2.0",
		"method":  "submit",
		"params": map[string]any{
			"id":     firstSessionID,
			"job_id": "job-1",
			"nonce":  "deadbeef",
			"result": "HASH64HEX",
		},
	})
	if _, err := miners[0].Write(submitLine); err != nil {
		t.Fatalf("write submit: %v", err)
	}

	response := readMinerResult(t, miners[0])
	if response.Error != nil {
		t.Fatalf("expected accepted share, got error %+v", response.Error)
	}
	waitFor(t, 2*time.Second, func() bool {
		return proxyInstance.Summary().Accepted == 1
	})
}

func TestNicehash_FullFlow_Bad(t *testing.T) {
	poolAddress, stopPool := startNicehashPool(t)
	defer stopPool()

	minerAddress := reserveAddress(t)
	proxyInstance := startProxyInstance(t, &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: minerAddress.port}},
		Pools:   []proxy.PoolConfig{{URL: poolAddress, User: "WALLET", Pass: "x", Enabled: true}},
	})
	defer proxyInstance.Stop()

	conn, _, loginResponse := loginMiner(t, minerAddress.address, "wallet-bad")
	defer conn.Close()

	submitLine := encodeLine(t, map[string]any{
		"id":      2,
		"jsonrpc": "2.0",
		"method":  "submit",
		"params": map[string]any{
			"id":     "wrong-session",
			"job_id": loginResponse.Result.Job.JobID,
			"nonce":  "deadbeef",
			"result": "HASH64HEX",
		},
	})
	if _, err := conn.Write(submitLine); err != nil {
		t.Fatalf("write submit: %v", err)
	}

	response := readMinerResult(t, conn)
	if response.Error == nil || response.Error.Message != "Unauthenticated" {
		t.Fatalf("expected unauthenticated error, got %+v", response)
	}

	keepalive := encodeLine(t, map[string]any{
		"id":      3,
		"jsonrpc": "2.0",
		"method":  "keepalived",
		"params":  map[string]any{"id": loginResponse.Result.ID},
	})
	if _, err := conn.Write(keepalive); err != nil {
		t.Fatalf("write keepalived: %v", err)
	}
	response = readMinerResult(t, conn)
	if response.Error != nil || response.Result["status"] != "KEEPALIVED" {
		t.Fatalf("expected keepalived success, got %+v", response)
	}
}

func TestNicehash_FullFlow_Ugly(t *testing.T) {
	poolAddress, connections, stopPool := startCountingNicehashPool(t)
	defer stopPool()

	minerAddress := reserveAddress(t)
	proxyInstance := startProxyInstance(t, &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: minerAddress.port}},
		Pools:   []proxy.PoolConfig{{URL: poolAddress, User: "WALLET", Pass: "x", Enabled: true}},
	})
	defer proxyInstance.Stop()

	miners := make([]net.Conn, 0, 257)
	for index := 0; index < 257; index++ {
		conn, _, response := loginMiner(t, minerAddress.address, fmt.Sprintf("wallet-%03d", index))
		if response.Error != nil {
			t.Fatalf("expected miner %d to log in successfully, got %+v", index, response.Error)
		}
		miners = append(miners, conn)
	}
	defer closeAll(miners)

	waitFor(t, 3*time.Second, func() bool {
		return connections() >= 2
	})
}

func startProxyInstance(t *testing.T, config *proxy.Config) *proxy.Proxy {
	t.Helper()
	instance, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}
	go instance.Start()
	waitFor(t, 2*time.Second, func() bool {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", config.Bind[0].Port)), 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	})
	return instance
}

func startNicehashPool(t *testing.T) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen pool: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleNicehashPoolConnection(conn)
		}
	}()
	return listener.Addr().String(), func() {
		_ = listener.Close()
		<-done
	}
}

func startCountingNicehashPool(t *testing.T) (string, func() int, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen pool: %v", err)
	}
	countCh := make(chan int, 300)
	done := make(chan struct{})
	go func() {
		defer close(done)
		count := 0
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			count++
			countCh <- count
			go handleNicehashPoolConnection(conn)
		}
	}()
	lastCount := 0
	return listener.Addr().String(), func() int {
			for {
				select {
				case lastCount = <-countCh:
				default:
					return lastCount
				}
			}
		}, func() {
			_ = listener.Close()
			<-done
		}
}

func handleNicehashPoolConnection(conn net.Conn) {
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
						"job_id": "job-1",
						"target": "b88d0600",
						"id":     "session-1",
					},
				},
			}))
		case "submit":
			_, _ = conn.Write(encodeLineBytes(map[string]any{
				"id":     request.ID,
				"result": map[string]any{"status": "OK"},
			}))
		}
	}
}

func loginMiner(t *testing.T, address, login string) (net.Conn, *bufio.Reader, minerLoginResponse) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatalf("dial miner endpoint: %v", err)
	}
	reader := bufio.NewReader(conn)
	loginLine := encodeLine(t, map[string]any{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "login",
		"params": map[string]any{
			"login": login,
			"pass":  "x",
			"agent": "integration-test",
		},
	})
	if _, err := conn.Write(loginLine); err != nil {
		t.Fatalf("write login: %v", err)
	}
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read login response: %v", err)
	}
	var response minerLoginResponse
	if err := json.Unmarshal(line, &response); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	if response.Error == nil && (response.Result == nil || response.Result.Job.JobID == "") {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			_ = conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				t.Fatalf("read login job notification: %v", err)
			}
			var notification struct {
				Method string `json:"method"`
				Params struct {
					Blob   string `json:"blob"`
					JobID  string `json:"job_id"`
					Target string `json:"target"`
				} `json:"params"`
			}
			if err := json.Unmarshal(line, &notification); err != nil {
				continue
			}
			if notification.Method == "job" && notification.Params.JobID != "" {
				if response.Result == nil {
					response.Result = &struct {
						ID  string `json:"id"`
						Job struct {
							Blob   string `json:"blob"`
							JobID  string `json:"job_id"`
							Target string `json:"target"`
						} `json:"job"`
					}{}
				}
				response.Result.Job = struct {
					Blob   string `json:"blob"`
					JobID  string `json:"job_id"`
					Target string `json:"target"`
				}{
					Blob:   notification.Params.Blob,
					JobID:  notification.Params.JobID,
					Target: notification.Params.Target,
				}
				break
			}
		}
		_ = conn.SetReadDeadline(time.Time{})
	}
	return conn, reader, response
}

func readMinerResult(t *testing.T, conn net.Conn) minerResultResponse {
	t.Helper()
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read miner result: %v", err)
	}
	var response minerResultResponse
	if err := json.Unmarshal(line, &response); err != nil {
		t.Fatalf("decode miner result: %v", err)
	}
	return response
}

type reservedAddress struct {
	address string
	port    uint16
}

func reserveAddress(t *testing.T) reservedAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	_, portString, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split reserved address: %v", err)
	}
	var port uint16
	if _, err := fmt.Sscanf(portString, "%d", &port); err != nil {
		t.Fatalf("parse reserved port: %v", err)
	}
	return reservedAddress{address: address, port: port}
}

func encodeLine(t *testing.T, payload any) []byte {
	t.Helper()
	return encodeLineBytes(payload)
}

func encodeLineBytes(payload any) []byte {
	data, _ := json.Marshal(payload)
	return append(data, '\n')
}

func zeroBlob() string {
	return strings.Repeat("0", 160)
}

func closeAll(connections []net.Conn) {
	for _, conn := range connections {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
