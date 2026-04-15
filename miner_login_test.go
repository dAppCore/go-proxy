package proxy

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

func TestMiner_HandleLogin_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := NewMiner(minerConn, 3333, nil)
	miner.algoEnabled = true
	miner.extNH = true
	miner.fixedByte = 0x2a
	miner.onLogin = func(m *Miner) {
		m.SetMapperID(1)
	}
	miner.currentJob = Job{
		Blob:     strings.Repeat("0", 160),
		JobID:    "job-1",
		Target:   "b88d0600",
		Algo:     "cn/r",
		Height:   7,
		SeedHash: "seed",
	}

	params, err := json.Marshal(loginParams{
		Login: "wallet",
		Pass:  "x",
		Agent: "xmrig",
		Algo:  []string{"cn/r"},
		RigID: "rig-1",
	})
	if err != nil {
		t.Fatalf("marshal login params: %v", err)
	}

	done := make(chan struct{})
	go func() {
		miner.handleLogin(stratumRequest{ID: 1, Method: "login", Params: params})
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read login response: %v", err)
	}
	<-done

	var payload struct {
		Error  json.RawMessage `json:"error"`
		Result struct {
			ID         string         `json:"id"`
			Status     string         `json:"status"`
			Extensions []string       `json:"extensions"`
			Job        map[string]any `json:"job"`
		} `json:"result"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal login response: %v", err)
	}

	if string(payload.Error) != "null" {
		t.Fatalf("expected login response error to be null, got %s", string(payload.Error))
	}
	if payload.Result.Status != "OK" {
		t.Fatalf("expected login success, got %q", payload.Result.Status)
	}
	if payload.Result.ID == "" {
		t.Fatalf("expected rpc id in login response")
	}
	if len(payload.Result.Extensions) != 1 || payload.Result.Extensions[0] != "algo" {
		t.Fatalf("expected algo extension, got %#v", payload.Result.Extensions)
	}
	if got := miner.LoginAlgos(); len(got) != 1 || got[0] != "cn/r" {
		t.Fatalf("expected login algo list to be stored, got %#v", got)
	}
	if got := payload.Result.Job["job_id"]; got != "job-1" {
		t.Fatalf("expected embedded job, got %#v", got)
	}
	if got := payload.Result.Job["algo"]; got != "cn/r" {
		t.Fatalf("expected embedded algo, got %#v", got)
	}
	blob, _ := payload.Result.Job["blob"].(string)
	if blob[78:80] != "2a" {
		t.Fatalf("expected fixed-byte patched blob, got %q", blob[78:80])
	}
	if miner.State() != MinerStateReady {
		t.Fatalf("expected miner ready after login reply with job, got %d", miner.State())
	}
}

func TestMiner_HandleLogin_Bad(t *testing.T) {
	t.Run("empty_login", func(t *testing.T) {
		minerConn, clientConn := net.Pipe()
		defer minerConn.Close()
		defer clientConn.Close()

		miner := NewMiner(minerConn, 3333, nil)
		miner.onLogin = func(*Miner) {}

		params, err := json.Marshal(loginParams{
			Login: "   ",
			Pass:  "x",
		})
		if err != nil {
			t.Fatalf("marshal login params: %v", err)
		}

		done := make(chan struct{})
		go func() {
			miner.handleLogin(stratumRequest{ID: 10, Method: "login", Params: params})
			close(done)
		}()

		line, err := bufio.NewReader(clientConn).ReadBytes('\n')
		if err != nil {
			t.Fatalf("read login rejection: %v", err)
		}
		<-done

		var payload struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(line, &payload); err != nil {
			t.Fatalf("unmarshal login rejection: %v", err)
		}
		if payload.Error.Message != "Invalid payment address provided" {
			t.Fatalf("expected empty login to be rejected, got %q", payload.Error.Message)
		}
		if miner.State() != MinerStateWaitLogin {
			t.Fatalf("expected rejected login to keep miner in wait-login state, got %d", miner.State())
		}
		if got := miner.sessionID(); got != "" {
			t.Fatalf("expected rejected login not to assign a session id, got %q", got)
		}
		if _, err := clientConn.Read(make([]byte, 1)); err == nil {
			t.Fatal("expected rejected login to close the connection")
		}
	})

	t.Run("bad_password", func(t *testing.T) {
		minerConn, clientConn := net.Pipe()
		defer minerConn.Close()
		defer clientConn.Close()

		miner := NewMiner(minerConn, 3333, nil)
		miner.accessPassword = "secret"
		miner.onLogin = func(*Miner) {}

		params, err := json.Marshal(loginParams{
			Login: "wallet",
			Pass:  "wrong",
		})
		if err != nil {
			t.Fatalf("marshal login params: %v", err)
		}

		done := make(chan struct{})
		go func() {
			miner.handleLogin(stratumRequest{ID: 11, Method: "login", Params: params})
			close(done)
		}()

		line, err := bufio.NewReader(clientConn).ReadBytes('\n')
		if err != nil {
			t.Fatalf("read login rejection: %v", err)
		}
		<-done

		var payload struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(line, &payload); err != nil {
			t.Fatalf("unmarshal login rejection: %v", err)
		}
		if payload.Error.Message != "Invalid password" {
			t.Fatalf("expected bad password to be rejected, got %q", payload.Error.Message)
		}
		if miner.State() != MinerStateWaitLogin {
			t.Fatalf("expected rejected login to keep miner in wait-login state, got %d", miner.State())
		}
		if got := miner.sessionID(); got != "" {
			t.Fatalf("expected rejected login not to assign a session id, got %q", got)
		}
		if _, err := clientConn.Read(make([]byte, 1)); err == nil {
			t.Fatal("expected rejected login to close the connection")
		}
	})
}

func TestProxy_New_Watch_Good(t *testing.T) {
	cfg := &Config{
		Mode:       "nicehash",
		Workers:    WorkersByRigID,
		Bind:       []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:      []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		Watch:      true,
		configPath: "/tmp/proxy.json",
	}

	proxyInstance, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}
	if proxyInstance.watcher == nil {
		t.Fatalf("expected config watcher when watch is enabled and source path is known")
	}
}

func TestMiner_HandleLogin_Ugly(t *testing.T) {
	for i := 0; i < 256; i++ {
		miner := &Miner{}
		miner.SetID(int64(i + 1))
		miner.SetMapperID(int64(i + 1))
	}

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.extNH = true
	miner.onLogin = func(*Miner) {}

	params, err := json.Marshal(loginParams{
		Login: "wallet",
		Pass:  "x",
	})
	if err != nil {
		t.Fatalf("marshal login params: %v", err)
	}

	done := make(chan []byte, 1)
	go func() {
		line, readErr := bufio.NewReader(clientConn).ReadBytes('\n')
		if readErr != nil {
			done <- nil
			return
		}
		done <- line
	}()

	miner.handleLogin(stratumRequest{ID: 2, Method: "login", Params: params})

	line := <-done
	if line == nil {
		t.Fatal("expected login rejection response")
	}

	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal login response: %v", err)
	}
	if payload.Error.Message != "Proxy is full, try again later" {
		t.Fatalf("expected full-table error, got %q", payload.Error.Message)
	}
	if payload.Result != nil {
		t.Fatalf("expected no login success payload, got %#v", payload.Result)
	}
	if miner.MapperID() != -1 {
		t.Fatalf("expected rejected miner to remain unassigned, got mapper %d", miner.MapperID())
	}
}

func TestMiner_HandleLogin_FailedAssignmentDoesNotDispatchLoginEvent(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	proxyInstance := &Proxy{
		config: &Config{
			Mode:    "nicehash",
			Workers: WorkersByUser,
			Bind:    []BindAddr{{Host: "127.0.0.1", Port: 3333}},
			Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		},
		events:  NewEventBus(),
		stats:   NewStats(),
		workers: NewWorkers(WorkersByUser, nil),
		miners:  make(map[int64]*Miner),
	}
	proxyInstance.events.Subscribe(EventLogin, proxyInstance.stats.OnLogin)
	proxyInstance.workers.bindEvents(proxyInstance.events)

	miner := NewMiner(minerConn, 3333, nil)
	miner.extNH = true
	miner.onLogin = func(*Miner) {}
	miner.onLoginReady = func(m *Miner) {
		proxyInstance.events.Dispatch(Event{Type: EventLogin, Miner: m})
	}
	proxyInstance.miners[miner.ID()] = miner

	params, err := json.Marshal(loginParams{
		Login: "wallet",
		Pass:  "x",
	})
	if err != nil {
		t.Fatalf("marshal login params: %v", err)
	}

	go miner.handleLogin(stratumRequest{ID: 12, Method: "login", Params: params})

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read login rejection: %v", err)
	}

	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal login rejection: %v", err)
	}
	if payload.Error.Message != "Proxy is full, try again later" {
		t.Fatalf("expected full-table rejection, got %q", payload.Error.Message)
	}
	if now, max := proxyInstance.MinerCount(); now != 0 || max != 0 {
		t.Fatalf("expected failed login not to affect miner counts, got now=%d max=%d", now, max)
	}
	if records := proxyInstance.WorkerRecords(); len(records) != 0 {
		t.Fatalf("expected failed login not to create worker records, got %d", len(records))
	}
}

func TestMiner_HandleLogin_CustomDiffCap_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := NewMiner(minerConn, 3333, nil)
	miner.onLogin = func(m *Miner) {
		m.SetRouteID(1)
		m.customDiff = 5000
	}
	miner.currentJob = Job{
		Blob:   strings.Repeat("0", 160),
		JobID:  "job-1",
		Target: "01000000",
	}

	params, err := json.Marshal(loginParams{
		Login: "wallet",
		Pass:  "x",
	})
	if err != nil {
		t.Fatalf("marshal login params: %v", err)
	}

	go miner.handleLogin(stratumRequest{ID: 3, Method: "login", Params: params})

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read login response: %v", err)
	}

	var payload struct {
		Result struct {
			Job struct {
				Target string `json:"target"`
			} `json:"job"`
		} `json:"result"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal login response: %v", err)
	}

	originalDiff := miner.currentJob.DifficultyFromTarget()
	cappedDiff := Job{Target: payload.Result.Job.Target}.DifficultyFromTarget()
	if cappedDiff == 0 || cappedDiff > 5000 {
		t.Fatalf("expected capped difficulty at or below 5000, got %d", cappedDiff)
	}
	if cappedDiff >= originalDiff {
		t.Fatalf("expected lowered target difficulty below %d, got %d", originalDiff, cappedDiff)
	}
	if miner.diff != cappedDiff {
		t.Fatalf("expected miner diff %d, got %d", cappedDiff, miner.diff)
	}
}

func TestMiner_HandleLogin_CustomDiffSuffix_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := NewMiner(minerConn, 3333, nil)
	miner.onLogin = func(m *Miner) {
		m.SetRouteID(1)
	}

	params, err := json.Marshal(loginParams{
		Login: "wallet+50000",
		Pass:  "x",
	})
	if err != nil {
		t.Fatalf("marshal login params: %v", err)
	}

	go miner.handleLogin(stratumRequest{ID: 4, Method: "login", Params: params})

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read login response: %v", err)
	}

	var payload struct {
		Result struct {
			Status string `json:"status"`
		} `json:"result"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal login response: %v", err)
	}
	if payload.Result.Status != "OK" {
		t.Fatalf("expected login success, got %q", payload.Result.Status)
	}
	if got := miner.User(); got != "wallet" {
		t.Fatalf("expected stripped wallet name, got %q", got)
	}
	if got := miner.customDiff; got != 50000 {
		t.Fatalf("expected custom diff 50000, got %d", got)
	}
}

func TestMiner_HandleKeepalived_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := NewMiner(minerConn, 3333, nil)

	done := make(chan struct{})
	go func() {
		miner.handleKeepalived(stratumRequest{ID: 9, Method: "keepalived"})
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read keepalived response: %v", err)
	}
	<-done

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal keepalived response: %v", err)
	}
	if _, ok := payload["error"]; !ok {
		t.Fatalf("expected keepalived response to include error field, got %s", string(line))
	}
	if string(payload["error"]) != "null" {
		t.Fatalf("expected keepalived response error to be null, got %s", string(payload["error"]))
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(payload["result"], &result); err != nil {
		t.Fatalf("unmarshal keepalived result: %v", err)
	}
	if result.Status != "KEEPALIVED" {
		t.Fatalf("expected KEEPALIVED status, got %q", result.Status)
	}
}

func TestMiner_ReadLoop_RFCLineLimit_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := NewMiner(minerConn, 3333, nil)
	miner.onLogin = func(m *Miner) {
		m.SetRouteID(1)
	}
	miner.Start()

	params, err := json.Marshal(loginParams{
		Login: "wallet",
		Pass:  "x",
		Agent: strings.Repeat("a", 5000),
	})
	if err != nil {
		t.Fatalf("marshal login params: %v", err)
	}
	request, err := json.Marshal(stratumRequest{ID: 4, Method: "login", Params: params})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if len(request) >= maxStratumLineLength {
		t.Fatalf("expected test request below RFC limit, got %d bytes", len(request))
	}

	if _, err := clientConn.Write(append(request, '\n')); err != nil {
		t.Fatalf("write login request: %v", err)
	}
	_ = clientConn.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read login response: %v", err)
	}
	if len(line) == 0 {
		t.Fatal("expected login response for request under RFC limit")
	}
}

func TestMiner_ReadLoop_RFCLineLimit_Ugly(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := NewMiner(minerConn, 3333, nil)
	miner.Start()

	params, err := json.Marshal(loginParams{
		Login: "wallet",
		Pass:  "x",
		Agent: strings.Repeat("b", maxStratumLineLength),
	})
	if err != nil {
		t.Fatalf("marshal login params: %v", err)
	}
	request, err := json.Marshal(stratumRequest{ID: 5, Method: "login", Params: params})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if len(request) <= maxStratumLineLength {
		t.Fatalf("expected test request above RFC limit, got %d bytes", len(request))
	}

	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := clientConn.Write(append(request, '\n'))
		writeDone <- writeErr
	}()

	var writeErr error
	select {
	case writeErr = <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("timed out writing oversized request")
	}
	if writeErr == nil {
		_ = clientConn.SetReadDeadline(time.Now().Add(time.Second))
		line, err := bufio.NewReader(clientConn).ReadBytes('\n')
		if err == nil || len(line) > 0 {
			t.Fatalf("expected oversized request to close the connection, got line=%q err=%v", string(line), err)
		}
		return
	}

	if !strings.Contains(writeErr.Error(), "closed pipe") {
		t.Fatalf("expected oversized request to close the connection, got write error %v", writeErr)
	}
}
