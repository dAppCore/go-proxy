package proxy

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"testing"
)

func TestMiner_HandleLogin_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := NewMiner(minerConn, 3333, nil)
	miner.algoEnabled = true
	miner.extNH = true
	miner.fixedByte = 0x2a
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

	if payload.Result.Status != "OK" {
		t.Fatalf("expected login success, got %q", payload.Result.Status)
	}
	if payload.Result.ID == "" {
		t.Fatalf("expected rpc id in login response")
	}
	if len(payload.Result.Extensions) != 1 || payload.Result.Extensions[0] != "algo" {
		t.Fatalf("expected algo extension, got %#v", payload.Result.Extensions)
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

func TestProxy_New_Watch_Good(t *testing.T) {
	cfg := &Config{
		Mode:       "nicehash",
		Workers:    WorkersByRigID,
		Bind:       []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:      []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		Watch:      true,
		sourcePath: "/tmp/proxy.json",
	}

	proxyInstance, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}
	if proxyInstance.watcher == nil {
		t.Fatalf("expected config watcher when watch is enabled and source path is known")
	}
}
