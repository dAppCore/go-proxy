package proxy

import (
	"net"
	"testing"
	"time"
)

func TestProxy_Start_Good(t *testing.T) {
	cfg := &Config{
		Mode:    "simple",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 0}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected proxy to build, got error: %v", result.Error)
	}

	done := make(chan struct{})
	go func() {
		p.Start()
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	var listenerAddr string
	for time.Now().Before(deadline) {
		if len(p.servers) > 0 && p.servers[0] != nil && p.servers[0].listener != nil {
			listenerAddr = p.servers[0].listener.Addr().String()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if listenerAddr == "" {
		p.Stop()
		<-done
		t.Fatal("expected proxy listener to be available")
	}

	conn, err := net.Dial("tcp", listenerAddr)
	if err != nil {
		p.Stop()
		<-done
		t.Fatalf("dial proxy listener: %v", err)
	}

	for time.Now().Before(deadline) {
		if len(p.MinerSnapshots()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(p.MinerSnapshots()) == 0 {
		_ = conn.Close()
		p.Stop()
		<-done
		t.Fatal("expected accepted connection to create a miner")
	}

	_ = conn.Close()
	p.Stop()
	<-done
}

func TestProxy_Start_Bad(t *testing.T) {
	var p *Proxy
	p.Start()
}

func TestProxy_Start_Ugly(t *testing.T) {
	cfg := &Config{
		Mode:    "simple",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 0}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected proxy to build, got error: %v", result.Error)
	}

	done := make(chan struct{})
	go func() {
		p.Start()
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	p.Stop()
	<-done

	if got := len(p.MinerSnapshots()); got != 0 {
		t.Fatalf("expected no miners when no connections were accepted, got %d", got)
	}
}
