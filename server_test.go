package proxy

import (
	"net"
	"testing"
	"time"
)

func TestProxy_buildServers_Good(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertPair(t, dir)

	p := &Proxy{
		config: &Config{
			Mode:    "nicehash",
			Workers: WorkersByRigID,
			Bind: []BindAddr{
				{Host: "127.0.0.1", Port: 0},
				{Host: "127.0.0.1", Port: 0, TLS: true},
			},
			Pools: []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
			TLS: TLSConfig{
				Enabled:  true,
				CertFile: certFile,
				KeyFile:  keyFile,
			},
		},
		done: make(chan struct{}),
	}

	if result := p.buildServers(); !result.OK {
		t.Fatalf("expected buildServers to succeed, got %v", result.Error)
	}
	if got := len(p.servers); got != 2 {
		t.Fatalf("expected two servers, got %d", got)
	}
	if addr := p.ServerListenerAddr(0); addr == "" {
		t.Fatal("expected first server listener address to be recorded")
	}
	if addr := p.ServerListenerAddr(1); addr == "" {
		t.Fatal("expected second server listener address to be recorded")
	}

	p.Stop()
}

func TestProxy_buildServers_Bad(t *testing.T) {
	var p *Proxy
	if result := p.buildServers(); !result.OK {
		t.Fatalf("expected nil proxy to be treated as a no-op success, got %v", result.Error)
	}
}

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
		if addr := p.ServerListenerAddr(0); addr != "" {
			listenerAddr = addr
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

func TestServer_listen_BlankHost_Bad(t *testing.T) {
	srv, result := NewServer(BindAddr{Host: "", Port: 0}, nil, nil, func(net.Conn, uint16) {})
	if result.OK || srv != nil {
		t.Fatalf("expected blank host to fail listener construction, got srv=%#v result=%+v", srv, result)
	}
}

func TestProxy_buildServers_TLSListenerRequiresEnabled_Bad(t *testing.T) {
	cfg := &Config{
		Mode:    "simple",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 0, TLS: true}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		TLS:     TLSConfig{Enabled: false},
	}
	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected proxy construction to succeed before buildServers check, got %v", result.Error)
	}

	if buildResult := p.buildServers(); buildResult.OK {
		t.Fatal("expected TLS listener without enabled TLS config to fail")
	}
}
