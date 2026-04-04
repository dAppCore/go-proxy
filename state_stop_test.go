package proxy

import (
	"net"
	"testing"
	"time"
)

func TestProxy_Stop_Good(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	miner := NewMiner(clientConn, 3333, nil)
	proxyInstance := &Proxy{
		done:   make(chan struct{}),
		miners: map[int64]*Miner{miner.ID(): miner},
	}

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := serverConn.Read(buf)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	proxyInstance.Stop()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected miner connection to close during Stop")
		}
	case <-time.After(time.Second):
		t.Fatalf("expected miner connection to close during Stop")
	}
}

func TestProxy_Stop_Bad(t *testing.T) {
	var proxyInstance *Proxy

	proxyInstance.Stop()
}

func TestProxy_Stop_Ugly(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	miner := NewMiner(clientConn, 3333, nil)
	proxyInstance := &Proxy{
		done:   make(chan struct{}),
		miners: map[int64]*Miner{miner.ID(): miner},
	}

	proxyInstance.Stop()
	proxyInstance.Stop()

	buf := make([]byte, 1)
	if _, err := serverConn.Read(buf); err == nil {
		t.Fatalf("expected closed connection after repeated Stop calls")
	}
}
