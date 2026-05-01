package proxy

import (
	"testing"
	"time"
)

func TestProxy_Stop_WaitsForSubmitDrain(t *testing.T) {
	p := &Proxy{
		done: make(chan struct{}),
	}
	p.submitCount.Store(1)

	stopped := make(chan struct{})
	go func() {
		p.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatalf("expected Stop to wait for pending submits")
	case <-time.After(50 * time.Millisecond):
	}

	p.submitCount.Store(0)

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatalf("expected Stop to finish after pending submits drain")
	}
}
