package pool

import (
	"sync/atomic"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

type disconnectSpy struct {
	disconnects atomic.Int64
}

func (s *disconnectSpy) OnJob(proxy.Job) {}

func (s *disconnectSpy) OnResultAccepted(int64, bool, string) {}

func (s *disconnectSpy) OnDisconnect() {
	s.disconnects.Add(1)
}

func TestFailoverStrategy_Disconnect_Good(t *testing.T) {
	spy := &disconnectSpy{}
	strategy := &FailoverStrategy{
		listener: spy,
		client:   &StratumClient{listener: nil},
	}
	strategy.client.listener = strategy

	strategy.Disconnect()
	time.Sleep(10 * time.Millisecond)

	if got := spy.disconnects.Load(); got != 0 {
		t.Fatalf("expected intentional disconnect to suppress reconnect, got %d listener calls", got)
	}
}

func TestFailoverStrategy_Disconnect_Bad(t *testing.T) {
	spy := &disconnectSpy{}
	strategy := &FailoverStrategy{listener: spy}

	strategy.OnDisconnect()

	if got := spy.disconnects.Load(); got != 1 {
		t.Fatalf("expected external disconnect to notify listener once, got %d", got)
	}
}

func TestFailoverStrategy_Disconnect_Ugly(t *testing.T) {
	spy := &disconnectSpy{}
	strategy := &FailoverStrategy{
		listener: spy,
		client:   &StratumClient{listener: nil},
	}
	strategy.client.listener = strategy

	strategy.Disconnect()
	strategy.Disconnect()
	time.Sleep(10 * time.Millisecond)

	if got := spy.disconnects.Load(); got != 0 {
		t.Fatalf("expected repeated intentional disconnects to remain silent, got %d listener calls", got)
	}
}
