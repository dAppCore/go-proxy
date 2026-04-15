package simple

import (
	"testing"
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

type simpleStrategySpy struct {
	active      bool
	connects    int
	submits     int
	disconnects int
	ticks       int
}

func (s *simpleStrategySpy) Connect() { s.connects++ }
func (s *simpleStrategySpy) Submit(string, string, string, string) int64 {
	s.submits++
	return int64(s.submits)
}
func (s *simpleStrategySpy) Disconnect()    { s.disconnects++ }
func (s *simpleStrategySpy) IsActive() bool { return s.active }
func (s *simpleStrategySpy) Tick(uint64)    { s.ticks++ }

func TestSimpleSplitter_OnClose_Good(t *testing.T) {
	spy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)
	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	splitter.OnClose(&proxy.CloseEvent{Miner: miner})

	if miner.RouteID() != -1 {
		t.Fatalf("expected route id to be cleared, got %d", miner.RouteID())
	}
	if len(splitter.active) != 0 || len(splitter.idle) != 1 {
		t.Fatalf("expected mapper to move to idle, got active=%d idle=%d", len(splitter.active), len(splitter.idle))
	}
	if spy.disconnects != 0 {
		t.Fatalf("expected reusable mapper not to disconnect immediately, got %d", spy.disconnects)
	}
}

func TestSimpleSplitter_Connect_Good(t *testing.T) {
	activeSpy := &simpleStrategySpy{active: true}
	idleSpy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return activeSpy
	})
	splitter.active[1] = &SimpleMapper{id: 1, strategy: activeSpy}
	splitter.idle[2] = &SimpleMapper{id: 2, strategy: idleSpy, idleAt: time.Now()}

	splitter.Connect()

	if activeSpy.connects != 1 || idleSpy.connects != 1 {
		t.Fatalf("expected both active and idle mappers to connect, got active=%d idle=%d", activeSpy.connects, idleSpy.connects)
	}
}

func TestSimpleSplitter_Connect_Bad(t *testing.T) {
	var splitter *SimpleSplitter
	splitter.Connect()
}

func TestSimpleSplitter_Connect_Ugly(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return &simpleStrategySpy{active: true}
	})
	splitter.Connect()
	if len(splitter.active) != 0 || len(splitter.idle) != 0 {
		t.Fatalf("expected empty splitter to remain empty, got active=%d idle=%d", len(splitter.active), len(splitter.idle))
	}
}

func TestSimpleSplitter_OnClose_Bad(t *testing.T) {
	var splitter *SimpleSplitter
	splitter.OnClose(nil)
}

func TestSimpleSplitter_OnClose_Ugly(t *testing.T) {
	spy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 0}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)
	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})
	splitter.OnClose(&proxy.CloseEvent{Miner: miner})

	if spy.disconnects != 1 {
		t.Fatalf("expected non-reusable mapper to disconnect, got %d", spy.disconnects)
	}
	if len(splitter.idle) != 0 {
		t.Fatalf("expected no idle mappers when reuse is disabled, got %d", len(splitter.idle))
	}
}

func TestSimpleSplitter_GC_Good(t *testing.T) {
	spy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 1}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	mapper := &SimpleMapper{id: 1, strategy: spy, idleAt: time.Now().Add(-2 * time.Second), stopped: true}
	splitter.idle[1] = mapper

	splitter.GC()

	if len(splitter.idle) != 0 {
		t.Fatalf("expected expired idle mapper to be collected, got %d", len(splitter.idle))
	}
	if spy.disconnects != 1 {
		t.Fatalf("expected collected mapper to disconnect, got %d", spy.disconnects)
	}
}

func TestSimpleSplitter_GC_Bad(t *testing.T) {
	var splitter *SimpleSplitter
	splitter.GC()
}

func TestSimpleSplitter_GC_Ugly(t *testing.T) {
	spy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	mapper := &SimpleMapper{id: 1, strategy: spy, idleAt: time.Now()}
	splitter.idle[1] = mapper

	splitter.GC()

	if len(splitter.idle) != 1 {
		t.Fatalf("expected young idle mapper to remain, got %d", len(splitter.idle))
	}
}

func TestSimpleSplitter_Tick_Good(t *testing.T) {
	spy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 1}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	splitter.active[1] = &SimpleMapper{id: 1, strategy: spy}

	splitter.Tick(60)

	if spy.ticks != 1 {
		t.Fatalf("expected tick to be forwarded, got %d", spy.ticks)
	}
}

func TestSimpleSplitter_Tick_Bad(t *testing.T) {
	var splitter *SimpleSplitter
	splitter.Tick(1)
}

func TestSimpleSplitter_Tick_Ugly(t *testing.T) {
	spy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 1}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	splitter.idle[1] = &SimpleMapper{id: 1, strategy: spy, stopped: true, idleAt: time.Now().Add(-2 * time.Second)}

	splitter.Tick(60)

	if len(splitter.idle) != 0 {
		t.Fatalf("expected GC to remove expired idle mapper during tick, got %d", len(splitter.idle))
	}
}

func TestSimpleSplitter_Disconnect_Good(t *testing.T) {
	spy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	splitter.active[1] = &SimpleMapper{id: 1, strategy: spy}
	splitter.idle[2] = &SimpleMapper{id: 2, strategy: spy, idleAt: time.Now()}

	splitter.Disconnect()

	if spy.disconnects != 2 {
		t.Fatalf("expected both mappers to disconnect, got %d", spy.disconnects)
	}
	if len(splitter.active) != 0 || len(splitter.idle) != 0 {
		t.Fatalf("expected all mappers to be cleared, got active=%d idle=%d", len(splitter.active), len(splitter.idle))
	}
}

func TestSimpleSplitter_Disconnect_Bad(t *testing.T) {
	var splitter *SimpleSplitter
	splitter.Disconnect()
}

func TestSimpleSplitter_Disconnect_Ugly(t *testing.T) {
	spy := &simpleStrategySpy{active: true}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	splitter.Disconnect()
	if spy.disconnects != 0 {
		t.Fatalf("expected empty splitter to leave strategy untouched, got %d", spy.disconnects)
	}
}

func TestSimpleMapper_OnDisconnect_Good(t *testing.T) {
	mapper := NewSimpleMapper(1, &simpleStrategySpy{active: true})
	mapper.OnDisconnect()
	if !mapper.stopped {
		t.Fatal("expected mapper to be marked stopped")
	}
}
