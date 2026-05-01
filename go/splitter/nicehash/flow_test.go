package nicehash

import (
	"testing"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

type nicehashStrategySpy struct {
	active      bool
	connects    int
	submits     int
	disconnects int
	ticks       int
}

func (s *nicehashStrategySpy) Connect() { s.connects++ }
func (s *nicehashStrategySpy) Submit(string, string, string, string) int64 {
	s.submits++
	return int64(s.submits)
}
func (s *nicehashStrategySpy) Disconnect()    { s.disconnects++ }
func (s *nicehashStrategySpy) IsActive() bool { return s.active }
func (s *nicehashStrategySpy) Tick(uint64)    { s.ticks++ }

func TestFlow_NonceSplitter_OnLogin_Good(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)

	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	if !miner.ExtendedNiceHash() {
		t.Fatal("expected miner to be marked as NiceHash")
	}
	if miner.MapperID() != 0 {
		t.Fatalf("expected mapper id 0, got %d", miner.MapperID())
	}
	if got := len(splitter.mappers); got != 1 {
		t.Fatalf("expected one mapper, got %d", got)
	}
	if spy.connects != 1 {
		t.Fatalf("expected mapper strategy to connect once, got %d", spy.connects)
	}
}

func TestFlow_NonceSplitter_Connect_Good(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})

	splitter.Connect()

	if got := len(splitter.mappers); got != 1 {
		t.Fatalf("expected one mapper after connect, got %d", got)
	}
	if spy.connects != 1 {
		t.Fatalf("expected upstream strategy to connect once, got %d", spy.connects)
	}
}

func TestFlow_NonceSplitter_Connect_Bad(t *testing.T) {
	var splitter *NonceSplitter
	splitter.Connect()
	if splitter != nil {
		t.Fatal("expected nil splitter to remain nil")
	}
}

func TestFlow_NonceSplitter_Connect_Ugly(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})

	splitter.Connect()
	splitter.Connect()

	if spy.connects != 1 {
		t.Fatalf("expected repeated connect calls to stay idempotent per mapper, got %d", spy.connects)
	}
}

func TestFlow_NonceSplitter_OnLogin_Bad(t *testing.T) {
	var splitter *NonceSplitter
	splitter.OnLogin(nil)
	if splitter != nil {
		t.Fatal("expected nil splitter to remain nil")
	}
}

func TestFlow_NonceSplitter_OnLogin_Ugly(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	for i := 0; i < 257; i++ {
		miner := &proxy.Miner{}
		miner.SetID(int64(i + 1))
		splitter.OnLogin(&proxy.LoginEvent{Miner: miner})
	}
	if got := len(splitter.mappers); got != 2 {
		t.Fatalf("expected a second mapper after 257 miners, got %d", got)
	}
}

func TestFlow_NonceSplitter_OnSubmit_Good(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)
	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})
	mapper := splitter.mappers[0]
	mapper.OnJob(proxy.Job{Blob: repeatString("0", 160), JobID: "job-1", Target: "b88d0600"})

	splitter.OnSubmit(&proxy.SubmitEvent{Miner: miner, JobID: "job-1", Nonce: "deadbeef", Result: "hash", RequestID: 7})

	if spy.submits != 1 {
		t.Fatalf("expected one submit routed to upstream, got %d", spy.submits)
	}
	if len(mapper.pending) != 1 {
		t.Fatalf("expected one pending submit, got %d", len(mapper.pending))
	}
}

func TestFlow_NonceSplitter_OnSubmit_Bad(t *testing.T) {
	var splitter *NonceSplitter
	splitter.OnSubmit(nil)
	if splitter != nil {
		t.Fatal("expected nil splitter to remain nil")
	}
}

func TestFlow_NonceSplitter_OnSubmit_Ugly(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)
	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})
	splitter.OnSubmit(&proxy.SubmitEvent{Miner: miner, JobID: "missing", Nonce: "deadbeef", Result: "hash", RequestID: 9})

	if spy.submits != 0 {
		t.Fatalf("expected invalid job submission not to reach upstream, got %d", spy.submits)
	}
}

func TestFlow_NonceSplitter_OnClose_Good(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)
	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})
	splitter.OnClose(&proxy.CloseEvent{Miner: miner})

	free, dead, active := splitter.mappers[0].storage.SlotCount()
	if dead != 1 || active != 0 || free != 255 {
		t.Fatalf("expected closed miner slot to become dead, got free=%d dead=%d active=%d", free, dead, active)
	}
}

func TestFlow_NonceSplitter_OnClose_Bad(t *testing.T) {
	var splitter *NonceSplitter
	splitter.OnClose(nil)
	if splitter != nil {
		t.Fatal("expected nil splitter to remain nil")
	}
}

func TestFlow_NonceSplitter_OnClose_Ugly(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)
	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})
	splitter.OnClose(&proxy.CloseEvent{Miner: miner})
	splitter.GC()

	if spy.disconnects != 0 {
		t.Fatalf("expected active mapper to remain connected before idle timeout, got %d", spy.disconnects)
	}
}

func TestFlow_NonceSplitter_Tick_Good(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)
	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	splitter.Tick(60)

	if spy.ticks != 1 {
		t.Fatalf("expected strategy tick to be forwarded, got %d", spy.ticks)
	}
}

func TestFlow_NonceSplitter_Disconnect_Good(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, proxy.NewEventBus(), func(pool.StratumListener) pool.Strategy {
		return spy
	})
	miner := &proxy.Miner{}
	miner.SetID(1)
	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	splitter.Disconnect()

	if spy.disconnects != 1 {
		t.Fatalf("expected strategy disconnect to be forwarded, got %d", spy.disconnects)
	}
	if len(splitter.mappers) != 0 {
		t.Fatalf("expected mappers to be cleared, got %d", len(splitter.mappers))
	}
}

func TestNonceMapper_OnJob_IsActive_Good(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{Mode: "nicehash"}, &nicehashStrategySpy{active: true})
	miner := &proxy.Miner{}
	miner.SetID(1)
	if !mapper.Add(miner) {
		t.Fatal("expected miner to be added")
	}
	if mapper.IsActive() {
		t.Fatal("expected mapper to be inactive before a job arrives")
	}
	mapper.OnJob(proxy.Job{Blob: repeatString("0", 160), JobID: "job-1", Target: "b88d0600"})
	if !mapper.IsActive() {
		t.Fatal("expected mapper to become active after job")
	}
}

func TestFlow_NonceMapper_OnDisconnect_Good(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{Mode: "nicehash"}, &nicehashStrategySpy{active: true})
	mapper.OnDisconnect()
	if mapper.active {
		t.Fatal("expected mapper to be inactive after disconnect")
	}
	if mapper.suspended != 1 {
		t.Fatalf("expected suspended counter to increment, got %d", mapper.suspended)
	}
}
