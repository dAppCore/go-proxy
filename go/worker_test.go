package proxy

import "testing"

func TestWorker_workerNameFor_Good(t *testing.T) {
	miner := &Miner{
		user:     "wallet",
		password: "secret",
		agent:    "xmrig/6.21.0",
		rigID:    "rig-1",
		ip:       "10.0.0.1",
	}

	cases := map[WorkersMode]string{
		WorkersByRigID:  "rig-1",
		WorkersByUser:   "wallet",
		WorkersByPass:   "secret",
		WorkersByAgent:  "xmrig/6.21.0",
		WorkersByIP:     "10.0.0.1",
		WorkersDisabled: "",
	}

	for mode, expected := range cases {
		if got := workerNameFor(mode, miner); got != expected {
			t.Fatalf("expected worker name %q for mode %q, got %q", expected, mode, got)
		}
	}
}

func TestWorker_workerNameFor_Bad(t *testing.T) {
	if got := workerNameFor(WorkersByUser, nil); got != "" {
		t.Fatalf("expected nil miner to yield empty worker name, got %q", got)
	}
}

func TestWorker_workerNameFor_Ugly(t *testing.T) {
	miner := &Miner{
		user: "wallet",
		ip:   "10.0.0.9",
	}

	if got := workerNameFor(WorkersByRigID, miner); got != "wallet" {
		t.Fatalf("expected missing rig id to fall back to user, got %q", got)
	}
	if got := workerNameFor(WorkersMode("mystery"), miner); got != "wallet" {
		t.Fatalf("expected unknown mode to fall back to user, got %q", got)
	}
}

func TestWorker_NewWorkers_Good(t *testing.T) {
	bus := NewEventBus()
	workers := NewWorkers(WorkersByRigID, bus)
	miner := &Miner{id: 7, user: "wallet", rigID: "rig-1", ip: "10.0.0.1"}

	bus.Dispatch(Event{Type: EventLogin, Miner: miner})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	if records[0].Name != "rig-1" {
		t.Fatalf("expected rig id worker name, got %q", records[0].Name)
	}
	if records[0].Connections != 1 {
		t.Fatalf("expected one connection, got %d", records[0].Connections)
	}
}

func TestWorker_NewWorkers_Bad(t *testing.T) {
	workers := NewWorkers(WorkersDisabled, nil)
	if workers == nil {
		t.Fatalf("expected workers instance")
	}
	if got := workers.List(); len(got) != 0 {
		t.Fatalf("expected no worker records, got %d", len(got))
	}
}

func TestWorker_NewWorkers_Ugly(t *testing.T) {
	bus := NewEventBus()
	workers := NewWorkers(WorkersByUser, bus)
	workers.bindEvents(bus)

	miner := &Miner{id: 11, user: "wallet", ip: "10.0.0.2"}
	bus.Dispatch(Event{Type: EventLogin, Miner: miner})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	if records[0].Connections != 1 {
		t.Fatalf("expected a single subscription path, got %d connections", records[0].Connections)
	}
}

// TestWorker_Hashrate_Good verifies that recording an accepted share produces a nonzero
// hashrate reading from the 60-second window.
//
//	record := proxy.WorkerRecord{}
//	record.Hashrate(60) // > 0.0 after an accepted share
func TestWorker_Hashrate_Good(t *testing.T) {
	bus := NewEventBus()
	workers := NewWorkers(WorkersByUser, bus)

	miner := &Miner{id: 100, user: "hashtest", ip: "10.0.0.10"}
	bus.Dispatch(Event{Type: EventLogin, Miner: miner})
	bus.Dispatch(Event{Type: EventAccept, Miner: miner, Diff: 50000})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	hr := records[0].Hashrate(60)
	if hr <= 0 {
		t.Fatalf("expected nonzero hashrate for 60-second window after accept, got %f", hr)
	}
}

// TestWorker_Hashrate_Bad verifies that an invalid window size returns 0.
//
//	record := proxy.WorkerRecord{}
//	record.Hashrate(999) // 0.0 (unsupported window)
func TestWorker_Hashrate_Bad(t *testing.T) {
	bus := NewEventBus()
	workers := NewWorkers(WorkersByUser, bus)

	miner := &Miner{id: 101, user: "hashtest-bad", ip: "10.0.0.11"}
	bus.Dispatch(Event{Type: EventLogin, Miner: miner})
	bus.Dispatch(Event{Type: EventAccept, Miner: miner, Diff: 50000})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	hr := records[0].Hashrate(999)
	if hr != 0 {
		t.Fatalf("expected zero hashrate for unsupported window, got %f", hr)
	}
	hrZero := records[0].Hashrate(0)
	if hrZero != 0 {
		t.Fatalf("expected zero hashrate for zero window, got %f", hrZero)
	}
	hrNeg := records[0].Hashrate(-1)
	if hrNeg != 0 {
		t.Fatalf("expected zero hashrate for negative window, got %f", hrNeg)
	}
}

// TestWorker_Hashrate_Ugly verifies that calling Hashrate on a nil record returns 0
// and that a worker with no accepts also returns 0.
//
//	var record *proxy.WorkerRecord
//	record.Hashrate(60) // 0.0
func TestWorker_Hashrate_Ugly(t *testing.T) {
	var nilRecord *WorkerRecord
	if hr := nilRecord.Hashrate(60); hr != 0 {
		t.Fatalf("expected zero hashrate for nil record, got %f", hr)
	}

	bus := NewEventBus()
	workers := NewWorkers(WorkersByUser, bus)

	miner := &Miner{id: 102, user: "hashtest-ugly", ip: "10.0.0.12"}
	bus.Dispatch(Event{Type: EventLogin, Miner: miner})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	hr := records[0].Hashrate(60)
	if hr != 0 {
		t.Fatalf("expected zero hashrate for worker with no accepts, got %f", hr)
	}
}

func TestWorker_CustomDiffOrdering_Good(t *testing.T) {
	// target symbol: CustomDiffOrdering
	cfg := &Config{
		Mode:          "nicehash",
		Workers:       WorkersByUser,
		Bind:          []BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:         []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		CustomDiff:    50000,
		AccessLogFile: "",
	}

	p, result := New(cfg)
	if !result.OK {
		t.Fatalf("expected valid proxy, got error: %v", result.Error)
	}

	miner := &Miner{
		id:   21,
		user: "WALLET+50000",
		ip:   "10.0.0.3",
		conn: noopConn{},
	}
	p.events.Dispatch(Event{Type: EventLogin, Miner: miner})

	records := p.WorkerRecords()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	if records[0].Name != "WALLET" {
		t.Fatalf("expected custom diff login suffix to be stripped before worker registration, got %q", records[0].Name)
	}
	if miner.User() != "WALLET" {
		t.Fatalf("expected miner user to be stripped before downstream consumers, got %q", miner.User())
	}
}

func TestWorker_Tick_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 201, user: "tick", ip: "10.0.0.13"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnAccept(Event{Miner: miner, Diff: 100})

	before := workers.List()[0].Hashrate(60)
	workers.Tick()
	after := workers.List()[0].Hashrate(60)

	if before == 0 || after == 0 {
		t.Fatalf("expected tick to preserve a nonzero hashrate sample, before=%f after=%f", before, after)
	}
}

func TestWorker_Tick_Bad(t *testing.T) {
	var workers *Workers
	workers.Tick()
	if workers != nil {
		t.Fatal("expected nil workers to remain nil")
	}
}

func TestWorker_Tick_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	workers.entries = []WorkerRecord{{Name: "manual"}}

	workers.Tick()
	if got := workers.List(); len(got) != 1 {
		t.Fatalf("expected tick to preserve zero-window records, got %d records", len(got))
	}
	if got := workers.List()[0].Name; got != "manual" {
		t.Fatalf("expected manual worker record to remain intact, got %q", got)
	}
}

func TestWorker_OnReject_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 301, user: "reject", ip: "10.0.0.20"}
	workers.OnLogin(Event{Miner: miner})

	workers.OnReject(Event{Miner: miner, Error: "Low difficulty share"})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	if records[0].Rejected != 1 {
		t.Fatalf("expected one rejected share, got %d", records[0].Rejected)
	}
	if records[0].Invalid != 1 {
		t.Fatalf("expected invalid share reason to increment invalid count, got %d", records[0].Invalid)
	}
	if records[0].LastIP != miner.ip {
		t.Fatalf("expected reject to update last IP, got %q", records[0].LastIP)
	}
}

func TestWorker_OnReject_Bad(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	workers.OnReject(Event{})

	if got := workers.List(); len(got) != 0 {
		t.Fatalf("expected nil reject event to be ignored, got %d records", len(got))
	}
}

func TestWorker_OnReject_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 302, user: "reject-ugly", ip: "10.0.0.21"}
	workers.OnLogin(Event{Miner: miner})

	workers.OnReject(Event{Miner: miner, Error: "transient upstream failure"})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	if records[0].Rejected != 1 {
		t.Fatalf("expected rejected share to be counted, got %d", records[0].Rejected)
	}
	if records[0].Invalid != 0 {
		t.Fatalf("expected non-invalid rejection reason to leave invalid count at zero, got %d", records[0].Invalid)
	}
}

func TestWorker_OnClose_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 401, user: "closing", ip: "10.0.0.30"}
	workers.OnLogin(Event{Miner: miner})

	workers.OnClose(Event{Miner: miner})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record to remain, got %d", len(records))
	}
	if records[0].Connections != 1 {
		t.Fatalf("expected cumulative connection count to remain at one, got %d", records[0].Connections)
	}
	if _, ok := workers.idIndex[miner.id]; ok {
		t.Fatal("expected miner id to be removed from worker index")
	}
}

func TestWorker_OnClose_Bad(t *testing.T) {
	var workers *Workers
	workers.OnClose(Event{})

	workers = NewWorkers(WorkersByUser, nil)
	workers.OnClose(Event{})
	workers.OnClose(Event{Miner: nil})

	if got := workers.List(); len(got) != 0 {
		t.Fatalf("expected nil close events to be ignored, got %d records", len(got))
	}
}

func TestWorker_OnClose_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 402, user: "closing-ugly", ip: "10.0.0.31"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnClose(Event{Miner: miner})

	unknown := &Miner{id: 999, user: "unknown", ip: "10.0.0.99"}
	workers.OnClose(Event{Miner: unknown})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected unknown closes not to affect existing records, got %d", len(records))
	}
	if records[0].Connections != 1 {
		t.Fatalf("expected original worker connection count to remain cumulative, got %d", records[0].Connections)
	}
}

func TestWorker_EventBusCloseCleanup_Good(t *testing.T) {
	// target symbol: EventBusCloseCleanup
	bus := NewEventBus()
	workers := NewWorkers(WorkersByUser, bus)
	miner := &Miner{id: 403, user: "close-bus", ip: "10.0.0.32"}

	bus.Dispatch(Event{Type: EventLogin, Miner: miner})
	bus.Dispatch(Event{Type: EventClose, Miner: miner})
	bus.Dispatch(Event{Type: EventAccept, Miner: miner, Diff: 1000})

	records := workers.List()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	if records[0].Accepted != 0 {
		t.Fatalf("expected closed miner accepts to be ignored, got %d", records[0].Accepted)
	}
}
