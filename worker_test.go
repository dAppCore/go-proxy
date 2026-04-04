package proxy

import "testing"

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
