package simple

import (
	"testing"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

func TestSimpleSplitterFile_NewSimpleSplitter_Good(t *testing.T) {
	bus := proxy.NewEventBus()
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, bus, nil)
	if splitter == nil {
		t.Fatal("expected splitter")
	}
	if splitter.config == nil || splitter.events != bus {
		t.Fatal("expected splitter to retain config and event bus")
	}
	if splitter.active == nil || splitter.idle == nil {
		t.Fatal("expected splitter maps to be initialised")
	}
}

func TestSimpleSplitterFile_NewSimpleSplitter_Bad(t *testing.T) {
	splitter := NewSimpleSplitter(nil, nil, nil)
	if splitter == nil {
		t.Fatal("expected splitter")
	}
	if splitter.factory == nil {
		t.Fatal("expected default strategy factory to be installed")
	}
}

func TestSimpleSplitterFile_NewSimpleSplitter_Ugly(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, proxy.NewEventBus(), func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	splitter.Connect()
	splitter.Connect()
	if len(splitter.active) != 0 || len(splitter.idle) != 0 {
		t.Fatalf("expected empty splitter to stay empty after repeated connect, got active=%d idle=%d", len(splitter.active), len(splitter.idle))
	}
}

func TestSimpleSplitterFile_OnClose_Good(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, proxy.NewEventBus(), func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	miner := &proxy.Miner{}
	miner.SetID(9)
	mapper := &SimpleMapper{id: 7, miner: miner, strategy: activeStrategy{}, pending: make(map[int64]submitContext)}
	splitter.active[miner.ID()] = mapper

	splitter.OnClose(&proxy.CloseEvent{Miner: miner})

	if miner.RouteID() != -1 {
		t.Fatalf("expected route id to be cleared, got %d", miner.RouteID())
	}
	if len(splitter.idle) != 1 {
		t.Fatalf("expected mapper to move to idle pool, got %d idle mappers", len(splitter.idle))
	}
}

func TestSimpleSplitterFile_OnClose_Bad(t *testing.T) {
	var splitter *SimpleSplitter
	splitter.OnClose(nil)
	if splitter != nil {
		t.Fatal("expected nil splitter to remain nil")
	}
}

func TestSimpleSplitterFile_OnClose_Ugly(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{}, nil, nil)
	splitter.OnClose(&proxy.CloseEvent{Miner: &proxy.Miner{}})
	if len(splitter.active) != 0 || len(splitter.idle) != 0 {
		t.Fatalf("expected close for unknown miner to leave maps empty, active=%d idle=%d", len(splitter.active), len(splitter.idle))
	}
}
