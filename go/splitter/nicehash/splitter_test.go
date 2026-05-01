package nicehash

import (
	"testing"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

func TestNicehashSplitterFile_NewNonceSplitter_Good(t *testing.T) {
	bus := proxy.NewEventBus()
	splitter := NewNonceSplitter(&proxy.Config{Mode: "nicehash"}, bus, nil)
	if splitter == nil {
		t.Fatal("expected splitter")
	}
	if splitter.config == nil || splitter.events != bus {
		t.Fatal("expected splitter to retain config and event bus")
	}
	if splitter.mapperByID == nil {
		t.Fatal("expected mapper index to be initialised")
	}
}

func TestNicehashSplitterFile_NewNonceSplitter_Bad(t *testing.T) {
	splitter := NewNonceSplitter(nil, nil, nil)
	if splitter == nil {
		t.Fatal("expected splitter")
	}
	if splitter.strategyFactory == nil {
		t.Fatal("expected default strategy factory to be installed")
	}
}

func TestNicehashSplitterFile_NewNonceSplitter_Ugly(t *testing.T) {
	splitter := NewNonceSplitter(&proxy.Config{}, proxy.NewEventBus(), func(listener pool.StratumListener) pool.Strategy {
		return mapperStrategyStub{}
	})
	splitter.Connect()
	splitter.Connect()
	if len(splitter.mappers) != 1 {
		t.Fatalf("expected repeated connects to keep a single mapper, got %d", len(splitter.mappers))
	}
}

func TestNicehashSplitterFile_OnLogin_Good(t *testing.T) {
	splitter := NewNonceSplitter(&proxy.Config{}, proxy.NewEventBus(), func(listener pool.StratumListener) pool.Strategy {
		return mapperStrategyStub{}
	})
	mapper := NewNonceMapper(1, &proxy.Config{}, mapperStrategyStub{})
	splitter.mappers = []*NonceMapper{mapper}
	splitter.mapperByID[mapper.id] = mapper
	miner := &proxy.Miner{}

	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	if !miner.ExtendedNiceHash() {
		t.Fatal("expected miner to enter nicehash mode")
	}
	if miner.MapperID() != mapper.id {
		t.Fatalf("expected mapper id %d, got %d", mapper.id, miner.MapperID())
	}
}

func TestNicehashSplitterFile_OnLogin_Bad(t *testing.T) {
	var splitter *NonceSplitter
	splitter.OnLogin(nil)
	splitter.OnLogin(&proxy.LoginEvent{})
}

func TestNicehashSplitterFile_OnLogin_Ugly(t *testing.T) {
	splitter := NewNonceSplitter(&proxy.Config{}, proxy.NewEventBus(), func(listener pool.StratumListener) pool.Strategy {
		return mapperStrategyStub{}
	})
	splitter.OnLogin(&proxy.LoginEvent{Miner: &proxy.Miner{}})
	if len(splitter.mappers) == 0 {
		t.Fatal("expected splitter to allocate a mapper for the login")
	}
}
