package nicehash

import (
	"testing"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

type mapperStrategyStub struct{}

func (mapperStrategyStub) Connect()                                    {}
func (mapperStrategyStub) Submit(string, string, string, string) int64 { return 0 }
func (mapperStrategyStub) Disconnect()                                 {}
func (mapperStrategyStub) IsActive() bool                              { return true }

func TestNicehashMapperFile_NewNonceMapper_Good(t *testing.T) {
	mapper := NewNonceMapper(7, &proxy.Config{}, mapperStrategyStub{})
	if mapper == nil {
		t.Fatal("expected mapper")
	}
	if mapper.id != 7 {
		t.Fatalf("expected mapper id 7, got %d", mapper.id)
	}
	if mapper.storage == nil {
		t.Fatal("expected storage to be initialised")
	}
	if mapper.pending == nil {
		t.Fatal("expected pending map to be initialised")
	}
}

func TestNicehashMapperFile_NewNonceMapper_Bad(t *testing.T) {
	mapper := NewNonceMapper(0, nil, nil)
	if mapper == nil {
		t.Fatal("expected mapper")
	}
	if mapper.storage == nil {
		t.Fatal("expected storage to be initialised even without config")
	}
}

func TestNicehashMapperFile_NewNonceMapper_Ugly(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, pool.Strategy(nil))
	if mapper == nil {
		t.Fatal("expected mapper")
	}
	if mapper.IsActive() {
		t.Fatal("expected new mapper to start inactive")
	}
}

func TestNicehashMapperFile_OnDisconnect_Good(t *testing.T) {
	mapper := NewNonceMapper(7, &proxy.Config{}, mapperStrategyStub{})
	mapper.OnDisconnect()
	if mapper.IsActive() {
		t.Fatal("expected disconnected mapper to be inactive")
	}
	if mapper.suspended != 1 {
		t.Fatalf("expected suspended count to increment, got %d", mapper.suspended)
	}
}

func TestNicehashMapperFile_OnDisconnect_Bad(t *testing.T) {
	var mapper *NonceMapper
	mapper.OnDisconnect()
	if mapper != nil {
		t.Fatal("expected nil mapper to remain nil")
	}
}

func TestNicehashMapperFile_OnDisconnect_Ugly(t *testing.T) {
	mapper := NewNonceMapper(7, &proxy.Config{}, mapperStrategyStub{})
	mapper.OnDisconnect()
	mapper.OnDisconnect()
	if mapper.suspended != 2 {
		t.Fatalf("expected repeated disconnects to keep incrementing suspended, got %d", mapper.suspended)
	}
}
