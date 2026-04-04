package proxy

import "testing"

func TestCustomDiff_OnLogin(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET+50000"}
	cd.OnLogin(Event{Miner: miner})
	if miner.User() != "WALLET" {
		t.Fatalf("expected stripped user, got %q", miner.User())
	}
	if miner.customDiff != 50000 {
		t.Fatalf("expected custom diff 50000, got %d", miner.customDiff)
	}

	miner = &Miner{user: "WALLET+abc"}
	cd.OnLogin(Event{Miner: miner})
	if miner.User() != "WALLET+abc" {
		t.Fatalf("expected invalid suffix to remain unchanged")
	}
	if miner.customDiff != 0 {
		t.Fatalf("expected invalid suffix to disable custom diff, got %d", miner.customDiff)
	}

	miner = &Miner{user: "WALLET"}
	cd.OnLogin(Event{Miner: miner})
	if miner.customDiff != 10000 {
		t.Fatalf("expected global diff fallback, got %d", miner.customDiff)
	}
}
