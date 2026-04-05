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

func TestCustomDiff_OnLogin_Ugly(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET+50000extra"}

	cd.OnLogin(Event{Miner: miner})

	if miner.User() != "WALLET+50000extra" {
		t.Fatalf("expected non-suffix plus segment to remain unchanged, got %q", miner.User())
	}
	if miner.customDiff != 0 {
		t.Fatalf("expected invalid suffix to disable custom diff, got %d", miner.customDiff)
	}
}

func TestEffectiveShareDifficulty_CustomDiffCapsPoolDifficulty(t *testing.T) {
	job := Job{Target: "01000000"}
	miner := &Miner{customDiff: 25000}

	if got := EffectiveShareDifficulty(job, miner); got != 25000 {
		t.Fatalf("expected capped difficulty 25000, got %d", got)
	}
}
