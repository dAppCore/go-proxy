package proxy

import "testing"

// TestCustomDiff_Apply_Good verifies a user suffix "+50000" sets customDiff and strips the suffix.
//
//	cd := proxy.NewCustomDiff(10000)
//	cd.Apply(&proxy.Miner{user: "WALLET+50000"})
//	// miner.User() == "WALLET", miner.customDiff == 50000
func TestCustomDiff_Apply_Good(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET+50000"}
	cd.OnLogin(Event{Miner: miner})
	if miner.User() != "WALLET" {
		t.Fatalf("expected stripped user, got %q", miner.User())
	}
	if miner.customDiff != 50000 {
		t.Fatalf("expected custom diff 50000, got %d", miner.customDiff)
	}
}

// TestCustomDiff_Apply_Bad verifies "+abc" (non-numeric) leaves user unchanged, customDiff=0.
//
//	cd := proxy.NewCustomDiff(10000)
//	cd.Apply(&proxy.Miner{user: "WALLET+abc"})
//	// miner.User() == "WALLET+abc", miner.customDiff == 0
func TestCustomDiff_Apply_Bad(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET+abc"}
	cd.OnLogin(Event{Miner: miner})
	if miner.User() != "WALLET+abc" {
		t.Fatalf("expected invalid suffix to remain unchanged, got %q", miner.User())
	}
	if miner.customDiff != 0 {
		t.Fatalf("expected invalid suffix to disable custom diff, got %d", miner.customDiff)
	}
}

// TestCustomDiff_Apply_Ugly verifies globalDiff=10000 is used when no suffix is present.
//
//	cd := proxy.NewCustomDiff(10000)
//	cd.Apply(&proxy.Miner{user: "WALLET"})
//	// miner.customDiff == 10000 (falls back to global)
func TestCustomDiff_Apply_Ugly(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET"}
	cd.OnLogin(Event{Miner: miner})
	if miner.customDiff != 10000 {
		t.Fatalf("expected global diff fallback 10000, got %d", miner.customDiff)
	}
}

// TestCustomDiff_OnLogin_NonNumericSuffix verifies a non-decimal suffix after plus is ignored.
//
//	cd := proxy.NewCustomDiff(10000)
//	cd.OnLogin(proxy.Event{Miner: &proxy.Miner{user: "WALLET+50000extra"}})
func TestCustomDiff_OnLogin_NonNumericSuffix(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET+50000extra"}

	cd.OnLogin(Event{Miner: miner})

	if miner.User() != "WALLET+50000extra" {
		t.Fatalf("expected non-numeric suffix plus segment to remain unchanged, got %q", miner.User())
	}
	if miner.customDiff != 0 {
		t.Fatalf("expected invalid suffix to disable custom diff, got %d", miner.customDiff)
	}
}

// TestEffectiveShareDifficulty_CustomDiffCapsPoolDifficulty verifies the cap applied by custom diff.
//
//	job := proxy.Job{Target: "01000000"}
//	miner := &proxy.Miner{customDiff: 25000}
//	proxy.EffectiveShareDifficulty(job, miner) // 25000 (capped)
func TestEffectiveShareDifficulty_CustomDiffCapsPoolDifficulty(t *testing.T) {
	job := Job{Target: "01000000"}
	miner := &Miner{customDiff: 25000}

	if got := EffectiveShareDifficulty(job, miner); got != 25000 {
		t.Fatalf("expected capped difficulty 25000, got %d", got)
	}
}
