package proxy

import "testing"

func TestCoreImpl_CustomDiff_OnLogin_Good(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET"}

	cd.OnLogin(Event{Miner: miner})

	if miner.User() != "WALLET" {
		t.Fatalf("expected wallet to remain unchanged, got %q", miner.User())
	}
	if miner.customDiff != 10000 {
		t.Fatalf("expected global custom diff fallback, got %d", miner.customDiff)
	}
	if !miner.customDiffResolved {
		t.Fatalf("expected login custom diff to be marked resolved")
	}
}

func TestCoreImpl_CustomDiff_OnLogin_Bad(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET"}

	cd.OnLogin(Event{})
	cd.OnLogin(Event{Miner: nil})
	cd.OnLogin(Event{Miner: miner})

	if miner.customDiff != 10000 {
		t.Fatalf("expected valid miner to still resolve after nil events, got %d", miner.customDiff)
	}
}

func TestCoreImpl_CustomDiff_OnLogin_Ugly(t *testing.T) {
	cd := NewCustomDiff(10000)
	miner := &Miner{user: "WALLET+50000"}

	cd.OnLogin(Event{Miner: miner})
	cd.globalDiff.Store(20000)
	cd.OnLogin(Event{Miner: miner})

	if miner.User() != "WALLET" {
		t.Fatalf("expected custom diff suffix to be stripped once, got %q", miner.User())
	}
	if miner.customDiff != 50000 {
		t.Fatalf("expected explicit custom diff to win over later global diff, got %d", miner.customDiff)
	}
	if !miner.customDiffFromLogin {
		t.Fatalf("expected login suffix to be recorded as from-login")
	}
}

// TestCustomDiff_Apply_Good verifies a user suffix "+50000" sets customDiff and strips the suffix.
//
//	cd := proxy.NewCustomDiff(10000)
//	cd.Apply(&proxy.Miner{user: "WALLET+50000"})
//	// miner.User() == "WALLET", miner.customDiff == 50000
func TestStateImpl_CustomDiff_Apply_Good(t *testing.T) {
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
func TestStateImpl_CustomDiff_Apply_Bad(t *testing.T) {
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
func TestStateImpl_CustomDiff_Apply_Ugly(t *testing.T) {
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

func TestCustomDiff_ApplyMethod_Good(t *testing.T) {
	cd := NewCustomDiff(5000)
	miner := &Miner{user: "WALLET+7500"}

	cd.Apply(miner)

	if miner.User() != "WALLET" {
		t.Fatalf("expected Apply to strip suffix, got %q", miner.User())
	}
	if miner.customDiff != 7500 {
		t.Fatalf("expected Apply to set custom diff, got %d", miner.customDiff)
	}
}

func TestCustomDiff_ApplyMethod_Bad(t *testing.T) {
	cd := NewCustomDiff(5000)
	var miner *Miner

	cd.Apply(miner)
}

func TestCustomDiff_ApplyMethod_Ugly(t *testing.T) {
	cd := NewCustomDiff(5000)
	miner := &Miner{user: "WALLET"}

	cd.Apply(miner)
	cd.Apply(miner)

	if miner.customDiff != 5000 {
		t.Fatalf("expected global diff to remain stable across repeated Apply calls, got %d", miner.customDiff)
	}
}
