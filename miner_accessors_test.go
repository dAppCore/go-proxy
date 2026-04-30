package proxy

import "testing"

func TestMiner_Accessors_Good(t *testing.T) {
	miner := &Miner{}
	job := Job{Blob: "blob", JobID: "job-1"}

	miner.SetExtendedNiceHash(true)
	miner.SetFixedByte(0x2a)
	miner.SetCurrentJob(job)
	miner.password = "secret"
	miner.rigID = "rig-1"
	miner.diff = 1234
	miner.remoteAddr = "10.0.0.1:49152"
	miner.loginAlgos = []string{"cn/r", "rx/0"}

	if !miner.ExtendedNiceHash() {
		t.Fatal("expected extended nice hash to be enabled")
	}
	if got := miner.FixedByte(); got != 0x2a {
		t.Fatalf("expected fixed byte 0x2a, got %d", got)
	}
	if got := miner.CurrentJob(); got != job {
		t.Fatalf("expected current job to round-trip, got %+v", got)
	}
	if got := miner.Password(); got != "secret" {
		t.Fatalf("expected password accessor to return secret, got %q", got)
	}
	if got := miner.RigID(); got != "rig-1" {
		t.Fatalf("expected rigid accessor to return rig-1, got %q", got)
	}
	if got := miner.Diff(); got != 1234 {
		t.Fatalf("expected diff accessor to return 1234, got %d", got)
	}
	if got := miner.RemoteAddr(); got != "10.0.0.1:49152" {
		t.Fatalf("expected remote addr accessor to return full address, got %q", got)
	}
	if got := miner.LoginAlgos(); len(got) != 2 || got[0] != "cn/r" || got[1] != "rx/0" {
		t.Fatalf("expected login algos to round-trip, got %#v", got)
	}
}

func TestMiner_Accessors_Bad(t *testing.T) {
	miner := &Miner{}
	var nilMiner *Miner

	if miner.ExtendedNiceHash() {
		t.Fatal("expected NiceHash flag to default to false")
	}
	if got := miner.FixedByte(); got != 0 {
		t.Fatalf("expected default fixed byte 0, got %d", got)
	}
	if got := miner.Password(); got != "" {
		t.Fatalf("expected empty password by default, got %q", got)
	}
	if got := miner.RigID(); got != "" {
		t.Fatalf("expected empty rigid by default, got %q", got)
	}
	if got := miner.Diff(); got != 0 {
		t.Fatalf("expected default diff 0, got %d", got)
	}
	if got := miner.RemoteAddr(); got != "" {
		t.Fatalf("expected empty remote addr by default, got %q", got)
	}
	if got := miner.LoginAlgos(); got != nil {
		t.Fatalf("expected empty login algos to return nil, got %#v", got)
	}
	if got := nilMiner.RemoteAddr(); got != "" {
		t.Fatalf("expected nil miner remote addr to be empty, got %q", got)
	}
	if got := nilMiner.LoginAlgos(); got != nil {
		t.Fatalf("expected nil miner login algos to be nil, got %#v", got)
	}
}

func TestMiner_Accessors_Ugly(t *testing.T) {
	miner := &Miner{}
	miner.SetExtendedNiceHash(true)
	miner.SetExtendedNiceHash(false)
	miner.SetFixedByte(0xff)
	miner.SetCurrentJob(Job{})
	miner.loginAlgos = []string{"cn/r"}

	gotAlgos := miner.LoginAlgos()
	gotAlgos[0] = "mutated"

	if miner.ExtendedNiceHash() {
		t.Fatal("expected NiceHash flag to be overwritten to false")
	}
	if got := miner.FixedByte(); got != 0xff {
		t.Fatalf("expected latest fixed byte to win, got %d", got)
	}
	if miner.CurrentJob().IsValid() {
		t.Fatal("expected empty job to remain invalid")
	}
	if miner.LoginAlgos()[0] != "cn/r" {
		t.Fatalf("expected LoginAlgos to return a copy, got %#v", miner.LoginAlgos())
	}
}

func TestStateImpl_Miner_IP_Good(t *testing.T) {
	miner := &Miner{ip: "10.0.0.1"}
	if got := miner.IP(); got != "10.0.0.1" {
		t.Fatalf("expected IP accessor to return 10.0.0.1, got %q", got)
	}
}

func TestStateImpl_Miner_IP_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.IP(); got != "" {
		t.Fatalf("expected empty IP by default, got %q", got)
	}
}

func TestStateImpl_Miner_IP_Ugly(t *testing.T) {
	miner := &Miner{ip: "2001:db8::1"}
	if got := miner.IP(); got != "2001:db8::1" {
		t.Fatalf("expected IP accessor to preserve IPv6 text, got %q", got)
	}
}
