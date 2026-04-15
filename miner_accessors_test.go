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
}

func TestMiner_Accessors_Bad(t *testing.T) {
	miner := &Miner{}

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
}

func TestMiner_Accessors_Ugly(t *testing.T) {
	miner := &Miner{}
	miner.SetExtendedNiceHash(true)
	miner.SetExtendedNiceHash(false)
	miner.SetFixedByte(0xff)
	miner.SetCurrentJob(Job{})

	if miner.ExtendedNiceHash() {
		t.Fatal("expected NiceHash flag to be overwritten to false")
	}
	if got := miner.FixedByte(); got != 0xff {
		t.Fatalf("expected latest fixed byte to win, got %d", got)
	}
	if miner.CurrentJob().IsValid() {
		t.Fatal("expected empty job to remain invalid")
	}
}
