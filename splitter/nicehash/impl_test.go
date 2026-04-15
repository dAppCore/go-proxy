package nicehash

import (
	"strings"
	"testing"

	"dappco.re/go/proxy"
)

func TestNonceStorage_NewNonceStorage_Good(t *testing.T) {
	storage := NewNonceStorage()
	if storage == nil {
		t.Fatal("expected storage")
	}
	free, dead, active := storage.SlotCount()
	if free != 256 || dead != 0 || active != 0 {
		t.Fatalf("expected empty storage, got free=%d dead=%d active=%d", free, dead, active)
	}
}

func TestNonceStorage_IsValidJobID_Bad(t *testing.T) {
	storage := NewNonceStorage()
	if storage.IsValidJobID("") {
		t.Fatal("expected empty job id to be invalid")
	}
	if storage.IsValidJobID("missing") {
		t.Fatal("expected unknown job id to be invalid")
	}
}

func TestNonceStorage_IsValidJobID_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	blob := strings.Repeat("0", 160)
	storage.SetJob(proxy.Job{JobID: "job-1", Blob: blob, ClientID: "session-1"})
	storage.SetJob(proxy.Job{JobID: "job-2", Blob: blob, ClientID: "session-1"})
	if !storage.IsValidJobID("job-1") {
		t.Fatal("expected previous job id to be accepted")
	}
	if storage.expired != 1 {
		t.Fatalf("expected expired counter to increment, got %d", storage.expired)
	}
}

func TestNonceStorage_SlotCount_Good(t *testing.T) {
	storage := NewNonceStorage()
	miner := &proxy.Miner{}
	miner.SetID(1)
	if !storage.Add(miner) {
		t.Fatal("expected miner to occupy a slot")
	}
	free, dead, active := storage.SlotCount()
	if free != 255 || dead != 0 || active != 1 {
		t.Fatalf("expected one active slot, got free=%d dead=%d active=%d", free, dead, active)
	}
}

func TestNonceStorage_SlotCount_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	miner := &proxy.Miner{}
	miner.SetID(1)
	if !storage.Add(miner) {
		t.Fatal("expected miner to occupy a slot")
	}
	storage.Remove(miner)
	free, dead, active := storage.SlotCount()
	if free != 255 || dead != 1 || active != 0 {
		t.Fatalf("expected one dead slot after removal, got free=%d dead=%d active=%d", free, dead, active)
	}
}
