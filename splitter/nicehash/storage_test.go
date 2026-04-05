package nicehash

import (
	"testing"

	"dappco.re/go/proxy"
)

// TestStorage_Add_Good verifies 256 sequential Add calls fill all slots with unique FixedByte values.
//
//	storage := nicehash.NewNonceStorage()
//	for i := 0; i < 256; i++ {
//	    m := &proxy.Miner{}
//	    m.SetID(int64(i + 1))
//	    ok := storage.Add(m) // true for all 256
//	}
func TestStorage_Add_Good(t *testing.T) {
	storage := NewNonceStorage()
	seen := make(map[uint8]bool)
	for i := 0; i < 256; i++ {
		m := &proxy.Miner{}
		m.SetID(int64(i + 1))
		ok := storage.Add(m)
		if !ok {
			t.Fatalf("expected add %d to succeed", i)
		}
		if seen[m.FixedByte()] {
			t.Fatalf("duplicate fixed byte %d at add %d", m.FixedByte(), i)
		}
		seen[m.FixedByte()] = true
	}
}

// TestStorage_Add_Bad verifies the 257th Add returns false when all 256 slots are occupied.
//
//	storage := nicehash.NewNonceStorage()
//	// fill 256 slots...
//	ok := storage.Add(overflowMiner) // false — table is full
func TestStorage_Add_Bad(t *testing.T) {
	storage := NewNonceStorage()
	for i := 0; i < 256; i++ {
		m := &proxy.Miner{}
		m.SetID(int64(i + 1))
		storage.Add(m)
	}

	overflow := &proxy.Miner{}
	overflow.SetID(257)
	if storage.Add(overflow) {
		t.Fatalf("expected 257th add to fail when table is full")
	}
}

// TestStorage_Add_Ugly verifies that a removed slot (dead) is reclaimed after SetJob clears it.
//
//	storage := nicehash.NewNonceStorage()
//	storage.Add(miner)
//	storage.Remove(miner) // slot becomes dead (-minerID)
//	storage.SetJob(job)   // dead slots cleared to 0
//	storage.Add(newMiner) // reclaimed slot succeeds
func TestStorage_Add_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	miner := &proxy.Miner{}
	miner.SetID(1)

	if !storage.Add(miner) {
		t.Fatalf("expected first add to succeed")
	}

	storage.Remove(miner)
	free, dead, active := storage.SlotCount()
	if dead != 1 || active != 0 {
		t.Fatalf("expected 1 dead slot, got free=%d dead=%d active=%d", free, dead, active)
	}

	// SetJob clears dead slots
	storage.SetJob(proxy.Job{Blob: "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", JobID: "job-1"})
	free, dead, active = storage.SlotCount()
	if dead != 0 {
		t.Fatalf("expected dead slots cleared after SetJob, got %d", dead)
	}

	// Reclaim the slot
	newMiner := &proxy.Miner{}
	newMiner.SetID(2)
	if !storage.Add(newMiner) {
		t.Fatalf("expected reclaimed slot add to succeed")
	}
}

// TestStorage_IsValidJobID_Good verifies the current job ID is accepted.
//
//	storage := nicehash.NewNonceStorage()
//	storage.SetJob(proxy.Job{JobID: "job-2", Blob: "..."})
//	storage.IsValidJobID("job-2") // true
func TestStorage_IsValidJobID_Good(t *testing.T) {
	storage := NewNonceStorage()
	storage.SetJob(proxy.Job{
		JobID: "job-1",
		Blob:  "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
	})

	if !storage.IsValidJobID("job-1") {
		t.Fatalf("expected current job to be valid")
	}
}

// TestStorage_IsValidJobID_Bad verifies an unknown job ID is rejected.
//
//	storage := nicehash.NewNonceStorage()
//	storage.IsValidJobID("nonexistent") // false
func TestStorage_IsValidJobID_Bad(t *testing.T) {
	storage := NewNonceStorage()
	storage.SetJob(proxy.Job{
		JobID: "job-1",
		Blob:  "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
	})

	if storage.IsValidJobID("nonexistent") {
		t.Fatalf("expected unknown job id to be invalid")
	}
	if storage.IsValidJobID("") {
		t.Fatalf("expected empty job id to be invalid")
	}
}

// TestStorage_IsValidJobID_Ugly verifies the previous job ID is accepted but counts as expired.
//
//	storage := nicehash.NewNonceStorage()
//	// job-1 is current, job-2 pushes job-1 to previous
//	storage.IsValidJobID("job-1") // true (but expired counter increments)
func TestStorage_IsValidJobID_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	blob160 := "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

	storage.SetJob(proxy.Job{JobID: "job-1", Blob: blob160, ClientID: "session-1"})
	storage.SetJob(proxy.Job{JobID: "job-2", Blob: blob160, ClientID: "session-1"})

	if !storage.IsValidJobID("job-2") {
		t.Fatalf("expected current job to be valid")
	}
	if !storage.IsValidJobID("job-1") {
		t.Fatalf("expected previous job to remain valid")
	}
	if storage.expired != 1 {
		t.Fatalf("expected one expired job validation, got %d", storage.expired)
	}
}

// TestStorage_SlotCount_Good verifies free/dead/active counts on a fresh storage.
//
//	storage := nicehash.NewNonceStorage()
//	free, dead, active := storage.SlotCount() // 256, 0, 0
func TestStorage_SlotCount_Good(t *testing.T) {
	storage := NewNonceStorage()
	free, dead, active := storage.SlotCount()
	if free != 256 || dead != 0 || active != 0 {
		t.Fatalf("expected 256/0/0, got free=%d dead=%d active=%d", free, dead, active)
	}
}
