package nicehash

import (
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

type recordingConn struct {
	mu     sync.Mutex
	writes [][]byte
	closed bool
}

func (c *recordingConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *recordingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}
func (c *recordingConn) Close() error                     { c.mu.Lock(); c.closed = true; c.mu.Unlock(); return nil }
func (c *recordingConn) LocalAddr() net.Addr              { return nil }
func (c *recordingConn) RemoteAddr() net.Addr             { return nil }
func (c *recordingConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error { return nil }

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

// TestStorage_SetJob_Good verifies that a valid job becomes current, is forwarded to
// active miners, and the previous job is retained when the upstream session remains stable.
//
//	storage := nicehash.NewNonceStorage()
//	storage.SetJob(proxy.Job{JobID: "job-1", ClientID: "session-1"})
//	storage.SetJob(proxy.Job{JobID: "job-2", ClientID: "session-1"}) // job-1 becomes prevJob
func TestStorage_SetJob_Good(t *testing.T) {
	storage := NewNonceStorage()
	conn := &recordingConn{}
	miner := proxy.NewMiner(conn, 3333, nil)
	miner.SetID(1)

	if !storage.Add(miner) {
		t.Fatal("expected miner to be assigned a slot")
	}

	job1 := proxy.Job{
		Blob:     strings.Repeat("0", 160),
		JobID:    "job-1",
		Target:   "b88d0600",
		ClientID: "session-1",
	}
	storage.SetJob(job1)

	if got := miner.CurrentJob(); got.JobID != job1.JobID {
		t.Fatalf("expected miner to receive first job, got %+v", got)
	}
	if len(conn.writes) != 1 {
		t.Fatalf("expected one forwarded job write, got %d", len(conn.writes))
	}

	var payload struct {
		Method string `json:"method"`
		Params struct {
			JobID string `json:"job_id"`
		} `json:"params"`
	}
	if err := json.Unmarshal(conn.writes[0], &payload); err != nil {
		t.Fatalf("decode forwarded job: %v", err)
	}
	if payload.Method != "job" {
		t.Fatalf("expected job notification, got %q", payload.Method)
	}
	if payload.Params.JobID != job1.JobID {
		t.Fatalf("expected forwarded job id %q, got %q", job1.JobID, payload.Params.JobID)
	}

	job2 := proxy.Job{
		Blob:     strings.Repeat("0", 160),
		JobID:    "job-2",
		Target:   "b88d0600",
		ClientID: "session-1",
	}
	storage.SetJob(job2)

	if got := storage.prevJob.JobID; got != job1.JobID {
		t.Fatalf("expected previous job to be archived for the same client, got %q", got)
	}
	if got := storage.job.JobID; got != job2.JobID {
		t.Fatalf("expected current job to be replaced, got %q", got)
	}
	if len(conn.writes) != 2 {
		t.Fatalf("expected second job to be forwarded, got %d writes", len(conn.writes))
	}
}

// TestStorage_SetJob_Bad verifies that invalid or nil receivers are ignored.
//
//	var storage *nicehash.NonceStorage
//	storage.SetJob(proxy.Job{JobID: "job-1"}) // no panic, no-op
func TestStorage_SetJob_Bad(t *testing.T) {
	var storage *NonceStorage
	storage.SetJob(proxy.Job{JobID: "job-1"})

	storage = NewNonceStorage()
	storage.SetJob(proxy.Job{})
	if storage.job.JobID != "" || storage.prevJob.JobID != "" {
		t.Fatalf("expected invalid job to leave storage unchanged, got current=%+v previous=%+v", storage.job, storage.prevJob)
	}
}

// TestStorage_SetJob_Ugly verifies that a pool session change resets the previous-job
// window instead of carrying stale job IDs across reconnects.
//
//	storage := nicehash.NewNonceStorage()
//	storage.SetJob(proxy.Job{JobID: "job-1", ClientID: "session-1"})
//	storage.SetJob(proxy.Job{JobID: "job-2", ClientID: "session-2"}) // prevJob resets
func TestStorage_SetJob_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	job1 := proxy.Job{
		Blob:     strings.Repeat("0", 160),
		JobID:    "job-1",
		Target:   "b88d0600",
		ClientID: "session-1",
	}
	job2 := proxy.Job{
		Blob:     strings.Repeat("0", 160),
		JobID:    "job-2",
		Target:   "b88d0600",
		ClientID: "session-2",
	}

	storage.SetJob(job1)
	storage.SetJob(job2)

	if got := storage.prevJob.JobID; got != "" {
		t.Fatalf("expected previous job to reset on upstream session change, got %q", got)
	}
	if got := storage.job.JobID; got != job2.JobID {
		t.Fatalf("expected current job to track the new pool session, got %q", got)
	}
}

func TestStorage_SetJob_EmptyClientID_Bad(t *testing.T) {
	storage := NewNonceStorage()
	blob160 := strings.Repeat("0", 160)

	storage.SetJob(proxy.Job{JobID: "job-1", Blob: blob160, Target: "b88d0600"})
	storage.SetJob(proxy.Job{JobID: "job-2", Blob: blob160, Target: "b88d0600"})

	if got := storage.prevJob.JobID; got != "" {
		t.Fatalf("expected previous job to reset when client ids are absent, got %q", got)
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
