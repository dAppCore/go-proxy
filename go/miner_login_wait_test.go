package proxy

import (
	"net"
	"testing"
	"time"
)

// validTestJob returns a Job that satisfies Job.IsValid for login-flow tests.
func validTestJob() Job {
	return Job{JobID: "job-1", Blob: repeatString("0", 160), Target: "b88d0600"}
}

// TestMiner_waitForLoginJob_NonPositiveTimeout verifies a non-positive timeout
// returns immediately without spinning.
func TestMiner_waitForLoginJob_NonPositiveTimeout(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)

	start := time.Now()
	miner.waitForLoginJob(0)
	miner.waitForLoginJob(-time.Second)
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("expected immediate return for non-positive timeout, waited %s", elapsed)
	}
}

// TestMiner_waitForLoginJob_ReturnsOnValidJob verifies the wait loop returns as
// soon as a valid job is available rather than blocking for the full timeout.
func TestMiner_waitForLoginJob_ReturnsOnValidJob(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.SetCurrentJob(validTestJob())

	start := time.Now()
	miner.waitForLoginJob(2 * time.Second)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected early return once a valid job is present, waited %s", elapsed)
	}
}

// TestMiner_waitForLoginJob_ReturnsOnClosing verifies the wait loop returns as
// soon as the miner enters the closing state, even with no valid job.
func TestMiner_waitForLoginJob_ReturnsOnClosing(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)

	go func() {
		time.Sleep(20 * time.Millisecond)
		miner.Close()
	}()

	start := time.Now()
	miner.waitForLoginJob(2 * time.Second)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected early return once the miner is closing, waited %s", elapsed)
	}
}

// TestMiner_rejectLogin_WhileClosing verifies that a login rejection issued
// while the miner is already closing does not flip the state back to
// WaitLogin — a closing miner stays closing.
func TestMiner_rejectLogin_WhileClosing(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	miner := NewMiner(serverConn, 3333, nil)
	miner.mu.Lock()
	miner.state = MinerStateClosing
	miner.mu.Unlock()

	// Drain any reply the rejection writes before the transport closes.
	go func() {
		buf := make([]byte, 256)
		_, _ = clientConn.Read(buf)
	}()

	miner.rejectLogin(1, "Login rejected")

	if miner.State() != MinerStateClosing {
		t.Fatalf("expected a closing miner to stay closing, got %v", miner.State())
	}
}
