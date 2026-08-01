package proxy

import (
	"testing"
	"time"
)

// TestRateLimiter_Allow_Good verifies the first N calls within budget are allowed.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 10})
//	limiter.Allow("1.2.3.4:3333") // true (first 10 calls)
func TestRatelimit_RateLimiter_Allow_Good(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 10, BanDurationSeconds: 60})

	for i := range 10 {
		if !rl.Allow("1.2.3.4:3333") {
			t.Fatalf("expected call %d to be allowed", i+1)
		}
	}
}

// TestRateLimiter_Allow_Bad verifies the 11th call fails when budget is 10/min.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 10})
//	// calls 1-10 pass, call 11 fails
func TestRatelimit_RateLimiter_Allow_Bad(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 10, BanDurationSeconds: 60})

	for range 10 {
		rl.Allow("1.2.3.4:3333")
	}
	if rl.Allow("1.2.3.4:3333") {
		t.Fatalf("expected 11th call to be rejected")
	}
}

// TestRateLimiter_Allow_Ugly verifies a banned IP stays banned for BanDurationSeconds.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 300})
//	limiter.Allow("1.2.3.4:3333") // true (exhausts budget)
//	limiter.Allow("1.2.3.4:3333") // false (banned for 300 seconds)
func TestRatelimit_RateLimiter_Allow_Ugly(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 300})

	if !rl.Allow("1.2.3.4:3333") {
		t.Fatalf("expected first call to pass")
	}
	if rl.Allow("1.2.3.4:3333") {
		t.Fatalf("expected second call to fail")
	}

	// Verify the IP is still banned even with a fresh bucket
	rl.mu.Lock()
	rl.bucketByHost["1.2.3.4"] = &tokenBucket{tokens: 100, lastRefill: time.Now()}
	rl.mu.Unlock()
	if rl.Allow("1.2.3.4:3333") {
		t.Fatalf("expected banned IP to remain banned regardless of fresh bucket")
	}

	rlNoBan := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 0})
	if !rlNoBan.Allow("1.2.3.4") {
		t.Fatalf("expected bare host to be allowed on first call")
	}
	if rlNoBan.Allow("1.2.3.4") {
		t.Fatalf("expected bare host to be rejected after the bucket is exhausted")
	}
	rlNoBan.mu.Lock()
	if len(rlNoBan.banUntilByHost) != 0 {
		rlNoBan.mu.Unlock()
		t.Fatalf("expected disabled ban duration to avoid creating ban entries, got %#v", rlNoBan.banUntilByHost)
	}
	rlNoBan.mu.Unlock()
}

// TestRateLimiter_Allow_ExhaustedBucketStartsBan verifies the ban starts as soon as
// the final token is consumed, not only on the next rejected attempt.
func TestRateLimiter_Allow_ExhaustedBucketStartsBan(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 300})

	if !rl.Allow("1.2.3.4:3333") {
		t.Fatalf("expected first call to pass")
	}

	rl.mu.Lock()
	until, banned := rl.banUntilByHost["1.2.3.4"]
	rl.mu.Unlock()
	if !banned {
		t.Fatalf("expected ban to start when the bucket reaches zero")
	}
	if until.Before(time.Now()) {
		t.Fatalf("expected ban deadline to be in the future")
	}
}

// TestRateLimiter_Allow_RejectsWhileBanned verifies a host with a live ban is
// rejected outright, before any token-bucket accounting.
func TestRateLimiter_Allow_RejectsWhileBanned(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 10, BanDurationSeconds: 60})

	rl.mu.Lock()
	rl.banUntilByHost["9.9.9.9"] = time.Now().Add(time.Minute)
	rl.mu.Unlock()

	if rl.Allow("9.9.9.9:3333") {
		t.Fatal("expected a host with a live ban to be rejected")
	}
}

// TestRateLimiter_Allow_AfterBanExpiry verifies that once a ban deadline has
// passed, the stale ban entry is dropped and the host is allowed again.
func TestRateLimiter_Allow_AfterBanExpiry(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 10, BanDurationSeconds: 60})

	// Seed an already-expired ban for the host.
	rl.mu.Lock()
	rl.banUntilByHost["8.8.8.8"] = time.Now().Add(-time.Minute)
	rl.mu.Unlock()

	if !rl.Allow("8.8.8.8:3333") {
		t.Fatal("expected an expired ban to be cleared and the host allowed")
	}

	rl.mu.Lock()
	_, stillBanned := rl.banUntilByHost["8.8.8.8"]
	rl.mu.Unlock()
	if stillBanned {
		t.Fatal("expected the expired ban entry to be deleted")
	}
}

// TestRateLimiter_Tick_Good verifies Tick removes expired bans.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 1})
//	limiter.Tick()
func TestRatelimit_RateLimiter_Tick_Good(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 1})

	rl.Allow("1.2.3.4:3333")
	rl.Allow("1.2.3.4:3333") // triggers ban

	// Simulate expired ban
	rl.mu.Lock()
	rl.banUntilByHost["1.2.3.4"] = time.Now().Add(-time.Second)
	rl.mu.Unlock()

	rl.Tick()

	rl.mu.Lock()
	_, banned := rl.banUntilByHost["1.2.3.4"]
	rl.mu.Unlock()
	if banned {
		t.Fatalf("expected expired ban to be removed by Tick")
	}
}

// TestRateLimiter_Tick_Bad verifies that a nil limiter is ignored.
func TestRatelimit_RateLimiter_Tick_Bad(t *testing.T) {
	var rl *RateLimiter
	rl.Tick()
	if rl != nil {
		t.Fatal("expected nil limiter to remain nil")
	}
}

// TestRateLimiter_Tick_Ugly verifies that a disabled limiter can still be ticked safely.
func TestRatelimit_RateLimiter_Tick_Ugly(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 0})

	rl.Tick()

	rl.mu.Lock()
	defer rl.mu.Unlock()
	if len(rl.bucketByHost) != 0 || len(rl.banUntilByHost) != 0 {
		t.Fatalf("expected disabled limiter tick to remain a no-op, got buckets=%d bans=%d", len(rl.bucketByHost), len(rl.banUntilByHost))
	}
}

// TestRateLimiter_Allow_ReplenishesHighLimits verifies token replenishment at high rates.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 120})
func TestRateLimiter_Allow_ReplenishesHighLimits(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 120, BanDurationSeconds: 1})
	rl.mu.Lock()
	rl.bucketByHost["1.2.3.4"] = &tokenBucket{
		tokens:     0,
		lastRefill: time.Now().Add(-30 * time.Second),
	}
	rl.mu.Unlock()

	if !rl.Allow("1.2.3.4:1234") {
		t.Fatalf("expected bucket to replenish at 120/min")
	}
}

// TestRateLimiter_Disabled_Good verifies a zero-budget limiter allows all connections.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 0})
//	limiter.Allow("any-ip") // always true
func TestRateLimiter_Disabled_Good(t *testing.T) {
	// target symbol: Disabled
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 0})

	for range 100 {
		if !rl.Allow("1.2.3.4:3333") {
			t.Fatalf("expected disabled limiter to allow all connections")
		}
	}
}

func TestRatelimit_RateLimiter_UpdateConfig_Good(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 60})
	if !rl.Allow("1.2.3.4:3333") {
		t.Fatal("expected first call to pass")
	}
	if rl.Allow("1.2.3.4:3333") {
		t.Fatal("expected bucket to be exhausted before update")
	}

	rl.UpdateConfig(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 60})

	if !rl.Allow("1.2.3.4:3333") {
		t.Fatal("expected reset limiter to start from a fresh policy")
	}
	if rl.Allow("1.2.3.4:3333") {
		t.Fatal("expected new policy to exhaust again after one token")
	}
}

func TestRatelimit_RateLimiter_UpdateConfig_Bad(t *testing.T) {
	var rl *RateLimiter
	rl.UpdateConfig(RateLimit{MaxConnectionsPerMinute: 10, BanDurationSeconds: 1})
	if rl != nil {
		t.Fatal("expected nil limiter to remain nil")
	}
}

func TestRatelimit_RateLimiter_UpdateConfig_Ugly(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 60})
	rl.Allow("1.2.3.4:3333")

	rl.UpdateConfig(RateLimit{MaxConnectionsPerMinute: 0, BanDurationSeconds: 0})

	for i := range 10 {
		if !rl.Allow("1.2.3.4:3333") {
			t.Fatalf("expected disabled limiter after update to allow call %d", i+1)
		}
	}
}
