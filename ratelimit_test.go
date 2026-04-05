package proxy

import (
	"testing"
	"time"
)

// TestRateLimiter_Allow_Good verifies the first N calls within budget are allowed.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 10})
//	limiter.Allow("1.2.3.4:3333") // true (first 10 calls)
func TestRateLimiter_Allow_Good(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 10, BanDurationSeconds: 60})

	for i := 0; i < 10; i++ {
		if !rl.Allow("1.2.3.4:3333") {
			t.Fatalf("expected call %d to be allowed", i+1)
		}
	}
}

// TestRateLimiter_Allow_Bad verifies the 11th call fails when budget is 10/min.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 10})
//	// calls 1-10 pass, call 11 fails
func TestRateLimiter_Allow_Bad(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 10, BanDurationSeconds: 60})

	for i := 0; i < 10; i++ {
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
func TestRateLimiter_Allow_Ugly(t *testing.T) {
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
}

// TestRateLimiter_Tick_Good verifies Tick removes expired bans.
//
//	limiter := proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 1})
//	limiter.Tick()
func TestRateLimiter_Tick_Good(t *testing.T) {
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
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 0})

	for i := 0; i < 100; i++ {
		if !rl.Allow("1.2.3.4:3333") {
			t.Fatalf("expected disabled limiter to allow all connections")
		}
	}
}
