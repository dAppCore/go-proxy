package proxy

import (
	"testing"
	"time"
)

func TestRateLimiter_Allow(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 1})
	if !rl.Allow("1.2.3.4:1234") {
		t.Fatalf("expected first call to pass")
	}
	if rl.Allow("1.2.3.4:1234") {
		t.Fatalf("expected second call to fail")
	}
}

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
