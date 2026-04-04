package proxy

import "testing"

func TestRateLimiter_Allow(t *testing.T) {
	rl := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1, BanDurationSeconds: 1})
	if !rl.Allow("1.2.3.4:1234") {
		t.Fatalf("expected first call to pass")
	}
	if rl.Allow("1.2.3.4:1234") {
		t.Fatalf("expected second call to fail")
	}
}
