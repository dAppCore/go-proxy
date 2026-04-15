package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"testing"
	"time"
)

func TestCoreImpl_HelperFunctions_Good(t *testing.T) {
	if result := newSuccessResult(); !result.OK || result.Error != nil {
		t.Fatalf("expected success result, got %+v", result)
	}
	if !isSupportedMode("nicehash") || !isSupportedMode("simple") {
		t.Fatal("expected supported proxy modes to be accepted")
	}
	if got := hostOnly("10.0.0.1:3333"); got != "10.0.0.1" {
		t.Fatalf("expected hostOnly to strip port, got %q", got)
	}
	if got := sha256Hex([]byte("abc")); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("unexpected sha256 hex: %q", got)
	}
	if got := targetFromDifficulty(10000); got != "b98d0600" {
		t.Fatalf("unexpected target for diff 10000: %q", got)
	}
}

func TestCoreImpl_HelperFunctions_Bad(t *testing.T) {
	if isSupportedMode("bogus") {
		t.Fatal("expected unsupported mode to be rejected")
	}
	if got := hostOnly("10.0.0.1"); got != "10.0.0.1" {
		t.Fatalf("expected bare host to remain unchanged, got %q", got)
	}
	sum := sha256.Sum256(nil)
	if got := sha256Hex(nil); got != hex.EncodeToString(sum[:]) {
		t.Fatalf("unexpected sha256 for empty input: %q", got)
	}
	var bucket tokenBucket
	refillBucket(&bucket, 30, time.Unix(0, 0))
	if bucket.tokens != 30 {
		t.Fatalf("expected empty bucket to refill to limit, got %d", bucket.tokens)
	}
}

func TestCoreImpl_HelperFunctions_Ugly(t *testing.T) {
	uuid, err := generateUUID()
	if err != nil {
		t.Fatalf("expected secure UUID generation to succeed, got %v", err)
	}
	if len(uuid) != 36 {
		t.Fatalf("expected UUID-like length 36, got %d", len(uuid))
	}
	if uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
		t.Fatalf("expected hyphenated UUID format, got %q", uuid)
	}
	if uuid[14] != '4' {
		t.Fatalf("expected version 4 UUID, got %q", uuid)
	}

	bucket := tokenBucket{tokens: 100, lastRefill: time.Unix(0, 0)}
	refillBucket(&bucket, 30, time.Unix(120, 0))
	if bucket.tokens != 30 {
		t.Fatalf("expected refill to cap at limit, got %d", bucket.tokens)
	}
}

func TestCoreImpl_targetFromDifficulty_Good(t *testing.T) {
	if got := targetFromDifficulty(10000); got != "b98d0600" {
		t.Fatalf("expected known target encoding, got %q", got)
	}
}

func TestCoreImpl_targetFromDifficulty_Bad(t *testing.T) {
	if got := targetFromDifficulty(0); got != "ffffffff" {
		t.Fatalf("expected zero difficulty to map to max target, got %q", got)
	}
	if got := targetFromDifficulty(1); got != "ffffffff" {
		t.Fatalf("expected difficulty 1 to map to max target, got %q", got)
	}
}

func TestCoreImpl_targetFromDifficulty_Ugly(t *testing.T) {
	if got := targetFromDifficulty(math.MaxUint64); got != "01000000" {
		t.Fatalf("expected huge difficulty to clamp to the minimum target, got %q", got)
	}
}

func TestCoreImpl_EffectiveShareDifficulty_Good(t *testing.T) {
	job := Job{Target: "01000000"}

	if got := EffectiveShareDifficulty(job, nil); got != math.MaxUint32 {
		t.Fatalf("expected nil miner to return pool difficulty, got %d", got)
	}
	if got := EffectiveShareDifficulty(job, &Miner{customDiff: 0}); got != math.MaxUint32 {
		t.Fatalf("expected zero custom diff to return pool difficulty, got %d", got)
	}
}

func TestCoreImpl_EffectiveShareDifficulty_Bad(t *testing.T) {
	job := Job{Target: "01000000"}
	miner := &Miner{customDiff: math.MaxUint32}

	if got := EffectiveShareDifficulty(job, miner); got != math.MaxUint32 {
		t.Fatalf("expected pool difficulty to win when it is lower, got %d", got)
	}
}

func TestCoreImpl_EffectiveShareDifficulty_Ugly(t *testing.T) {
	job := Job{Target: "01000000"}
	miner := &Miner{customDiff: 25000}

	if got := EffectiveShareDifficulty(job, miner); got != 25000 {
		t.Fatalf("expected custom diff cap to apply, got %d", got)
	}
}

func TestCoreImpl_isLoopbackHTTPHost_Good(t *testing.T) {
	cases := []string{"localhost", "127.0.0.1", "::1"}
	for _, host := range cases {
		if !isLoopbackHTTPHost(host) {
			t.Fatalf("expected %q to be treated as loopback", host)
		}
	}
}

func TestCoreImpl_isLoopbackHTTPHost_Bad(t *testing.T) {
	cases := []string{"", "0.0.0.0", "example.com"}
	for _, host := range cases {
		if isLoopbackHTTPHost(host) {
			t.Fatalf("expected %q to be treated as non-loopback", host)
		}
	}
}

func TestCoreImpl_isLoopbackHTTPHost_Ugly(t *testing.T) {
	if !isLoopbackHTTPHost("  LOCALHOST  ") {
		t.Fatal("expected trimmed case-insensitive localhost to be treated as loopback")
	}
}

func TestCoreImpl_refillBucket_Good(t *testing.T) {
	refillBucket(nil, 30, time.Now())

	bucket := tokenBucket{tokens: 0}
	refillBucket(&bucket, 0, time.Now())
	if bucket.tokens != 0 {
		t.Fatalf("expected zero limit to leave bucket unchanged, got %d", bucket.tokens)
	}
}

func TestCoreImpl_refillBucket_Bad(t *testing.T) {
	bucket := tokenBucket{tokens: 0}
	refillBucket(&bucket, 30, time.Unix(0, 0))
	if bucket.tokens != 30 {
		t.Fatalf("expected empty bucket to refill to limit, got %d", bucket.tokens)
	}
}

func TestCoreImpl_refillBucket_Ugly(t *testing.T) {
	bucket := tokenBucket{tokens: 100, lastRefill: time.Unix(0, 0)}
	refillBucket(&bucket, 30, time.Unix(120, 0))
	if bucket.tokens != 30 {
		t.Fatalf("expected refill to cap at limit, got %d", bucket.tokens)
	}
}
