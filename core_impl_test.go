package proxy

import (
	"crypto/sha256"
	"encoding/hex"
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
	uuid := generateUUID()
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
