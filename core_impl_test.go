package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"testing"
	"time"
)

type shareSinkSpy struct {
	accepts     int
	rejects     int
	panicAccept bool
	panicReject bool
	lastAccept  Event
	lastReject  Event
}

func (s *shareSinkSpy) OnAccept(e Event) {
	if s.panicAccept {
		panic("accept boom")
	}
	s.accepts++
	s.lastAccept = e
}

func (s *shareSinkSpy) OnReject(e Event) {
	if s.panicReject {
		panic("reject boom")
	}
	s.rejects++
	s.lastReject = e
}

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

func TestCoreImpl_shareSinkGroup_Good(t *testing.T) {
	first := &shareSinkSpy{}
	second := &shareSinkSpy{}

	group := newShareSinkGroup(nil, first, second)
	if group == nil {
		t.Fatal("expected share sink group")
	}
	if got := len(group.sinks); got != 2 {
		t.Fatalf("expected nil sinks to be filtered, got %d sinks", got)
	}

	accept := Event{Type: EventAccept, Diff: 1234}
	reject := Event{Type: EventReject, Error: "invalid"}
	group.OnAccept(accept)
	group.OnReject(reject)

	if first.accepts != 1 || second.accepts != 1 {
		t.Fatalf("expected all sinks to receive accept event, got first=%d second=%d", first.accepts, second.accepts)
	}
	if first.rejects != 1 || second.rejects != 1 {
		t.Fatalf("expected all sinks to receive reject event, got first=%d second=%d", first.rejects, second.rejects)
	}
	if first.lastAccept != accept || second.lastReject != reject {
		t.Fatalf("expected event payloads to be forwarded, got first=%+v second=%+v", first.lastAccept, second.lastReject)
	}
}

func TestCoreImpl_shareSinkGroup_Bad(t *testing.T) {
	var group *shareSinkGroup
	group.OnAccept(Event{Type: EventAccept})
	group.OnReject(Event{Type: EventReject})
}

func TestCoreImpl_shareSinkGroup_Ugly(t *testing.T) {
	first := &shareSinkSpy{panicAccept: true, panicReject: true}
	second := &shareSinkSpy{}
	group := newShareSinkGroup(first, second)

	group.OnAccept(Event{Type: EventAccept, Diff: 42})
	group.OnReject(Event{Type: EventReject, Error: "boom"})

	if second.accepts != 1 {
		t.Fatalf("expected later accept handlers to run after a panic, got %d", second.accepts)
	}
	if second.rejects != 1 {
		t.Fatalf("expected later reject handlers to run after a panic, got %d", second.rejects)
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

func TestCoreImpl_isHexString_Good(t *testing.T) {
	if !isHexString("0123456789abcdef") {
		t.Fatal("expected lowercase hex to be accepted")
	}
	if !isHexString("ABCDEF") {
		t.Fatal("expected uppercase hex to be accepted")
	}
	if !isHexStringLen("deadBEEF", 8) {
		t.Fatal("expected exact-length hex string to be accepted")
	}
}

func TestCoreImpl_isHexString_Bad(t *testing.T) {
	if isHexString("") {
		t.Fatal("expected empty string to be rejected")
	}
	if isHexString("g123") {
		t.Fatal("expected non-hex characters to be rejected")
	}
	if isHexStringLen("abc", 8) {
		t.Fatal("expected length mismatch to be rejected")
	}
}

func TestCoreImpl_isHexString_Ugly(t *testing.T) {
	if isHexString("1234zzzz") {
		t.Fatal("expected invalid characters at the end to be rejected")
	}
	if isHexStringLen("1234zzzz", 8) {
		t.Fatal("expected invalid characters to cause isHexStringLen to fail")
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
