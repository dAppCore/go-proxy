package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
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

func TestCoreImpl_RegisterSplitterFactory_Good(t *testing.T) {
	mode := "ax-register-good"
	RegisterSplitterFactory(mode, func(*Config, *EventBus) Splitter { return &noopSplitter{} })
	factory, ok := splitterFactoryForMode(mode)
	if !ok || factory == nil {
		t.Fatalf("expected registered splitter factory, ok=%v factoryNil=%v", ok, factory == nil)
	}
}

func TestCoreImpl_RegisterSplitterFactory_Bad(t *testing.T) {
	mode := "ax-register-bad"
	RegisterSplitterFactory(mode, nil)
	factory, ok := splitterFactoryForMode(mode)
	if !ok || factory != nil {
		t.Fatalf("expected nil factory to be registered as nil, ok=%v factoryNil=%v", ok, factory == nil)
	}
}

func TestCoreImpl_RegisterSplitterFactory_Ugly(t *testing.T) {
	RegisterSplitterFactory(" ax-register-ugly ", func(*Config, *EventBus) Splitter { return &noopSplitter{} })
	RegisterSplitterFactory("ax-register-ugly", func(*Config, *EventBus) Splitter { return nil })
	factory, ok := splitterFactoryForMode("AX-REGISTER-UGLY")
	if !ok || factory == nil || factory(nil, nil) != nil {
		t.Fatalf("expected normalized overwrite to return nil splitter, ok=%v factoryNil=%v", ok, factory == nil)
	}
}

func TestCoreImpl_NewEventBus_Good(t *testing.T) {
	bus := NewEventBus()
	if bus == nil || bus.listeners == nil {
		t.Fatalf("expected initialized event bus, got %+v", bus)
	}
}

func TestCoreImpl_NewEventBus_Bad(t *testing.T) {
	bus := NewEventBus()
	bus.Dispatch(Event{Type: EventAccept})
	if len(bus.listeners) != 0 {
		t.Fatalf("expected dispatch without subscribers to keep listener map empty, got %+v", bus.listeners)
	}
}

func TestCoreImpl_NewEventBus_Ugly(t *testing.T) {
	bus := NewEventBus()
	bus.Subscribe(EventLogin, nil)
	if len(bus.listeners[EventLogin]) != 0 {
		t.Fatalf("expected nil handler to be ignored, got %+v", bus.listeners)
	}
}

func TestCoreImpl_SinkGroup_OnAccept_Good(t *testing.T) {
	sink := &shareSinkSpy{}
	group := newShareSinkGroup(sink)
	group.OnAccept(Event{Diff: 7})
	if sink.accepts != 1 || sink.lastAccept.Diff != 7 {
		t.Fatalf("expected accept dispatched once, sink=%+v", sink)
	}
}

func TestCoreImpl_SinkGroup_OnAccept_Bad(t *testing.T) {
	var group *shareSinkGroup
	group.OnAccept(Event{Diff: 7})
	if group != nil {
		t.Fatal("expected nil sink group to remain nil")
	}
}

func TestCoreImpl_SinkGroup_OnAccept_Ugly(t *testing.T) {
	sink := &shareSinkSpy{panicAccept: true}
	group := newShareSinkGroup(sink, &shareSinkSpy{})
	group.OnAccept(Event{Diff: 9})
	if len(group.sinks) != 2 {
		t.Fatalf("expected panic recovery to preserve sinks, got %+v", group.sinks)
	}
}

func TestCoreImpl_SinkGroup_OnReject_Good(t *testing.T) {
	sink := &shareSinkSpy{}
	group := newShareSinkGroup(sink)
	group.OnReject(Event{Error: "invalid"})
	if sink.rejects != 1 || sink.lastReject.Error != "invalid" {
		t.Fatalf("expected reject dispatched once, sink=%+v", sink)
	}
}

func TestCoreImpl_SinkGroup_OnReject_Bad(t *testing.T) {
	var group *shareSinkGroup
	group.OnReject(Event{Error: "invalid"})
	if group != nil {
		t.Fatal("expected nil sink group to remain nil")
	}
}

func TestCoreImpl_SinkGroup_OnReject_Ugly(t *testing.T) {
	sink := &shareSinkSpy{panicReject: true}
	group := newShareSinkGroup(sink, &shareSinkSpy{})
	group.OnReject(Event{Error: "invalid"})
	if len(group.sinks) != 2 {
		t.Fatalf("expected panic recovery to preserve sinks, got %+v", group.sinks)
	}
}

func TestCoreImpl_NewCustomDiff_Good(t *testing.T) {
	resolver := NewCustomDiff(50000)
	miner := &Miner{user: "wallet"}
	resolver.OnLogin(Event{Miner: miner})
	if miner.customDiff != 50000 || miner.user != "wallet" {
		t.Fatalf("expected global custom diff applied, miner=%+v", miner)
	}
}

func TestCoreImpl_NewCustomDiff_Bad(t *testing.T) {
	resolver := NewCustomDiff(0)
	miner := &Miner{user: "wallet"}
	resolver.OnLogin(Event{Miner: miner})
	if miner.customDiff != 0 || !miner.customDiffResolved {
		t.Fatalf("expected zero global diff to resolve without diff, miner=%+v", miner)
	}
}

func TestCoreImpl_NewCustomDiff_Ugly(t *testing.T) {
	resolver := NewCustomDiff(50000)
	miner := &Miner{user: "wallet+25000"}
	resolver.OnLogin(Event{Miner: miner})
	if miner.user != "wallet" || miner.customDiff != 25000 || !miner.customDiffFromLogin {
		t.Fatalf("expected login custom diff to override global, miner=%+v", miner)
	}
}

func TestCoreImpl_NewRateLimiter_Good(t *testing.T) {
	limiter := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 2})
	if limiter == nil || limiter.bucketByHost == nil || limiter.banUntilByHost == nil {
		t.Fatalf("expected initialized limiter, got %+v", limiter)
	}
}

func TestCoreImpl_NewRateLimiter_Bad(t *testing.T) {
	limiter := NewRateLimiter(RateLimit{})
	if !limiter.Allow("203.0.113.10:3333") {
		t.Fatal("expected zero-limit limiter to allow connections")
	}
}

func TestCoreImpl_NewRateLimiter_Ugly(t *testing.T) {
	limiter := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1})
	if !limiter.Allow("203.0.113.10:3333") || limiter.Allow("203.0.113.10:4444") {
		t.Fatalf("expected host-only bucket to exhaust after first allow, buckets=%+v", limiter.bucketByHost)
	}
}

func TestCoreImpl_NewConfigWatcher_Good(t *testing.T) {
	path := t.TempDir() + "/config.json"
	if err := os.WriteFile(path, []byte(`{"mode":"simple"}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	watcher := NewConfigWatcher(path, func(*Config) {})
	if watcher == nil || watcher.configPath != path || watcher.lastModifiedAt.IsZero() {
		t.Fatalf("expected watcher with stat metadata, got %+v", watcher)
	}
}

func TestCoreImpl_NewConfigWatcher_Bad(t *testing.T) {
	watcher := NewConfigWatcher("", nil)
	if watcher == nil || watcher.configPath != "" || watcher.onConfigChange != nil {
		t.Fatalf("expected watcher with empty path and nil callback, got %+v", watcher)
	}
}

func TestCoreImpl_NewConfigWatcher_Ugly(t *testing.T) {
	watcher := NewConfigWatcher(t.TempDir()+"/missing.json", func(*Config) {})
	if watcher == nil || !watcher.lastModifiedAt.IsZero() {
		t.Fatalf("expected missing config path to leave zero mtime, got %+v", watcher)
	}
}
