package proxy

import "sync"

// CustomDiffBucketStats tracks per-custom-difficulty share outcomes.
type CustomDiffBucketStats struct {
	Accepted    uint64 `json:"accepted"`
	Rejected    uint64 `json:"rejected"`
	Invalid     uint64 `json:"invalid"`
	Expired     uint64 `json:"expired"`
	HashesTotal uint64 `json:"hashes_total"`
}

// CustomDiffBuckets groups share totals by the miner's resolved custom difficulty.
//
//	buckets := NewCustomDiffBuckets(true)
//	buckets.OnAccept(Event{Miner: &Miner{customDiff: 50000}, Diff: 25000})
type CustomDiffBuckets struct {
	enabled bool
	buckets map[uint64]*CustomDiffBucketStats
	mu      sync.Mutex
}

// NewCustomDiffBuckets creates a per-difficulty share tracker.
func NewCustomDiffBuckets(enabled bool) *CustomDiffBuckets {
	return &CustomDiffBuckets{
		enabled: enabled,
		buckets: make(map[uint64]*CustomDiffBucketStats),
	}
}

// SetEnabled toggles recording without discarding any collected buckets.
func (b *CustomDiffBuckets) SetEnabled(enabled bool) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.enabled = enabled
}

// OnAccept records an accepted share for the miner's custom difficulty bucket.
func (b *CustomDiffBuckets) OnAccept(e Event) {
	if b == nil || !b.enabled || e.Miner == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bucket := b.bucketLocked(e.Miner.customDiff)
	bucket.Accepted++
	if e.Expired {
		bucket.Expired++
	}
	if e.Diff > 0 {
		bucket.HashesTotal += e.Diff
	}
}

// OnReject records a rejected share for the miner's custom difficulty bucket.
func (b *CustomDiffBuckets) OnReject(e Event) {
	if b == nil || !b.enabled || e.Miner == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bucket := b.bucketLocked(e.Miner.customDiff)
	bucket.Rejected++
	if isInvalidShareReason(e.Error) {
		bucket.Invalid++
	}
}

// Snapshot returns a copy of the current bucket totals.
//
//	summary := buckets.Snapshot()
func (b *CustomDiffBuckets) Snapshot() map[uint64]CustomDiffBucketStats {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.enabled || len(b.buckets) == 0 {
		return nil
	}
	out := make(map[uint64]CustomDiffBucketStats, len(b.buckets))
	for diff, bucket := range b.buckets {
		if bucket == nil {
			continue
		}
		out[diff] = *bucket
	}
	return out
}

func (b *CustomDiffBuckets) bucketLocked(diff uint64) *CustomDiffBucketStats {
	if b.buckets == nil {
		b.buckets = make(map[uint64]*CustomDiffBucketStats)
	}
	bucket, ok := b.buckets[diff]
	if !ok {
		bucket = &CustomDiffBucketStats{}
		b.buckets[diff] = bucket
	}
	return bucket
}

func isInvalidShareReason(reason string) bool {
	reason = lowerString(reason)
	if reason == "" {
		return false
	}
	return containsString(reason, "low diff") ||
		containsString(reason, "lowdifficulty") ||
		containsString(reason, "low difficulty") ||
		containsString(reason, "malformed") ||
		containsString(reason, "difficulty") ||
		containsString(reason, "invalid") ||
		containsString(reason, "nonce")
}
