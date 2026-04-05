package proxy

import "testing"

func TestProxy_Stats_InvalidRejectReasons_Good(t *testing.T) {
	stats := NewStats()

	stats.OnReject(Event{Error: "Low difficulty share"})
	stats.OnReject(Event{Error: "Malformed share"})

	summary := stats.Summary()
	if summary.Rejected != 2 {
		t.Fatalf("expected two rejected shares, got %d", summary.Rejected)
	}
	if summary.Invalid != 2 {
		t.Fatalf("expected two invalid shares, got %d", summary.Invalid)
	}
}
