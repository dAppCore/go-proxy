package proxy

import (
	"strings"
	"testing"
)

func TestJob_BlobWithFixedByte(t *testing.T) {
	job := Job{Blob: strings.Repeat("0", 160)}
	got := job.BlobWithFixedByte(0x2A)
	if len(got) != 160 {
		t.Fatalf("expected length 160, got %d", len(got))
	}
	if got[78:80] != "2a" {
		t.Fatalf("expected fixed byte patch, got %q", got[78:80])
	}
}

func TestJob_DifficultyFromTarget(t *testing.T) {
	job := Job{Target: "b88d0600"}
	if got := job.DifficultyFromTarget(); got == 0 {
		t.Fatalf("expected non-zero difficulty")
	}
}
