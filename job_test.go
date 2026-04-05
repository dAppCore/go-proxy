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
	if got := job.DifficultyFromTarget(); got != 10000 {
		t.Fatalf("expected difficulty 10000, got %d", got)
	}
}

func TestJob_DifficultyFromTarget_MaxTarget(t *testing.T) {
	job := Job{Target: "ffffffff"}
	if got := job.DifficultyFromTarget(); got != 1 {
		t.Fatalf("expected minimum difficulty 1, got %d", got)
	}
}
