package proxy

import (
	"strings"
	"testing"
)

// TestJob_BlobWithFixedByte_Good verifies nonce patching on a full 160-char blob.
//
//	job := proxy.Job{Blob: strings.Repeat("0", 160)}
//	result := job.BlobWithFixedByte(0x2A) // chars 78-79 become "2a"
func TestCoreImpl_Job_BlobWithFixedByte_Good(t *testing.T) {
	job := Job{Blob: strings.Repeat("0", 160), JobID: "job-1", Target: "b88d0600"}
	got := job.BlobWithFixedByte(0x2A)
	if len(got) != 160 {
		t.Fatalf("expected length 160, got %d", len(got))
	}
	if got[78:80] != "2a" {
		t.Fatalf("expected fixed byte patch, got %q", got[78:80])
	}
}

// TestJob_BlobWithFixedByte_Bad verifies a short blob is returned unchanged.
//
//	job := proxy.Job{Blob: "0000"}
//	result := job.BlobWithFixedByte(0x2A) // too short, returned as-is
func TestCoreImpl_Job_BlobWithFixedByte_Bad(t *testing.T) {
	shortBlob := "0000"
	job := Job{Blob: shortBlob}
	got := job.BlobWithFixedByte(0x2A)
	if got != shortBlob {
		t.Fatalf("expected short blob to be returned unchanged, got %q", got)
	}
}

// TestJob_BlobWithFixedByte_Ugly verifies fixedByte 0xFF renders as lowercase "ff".
//
//	job := proxy.Job{Blob: strings.Repeat("0", 160)}
//	result := job.BlobWithFixedByte(0xFF) // chars 78-79 become "ff" (not "FF")
func TestCoreImpl_Job_BlobWithFixedByte_Ugly(t *testing.T) {
	job := Job{Blob: strings.Repeat("0", 160), JobID: "job-1", Target: "b88d0600"}
	got := job.BlobWithFixedByte(0xFF)
	if got[78:80] != "ff" {
		t.Fatalf("expected lowercase 'ff', got %q", got[78:80])
	}
	if len(got) != 160 {
		t.Fatalf("expected blob length preserved, got %d", len(got))
	}
}

// TestJob_IsValid_Good verifies a job with blob and job ID is valid.
//
//	job := proxy.Job{Blob: "abc", JobID: "job-1"}
//	job.IsValid() // true
func TestCoreImpl_Job_IsValid_Good(t *testing.T) {
	job := Job{Blob: strings.Repeat("0", 160), JobID: "job-1", Target: "b88d0600"}
	if !job.IsValid() {
		t.Fatalf("expected job with blob and job id to be valid")
	}
}

// TestJob_IsValid_Bad verifies a job with empty blob or job ID is invalid.
//
//	job := proxy.Job{Blob: "", JobID: "job-1"}
//	job.IsValid() // false
func TestCoreImpl_Job_IsValid_Bad(t *testing.T) {
	if (Job{Blob: "", JobID: "job-1", Target: "b88d0600"}).IsValid() {
		t.Fatalf("expected empty blob to be invalid")
	}
	if (Job{Blob: strings.Repeat("0", 160), JobID: "", Target: "b88d0600"}).IsValid() {
		t.Fatalf("expected empty job id to be invalid")
	}
	if (Job{Blob: strings.Repeat("0", 160), JobID: "job-1", Target: "bad"}).IsValid() {
		t.Fatalf("expected invalid target to be rejected")
	}
}

// TestJob_IsValid_Ugly verifies a zero-value job is invalid.
//
//	job := proxy.Job{}
//	job.IsValid() // false
func TestCoreImpl_Job_IsValid_Ugly(t *testing.T) {
	if (Job{}).IsValid() {
		t.Fatalf("expected zero-value job to be invalid")
	}
}
