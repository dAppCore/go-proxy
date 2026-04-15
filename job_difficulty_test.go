package proxy

import "testing"

// TestJob_DifficultyFromTarget_Good verifies a known target converts to the expected difficulty.
//
//	job := proxy.Job{Target: "b88d0600"}
//	diff := job.DifficultyFromTarget() // 10000
func TestJob_DifficultyFromTarget_Good(t *testing.T) {
	job := Job{Target: "b88d0600"}
	if got := job.DifficultyFromTarget(); got != 10000 {
		t.Fatalf("expected difficulty 10000, got %d", got)
	}
}

// TestJob_DifficultyFromTarget_Bad verifies a zero target produces difficulty 0 without panic.
//
//	job := proxy.Job{Target: "00000000"}
//	diff := job.DifficultyFromTarget() // 0 (no divide-by-zero)
func TestJob_DifficultyFromTarget_Bad(t *testing.T) {
	cases := []Job{
		{Target: ""},
		{Target: "123"},
		{Target: "zzzzzzzz"},
		{Target: "00000000"},
	}
	for _, job := range cases {
		if got := job.DifficultyFromTarget(); got != 0 {
			t.Fatalf("expected difficulty 0 for malformed target %q, got %d", job.Target, got)
		}
	}
}

// TestJob_DifficultyFromTarget_Ugly verifies the maximum target "ffffffff" yields difficulty 1.
//
//	job := proxy.Job{Target: "ffffffff"}
//	diff := job.DifficultyFromTarget() // 1
func TestJob_DifficultyFromTarget_Ugly(t *testing.T) {
	job := Job{Target: "ffffffff"}
	if got := job.DifficultyFromTarget(); got != 1 {
		t.Fatalf("expected minimum difficulty 1, got %d", got)
	}
}
