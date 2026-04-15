package pool

import "testing"

func TestPoolAxHelpers_Good(t *testing.T) {
	if got := trimString("  hello  "); got != "hello" {
		t.Fatalf("expected trimmed string, got %q", got)
	}
	if got := lowerString("HeLLo"); got != "hello" {
		t.Fatalf("expected lower-case string, got %q", got)
	}
	if !containsString("hello world", "world") {
		t.Fatal("expected containsString to match substring")
	}
	if !equalFoldString("XMRig", "xmrig") {
		t.Fatal("expected equalFoldString to ignore case")
	}
	if got := valueString("agent"); got != "agent" {
		t.Fatalf("expected string value to round-trip, got %q", got)
	}
	if got := valueUint64(int64(7)); got != 7 {
		t.Fatalf("expected int64 to convert, got %d", got)
	}
}

func TestPoolAxHelpers_Bad(t *testing.T) {
	if got := valueString(123); got != "" {
		t.Fatalf("expected non-string to return empty string, got %q", got)
	}
}

func TestPoolAxHelpers_valueUint64_Good(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want uint64
	}{
		{name: "uint64", in: uint64(42), want: 42},
		{name: "uint32", in: uint32(43), want: 43},
		{name: "int", in: 44, want: 44},
		{name: "int64", in: int64(45), want: 45},
		{name: "float64", in: 46.75, want: 46},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := valueUint64(tc.in); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
}

func TestPoolAxHelpers_Ugly(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want uint64
	}{
		{name: "negative_int", in: -7, want: 0},
		{name: "negative_int64", in: int64(-8), want: 0},
		{name: "negative_float64", in: -9.5, want: 0},
		{name: "default", in: true, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := valueUint64(tc.in); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
	if got := containsString("abc", ""); !got {
		t.Fatal("expected empty needle to match")
	}
}
