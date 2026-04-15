package proxy

import (
	"reflect"
	"testing"
)

func TestAxHelpers_splitFieldsBySeparators_Good(t *testing.T) {
	got := splitFieldsBySeparators("alpha, beta:gamma;delta|epsilon zeta")
	want := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestAxHelpers_valueStringSlice_Bad(t *testing.T) {
	if got := valueStringSlice(123); got != nil {
		t.Fatalf("expected non-slice input to return nil, got %v", got)
	}
}

func TestAxHelpers_valueStringSlice_Ugly(t *testing.T) {
	got := valueStringSlice([]any{"a", 1, "b", true, "c"})
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected filtered strings %v, got %v", want, got)
	}
}

func TestAxHelpers_valueUint64_Good(t *testing.T) {
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
		{name: "negative_int", in: -1, want: 0},
		{name: "negative_int64", in: int64(-2), want: 0},
		{name: "negative_float64", in: -3.5, want: 0},
		{name: "default", in: true, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := valueUint64(tc.in); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
}
