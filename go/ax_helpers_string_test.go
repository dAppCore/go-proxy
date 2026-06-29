package proxy

import (
	"reflect"
	"testing"
)

func TestAxHelpersString_repeatString_Good(t *testing.T) {
	if got := repeatString("ab", 3); got != "ababab" {
		t.Fatalf("expected ababab, got %q", got)
	}
}

func TestAxHelpersString_repeatString_Bad(t *testing.T) {
	if got := repeatString("ab", 0); got != "" {
		t.Fatalf("expected empty on zero count, got %q", got)
	}
	if got := repeatString("ab", -5); got != "" {
		t.Fatalf("expected empty on negative count, got %q", got)
	}
}

func TestAxHelpersString_repeatString_Ugly(t *testing.T) {
	if got := repeatString("", 9); got != "" {
		t.Fatalf("expected empty when value is empty, got %q", got)
	}
}

func TestAxHelpersString_countString_Good(t *testing.T) {
	if got := countString("ababab", "ab"); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestAxHelpersString_countString_Bad(t *testing.T) {
	if got := countString("anything", ""); got != 0 {
		t.Fatalf("expected 0 for empty needle, got %d", got)
	}
}

func TestAxHelpersString_countString_Ugly(t *testing.T) {
	// Overlapping matches do not double-count: the scan advances one byte at a
	// time but each match is counted at a distinct offset.
	if got := countString("aaa", "aa"); got != 2 {
		t.Fatalf("expected 2 overlapping matches, got %d", got)
	}
	if got := countString("ab", "abc"); got != 0 {
		t.Fatalf("expected 0 when needle longer than value, got %d", got)
	}
}

func TestAxHelpersString_containsAnyString_Good(t *testing.T) {
	if !containsAnyString("hello", "xyzl") {
		t.Fatal("expected true when value shares a rune with chars")
	}
}

func TestAxHelpersString_containsAnyString_Bad(t *testing.T) {
	if containsAnyString("hello", "xyz") {
		t.Fatal("expected false when no rune overlaps")
	}
	if containsAnyString("", "abc") {
		t.Fatal("expected false for empty value")
	}
	if containsAnyString("abc", "") {
		t.Fatal("expected false for empty chars")
	}
}

func TestAxHelpersString_containsAnyString_Ugly(t *testing.T) {
	// Multi-byte runes must match by rune, not by byte.
	if !containsAnyString("café", "é") {
		t.Fatal("expected true for multi-byte rune match")
	}
}

func TestAxHelpersString_trimSpaceString_Good(t *testing.T) {
	if got := trimSpaceString("  padded  "); got != "padded" {
		t.Fatalf("expected trimmed value, got %q", got)
	}
}

func TestAxHelpersString_trimSpaceString_Bad(t *testing.T) {
	if got := trimSpaceString(""); got != "" {
		t.Fatalf("expected empty for empty input, got %q", got)
	}
}

func TestAxHelpersString_trimSpaceString_Ugly(t *testing.T) {
	if got := trimSpaceString("\t\n no-edge-space \r\n"); got != "no-edge-space" {
		t.Fatalf("expected interior space preserved, got %q", got)
	}
}

func TestAxHelpersString_lastIndexByte_Good(t *testing.T) {
	if got := lastIndexByte("a.b.c", '.'); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestAxHelpersString_lastIndexByte_Bad(t *testing.T) {
	if got := lastIndexByte("abc", '.'); got != -1 {
		t.Fatalf("expected -1 when byte absent, got %d", got)
	}
	if got := lastIndexByte("", 'a'); got != -1 {
		t.Fatalf("expected -1 for empty string, got %d", got)
	}
}

func TestAxHelpersString_lastIndexByte_Ugly(t *testing.T) {
	if got := lastIndexByte("....", '.'); got != 3 {
		t.Fatalf("expected last index 3 when all bytes match, got %d", got)
	}
}

func TestAxHelpersString_equalFoldString_Good(t *testing.T) {
	if !equalFoldString("Mining", "mining") {
		t.Fatal("expected case-insensitive equality")
	}
}

func TestAxHelpersString_equalFoldString_Bad(t *testing.T) {
	if equalFoldString("mining", "minor") {
		t.Fatal("expected inequality for different words")
	}
}

func TestAxHelpersString_equalFoldString_Ugly(t *testing.T) {
	if !equalFoldString("", "") {
		t.Fatal("expected two empty strings to be equal")
	}
}

func TestAxHelpersString_splitStringN_Good(t *testing.T) {
	got := splitStringN("a:b:c", ":", 2)
	want := []string{"a", "b:c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestAxHelpersString_splitStringN_Ugly(t *testing.T) {
	got := splitStringN("nopeseparator", ":", 2)
	want := []string{"nopeseparator"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected single-element slice, got %v", got)
	}
}

func TestAxHelpersString_valueString_Good(t *testing.T) {
	if got := valueString("hello"); got != "hello" {
		t.Fatalf("expected hello, got %q", got)
	}
}

func TestAxHelpersString_valueString_Bad(t *testing.T) {
	if got := valueString(123); got != "" {
		t.Fatalf("expected empty for non-string input, got %q", got)
	}
	if got := valueString(nil); got != "" {
		t.Fatalf("expected empty for nil input, got %q", got)
	}
}

func TestAxHelpersString_valueMap_Good(t *testing.T) {
	in := map[string]any{"k": "v"}
	got := valueMap(in)
	if got["k"] != "v" {
		t.Fatalf("expected map round-trip, got %v", got)
	}
}

func TestAxHelpersString_valueMap_Bad(t *testing.T) {
	if got := valueMap("not-a-map"); got != nil {
		t.Fatalf("expected nil for non-map input, got %v", got)
	}
}

func TestAxHelpersString_valueStringSlice_Good(t *testing.T) {
	in := []string{"a", "b"}
	got := valueStringSlice(in)
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	// The helper must copy, not alias, the input slice.
	got[0] = "mutated"
	if in[0] != "a" {
		t.Fatal("expected valueStringSlice to copy, not alias, the input")
	}
}

func TestAxHelpersString_trimString_Good(t *testing.T) {
	if got := trimString("  x  "); got != "x" {
		t.Fatalf("expected trimmed value, got %q", got)
	}
}

func TestAxHelpersString_lowerString_Good(t *testing.T) {
	if got := lowerString("MiXeD"); got != "mixed" {
		t.Fatalf("expected lowercased value, got %q", got)
	}
}

func TestAxHelpersString_containsString_Good(t *testing.T) {
	if !containsString("haystack", "ays") {
		t.Fatal("expected substring match")
	}
	if containsString("haystack", "zzz") {
		t.Fatal("expected no match for absent substring")
	}
}
