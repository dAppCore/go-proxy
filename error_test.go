package proxy

import (
	"errors"
	"testing"
)

func TestError_NewScopedError_Good(t *testing.T) {
	err := NewScopedError("proxy.config", "bind list is empty", nil)

	if err == nil {
		t.Fatalf("expected scoped error")
	}
	if got := err.Error(); got != "proxy.config: bind list is empty" {
		t.Fatalf("unexpected scoped error string: %q", got)
	}
}

func TestError_NewScopedError_Bad(t *testing.T) {
	cause := errors.New("permission denied")
	err := NewScopedError("proxy.config", "read config failed", cause)

	if err == nil {
		t.Fatalf("expected scoped error")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("expected errors.Is to unwrap the original cause")
	}
	if got := err.Error(); got != "proxy.config: read config failed: permission denied" {
		t.Fatalf("unexpected wrapped error string: %q", got)
	}
}

func TestError_NewScopedError_Ugly(t *testing.T) {
	var scoped *ScopedError

	if got := scoped.Error(); got != "" {
		t.Fatalf("expected nil scoped error string to be empty, got %q", got)
	}
	if scoped.Unwrap() != nil {
		t.Fatalf("expected nil scoped error to unwrap to nil")
	}
}
