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

func TestError_ScopedError_Error_Good(t *testing.T) {
	err := &ScopedError{Scope: "proxy.config", Message: "load failed"}
	if got := err.Error(); got != "proxy.config: load failed" {
		t.Fatalf("expected scoped error text, got %q", got)
	}
}

func TestError_ScopedError_Error_Bad(t *testing.T) {
	var err *ScopedError
	if got := err.Error(); got != "" {
		t.Fatalf("expected nil scoped error text empty, got %q", got)
	}
}

func TestError_ScopedError_Error_Ugly(t *testing.T) {
	cause := errors.New("root")
	err := &ScopedError{Scope: "proxy.pool", Message: "connect failed", Cause: cause}
	if got := err.Error(); got != "proxy.pool: connect failed: root" {
		t.Fatalf("expected scoped error with cause, got %q", got)
	}
}

func TestError_ScopedError_Unwrap_Good(t *testing.T) {
	cause := errors.New("root")
	err := &ScopedError{Cause: cause}
	if got := err.Unwrap(); got != cause {
		t.Fatalf("expected unwrap cause, got %v", got)
	}
}

func TestError_ScopedError_Unwrap_Bad(t *testing.T) {
	err := &ScopedError{}
	if got := err.Unwrap(); got != nil {
		t.Fatalf("expected nil cause unwrap, got %v", got)
	}
}

func TestError_ScopedError_Unwrap_Ugly(t *testing.T) {
	var err *ScopedError
	if got := err.Unwrap(); got != nil {
		t.Fatalf("expected nil receiver unwrap nil, got %v", got)
	}
}
