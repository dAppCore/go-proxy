package proxy

import (
	"testing"

	core "dappco.re/go"
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
	cause := core.NewError("permission denied")
	err := NewScopedError("proxy.config", "read config failed", cause)

	if err == nil {
		t.Fatalf("expected scoped error")
	}
	if err.CauseError() != cause {
		t.Fatalf("expected errors.Is to unwrap the original cause")
	}
	if got := err.Error(); got != "proxy.config: read config failed: permission denied" {
		t.Fatalf("unexpected wrapped error string: %q", got)
	}
}

func TestError_NewScopedError_Ugly(t *testing.T) {
	// target symbol: NewScopedError
	var scoped *ScopedError

	if got := scoped.Error(); got != "" {
		t.Fatalf("expected nil scoped error string to be empty, got %q", got)
	}
	if scoped.CauseError() != nil {
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
	cause := core.NewError("root")
	err := &ScopedError{Scope: "proxy.pool", Message: "connect failed", Cause: cause}
	if got := err.Error(); got != "proxy.pool: connect failed: root" {
		t.Fatalf("expected scoped error with cause, got %q", got)
	}
}

func TestError_ScopedError_CauseError_Good(t *testing.T) {
	cause := core.NewError("root")
	err := &ScopedError{Cause: cause}
	if got := err.CauseError(); got != cause {
		t.Fatalf("expected unwrap cause, got %v", got)
	}
}

func TestError_ScopedError_CauseError_Bad(t *testing.T) {
	err := &ScopedError{}
	if got := err.CauseError(); got != nil {
		t.Fatalf("expected nil cause unwrap, got %v", got)
	}
}

func TestError_ScopedError_CauseError_Ugly(t *testing.T) {
	var err *ScopedError
	if got := err.CauseError(); got != nil {
		t.Fatalf("expected nil receiver unwrap nil, got %v", got)
	}
}
