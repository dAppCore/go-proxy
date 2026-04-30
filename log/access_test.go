package log

import (
	"path/filepath"
	"testing"
)

func TestAccessLog_NewAccessLog_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	sink := NewAccessLog(path)
	if sink == nil {
		t.Fatal("expected access log sink")
	}
	if sink.path != path {
		t.Fatalf("expected path %q, got %q", path, sink.path)
	}
	sink.Close()
}

func TestAccessLog_NewAccessLog_Bad(t *testing.T) {
	sink := NewAccessLog("")
	if sink == nil {
		t.Fatal("expected access log sink")
	}
	if sink.path != "" {
		t.Fatalf("expected empty path to be preserved, got %q", sink.path)
	}
	sink.Close()
}

func TestAccessLog_NewAccessLog_Ugly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access log.txt")
	sink := NewAccessLog(path)
	if sink == nil {
		t.Fatal("expected access log sink")
	}
	if sink.path != path {
		t.Fatalf("expected path %q, got %q", path, sink.path)
	}
	sink.Close()
}
