package proxylog

import (
	"testing"
)

func TestShareLog_NewShareLog_Good(t *testing.T) {
	path := pathJoin(t.TempDir(), "shares.log")
	sink := NewShareLog(path)
	if sink == nil {
		t.Fatal("expected share log sink")
	}
	if sink.path != path {
		t.Fatalf("expected path %q, got %q", path, sink.path)
	}
	sink.Close()
}

func TestShareLog_NewShareLog_Bad(t *testing.T) {
	sink := NewShareLog("")
	if sink == nil {
		t.Fatal("expected share log sink")
	}
	if sink.path != "" {
		t.Fatalf("expected empty path to be preserved, got %q", sink.path)
	}
	sink.Close()
}

func TestShareLog_NewShareLog_Ugly(t *testing.T) {
	path := pathJoin(t.TempDir(), "share log.txt")
	sink := NewShareLog(path)
	if sink == nil {
		t.Fatal("expected share log sink")
	}
	if sink.path != path {
		t.Fatalf("expected path %q, got %q", path, sink.path)
	}
	sink.Close()
}
