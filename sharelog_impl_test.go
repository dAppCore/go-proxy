package proxy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShareLogImpl_OnAccept_Good(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shares.log")

	sink := newShareLogSink(path)
	sink.OnAccept(Event{Miner: &Miner{user: "WALLET"}, Diff: 1234, Latency: 56})
	sink.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read share log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "ACCEPT  WALLET  diff=1234  latency=56ms") {
		t.Fatalf("expected accept line, got %q", text)
	}
}

func TestShareLogImpl_OnReject_Bad(t *testing.T) {
	var sink *shareLogSink
	sink.OnReject(Event{})
	sink.OnReject(Event{Miner: nil})
}

func TestShareLogImpl_sanitizeLogField_Ugly(t *testing.T) {
	got := sanitizeLogField("bad\"\nreason\r\u2028\u2029\\")
	if !strings.Contains(got, "\\\"") {
		t.Fatalf("expected quotes to be escaped, got %q", got)
	}
	if !strings.Contains(got, "\\\\") {
		t.Fatalf("expected backslashes to be escaped, got %q", got)
	}
	if strings.ContainsAny(got, "\n\r") {
		t.Fatalf("expected newlines to be normalised, got %q", got)
	}
	if strings.Contains(got, "\u2028") || strings.Contains(got, "\u2029") {
		t.Fatalf("expected unicode line separators to be normalised, got %q", got)
	}
}
